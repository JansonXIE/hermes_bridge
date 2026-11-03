"""Worker process: claims queued tasks and supervises one Claude Code process per attempt.

Claude's stdout and stderr go straight to files rather than pipes, and the
process runs in its own session. A worker can therefore die or restart without
disturbing Claude, and its successor picks up the log from the saved offset.
"""

from __future__ import annotations

import fnmatch
import json
import logging
import os
import signal
import socket
import subprocess
import threading
import time
from pathlib import Path
from typing import Optional

from . import procs, stream_parser, workspace
from .config import NESTED_SESSION_ENV, Config, Project
from .dispatcher import Dispatcher
from .redact import Redactor
from .store import CANCELLED, CANCELLING, FAILED, RUNNING, SUCCEEDED, TIMED_OUT, LeaseLost, Store, new_id

log = logging.getLogger("hermes_bridge.worker")

SUMMARY_LIMIT = 8000
READ_CHUNK = 1 << 20
MAX_LINE_BYTES = 1 << 28
STDERR_EVENT_INTERVAL = 5.0
LIVENESS_WRITE_INTERVAL = 1.0
STOP_RETRY_INTERVAL = 5.0


def build_child_env(config: Config, project: Project, task: dict, attempt: dict, run_dir: Path) -> dict[str, str]:
    """Allowlisted environment: bridge secrets and unrelated credentials never reach the child."""
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in NESTED_SESSION_ENV and any(fnmatch.fnmatchcase(k, p) for p in config.env_passthrough)
    }
    env.update(workspace.private_dirs(run_dir))
    env.update(project.env)
    slot = task["slot"] or 0
    env.update({
        procs.MARKER_ENV: attempt["id"],
        "BRIDGE_TASK_ID": task["id"],
        "BRIDGE_SLOT": str(slot),
        "BRIDGE_PORT_BASE": str(config.port_base + slot * config.port_block),
        "BRIDGE_PORT_COUNT": str(config.port_block),
    })
    return env


def build_argv(config: Config, project: Project, task: dict, resume_session: Optional[str]) -> list[str]:
    """Argument array for the installed CLI. The prompt is passed on stdin, never on the command line."""
    argv = [*config.claude_command, "-p", "--output-format", "stream-json", "--verbose"]
    if config.include_partial_messages:
        argv.append("--include-partial-messages")
    argv += ["--permission-mode", project.permission_mode, "--name", task["id"]]
    if project.allowed_tools:
        argv += ["--allowedTools", ",".join(project.allowed_tools)]
    if project.disallowed_tools:
        argv += ["--disallowedTools", ",".join(project.disallowed_tools)]
    model = task["model"] or project.model
    if model:
        argv += ["--model", model]
    effort = task["effort"] or project.effort
    if effort:
        argv += ["--effort", effort]
    budget = task["max_budget_usd"] or project.max_budget_usd
    if budget:
        argv += ["--max-budget-usd", str(budget)]
    if config.append_system_prompt:
        argv += ["--append-system-prompt", config.append_system_prompt]
    if resume_session:
        argv += ["--resume", resume_session]
    return [*argv, *config.claude_extra_args]


class AttemptRunner(threading.Thread):
    def __init__(self, worker: "Worker", task: dict, attempt: dict, recovering: bool = False):
        super().__init__(name=f"attempt-{attempt['id']}", daemon=True)
        self.worker = worker
        self.config = worker.config
        self.store = worker.store
        self.redactor = worker.redactor
        self.task_id = task["id"]
        self.attempt_id = attempt["id"]
        self.token = attempt["lease_token"]
        self.recovering = recovering
        self.project = self.config.projects.get(task["project_id"])
        self.proc: Optional[subprocess.Popen] = None
        self.stdout_offset = attempt["stdout_offset"]
        self.stderr_offset = attempt["stderr_offset"]
        self.stdout_path = attempt["stdout_path"]
        self.stderr_path = attempt["stderr_path"]
        self.pid = attempt["pid"]
        self.start_time = attempt["proc_start_time"]
        self.boot = attempt["boot_id"]
        self.spawn_intent_at = attempt["spawn_intent_at"]
        self.number = attempt["number"]
        self.result = json.loads(attempt["result"]) if attempt["result"] else None
        self.lease_renewed_at = time.time()
        self.last_liveness_write = 0.0
        self.last_stderr_event = 0.0
        self.silence_alerted = False
        self.timed_out = False
        self.parse_errors = 0
        self.pending_stderr: list[str] = []

    # ---- lifecycle ------------------------------------------------------

    def run(self) -> None:
        try:
            if self.recovering:
                if not self._recover():
                    return
            elif not self._start():
                return
            self._supervise()
        except LeaseLost:
            log.warning("attempt %s: lease lost, leaving it to the new owner", self.attempt_id)
        except Exception as exc:
            log.exception("attempt %s: supervisor error", self.attempt_id)
            self._finish_after_internal_error(exc)

    def _finish_after_internal_error(self, exc: Exception) -> None:
        """Stop the child if possible; without confirmation leave the lease to expire for recovery."""
        try:
            if self._stop_tree_once():
                self._finish(FAILED, error_code="bridge_error", error_message=self.redactor.text(str(exc))[:500])
        except Exception:
            log.exception("attempt %s: could not record internal error", self.attempt_id)

    def _task(self) -> dict:
        return self.store.get_task(self.task_id)

    def _finish(self, status: str, **fields) -> None:
        self.store.finish_attempt(self.attempt_id, self.token, self.task_id, status, **fields)
        log.info("task %s finished: %s", self.task_id, status)

    def _start(self) -> bool:
        task = self._task()
        if task["cancel_requested_at"]:
            self._finish(CANCELLED)
            return False
        if self.project is None:
            self._finish(FAILED, error_code="project_unavailable", error_message="project removed from configuration")
            return False

        source = self.store.get_task(task["continue_task_id"]) if task["continue_task_id"] else None
        if source and not (source["claude_session_id"] and source["workdir"] and Path(source["workdir"]).is_dir()):
            self._finish(FAILED, error_code="continue_unavailable", error_message="source task has no resumable session or worktree")
            return False

        run_dir = self.config.runs_dir / self.task_id
        workspace.ensure_private(self.config.runs_dir)
        workspace.ensure_private(run_dir)
        attempt_dir = run_dir / f"attempt-{self.number}"
        workspace.ensure_private(attempt_dir)
        try:
            if source:
                ws = workspace.Workspace(Path(source["workdir"]), source["branch"], source["base_commit"])
            else:
                ws = workspace.prepare(self.config, self.project, task, self._dependency_branches(task))
        except workspace.WorkspaceError as exc:
            self._finish(FAILED, error_code=exc.code, error_message=self.redactor.text(str(exc)))
            return False
        self.store.set_workspace(self.attempt_id, self.token, self.task_id, str(ws.workdir), ws.branch, ws.base_commit, str(run_dir))

        prompt_path = attempt_dir / "prompt.txt"
        prompt_path.write_text(self._effective_prompt(task), encoding="utf-8")
        os.chmod(prompt_path, 0o600)
        self.stdout_path = str(attempt_dir / "stdout.jsonl")
        self.stderr_path = str(attempt_dir / "stderr.log")
        task = self._task()
        attempt = self.store.get_attempt(self.attempt_id)
        env = build_child_env(self.config, self.project, task, attempt, run_dir)
        argv = build_argv(self.config, self.project, task, source["claude_session_id"] if source else None)

        self.store.mark_spawn_intent(self.attempt_id, self.token, self.stdout_path, self.stderr_path)
        self.spawn_intent_at = time.time()
        try:
            with open(prompt_path, "rb") as stdin, open(self.stdout_path, "ab") as out, open(self.stderr_path, "ab") as err:
                self.proc = subprocess.Popen(
                    argv, cwd=ws.workdir, env=env, stdin=stdin, stdout=out, stderr=err, start_new_session=True,
                )
        except OSError as exc:
            self._finish(FAILED, error_code="spawn_error", error_message=self.redactor.text(str(exc))[:500])
            return False
        self.pid = self.proc.pid
        stat = procs.read_stat(self.pid)
        self.start_time = stat.start_time if stat else None
        self.boot = procs.boot_id()
        self.store.mark_spawned(self.attempt_id, self.token, self.task_id, self.pid, self.start_time or 0, self.boot)
        return True

    def _dependency_branches(self, task: dict) -> list[str]:
        deps = (self.store.get_task(d) for d in json.loads(task["depends_on"]))
        return [d["branch"] for d in deps if d and d["branch"]]

    def _effective_prompt(self, task: dict) -> str:
        branches = self._dependency_branches(task)
        if task["kind"] != "integration" or not branches:
            return task["prompt"]
        listing = "\n".join(f"- {b}" for b in branches)
        return (
            f"{task['prompt']}\n\n"
            f"Integrate these branches into the current branch with `git merge`, in this order, and "
            f"resolve any conflicts while keeping the intent of both sides:\n{listing}\n"
            f"Afterwards run the relevant tests and report the results."
        )

    # ---- recovery after a lost lease ------------------------------------

    def _recover(self) -> bool:
        """Settle what happened to the process. Returns True when it is alive and adopted."""
        task = self._task()
        if not self.spawn_intent_at:
            # Crashed before spawning: nothing ran, so this is not a re-execution.
            self.store.requeue_for_retry(self.attempt_id, self.token, self.task_id, "worker_lost_before_spawn")
            return False
        if self.pid is None:
            # The crash fell between spawning and recording the identity: look for the marker.
            marked = procs.find_marked(self.attempt_id)
            leader = next((p for p in marked if (s := procs.read_stat(p)) and s.pgrp == p), None)
            stat = procs.read_stat(leader) if leader else None
            if stat is None:
                return self._conclude_dead_process(task)
            self.pid, self.start_time, self.boot = leader, stat.start_time, procs.boot_id()
            self.store.mark_spawned(self.attempt_id, self.token, self.task_id, self.pid, self.start_time, self.boot)
        elif not procs.is_alive(self.pid, self.start_time, self.boot):
            return self._conclude_dead_process(task)
        status = CANCELLING if task["cancel_requested_at"] else RUNNING
        self.store.set_status(self.attempt_id, self.token, self.task_id, status, adopted=True, pid=self.pid)
        log.info("task %s: adopted live process %s", self.task_id, self.pid)
        return True

    def _conclude_dead_process(self, task: dict) -> bool:
        """The process is gone: its saved output is the only evidence of how it ended."""
        self._ingest(final=True)
        self._stop_tree_once()
        if task["cancel_requested_at"]:
            self._finish(CANCELLED)
        elif self.result is not None:
            self._conclude(exit_code=None, exit_signal=None)
        elif task["safe_to_retry"] and task["attempt_count"] < self.config.max_attempts:
            self.store.requeue_for_retry(self.attempt_id, self.token, self.task_id, "process_lost")
        else:
            # Side effects are unknown. Hermes decides; the bridge does not re-run on its own.
            self._finish(
                FAILED, error_code="process_lost",
                error_message="worker and process were lost before a final result; the work may be partially applied",
                changes=self._safe_changes(task),
            )
        return False

    # ---- supervision ----------------------------------------------------

    def _supervise(self) -> None:
        stop_started = None
        stop_escalated = False
        unconfirmed_reported = False
        while True:
            self._ingest()
            now = time.time()
            self._maybe_renew(now)
            task = self._task()

            if self.worker.shutting_down.is_set():
                self.store.release_lease(self.attempt_id, self.token)
                return

            if stop_started is None:
                if task["cancel_requested_at"]:
                    if task["status"] != CANCELLING:
                        self.store.set_status(self.attempt_id, self.token, self.task_id, CANCELLING)
                    stop_started = now
                elif task["started_at"] and now >= task["started_at"] + task["timeout_seconds"]:
                    self.timed_out = True
                    self.store.add_attempt_event(self.attempt_id, self.token, self.task_id, "task.timeout",
                                                 {"timeout_seconds": task["timeout_seconds"]})
                    stop_started = now
                if stop_started is not None:
                    procs.signal_pids(procs.tree_members(self.attempt_id), signal.SIGTERM)

            if stop_started is not None:
                members = self._reap_and_list_tree()
                if not members:
                    self._ingest(final=True)
                    if self.timed_out:
                        self._finish(TIMED_OUT, error_code="timeout", changes=self._safe_changes(task), **self._exit_info())
                    else:
                        self._finish(CANCELLED, changes=self._safe_changes(task), **self._exit_info())
                    return
                elapsed = now - stop_started
                if elapsed >= self.config.term_grace_seconds and not stop_escalated:
                    procs.signal_pids(members, signal.SIGKILL)
                    stop_escalated = True
                elif stop_escalated and elapsed >= self.config.term_grace_seconds + self.config.kill_wait_seconds:
                    # Never report "stopped" until it is confirmed; keep trying.
                    if not unconfirmed_reported:
                        self.store.add_attempt_event(self.attempt_id, self.token, self.task_id, "stop.unconfirmed", {"pids": members})
                        unconfirmed_reported = True
                    procs.signal_pids(members, signal.SIGKILL)
                    stop_started = now - self.config.term_grace_seconds - self.config.kill_wait_seconds + STOP_RETRY_INTERVAL
            elif self._process_exited():
                self._ingest(final=True)
                self._conclude(**self._exit_info())
                return
            else:
                self._check_silence(task, now)

            time.sleep(self.config.tick_seconds)

    def _process_exited(self) -> bool:
        if self.proc is not None:
            return self.proc.poll() is not None
        return not procs.is_alive(self.pid, self.start_time, self.boot)

    def _exit_info(self) -> dict:
        if self.proc is None or self.proc.returncode is None:
            return {"exit_code": None, "exit_signal": None}
        code = self.proc.returncode
        return {"exit_code": code, "exit_signal": None} if code >= 0 else {"exit_code": None, "exit_signal": -code}

    def _reap_and_list_tree(self) -> list[int]:
        if self.proc is not None:
            self.proc.poll()
        return procs.tree_members(self.attempt_id)

    def _stop_tree_once(self) -> bool:
        """Stop whatever remains of the attempt's tree. Returns True once confirmed gone."""
        deadline = time.time() + self.config.term_grace_seconds
        members = self._reap_and_list_tree()
        if members:
            procs.signal_pids(members, signal.SIGTERM)
        while members and time.time() < deadline:
            time.sleep(min(self.config.tick_seconds, 0.2))
            self._maybe_renew(time.time())
            members = self._reap_and_list_tree()
        deadline = time.time() + self.config.kill_wait_seconds
        while members and time.time() < deadline:
            procs.signal_pids(members, signal.SIGKILL)
            time.sleep(0.2)
            self._maybe_renew(time.time())
            members = self._reap_and_list_tree()
        return not members

    def _maybe_renew(self, now: float) -> None:
        if now - self.lease_renewed_at >= self.config.lease_seconds / 3:
            self.store.renew_lease(self.attempt_id, self.token, now + self.config.lease_seconds)
            self.lease_renewed_at = now

    def _check_silence(self, task: dict, now: float) -> None:
        # Silence is worth flagging but is not evidence of a hang; the task keeps running.
        last = task["last_event_at"]
        if self.silence_alerted or not last or now - last < self.config.silence_alert_seconds:
            return
        self.silence_alerted = True
        self.store.raise_alert(self.attempt_id, self.token, self.task_id, "silent", {"silent_seconds": int(now - last)})

    # ---- output ingestion -----------------------------------------------

    def _read_new(self, path: Optional[str], offset: int) -> bytes:
        """Read from offset, growing past READ_CHUNK until at least one whole line is available."""
        if not path:
            return b""
        size = READ_CHUNK
        try:
            with open(path, "rb") as f:
                while True:
                    f.seek(offset)
                    data = f.read(size)
                    if b"\n" in data or len(data) < size or size >= MAX_LINE_BYTES:
                        return data
                    size *= 2
        except OSError:
            return b""

    def _ingest(self, final: bool = False) -> None:
        while True:
            if not self._ingest_chunk(final):
                return

    def _ingest_chunk(self, final: bool) -> bool:
        """Process one chunk of new output. Returns True if there may be more to read."""
        events: list[tuple[str, dict]] = []
        last_action = None
        claude_session_id = None
        new_result = None

        stdout = self._read_new(self.stdout_path, self.stdout_offset)
        end = stdout.rfind(b"\n") + 1
        if stdout and (final or len(stdout) >= MAX_LINE_BYTES) and end == 0:
            end = len(stdout)  # An unterminated last line is all we will ever get.
        parse_errors = 0
        for line in stdout[:end].splitlines():
            if not line.strip():
                continue
            parsed = stream_parser.parse_line(line)
            if parsed.unparsable:
                parse_errors += 1
                continue
            events += [(t, self.redactor.value(p)) for t, p in parsed.events]
            last_action = parsed.last_action or last_action
            claude_session_id = parsed.claude_session_id or claude_session_id
            new_result = parsed.result or new_result

        # Partial-message chunks only prove liveness, so limit how often they cost a commit.
        now = time.time()
        stderr = self._read_new(self.stderr_path, self.stderr_offset)
        err_end = len(stderr) if final else stderr.rfind(b"\n") + 1
        if (not events and new_result is None and not final and not err_end
                and now - self.last_liveness_write < LIVENESS_WRITE_INTERVAL):
            return False
        if err_end:
            text = stderr[:err_end].decode("utf-8", "replace")
            self.pending_stderr += [ln for ln in text.splitlines() if ln.strip()][-20:]
            self.pending_stderr = self.pending_stderr[-20:]
        if self.pending_stderr and (final or now - self.last_stderr_event >= STDERR_EVENT_INTERVAL):
            events.append(("log.stderr", {"lines": [self.redactor.text(ln)[:500] for ln in self.pending_stderr]}))
            self.pending_stderr = []
            self.last_stderr_event = now

        progressed = end > 0 or err_end > 0
        if not progressed and not events:
            return False

        touched = end > 0
        self.parse_errors += parse_errors
        if new_result is not None:
            new_result["summary"] = self.redactor.text(new_result["summary"] or "")[:SUMMARY_LIMIT] or None
            new_result["errors"] = self.redactor.value(new_result["errors"])
            new_result["permission_denials"] = self.redactor.value(new_result["permission_denials"])
            self.result = new_result
        self.stdout_offset += end
        self.stderr_offset += err_end
        self.store.ingest(
            self.attempt_id, self.token, self.task_id, events, self.stdout_offset, self.stderr_offset,
            last_action and self.redactor.text(last_action), claude_session_id, new_result, touched,
        )
        if touched:
            self.last_liveness_write = now
            self.silence_alerted = False
        return len(stdout) >= READ_CHUNK or len(stderr) >= READ_CHUNK

    # ---- conclusion -----------------------------------------------------

    def _safe_changes(self, task: dict) -> Optional[dict]:
        if not task.get("workdir") or self.project is None:
            return None
        try:
            return workspace.collect_changes(task["workdir"], task["branch"], task["base_commit"], self.project, self.task_id)
        except workspace.WorkspaceError as exc:
            return {"error": self.redactor.text(str(exc))}

    def _conclude(self, exit_code: Optional[int], exit_signal: Optional[int]) -> None:
        """Decide the final state from the result event and exit status together."""
        task = self._task()
        self._stop_tree_once()
        if task["cancel_requested_at"]:
            self._finish(CANCELLED, changes=self._safe_changes(task), result=self.result,
                         exit_code=exit_code, exit_signal=exit_signal)
            return
        exit_fields = {"exit_code": exit_code, "exit_signal": exit_signal}
        changes = self._safe_changes(task)
        result = self.result

        if result is None:
            detail = f"exit_code={exit_code}, signal={exit_signal}, unparsable_lines={self.parse_errors}"
            self._finish(FAILED, error_code="no_result", changes=changes,
                         error_message=f"process ended without a final result event ({detail})", **exit_fields)
            return
        if result["is_error"] or exit_code not in (0, None) or exit_signal is not None:
            errors = "; ".join(str(e) for e in result["errors"])[:500]
            self._finish(FAILED, error_code=result["subtype"] or "claude_error", result=result, changes=changes,
                         error_message=errors or f"Claude Code reported failure (exit_code={exit_code})", **exit_fields)
            return

        # CLI success is not acceptance: the project's own check gives the verdict.
        acceptance = self._run_acceptance(task)
        latest = self._task()
        if latest["cancel_requested_at"]:
            self._finish(CANCELLED, result=result, changes=changes, acceptance=acceptance, **exit_fields)
            return
        self._finish(SUCCEEDED, result=result, changes=changes, acceptance=acceptance, **exit_fields)

    def _run_acceptance(self, task: dict) -> dict:
        command = self.project.verify_command if self.project else ()
        if not command:
            return {"status": "not_run", "reason": "project has no verify_command"}
        self.store.add_attempt_event(self.attempt_id, self.token, self.task_id, "acceptance.started", {"command": list(command)})
        run_dir = Path(task["run_dir"])
        log_path = run_dir / f"attempt-{self.number}" / "verify.log"
        env = build_child_env(self.config, self.project, task, self.store.get_attempt(self.attempt_id), run_dir)
        deadline = time.time() + self.project.verify_timeout_seconds
        started = time.time()
        with open(log_path, "wb") as out:
            try:
                proc = subprocess.Popen(
                    list(command), cwd=task["workdir"], env=env, stdin=subprocess.DEVNULL,
                    stdout=out, stderr=subprocess.STDOUT, start_new_session=True,
                )
            except OSError as exc:
                return {"status": "failed", "reason": f"could not start: {self.redactor.text(str(exc))[:300]}"}
            outcome = None
            while proc.poll() is None:
                now = time.time()
                self._maybe_renew(now)
                if now >= deadline:
                    outcome = "timed out"
                elif self._task()["cancel_requested_at"]:
                    outcome = "cancelled"
                if outcome:
                    self._stop_tree_once()
                    break
                time.sleep(self.config.tick_seconds)
        proc.wait()
        tail = log_path.read_bytes()[-4000:].decode("utf-8", "replace")
        acceptance = {
            "status": "passed" if proc.returncode == 0 and not outcome else "failed",
            "command": list(command),
            "exit_code": proc.returncode,
            "duration_seconds": round(time.time() - started, 1),
            "output_tail": self.redactor.text(tail),
            "log": str(log_path),
        }
        if outcome:
            acceptance["reason"] = outcome
        self.store.add_attempt_event(self.attempt_id, self.token, self.task_id, "acceptance.finished",
                                     {"status": acceptance["status"], "exit_code": proc.returncode})
        return acceptance


class Worker:
    def __init__(self, config: Config):
        self.config = config
        self.store = Store(config.db_path)
        self.redactor = Redactor(config.secrets())
        self.worker_id = f"{socket.gethostname()}:{os.getpid()}:{new_id('w')[2:10]}"
        self.started_at = time.time()
        self.shutting_down = threading.Event()
        self.runners: list[AttemptRunner] = []
        self.dispatcher = Dispatcher(config, self.store)
        self._last_maintenance = 0.0

    def request_shutdown(self, *_: object) -> None:
        self.shutting_down.set()

    def run(self) -> None:
        log.info("worker %s started (max_concurrency=%s)", self.worker_id, self.config.max_concurrency)
        self.dispatcher.start()
        try:
            while not self.shutting_down.is_set():
                try:
                    self.tick()
                except Exception:
                    log.exception("worker tick failed")
                self.shutting_down.wait(self.config.poll_seconds)
        finally:
            # Claude keeps running on shutdown; leases are released so the next worker adopts at once.
            self.dispatcher.stop()
            for runner in self.runners:
                runner.join(timeout=self.config.tick_seconds * 10)
            self.store.remove_worker(self.worker_id)
            log.info("worker %s stopped", self.worker_id)

    def tick(self) -> None:
        now = time.time()
        self.store.worker_heartbeat(self.worker_id, os.getpid(), self.started_at)
        self.runners = [r for r in self.runners if r.is_alive()]
        self._recover_expired(now)
        self.store.block_failed_dependents()
        while not self.shutting_down.is_set():
            claimed = self.store.claim_next(self.worker_id, self.config.lease_seconds, self.config.max_concurrency)
            if claimed is None:
                break
            self._launch(*claimed)
        if now - self._last_maintenance > 3600:
            self._last_maintenance = now
            self.store.prune_events(now - self.config.event_retention_days * 86400)
            self.store.prune_workers(now - 3600)

    def _recover_expired(self, now: float) -> None:
        for attempt in self.store.expired_attempts(now):
            taken = self.store.take_over(attempt, self.worker_id, self.config.lease_seconds)
            if taken:
                log.warning("task %s: lease expired, recovering attempt %s", attempt["task_id"], attempt["id"])
                self._launch(*taken, recovering=True)

    def _launch(self, task: dict, attempt: dict, recovering: bool = False) -> None:
        runner = AttemptRunner(self, task, attempt, recovering=recovering)
        self.runners.append(runner)
        runner.start()


def run_worker(config: Config) -> None:
    worker = Worker(config)
    signal.signal(signal.SIGTERM, worker.request_shutdown)
    signal.signal(signal.SIGINT, worker.request_shutdown)
    worker.run()

