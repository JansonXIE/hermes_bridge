"""The first-version acceptance criteria from the design, each exercised on real processes."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
import unittest
from pathlib import Path

from .harness import ROOT, BridgeCase, git, http


def alive(pid: int) -> bool:
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()[0] != "Z"
    except OSError:
        return False


class ConcurrencyAndIsolation(BridgeCase):
    def test_five_tasks_run_at_most_three_at_once_in_isolated_worktrees(self):
        ids = [self.submit(f"SLEEP=1\nWRITE=out-{i}.txt") for i in range(5)]

        peak = 0
        deadline = time.time() + 30
        while time.time() < deadline:
            status, listing = http("GET", f"{self.api}/v1/tasks", headers=self.headers())
            states = [t["status"] for t in listing["tasks"]]
            peak = max(peak, sum(s in ("starting", "running") for s in states))
            if all(s == "succeeded" for s in states):
                break
            time.sleep(0.03)
        self.assertEqual(peak, 3, "exactly three should have run together")
        self.assertTrue(all(s == "succeeded" for s in states), states)

        tasks = [self.task(i) for i in ids]
        workdirs = {t["artifacts"]["workdir"] for t in tasks}
        branches = {t["artifacts"]["branch"] for t in tasks}
        self.assertEqual(len(workdirs), 5)
        self.assertEqual(len(branches), 5)
        for index, task in enumerate(tasks):
            self.assertEqual(task["changes"]["files"], [f"out-{index}.txt"])
            self.assertEqual(sorted(os.listdir(task["artifacts"]["workdir"])), [".git", "README", f"out-{index}.txt"])
            self.assertEqual(len(self.runs_of(task["task_id"])), 1)
        self.assertEqual(sorted(os.listdir(self.repo)), [".git", "README"], "the source checkout stays untouched")

        # Every concurrently running task gets its own ports and scratch directory.
        runs = self.ledger_entries()
        self.assertEqual(len({r["tmpdir"] for r in runs}), 5)
        self.assertTrue(all(r["port_base"] for r in runs))
        self.assertEqual(sum(1 for r in runs if r["leaked"]), 0, "child environment must be allowlisted")


class ProgressAndResume(BridgeCase):
    def test_progress_is_queryable_and_sse_resumes_with_last_event_id(self):
        task_id = self.submit("SECRET=sk-abcdefghijklmnopqrstuvwx\nWRITE=a.txt\nSLEEP=1.5")
        self.wait_status(task_id, "running")

        first, ended = self.read_sse(task_id, stop_after=2)
        self.assertFalse(ended)
        self.assertEqual([e["id"] for e in first], [1, 2])

        rest, ended = self.read_sse(task_id, last_event_id=first[-1]["id"])
        self.assertTrue(ended, "the stream closes after the terminal state")
        seqs = [e["id"] for e in rest]
        self.assertEqual(seqs, list(range(3, 3 + len(seqs))), "resume has no gap and no repeat")

        types = {e["event"] for e in rest}
        self.assertLessEqual({"claude.init", "tool.use", "tool.result", "claude.result", "state.changed"}, types)
        self.assertEqual(rest[-1]["data"]["data"]["status"], "succeeded")

        task = self.task(task_id)
        self.assertEqual(task["status"], "succeeded")
        self.assertTrue(task["last_action"])
        self.assertTrue(task["claude_session_id"])

        everything = str(first) + str(rest) + str(task)
        self.assertNotIn("PRIVATE-THOUGHT", everything, "thinking is never recorded")
        self.assertNotIn("sk-abcdefghijklmnopqrstuvwx", everything, "credentials are scrubbed")
        self.assertEqual(self.task(task_id)["result"]["summary"], "done ***")

        # Offline clients can also page through events as JSON.
        status, page = http("GET", f"{self.api}/v1/tasks/{task_id}/events?format=json&after=1&limit=2", headers=self.headers())
        self.assertEqual(status, 200)
        self.assertEqual([e["seq"] for e in page["events"]], [2, 3])

    def test_expired_cursor_asks_for_a_snapshot(self):
        task_id = self.submit("WRITE=a.txt")
        self.wait_status(task_id, "succeeded")
        from hermes_bridge.store import Store
        store = Store(self.tmp / "data" / "bridge.db")
        store.conn.execute("DELETE FROM events WHERE task_id=? AND seq<=3", (task_id,))
        status, reply = http("GET", f"{self.api}/v1/tasks/{task_id}/events?format=json&after=1", headers=self.headers())
        self.assertEqual(status, 410, reply)
        self.assertEqual(reply["error"]["code"], "cursor_expired")
        status, _ = http("GET", f"{self.api}/v1/tasks/{task_id}/events?format=json&after=3", headers=self.headers())
        self.assertEqual(status, 200)


class Cancellation(BridgeCase):
    def test_cancel_stops_the_process_tree_before_reporting_cancelled(self):
        task_id = self.submit("CHILD\nSLEEP=120")
        self.wait_status(task_id, "running")
        child = self.wait_until(lambda: [e for e in self.ledger_entries("child") if e["task"] == task_id], message="child")[0]
        main = self.runs_of(task_id)[0]
        self.assertTrue(alive(child["pid"]))

        status, reply = http("POST", f"{self.api}/v1/tasks/{task_id}/cancel", {}, self.headers())
        self.assertEqual(status, 202, reply)
        self.assertEqual(reply["status"], "cancelling")

        self.wait_status(task_id, "cancelled")
        self.assertFalse(alive(main["pid"]), "claude is gone once cancelled is reported")
        self.assertFalse(alive(child["pid"]), "grandchild is gone once cancelled is reported")

        # Idempotent.
        status, reply = http("POST", f"{self.api}/v1/tasks/{task_id}/cancel", {}, self.headers())
        self.assertEqual((status, reply["status"]), (200, "cancelled"))

    def test_process_ignoring_sigterm_is_killed(self):
        task_id = self.submit("IGNORE_TERM\nSLEEP=120")
        self.wait_status(task_id, "running")
        pid = self.wait_until(lambda: self.runs_of(task_id), message="start")[0]["pid"]
        time.sleep(0.3)
        http("POST", f"{self.api}/v1/tasks/{task_id}/cancel", {}, self.headers())
        self.assertEqual(self.task(task_id)["status"], "cancelling")
        self.wait_status(task_id, "cancelled")
        self.assertFalse(alive(pid))

    def test_cancel_queued_task_never_starts_it(self):
        blockers = [self.submit("SLEEP=120") for _ in range(3)]
        for blocker in blockers:
            self.wait_status(blocker, "running")
        queued = self.submit("WRITE=never.txt")
        self.assertEqual(self.task(queued)["status"], "queued")
        status, reply = http("POST", f"{self.api}/v1/tasks/{queued}/cancel", {}, self.headers())
        self.assertEqual((status, reply["status"]), (200, "cancelled"))
        for blocker in blockers:
            http("POST", f"{self.api}/v1/tasks/{blocker}/cancel", {}, self.headers())
        for blocker in blockers:
            self.wait_status(blocker, "cancelled")
        self.assertEqual(self.runs_of(queued), [])

    def test_timeout_stops_the_process_as_timed_out(self):
        task_id = self.submit("CHILD\nSLEEP=120", timeout_seconds=1)
        final = self.wait_status(task_id, "timed_out")
        self.assertEqual(final["error"]["code"], "timeout")
        self.assertFalse(alive(self.runs_of(task_id)[0]["pid"]))


class ReliableNotification(BridgeCase):
    start_adapter = False

    def test_callback_failures_retry_and_wake_only_once(self):
        task_id = self.submit("WRITE=a.txt")
        self.wait_status(task_id, "succeeded")

        from hermes_bridge.store import Store
        store = Store(self.tmp / "data" / "bridge.db")
        row = self.wait_until(
            lambda: (r := store.conn.execute("SELECT * FROM outbox WHERE task_id=?", (task_id,)).fetchone()) and r["attempts"] >= 2 and r,
            message="delivery retries while the adapter is down",
        )
        self.assertEqual(row["state"], "pending")
        self.assertTrue(row["last_error"])

        self.start("adapter")
        self.wait_port(self.adapter_port)
        self.wait_until(
            lambda: store.conn.execute("SELECT state FROM outbox WHERE task_id=?", (task_id,)).fetchone()["state"] == "delivered",
            message="delivery after adapter recovery",
        )

        # Simulate a lost HTTP response: the dispatcher sends the same event again.
        store.conn.execute("UPDATE outbox SET state='pending', next_attempt_at=0 WHERE task_id=?", (task_id,))
        self.wait_until(
            lambda: store.conn.execute("SELECT state FROM outbox WHERE task_id=?", (task_id,)).fetchone()["state"] == "delivered",
            message="duplicate delivery",
        )

        delivery = self.wait_until(self.claim, message="wake-up to claim")
        self.assertEqual([e["task_id"] for e in delivery["events"]], [task_id])
        self.assertIn(task_id, delivery["text"])
        self.assertEqual(self.ack(delivery["delivery_id"])[0], 200)
        time.sleep(0.5)
        self.assertIsNone(self.claim(), "the duplicate must not wake the session again")


class SessionQueue(BridgeCase):
    def test_same_session_is_serial_and_bursts_coalesce(self):
        ids = [self.submit("WRITE=a.txt") for _ in range(3)]
        for task_id in ids:
            self.wait_status(task_id, "succeeded")
        first = self.wait_until(self.claim, message="first batch")
        claimed = [e["task_id"] for e in first["events"]]
        self.assertGreaterEqual(len(claimed), 1)

        # While a delivery is in flight, nothing else is released for that session.
        later = [self.submit("WRITE=b.txt")]
        self.wait_status(later[0], "succeeded")
        time.sleep(0.6)
        self.assertIsNone(self.claim(), "only one delivery in flight per session")
        self.ack(first["delivery_id"])

        seen = list(claimed)
        while len(seen) < 4:
            batch = self.wait_until(self.claim, message="next batch")
            seen += [e["task_id"] for e in batch["events"]]
            self.ack(batch["delivery_id"])
        self.assertEqual(sorted(seen), sorted(ids + later))

    def test_failed_and_abandoned_deliveries_come_back(self):
        task_id = self.submit("WRITE=a.txt")
        self.wait_status(task_id, "succeeded")
        first = self.wait_until(self.claim, message="delivery")
        self.ack(first["delivery_id"], ok=False)
        second = self.wait_until(self.claim, message="redelivery after nack")
        self.assertEqual(second["events"][0]["event_id"], first["events"][0]["event_id"])
        # Consumer vanishes without acking: the lease expires and the event returns.
        third = self.wait_until(self.claim, timeout=10, message="redelivery after lease expiry")
        self.assertEqual(third["events"][0]["event_id"], first["events"][0]["event_id"])
        self.ack(third["delivery_id"])
        self.assertEqual(self.ack(second["delivery_id"])[0], 409, "a stale lease cannot ack")

    def test_sessions_are_independent(self):
        task_a = self.submit("WRITE=a.txt", hermes_session_id="hs_a")
        task_b = self.submit("WRITE=a.txt", hermes_session_id="hs_b")
        self.wait_status(task_a, "succeeded")
        self.wait_status(task_b, "succeeded")
        batch_a = self.wait_until(lambda: self.claim(["hs_a"]), message="session a")
        batch_b = self.wait_until(lambda: self.claim(["hs_b"]), message="session b, while a is in flight")
        self.assertEqual(batch_a["events"][0]["task_id"], task_a)
        self.assertEqual(batch_b["events"][0]["task_id"], task_b)
        self.assertIsNone(self.claim(["hs_unrelated"]))

    def test_gateway_consumer_claims_only_keyed_sessions(self):
        keyed = self.submit("WRITE=a.txt", hermes_session_id="hs_gw", hermes_session_key="agent:main:qq:dm:1")
        plain = self.submit("WRITE=a.txt", hermes_session_id="hs_cli")
        self.wait_status(keyed, "succeeded")
        self.wait_status(plain, "succeeded")
        batch = self.wait_until(lambda: self.claim([], any_session_key=True), message="keyed batch")
        self.assertEqual(batch["hermes_session_key"], "agent:main:qq:dm:1")
        self.assertEqual(batch["events"][0]["task_id"], keyed)
        self.ack(batch["delivery_id"])
        time.sleep(0.5)
        self.assertIsNone(self.claim([], any_session_key=True))


class RestartRecovery(BridgeCase):
    lease_seconds = 1.0

    def test_kill_everything_keeps_tasks_and_does_not_rerun(self):
        running = self.submit("WRITE=adopted.txt\nSLEEP=4")
        fillers = [self.submit("SLEEP=4") for _ in range(2)]
        queued = self.submit("WRITE=later.txt")
        self.wait_status(running, "running")
        for filler in fillers:
            self.wait_status(filler, "running")
        self.assertEqual(self.task(queued)["status"], "queued")
        before_seq = self.task(running)["last_seq"]

        self.kill("api")
        self.kill("worker")
        pid = self.runs_of(running)[0]["pid"]
        self.assertTrue(alive(pid), "claude survives a worker crash")
        time.sleep(1.5)  # let the lease expire
        self.start("api")
        self.wait_port(self.api_port)
        self.assertEqual(self.task(queued)["status"], "queued", "submitted work survives the restart")
        self.start("worker")

        final = self.wait_status(running, "succeeded", timeout=30)
        self.assertEqual(final["attempt_count"], 1)
        self.assertEqual(len(self.runs_of(running)), 1, "the live process is adopted, not re-run")
        self.assertEqual(final["changes"]["files"], ["adopted.txt"])
        for filler in fillers:
            self.wait_status(filler, "succeeded", timeout=30)
            self.assertEqual(len(self.runs_of(filler)), 1)
        self.wait_status(queued, "succeeded", timeout=30)
        self.assertEqual(len(self.runs_of(queued)), 1)

        # The adopted task's event log has no gap or repeat across the crash.
        events, _ = self.read_sse(running)
        seqs = [e["id"] for e in events]
        self.assertEqual(seqs, list(range(1, len(seqs) + 1)))
        self.assertGreater(len(seqs), before_seq)
        statuses = [e["data"]["data"].get("status") for e in events if e["event"] == "state.changed"]
        self.assertIn("recovering", statuses)
        self.assertEqual(sum(e["event"] == "claude.result" for e in events), 1)

    def test_graceful_worker_restart_hands_over_immediately(self):
        task_id = self.submit("SLEEP=3")
        self.wait_status(task_id, "running")
        self.kill("worker", signal.SIGTERM)
        self.assertTrue(alive(self.runs_of(task_id)[0]["pid"]), "graceful shutdown leaves claude running")
        self.start("worker")
        final = self.wait_status(task_id, "succeeded")
        self.assertEqual(len(self.runs_of(task_id)), 1)
        self.assertEqual(final["attempt_count"], 1)

    def test_process_lost_without_result_is_not_rerun(self):
        task_id = self.submit("WRITE=side-effect.txt\nSLEEP=120")
        self.wait_status(task_id, "running")
        self.wait_until(lambda: self.runs_of(task_id), message="start")
        self.kill("worker")
        os.kill(self.runs_of(task_id)[0]["pid"], signal.SIGKILL)
        time.sleep(1.5)
        self.start("worker")
        final = self.wait_status(task_id, "failed")
        self.assertEqual(final["error"]["code"], "process_lost")
        self.assertEqual(len(self.runs_of(task_id)), 1, "side effects unknown: never re-run automatically")
        self.assertEqual(final["changes"]["files"], ["side-effect.txt"])

    def test_process_lost_is_retried_when_declared_safe(self):
        task_id = self.submit("SLEEP=2\nONCE=1", safe_to_retry=True)
        self.wait_status(task_id, "running")
        self.wait_until(lambda: self.runs_of(task_id), message="start")
        self.kill("worker")
        os.kill(self.runs_of(task_id)[0]["pid"], signal.SIGKILL)
        time.sleep(1.5)
        self.start("worker")
        final = self.wait_status(task_id, "succeeded", timeout=30)
        self.assertEqual(final["attempt_count"], 2)
        self.assertEqual(len(self.runs_of(task_id)), 2)

    def test_finished_while_worker_was_down_uses_saved_output(self):
        task_id = self.submit("WRITE=a.txt\nSLEEP=1")
        self.wait_status(task_id, "running")
        self.wait_until(lambda: self.runs_of(task_id), message="start")
        self.kill("worker")
        pid = self.runs_of(task_id)[0]["pid"]
        self.wait_until(lambda: not alive(pid), message="claude finishing on its own")
        time.sleep(1.2)
        self.start("worker")
        final = self.wait_status(task_id, "succeeded")
        self.assertEqual(final["result"]["summary"].strip(), "done")
        self.assertEqual(len(self.runs_of(task_id)), 1)

    def test_old_worker_is_fenced_after_takeover(self):
        from hermes_bridge.store import LeaseLost, Store
        store = Store(self.tmp / "data" / "bridge.db")
        task_id = self.submit("SLEEP=120")
        self.wait_status(task_id, "running")
        attempt = store.get_attempt(store.get_task(task_id)["current_attempt"])
        self.kill("worker")
        store.conn.execute("UPDATE attempts SET lease_expires_at=0 WHERE id=?", (attempt["id"],))
        fresh = store.get_attempt(attempt["id"])
        taken = store.take_over(fresh, "new-worker", 60)
        self.assertIsNotNone(taken)
        with self.assertRaises(LeaseLost):
            store.finish_attempt(attempt["id"], fresh["lease_token"], task_id, "succeeded")
        self.assertEqual(store.get_task(task_id)["status"], "recovering", "the stale write changed nothing")
        self.assertIsNone(store.take_over(fresh, "third-worker", 60), "only one taker wins")


class Integration(BridgeCase):
    verify_command = ["sh", "-c", "test -f marker-ok && echo verified"]

    def test_dependencies_merge_and_retest(self):
        # Acceptance needs marker-ok, which each feature task creates on its own branch.
        main_before = git(self.repo, "rev-parse", "main")
        fix = self.submit("WRITE=marker-ok\nSLEEP=0.3")
        tests = self.submit("WRITE=marker-ok\nSLEEP=0.3", )
        fix_task = self.wait_status(fix, "succeeded")
        self.assertEqual(fix_task["acceptance"]["status"], "passed")
        self.assertIn("verified", fix_task["acceptance"]["output_tail"])
        self.wait_status(tests, "succeeded")

        downstream = self.submit("WRITE=later.txt", depends_on=[fix])
        final = self.wait_status(downstream, "succeeded")
        self.assertIn("marker-ok", os.listdir(final["artifacts"]["workdir"]), "dependency branch merged in")

        integration = self.submit(
            f"MERGE=bridge/{downstream}\nfinish integration", kind="integration", depends_on=[downstream],
        )
        merged = self.wait_status(integration, "succeeded")
        run = self.runs_of(integration)[0]
        self.assertIn(f"- bridge/{downstream}", run["prompt"], "the prompt lists the branches to integrate")
        self.assertEqual(merged["acceptance"]["status"], "passed", "tests re-run on the integrated tree")
        self.assertEqual(git(self.repo, "rev-parse", "main"), main_before, "main is not touched automatically")
        self.assertEqual(sorted(os.listdir(self.repo)), [".git", "README"])
        self.assertIn("marker-ok", os.listdir(merged["artifacts"]["workdir"]))

    def test_failed_acceptance_is_reported_and_blocks_dependents(self):
        no_marker = self.submit("WRITE=other.txt")
        final = self.wait_status(no_marker, "succeeded")
        self.assertEqual(final["acceptance"]["status"], "failed", "CLI success is not acceptance")
        self.assertNotEqual(final["acceptance"]["exit_code"], 0)
        downstream = self.submit("WRITE=x.txt", depends_on=[no_marker])
        blocked = self.wait_status(downstream, "failed")
        self.assertEqual(blocked["error"]["code"], "dependency_failed")
        self.assertEqual(self.runs_of(downstream), [])

    def test_integration_tasks_run_serially_per_project(self):
        first = self.submit("SLEEP=1\nWRITE=marker-ok", kind="integration")
        second = self.submit("SLEEP=1\nWRITE=marker-ok", kind="integration")
        self.wait_status(first, "running")
        time.sleep(0.5)
        self.assertEqual(self.task(second)["status"], "queued")
        self.wait_status(first, "succeeded")
        self.wait_status(second, "succeeded")
        run_a, run_b = self.runs_of(first)[0], self.runs_of(second)[0]
        self.assertGreaterEqual(run_b["at"], run_a["at"] + 1)


class FailureSemantics(BridgeCase):
    def test_final_state_combines_result_event_and_exit_code(self):
        cases = {
            "RESULT_ERROR": "error_during_execution",
            "RESULT_ERROR\nEXIT=1": "error_during_execution",
            "NO_RESULT": "no_result",
            "NO_RESULT\nEXIT=3": "no_result",
            "EXIT=1": "success",  # clean result event but a failing exit code
        }
        ids = {prompt: self.submit(prompt, project="scratch") for prompt in cases}
        for prompt, code in cases.items():
            final = self.wait_status(ids[prompt], "failed")
            self.assertEqual(final["error"]["code"], code, prompt)

    def test_upstream_failure_cascades_without_running_dependents(self):
        root = self.submit("NO_RESULT", project="scratch")
        middle = self.submit("WRITE=a", project="scratch", depends_on=[root])
        leaf = self.submit("WRITE=a", project="scratch", depends_on=[middle])
        self.wait_status(root, "failed")
        self.assertEqual(self.wait_status(middle, "failed")["error"]["code"], "dependency_failed")
        self.assertEqual(self.wait_status(leaf, "failed")["error"]["code"], "dependency_failed")
        self.assertEqual(self.runs_of(middle) + self.runs_of(leaf), [])

    def test_large_prompt_goes_through_stdin(self):
        prompt = "WRITE=a\n" + "x" * 150_000
        task_id = self.submit(prompt, project="scratch")
        self.wait_status(task_id, "succeeded")
        run = self.runs_of(task_id)[0]
        self.assertEqual(run["prompt"], prompt)
        self.assertNotIn("x" * 100, " ".join(run["argv"]))


class RepositoryHooks(BridgeCase):
    def test_rejecting_hook_is_reported_and_not_bypassed(self):
        hooks = self.tmp / "hooks"
        hooks.mkdir()
        hook = hooks / "commit-msg"
        hook.write_text("#!/bin/sh\necho rejected-by-policy\nexit 1\n")
        hook.chmod(0o755)
        git(self.repo, "config", "core.hooksPath", str(hooks))

        task_id = self.submit("WRITE=a.txt")
        final = self.wait_status(task_id, "succeeded")
        changes = final["changes"]
        self.assertIn("rejected-by-policy", changes["snapshot_error"])
        self.assertTrue(changes["uncommitted"], "a rejected snapshot stays visible as uncommitted work")
        self.assertIn("a.txt", changes["uncommitted_files"])
        self.assertEqual(changes["commits"], 0, "no commit may slip past the hook")


class Housekeeping(BridgeCase):
    def test_gc_keeps_branch_and_record_and_refuses_dirty_or_needed_worktrees(self):
        done = self.submit("WRITE=a.txt")
        done_task = self.wait_status(done, "succeeded")
        workdir = done_task["artifacts"]["workdir"]

        dirty = self.submit("WRITE=b.txt")
        dirty_task = self.wait_status(dirty, "succeeded")
        (Path(dirty_task["artifacts"]["workdir"]) / "unsaved.txt").write_text("x")

        blocker = self.submit("SLEEP=30")
        follow_up_source = self.submit("WRITE=c.txt")
        source_task = self.wait_status(follow_up_source, "succeeded")
        self.submit("WRITE=d.txt", continue_task_id=follow_up_source, depends_on=[blocker])

        def gc(*args):
            return subprocess.run(
                [sys.executable, "-m", "hermes_bridge", "-c", str(self.config_path), "gc", "--days", "0", *args],
                cwd=ROOT, capture_output=True, text=True,
            )

        preview = gc("--dry-run")
        self.assertIn(workdir, preview.stdout)
        self.assertTrue(os.path.isdir(workdir), "dry run removes nothing")

        result = gc()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(os.path.exists(workdir))
        self.assertEqual(self.task(done)["status"], "succeeded", "the record stays")
        self.assertTrue(git(self.repo, "rev-parse", f"bridge/{done}"), "the branch stays")
        self.assertTrue(os.path.isdir(dirty_task["artifacts"]["workdir"]), "uncommitted work is not deleted")
        self.assertIn("skipped", result.stderr)
        self.assertTrue(os.path.isdir(source_task["artifacts"]["workdir"]), "a queued follow-up still needs it")

        follow_up = self.submit("WRITE=e.txt", continue_task_id=done)
        self.assertEqual(self.wait_status(follow_up, "failed")["error"]["code"], "continue_unavailable")
        http("POST", f"{self.api}/v1/tasks/{blocker}/cancel", {}, self.headers())


class LargeOutput(BridgeCase):
    def test_stream_line_larger_than_the_read_chunk(self):
        task_id = self.submit("BIGLINE=3000000\nWRITE=a", project="scratch")
        final = self.wait_status(task_id, "succeeded")
        self.assertEqual(final["result"]["summary"].strip(), "done")
        status, page = http("GET", f"{self.api}/v1/tasks/{task_id}/events?format=json&limit=100", headers=self.headers())
        message = [e for e in page["events"] if e["type"] == "assistant.message" and "more chars" in e["data"]["text"]]
        self.assertEqual(len(message), 1, "stored clipped, not whole")
        self.assertLess(len(message[0]["data"]["text"]), 2000)


class InPlace(BridgeCase):
    def _config(self):
        config = super()._config().replace('projects = ["demo", "scratch"]', 'projects = ["demo", "scratch", "live"]')
        return config + f"""
[projects.live]
path = "{self.repo}"
isolation = "inplace"
allowed_tools = ["Read", "Glob", "Grep"]
[projects.live.env]
FAKE_CLAUDE_LEDGER = "{self.ledger}"
"""

    def test_runs_read_only_in_the_live_checkout_and_gc_leaves_it(self):
        first = self.submit("hello", project="live")
        second = self.submit("hello", project="live")
        for task_id in (first, second):
            final = self.wait_status(task_id, "succeeded")
            self.assertEqual(final["artifacts"]["workdir"], str(self.repo))
            self.assertIsNone(final["artifacts"]["branch"])
            self.assertIsNone(final["changes"])
            run = self.runs_of(task_id)[0]
            self.assertEqual(run["cwd"], str(self.repo))
            denied = run["argv"][run["argv"].index("--disallowedTools") + 1].split(",")
            self.assertEqual(sorted(denied), ["Bash", "Edit", "NotebookEdit", "Write"])
        self.assertEqual(git(self.repo, "worktree", "list").count("\n"), 0, "no worktree was made")

        follow_up = self.submit("hello", continue_task_id=first, project="live")
        self.assertEqual(self.wait_status(follow_up, "succeeded")["artifacts"]["workdir"], str(self.repo))

        result = subprocess.run(
            [sys.executable, "-m", "hermes_bridge", "-c", str(self.config_path), "gc", "--days", "0"],
            cwd=ROOT, capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn(str(self.repo), result.stdout)
        self.assertTrue((self.repo / "README").is_file(), "the live checkout is never collected")

    def test_config_refuses_anything_that_could_write(self):
        from hermes_bridge.config import ConfigError, load_config
        base = self.config_path.read_text()
        for bad in (
            'allowed_tools = ["Read", "Edit"]',
            'allowed_tools = ["Read", "Bash(ls *)"]',
            'allowed_tools = ["Read"]\npermission_mode = "acceptEdits"',
            'allowed_tools = ["Read"]\nverify_command = ["make"]',
        ):
            path = self.tmp / "bad.toml"
            path.write_text(base.replace('allowed_tools = ["Read", "Glob", "Grep"]', bad))
            with self.subTest(bad=bad), self.assertRaises(ConfigError):
                load_config(path)


class WritableInPlace(BridgeCase):
    def _config(self):
        config = super()._config().replace('projects = ["demo", "scratch"]', 'projects = ["demo", "scratch", "sdk"]')
        return config + f"""
[projects.sdk]
path = "{self.tmp / 'sdk'}"
isolation = "inplace"
allow_inplace_writes = true
permission_mode = "bypassPermissions"
allow_bypass_permissions = true
[projects.sdk.env]
FAKE_CLAUDE_LEDGER = "{self.ledger}"
"""

    def setUp(self):
        super().setUp()
        (self.tmp / "sdk").mkdir()

    def test_writes_in_the_live_directory_one_task_at_a_time(self):
        live = self.tmp / "sdk"
        ids = [self.submit(f"SLEEP=1\nWRITE=out-{i}.txt", project="sdk") for i in range(2)]
        other = self.submit("SLEEP=1", project="scratch")

        peak = 0
        deadline = time.time() + 30
        while time.time() < deadline:
            states = [self.task(i)["status"] for i in ids]
            peak = max(peak, sum(s in ("starting", "running") for s in states))
            if all(s == "succeeded" for s in states):
                break
            time.sleep(0.03)
        self.assertEqual(peak, 1, "tasks of a writable inplace project never overlap")
        self.wait_status(other, "succeeded")
        self.assertEqual(sorted(os.listdir(live)), ["out-0.txt", "out-1.txt"])
        for task_id in ids:
            final = self.task(task_id)
            self.assertEqual(final["artifacts"]["workdir"], str(live))
            self.assertIsNone(final["changes"])
            argv = self.runs_of(task_id)[0]["argv"]
            self.assertEqual(argv[argv.index("--permission-mode") + 1], "bypassPermissions")
            self.assertNotIn("--disallowedTools", argv)

    def test_opt_in_is_explicit(self):
        from hermes_bridge.config import ConfigError, load_config
        base = self.config_path.read_text()
        for old, new in (
            ("allow_inplace_writes = true", 'allow_inplace_writes = "yes"'),
            ('isolation = "inplace"\n', 'isolation = "worktree"\n'),
            ("allow_inplace_writes = true", 'allow_inplace_writes = true\nverify_command = ["make"]'),
            ("allow_inplace_writes = true\n", ""),
        ):
            path = self.tmp / "bad.toml"
            path.write_text(base.replace(old, new))
            with self.subTest(new=new), self.assertRaises(ConfigError):
                load_config(path)


class Continuation(BridgeCase):
    def test_follow_up_resumes_the_same_session_and_worktree(self):
        first = self.submit("WRITE=one.txt")
        first_task = self.wait_status(first, "succeeded")
        follow_up = self.submit("WRITE=two.txt", continue_task_id=first)
        second_task = self.wait_status(follow_up, "succeeded")
        self.assertEqual(second_task["artifacts"]["workdir"], first_task["artifacts"]["workdir"])
        self.assertEqual(second_task["claude_session_id"], first_task["claude_session_id"])
        argv = self.runs_of(follow_up)[0]["argv"]
        self.assertEqual(argv[argv.index("--resume") + 1], first_task["claude_session_id"])

    def test_session_never_has_two_writers(self):
        first = self.submit("SLEEP=1")
        follow_a = self.submit("SLEEP=1", continue_task_id=first)
        follow_b = self.submit("SLEEP=1", continue_task_id=first)
        self.wait_status(first, "running")
        self.assertEqual(self.task(follow_a)["status"], "queued", "waits until the source task ended")
        self.wait_status(first, "succeeded")
        self.wait_status(follow_a, "running")
        self.assertEqual(self.task(follow_b)["status"], "queued", "a second follow-up waits for the first")
        self.wait_status(follow_a, "succeeded")
        self.wait_status(follow_b, "succeeded")


class ApiContract(BridgeCase):
    def test_authentication_is_required_everywhere(self):
        task_id = self.submit("WRITE=a", project="scratch")
        for method, path in (
            ("POST", "/v1/tasks"), ("GET", "/v1/tasks"), ("GET", f"/v1/tasks/{task_id}"),
            ("GET", f"/v1/tasks/{task_id}/events"), ("POST", f"/v1/tasks/{task_id}/cancel"), ("GET", "/v1/health"),
        ):
            body = {} if method == "POST" else None
            for headers in ({}, {"Authorization": "Bearer wrong-token-value"}):
                status, _ = http(method, f"{self.api}{path}", body, headers)
                self.assertEqual(status, 401, f"{method} {path}")

    def test_owner_and_project_scoping(self):
        from .harness import OTHER_TOKEN
        task_id = self.submit("WRITE=a")
        self.assertEqual(http("GET", f"{self.api}/v1/tasks/{task_id}", headers=self.headers(OTHER_TOKEN))[0], 404)
        self.assertEqual(http("POST", f"{self.api}/v1/tasks/{task_id}/cancel", {}, self.headers(OTHER_TOKEN))[0], 404)
        self.assertEqual(http("GET", f"{self.api}/v1/tasks/{task_id}/events", headers=self.headers(OTHER_TOKEN))[0], 404)
        _, listing = http("GET", f"{self.api}/v1/tasks", headers=self.headers(OTHER_TOKEN))
        self.assertEqual(listing["tasks"], [])
        reply = self.submit("x", project="scratch", token=OTHER_TOKEN, expect=403)
        self.assertEqual(reply["error"]["code"], "project_forbidden")
        reply = self.submit("x", project="nonexistent", expect=403)
        self.assertEqual(reply["error"]["code"], "project_forbidden")
        reply = self.submit("x", token=OTHER_TOKEN, expect=403)
        self.assertEqual(reply["error"]["code"], "session_forbidden", "hs_test is bound to its first owner")
        reply = self.submit("x", token=OTHER_TOKEN, expect=422, hermes_session_id="hs_other", depends_on=[task_id])
        self.assertEqual(reply["error"]["code"], "invalid_reference")

    def test_idempotency(self):
        task_id = self.submit("SLEEP=0", key="same-key", project="scratch")
        status, reply = http("POST", f"{self.api}/v1/tasks",
                             {"project_id": "scratch", "prompt": "SLEEP=0", "hermes_session_id": "hs_test"},
                             self.headers(key="same-key"))
        self.assertEqual((status, reply["task_id"], reply["idempotent_replay"]), (202, task_id, True))
        status, reply = http("POST", f"{self.api}/v1/tasks",
                             {"project_id": "scratch", "prompt": "changed", "hermes_session_id": "hs_test"},
                             self.headers(key="same-key"))
        self.assertEqual((status, reply["error"]["code"]), (409, "idempotency_conflict"))
        status, reply = http("POST", f"{self.api}/v1/tasks", {"project_id": "scratch", "prompt": "x"}, self.headers())
        self.assertEqual((status, reply["error"]["code"]), (400, "idempotency_key_required"))
        self.wait_status(task_id, "succeeded")
        self.assertEqual(len(self.runs_of(task_id)), 1)

    def test_validation_and_filters(self):
        for fields in ({"timeout_seconds": 0}, {"timeout_seconds": "9"}, {"depends_on": "x"},
                       {"kind": "nope"}, {"effort": "extreme"}, {"max_budget_usd": -1},
                       {"hermes_session_id": "bad id/../"}, {"model": "a b"}):
            self.submit("x", project="scratch", expect=422, **fields)
        self.submit("  ", project="scratch", expect=422)
        self.submit("x", project="scratch", kind="integration", expect=422)
        self.submit("x", project="scratch", depends_on=["task_missing"], expect=422)

        one = self.submit("WRITE=a", project="scratch", hermes_session_id="hs_one")
        two = self.submit("WRITE=a", project="demo", hermes_session_id="hs_two")
        self.wait_status(one, "succeeded")
        self.wait_status(two, "succeeded")

        def ids(query):
            status, reply = http("GET", f"{self.api}/v1/tasks?{query}", headers=self.headers())
            self.assertEqual(status, 200, reply)
            return [t["task_id"] for t in reply["tasks"]], reply["next_cursor"]

        self.assertEqual(ids("hermes_session_id=hs_one")[0], [one])
        self.assertEqual(ids("project_id=demo")[0], [two])
        self.assertEqual(ids("status=failed")[0], [])
        self.assertEqual(ids("status=succeeded")[0], [two, one])
        page, cursor = ids("limit=1")
        self.assertEqual(page, [two])
        self.assertEqual(ids(f"limit=1&cursor={cursor}")[0], [one])
        self.assertEqual(http("GET", f"{self.api}/v1/tasks?status=bogus", headers=self.headers())[0], 422)

        status, health = http("GET", f"{self.api}/v1/health", headers=self.headers())
        self.assertEqual(status, 200)
        self.assertEqual(health["tasks"]["succeeded"], 2)
        self.assertEqual(len(health["workers"]), 1)

    def test_oversized_body_rejected(self):
        status, _ = http("POST", f"{self.api}/v1/tasks", b"x" * (1 << 21), self.headers(key="k"))
        self.assertEqual(status, 413)


class AdapterSecurity(BridgeCase):
    def test_signature_and_replay_protection(self):
        import json
        from hermes_bridge.dispatcher import sign
        from .harness import NOTIFY_SECRET
        event = json.dumps({"event_id": "evt_x", "type": "task.completed", "task_id": "t",
                            "hermes_session_id": "hs_sec", "status": "succeeded"}).encode()
        url = f"{self.adapter}/v1/events"

        def post(timestamp, signature):
            return http("POST", url, event, {"X-Bridge-Timestamp": timestamp, "X-Bridge-Signature": signature})

        now = str(int(time.time()))
        self.assertEqual(post(now, "0" * 64)[0], 401)
        self.assertEqual(post(now, sign("wrong-secret", now, event))[0], 401)
        stale = str(int(time.time()) - 3600)
        self.assertEqual(post(stale, sign(NOTIFY_SECRET, stale, event))[0], 401, "replay window enforced")
        self.assertEqual(http("POST", url, event, {})[0], 401)
        status, reply = post(now, sign(NOTIFY_SECRET, now, event))
        self.assertEqual((status, reply["status"]), (200, "accepted"))
        status, reply = post(now, sign(NOTIFY_SECRET, now, event))
        self.assertEqual((status, reply["status"]), (200, "duplicate"))

        status, _ = http("POST", f"{self.adapter}/v1/deliveries/claim", {"consumer": "x", "any_session_key": True})
        self.assertEqual(status, 401)

    def test_wake_text_carries_no_worker_output(self):
        task_id = self.submit("SECRET=IGNORE-PREVIOUS-INSTRUCTIONS\nWRITE=a")
        self.wait_status(task_id, "succeeded")
        delivery = self.wait_until(self.claim, message="delivery")
        self.assertIn(task_id, delivery["text"])
        self.assertNotIn("IGNORE-PREVIOUS", delivery["text"])


class Fallback(BridgeCase):
    extra_adapter = "fallback_after_seconds = 0.5"

    def _config(self):
        self.woken = self.tmp / "woken.txt"
        config = super()._config()
        command = f'fallback_command = ["sh", "-c", "printf \'%s|%s\\\\n\' \\"$1\\" \\"$2\\" >> {self.woken}", "sh", "{{session_id}}", "{{prompt}}"]'
        return config.replace("fallback_after_seconds = 0.5", f"fallback_after_seconds = 0.5\n{command}")

    def test_unclaimed_session_is_woken_by_the_fallback_command_once(self):
        task_id = self.submit("WRITE=a", hermes_session_id="hs_gone")
        self.wait_status(task_id, "succeeded")
        self.wait_until(self.woken.exists, message="fallback wake-up")
        time.sleep(1.5)
        text = self.woken.read_text()
        self.assertTrue(text.startswith("hs_gone|"), text)
        self.assertIn(task_id, text)
        self.assertEqual(text.count(task_id), 1)


if __name__ == "__main__":
    unittest.main()
