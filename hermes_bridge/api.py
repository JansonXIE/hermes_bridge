"""HTTP API: submit, monitor (JSON and SSE) and cancel tasks.

This is a custom contract, not standard A2A. Every endpoint is authenticated
and scoped to the calling principal; another owner's task answers 404.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import signal
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from .config import Config, Principal
from .httpbase import HttpError, Request, Response, make_server, route
from .store import (
    ACTIVE, CANCELLED, TERMINAL, IdempotencyConflict, InvalidReference, SessionForbidden, Store,
)

MAX_PROMPT_CHARS = 200_000
_ID_RE = re.compile(r"^[\w.:@\-]{1,200}$")
_MODEL_RE = re.compile(r"^[\w.:\[\]\-]{1,100}$")
_EFFORTS = {"low", "medium", "high", "xhigh", "max"}
_ALL_STATUSES = {"queued", *ACTIVE, *TERMINAL}
_SSE_POLL_SECONDS = 0.25
_SSE_KEEPALIVE_SECONDS = 15.0


def iso(ts: Optional[float]) -> Optional[str]:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="seconds") if ts else None


def _loads(value: Optional[str]):
    return json.loads(value) if value else None


def present_task(task: dict, attempt: Optional[dict] = None) -> dict:
    """Public view of a task. Worker output in result/acceptance is untrusted data."""
    now = time.time()
    view = {
        "task_id": task["id"],
        "project_id": task["project_id"],
        "hermes_session_id": task["hermes_session_id"],
        "title": task["title"],
        "kind": task["kind"],
        "status": task["status"],
        "last_action": task["last_action"],
        "last_event_at": iso(task["last_event_at"]),
        "silent_seconds": int(now - task["last_event_at"]) if task["last_event_at"] and task["status"] in ACTIVE else None,
        "last_seq": task["last_seq"],
        "timeout_seconds": task["timeout_seconds"],
        "depends_on": json.loads(task["depends_on"]),
        "continue_task_id": task["continue_task_id"],
        "cancel_requested": bool(task["cancel_requested_at"]),
        "attempt_count": task["attempt_count"],
        "claude_session_id": task["claude_session_id"],
        "created_at": iso(task["created_at"]),
        "started_at": iso(task["started_at"]),
        "finished_at": iso(task["finished_at"]),
        "result": _loads(task["result"]),
        "changes": _loads(task["changes"]),
        "acceptance": _loads(task["acceptance"]),
        "error": {"code": task["error_code"], "message": task["error_message"]} if task["error_code"] else None,
        "artifacts": {"workdir": task["workdir"], "branch": task["branch"], "run_dir": task["run_dir"]},
    }
    if attempt:
        view["attempt"] = {
            "number": attempt["number"],
            "worker_id": attempt["worker_id"],
            "pid": attempt["pid"],
            "exit_code": attempt["exit_code"],
            "exit_signal": attempt["exit_signal"],
            "end_reason": attempt["end_reason"],
        }
    return view


def present_event(row: dict) -> dict:
    return {
        "seq": row["seq"],
        "event_id": row["event_id"],
        "type": row["type"],
        "created_at": iso(row["created_at"]),
        "data": json.loads(row["payload"]),
    }


class Api:
    def __init__(self, config: Config):
        self.config = config
        self.store = Store(config.db_path)

    def routes(self):
        return [
            route("POST", "/v1/tasks", self.submit),
            route("GET", "/v1/tasks", self.list_tasks),
            route("GET", "/v1/tasks/{id}", self.get_task),
            route("GET", "/v1/tasks/{id}/events", self.events),
            route("POST", "/v1/tasks/{id}/cancel", self.cancel),
            route("GET", "/v1/health", self.health),
        ]

    def _auth(self, request: Request) -> Principal:
        header = request.headers.get("Authorization", "")
        token = header[7:] if header.startswith("Bearer ") else ""
        found = None
        for principal in self.config.principals:
            # No early exit, so timing does not reveal how many tokens were compared.
            if hmac.compare_digest(principal.token.encode(), token.encode()):
                found = principal
        if not found:
            raise HttpError(401, "unauthorized", "missing or invalid bearer token")
        return found

    def _owned_task(self, request: Request, principal: Principal) -> dict:
        task = self.store.get_task(request.params[0], owner=principal.name)
        if not task or not principal.can_access(task["project_id"]):
            raise HttpError(404, "task_not_found", "no such task")
        return task

    def _validated_spec(self, body: dict, principal: Principal) -> dict:
        def fail(field: str, why: str):
            raise HttpError(422, "invalid_request", f"{field}: {why}")

        project_id = body.get("project_id")
        if not isinstance(project_id, str):
            fail("project_id", "required string")
        # Same answer for unknown and forbidden, so project names are not enumerable.
        project = self.config.projects.get(project_id)
        if not project or not principal.can_access(project_id):
            raise HttpError(403, "project_forbidden", "project is not available to this token")

        prompt = body.get("prompt")
        if not isinstance(prompt, str) or not prompt.strip():
            fail("prompt", "required non-empty string")
        if len(prompt) > MAX_PROMPT_CHARS:
            fail("prompt", f"longer than {MAX_PROMPT_CHARS} characters")

        timeout = body.get("timeout_seconds", self.config.default_timeout_seconds)
        if type(timeout) is not int or not 1 <= timeout <= self.config.max_timeout_seconds:
            fail("timeout_seconds", f"integer between 1 and {self.config.max_timeout_seconds}")

        depends_on = body.get("depends_on", [])
        if not isinstance(depends_on, list) or not all(isinstance(d, str) for d in depends_on) or len(depends_on) > 50:
            fail("depends_on", "list of up to 50 task ids")

        session_id = body.get("hermes_session_id")
        if session_id is not None and not (isinstance(session_id, str) and _ID_RE.match(session_id)):
            fail("hermes_session_id", "invalid identifier")
        session_key = body.get("hermes_session_key")
        if session_key is not None and not (isinstance(session_key, str) and 0 < len(session_key) <= 512 and session_key.isprintable()):
            fail("hermes_session_key", "invalid identifier")

        kind = body.get("kind", "task")
        if kind not in ("task", "integration"):
            fail("kind", "must be 'task' or 'integration'")
        if kind == "integration" and project.isolation != "worktree":
            fail("kind", "integration requires a worktree project")

        model = body.get("model")
        if model is not None and not (isinstance(model, str) and _MODEL_RE.match(model)):
            fail("model", "invalid model name")
        effort = body.get("effort")
        if effort is not None and effort not in _EFFORTS:
            fail("effort", f"one of {sorted(_EFFORTS)}")

        budget = body.get("max_budget_usd")
        if budget is not None:
            if isinstance(budget, bool) or not isinstance(budget, (int, float)) or budget <= 0:
                fail("max_budget_usd", "positive number")
            if project.max_budget_usd and budget > project.max_budget_usd:
                fail("max_budget_usd", f"exceeds the project limit of {project.max_budget_usd}")

        title = body.get("title")
        if title is not None and not (isinstance(title, str) and len(title) <= 200):
            fail("title", "string of up to 200 characters")
        continue_task_id = body.get("continue_task_id")
        if continue_task_id is not None and not isinstance(continue_task_id, str):
            fail("continue_task_id", "must be a task id")
        safe_to_retry = body.get("safe_to_retry", False)
        if not isinstance(safe_to_retry, bool):
            fail("safe_to_retry", "must be a boolean")

        return {
            "project_id": project_id, "prompt": prompt, "timeout_seconds": timeout, "depends_on": depends_on,
            "hermes_session_id": session_id, "hermes_session_key": session_key, "kind": kind, "model": model,
            "effort": effort, "max_budget_usd": budget, "title": title, "continue_task_id": continue_task_id,
            "safe_to_retry": safe_to_retry,
        }

    def submit(self, request: Request) -> Response:
        principal = self._auth(request)
        key = request.headers.get("Idempotency-Key", "")
        if not key or len(key) > 200:
            raise HttpError(400, "idempotency_key_required", "Idempotency-Key header is required")
        spec = self._validated_spec(request.json(), principal)
        digest = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
        try:
            task, created = self.store.create_task(principal.name, key, digest, spec)
        except IdempotencyConflict:
            raise HttpError(409, "idempotency_conflict", "this Idempotency-Key was used with a different request")
        except SessionForbidden:
            raise HttpError(403, "session_forbidden", "hermes_session_id belongs to another owner")
        except InvalidReference as exc:
            raise HttpError(422, "invalid_reference", f"task {exc} does not exist in this project")
        return Response(202, {
            "task_id": task["id"],
            "status": task["status"],
            "idempotent_replay": not created,
            "task_ref": f"/v1/tasks/{task['id']}",
            "events_ref": f"/v1/tasks/{task['id']}/events",
        })

    def list_tasks(self, request: Request) -> Response:
        principal = self._auth(request)
        statuses = [s for s in request.query.get("status", "").split(",") if s]
        unknown = set(statuses) - _ALL_STATUSES
        if unknown:
            raise HttpError(422, "invalid_request", f"unknown status: {sorted(unknown)}")
        try:
            limit = min(max(int(request.query.get("limit", 50)), 1), 200)
            cursor = int(request.query["cursor"]) if "cursor" in request.query else None
        except ValueError:
            raise HttpError(422, "invalid_request", "limit and cursor must be integers")
        rows = self.store.list_tasks(
            principal.name, statuses or None, request.query.get("hermes_session_id"),
            request.query.get("project_id"), cursor, limit,
        )
        rows = [r for r in rows if principal.can_access(r["project_id"])]
        return Response(200, {
            "tasks": [present_task(r) for r in rows],
            "next_cursor": rows[-1]["seq"] if len(rows) == limit else None,
        })

    def get_task(self, request: Request) -> Response:
        task = self._owned_task(request, self._auth(request))
        return Response(200, present_task(task, self.store.get_attempt(task["current_attempt"])))

    def cancel(self, request: Request) -> Response:
        task = self._owned_task(request, self._auth(request))
        status = self.store.request_cancel(task["id"])
        settled = status in TERMINAL
        return Response(200 if settled else 202, {
            "task_id": task["id"],
            "status": status,
            "cancelled": status == CANCELLED,
            "note": None if settled else "stop requested; cancelled is set only after the process tree is confirmed gone",
        })

    def health(self, request: Request) -> Response:
        self._auth(request)
        now = time.time()
        workers = self.store.list_workers()
        return Response(200, {
            "status": "ok",
            "max_concurrency": self.config.max_concurrency,
            "tasks": self.store.count_by_status(),
            "outbox": self.store.outbox_counts(),
            "workers": [
                {"id": w["id"], "pid": w["pid"], "heartbeat_age_seconds": round(now - w["heartbeat_at"], 1)}
                for w in workers
            ],
        })

    # ---- event replay and streaming -------------------------------------

    def events(self, request: Request) -> Optional[Response]:
        task = self._owned_task(request, self._auth(request))
        raw_cursor = request.headers.get("Last-Event-ID") or request.query.get("after") or "0"
        try:
            after = int(raw_cursor)
        except ValueError:
            raise HttpError(422, "invalid_request", "event cursor must be an integer")
        if self.store.cursor_expired(task, after):
            raise HttpError(410, "cursor_expired", f"events were pruned; fetch /v1/tasks/{task['id']} for a snapshot")

        if request.query.get("format") == "json" or request.headers.get("Accept", "").startswith("application/json"):
            try:
                limit = min(max(int(request.query.get("limit", 100)), 1), 500)
            except ValueError:
                raise HttpError(422, "invalid_request", "limit must be an integer")
            rows = self.store.events_after(task["id"], after, limit)
            return Response(200, {
                "events": [present_event(r) for r in rows],
                "next_after": rows[-1]["seq"] if rows else after,
                "status": task["status"],
            })
        self._stream(request, task["id"], after)
        return None

    def _stream(self, request: Request, task_id: str, after: int) -> None:
        handler = request.handler
        handler.close_connection = True
        handler.send_response(200)
        handler.send_header("Content-Type", "text/event-stream; charset=utf-8")
        handler.send_header("Cache-Control", "no-store")
        handler.send_header("Connection", "close")
        handler.send_header("X-Accel-Buffering", "no")
        handler.end_headers()
        out = handler.wfile
        out.write(b"retry: 3000\n\n")
        out.flush()
        last_write = time.time()
        while True:
            # Read the status before the events, so an event committed just before a
            # terminal state cannot be skipped by the end-of-stream check below.
            task = self.store.get_task(task_id)
            rows = self.store.events_after(task_id, after)
            for row in rows:
                data = json.dumps(present_event(row), ensure_ascii=False)
                out.write(f"id: {row['seq']}\nevent: {row['type']}\ndata: {data}\n\n".encode())
                after = row["seq"]
            if rows:
                out.flush()
                last_write = time.time()
                continue
            if task["status"] in TERMINAL and after >= task["last_seq"]:
                out.write(f"event: stream.end\ndata: {json.dumps({'status': task['status']})}\n\n".encode())
                out.flush()
                return
            if time.time() - last_write >= _SSE_KEEPALIVE_SECONDS:
                out.write(b": keepalive\n\n")
                out.flush()
                last_write = time.time()
            time.sleep(_SSE_POLL_SECONDS)


def run_api(config: Config) -> None:
    api = Api(config)
    server = make_server(config.api_host, config.api_port, api.routes())

    def stop(*_: object) -> None:
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    server.serve_forever()
