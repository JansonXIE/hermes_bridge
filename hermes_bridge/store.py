"""SQLite-backed task queue, event log and notification outbox.

The database is the only channel between the API and worker processes. Every
write from an attempt is fenced by its lease token, so a worker that lost its
lease cannot overwrite the state of whoever took over.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional

QUEUED = "queued"
STARTING = "starting"
RUNNING = "running"
CANCELLING = "cancelling"
RECOVERING = "recovering"
SUCCEEDED = "succeeded"
FAILED = "failed"
CANCELLED = "cancelled"
TIMED_OUT = "timed_out"

ACTIVE = (STARTING, RUNNING, CANCELLING, RECOVERING)
TERMINAL = (SUCCEEDED, FAILED, CANCELLED, TIMED_OUT)
_ACTIVE_SQL = ",".join(f"'{s}'" for s in ACTIVE)
_TERMINAL_SQL = ",".join(f"'{s}'" for s in TERMINAL)

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    hermes_session_id TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    owner TEXT NOT NULL,
    project_id TEXT NOT NULL,
    hermes_session_id TEXT,
    hermes_session_key TEXT,
    title TEXT,
    prompt TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'task',
    status TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    timeout_seconds INTEGER NOT NULL,
    depends_on TEXT NOT NULL DEFAULT '[]',
    continue_task_id TEXT,
    safe_to_retry INTEGER NOT NULL DEFAULT 0,
    model TEXT,
    effort TEXT,
    max_budget_usd REAL,
    workdir TEXT,
    branch TEXT,
    base_commit TEXT,
    run_dir TEXT,
    slot INTEGER,
    claude_session_id TEXT,
    current_attempt TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    cancel_requested_at REAL,
    last_event_at REAL,
    last_action TEXT,
    last_seq INTEGER NOT NULL DEFAULT 0,
    result TEXT,
    changes TEXT,
    acceptance TEXT,
    error_code TEXT,
    error_message TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    started_at REAL,
    finished_at REAL,
    UNIQUE (owner, idempotency_key)
);
CREATE INDEX IF NOT EXISTS tasks_status ON tasks (status, seq);
CREATE INDEX IF NOT EXISTS tasks_session ON tasks (hermes_session_id, seq);
CREATE INDEX IF NOT EXISTS tasks_project ON tasks (project_id, seq);
CREATE TABLE IF NOT EXISTS attempts (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks (id),
    number INTEGER NOT NULL,
    worker_id TEXT,
    lease_token INTEGER NOT NULL DEFAULT 1,
    lease_expires_at REAL NOT NULL,
    spawn_intent_at REAL,
    pid INTEGER,
    proc_start_time INTEGER,
    boot_id TEXT,
    stdout_path TEXT,
    stderr_path TEXT,
    stdout_offset INTEGER NOT NULL DEFAULT 0,
    stderr_offset INTEGER NOT NULL DEFAULT 0,
    result TEXT,
    exit_code INTEGER,
    exit_signal INTEGER,
    end_reason TEXT,
    started_at REAL NOT NULL,
    ended_at REAL,
    UNIQUE (task_id, number)
);
CREATE TABLE IF NOT EXISTS events (
    task_id TEXT NOT NULL REFERENCES tasks (id),
    seq INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    type TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (task_id, seq)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS outbox (
    event_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    type TEXT NOT NULL,
    payload TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL,
    lease_until REAL NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at REAL NOT NULL,
    delivered_at REAL
);
CREATE INDEX IF NOT EXISTS outbox_due ON outbox (state, next_attempt_at);
CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY,
    pid INTEGER NOT NULL,
    started_at REAL NOT NULL,
    heartbeat_at REAL NOT NULL
);
"""


class LeaseLost(Exception):
    """This worker no longer owns the attempt; another worker has taken over."""


class IdempotencyConflict(Exception):
    pass


class SessionForbidden(Exception):
    pass


class InvalidReference(Exception):
    pass


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:20]}"


def open_connection(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    # Changing the journal mode ignores the busy timeout when several processes start together.
    deadline = time.time() + 30
    while True:
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            break
        except sqlite3.OperationalError:
            if time.time() >= deadline:
                raise
            time.sleep(0.05)
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _row(row: Optional[sqlite3.Row]) -> Optional[dict]:
    return dict(row) if row is not None else None


class Store:
    def __init__(self, path: str | Path):
        self.path = str(path)
        self._local = threading.local()
        Path(self.path).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.conn.executescript(SCHEMA)

    @property
    def conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = open_connection(self.path)
            self._local.conn = conn
        return conn

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")

    # ---- task submission and queries ------------------------------------

    def create_task(self, owner: str, idempotency_key: str, request_hash: str, spec: dict) -> tuple[dict, bool]:
        """Insert a queued task. Returns (task, created); a replayed key returns the original."""
        now = time.time()
        with self.tx() as c:
            existing = c.execute(
                "SELECT * FROM tasks WHERE owner=? AND idempotency_key=?", (owner, idempotency_key)
            ).fetchone()
            if existing:
                if existing["request_hash"] != request_hash:
                    raise IdempotencyConflict(idempotency_key)
                return dict(existing), False

            session_id = spec.get("hermes_session_id")
            if session_id:
                bound = c.execute(
                    "SELECT owner FROM sessions WHERE hermes_session_id=?", (session_id,)
                ).fetchone()
                if bound and bound["owner"] != owner:
                    raise SessionForbidden(session_id)
                if not bound:
                    c.execute("INSERT INTO sessions VALUES (?,?,?)", (session_id, owner, now))

            references = list(spec.get("depends_on", []))
            if spec.get("continue_task_id"):
                references.append(spec["continue_task_id"])
            for ref in references:
                row = c.execute("SELECT owner, project_id FROM tasks WHERE id=?", (ref,)).fetchone()
                if not row or row["owner"] != owner or row["project_id"] != spec["project_id"]:
                    raise InvalidReference(ref)

            task_id = new_id("task")
            c.execute(
                """INSERT INTO tasks (id, owner, project_id, hermes_session_id, hermes_session_key, title,
                       prompt, kind, status, idempotency_key, request_hash, timeout_seconds, depends_on,
                       continue_task_id, safe_to_retry, model, effort, max_budget_usd, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    task_id, owner, spec["project_id"], session_id, spec.get("hermes_session_key"),
                    spec.get("title"), spec["prompt"], spec.get("kind", "task"), QUEUED, idempotency_key,
                    request_hash, spec["timeout_seconds"], json.dumps(spec.get("depends_on", [])),
                    spec.get("continue_task_id"), int(bool(spec.get("safe_to_retry"))), spec.get("model"),
                    spec.get("effort"), spec.get("max_budget_usd"), now, now,
                ),
            )
            self._add_event(c, task_id, "state.changed", {"status": QUEUED})
            return dict(c.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()), True

    def get_task(self, task_id: str, owner: Optional[str] = None) -> Optional[dict]:
        if owner is None:
            return _row(self.conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone())
        return _row(self.conn.execute("SELECT * FROM tasks WHERE id=? AND owner=?", (task_id, owner)).fetchone())

    def get_attempt(self, attempt_id: Optional[str]) -> Optional[dict]:
        return _row(self.conn.execute("SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone())

    def list_tasks(
        self,
        owner: str,
        statuses: Optional[list[str]] = None,
        hermes_session_id: Optional[str] = None,
        project_id: Optional[str] = None,
        cursor: Optional[int] = None,
        limit: int = 50,
    ) -> list[dict]:
        where, params = ["owner=?"], [owner]
        if statuses:
            where.append(f"status IN ({','.join('?' * len(statuses))})")
            params += statuses
        if hermes_session_id:
            where.append("hermes_session_id=?")
            params.append(hermes_session_id)
        if project_id:
            where.append("project_id=?")
            params.append(project_id)
        if cursor is not None:
            where.append("seq<?")
            params.append(cursor)
        sql = f"SELECT * FROM tasks WHERE {' AND '.join(where)} ORDER BY seq DESC LIMIT ?"
        return [dict(r) for r in self.conn.execute(sql, [*params, limit])]

    def count_by_status(self) -> dict[str, int]:
        return {r["status"]: r["n"] for r in self.conn.execute("SELECT status, count(*) n FROM tasks GROUP BY status")}

    # ---- events ---------------------------------------------------------

    def _add_event(self, c: sqlite3.Connection, task_id: str, event_type: str, payload: dict) -> int:
        now = time.time()
        c.execute("UPDATE tasks SET last_seq=last_seq+1, updated_at=? WHERE id=?", (now, task_id))
        seq = c.execute("SELECT last_seq FROM tasks WHERE id=?", (task_id,)).fetchone()[0]
        c.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            (task_id, seq, new_id("evt"), event_type, json.dumps(payload, ensure_ascii=False), now),
        )
        return seq

    def events_after(self, task_id: str, after: int, limit: int = 200) -> list[dict]:
        return [
            dict(r)
            for r in self.conn.execute(
                "SELECT * FROM events WHERE task_id=? AND seq>? ORDER BY seq LIMIT ?", (task_id, after, limit)
            )
        ]

    def cursor_expired(self, task: dict, after: int) -> bool:
        """True when events newer than `after` were pruned, so a replay would have a gap."""
        if after >= task["last_seq"]:
            return False
        lowest = self.conn.execute("SELECT min(seq) FROM events WHERE task_id=?", (task["id"],)).fetchone()[0]
        return lowest is None or after < lowest - 1

    def prune_events(self, older_than: float) -> int:
        cur = self.conn.execute(
            f"""DELETE FROM events WHERE task_id IN
                (SELECT id FROM tasks WHERE status IN ({_TERMINAL_SQL}) AND finished_at < ?)""",
            (older_than,),
        )
        return cur.rowcount

    # ---- claiming -------------------------------------------------------

    @staticmethod
    def _dependency_satisfied(row: sqlite3.Row) -> bool:
        if row["status"] != SUCCEEDED:
            return False
        acceptance = json.loads(row["acceptance"]) if row["acceptance"] else {}
        return acceptance.get("status") != "failed"

    def claim_next(self, worker_id: str, lease_seconds: float, max_concurrency: int) -> Optional[tuple[dict, dict]]:
        """Atomically move the oldest runnable queued task to `starting`, respecting global limits."""
        now = time.time()
        with self.tx() as c:
            active = c.execute(f"SELECT id, slot FROM tasks WHERE status IN ({_ACTIVE_SQL})").fetchall()
            if len(active) >= max_concurrency:
                return None
            for task in c.execute("SELECT * FROM tasks WHERE status=? ORDER BY seq LIMIT 200", (QUEUED,)).fetchall():
                if self._runnable(c, task):
                    return self._claim(c, task, worker_id, lease_seconds, {r["slot"] for r in active}, now)
        return None

    def _runnable(self, c: sqlite3.Connection, task: sqlite3.Row) -> bool:
        for dep in json.loads(task["depends_on"]):
            row = c.execute("SELECT status, acceptance FROM tasks WHERE id=?", (dep,)).fetchone()
            if not row or not self._dependency_satisfied(row):
                return False
        if task["kind"] == "integration":
            # Merges into a project run strictly one at a time.
            busy = c.execute(
                f"SELECT 1 FROM tasks WHERE project_id=? AND kind='integration' AND status IN ({_ACTIVE_SQL}) LIMIT 1",
                (task["project_id"],),
            ).fetchone()
            if busy:
                return False
        source_id = task["continue_task_id"]
        if source_id:
            # A Claude session must never have two processes writing to it.
            source = c.execute("SELECT status, workdir FROM tasks WHERE id=?", (source_id,)).fetchone()
            if not source or source["status"] not in TERMINAL:
                return False
            busy = c.execute(
                f"""SELECT 1 FROM tasks WHERE status IN ({_ACTIVE_SQL})
                    AND (continue_task_id=? OR (workdir IS NOT NULL AND workdir=?)) LIMIT 1""",
                (source_id, source["workdir"]),
            ).fetchone()
            if busy:
                return False
        return True

    def _claim(self, c, task, worker_id, lease_seconds, used_slots, now) -> tuple[dict, dict]:
        slot = next(i for i in range(len(used_slots) + 1) if i not in used_slots)
        number = task["attempt_count"] + 1
        attempt_id = new_id("att")
        c.execute(
            "INSERT INTO attempts (id, task_id, number, worker_id, lease_expires_at, started_at) VALUES (?,?,?,?,?,?)",
            (attempt_id, task["id"], number, worker_id, now + lease_seconds, now),
        )
        c.execute(
            "UPDATE tasks SET status=?, slot=?, current_attempt=?, attempt_count=? WHERE id=?",
            (STARTING, slot, attempt_id, number, task["id"]),
        )
        self._add_event(c, task["id"], "state.changed", {"status": STARTING, "attempt": number, "worker": worker_id})
        return (
            dict(c.execute("SELECT * FROM tasks WHERE id=?", (task["id"],)).fetchone()),
            dict(c.execute("SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()),
        )

    # ---- fenced attempt writes ------------------------------------------

    def _fence(self, c: sqlite3.Connection, attempt_id: str, token: int, assignments: str = "", params: tuple = ()) -> None:
        sql = f"UPDATE attempts SET {assignments + ', ' if assignments else ''}id=id WHERE id=? AND lease_token=? AND ended_at IS NULL"
        if c.execute(sql, (*params, attempt_id, token)).rowcount != 1:
            raise LeaseLost(attempt_id)

    def renew_lease(self, attempt_id: str, token: int, expires_at: float) -> None:
        with self.tx() as c:
            self._fence(c, attempt_id, token, "lease_expires_at=?", (expires_at,))

    def release_lease(self, attempt_id: str, token: int) -> None:
        """Graceful worker shutdown: hand the live process to the next worker right away."""
        with self.tx() as c:
            self._fence(c, attempt_id, token, "lease_expires_at=0")

    def set_workspace(self, attempt_id: str, token: int, task_id: str, workdir: str, branch: Optional[str],
                      base_commit: Optional[str], run_dir: str) -> None:
        with self.tx() as c:
            self._fence(c, attempt_id, token)
            c.execute(
                "UPDATE tasks SET workdir=?, branch=?, base_commit=?, run_dir=? WHERE id=?",
                (workdir, branch, base_commit, run_dir, task_id),
            )
            self._add_event(c, task_id, "workspace.ready", {"branch": branch, "base_commit": base_commit})

    def mark_spawn_intent(self, attempt_id: str, token: int, stdout_path: str, stderr_path: str) -> None:
        """Recorded before spawning: after this point a crash may have left side effects."""
        with self.tx() as c:
            self._fence(c, attempt_id, token, "spawn_intent_at=?, stdout_path=?, stderr_path=?",
                        (time.time(), stdout_path, stderr_path))

    def mark_spawned(self, attempt_id: str, token: int, task_id: str, pid: int, start_time: int, boot_id: str) -> None:
        now = time.time()
        with self.tx() as c:
            self._fence(c, attempt_id, token, "pid=?, proc_start_time=?, boot_id=?", (pid, start_time, boot_id))
            c.execute(
                "UPDATE tasks SET status=CASE WHEN status=? THEN ? ELSE status END, started_at=coalesce(started_at, ?), last_event_at=? WHERE id=?",
                (STARTING, RUNNING, now, now, task_id),
            )
            status = c.execute("SELECT status FROM tasks WHERE id=?", (task_id,)).fetchone()[0]
            self._add_event(c, task_id, "state.changed", {"status": status, "pid": pid})

    def set_status(self, attempt_id: str, token: int, task_id: str, status: str, **payload: Any) -> None:
        with self.tx() as c:
            self._fence(c, attempt_id, token)
            c.execute("UPDATE tasks SET status=? WHERE id=?", (status, task_id))
            self._add_event(c, task_id, "state.changed", {"status": status, **payload})

    def add_attempt_event(self, attempt_id: str, token: int, task_id: str, event_type: str, payload: dict) -> None:
        with self.tx() as c:
            self._fence(c, attempt_id, token)
            self._add_event(c, task_id, event_type, payload)

    def ingest(self, attempt_id: str, token: int, task_id: str, events: list[tuple[str, dict]],
               stdout_offset: int, stderr_offset: int, last_action: Optional[str],
               claude_session_id: Optional[str], result: Optional[dict], touched: bool) -> None:
        """Store parsed events and the log offsets together so a takeover neither loses nor repeats any."""
        now = time.time()
        with self.tx() as c:
            self._fence(c, attempt_id, token, "stdout_offset=?, stderr_offset=?", (stdout_offset, stderr_offset))
            if result is not None:
                c.execute("UPDATE attempts SET result=? WHERE id=?", (json.dumps(result, ensure_ascii=False), attempt_id))
            for event_type, payload in events:
                self._add_event(c, task_id, event_type, payload)
            c.execute(
                """UPDATE tasks SET last_action=coalesce(?, last_action),
                       claude_session_id=coalesce(?, claude_session_id),
                       last_event_at=CASE WHEN ? THEN ? ELSE last_event_at END WHERE id=?""",
                (last_action, claude_session_id, int(touched), now, task_id),
            )

    def finish_attempt(self, attempt_id: str, token: int, task_id: str, status: str, *,
                       result: Optional[dict] = None, changes: Optional[dict] = None,
                       acceptance: Optional[dict] = None, error_code: Optional[str] = None,
                       error_message: Optional[str] = None, exit_code: Optional[int] = None,
                       exit_signal: Optional[int] = None) -> None:
        """Commit the terminal state and its notification in one transaction."""
        now = time.time()
        with self.tx() as c:
            self._fence(c, attempt_id, token, "exit_code=?, exit_signal=?, end_reason=?, ended_at=?",
                        (exit_code, exit_signal, error_code or status, now))
            self._finish_task(c, task_id, status, now, result, changes, acceptance, error_code, error_message)

    def _finish_task(self, c, task_id, status, now, result=None, changes=None, acceptance=None,
                     error_code=None, error_message=None) -> None:
        c.execute(
            """UPDATE tasks SET status=?, result=?, changes=?, acceptance=?, error_code=?, error_message=?,
                   finished_at=?, slot=NULL WHERE id=?""",
            (
                status,
                json.dumps(result, ensure_ascii=False) if result is not None else None,
                json.dumps(changes, ensure_ascii=False) if changes is not None else None,
                json.dumps(acceptance, ensure_ascii=False) if acceptance is not None else None,
                error_code, error_message, now, task_id,
            ),
        )
        self._add_event(c, task_id, "state.changed", {"status": status, "error_code": error_code})
        self._enqueue_notification(c, task_id, "task.completed", now)

    def requeue_for_retry(self, attempt_id: str, token: int, task_id: str, reason: str) -> None:
        now = time.time()
        with self.tx() as c:
            self._fence(c, attempt_id, token, "end_reason=?, ended_at=?", (reason, now))
            c.execute("UPDATE tasks SET status=?, slot=NULL, current_attempt=NULL WHERE id=?", (QUEUED, task_id))
            self._add_event(c, task_id, "state.changed", {"status": QUEUED, "reason": reason})

    def raise_alert(self, attempt_id: str, token: int, task_id: str, reason: str, detail: dict) -> None:
        now = time.time()
        with self.tx() as c:
            self._fence(c, attempt_id, token)
            self._add_event(c, task_id, "task.alert", {"reason": reason, **detail})
            self._enqueue_notification(c, task_id, "task.alert", now, {"reason": reason})

    # ---- recovery -------------------------------------------------------

    def expired_attempts(self, now: float) -> list[dict]:
        rows = self.conn.execute(
            f"""SELECT a.* FROM attempts a JOIN tasks t ON t.current_attempt = a.id
                WHERE t.status IN ({_ACTIVE_SQL}) AND a.ended_at IS NULL AND a.lease_expires_at < ?""",
            (now,),
        )
        return [dict(r) for r in rows]

    def take_over(self, attempt: dict, worker_id: str, lease_seconds: float) -> Optional[tuple[dict, dict]]:
        """Bump the lease token so the previous owner is fenced out, and enter `recovering`."""
        now = time.time()
        with self.tx() as c:
            updated = c.execute(
                """UPDATE attempts SET worker_id=?, lease_token=lease_token+1, lease_expires_at=?
                   WHERE id=? AND lease_token=? AND lease_expires_at < ? AND ended_at IS NULL""",
                (worker_id, now + lease_seconds, attempt["id"], attempt["lease_token"], now),
            )
            if updated.rowcount != 1:
                return None
            c.execute("UPDATE tasks SET status=? WHERE id=?", (RECOVERING, attempt["task_id"]))
            self._add_event(c, attempt["task_id"], "state.changed", {
                "status": RECOVERING, "attempt": attempt["number"], "previous_worker": attempt["worker_id"],
            })
            return (
                dict(c.execute("SELECT * FROM tasks WHERE id=?", (attempt["task_id"],)).fetchone()),
                dict(c.execute("SELECT * FROM attempts WHERE id=?", (attempt["id"],)).fetchone()),
            )

    # ---- cancellation and dependencies ----------------------------------

    def request_cancel(self, task_id: str) -> str:
        now = time.time()
        with self.tx() as c:
            row = c.execute("SELECT status FROM tasks WHERE id=?", (task_id,)).fetchone()
            status = row["status"]
            if status in TERMINAL or status == CANCELLING:
                return status
            if status == QUEUED:
                c.execute("UPDATE tasks SET cancel_requested_at=? WHERE id=?", (now, task_id))
                self._finish_task(c, task_id, CANCELLED, now)
                return CANCELLED
            c.execute("UPDATE tasks SET cancel_requested_at=coalesce(cancel_requested_at, ?) WHERE id=?", (now, task_id))
            if status == RECOVERING:
                # The recovering worker applies the request once process ownership is settled.
                self._add_event(c, task_id, "cancel.requested", {})
                return status
            c.execute("UPDATE tasks SET status=? WHERE id=?", (CANCELLING, task_id))
            self._add_event(c, task_id, "state.changed", {"status": CANCELLING})
            return CANCELLING

    def block_failed_dependents(self) -> int:
        """Fail queued tasks whose dependency can no longer succeed; cascades across calls."""
        now = time.time()
        blocked = 0
        with self.tx() as c:
            for task in c.execute("SELECT id, depends_on FROM tasks WHERE status=? AND depends_on != '[]'", (QUEUED,)).fetchall():
                for dep in json.loads(task["depends_on"]):
                    row = c.execute("SELECT status, acceptance FROM tasks WHERE id=?", (dep,)).fetchone()
                    if row and row["status"] in TERMINAL and not self._dependency_satisfied(row):
                        self._finish_task(c, task["id"], FAILED, now, error_code="dependency_failed",
                                          error_message=f"dependency {dep} ended as {row['status']}")
                        blocked += 1
                        break
        return blocked

    # ---- outbox ---------------------------------------------------------

    def _enqueue_notification(self, c, task_id: str, event_type: str, now: float, extra: Optional[dict] = None) -> None:
        task = c.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        if not task["hermes_session_id"]:
            return
        acceptance = json.loads(task["acceptance"]) if task["acceptance"] else {}
        event_id = new_id("evt")
        payload = {
            "event_id": event_id,
            "type": event_type,
            "task_id": task_id,
            "project_id": task["project_id"],
            "hermes_session_id": task["hermes_session_id"],
            "hermes_session_key": task["hermes_session_key"],
            "status": task["status"],
            "acceptance_status": acceptance.get("status"),
            "error_code": task["error_code"],
            "result_ref": f"/v1/tasks/{task_id}",
            "occurred_at": now,
            **(extra or {}),
        }
        c.execute(
            "INSERT INTO outbox (event_id, task_id, type, payload, next_attempt_at, created_at) VALUES (?,?,?,?,?,?)",
            (event_id, task_id, event_type, json.dumps(payload, ensure_ascii=False), now, now),
        )

    def claim_due_outbox(self, limit: int, lease_seconds: float = 30.0) -> list[dict]:
        now = time.time()
        with self.tx() as c:
            rows = c.execute(
                """SELECT * FROM outbox WHERE state='pending' AND next_attempt_at<=? AND lease_until<=?
                   ORDER BY created_at LIMIT ?""",
                (now, now, limit),
            ).fetchall()
            for row in rows:
                c.execute("UPDATE outbox SET lease_until=? WHERE event_id=?", (now + lease_seconds, row["event_id"]))
        return [dict(r) for r in rows]

    def outbox_delivered(self, event_id: str) -> None:
        self.conn.execute(
            "UPDATE outbox SET state='delivered', delivered_at=?, attempts=attempts+1, last_error=NULL, lease_until=0 WHERE event_id=?",
            (time.time(), event_id),
        )

    def outbox_failed(self, event_id: str, error: str, next_attempt_at: Optional[float]) -> None:
        """Schedule a retry, or move to the dead-letter state when next_attempt_at is None."""
        if next_attempt_at is None:
            self.conn.execute(
                "UPDATE outbox SET state='dead', attempts=attempts+1, last_error=?, lease_until=0 WHERE event_id=?",
                (error, event_id),
            )
        else:
            self.conn.execute(
                "UPDATE outbox SET attempts=attempts+1, last_error=?, next_attempt_at=?, lease_until=0 WHERE event_id=?",
                (error, next_attempt_at, event_id),
            )

    def dead_letters(self) -> list[dict]:
        return [dict(r) for r in self.conn.execute("SELECT * FROM outbox WHERE state='dead' ORDER BY created_at")]

    def retry_dead_letter(self, event_id: str) -> bool:
        cur = self.conn.execute(
            "UPDATE outbox SET state='pending', attempts=0, next_attempt_at=?, last_error=NULL WHERE event_id=? AND state='dead'",
            (time.time(), event_id),
        )
        return cur.rowcount == 1

    def outbox_counts(self) -> dict[str, int]:
        return {r["state"]: r["n"] for r in self.conn.execute("SELECT state, count(*) n FROM outbox GROUP BY state")}

    # ---- housekeeping ---------------------------------------------------

    def collectable_tasks(self, finished_before: float) -> list[dict]:
        """Finished tasks with a working directory that no queued or active follow-up still needs."""
        rows = self.conn.execute(
            f"""SELECT t.* FROM tasks t
                WHERE t.status IN ({_TERMINAL_SQL}) AND t.finished_at < ? AND t.workdir IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM tasks f
                      WHERE (f.continue_task_id = t.id OR f.workdir = t.workdir) AND f.id != t.id
                        AND f.status NOT IN ({_TERMINAL_SQL}))""",
            (finished_before,),
        )
        return [dict(r) for r in rows]

    # ---- worker registry ------------------------------------------------

    def worker_heartbeat(self, worker_id: str, pid: int, started_at: float) -> None:
        self.conn.execute(
            """INSERT INTO workers VALUES (?,?,?,?)
               ON CONFLICT (id) DO UPDATE SET heartbeat_at=excluded.heartbeat_at""",
            (worker_id, pid, started_at, time.time()),
        )

    def remove_worker(self, worker_id: str) -> None:
        self.conn.execute("DELETE FROM workers WHERE id=?", (worker_id,))

    def list_workers(self) -> list[dict]:
        return [dict(r) for r in self.conn.execute("SELECT * FROM workers ORDER BY started_at")]

    def prune_workers(self, older_than: float) -> None:
        self.conn.execute("DELETE FROM workers WHERE heartbeat_at < ?", (older_than,))
