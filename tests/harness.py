"""Runs real api / worker / adapter processes against a throwaway repo and fake Claude."""

from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FAKE_CLAUDE = Path(__file__).resolve().parent / "fake_claude.py"
TOKEN = "test-token-hermes-0123456789"
OTHER_TOKEN = "test-token-other-0123456789"
NOTIFY_SECRET = "test-notify-secret-0123456789"
ADAPTER_TOKEN = "test-adapter-token-0123456789"

_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http(method, url, body=None, headers=None, raw=False, timeout=10):
    data = json.dumps(body).encode() if isinstance(body, dict) else body
    request = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        with _opener.open(request, timeout=timeout) as response:
            payload = response.read()
            return response.status, payload if raw else json.loads(payload or b"null")
    except urllib.error.HTTPError as exc:
        payload = exc.read()
        return exc.code, payload if raw else json.loads(payload or b"null")


# Repository hooks stay active in tests, as they will in production.
COMMIT_MESSAGE = "[chore](test): fixture commit\n\n[description ]:test fixture\n[root   cause]:NA\n"


def git(cwd, *args):
    return subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", *args],
        cwd=cwd, check=True, capture_output=True, text=True,
    ).stdout.strip()


class BridgeCase(unittest.TestCase):
    max_concurrency = 3
    lease_seconds = 2.0
    verify_command = None
    extra_worker = ""
    extra_adapter = ""
    start_adapter = True

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="bridge-test-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.repo = self.tmp / "repo"
        self.repo.mkdir()
        git(self.repo, "init", "-q")
        git(self.repo, "symbolic-ref", "HEAD", "refs/heads/main")
        (self.repo / "README").write_text("base\n")
        git(self.repo, "add", ".")
        git(self.repo, "commit", "-q", "-m", COMMIT_MESSAGE)
        self.ledger = self.tmp / "ledger.jsonl"
        self.ledger.touch()
        self.api_port = free_port()
        self.adapter_port = free_port()
        self.api = f"http://127.0.0.1:{self.api_port}"
        self.adapter = f"http://127.0.0.1:{self.adapter_port}"
        self.config_path = self.tmp / "bridge.toml"
        self.config_path.write_text(self._config())
        self.procs = {}
        self.addCleanup(self._teardown)
        self.start("api")
        self.start("worker")
        if self.start_adapter:
            self.start("adapter")
        self.wait_port(self.api_port)
        if self.start_adapter:
            self.wait_port(self.adapter_port)

    def _config(self) -> str:
        verify = f"verify_command = {json.dumps(self.verify_command)}" if self.verify_command else ""
        return f"""
data_dir = "data"

[api]
host = "127.0.0.1"
port = {self.api_port}
[[api.tokens]]
name = "hermes"
token = "{TOKEN}"
projects = ["demo", "scratch"]
[[api.tokens]]
name = "other"
token = "{OTHER_TOKEN}"
projects = ["demo"]

[worker]
max_concurrency = {self.max_concurrency}
lease_seconds = {self.lease_seconds}
poll_seconds = 0.1
tick_seconds = 0.1
term_grace_seconds = 1.0
kill_wait_seconds = 2.0
{self.extra_worker}

[claude]
command = ["{sys.executable}", "{FAKE_CLAUDE}"]

[notify]
url = "http://127.0.0.1:{self.adapter_port}/v1/events"
secret = "{NOTIFY_SECRET}"
max_attempts = 50
base_backoff = 0.2
max_backoff = 1.0

[adapter]
host = "127.0.0.1"
port = {self.adapter_port}
token = "{ADAPTER_TOKEN}"
coalesce_seconds = 0.3
lease_seconds = 2.0
{self.extra_adapter}

[projects.demo]
path = "{self.repo}"
allowed_tools = ["Read", "Write"]
{verify}
[projects.demo.env]
FAKE_CLAUDE_LEDGER = "{self.ledger}"

[projects.scratch]
isolation = "directory"
[projects.scratch.env]
FAKE_CLAUDE_LEDGER = "{self.ledger}"
"""

    def start(self, role):
        env = {**os.environ, "FAKE_CLAUDE_LEDGER": str(self.ledger), "PYTHONPATH": str(ROOT), "UNRELATED_SECRET": "leak-me-please"}
        log = open(self.tmp / f"{role}.log", "ab")
        self.addCleanup(log.close)
        self.procs[role] = subprocess.Popen(
            [sys.executable, "-m", "hermes_bridge", "-c", str(self.config_path), role],
            env=env, stdout=log, stderr=subprocess.STDOUT, cwd=ROOT,
        )

    def kill(self, role, sig=signal.SIGKILL):
        proc = self.procs.pop(role)
        proc.send_signal(sig)
        proc.wait(timeout=15)

    def _teardown(self):
        for role in list(self.procs):
            self.kill(role, signal.SIGTERM if role != "worker" else signal.SIGKILL)
        # Fake Claude processes outlive a killed worker by design; clean them up here.
        for entry in self.ledger_entries():
            try:
                os.kill(entry["pid"], signal.SIGKILL)
            except OSError:
                pass

    def wait_port(self, port, timeout=15):
        deadline = time.time() + timeout
        while time.time() < deadline:
            with socket.socket() as s:
                if s.connect_ex(("127.0.0.1", port)) == 0:
                    return
            time.sleep(0.05)
        self.fail(f"port {port} did not open; logs: {self.logs()}")

    def logs(self) -> str:
        return "\n".join(f"--- {p.name}\n{p.read_text()[-3000:]}" for p in self.tmp.glob("*.log"))

    def headers(self, token=TOKEN, key=None):
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        if key:
            headers["Idempotency-Key"] = key
        return headers

    def submit(self, prompt="hello", project="demo", key=None, token=TOKEN, expect=202, **fields):
        body = {"project_id": project, "prompt": prompt, "hermes_session_id": "hs_test", **fields}
        status, reply = http("POST", f"{self.api}/v1/tasks", body, self.headers(token, key or uuid.uuid4().hex))
        self.assertEqual(status, expect, reply)
        return reply["task_id"] if expect == 202 else reply

    def task(self, task_id, token=TOKEN):
        status, reply = http("GET", f"{self.api}/v1/tasks/{task_id}", headers=self.headers(token))
        self.assertEqual(status, 200, reply)
        return reply

    def wait_status(self, task_id, *statuses, timeout=20):
        deadline = time.time() + timeout
        task = None
        while time.time() < deadline:
            task = self.task(task_id)
            if task["status"] in statuses:
                return task
            time.sleep(0.05)
        self.fail(f"{task_id} never reached {statuses}, last: {task}\n{self.logs()}")

    def wait_until(self, predicate, timeout=20, message="condition"):
        deadline = time.time() + timeout
        while time.time() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(0.05)
        self.fail(f"timed out waiting for {message}\n{self.logs()}")

    def ledger_entries(self, event="start"):
        lines = self.ledger.read_text().splitlines() if self.ledger.exists() else []
        return [e for e in map(json.loads, lines) if e["event"] == event]

    def runs_of(self, task_id):
        return [e for e in self.ledger_entries() if e["task"] == task_id]

    def claim(self, session_ids=("hs_test",), any_session_key=False):
        status, reply = http("POST", f"{self.adapter}/v1/deliveries/claim", {
            "consumer": "test", "session_ids": list(session_ids), "any_session_key": any_session_key,
        }, self.headers(ADAPTER_TOKEN))
        self.assertEqual(status, 200, reply)
        return reply["delivery"]

    def ack(self, delivery_id, ok=True):
        return http("POST", f"{self.adapter}/v1/deliveries/{delivery_id}/ack", {"ok": ok}, self.headers(ADAPTER_TOKEN))

    def read_sse(self, task_id, last_event_id=None, stop_after=None, timeout=20):
        """Returns (events, ended). Each event is a dict with id, event, data."""
        headers = self.headers()
        if last_event_id is not None:
            headers["Last-Event-ID"] = str(last_event_id)
        request = urllib.request.Request(f"{self.api}/v1/tasks/{task_id}/events", headers=headers)
        events, current, ended = [], {}, False
        with _opener.open(request, timeout=timeout) as response:
            self.assertTrue(response.headers["Content-Type"].startswith("text/event-stream"))
            for raw in response:
                line = raw.decode().rstrip("\n")
                if line.startswith("id: "):
                    current["id"] = int(line[4:])
                elif line.startswith("event: "):
                    current["event"] = line[7:]
                elif line.startswith("data: "):
                    current["data"] = json.loads(line[6:])
                elif not line and current:
                    if current.get("event") == "stream.end":
                        ended = True
                        break
                    if "id" in current:
                        events.append(current)
                    current = {}
                    if stop_after and len(events) >= stop_after:
                        break
        return events, ended
