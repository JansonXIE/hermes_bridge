"""Plugin against the installed Hermes (real plugin discovery, registry and grant checks)."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
import unittest
from pathlib import Path

from .harness import ADAPTER_TOKEN, ROOT, TOKEN, BridgeCase, http

HERMES_DIR = Path(os.environ.get("HERMES_AGENT_DIR", Path.home() / ".hermes" / "hermes-agent"))
# A venv install by default; installs that bundle their own interpreter can point HERMES_PYTHON at it.
HERMES_PYTHON = Path(os.environ.get("HERMES_PYTHON", HERMES_DIR / "venv" / "bin" / "python"))


@unittest.skipUnless(HERMES_PYTHON.exists(), "Hermes is not installed")
class HermesPlugin(BridgeCase):
    def start_driver(self, grant: bool):
        home = self.tmp / f"hermes-home-{grant}"
        (home / "plugins").mkdir(parents=True)
        shutil.copytree(ROOT / "hermes_plugin" / "claude-bridge", home / "plugins" / "claude-bridge")
        entries = "\n  entries:\n    claude-bridge:\n      allow_gateway_injection: true" if grant else ""
        (home / "config.yaml").write_text(f"plugins:\n  enabled:\n    - claude-bridge{entries}\n")
        env = {
            **os.environ,
            "HERMES_HOME": str(home),
            "PYTHONPATH": str(HERMES_DIR),
            "CLAUDE_BRIDGE_URL": self.api,
            "CLAUDE_BRIDGE_TOKEN": TOKEN,
            "CLAUDE_BRIDGE_ADAPTER_URL": self.adapter,
            "CLAUDE_BRIDGE_ADAPTER_TOKEN": ADAPTER_TOKEN,
        }
        driver = subprocess.Popen(
            [str(HERMES_PYTHON), str(Path(__file__).parent / "hermes_plugin_driver.py")],
            env=env, cwd=home, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
        )
        self.addCleanup(driver.stdout.close)
        self.addCleanup(driver.stdin.close)
        self.addCleanup(driver.wait)
        self.addCleanup(driver.kill)
        return driver

    def call(self, driver, **command):
        driver.stdin.write(json.dumps(command) + "\n")
        driver.stdin.flush()
        return json.loads(driver.stdout.readline())

    def inbox_states(self):
        from hermes_bridge.adapter import Inbox
        from hermes_bridge.config import load_config
        return Inbox(load_config(self.config_path)).counts()

    def test_tools_and_gateway_wake_up_with_grant(self):
        driver = self.start_driver(grant=True)
        self.assertEqual(self.call(driver, op="tools"),
                         ["claude_task_cancel", "claude_task_get", "claude_task_list", "claude_task_start",
                          "claude_workflow_decide", "claude_workflow_get", "claude_workflow_list",
                          "claude_workflow_merge_check", "claude_workflow_review", "claude_workflow_rework",
                          "claude_workflow_start"])
        self.call(driver, op="gateway_on")

        key = "agent:main:weixin:dm:42"
        started = self.call(driver, op="tool", name="claude_task_start", session_id="hs_gw", session_key=key,
                            args={"project_id": "demo", "prompt": "WRITE=a.txt"})
        self.assertTrue(started["success"], started)
        task_id = started["task_id"]

        final = self.wait_status(task_id, "succeeded")
        self.assertEqual(final["hermes_session_id"], "hs_gw")
        injected = self.wait_until(lambda: self.call(driver, op="injected"), timeout=20, message="gateway injection")
        self.assertEqual(len(injected), 1)
        self.assertEqual(injected[0]["session_key"], key, "the wake-up goes to the session that started the task")
        self.assertEqual(injected[0]["plugin_id"], "claude-bridge")
        self.assertIn(task_id, injected[0]["content"])
        self.wait_until(lambda: self.inbox_states().get("delivered") == 1, message="delivery acked")

        got = self.call(driver, op="tool", name="claude_task_get", args={"task_id": task_id, "events_after": 0}, session_id="hs_gw")
        self.assertEqual(got["task"]["status"], "succeeded")
        self.assertIn("untrusted", got["trust_note"])
        self.assertGreater(len(got["events"]["events"]), 3)
        listed = self.call(driver, op="tool", name="claude_task_list", args={}, session_id="hs_gw")
        self.assertEqual([t["task_id"] for t in listed["tasks"]], [task_id])
        missing = self.call(driver, op="tool", name="claude_task_get", args={"task_id": "task_nope"})
        self.assertFalse(missing["success"])
        self.assertIn("404", missing["error"])

        running = self.call(driver, op="tool", name="claude_task_start", session_id="hs_gw", session_key=key,
                            args={"project_id": "demo", "prompt": "SLEEP=60"})
        self.wait_status(running["task_id"], "running")
        cancelled = self.call(driver, op="tool", name="claude_task_cancel", args={"task_id": running["task_id"]})
        self.assertEqual(cancelled["status"], "cancelling")
        self.wait_status(running["task_id"], "cancelled")

    def test_workflow_tools_and_wake_up(self):
        driver = self.start_driver(grant=True)
        self.call(driver, op="gateway_on")
        key = "agent:main:weixin:dm:7"
        # No Gerrit remote here: the developer cannot push, so the workflow stops for a human.
        started = self.call(driver, op="tool", name="claude_workflow_start", session_id="hs_wf", session_key=key, args={
            "project_id": "demo", "title": "demo change", "requirement": "add a file", "branch": "master",
            "acceptance_criteria": ["the file exists"], "instructions": "WRITE=a.txt",
        })
        self.assertTrue(started["success"], started)
        injected = self.wait_until(lambda: self.call(driver, op="injected"), timeout=30, message="workflow wake-up")
        self.assertEqual(injected[0]["session_key"], key)
        self.assertIn(started["workflow_id"], injected[0]["content"])
        self.assertIn("需人工处理", injected[0]["content"])

        got = self.call(driver, op="tool", name="claude_workflow_get", args={"workflow_id": started["workflow_id"]})
        self.assertEqual(got["workflow"]["business_status"], "needs_human")
        self.assertIn("evidence_missing", got["workflow"]["dev_submission"]["problems"])
        self.assertIn("仅适用于本次 Patch Set", got["workflow"]["notification"])
        refused = self.call(driver, op="tool", name="claude_workflow_review", args={"workflow_id": started["workflow_id"]})
        self.assertFalse(refused["success"])
        self.assertIn("invalid_state", refused["error"])
        listed = self.call(driver, op="tool", name="claude_workflow_list", args={}, session_id="hs_wf")
        self.assertEqual([w["workflow_id"] for w in listed["workflows"]], [started["workflow_id"]])
        decided = self.call(driver, op="tool", name="claude_workflow_decide",
                            args={"workflow_id": started["workflow_id"], "decision": "abandon", "source_ref": "m1"})
        self.assertEqual(decided["business_status"], "abandoned")

    def test_without_grant_hermes_refuses_and_the_event_is_kept(self):
        driver = self.start_driver(grant=False)
        self.call(driver, op="gateway_on")
        started = self.call(driver, op="tool", name="claude_task_start", session_id="hs_nogrant", session_key="agent:main:x",
                            args={"project_id": "demo", "prompt": "WRITE=a.txt"})
        self.wait_status(started["task_id"], "succeeded")
        time.sleep(6)
        self.assertEqual(self.call(driver, op="injected"), [], "Hermes' own permission check blocks it")
        self.assertNotIn("delivered", self.inbox_states(), "a refused wake-up is not lost")

    def test_cli_turn_is_not_interrupted(self):
        driver = self.start_driver(grant=False)
        self.call(driver, op="cli_on")
        started = self.call(driver, op="tool", name="claude_task_start", session_id="hs_cli",
                            args={"project_id": "demo", "prompt": "WRITE=a.txt"})
        self.wait_status(started["task_id"], "succeeded")
        time.sleep(5)
        queues = self.call(driver, op="cli_queues")
        self.assertEqual((queues["pending"], queues["interrupt"]), (0, 0), "held while the turn is running")

        self.call(driver, op="cli_idle")
        queues = self.wait_until(lambda: (q := self.call(driver, op="cli_queues"))["pending"] and q, message="queued as next input")
        self.assertEqual(queues["interrupt"], 0)
        self.assertIn(started["task_id"], queues["text"])


if __name__ == "__main__":
    unittest.main()
