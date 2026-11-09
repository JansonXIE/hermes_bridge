"""The develop → Gerrit → review workflow, on real processes with a bare repository as the Gerrit remote."""

from __future__ import annotations

import json
import subprocess
import time
import unittest
import uuid

from hermes_bridge import workflow as wfm

from .harness import ADAPTER_TOKEN, BridgeCase, git, http

CHANGE_ID = "I" + "a" * 40
GERRIT_URL = "http://gerrit.test/c/demo/+/101"


def commit_and_push(path="feature.txt", content="feature", change_id=CHANGE_ID, amend=False, dest="refs/for/main"):
    """Directives for a developer that commits like gerrit-submit and pushes to the review ref."""
    commit = "commit -q --amend" if amend else "commit -q"
    return "\n".join([
        f"BASH=echo {content} > {path} && git add {path}",
        f"BASH=git -c user.name=t -c user.email=t@t {commit} -m '[feat](demo):add feature' -m 'Change-Id: {change_id}'",
        f"BASH=git push origin +HEAD:{dest}",
    ])


EVIDENCE = 'JSONFILE=.bridge/dev-evidence.json {"revision": "{HEAD}", "change_id": "{CHANGE_ID}", "pushed_ref": "refs/for/main", "self_tests": [{"command": "make test", "result": "passed"}]}'


def report(verdict="approve_recommended", findings=(), results=("pass",), blocked=()):
    data = {
        "subject": {"revision": "{HEAD}", "change_id": "{CHANGE_ID}"},
        "verdict": verdict,
        "summary": "reviewed",
        "blocked_reasons": list(blocked),
        "findings": list(findings),
        "criteria": [{"id": f"AC{i + 1}", "result": r, "evidence": "checked"} for i, r in enumerate(results)],
        "tests": [{"command": "make test", "scope": "targeted", "result": "passed"}],
        "test_weakening": {"found": False},
        "ci": {"status": "none"},
    }
    return "JSONFILE=.bridge/review-report.json " + json.dumps(data)


MAJOR = {"severity": "major", "file": "feature.txt", "line": 1, "title": "wrong content", "detail": "fix it"}


class WorkflowCase(BridgeCase):
    policy = "confirm"
    max_rework = 2

    def _config(self):
        return super()._config() + f"""
[workflow]
remote = "origin"
max_rework_rounds = {self.max_rework}
rework_policy = "{self.policy}"
fetch_timeout_seconds = 30
[workflow.branches]
main = "main"
"""

    def setUp(self):
        super().setUp()
        self.origin = self.tmp / "origin.git"
        subprocess.run(["git", "init", "-q", "--bare", str(self.origin)], check=True)
        # Gerrit announces the change on push; a hook prints the same kind of line.
        hook = self.origin / "hooks" / "post-receive"
        hook.write_text(f"#!/bin/sh\necho '  {GERRIT_URL} [feat](demo):add feature [NEW]'\n")
        hook.chmod(0o755)
        git(self.repo, "remote", "add", "origin", str(self.origin))
        git(self.repo, "push", "-q", "origin", "main")

    # ---- helpers ---------------------------------------------------------------

    def start_workflow(self, dev_directives, expect=202, key=None, **fields):
        # Directives go in the developer-only instructions: the review prompt repeats the requirement.
        body = {
            "project_id": "demo", "title": "add feature", "requirement": "add feature.txt", "branch": "main",
            "instructions": dev_directives,
            "acceptance_criteria": ["feature.txt exists"], "hermes_session_id": "hs_test", **fields,
        }
        status, reply = http("POST", f"{self.api}/v1/workflows", body, self.headers(key=key or uuid.uuid4().hex))
        self.assertEqual(status, expect, reply)
        return reply

    def workflow(self, workflow_id):
        status, reply = http("GET", f"{self.api}/v1/workflows/{workflow_id}", headers=self.headers())
        self.assertEqual(status, 200, reply)
        return reply

    def wait_business(self, workflow_id, *statuses, timeout=30):
        last = {}

        def reached():
            nonlocal last
            last = self.workflow(workflow_id)
            return last if last["business_status"] in statuses else None

        deadline = time.time() + timeout
        while time.time() < deadline:
            if reached():
                return last
            time.sleep(0.1)
        self.fail(f"{workflow_id} never reached {statuses}: {json.dumps(last, ensure_ascii=False)[:3000]}\n{self.logs()}")

    def action(self, workflow_id, name, body=None, expect=202, key=None):
        headers = self.headers(key=key)
        status, reply = http("POST", f"{self.api}/v1/workflows/{workflow_id}/{name}", body or {}, headers)
        self.assertEqual(status, expect, reply)
        return reply

    def decide(self, workflow_id, decision, source_ref=None, expect=200, **fields):
        body = {"decision": decision, "source_ref": source_ref or uuid.uuid4().hex, **fields}
        return self.action(workflow_id, "decisions", body, expect=expect)

    def developed(self, dev_directives=None):
        reply = self.start_workflow(dev_directives or f"{commit_and_push()}\n{EVIDENCE}")
        wf = self.wait_business(reply["workflow_id"], wfm.AWAITING_REVIEW, wfm.NEEDS_HUMAN)
        return reply, wf

    def wake_texts(self):
        texts = []
        while True:
            delivery = self.claim()
            if not delivery:
                return texts
            texts.append(delivery["text"])
            self.ack(delivery["delivery_id"])


class HappyPath(WorkflowCase):
    def test_develop_review_approve_and_merge(self):
        reply, wf = self.developed()
        self.assertEqual(wf["business_status"], wfm.AWAITING_REVIEW, wf)
        head = git(self.origin, "rev-parse", "refs/for/main")
        change = wf["change"]
        self.assertEqual(change["revision"], head)
        self.assertEqual(change["change_id"], CHANGE_ID)
        self.assertEqual(change["gerrit_url"], GERRIT_URL)
        self.assertEqual(change["change_number"], 101)
        self.assertEqual(wf["dev_submission"]["problems"], [])
        self.assertEqual(wf["dev_submission"]["gerrit"]["target_check"], "clean")
        self.assertEqual(git(self.origin, "rev-parse", "main"), git(self.repo, "rev-parse", "main"), "target untouched")
        wake = self.wait_until(self.wake_texts, message="dev wake-up")
        self.assertIn("claude_workflow_review", "\n".join(wake))

        review = self.action(reply["workflow_id"], "review", {"instructions": report()})
        self.assertEqual(review["role"], "review")
        wf = self.wait_business(reply["workflow_id"], wfm.AWAITING_APPROVAL)
        self.assertTrue(wf["latest_report"]["valid"], wf["latest_report"])
        self.assertEqual(wf["latest_report"]["verdict"], wfm.APPROVE)
        self.assertIn("建议通过", wf["notification"])
        self.assertIn(GERRIT_URL, wf["notification"])
        self.assertIn("仅适用于本次 Patch Set", wf["notification"])

        dev_run = self.runs_of(reply["task_id"])[0]
        review_run = self.runs_of(review["task_id"])[0]
        self.assertNotEqual(dev_run["cwd"], review_run["cwd"], "review uses its own worktree")
        self.assertNotIn("--resume", review_run["argv"], "review uses a new session")
        self.assertIn("Bash(git push:*)", review_run["argv"][review_run["argv"].index("--disallowedTools") + 1])
        self.assertIn(f"Bash(git push origin HEAD:main:*)", dev_run["argv"][dev_run["argv"].index("--disallowedTools") + 1])
        self.assertIn(CHANGE_ID, review_run["prompt"])

        approved = self.decide(reply["workflow_id"], "approve", source_ref="msg-1")
        self.assertEqual(approved["business_status"], wfm.AWAITING_MERGE)
        self.assertTrue(self.decide(reply["workflow_id"], "approve", source_ref="msg-1")["idempotent_replay"])

        # Not merged yet: the check says so and the state stays.
        self.action(reply["workflow_id"], "merge-check")
        self.wait_until(lambda: self.workflow(reply["workflow_id"])["merge_check"]["checked_at"], message="merge check")
        self.assertEqual(self.workflow(reply["workflow_id"])["business_status"], wfm.AWAITING_MERGE)

        # The user merges in Gerrit; the change lands on the target branch.
        git(self.repo, "push", "-q", "origin", f"{head}:refs/heads/main")
        self.action(reply["workflow_id"], "merge-check")
        wf = self.wait_business(reply["workflow_id"], wfm.MERGED)
        self.assertEqual(wf["merged_commit"], head)
        seen = []
        self.wait_until(lambda: seen.extend(self.wake_texts()) or any("已在目标分支找到该改动" in t for t in seen),
                        message="merge wake-up")


class Rework(WorkflowCase):
    def test_changes_requested_rework_new_patch_set_and_second_round(self):
        reply, wf = self.developed()
        first_revision = wf["change"]["revision"]
        self.action(reply["workflow_id"], "review", {"instructions": report(wfm.CHANGES, [MAJOR], ["fail"])})
        wf = self.wait_business(reply["workflow_id"], wfm.CHANGES_REQUESTED)
        self.assertIn("需要你确认", wf["notification"])

        # The confirm policy refuses a rework the user did not ask for.
        refused = self.action(reply["workflow_id"], "rework", {"source": "auto"}, expect=409)
        self.assertEqual(refused["error"]["code"], "confirmation_required")

        rework_body = {"source": "user", "instructions": f"{commit_and_push(content='fixed', amend=True)}\n{EVIDENCE}"}
        rework = self.action(reply["workflow_id"], "rework", rework_body)
        again = self.action(reply["workflow_id"], "rework", rework_body)
        self.assertEqual(again["task_id"], rework["task_id"])
        self.assertTrue(again["idempotent_replay"])
        wf = self.wait_business(reply["workflow_id"], wfm.AWAITING_REVIEW, wfm.NEEDS_HUMAN)
        self.assertEqual(wf["business_status"], wfm.AWAITING_REVIEW, wf)
        self.assertNotEqual(wf["change"]["revision"], first_revision)
        self.assertEqual(wf["change"]["change_id"], CHANGE_ID, "same change, new patch set")
        self.assertEqual(wf["rework_count"], 1)

        dev_run = self.runs_of(reply["task_id"])[0]
        rework_run = self.runs_of(rework["task_id"])[0]
        self.assertEqual(rework_run["cwd"], dev_run["cwd"], "rework continues in the developer's worktree")
        self.assertEqual(rework_run["argv"][rework_run["argv"].index("--resume") + 1], dev_run["session"])
        self.assertIn("wrong content", rework_run["prompt"])

        self.action(reply["workflow_id"], "review", {"instructions": report()})
        wf = self.wait_business(reply["workflow_id"], wfm.AWAITING_APPROVAL)
        self.assertEqual(wf["review_round"], 2)
        self.assertEqual(wf["latest_report"]["revision"], wf["change"]["revision"])
        second_review = [t for t in wf["tasks"] if t["role"] == "review"][-1]
        self.assertIn("wrong content", self.runs_of(second_review["task_id"])[0]["prompt"], "previous findings passed on")


class AutoReworkLimit(WorkflowCase):
    policy = "auto"
    max_rework = 0

    def test_limit_reached_goes_to_a_human(self):
        reply, _ = self.developed()
        self.action(reply["workflow_id"], "review", {"instructions": report(wfm.CHANGES, [MAJOR], ["fail"])})
        wf = self.wait_business(reply["workflow_id"], wfm.CHANGES_REQUESTED)
        self.assertIn("返工上限", wf["notification"])
        refused = self.action(reply["workflow_id"], "rework", {"source": "auto"}, expect=409)
        self.assertEqual(refused["error"]["code"], "rework_limit_reached")
        wf = self.workflow(reply["workflow_id"])
        self.assertEqual(wf["business_status"], wfm.NEEDS_HUMAN)
        self.assertEqual(len([t for t in wf["tasks"] if t["role"] == "rework"]), 0)


class ReviewBlocked(WorkflowCase):
    def test_missing_report_tampering_and_blocked_verdict(self):
        reply, _ = self.developed()
        self.action(reply["workflow_id"], "review", {"instructions": "no report written"})
        wf = self.wait_business(reply["workflow_id"], wfm.REVIEW_BLOCKED)
        self.assertFalse(wf["latest_report"]["valid"])
        self.assertIn("missing", wf["latest_report"]["invalid_reason"])
        self.assertIn("审核受阻", wf["notification"])

        self.decide(reply["workflow_id"], "rereview")
        tamper = f"BASH=echo changed > README\n{report()}"
        self.action(reply["workflow_id"], "review", {"instructions": tamper})
        wf = self.wait_business(reply["workflow_id"], wfm.REVIEW_BLOCKED)
        self.assertIn("modified tracked files", wf["latest_report"]["invalid_reason"])

        self.decide(reply["workflow_id"], "rereview")
        self.action(reply["workflow_id"], "review",
                    {"instructions": report(wfm.BLOCKED, results=["unverified"], blocked=["no board available"])})
        wf = self.wait_business(reply["workflow_id"], wfm.REVIEW_BLOCKED)
        self.assertTrue(wf["latest_report"]["valid"])
        self.assertIn("no board available", wf["notification"])

    def test_inconsistent_report_is_not_taken_as_approval(self):
        reply, _ = self.developed()
        self.action(reply["workflow_id"], "review", {"instructions": report(wfm.APPROVE, results=["unverified"])})
        wf = self.wait_business(reply["workflow_id"], wfm.REVIEW_BLOCKED)
        self.assertIn("every acceptance criterion", wf["latest_report"]["invalid_reason"])


class DevChecks(WorkflowCase):
    def test_direct_push_to_target_is_caught(self):
        _, wf = self.developed(f"{commit_and_push(dest='refs/heads/main')}\n{EVIDENCE}")
        self.assertEqual(wf["business_status"], wfm.NEEDS_HUMAN)
        problems = wf["dev_submission"]["problems"]
        self.assertIn("direct_push_detected", problems)
        self.assertIn("push_not_found", problems)
        self.assertIn("target_contains_change", problems)
        self.assertIn("需要人工处理", wf["notification"])

    def test_missing_evidence_and_change_id(self):
        directives = "\n".join([
            "BASH=echo x > f.txt && git add f.txt && git -c user.name=t -c user.email=t@t commit -q -m '[feat](demo):x'",
            "BASH=git push origin HEAD:refs/for/main",
        ])
        _, wf = self.developed(directives)
        self.assertEqual(wf["business_status"], wfm.NEEDS_HUMAN)
        self.assertIn("evidence_missing", wf["dev_submission"]["problems"])
        self.assertIn("change_id_missing", wf["dev_submission"]["problems"])
        refused = self.action(wf["workflow_id"], "review", expect=409)
        self.assertEqual(refused["error"]["code"], "invalid_state")


class Idempotency(WorkflowCase):
    def test_repeated_requests_do_not_duplicate_work(self):
        key = uuid.uuid4().hex
        first = self.start_workflow(f"{commit_and_push()}\n{EVIDENCE}", key=key)
        replay = self.start_workflow(f"{commit_and_push()}\n{EVIDENCE}", key=key)
        self.assertEqual(replay["workflow_id"], first["workflow_id"])
        self.assertTrue(replay["idempotent_replay"])
        self.wait_business(first["workflow_id"], wfm.AWAITING_REVIEW)

        reviews = {self.action(first["workflow_id"], "review", {"instructions": f"SLEEP=1\n{report()}"},
                               key=uuid.uuid4().hex)["task_id"] for _ in range(3)}
        self.assertEqual(len(reviews), 1, "a repeated wake-up must not open a second review")
        self.wait_business(first["workflow_id"], wfm.AWAITING_APPROVAL)
        wf = self.workflow(first["workflow_id"])
        self.assertEqual(len([t for t in wf["tasks"] if t["role"] == "review"]), 1)
        self.assertEqual(len(wf["tasks"]), 2)

    def test_unknown_branch_and_project_checks(self):
        refused = self.start_workflow("x", expect=422, branch="nope")
        self.assertIn("branch", refused["error"]["message"])
        refused = self.start_workflow("x", expect=422, project_id="scratch")
        self.assertIn("worktree project", refused["error"]["message"])


class ExternalPatchSet(WorkflowCase):
    def test_rereview_of_a_patch_set_pushed_outside_the_bridge(self):
        reply, _ = self.developed()
        self.action(reply["workflow_id"], "review", {"instructions": report()})
        self.wait_business(reply["workflow_id"], wfm.AWAITING_APPROVAL)

        # The user amends the change elsewhere and pushes it; the old approval no longer applies.
        other = self.tmp / "elsewhere"
        subprocess.run(["git", "clone", "-q", str(self.origin), str(other)], check=True)
        git(other, "fetch", "-q", "origin", "refs/for/main")
        git(other, "checkout", "-q", "FETCH_HEAD")
        (other / "feature.txt").write_text("by hand\n")
        git(other, "commit", "-q", "-a", "--amend", "-m", "[feat](demo):add feature", "-m", f"Change-Id: {CHANGE_ID}")
        git(other, "push", "-q", "origin", "+HEAD:refs/for/main")
        revision = git(other, "rev-parse", "HEAD")

        self.decide(reply["workflow_id"], "rereview", revision=revision, fetch_ref="refs/for/main")
        wf = self.workflow(reply["workflow_id"])
        self.assertEqual(wf["business_status"], wfm.AWAITING_REVIEW)
        self.assertEqual(wf["change"]["revision"], revision)
        refused = self.decide(reply["workflow_id"], "approve", expect=409)
        self.assertEqual(refused["error"]["code"], "invalid_state")

        review = self.action(reply["workflow_id"], "review", {"instructions": report()})
        wf = self.wait_business(reply["workflow_id"], wfm.AWAITING_APPROVAL)
        self.assertEqual(wf["latest_report"]["revision"], revision)
        self.assertEqual(wf["review_round"], 2)
        self.assertNotEqual(self.runs_of(review["task_id"])[0]["cwd"], str(other))


class NotificationRetry(WorkflowCase):
    def test_adapter_outage_retries_the_event_without_rerunning_tasks(self):
        self.kill("adapter")
        reply = self.start_workflow(f"{commit_and_push()}\n{EVIDENCE}")
        self.wait_business(reply["workflow_id"], wfm.AWAITING_REVIEW)
        time.sleep(1.0)
        self.start("adapter")
        self.wait_port(self.adapter_port)
        texts = self.wait_until(self.wake_texts, message="delivery after the adapter came back")
        self.assertEqual(sum(t.count("开发任务") for t in texts), 1)
        self.assertEqual(self.task(reply["task_id"])["attempt_count"], 1)
        self.assertEqual(len(self.runs_of(reply["task_id"])), 1)


class Rejection(WorkflowCase):
    def test_user_rejects_and_nothing_reaches_the_target(self):
        reply, _ = self.developed()
        self.action(reply["workflow_id"], "review", {"instructions": report()})
        self.wait_business(reply["workflow_id"], wfm.AWAITING_APPROVAL)
        main_before = git(self.origin, "rev-parse", "main")
        rejected = self.decide(reply["workflow_id"], "reject", note="not now")
        self.assertEqual(rejected["business_status"], wfm.REJECTED)
        self.assertEqual(git(self.origin, "rev-parse", "main"), main_before)
        refused = self.action(reply["workflow_id"], "merge-check", expect=409)
        self.assertEqual(refused["error"]["code"], "invalid_state")


class Units(unittest.TestCase):
    def test_push_destinations(self):
        self.assertEqual(wfm.push_destinations("git push origin HEAD:refs/for/master"), ["refs/for/master"])
        self.assertEqual(wfm.push_destinations("git push origin +HEAD:refs/for/v4.2.0%topic=x"), ["refs/for/v4.2.0%topic=x"])
        self.assertEqual(wfm.push_destinations("git push origin HEAD:master"), ["master"])
        self.assertEqual(wfm.push_destinations("git push"), [""])
        self.assertEqual(wfm.push_destinations("git push -f origin master"), ["master"])

    def test_url_and_change_id(self):
        text = "remote: \nremote:   https://gerrit-ai.example/c/cvi_bmtest/+/1234 [feat](x):y [NEW]\n"
        self.assertEqual(wfm.extract_gerrit_url(None, text), ("https://gerrit-ai.example/c/cvi_bmtest/+/1234", 1234))
        self.assertEqual(wfm.change_id_of("t\n\nChange-Id: I" + "b" * 40 + "\n"), "I" + "b" * 40)

    def test_report_rules(self):
        subject = {"revision": "a" * 40, "change_id": CHANGE_ID}
        criteria = [{"id": "AC1", "text": "x"}]
        base = {"subject": {"revision": "a" * 12}, "verdict": "changes_requested", "criteria": [{"id": "AC1", "result": "pass"}]}
        self.assertIn("needs a blocker", wfm.validate_report(base, subject, criteria)[1])
        self.assertIn("missing", wfm.validate_report({**base, "criteria": []}, subject, criteria)[1])
        self.assertIn("revision", wfm.validate_report({**base, "subject": {"revision": "b" * 40}}, subject, criteria)[1])
        ok, reason = wfm.validate_report({**base, "verdict": "approve_recommended"}, subject, criteria)
        self.assertIsNone(reason)
        self.assertEqual(ok["subject"]["revision"], "a" * 40)


if __name__ == "__main__":
    unittest.main()
