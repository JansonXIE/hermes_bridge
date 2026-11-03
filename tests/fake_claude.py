#!/usr/bin/env python3
"""Deterministic stand-in for `claude -p --output-format stream-json`, same event shapes.

The prompt arrives on stdin, as with the real CLI. Directive lines drive behaviour:
  SLEEP=<s>        run for that long before finishing
  WRITE=<path>     write a file in the working directory
  EXIT=<code>      exit with this code
  NO_RESULT        exit without a final result event
  RESULT_ERROR     final result event reports an error
  CHILD            start a long-lived grandchild process
  IGNORE_TERM      ignore SIGTERM
  MERGE=<branch>   git merge a branch
  SECRET=<value>   echo a value in assistant text and in the result
  BIGLINE=<bytes>  emit one stream line of about that size
Every start is appended to $FAKE_CLAUDE_LEDGER as one JSON line.
"""
import json
import os
import signal
import subprocess
import sys
import time
import uuid


def emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def main():
    prompt = sys.stdin.read()
    directives = {}
    for line in prompt.splitlines():
        key, _, value = line.strip().partition("=")
        if key.isupper():
            directives[key] = value

    if "--resume" in sys.argv:
        session_id = sys.argv[sys.argv.index("--resume") + 1]
    else:
        session_id = str(uuid.uuid4())

    with open(os.environ["FAKE_CLAUDE_LEDGER"], "a") as ledger:
        ledger.write(json.dumps({
            "event": "start", "task": os.environ.get("BRIDGE_TASK_ID"), "pid": os.getpid(), "cwd": os.getcwd(),
            "at": time.time(), "argv": sys.argv[1:], "prompt": prompt, "session": session_id,
            "port_base": os.environ.get("BRIDGE_PORT_BASE"), "tmpdir": os.environ.get("TMPDIR"),
            "leaked": sorted(k for k in os.environ if k in ("CLAUDECODE", "BRIDGE_API_TOKEN", "UNRELATED_SECRET")),
        }) + "\n")

    if "IGNORE_TERM" in directives:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

    emit({"type": "system", "subtype": "init", "session_id": session_id, "model": "fake", "tools": ["Read"],
          "permissionMode": "dontAsk", "claude_code_version": "fake", "cwd": os.getcwd()})
    emit({"type": "stream_event", "session_id": session_id, "event": {"type": "message_start"}})
    emit({"type": "assistant", "session_id": session_id, "parent_tool_use_id": None, "message": {"content": [
        {"type": "thinking", "thinking": "PRIVATE-THOUGHT"},
        {"type": "text", "text": "working " + directives.get("SECRET", "")},
    ]}})

    if "BIGLINE" in directives:
        emit({"type": "assistant", "session_id": session_id, "parent_tool_use_id": None, "message": {"content": [
            {"type": "text", "text": "y" * int(directives["BIGLINE"])},
        ]}})

    if "CHILD" in directives:
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
        with open(os.environ["FAKE_CLAUDE_LEDGER"], "a") as ledger:
            ledger.write(json.dumps({"event": "child", "task": os.environ.get("BRIDGE_TASK_ID"), "pid": child.pid}) + "\n")

    if "WRITE" in directives:
        path = directives["WRITE"]
        emit({"type": "assistant", "session_id": session_id, "parent_tool_use_id": None, "message": {"content": [
            {"type": "tool_use", "id": "tu_1", "name": "Write", "input": {"file_path": path, "content": "x"}},
        ]}})
        with open(path, "w") as f:
            f.write(f"written by {os.environ.get('BRIDGE_TASK_ID')}\n")
        emit({"type": "user", "session_id": session_id, "parent_tool_use_id": None, "message": {"content": [
            {"type": "tool_result", "tool_use_id": "tu_1", "content": "ok", "is_error": False},
        ]}})

    if "MERGE" in directives:
        for branch in directives["MERGE"].split(","):
            message = "[chore](test): merge\n\n[description ]:merge for test\n[root   cause]:NA\n"
            subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", "merge", "--no-edit", "-m", message, branch],
                           check=True, stdout=subprocess.DEVNULL)

    deadline = time.time() + float(directives.get("SLEEP", 0))
    while time.time() < deadline:
        emit({"type": "stream_event", "session_id": session_id, "event": {"type": "content_block_delta"}})
        time.sleep(0.2)

    print("a warning on stderr", file=sys.stderr, flush=True)
    if "NO_RESULT" in directives:
        sys.exit(int(directives.get("EXIT", 0)))
    is_error = "RESULT_ERROR" in directives
    emit({"type": "result", "subtype": "error_during_execution" if is_error else "success", "is_error": is_error,
          "result": "done " + directives.get("SECRET", ""), "session_id": session_id, "num_turns": 1,
          "duration_ms": 5, "total_cost_usd": 0.01, "errors": ["boom"] if is_error else [], "permission_denials": []})
    sys.exit(int(directives.get("EXIT", 0)))


if __name__ == "__main__":
    main()
