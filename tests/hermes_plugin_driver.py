"""Runs inside Hermes' own venv: drives the plugin through the real PluginManager and tool registry.

Reads commands as JSON lines on stdin and answers on stdout. The gateway's injector is a
recorder, so the real grant check and inject_message() run but no platform is involved.
"""
import json
import sys
import threading


class FakeCli:
    def __init__(self):
        import queue
        self._agent_running = True
        self._pending_input = queue.Queue()
        self._interrupt_queue = queue.Queue()


def main():
    from gateway.session_context import set_session_vars
    from hermes_cli.plugins import discover_plugins, get_plugin_manager
    from tools.registry import registry

    discover_plugins()
    manager = get_plugin_manager()
    injected = []
    lock = threading.Lock()

    def injector(**kwargs):
        with lock:
            injected.append(kwargs)
        return True

    cli = None
    for line in sys.stdin:
        command = json.loads(line)
        op = command["op"]
        if op == "tools":
            reply = sorted(n for n in registry.get_all_tool_names() if n.startswith("claude_task"))
        elif op == "gateway_on":
            manager.set_gateway_message_injector(manager, injector)
            reply = True
        elif op == "tool":
            set_session_vars(session_key=command.get("session_key", ""))
            reply = json.loads(registry.dispatch(command["name"], command["args"], session_id=command.get("session_id", "")))
        elif op == "injected":
            with lock:
                reply = list(injected)
        elif op == "cli_on":
            cli = FakeCli()
            manager._cli_ref = cli
            reply = True
        elif op == "cli_idle":
            cli._agent_running = False
            reply = True
        elif op == "cli_queues":
            reply = {"pending": cli._pending_input.qsize(), "interrupt": cli._interrupt_queue.qsize(),
                     "text": cli._pending_input.queue[0] if cli._pending_input.qsize() else None}
        else:
            reply = {"error": f"unknown op {op}"}
        print(json.dumps(reply), flush=True)


if __name__ == "__main__":
    main()
