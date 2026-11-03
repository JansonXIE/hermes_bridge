"""Command line entry point: `python -m hermes_bridge -c bridge.toml <command>`."""

from __future__ import annotations

import argparse
import json
import logging
import os
import secrets
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

from .adapter import Inbox, run_adapter
from .api import run_api
from .config import ConfigError, load_config
from .provision import ProvisionError, add_projects, init_home, load_env_file
from .store import Store
from .worker import run_worker
from .workspace import WorkspaceError, is_bridge_workdir, remove_workdir


def _run_all(config_path: str) -> int:
    """Development convenience. In production run each role as its own service (see deploy/)."""
    children = {
        role: subprocess.Popen([sys.executable, "-m", "hermes_bridge", "-c", config_path, role])
        for role in ("api", "worker", "adapter")
    }
    stopping = []
    signal.signal(signal.SIGTERM, lambda *_: stopping.append(True))
    signal.signal(signal.SIGINT, lambda *_: stopping.append(True))
    while not stopping and all(p.poll() is None for p in children.values()):
        time.sleep(0.5)
    for proc in children.values():
        if proc.poll() is None:
            proc.terminate()
    for proc in children.values():
        proc.wait()
    return 0 if stopping else 1


def _dead_letters(config) -> int:
    for row in Store(config.db_path).dead_letters():
        print(json.dumps({"queue": "outbox", "event_id": row["event_id"], "task_id": row["task_id"],
                          "attempts": row["attempts"], "last_error": row["last_error"]}, ensure_ascii=False))
    for row in Inbox(config).dead_letters():
        print(json.dumps({"queue": "inbox", "event_id": row["event_id"], "route": row["route"],
                          "attempts": row["attempts"]}, ensure_ascii=False))
    return 0


def _retry_dead(config, event_id: str) -> int:
    found = Store(config.db_path).retry_dead_letter(event_id) or Inbox(config).retry_dead_letter(event_id)
    print("requeued" if found else "not found in dead letters")
    return 0 if found else 1


def _gc(config, days: float, dry_run: bool) -> int:
    """Reclaim disk from finished tasks. Task records and branches are kept."""
    store = Store(config.db_path)
    removed = skipped = 0
    for task in store.collectable_tasks(time.time() - days * 86400):
        if not os.path.isdir(task["workdir"]) or not is_bridge_workdir(config, task["workdir"]):
            continue
        if dry_run:
            print(f"would remove {task['workdir']}")
            continue
        try:
            remove_workdir(config, config.projects.get(task["project_id"]), task)
            if task["run_dir"]:
                for name in ("tmp", "cache"):
                    shutil.rmtree(os.path.join(task["run_dir"], name), ignore_errors=True)
            print(f"removed {task['workdir']}")
            removed += 1
        except (WorkspaceError, OSError) as exc:
            print(f"skipped {task['id']}: {exc}", file=sys.stderr)
            skipped += 1
    print(f"{removed} removed, {skipped} skipped")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="hermes-bridge", description=__doc__)
    parser.add_argument("-c", "--config", default="bridge.toml")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("api", help="run the HTTP API")
    sub.add_parser("worker", help="run a worker and the outbox dispatcher")
    sub.add_parser("adapter", help="run the Hermes receiving adapter")
    sub.add_parser("all", help="run api, worker and adapter together (development)")
    sub.add_parser("check", help="validate the configuration")
    sub.add_parser("dead-letters", help="list notifications that exhausted their retries")
    retry = sub.add_parser("retry-dead", help="requeue a dead-lettered notification")
    retry.add_argument("event_id")
    gc = sub.add_parser("gc", help="remove working directories of tasks that finished long ago")
    gc.add_argument("--days", type=float, default=14.0)
    gc.add_argument("--dry-run", action="store_true")
    sub.add_parser("gen-secret", help="print a random secret")
    init = sub.add_parser("init", help="create bridge.toml and bridge.env next to -c if they are missing")
    init.add_argument("--api-port", type=int, default=8765)
    init.add_argument("--adapter-port", type=int, default=8766)
    init.add_argument("--claude", help="path of the claude CLI (default: found on PATH)")
    add = sub.add_parser("add-projects", help="add git repositories or repo workspaces as projects")
    add.add_argument("paths", nargs="+")
    add.add_argument("--name", help="project id (git repository) or id prefix (repo workspace)")
    add.add_argument("--full-access", action="store_true",
                     help="bypassPermissions for git projects: any tool and shell command, no sandbox")
    add.add_argument("--dry-run", action="store_true")
    sub.add_parser("projects", help="list configured projects")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format=f"%(asctime)s %(levelname)s [{args.command}] %(name)s: %(message)s",
    )
    if args.command == "gen-secret":
        print(secrets.token_urlsafe(32))
        return 0
    config_path = Path(args.config).resolve()
    # Manual commands then work without sourcing the file first; set variables still win.
    load_env_file(config_path.parent / "bridge.env")
    try:
        if args.command == "init":
            for line in init_home(config_path, args.api_port, args.adapter_port, args.claude) or ["nothing to do, files exist"]:
                print(line)
            return 0
        if args.command == "add-projects":
            added, notes = add_projects(config_path, args.paths, args.name, args.full_access, args.dry_run)
            for note in notes:
                print(note)
            for p in added:
                print(f"{'would add' if args.dry_run else 'added'} {p.id:40} {p.isolation:9} {p.path}")
            print(f"{len(added)} project(s) {'to add' if args.dry_run else 'added'}")
            return 0
    except ProvisionError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    try:
        config = load_config(args.config)
    except (ConfigError, OSError, KeyError, ValueError) as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2

    os.umask(0o077)
    if args.command == "api":
        run_api(config)
    elif args.command == "worker":
        run_worker(config)
    elif args.command == "adapter":
        run_adapter(config)
    elif args.command == "all":
        return _run_all(args.config)
    elif args.command == "check":
        print(f"ok: {len(config.projects)} project(s), data_dir={config.data_dir}")
    elif args.command == "projects":
        for p in config.projects.values():
            print(f"{p.id:45} {p.isolation:9} {p.permission_mode:17} {p.path or ''}")
    elif args.command == "dead-letters":
        return _dead_letters(config)
    elif args.command == "retry-dead":
        return _retry_dead(config, args.event_id)
    elif args.command == "gc":
        return _gc(config, args.days, args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
