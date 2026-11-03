"""Linux process identity and process-tree control via /proc.

A PID alone is not an identity: it can be reused. An attempt's process is
identified by (pid, kernel start time, boot id), and every descendant inherits
a unique marker environment variable so the tree can be found after a restart.
"""

from __future__ import annotations

import os
import signal
from dataclasses import dataclass
from typing import Iterable, Optional

MARKER_ENV = "BRIDGE_ATTEMPT_ID"


@dataclass(frozen=True)
class ProcStat:
    state: str
    pgrp: int
    start_time: int


def read_stat(pid: int) -> Optional[ProcStat]:
    try:
        with open(f"/proc/{pid}/stat", "rb") as f:
            data = f.read()
    except OSError:
        return None
    # comm may contain spaces and parentheses; fields resume after the last ')'.
    fields = data[data.rfind(b")") + 2 :].split()
    return ProcStat(state=fields[0].decode(), pgrp=int(fields[2]), start_time=int(fields[19]))


def boot_id() -> str:
    with open("/proc/sys/kernel/random/boot_id") as f:
        return f.read().strip()


def is_alive(pid: Optional[int], start_time: Optional[int], boot: Optional[str]) -> bool:
    """True if the exact process recorded earlier still runs (zombies count as dead)."""
    if not pid or start_time is None or boot != boot_id():
        return False
    stat = read_stat(pid)
    return stat is not None and stat.start_time == start_time and stat.state not in ("Z", "X")


def _all_pids() -> Iterable[int]:
    for name in os.listdir("/proc"):
        if name.isdigit():
            yield int(name)


def find_marked(marker: str) -> list[int]:
    needle = f"{MARKER_ENV}={marker}".encode()
    found = []
    for pid in _all_pids():
        try:
            with open(f"/proc/{pid}/environ", "rb") as f:
                environ = f.read()
        except OSError:
            continue
        if needle in environ.split(b"\0"):
            stat = read_stat(pid)
            if stat and stat.state not in ("Z", "X"):
                found.append(pid)
    return found


def tree_members(marker: str) -> list[int]:
    """Live processes of an attempt: marker holders plus members of their process groups."""
    marked = find_marked(marker)
    groups = {s.pgrp for s in map(read_stat, marked) if s}
    members = set(marked)
    if groups:
        for pid in _all_pids():
            stat = read_stat(pid)
            if stat and stat.pgrp in groups and stat.state not in ("Z", "X"):
                members.add(pid)
    members.discard(os.getpid())
    return sorted(members)


def signal_pids(pids: Iterable[int], sig: signal.Signals) -> None:
    for pid in pids:
        try:
            os.kill(pid, sig)
        except (ProcessLookupError, PermissionError):
            pass
