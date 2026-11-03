"""Per-task working directories: one git worktree and branch per task.

Bridge-made commits run the repository's hooks like any other commit. If a hook
rejects a snapshot, the failure is reported on the task instead of being bypassed.
"""

from __future__ import annotations

import fcntl
import os
import shutil
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

from .config import Config, Project

# Bridge commits land on the task's private branch only, so they carry a bot identity.
_GIT_IDENTITY = ["-c", "user.name=hermes-bridge", "-c", "user.email=hermes-bridge@localhost"]
_MAX_LISTED_FILES = 200


class WorkspaceError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class Workspace:
    workdir: Path
    branch: Optional[str]
    base_commit: Optional[str]


def _git(args: list[str], cwd: Path, check: bool = True) -> subprocess.CompletedProcess:
    try:
        result = subprocess.run(
            ["git", *args], cwd=cwd, capture_output=True, text=True, errors="replace", stdin=subprocess.DEVNULL,
        )
    except OSError as exc:
        raise WorkspaceError("project_unavailable", f"cannot run git in {cwd}: {exc}") from exc
    if check and result.returncode != 0:
        raise WorkspaceError("git_error", f"git {args[0]} failed: {result.stderr.strip()[:500]}")
    return result


@contextmanager
def _project_lock(config: Config, project_id: str) -> Iterator[None]:
    """Serialize worktree bookkeeping across workers; git's own locks fail rather than wait."""
    config.locks_dir.mkdir(parents=True, exist_ok=True)
    with open(config.locks_dir / f"{project_id}.lock", "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield


def _commit_message(project: Project, action: str, task_id: str) -> str:
    return project.commit_message.replace("{action}", action).replace("{task_id}", task_id)


def branch_name(task_id: str) -> str:
    return f"bridge/{task_id}"


def prepare(config: Config, project: Project, task: dict, dependency_branches: list[str]) -> Workspace:
    """Create the task's directory, or reuse it on retry. Safe to call repeatedly."""
    workdir = config.workspaces_dir / project.id / task["id"]
    if project.isolation == "directory":
        workdir.mkdir(parents=True, exist_ok=True)
        return Workspace(workdir, None, None)

    repo = project.path
    if not repo or not repo.is_dir():
        raise WorkspaceError("project_unavailable", f"project path does not exist: {repo}")
    if project.isolation == "inplace":
        return Workspace(repo, None, None)
    if _git(["rev-parse", "--is-inside-work-tree"], repo, check=False).returncode != 0:
        raise WorkspaceError("project_unavailable", f"not a git repository: {repo}")

    branch = branch_name(task["id"])
    with _project_lock(config, project.id):
        if workdir.is_dir() and _git(["rev-parse", "--git-dir"], workdir, check=False).returncode == 0:
            return Workspace(workdir, branch, task.get("base_commit") or _git(["rev-parse", "HEAD"], workdir).stdout.strip())
        base = _git(["rev-parse", "--verify", f"{project.base_ref}^{{commit}}"], repo).stdout.strip()
        workdir.parent.mkdir(parents=True, exist_ok=True)
        _git(["worktree", "add", "-q", "-b", branch, str(workdir), base], repo)

    # Integration tasks merge inside Claude so that it can resolve conflicts itself.
    if task["kind"] != "integration":
        for dep_branch in dependency_branches:
            message = _commit_message(project, f"merge {dep_branch}", task["id"])
            merged = _git([*_GIT_IDENTITY, "merge", "--no-edit", "-m", message, dep_branch], workdir, check=False)
            if merged.returncode != 0:
                _git(["merge", "--abort"], workdir, check=False)
                detail = (merged.stdout + merged.stderr).strip()[-500:]
                raise WorkspaceError("dependency_merge_failed", f"cannot merge {dep_branch}: {detail}")
    return Workspace(workdir, branch, base)


def collect_changes(workspace_dir: str, branch: Optional[str], base_commit: Optional[str], project: Project, task_id: str) -> Optional[dict]:
    """Describe what the task changed, snapshotting uncommitted edits onto its private branch."""
    workdir = Path(workspace_dir)
    if not branch or not workdir.is_dir():
        return None
    uncommitted = _dirty_files(workdir)
    snapshot_error = None
    if uncommitted and project.auto_commit:
        _git(["add", "-A"], workdir)
        message = _commit_message(project, "snapshot changes", task_id)
        committed = _git([*_GIT_IDENTITY, "commit", "-q", "-m", message], workdir, check=False)
        if committed.returncode != 0:
            snapshot_error = (committed.stdout + committed.stderr).strip()[-500:]
        uncommitted = _dirty_files(workdir)

    head = _git(["rev-parse", "HEAD"], workdir).stdout.strip()
    span = f"{base_commit}..{head}" if base_commit else head
    files = _git(["diff", "--name-only", span], workdir).stdout.splitlines()
    changes = {
        "branch": branch,
        "base_commit": base_commit,
        "head_commit": head,
        "commits": int(_git(["rev-list", "--count", span], workdir).stdout.strip() or 0),
        "files": files[:_MAX_LISTED_FILES],
        "files_total": len(files),
        "diffstat": _git(["diff", "--shortstat", span], workdir).stdout.strip(),
        "uncommitted": bool(uncommitted),
        "uncommitted_files": uncommitted[:_MAX_LISTED_FILES],
    }
    if snapshot_error:
        changes["snapshot_error"] = snapshot_error
    return changes


def _dirty_files(workdir: Path) -> list[str]:
    return [line[3:] for line in _git(["status", "--porcelain"], workdir).stdout.splitlines()]


def is_bridge_workdir(config: Config, workdir: str | Path) -> bool:
    """Only directories the bridge created may be deleted; inplace tasks run in the live checkout."""
    return Path(workdir).resolve().is_relative_to(config.workspaces_dir.resolve())


def remove_workdir(config: Config, project: Optional[Project], task: dict) -> None:
    """Delete a finished task's working directory. Its branch stays, so committed work is kept."""
    workdir = Path(task["workdir"])
    if not workdir.is_dir():
        return
    if not is_bridge_workdir(config, workdir):
        raise WorkspaceError("not_owned", f"{workdir} is not a bridge workspace (inplace project)")
    if not task["branch"] or project is None or project.path is None:
        shutil.rmtree(workdir)
        return
    if _dirty_files(workdir):
        raise WorkspaceError("worktree_dirty", f"uncommitted changes in {workdir}")
    with _project_lock(config, project.id):
        _git(["worktree", "remove", str(workdir)], project.path)


def private_dirs(run_dir: Path) -> dict[str, str]:
    """Per-task scratch locations exported to the child, so parallel tasks do not share caches."""
    tmp, cache = run_dir / "tmp", run_dir / "cache"
    for path in (tmp, cache):
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    return {"TMPDIR": str(tmp), "XDG_CACHE_HOME": str(cache)}


def ensure_private(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)
