"""init and add-projects: the configuration half of deploy/setup.sh."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path

from .harness import COMMIT_MESSAGE, ROOT, git


def make_repo(path: Path) -> Path:
    path.mkdir(parents=True)
    git(path, "init", "-q")
    (path / "README").write_text("x\n")
    git(path, "add", ".")
    git(path, "commit", "-q", "-m", COMMIT_MESSAGE)
    return path


class Provision(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="bridge-provision-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.config = self.tmp / "home" / "bridge.toml"
        self.env = {k: v for k, v in os.environ.items() if not k.startswith("BRIDGE_")}

    def cli(self, *args, ok=True):
        result = subprocess.run(
            [sys.executable, "-m", "hermes_bridge", "-c", str(self.config), *args],
            cwd=ROOT, env=self.env, capture_output=True, text=True,
        )
        if ok:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def projects(self):
        return tomllib.loads(self.config.read_text())["projects"]

    def test_init_creates_private_secrets_once_and_check_reads_them_without_sourcing(self):
        self.cli("init", "--claude", sys.executable)
        env_file = self.config.parent / "bridge.env"
        self.assertEqual(oct(env_file.stat().st_mode & 0o777), "0o600")
        first = env_file.read_text()
        self.assertEqual(len(first.splitlines()), 3)
        self.cli("init", "--claude", sys.executable)
        self.assertEqual(env_file.read_text(), first, "secrets are never regenerated")
        self.assertIn("ok: 0 project(s)", self.cli("check").stdout)

    def test_git_repository_and_repo_workspace(self):
        self.cli("init", "--claude", sys.executable)
        single = make_repo(self.tmp / "tools")
        ws = self.tmp / "sdk"
        make_repo(ws / "linux_5.10")
        make_repo(ws / "oss" / "zlib")
        (ws / "missing").mkdir()
        (ws / ".repo").mkdir()
        (ws / ".repo" / "project.list").write_text("linux_5.10\noss/zlib\nmissing\n")

        self.cli("add-projects", str(single), str(ws))
        projects = self.projects()
        self.assertEqual(sorted(projects), ["sdk", "sdk-linux_5.10", "sdk-oss-zlib", "tools"])
        self.assertEqual(projects["sdk"]["isolation"], "inplace")
        self.assertEqual(projects["sdk-oss-zlib"]["path"], str((ws / "oss" / "zlib").resolve()))
        self.assertEqual(projects["tools"]["permission_mode"], "dontAsk")
        self.assertIn(f"Read(/{self.config.parent.resolve()}/bridge.env)", projects["tools"]["disallowed_tools"])

        again = self.cli("add-projects", str(ws), str(single)).stdout
        self.assertIn("0 project(s) added", again, "re-running adds nothing")
        self.assertIn("ok: 4 project(s)", self.cli("check").stdout)

    def test_full_access_names_and_collisions(self):
        self.cli("init", "--claude", sys.executable)
        self.config.write_text(self.config.read_text() + "\n# kept comment\n")
        self.cli("add-projects", "--full-access", "--name", "fw", str(make_repo(self.tmp / "a" / "firmware")))
        project = self.projects()["fw"]
        self.assertEqual(project["permission_mode"], "bypassPermissions")
        self.assertTrue(project["allow_bypass_permissions"])
        self.assertIn("# kept comment", self.config.read_text())

        clash = self.cli("add-projects", "--name", "fw", str(make_repo(self.tmp / "b" / "firmware")), ok=False)
        self.assertEqual(clash.returncode, 2)
        self.assertIn("is taken", clash.stderr)
        self.assertEqual(sorted(self.projects()), ["fw"], "a failed run changes nothing")

        preview = self.cli("add-projects", "--dry-run", str(self.tmp / "b" / "firmware")).stdout
        self.assertIn("would add firmware", preview)
        self.assertEqual(sorted(self.projects()), ["fw"])

        bad = self.cli("add-projects", str(self.tmp), ok=False)
        self.assertIn("neither a git repository", bad.stderr)


if __name__ == "__main__":
    unittest.main()
