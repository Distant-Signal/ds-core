"""Tests for scripts/check-migration-order.py, run against throwaway git repos.

  uv run python -m unittest discover -s scripts/tests

Each test builds a repo whose first commit is BASE, changes
crates/api/migrations in a second commit, and runs the script (as CI does:
from the repo root, with BASE as its argument).
"""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import override

SCRIPT = Path(__file__).resolve().parent.parent / "check-migration-order.py"
MIGRATIONS = "crates/api/migrations"
OLD = f"{MIGRATIONS}/20260901000000_old.sql"
NEWEST = f"{MIGRATIONS}/20260927120200_newest.sql"
# A fixed identity and no user/system config, so the host's git setup
# (signing, hooks, default branch) cannot leak into the throwaway repos.
GIT_ENV = {
    **os.environ,
    "GIT_CONFIG_GLOBAL": os.devnull,
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_AUTHOR_NAME": "test",
    "GIT_AUTHOR_EMAIL": "test@example.invalid",
    "GIT_COMMITTER_NAME": "test",
    "GIT_COMMITTER_EMAIL": "test@example.invalid",
}


class CheckMigrationOrderTest(unittest.TestCase):
    """Behaviour of the check over BASE..HEAD."""

    @override
    def setUp(self) -> None:
        """Create an empty repo in a temporary directory."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.repo = Path(tmp.name)
        self.git("init", "-q")

    def git(self, *args: str) -> str:
        """Run git in the repo; return its stdout."""
        return subprocess.run(  # noqa: S603  # fixed git subcommands
            ["git", *args],  # noqa: S607  # git from PATH
            check=True,
            capture_output=True,
            text=True,
            cwd=self.repo,
            env=GIT_ENV,
        ).stdout.strip()

    def write(self, path: str, text: str = "SELECT 1;\n") -> None:
        """Create or overwrite a file in the repo."""
        target = self.repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")

    def commit(self) -> str:
        """Commit everything (empty commits allowed); return the commit id."""
        self.git("add", "-A")
        self.git("commit", "-q", "--allow-empty", "-m", "change")
        return self.git("rev-parse", "HEAD")

    def base_with(self, *paths: str) -> str:
        """Commit BASE holding the given files; return its id."""
        for path in paths:
            self.write(path)
        return self.commit()

    def check(self, base: str) -> tuple[int, str]:
        """Run the script from the repo root; return (status, stdout)."""
        result = subprocess.run(  # noqa: S603  # this repo's script
            [sys.executable, str(SCRIPT), base],
            check=False,
            capture_output=True,
            text=True,
            cwd=self.repo,
            env=GIT_ENV,
        )
        return result.returncode, result.stdout

    def test_no_migrations_at_base(self) -> None:
        """A base without migrations has nothing to compare against."""
        base = self.base_with("README")
        self.write(OLD)
        self.commit()
        self.assertEqual(
            self.check(base), (0, f"no migrations at {base}; nothing to compare\n")
        )

    def test_nothing_changed(self) -> None:
        """No migration changes passes and says so twice."""
        base = self.base_with(OLD, NEWEST)
        self.write("README")
        self.commit()
        status, out = self.check(base)
        self.assertEqual(status, 0)
        self.assertEqual(
            out.splitlines(),
            [
                f"no migrations added since {base} (newest there: 20260927120200)",
                f"no existing migrations modified, deleted or renamed since {base}",
            ],
        )

    def test_added_newer_passes(self) -> None:
        """A migration newer than every base migration is fine."""
        base = self.base_with(OLD, NEWEST)
        added = f"{MIGRATIONS}/20260927120300_next.sql"
        self.write(added)
        self.commit()
        status, out = self.check(base)
        self.assertEqual(status, 0)
        self.assertEqual(
            out.splitlines(),
            [
                f"ok: {added} (20260927120300 > 20260927120200)",
                f"no existing migrations modified, deleted or renamed since {base}",
            ],
        )

    def test_added_older_or_equal_fails(self) -> None:
        """A migration not newer than the base's newest fails, even if equal."""
        base = self.base_with(OLD, NEWEST)
        older = f"{MIGRATIONS}/20260927120100_late.sql"
        equal = f"{MIGRATIONS}/20260927120200_same.sql"
        self.write(older)
        self.write(equal)
        self.commit()
        status, out = self.check(base)
        self.assertEqual(status, 1)
        for path, version in ((older, "20260927120100"), (equal, "20260927120200")):
            self.assertIn(
                f"::error file={path}::{path} (version {version}) is not newer than "
                "the newest migration on the base (20260927120200); sqlx would "
                "apply it out of order. Give it a later timestamp.\n",
                out,
            )

    def test_versions_compare_numerically(self) -> None:
        """Versions of different lengths compare as numbers, not strings."""
        base = self.base_with(f"{MIGRATIONS}/9_short.sql")
        added = f"{MIGRATIONS}/10_longer.sql"
        self.write(added)
        self.commit()
        status, out = self.check(base)
        self.assertEqual(status, 0)
        self.assertIn(f"ok: {added} (10 > 9)\n", out)

    def test_added_without_version_fails(self) -> None:
        """An added .sql file without a numeric version prefix fails."""
        base = self.base_with(NEWEST)
        added = f"{MIGRATIONS}/no_version.sql"
        self.write(added)
        self.commit()
        status, out = self.check(base)
        self.assertEqual(status, 1)
        self.assertIn(
            f"::error file={added}::cannot read a numeric version from {added}\n", out
        )

    def test_modified_fails(self) -> None:
        """Editing a migration the base already had fails."""
        base = self.base_with(OLD, NEWEST)
        self.write(OLD, "SELECT 2;\n")
        self.commit()
        status, out = self.check(base)
        self.assertEqual(status, 1)
        self.assertIn(
            f"::error file={OLD}::{OLD} already exists on the base and was modified. "
            "Merged migrations are immutable: sqlx checks every applied file's "
            "checksum at startup. Add a new migration instead.\n",
            out,
        )
        self.assertIn(
            f"no migrations added since {base} (newest there: 20260927120200)\n", out
        )

    def test_deleted_fails(self) -> None:
        """Deleting a migration the base already had fails."""
        base = self.base_with(OLD, NEWEST)
        (self.repo / OLD).unlink()
        self.commit()
        status, out = self.check(base)
        self.assertEqual(status, 1)
        self.assertIn(
            f"::error file={OLD}::{OLD} already exists on the base and was deleted "
            "(or renamed).",
            out,
        )

    def test_renamed_fails_as_delete_plus_add(self) -> None:
        """A rename is a deletion of the old name plus a check of the new one."""
        base = self.base_with(OLD, NEWEST)
        renamed = f"{MIGRATIONS}/20260927120300_old.sql"
        self.git("mv", OLD, renamed)
        self.commit()
        status, out = self.check(base)
        self.assertEqual(status, 1)
        self.assertIn(f"::error file={OLD}::{OLD} already exists", out)
        self.assertIn(f"ok: {renamed} (20260927120300 > 20260927120200)\n", out)

    def test_non_sql_files_ignored(self) -> None:
        """Only .sql files under the migrations directory count."""
        base = self.base_with(OLD, NEWEST, f"{MIGRATIONS}/README.md")
        self.write(f"{MIGRATIONS}/README.md", "changed\n")
        self.write(f"{MIGRATIONS}/notes.txt")
        self.write("other/20200101000000_elsewhere.sql")
        self.commit()
        status, _ = self.check(base)
        self.assertEqual(status, 0)

    def test_unknown_base_fails_with_git_status(self) -> None:
        """An unknown BASE fails with git's own exit status."""
        self.base_with(OLD)
        status, out = self.check("0" * 40)
        self.assertNotEqual(status, 0)
        self.assertEqual(out, "")


if __name__ == "__main__":
    unittest.main()
