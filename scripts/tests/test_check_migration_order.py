"""Tests for scripts/check-migration-order.py.

  uv run python -m unittest discover -s scripts/tests

CheckMigrationOrderTest builds throwaway repos whose first commit is BASE,
changes crates/ds-store/migrations in a second commit, and runs the script (as
CI does: from the repo root, with BASE as its argument).
DestructiveDdlTest runs the destructive-DDL scanner over the fixture
migrations in fixtures/migration-contract/{fails,passes}.
"""

import importlib.util
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from typing import override

SCRIPT = Path(__file__).resolve().parent.parent / "check-migration-order.py"
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "migration-contract"
REPO_MIGRATIONS = (
    Path(__file__).resolve().parents[2] / "crates" / "ds-store" / "migrations"
)
MIGRATIONS = "crates/ds-store/migrations"
# The directory before plan task 1B.1 moved it.
API_MIGRATIONS = "crates/api/migrations"
OLD = f"{MIGRATIONS}/20260901000000_old.sql"
NEWEST = f"{MIGRATIONS}/20260927120200_newest.sql"
NO_CONTRACT_FINDINGS = (
    "no destructive DDL without a contract header in {} checked migration(s) "
    "(added since BASE, or newer than 20261007210000)"
)
DROP = "DROP TABLE old_things;\n"
HEADER = "-- contract: drop old_things (code stopped using it in 0123abc)\n"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("check_migration_order", SCRIPT)
    if spec is None or spec.loader is None:
        msg = f"cannot load {SCRIPT}"
        raise ImportError(msg)
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_migration_order"] = module
    spec.loader.exec_module(module)
    return module


cmo = _load()
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
                NO_CONTRACT_FINDINGS.format(0),
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
                NO_CONTRACT_FINDINGS.format(1),
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

    def move_from_the_api(self) -> str:
        """Commit BASE with OLD and NEWEST in the api's directory, then move them."""
        base = self.base_with(
            OLD.replace(MIGRATIONS, API_MIGRATIONS),
            NEWEST.replace(MIGRATIONS, API_MIGRATIONS),
        )
        (self.repo / MIGRATIONS).parent.mkdir(parents=True)
        self.git("mv", API_MIGRATIONS, MIGRATIONS)
        return base

    def test_the_move_to_ds_store_is_no_finding(self) -> None:
        """Plan 1B.1's rename from the api's directory passes."""
        base = self.move_from_the_api()
        self.commit()
        status, out = self.check(base)
        self.assertEqual(status, 0, out)
        self.assertEqual(
            out.splitlines(),
            [
                f"no migrations added since {base} (newest there: 20260927120200)",
                f"no existing migrations modified, deleted or renamed since {base}",
                NO_CONTRACT_FINDINGS.format(0),
            ],
        )

    def test_the_move_is_checked_file_by_file(self) -> None:
        """Across the move, an edit, a deletion and an old version still fail."""
        base = self.move_from_the_api()
        self.write(NEWEST, "SELECT 2;\n")
        (self.repo / OLD).unlink()
        late = f"{MIGRATIONS}/20260927120100_late.sql"
        self.write(late)
        self.commit()
        status, out = self.check(base)
        self.assertEqual(status, 1)
        old = OLD.replace(MIGRATIONS, API_MIGRATIONS)
        self.assertIn(f"::error file={old}::{old} already exists on the base", out)
        self.assertIn(
            f"::error file={NEWEST}::{NEWEST} already exists on the base and was "
            "modified.",
            out,
        )
        self.assertIn(f"::error file={late}::{late} (version 20260927120100)", out)

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

    def test_added_destructive_without_header_fails(self) -> None:
        """An added migration with destructive DDL and no header fails."""
        base = self.base_with(OLD, NEWEST)
        added = f"{MIGRATIONS}/20260927120300_drop.sql"
        self.write(added, f"-- Drop it.\n\n{DROP}")
        self.commit()
        status, out = self.check(base)
        self.assertEqual(status, 1)
        self.assertIn(f"ok: {added} (20260927120300 > 20260927120200)\n", out)
        self.assertIn(
            f"::error file={added},line=3::{added}:3: DROP TABLE old_things is "
            "destructive DDL. Add a `-- contract: <what> (code stopped using it in "
            "<commit>)` header",
            out,
        )
        self.assertNotIn("no destructive DDL", out)

    def test_added_destructive_with_header_passes(self) -> None:
        """The contract header makes the same migration pass."""
        base = self.base_with(OLD, NEWEST)
        self.write(f"{MIGRATIONS}/20260927120300_drop.sql", f"{HEADER}{DROP}")
        self.commit()
        status, out = self.check(base)
        self.assertEqual(status, 0)
        self.assertIn(NO_CONTRACT_FINDINGS.format(1), out)

    def test_grandfathered_destructive_migration_passes(self) -> None:
        """An unchanged base migration at or below the cutoff is not scanned."""
        old_drop = f"{MIGRATIONS}/{cmo.CONTRACT_CHECK_CUTOFF}_old_drop.sql"
        self.write(old_drop, DROP)
        base = self.base_with(OLD)
        self.write("README")
        self.commit()
        status, out = self.check(base)
        self.assertEqual(status, 0)
        self.assertIn(NO_CONTRACT_FINDINGS.format(0), out)

    def test_unchanged_migration_after_cutoff_is_scanned(self) -> None:
        """A base migration newer than the cutoff is scanned even if unchanged."""
        newer = f"{MIGRATIONS}/{cmo.CONTRACT_CHECK_CUTOFF + 1}_drop.sql"
        self.write(newer, DROP)
        base = self.base_with(OLD)
        self.write("README")
        self.commit()
        status, out = self.check(base)
        self.assertEqual(status, 1)
        self.assertIn(f"::error file={newer},line=1::", out)


class DestructiveDdlTest(unittest.TestCase):
    """The destructive-DDL scanner, over the fixture migrations."""

    def findings(self, kind: str, name: str) -> list[tuple[int, str]]:
        """Return the scanner's (line, description) list for one fixture."""
        sql = (FIXTURES / kind / name).read_text(encoding="utf-8")
        return list(cmo.destructive_statements(sql))

    def test_every_fixture_is_covered(self) -> None:
        """Each fixture file has a test below (a new one must get one)."""
        names = {f"{p.parent.name}/{p.name}" for p in FIXTURES.glob("*/*.sql")}
        self.assertEqual(
            names,
            {
                "fails/alters.sql",
                "fails/do-block.sql",
                "fails/drops.sql",
                "fails/header-too-late.sql",
                "passes/additive.sql",
                "passes/contract-header.sql",
                "passes/no-transaction-header.sql",
                "passes/same-file.sql",
            },
        )

    def test_fails_fixtures_have_no_header(self) -> None:
        """Every failing fixture lacks a (leading) contract header."""
        for path in (FIXTURES / "fails").glob("*.sql"):
            with self.subTest(path=path.name):
                sql = path.read_text(encoding="utf-8")
                self.assertFalse(cmo.has_contract_header(sql))
                self.assertTrue(cmo.contract_findings(path.name, sql))

    def test_passes_fixtures_pass(self) -> None:
        """Every passing fixture yields no ::error line."""
        for path in (FIXTURES / "passes").glob("*.sql"):
            with self.subTest(path=path.name):
                sql = path.read_text(encoding="utf-8")
                self.assertEqual(cmo.contract_findings(path.name, sql), [])

    def test_drops(self) -> None:
        """DROP of each object kind, and column drops with or without COLUMN."""
        self.assertEqual(
            self.findings("fails", "drops.sql"),
            [
                (4, "DROP TABLE old_things"),
                (5, "DROP VIEW legacy_view"),
                (6, "DROP MATERIALIZED VIEW legacy_stats"),
                (7, "DROP FUNCTION legacy_fn"),
                (10, "ALTER TABLE trains DROP COLUMN legacy_uid"),
                (11, "ALTER TABLE trains DROP COLUMN legacy_headcode"),
                (12, "ALTER TABLE quoted DROP COLUMN gone"),
            ],
        )

    def test_alters(self) -> None:
        """Renames, type changes and SET NOT NULL; SET DEFAULT is fine."""
        self.assertEqual(
            self.findings("fails", "alters.sql"),
            [
                (4, "ALTER TABLE stations ... RENAME"),
                (5, "ALTER TABLE stops ... RENAME"),
                (6, "ALTER INDEX stations_crs ... RENAME"),
                (7, "ALTER TABLE stops ALTER COLUMN tiploc TYPE"),
                (10, "ALTER TABLE stops ALTER COLUMN name SET NOT NULL"),
                (11, "ALTER TABLE stops ALTER COLUMN lat TYPE"),
            ],
        )

    def test_do_block_body_is_scanned(self) -> None:
        """DDL inside a DO block (after IF ... THEN) is found, on its line."""
        self.assertEqual(
            self.findings("fails", "do-block.sql"), [(5, "DROP TABLE old_things")]
        )

    def test_header_after_a_statement_does_not_count(self) -> None:
        """A contract line after the first statement is just a comment."""
        self.assertEqual(
            self.findings("fails", "header-too-late.sql"),
            [(4, "DROP TABLE old_things")],
        )

    def test_additive_ddl_comments_strings_and_function_bodies(self) -> None:
        """Additive DDL, comments, literals and function bodies are clean."""
        self.assertEqual(self.findings("passes", "additive.sql"), [])

    def test_changes_to_objects_created_in_the_same_file(self) -> None:
        """New tables and columns may be changed in the migration creating them."""
        self.assertEqual(self.findings("passes", "same-file.sql"), [])

    def test_headers_cover_destructive_ddl(self) -> None:
        """The header files do hold destructive DDL, which the header allows."""
        for name in ("contract-header.sql", "no-transaction-header.sql"):
            with self.subTest(name=name):
                self.assertTrue(self.findings("passes", name))
                sql = (FIXTURES / "passes" / name).read_text(encoding="utf-8")
                self.assertTrue(cmo.has_contract_header(sql))

    def test_empty_contract_header_does_not_count(self) -> None:
        """`-- contract:` needs text after it."""
        self.assertFalse(cmo.has_contract_header("-- contract:\nDROP TABLE t;\n"))

    def test_cutoff_is_an_existing_migration(self) -> None:
        """The cutoff names a real migration, and no later one is destructive."""
        versions = {int(p.name.split("_", 1)[0]) for p in REPO_MIGRATIONS.glob("*.sql")}
        self.assertIn(cmo.CONTRACT_CHECK_CUTOFF, versions)
        for path in REPO_MIGRATIONS.glob("*.sql"):
            if int(path.name.split("_", 1)[0]) > cmo.CONTRACT_CHECK_CUTOFF:
                with self.subTest(path=path.name):
                    sql = path.read_text(encoding="utf-8")
                    self.assertEqual(cmo.contract_findings(path.name, sql), [])


if __name__ == "__main__":
    unittest.main()
