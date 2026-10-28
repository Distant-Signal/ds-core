"""Tests for scripts/db-tests.py: the CI DB passes as cargo-nextest steps.

  uv run python -m unittest discover -s scripts/tests

`run` is exercised against a fake `cargo-nextest` on PATH that records its
arguments and environment, so nothing here builds or needs a database.
The role check reads db-grants.yaml through test-postgres-roles.py (PyYAML,
the lint group).
"""

import importlib.util
import io
import json
import os
import stat
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import ModuleType
from typing import TypedDict, cast, override

REPO = Path(__file__).resolve().parents[2]


def _load(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        msg = f"cannot load {path}"
        raise ImportError(msg)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


db = _load("db_tests", REPO / "scripts" / "db-tests.py")

# A fake cargo-nextest: appends {"argv", "database_url", "narrowed"} to
# $FAKE_LOG and exits with the status in $FAKE_FAIL_ON_STEP's step (by
# call number), else 0.
FAKE = """#!/usr/bin/env python3
import json, os, sys
log = os.environ["FAKE_LOG"]
with open(log, "a", encoding="utf-8") as f:
    f.write(json.dumps({
        "argv": sys.argv[1:],
        "database_url": os.environ.get("DATABASE_URL"),
        "narrowed": os.environ.get("DS_TEST_NARROWED_ROLES"),
    }) + "\\n")
with open(log, encoding="utf-8") as f:
    calls = sum(1 for _ in f)
sys.exit(3 if str(calls) == os.environ.get("FAKE_FAIL_ON_CALL") else 0)
"""


class Call(TypedDict):
    """One recorded fake cargo-nextest call."""

    argv: list[str]
    database_url: str | None
    narrowed: str | None


class StepTests(unittest.TestCase):
    """How one former `cargo test` invocation becomes nextest arguments."""

    def test_filtersets(self) -> None:
        """Packages, one package, a --test target and a --bin target."""
        step = db.Step(packages=("api", "ds-store"))
        self.assertEqual(step.filterset(), "(package(=api) | package(=ds-store))")
        self.assertEqual(db.Step(packages=("api",)).filterset(), "package(=api)")
        self.assertEqual(
            db.Step(packages=("common",), test_target="redis_acl").filterset(),
            "binary_id(=common::redis_acl)",
        )
        self.assertEqual(
            db.Step(
                packages=("ingest-writer",), bin_target="writer-maintenance"
            ).filterset(),
            "binary_id(=ingest-writer::bin/writer-maintenance)",
        )

    def test_libtest_arguments_are_kept(self) -> None:
        """--ignored, the name filters, every --skip and --exact, after `--`."""
        step = db.Step(
            packages=("api",),
            filters=("a::b",),
            skips=("x", "y"),
            exact=True,
        )
        self.assertEqual(
            step.libtest_args(),
            ["--ignored", "a::b", "--skip", "x", "--skip", "y", "--exact"],
        )

    def test_run_from_an_extracted_archive(self) -> None:
        """No cargo: the extracted metadata, serial unless the step says not."""
        extracted = Path("/x")
        argv = db.nextest_argv(db.Step(packages=("api",), skips=("s",)), extracted)
        self.assertEqual(argv[:3], ["cargo-nextest", "nextest", "run"])
        self.assertIn("--binaries-metadata", argv)
        self.assertEqual(
            argv[argv.index("--target-dir-remap") + 1], str(extracted / "target")
        )
        self.assertEqual(argv[argv.index("--workspace-remap") + 1], str(db.ROOT))
        self.assertEqual(argv[argv.index("--test-threads") + 1], "1")
        self.assertEqual(argv[argv.index("-E") + 1], "package(=api)")
        self.assertEqual(argv[argv.index("--") :], ["--", "--ignored", "--skip", "s"])
        parallel = db.nextest_argv(
            db.Step(packages=("schedule-ingest",), serial=False), extracted
        )
        self.assertNotIn("--test-threads", parallel)

    def test_run_from_source_builds_the_archive_selection(self) -> None:
        """Without an archive, cargo builds the same packages and features."""
        argv = db.nextest_argv(db.Step(packages=("api",)), None)
        self.assertEqual(argv[:4], ["cargo", "nextest", "run", "--locked"])
        packages = [argv[i + 1] for i, a in enumerate(argv) if a == "-p"]
        self.assertEqual(packages, list(db.ARCHIVE_PACKAGES))
        self.assertEqual(argv[argv.index("--features") + 1], "common/postgres")

    def test_archive_builds_every_package_once(self) -> None:
        """One `cargo nextest archive` with every package and feature."""
        argv = db.archive_argv(Path("out.tar.zst"))
        self.assertEqual(argv[:3], ["cargo", "nextest", "archive"])
        packages = [argv[i + 1] for i, a in enumerate(argv) if a == "-p"]
        self.assertEqual(sorted(packages), sorted(set(db.ARCHIVE_PACKAGES)))
        self.assertEqual(argv[-2:], ["--archive-file", "out.tar.zst"])

    def test_describe_matches_the_old_commands(self) -> None:
        """describe() gives back the `cargo test` line a step replaced."""
        narrow = db.PASSES["narrow"]
        self.assertEqual(
            narrow[0].describe(),
            'DATABASE_URL="$DATABASE_URL_API" cargo test -p ds-store -- --ignored '
            "backlog::matching::db_tests::the_api_queues_its_replay_for_the_writer "
            "--exact --test-threads=1",
        )
        superuser = db.PASSES["superuser"]
        self.assertEqual(
            superuser[1].describe(),
            "cargo test -p common pg::tests -- --ignored --test-threads=1",
        )


class PassTests(unittest.TestCase):
    """The passes as data."""

    def test_every_package_is_in_the_archive(self) -> None:
        """A step can only run binaries the archive built."""
        for name, steps in db.PASSES.items():
            for step in steps:
                with self.subTest(pass_name=name, step=step.describe()):
                    self.assertLessEqual(set(step.packages), set(db.ARCHIVE_PACKAGES))

    def test_targets_name_one_package(self) -> None:
        """--test/--bin steps name exactly one package."""
        for steps in db.PASSES.values():
            for step in steps:
                if step.test_target or step.bin_target:
                    self.assertEqual(len(step.packages), 1, step.describe())

    def test_role_passes_use_roles_test_postgres_roles_creates(self) -> None:
        """Each role is a created role of db-grants.yaml (DATABASE_URL_<ROLE>)."""
        roles = _load(
            "test_postgres_roles", REPO / "scripts" / "test-postgres-roles.py"
        )
        _, created = roles.per_service_kinds()
        for name in ("per-service", "narrow"):
            for step in db.PASSES[name]:
                with self.subTest(pass_name=name, step=step.describe()):
                    self.assertIn(step.role, created)
        for name in ("superuser", "existing", "new"):
            for step in db.PASSES[name]:
                self.assertIsNone(step.role, step.describe())

    def test_the_stations_role_runs_last(self) -> None:
        """poller-stations' narrow role leaves rows it cannot delete."""
        self.assertEqual(db.PASSES["per-service"][-1].packages, ("poller-stations",))


class RunTests(unittest.TestCase):
    """run_pass against a fake cargo-nextest."""

    @override
    def setUp(self) -> None:
        """Put a fake cargo-nextest first on PATH."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        fake = root / "cargo-nextest"
        fake.write_text(FAKE, encoding="utf-8")
        fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
        self.log = root / "calls.jsonl"
        self.env = {
            "PATH": f"{root}{os.pathsep}{os.environ['PATH']}",
            "FAKE_LOG": str(self.log),
            "DATABASE_URL": "postgres://app",
            "DS_TEST_NARROWED_ROLES": "api,aggregator",
        }

    def calls(self) -> list[Call]:
        """Return each recorded fake cargo-nextest call."""
        lines = self.log.read_text(encoding="utf-8").splitlines()
        return [cast("Call", json.loads(line)) for line in lines]

    def run_pass(self, name: str, env: dict[str, str]) -> tuple[int, str]:
        """Run a pass from a (fake) archive; return its status and output."""
        out = io.StringIO()
        with redirect_stdout(out):
            status = db.run_pass(name, Path("a.tar.zst"), base_env=env)
        return status, out.getvalue()

    def test_extracts_once_then_runs_each_step_with_its_role(self) -> None:
        """One extraction, then every step in order, as its role."""
        env = {
            **self.env,
            "DATABASE_URL_API": "postgres://api",
            "DATABASE_URL_AGGREGATOR": "postgres://aggregator",
        }
        status, out = self.run_pass("narrow", env)
        self.assertEqual(status, 0, out)
        calls = self.calls()
        self.assertEqual(len(calls), 1 + len(db.PASSES["narrow"]))
        extract = calls[0]["argv"]
        self.assertEqual(extract[:2], ["nextest", "list"])
        self.assertIn("--extract-to", extract)
        self.assertEqual(
            [c["database_url"] for c in calls[1:]],
            ["postgres://api", "postgres://api", "postgres://aggregator"],
        )
        self.assertEqual({c["narrowed"] for c in calls}, {"api,aggregator"})
        for call in calls[1:]:
            argv = call["argv"]
            self.assertEqual(argv[:2], ["nextest", "run"])
            self.assertNotIn("--archive-file", argv)

    def test_extracts_where_asked(self) -> None:
        """--extract-to: the binaries land where build-time paths point."""
        where = Path(self.tmp.name) / "checkout"
        out = io.StringIO()
        with redirect_stdout(out):
            status = db.run_pass(
                "new", Path("a.tar.zst"), extract_to=where, base_env=self.env
            )
        self.assertEqual(status, 0, out.getvalue())
        extract, run = (call["argv"] for call in self.calls())
        self.assertEqual(extract[extract.index("--extract-to") + 1], str(where))
        self.assertEqual(
            run[run.index("--target-dir-remap") + 1], str(where / "target")
        )

    def test_steps_without_a_role_keep_database_url(self) -> None:
        """The app-role passes keep the DATABASE_URL test-postgres-roles set."""
        status, _ = self.run_pass("new", self.env)
        self.assertEqual(status, 0)
        self.assertEqual(self.calls()[1]["database_url"], "postgres://app")

    def test_stops_at_the_first_failing_step(self) -> None:
        """A failing step's status is the pass's; later steps do not run."""
        env = {**self.env, "FAKE_FAIL_ON_CALL": "3"}
        status, out = self.run_pass("existing", env)
        self.assertEqual(status, 3)
        self.assertEqual(len(self.calls()), 3)  # extraction, steps 1 and 2
        self.assertIn("existing step 2 exited 3", out)

    def test_a_missing_role_url_fails_before_running(self) -> None:
        """Without DATABASE_URL_<ROLE> (not under --mode per-service): exit 2."""
        status, out = self.run_pass("per-service", self.env)
        self.assertEqual(status, 2)
        self.assertIn("DATABASE_URL_API is not set", out)
        self.assertEqual(len(self.calls()), 1)  # only the extraction

    def test_dry_run_runs_nothing(self) -> None:
        """--dry-run prints each step's old and new command lines."""
        out = io.StringIO()
        with redirect_stdout(out):
            status = db.main(["run", "superuser", "--archive-file", "a", "--dry-run"])
        self.assertEqual(status, 0)
        self.assertFalse(self.log.exists())
        self.assertIn("cargo test -p ingest-stream --test redis_stream", out.getvalue())


if __name__ == "__main__":
    unittest.main()
