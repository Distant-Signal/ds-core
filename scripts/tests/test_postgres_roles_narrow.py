"""Tests for scripts/test-postgres-roles.py --narrow (DS-118).

`render_narrowed` renders postgres-grants.sql as if the named roles were
`status: narrow` in db-grants.yaml, so CI ran release D's narrowing before
db-grants.yaml made it (2026-10-28). CI's per-service DB test pass
(.github/db-tests.toml) runs it against Postgres in its "Release D's
narrowed roles" command, and runs the four roles' suites `--observe`d in
its Stage 0b command until their fixtures move to the owner.
"""

import importlib.util
import pathlib
import shlex
import sys
import tomllib
import types
import unittest
from typing import Any, cast, override

import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "test-postgres-roles.py"
PASSES = REPO / ".github" / "db-tests.toml"
GRANTS_YAML = REPO / "crates" / "ds-store" / "db-grants.yaml"


def per_service_runs() -> dict[str, list[str]]:
    """Return {db-tests.py pass: test-postgres-roles.py args} from CI's passes.

    Each command in .github/db-tests.toml's pass scripts (what CI's
    rust-db-tests job runs) that runs `test-postgres-roles.py --mode
    per-service -- ... db-tests.py run PASS`, keyed by PASS.
    """
    passes = tomllib.loads(PASSES.read_text(encoding="utf-8"))
    runs: dict[str, list[str]] = {}
    for test_pass in cast("list[dict[str, Any]]", passes["pass"]):
        script = str(test_pass["run"]).replace("\\\n", " ")
        for text in script.splitlines():
            if "scripts/test-postgres-roles.py" not in text:
                continue
            argv = shlex.split(text)
            if "--" not in argv:
                continue
            split = argv.index("--")
            roles_args, command = argv[:split], argv[split + 1 :]
            if "--mode" not in roles_args:
                continue
            if roles_args[roles_args.index("--mode") + 1] != "per-service":
                continue
            db_tests = command.index("scripts/db-tests.py")
            if command[db_tests + 1] == "run":
                runs[command[db_tests + 2]] = roles_args
    return runs


def flag(args: list[str], name: str) -> set[str]:
    """Return the comma-separated roles of --name in args (empty if absent)."""
    if name not in args:
        return set()
    return set(args[args.index(name) + 1].split(","))


def load_script() -> types.ModuleType:
    """Import scripts/test-postgres-roles.py as a module."""
    spec = importlib.util.spec_from_file_location("test_postgres_roles", SCRIPT)
    if spec is None or spec.loader is None:
        msg = f"cannot load {SCRIPT}"
        raise RuntimeError(msg)
    module = importlib.util.module_from_spec(spec)
    sys.modules["test_postgres_roles"] = module
    spec.loader.exec_module(module)
    return module


class NarrowObserveTest(unittest.TestCase):
    """--narrow and --observe, now that every role is narrow (release E)."""

    @override
    def setUp(self) -> None:
        """Load the script."""
        self.script = load_script()

    def test_the_committed_sql_narrows_every_role_and_has_no_app_role(self) -> None:
        """Release D narrowed the four; release E dropped the app role."""
        committed = self.script.GRANTS_SQL.read_text(encoding="utf-8")
        for kind in ("api", "aggregator", "enricher", "notifier"):
            self.assertIn(f"('{kind}', 'narrow')", committed)
        self.assertNotIn("'observed'", committed)
        self.assertNotIn("distant_signal_app", committed)
        # The grants DS-118 adds: the api's outbox insert, the aggregator's
        # UPDATE on trains.id for its archive's row lock.
        self.assertIn(
            "('train_event_outbox', 'api', 'INSERT', "
            "'tracked_train_id,dedup_key,event')",
            committed,
        )
        self.assertIn("('trains', 'aggregator', 'UPDATE', 'id')", committed)
        self.assertNotIn("('train_current_state', 'api', 'UPDATE'", committed)

    def test_observe_gives_table_wide_dml_but_not_the_migration_history(self) -> None:
        """The test-only stand-in for the old app membership."""
        sql = self.script.observed_dml_sql('"r"')
        self.assertIn("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES", sql)
        self.assertIn("REVOKE INSERT, UPDATE, DELETE ON public._sqlx_migrations", sql)
        self.assertIn("GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES", sql)
        self.assertNotIn("TRUNCATE", sql)

    def test_an_unknown_role_is_refused(self) -> None:
        """A typo must not silently assert (or observe) nothing."""
        _, created = self.script.per_service_kinds()
        self.script.check_role_keys(["api", "writer"], created)
        with self.assertRaises(RuntimeError):
            self.script.check_role_keys(["apii"], created)


class CiInvocationTest(unittest.TestCase):
    """CI's per-service passes (.github/db-tests.toml) against db-grants.yaml."""

    def test_the_stage_0b_pass_observes_exactly_the_release_d_roles(self) -> None:
        """Release D's roles: observed for the full suites, narrowed for the rest.

        The narrowed pass narrows the same roles, and that set is
        db-grants.yaml's phase 5 narrow roles: a role narrowed in
        production without both passes, or a list left behind, fails here.
        """
        raw = cast(
            "dict[str, Any]", yaml.safe_load(GRANTS_YAML.read_text(encoding="utf-8"))
        )
        release_d = {
            key
            for key, role in cast("dict[str, dict[str, Any]]", raw["roles"]).items()
            if role.get("status") == "narrow" and str(role.get("phase")) == "5"
        }
        self.assertEqual(release_d, {"api", "aggregator", "enricher", "notifier"})
        runs = per_service_runs()
        self.assertEqual(set(runs), {"per-service", "narrow"})
        stage_0b = runs["per-service"]
        self.assertIn("--check-setup-job", stage_0b)
        self.assertEqual(flag(stage_0b, "--observe"), release_d)
        self.assertEqual(flag(stage_0b, "--narrow"), set())
        self.assertEqual(flag(runs["narrow"], "--narrow"), release_d)
        self.assertEqual(flag(runs["narrow"], "--observe"), set())


if __name__ == "__main__":
    unittest.main()
