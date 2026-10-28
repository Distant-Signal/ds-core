"""Tests for scripts/test-postgres-roles.py --narrow (DS-118).

`render_narrowed` renders postgres-grants.sql as if the named roles were
`status: narrow` in db-grants.yaml, so CI ran release D's narrowing before
db-grants.yaml made it (2026-10-28). CI's "Release D's narrowed roles" step
runs it against Postgres; its per-service step runs the four roles'
suites `--observe`d until their fixtures move to the owner.
"""

import importlib.util
import pathlib
import shlex
import sys
import types
import unittest
from typing import Any, cast, override

import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "test-postgres-roles.py"
CI = REPO / ".github" / "workflows" / "ci.yml"
GRANTS_YAML = REPO / "charts" / "distant-signal" / "files" / "db-grants.yaml"


def per_service_runs() -> dict[str, list[str]]:
    """Return {db-tests.py pass: test-postgres-roles.py args} from ci.yml.

    Each step that runs `test-postgres-roles.py --mode per-service -- ...
    db-tests.py run PASS`, keyed by PASS.
    """
    workflow = cast("dict[str, Any]", yaml.safe_load(CI.read_text(encoding="utf-8")))
    runs: dict[str, list[str]] = {}
    for job in cast("dict[str, dict[str, Any]]", workflow["jobs"]).values():
        for step in cast("list[dict[str, Any]]", job.get("steps") or []):
            text = str(step.get("run") or "")
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
            script = command.index("scripts/db-tests.py")
            if command[script + 1] == "run":
                runs[command[script + 2]] = roles_args
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


class RenderNarrowedTest(unittest.TestCase):
    """The scratch postgres-grants.sql --narrow runs."""

    @override
    def setUp(self) -> None:
        """Load the script."""
        self.script = load_script()

    def test_the_named_roles_render_narrow_and_get_their_own_grants(self) -> None:
        """A narrowed role leaves app and gets its db-grants.yaml rows."""
        committed = self.script.GRANTS_SQL.read_text(encoding="utf-8")
        # Release D (2026-10-28): the committed SQL narrows all four.
        for kind in ("api", "aggregator", "enricher", "notifier"):
            self.assertIn(f"('{kind}', 'narrow')", committed)
            self.assertNotIn(f"('{kind}', 'observed')", committed)
        narrowed = self.script.render_with_statuses(
            {"api": "narrow", "aggregator": "narrow", "notifier": "observed"}
        )
        self.assertIn("('api', 'narrow')", narrowed)
        self.assertIn("('aggregator', 'narrow')", narrowed)
        self.assertNotIn("('api', 'observed')", narrowed)
        self.assertIn("('notifier', 'observed')", narrowed)
        # Unnamed roles keep their db-grants.yaml status.
        self.assertIn("('enricher', 'narrow')", narrowed)
        # The grants DS-118 adds: the api's outbox insert, the aggregator's
        # UPDATE on trains.id for its archive's row lock.
        self.assertIn(
            "('train_event_outbox', 'api', 'INSERT', "
            "'tracked_train_id,dedup_key,event')",
            narrowed,
        )
        self.assertIn("('trains', 'aggregator', 'UPDATE', 'id')", narrowed)
        self.assertNotIn("('train_current_state', 'api', 'UPDATE'", narrowed)

    def test_observe_keeps_a_role_in_the_app_role(self) -> None:
        """--observe renders a role observed, with no grants of its own."""
        rendered = self.script.render_with_statuses({"writer": "observed"})
        self.assertIn("('writer', 'observed')", rendered)
        self.assertNotIn("('trains', 'writer', 'INSERT'", rendered)
        # CI's per-service step: release D's four, observed again.
        rendered = self.script.render_with_statuses(
            dict.fromkeys(("api", "aggregator", "enricher", "notifier"), "observed")
        )
        self.assertIn("('api', 'observed')", rendered)
        self.assertNotIn("('train_event_outbox', 'api', 'INSERT'", rendered)
        self.assertNotIn("('trains', 'aggregator', 'UPDATE'", rendered)

    def test_an_unknown_role_is_refused(self) -> None:
        """A typo must not silently narrow nothing."""
        with self.assertRaises(RuntimeError):
            self.script.render_narrowed(["apii"])


class CiInvocationTest(unittest.TestCase):
    """ci.yml's per-service passes against db-grants.yaml (release D)."""

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
