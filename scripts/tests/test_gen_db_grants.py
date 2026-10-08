"""Tests for scripts/gen-db-grants.py (no database needed).

uv run python -m unittest discover -s scripts/tests
"""

import copy
import importlib.util
import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import ModuleType
from typing import Any

import yaml

SCRIPT = Path(__file__).resolve().parent.parent / "gen-db-grants.py"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("gen_db_grants", SCRIPT)
    if spec is None or spec.loader is None:
        msg = f"cannot load {SCRIPT}"
        raise ImportError(msg)
    module = importlib.util.module_from_spec(spec)
    sys.modules["gen_db_grants"] = module
    spec.loader.exec_module(module)
    return module


gen = _load()


def _raw() -> dict[str, Any]:
    raw: dict[str, Any] = yaml.safe_load(gen.GRANTS_YAML.read_text(encoding="utf-8"))
    return raw


class RepoFilesTest(unittest.TestCase):
    """The committed YAML and SQL."""

    def test_the_committed_files_pass_the_offline_check(self) -> None:
        """`check` without a database: valid YAML, budget, SQL current."""
        out = io.StringIO()
        with redirect_stdout(out):
            status = gen.main(["check"])
        self.assertEqual(status, 0, out.getvalue())

    def test_phase_0b_creates_observed_members_of_app_and_the_narrow_ones(self) -> None:
        """Phase 0b/1B: the four DB services and the writer; phases 2 and 3b: narrow roles."""
        model = gen.load()
        status = {r.key: r.status for r in model.created()}
        self.assertEqual(
            status,
            {
                "aggregator": "observed",
                "api": "observed",
                "enricher": "observed",
                "notifier": "observed",
                "writer": "observed",
                "incidents": "narrow",
                "schedule_ingest": "narrow",
                "schedule_reference": "narrow",
                "stations": "narrow",
                "trust_backlog": "narrow",
                "trust_consumer": "narrow",
                # Phase 4 (plan 4.7): the read-only readers.
                "full_coverage_ro": "narrow",
                "ldbws_ro": "narrow",
            },
        )


class ParseTest(unittest.TestCase):
    """Malformed YAML is refused with the offending key named."""

    def assert_refused(self, raw: dict[str, Any], needle: str) -> None:
        """parse(raw) raises GrantsError mentioning needle."""
        with self.assertRaises(gen.GrantsError) as ctx:
            gen.parse(raw)
        self.assertIn(needle, str(ctx.exception))

    def test_unknown_role_in_a_grant(self) -> None:
        """A grant to a role the YAML does not define."""
        raw = _raw()
        raw["tables"]["users"]["grants"]["nobody"] = "S"
        self.assert_refused(raw, "unknown role 'nobody'")

    def test_unknown_privilege(self) -> None:
        """TRUNCATE (T) is never granted."""
        raw = _raw()
        raw["tables"]["users"]["grants"]["api"] = "SIUDT"
        self.assert_refused(raw, "tables.users.grants.api")

    def test_unknown_class(self) -> None:
        """Classes are a closed set."""
        raw = _raw()
        raw["tables"]["users"]["class"] = "secret"
        self.assert_refused(raw, "tables.users.class")

    def test_sequence_of_an_unlisted_table(self) -> None:
        """A sequence must belong to a listed table."""
        raw = _raw()
        raw["sequences"]["x_seq"] = {"table": "nope"}
        self.assert_refused(raw, "sequences.x_seq.table")

    def test_column_grants_parse(self) -> None:
        """`{privileges, columns}` is a column-level grant."""
        raw = _raw()
        raw["tables"]["incidents"]["grants"]["enricher"] = {
            "privileges": "U",
            "columns": ["extraction", "extracted_at"],
        }
        model = gen.parse(raw)
        grant = next(
            g for g in model.tables["incidents"].grants if g.role == "enricher"
        )
        self.assertEqual(grant.columns, ("extraction", "extracted_at"))


class BudgetTest(unittest.TestCase):
    """Connection limits must fit max_connections - superuser_reserved."""

    def test_limits_summing_over_the_budget_fail(self) -> None:
        """Raising one role's limit past the 97 slots fails the check."""
        raw = _raw()
        model = gen.parse(raw)
        spare = model.budget() - model.limit_sum()
        self.assertGreaterEqual(spare, 0)
        over = copy.deepcopy(raw)
        over["roles"]["api"]["connection_limit"] += spare + 1
        problems = gen.problems_in_model(gen.parse(over))
        self.assertTrue(any("connection limits sum" in p for p in problems), problems)
        exact = copy.deepcopy(raw)
        exact["roles"]["api"]["connection_limit"] += spare
        self.assertEqual(gen.problems_in_model(gen.parse(exact)), [])

    def test_duplicate_role_names_fail(self) -> None:
        """Two keys may not share a Postgres role name."""
        raw = _raw()
        raw["roles"]["notifier"]["name"] = raw["roles"]["api"]["name"]
        problems = gen.problems_in_model(gen.parse(raw))
        self.assertTrue(any("used twice" in p for p in problems), problems)


class DatabaseComparisonTest(unittest.TestCase):
    """Classification gaps against a (simulated) migrated schema."""

    def objects(self) -> dict[str, set[str]]:
        """Exactly what the YAML lists, as the DB query would return it."""
        model = gen.load()
        return {
            "table": set(model.tables),
            "view": set(model.views),
            "sequence": set(model.sequences),
            "function": {"analyze_publish_keys(target text)"},
        }

    def test_matching_schema_has_no_problems(self) -> None:
        """Function arguments are compared by name only."""
        self.assertEqual(gen.problems_against_database(gen.load(), self.objects()), [])

    def test_an_unclassified_table_view_or_sequence_fails(self) -> None:
        """A migration that adds an object must classify it."""
        objects = self.objects()
        objects["table"].add("new_table")
        objects["view"].add("new_view")
        objects["sequence"].add("new_table_id_seq")
        problems = gen.problems_against_database(gen.load(), objects)
        self.assertEqual(len(problems), 3, problems)
        self.assertTrue(all("not classified" in p for p in problems))

    def test_a_listed_object_missing_from_the_schema_fails(self) -> None:
        """A stale YAML entry (dropped table) fails too."""
        objects = self.objects()
        objects["table"].discard("tocs")
        problems = gen.problems_against_database(gen.load(), objects)
        self.assertEqual(
            problems, ["table tocs is in db-grants.yaml but not in the migrated schema"]
        )


class RenderTest(unittest.TestCase):
    """The rendered SQL."""

    def test_stale_sql_fails_the_check(self) -> None:
        """An edited YAML without a re-render fails."""
        with tempfile.TemporaryDirectory() as tmp:
            sql = Path(tmp) / "postgres-grants.sql"
            sql.write_text(gen.render(gen.load()) + "-- edited\n", encoding="utf-8")
            out = io.StringIO()
            with redirect_stdout(out):
                status = gen.main(["check", "--sql", str(sql)])
            self.assertEqual(status, 1)
            self.assertIn("is stale", out.getvalue())

    def test_observed_roles_get_no_table_grants(self) -> None:
        """Phase 0b: observed roles get memberships, never GRANT rows.

        Narrow roles get their grants: 2a's schedule_reference, 2b's
        stations exactly its two tables, and 3b's TRUST roles exactly theirs.
        """
        sql = gen.render(gen.load())
        self.assertIn("('api', 'observed')", sql)
        for kind in ("api", "aggregator", "enricher", "notifier", "writer"):
            self.assertNotIn(f"'{kind}', 'SELECT', ''", sql)
        self.assertIn("('stanox_crs', 'schedule_reference', 'DELETE', '')", sql)
        self.assertIn("('schedule_reference', 'narrow')", sql)
        self.assertIn("('stations', 'narrow')", sql)
        grant_rows = sorted(
            line.strip().rstrip(",")
            for line in sql.splitlines()
            if "'stations', 'SELECT', ''" in line
            or "'stations', 'INSERT', ''" in line
            or "'stations', 'UPDATE', ''" in line
            or "'stations', 'DELETE', ''" in line
        )
        self.assertEqual(
            [row.split(",")[0] for row in grant_rows],
            ["('ingest_freshness'"] * 3 + ["('stations'"] * 3,
        )
        self.assertIn("('trust_backlog', 'narrow')", sql)
        self.assertIn("('trust_consumer', 'narrow')", sql)

        def tables(kind: str, privilege: str) -> list[str]:
            return sorted(
                line.strip().split(",")[0].strip("('")
                for line in sql.splitlines()
                if f"'{kind}', '{privilege}', ''" in line
            )

        self.assertEqual(
            tables("trust_consumer", "INSERT"),
            [
                "notifier_forward_queue",
                "train_current_state",
                "train_event_outbox",
                "train_movement_events",
            ],
        )
        self.assertEqual(
            tables("trust_consumer", "UPDATE"),
            ["train_current_state", "train_movement_events"],
        )
        self.assertEqual(
            tables("trust_backlog", "INSERT"),
            [
                "train_current_state",
                "train_movement_events",
                "train_reasons",
                "trains",
                "trust_event_backlog",
            ],
        )
        self.assertEqual(tables("trust_backlog", "DELETE"), [])
        self.assertEqual(tables("trust_consumer", "DELETE"), [])
        self.assertIn("\\getenv api_password DS_PG_API_PASSWORD", sql)

    def test_a_narrow_role_gets_its_grants_and_sequences(self) -> None:
        """Once narrowed, the YAML's grants and the owning sequences render."""
        raw = _raw()
        raw["roles"]["notifier"]["status"] = "narrow"
        raw["tables"]["incidents"]["grants"]["notifier"] = {
            "privileges": "U",
            "columns": ["summary"],
        }
        sql = gen.render(gen.parse(raw))
        self.assertIn("('journeys', 'notifier', 'INSERT', '')", sql)
        self.assertIn("('journeys_id_seq', 'notifier')", sql)
        self.assertIn("('incidents', 'notifier', 'UPDATE', 'summary')", sql)
        self.assertNotIn("('users', 'notifier'", sql)
        self.assertIn("('notifier', 'narrow')", sql)

    def test_row_policies_render_for_created_roles_only(self) -> None:
        """Plan 3c.3: the writer's line_status policy; none for a planned role."""
        sql = gen.render(gen.load())
        self.assertIn("('line_status', 'writer', 'source = ''tfl''')", sql)
        self.assertIn("AS RESTRICTIVE FOR ALL TO %I", sql)
        raw = _raw()
        # Planned here explicitly: every shipped role may be created.
        raw["roles"]["trust_backlog"]["status"] = "planned"
        raw["row_policies"]["line_status"]["trust_backlog"] = "false"
        sql = gen.render(gen.parse(raw))
        self.assertNotIn("'trust_backlog', 'false'", sql)


class RowPolicyParseTest(unittest.TestCase):
    """`row_policies` must name listed tables and known roles."""

    def test_the_writer_is_pinned_to_tfl_rows(self) -> None:
        """D10: the shipped YAML's one policy."""
        self.assertEqual(
            gen.load().row_policies, {"line_status": {"writer": "source = 'tfl'"}}
        )

    def test_unlisted_table_and_unknown_role_are_refused(self) -> None:
        """Either mistake names the key."""
        raw = _raw()
        raw["row_policies"]["nope"] = {"writer": "true"}
        with self.assertRaises(gen.GrantsError) as ctx:
            gen.parse(raw)
        self.assertIn("row_policies.nope", str(ctx.exception))
        raw = _raw()
        raw["row_policies"]["line_status"]["nobody"] = "true"
        with self.assertRaises(gen.GrantsError) as ctx:
            gen.parse(raw)
        self.assertIn("unknown role 'nobody'", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
