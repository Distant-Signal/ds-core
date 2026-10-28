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


RELEASE_D = ("api", "aggregator", "enricher", "notifier")


def _raw_observed(*kinds: str) -> dict[str, Any]:
    """Return the committed YAML with `kinds` (default: release D's four) observed."""
    raw = _raw()
    for kind in kinds or RELEASE_D:
        raw["roles"][kind]["status"] = "observed"
    return raw


class RepoFilesTest(unittest.TestCase):
    """The committed YAML and SQL."""

    def test_the_committed_files_pass_the_offline_check(self) -> None:
        """`check` without a database: valid YAML, budget, SQL current."""
        out = io.StringIO()
        with redirect_stdout(out):
            status = gen.main(["check"])
        self.assertEqual(status, 0, out.getvalue())

    def test_every_created_role_is_narrow_since_release_d(self) -> None:
        """Release D (5.5 step 3, 2026-10-28): no role is an app member any more."""
        model = gen.load()
        status = {r.key: r.status for r in model.created()}
        self.assertEqual(
            status,
            {
                # Release D (DS-118): the phase 0b DB services.
                "aggregator": "narrow",
                "api": "narrow",
                "enricher": "narrow",
                "notifier": "narrow",
                # Security review M1 (2026-10-08): narrow, not an app member.
                "writer": "narrow",
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

    def test_a_list_gives_different_columns_per_privilege(self) -> None:
        """`[I, {privileges: S, columns: [...]}]` is two grants."""
        raw = _raw()
        raw["tables"]["incidents"]["grants"]["enricher"] = [
            "I",
            {"privileges": "S", "columns": ["id"]},
        ]
        model = gen.parse(raw)
        grants = [g for g in model.tables["incidents"].grants if g.role == "enricher"]
        self.assertEqual(
            [(g.privileges, g.columns) for g in grants], [("I", ()), ("S", ("id",))]
        )

    def test_a_privilege_twice_or_a_column_delete_is_refused(self) -> None:
        """A letter in two entries, or DELETE on columns, is malformed."""
        raw = _raw()
        raw["tables"]["incidents"]["grants"]["enricher"] = [
            "SI",
            {"privileges": "S", "columns": ["id"]},
        ]
        self.assert_refused(raw, "S given twice")
        raw = _raw()
        raw["tables"]["incidents"]["grants"]["enricher"] = {
            "privileges": "D",
            "columns": ["id"],
        }
        self.assert_refused(raw, "DELETE is table-wide")


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
        (Release D narrowed the four phase 0b roles; observed again here.)
        """
        sql = gen.render(gen.parse(_raw_observed()))
        self.assertIn("('api', 'observed')", sql)
        # Security review M1: no member may SET ROLE to app or a group.
        self.assertEqual(sql.count("'GRANT %I TO %I WITH INHERIT TRUE, SET FALSE'"), 2)
        self.assertNotIn("'GRANT %I TO %I'", sql)
        for kind in ("api", "aggregator", "enricher", "notifier"):
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
        self.assertEqual(tables("trust_consumer", "UPDATE"), ["train_current_state"])
        # Security review L3/M2: no table-wide SELECT on trains, the
        # movements or the forward queue; column SELECTs on the conflict
        # targets and the subscription lookup only.
        for table in ("trains", "train_movement_events", "notifier_forward_queue"):
            self.assertNotIn(table, tables("trust_consumer", "SELECT"))
        for table, kind, privilege, columns in (
            (
                "train_movement_events",
                "trust_consumer",
                "SELECT",
                "trains_id,dedup_key",
            ),
            ("notifier_forward_queue", "trust_consumer", "SELECT", "dedup_key"),
            ("train_subscriptions", "trust_consumer", "SELECT", "id,trains_id"),
            (
                "train_subscriptions",
                "trust_backlog",
                "UPDATE",
                "resolution_status,unresolved_from",
            ),
            ("schedule_feed_ingests", "schedule_ingest", "SELECT", "delivered_at"),
        ):
            self.assertIn(f"('{table}', '{kind}', '{privilege}', '{columns}')", sql)
        self.assertNotIn("train_movement_events", tables("trust_backlog", "UPDATE"))
        self.assertNotIn("train_subscriptions", tables("trust_backlog", "UPDATE"))
        # The writer (M1, L3): narrow, without the unused grants.
        self.assertIn("('writer', 'narrow')", sql)
        for table in ("train_movement_events", "trust_event_backlog"):
            self.assertNotIn(table, tables("writer", "UPDATE"))
        self.assertNotIn("corpus_crosswalk_build", tables("writer", "DELETE"))
        # train_subscriptions is personal: no longer in read_shared.
        self.assertNotIn("('train_subscriptions')", sql)
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

    def test_the_enricher_targets_column_update_on_incidents(self) -> None:
        """Q3 prep: table SELECT plus UPDATE on the extraction columns only.

        While the enricher is observed nothing renders for it; narrowed (5.5,
        the committed YAML since release D) the same rows give exactly those
        grants and no table-wide UPDATE.
        """
        extraction = [
            "extracted_at",
            "extracted_category",
            "extracted_periods",
            "extraction_model_version",
            "source_text_hash",
        ]
        model = gen.load()
        grants = [
            (g.privileges, tuple(sorted(g.columns)))
            for g in model.tables["incidents"].grants
            if g.role == "enricher"
        ]
        self.assertEqual(grants, [("S", ()), ("U", tuple(extraction))])
        self.assertNotIn(
            "'enricher', 'UPDATE'", gen.render(gen.parse(_raw_observed("enricher")))
        )

        sql = gen.render(model)
        self.assertIn("('incidents', 'enricher', 'SELECT', '')", sql)
        self.assertNotIn("('incidents', 'enricher', 'UPDATE', '')", sql)
        updates = sorted(
            line.strip().rstrip(",")
            for line in sql.splitlines()
            if "('incidents', 'enricher', 'UPDATE'," in line
        )
        self.assertEqual(len(updates), 1, updates)
        # The YAML's own column order.
        self.assertIn(
            "'source_text_hash,extracted_category,extracted_periods,"
            "extraction_model_version,extracted_at'",
            updates[0],
        )

    def test_row_policies_render_for_created_roles_only(self) -> None:
        """Plan 3c.3: the writer's line_status policy; none for a planned role."""
        sql = gen.render(gen.load())
        self.assertIn(
            "('line_status', 'writer', 'ALL', 'source IN (''tfl'', ''transit'')')", sql
        )
        self.assertIn("AS RESTRICTIVE FOR %s TO %I", sql)
        raw = _raw()
        # Planned here explicitly: every shipped role may be created.
        raw["roles"]["trust_backlog"]["status"] = "planned"
        raw["row_policies"]["line_status"]["trust_backlog"] = "false"
        sql = gen.render(gen.parse(raw))
        self.assertNotIn("'trust_backlog', 'ALL', 'false'", sql)

    def test_an_unchanged_row_policy_is_left_alone(self) -> None:
        """2026-10-14: no DROP/CREATE POLICY (ACCESS EXCLUSIVE) when it is right.

        The live policy is compared with the wanted one, deparsed on a TEMP
        copy of the table, and only a missing or different one is created;
        only an unlisted ds_grants_* one is dropped. The steady state is
        proved against Postgres by scripts/test-postgres-roles.py
        --check-setup-job.
        """
        sql = gen.render(gen.load())
        policies = sql[sql.index("DO $policies$") : sql.index("$policies$;")]
        self.assertIn(
            "CREATE TEMP TABLE ds_grants_policy_probe (LIKE public.%I)", policies
        )
        self.assertIn("IS NOT DISTINCT FROM want_qual", policies)
        self.assertIn("IS NOT DISTINCT FROM want_check", policies)
        self.assertIn("<> ALL (wanted)", policies)
        self.assertLess(policies.index("CONTINUE;"), policies.index("CREATE POLICY %I"))

    def test_a_writes_only_policy_renders_one_row_per_write_command(self) -> None:
        """L4: `{writes: ...}` restricts INSERT, UPDATE and DELETE, not SELECT."""
        sql = gen.render(gen.load())
        for command in ("INSERT", "UPDATE", "DELETE"):
            self.assertIn(
                f"('ingest_freshness', 'stations', '{command}', "
                "'source = ''stations''')",
                sql,
            )
        self.assertNotIn("('ingest_freshness', 'stations', 'ALL'", sql)


class RowPolicyParseTest(unittest.TestCase):
    """`row_policies` must name listed tables and known roles."""

    def test_the_writer_is_pinned_to_tfl_and_transit_rows(self) -> None:
        """D10 and transit spec §10.4: the writer's line_status policy."""
        self.assertEqual(
            gen.load().row_policies["line_status"],
            {"writer": gen.RowPolicy("source IN ('tfl', 'transit')")},
        )

    def test_each_freshness_producer_writes_only_its_own_rows(self) -> None:
        """L4: writes-only policies for every role that writes ingest_freshness."""
        model = gen.load()
        policies = model.row_policies["ingest_freshness"]
        self.assertEqual(
            policies["stations"],
            gen.RowPolicy("source = 'stations'", writes_only=True),
        )
        self.assertEqual(
            policies["incidents"],
            gen.RowPolicy("source = 'incidents'", writes_only=True),
        )
        self.assertTrue(policies["writer"].writes_only)
        self.assertIn("'tfl'", policies["writer"].condition)
        # Every role with a write grant on the table has a policy.
        writers = {
            g.role
            for g in model.tables["ingest_freshness"].grants
            if set(g.privileges) - {"S"}
        }
        self.assertEqual(writers, set(policies))

    def test_a_writes_entry_must_hold_only_writes(self) -> None:
        """Any other key in the mapping is refused."""
        raw = _raw()
        raw["row_policies"]["line_status"]["writer"] = {"reads": "true"}
        with self.assertRaises(gen.GrantsError) as ctx:
            gen.parse(raw)
        self.assertIn("row_policies.line_status.writer", str(ctx.exception))

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


class AppRevokesTest(unittest.TestCase):
    """`app_revokes`: what the app role loses (ingest phase 5, release B)."""

    def assert_refused(self, raw: dict[str, Any], needle: str) -> None:
        """parse(raw) raises GrantsError mentioning needle."""
        with self.assertRaises(gen.GrantsError) as ctx:
            gen.parse(raw)
        self.assertIn(needle, str(ctx.exception))

    def test_release_b_takes_the_schedule_publish_staging_from_app(self) -> None:
        """Only the api's /private publish routes used them through app."""
        model = gen.load()
        self.assertEqual(
            set(model.app_revoke_tables),
            {
                "schedule_calling_points_full_publish_keys",
                "schedule_destination_departures_publish_keys",
            },
        )
        self.assertEqual(model.app_revoke_functions, ("analyze_publish_keys(text)",))
        # schedule-reference keeps its own grants on them.
        for table in model.app_revoke_tables:
            grants = {g.role: g.privileges for g in model.tables[table].grants}
            self.assertEqual(grants, {"schedule_reference": "SID"})
        self.assertEqual(
            model.functions["analyze_publish_keys(text)"], ("schedule_reference",)
        )

    def test_the_revokes_render_after_the_grants(self) -> None:
        """Step 6 of the SQL, from app, for existing objects only."""
        sql = gen.render(gen.load())
        self.assertIn("('schedule_calling_points_full_publish_keys')", sql)
        self.assertIn("('analyze_publish_keys(text)')", sql)
        self.assertIn("REVOKE ALL ON public.%I FROM %I', r.tbl, app", sql)
        self.assertIn("REVOKE EXECUTE ON FUNCTION %s FROM %I", sql)
        self.assertLess(sql.index("$policies$;"), sql.index("$app_revokes$"))
        self.assertLess(sql.index("$app_revokes$;"), sql.index("COMMIT;"))

    def test_no_revokes_render_typed_empty_lists(self) -> None:
        """Without the section nothing is revoked, and the SQL still parses."""
        raw = _raw()
        del raw["app_revokes"]
        model = gen.parse(raw)
        self.assertEqual(model.app_revoke_tables, ())
        self.assertEqual(model.app_revoke_functions, ())
        sql = gen.render(model)
        self.assertIn("(SELECT NULL::text WHERE false) AS v(tbl)", sql)
        self.assertIn("(SELECT NULL::text WHERE false) AS v(sig)", sql)

    def test_an_unlisted_table_or_function_is_refused(self) -> None:
        """Each entry must be classified in its own section."""
        raw = _raw()
        raw["app_revokes"]["tables"] = ["nope"]
        self.assert_refused(raw, "app_revokes.tables: 'nope'")
        raw = _raw()
        raw["app_revokes"]["functions"] = ["nope(text)"]
        self.assert_refused(raw, "app_revokes.functions: 'nope(text)'")
        raw = _raw()
        raw["app_revokes"]["views"] = []
        self.assert_refused(raw, "app_revokes: unknown key(s) ['views']")

    def test_what_an_observed_role_is_granted_cannot_be_revoked(self) -> None:
        """An observed role has nothing of its own: it would lose the grant."""
        raw = _raw_observed()
        raw["app_revokes"]["tables"] = ["users"]
        self.assert_refused(raw, "granted to observed role(s) ['api']")
        raw = _raw_observed()
        raw["functions"]["analyze_publish_keys(text)"]["grants"].append("notifier")
        self.assert_refused(raw, "granted to observed role(s) ['notifier']")


if __name__ == "__main__":
    unittest.main()
