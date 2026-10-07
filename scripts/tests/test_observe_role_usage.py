"""Tests for scripts/observe-role-usage.py's statement parsing and report.

uv run python -m unittest discover -s scripts/tests
"""

import importlib.util
import sys
import unittest
from pathlib import Path
from types import ModuleType

SCRIPT = Path(__file__).resolve().parent.parent / "observe-role-usage.py"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("observe_role_usage", SCRIPT)
    if spec is None or spec.loader is None:
        msg = f"cannot load {SCRIPT}"
        raise ImportError(msg)
    module = importlib.util.module_from_spec(spec)
    sys.modules["observe_role_usage"] = module
    spec.loader.exec_module(module)
    return module


obs = _load()
KNOWN = [
    "trains",
    "train_subscriptions",
    "train_movement_events",
    "incidents",
    "incident_history",
    "line_status",
    "journeys",
    "stations",
    "notifier_cursor",
]


def verbs(sql: str) -> dict[str, str]:
    """{table: verbs as a sorted SIUD string} for one statement."""
    return {
        table: "".join(v for v in "SIUD" if v in found)
        for table, found in obs.verbs_by_table(sql, KNOWN).items()
    }


class VerbsTest(unittest.TestCase):
    """Normalised pg_stat_statements text to table verbs."""

    def test_select_with_joins_and_a_comma_list(self) -> None:
        """FROM, JOIN and a comma-separated FROM list are reads."""
        sql = (
            "SELECT t.id FROM trains t, stations st JOIN train_subscriptions s"
            " ON s.train_id = t.id WHERE st.crs = $1"
        )
        self.assertEqual(
            verbs(sql), {"trains": "S", "train_subscriptions": "S", "stations": "S"}
        )

    def test_cte_names_are_not_tables_and_cte_bodies_count(self) -> None:
        """A CTE named like nothing known, writing and reading real tables."""
        sql = (
            "WITH moved AS (UPDATE trains SET resolved = $1 WHERE id = ANY($2)"
            " RETURNING id), picked AS MATERIALIZED (SELECT id FROM moved)"
            " SELECT count(*) FROM picked"
            " JOIN train_movement_events e ON e.train_id = picked.id"
        )
        self.assertEqual(verbs(sql), {"trains": "SU", "train_movement_events": "S"})

    def test_update_from(self) -> None:
        """UPDATE t ... FROM other: U (and S for WHERE) on t, S on other."""
        sql = (
            "UPDATE incidents i SET summary = h.summary FROM incident_history h"
            " WHERE h.incident_id = i.incident_id"
        )
        self.assertEqual(verbs(sql), {"incidents": "SU", "incident_history": "S"})

    def test_insert_select(self) -> None:
        """INSERT ... SELECT reads the source."""
        sql = (
            "INSERT INTO incident_history (id, body)"
            " SELECT id, body FROM incidents WHERE id = $1"
        )
        self.assertEqual(verbs(sql), {"incident_history": "I", "incidents": "S"})

    def test_on_conflict_do_update_needs_update(self) -> None:
        """An upsert needs I, U and S; DO NOTHING only I."""
        upsert = (
            "INSERT INTO line_status (line_id, status) VALUES ($1, $2)"
            " ON CONFLICT (line_id) DO UPDATE SET status = EXCLUDED.status"
        )
        self.assertEqual(verbs(upsert), {"line_status": "SIU"})
        nothing = "INSERT INTO notifier_cursor (id) VALUES ($1) ON CONFLICT DO NOTHING"
        self.assertEqual(verbs(nothing), {"notifier_cursor": "I"})

    def test_delete_using_and_returning(self) -> None:
        """DELETE FROM t USING other: D (and S) on t, S on other."""
        sql = (
            "DELETE FROM train_movement_events e USING trains t"
            " WHERE e.train_id = t.id AND t.service_date < $1 RETURNING e.id"
        )
        self.assertEqual(verbs(sql), {"train_movement_events": "SD", "trains": "S"})

    def test_bare_delete_is_not_a_read(self) -> None:
        """No WHERE, no RETURNING: D only."""
        self.assertEqual(verbs("DELETE FROM journeys"), {"journeys": "D"})

    def test_select_for_update_needs_update(self) -> None:
        """Row locks need UPDATE privilege; DO UPDATE / FOR UPDATE are not UPDATE t."""
        sql = "SELECT id FROM journeys WHERE user_id = $1 FOR UPDATE"
        self.assertEqual(verbs(sql), {"journeys": "SU"})

    def test_comments_strings_schema_and_unknown_names(self) -> None:
        """Literals and comments cannot fake a table; public. is stripped."""
        sql = (
            "/* from trains */ SELECT 'from incidents' FROM public.stations"
            " -- join journeys\n JOIN pg_catalog.pg_class c ON true"
            " JOIN unnest($1::text[]) u ON true"
        )
        self.assertEqual(verbs(sql), {"stations": "S"})


class ReportTest(unittest.TestCase):
    """Aggregation and the diff against db-grants.yaml."""

    def test_rows_round_trip_and_diff(self) -> None:
        """Rows from psql -z -0, then used-not-granted and granted-not-seen."""
        data = (
            b"distant_signal_enricher\x0012\x00"
            b"UPDATE incidents SET x = $1 WHERE id = $2\x00"
            b"distant_signal_enricher\x003\x00SELECT 1 FROM stations\x00"
        )
        rows = obs.parse_rows(data)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].calls, 12)
        usage = obs.observe(rows, KNOWN)
        raw = {
            "roles": {
                "enricher": {
                    "name": "distant_signal_enricher",
                    "groups": ["schema_gate"],
                }
            },
            "tables": {
                "incidents": {"class": "ingest", "grants": {"enricher": "SU"}},
                "incident_history": {"class": "ingest", "grants": {"enricher": "S"}},
                "stations": {"class": "reference", "grants": {}},
            },
        }
        text = obs.report(usage, obs.granted(raw))
        self.assertIn("| `stations` | S |  | S |  |", text)
        self.assertIn("| `incident_history` |  | S |  | S |", text)
        self.assertIn("| `incidents` | SU | SU |  |  |", text)
        self.assertIn("2 statements, 15 calls.", text)


if __name__ == "__main__":
    unittest.main()
