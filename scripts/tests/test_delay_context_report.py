"""Tests for scripts/delay-context-report.py: categories, classes and rendering.

uv run python -m unittest discover -s scripts/tests
"""

import datetime as dt
import importlib.util
import re
import sys
import unittest
from pathlib import Path
from types import ModuleType

SCRIPTS = Path(__file__).resolve().parent.parent
SCRIPT = SCRIPTS / "delay-context-report.py"
MIGRATION = (
    SCRIPTS.parent
    / "crates"
    / "ds-store"
    / "migrations"
    / "20261113120000_train_service_labels.sql"
)


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("delay_context_report", SCRIPT)
    if spec is None or spec.loader is None:
        msg = f"cannot load {SCRIPT}"
        raise ImportError(msg)
    module = importlib.util.module_from_spec(spec)
    sys.modules["delay_context_report"] = module
    spec.loader.exec_module(module)
    return module


rep = _load()

CSV = """week,line_id,service_group,event,reason_code,n
2026-11-02,,freight,service,,10
2026-11-02,,freight,cancelled,FL,3
2026-11-02,c2c,passenger,service,,100
2026-11-02,c2c,passenger,late,,20
2026-11-02,c2c,passenger,late,I5,2
2026-11-02,c2c,passenger,cancelled,FL,1
2026-11-02,,passenger,cancelled,,4
"""


class CategoryTests(unittest.TestCase):
    """Delay attribution codes to report categories."""

    def test_freight_and_possession_codes(self) -> None:
        """Freight and possession codes."""
        for code in ("FL", "FZ", "Q0", "OW"):
            self.assertEqual(rep.category(code), "freight_operator", code)
        for code in ("I5", "I7", "IU", "MV", "J8", "JG"):
            self.assertEqual(rep.category(code), "possession_overrun_plant", code)
        for code in ("PA", "PF", "PI", "QB", "QP", "JN"):
            self.assertEqual(rep.category(code), "planned_possession", code)

    def test_system_and_blank_codes(self) -> None:
        """System and blank codes."""
        for code in ("PD", "PG", "PK", "ZW"):
            self.assertEqual(rep.category(code), "planned_or_system_canx", code)
        self.assertEqual(rep.category(""), "none")
        self.assertEqual(rep.category("  "), "none")

    def test_families(self) -> None:
        """Families."""
        self.assertEqual(rep.category("IB"), "infrastructure")
        self.assertEqual(rep.category("pb"), "infrastructure")
        self.assertEqual(rep.category("OU"), "network_operations")
        self.assertEqual(rep.category("TB"), "train_operator")
        self.assertEqual(rep.category("M8"), "fleet")
        self.assertEqual(rep.category("XA"), "external")
        self.assertEqual(rep.category("YI"), "external")
        self.assertEqual(rep.category("RB"), "station_operations")
        self.assertEqual(rep.category("D1"), "other")


class ClassTests(unittest.TestCase):
    """Headcode digits to service classes."""

    def test_digit_is_the_third_character(self) -> None:
        """Digit is the third character."""
        self.assertEqual(rep.service_class("122A47MG15"), "passenger_ordinary")
        self.assertEqual(rep.service_class("151A04MG15"), "passenger_express")
        self.assertEqual(rep.service_class("326Z90MM15"), "freight_or_engineering")
        self.assertEqual(rep.service_class("895Y12AB15"), "ecs")

    def test_unusual_ids_are_unknown(self) -> None:
        """Unusual ids are unknown."""
        self.assertEqual(rep.service_class(None), "unknown")
        self.assertEqual(rep.service_class("12"), "unknown")
        self.assertEqual(rep.service_class("12XA47MG15"), "unknown")

    def test_class_nine_is_passenger_only_with_a_timetable(self) -> None:
        """Class nine is passenger only with a timetable."""
        self.assertEqual(rep.service_group("special", scheduled=True), "passenger")
        self.assertEqual(rep.service_group("special", scheduled=False), "special")
        self.assertEqual(rep.service_group("ecs", scheduled=True), "ecs")

    def test_migration_check_lists_every_class(self) -> None:
        """Migration check lists every class."""
        sql = MIGRATION.read_text(encoding="utf-8")
        allowed = set(
            re.findall(
                r"'([a-z_]+)'", sql.split("CHECK (service_class IN (")[1].split("))")[0]
            )
        )
        self.assertEqual(allowed, {*rep.CLASS_BY_DIGIT.values(), "unknown"})


class QueryTests(unittest.TestCase):
    """The SQL the report runs."""

    def test_query_is_one_read_only_select(self) -> None:
        """Query is one read only select."""
        sql = rep.build_query(dt.date(2026, 10, 19), dt.date(2026, 11, 16), 5)
        self.assertTrue(sql.startswith("WITH"))
        self.assertIn("DATE '2026-10-19'", sql)
        self.assertIn("train_service_labels", sql)
        self.assertNotIn("toc_id", sql)
        self.assertNotRegex(sql.lower(), r"\b(insert|update|delete|drop|alter)\b")

    def test_without_labels_it_does_not_name_the_table(self) -> None:
        """Without labels it does not name the table."""
        sql = rep.build_query(
            dt.date(2026, 10, 19), dt.date(2026, 11, 16), 5, labels=False
        )
        self.assertNotIn("train_service_labels", sql)
        self.assertIn("WHEN '6' THEN 'freight_or_engineering'", sql)


class ReportTests(unittest.TestCase):
    """Parsing and rendering."""

    def test_parse_skips_a_stray_command_tag(self) -> None:
        """Parse skips a stray command tag."""
        rows = rep.parse_rows("SET\n" + CSV)
        self.assertEqual(len(rows), 7)
        self.assertEqual(rep.parse_rows(""), [])
        with self.assertRaises(ValueError):
            rep.parse_rows("ERROR: nope\n")

    def test_text_report_counts(self) -> None:
        """Text report counts."""
        text = rep.render(rep.parse_rows(CSV), late_minutes=5)
        self.assertIn("Passenger services: 100; with a line on record: 100.", text)
        self.assertIn("freight_operator", text)
        late = text.split("Late passenger services by reason category")[1].split(
            "\n\n"
        )[0]
        self.assertRegex(late, r"none\s+20\s+20")
        self.assertRegex(late, r"possession_overrun_plant\s+2\s+2")
        other = text.split("Other service classes")[1]
        self.assertRegex(other, r"freight\s+10\s+0\s+3\s+3\s+0")

    def test_csv_has_no_operator_field(self) -> None:
        """Csv has no operator field."""
        out = rep.render_csv(rep.parse_rows(CSV))
        self.assertEqual(out.splitlines()[0], "week,line,group,event,category,n")
        self.assertNotIn("toc", out)


if __name__ == "__main__":
    unittest.main()
