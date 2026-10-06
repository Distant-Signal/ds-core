"""Tests for scripts/generate-pass-through.py's pure parts (no database).

uv run python -m unittest discover -s scripts/tests
"""

import datetime as dt
import importlib.util
import sys
import tomllib
import unittest
from collections import Counter
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType
from typing import Protocol

SCRIPT = Path(__file__).resolve().parent.parent / "generate-pass-through.py"


def load_script() -> ModuleType:
    """Import the hyphenated script as a module."""
    spec = importlib.util.spec_from_file_location("generate_pass_through", SCRIPT)
    if spec is None or spec.loader is None:
        raise ImportError(SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules["generate_pass_through"] = module
    spec.loader.exec_module(module)
    return module


gen = load_script()

KNOWN = {"VIC", "CLJ", "LBG", "ECR", "NXG", "SYD", "NWD", "BCY", "CYP", "WNW", "PUR"}


def train(operator: str, *path: str) -> object:
    """Build a Train."""
    return gen.Train(operator, tuple(path))


class LegLike(Protocol):
    """The fields of the script's Leg the tests read."""

    origin: str
    destination: str
    via: tuple[str, ...]
    runs: bool


def running(legs: Sequence[LegLike]) -> list[LegLike]:
    """Keep the legs trains run (not the breaks)."""
    return [leg for leg in legs if leg.runs]


def breaks(legs: Sequence[LegLike]) -> list[tuple[str, str]]:
    """Return the breaks as (from, to) pairs."""
    return [(leg.origin, leg.destination) for leg in legs if not leg.runs]


def bml() -> object:
    """Build a Brighton-Main-Line-shaped line: two London branches, then south."""
    return gen.Line("bml", frozenset({"SN"}), ("VIC", "CLJ", "LBG", "ECR", "PUR"))


def metro() -> object:
    """Build a metro line sharing Clapham Junction and London Bridge with it."""
    return gen.Line("metro", frozenset({"SN"}), ("CLJ", "WNW", "CYP", "LBG"))


class PassThroughGeneratorTest(unittest.TestCase):
    """The leg rules over synthetic trains."""

    def test_trains_from_rows_groups_in_path_order_and_drops_repeats(self) -> None:
        """Consecutive TIPLOCs of one CRS become one station."""
        rows = [
            ["2026-10-07 A1", "SN", "LBG"],
            ["2026-10-07 A1", "SN", "NXG "],
            ["2026-10-07 A1", "SN", "NXG"],
            ["2026-10-07 A1", "SN", "ECR"],
            ["2026-10-07 B2", "TL", "LBG"],
        ]
        trains = gen.trains_from_rows(rows)
        self.assertEqual(trains[0], gen.Train("SN", ("LBG", "NXG", "ECR")))
        self.assertEqual(trains[1], gen.Train("TL", ("LBG",)))

    def test_a_leg_gets_the_stations_its_trains_run_through(self) -> None:
        """Fast trains pass a few timing points; stopping trains list all."""
        trains = [
            *[train("SN", "LBG", "NXG", "SYD", "NWD", "ECR", "PUR")] * 5,
            train("SN", "LBG", "NXG", "BCY", "SYD", "NWD", "ECR", "PUR"),
            train("SN", "LBG", "NXG", "BCY", "SYD", "NWD", "ECR", "PUR"),
        ]
        index = gen.Index(trains)
        lines = [bml()]
        owned = gen.line_trains(index, lines)
        legs = running(gen.legs_for(index, owned["bml"], bml(), KNOWN))
        self.assertEqual(len(legs), 1)
        self.assertEqual((legs[0].origin, legs[0].destination), ("LBG", "ECR"))
        # The fuller record of the same route wins (2 of 7 trains >= 1/5).
        self.assertEqual(legs[0].via, ("NXG", "BCY", "SYD", "NWD"))

    def test_trains_running_the_other_way_give_the_same_leg(self) -> None:
        """A path from B to A is read backwards."""
        index = gen.Index([train("SN", "PUR", "ECR", "NWD", "SYD", "NXG", "LBG")])
        owned = gen.line_trains(index, [bml()])
        legs = running(gen.legs_for(index, owned["bml"], bml(), KNOWN))
        self.assertEqual(legs[0].via, ("NXG", "SYD", "NWD"))

    def test_a_branch_boundary_is_a_break(self) -> None:
        """Another line's train between two of this line's stations is not its own.

        The metro train reaches three bml stations (VIC, CLJ, LBG) but more
        of metro's, so bml's CLJ-LBG pair (a branch boundary in its station
        order) gets no stations and is recorded as a break, as are the pairs
        none of its trains runs between here.
        """
        trains = [
            train("SN", "VIC", "CLJ", "WNW", "CYP", "LBG"),
            train("SN", "LBG", "NXG", "SYD", "NWD", "ECR", "PUR"),
        ]
        index = gen.Index(trains)
        owned = gen.line_trains(index, [bml(), metro()])
        legs = gen.legs_for(index, owned["bml"], bml(), KNOWN)
        self.assertEqual(
            [(leg.origin, leg.destination) for leg in running(legs)], [("LBG", "ECR")]
        )
        self.assertEqual(breaks(legs), [("VIC", "CLJ"), ("CLJ", "LBG")])

    def test_unknown_crs_and_other_line_stations_are_left_out(self) -> None:
        """Only reference-table stations; a path through another stop is skipped."""
        index = gen.Index([train("SN", "LBG", "XJN", "NXG", "ECR", "PUR")])
        owned = gen.line_trains(index, [bml()])
        legs = running(gen.legs_for(index, owned["bml"], bml(), KNOWN))
        self.assertEqual(legs[0].via, ("NXG",))
        # LBG-ECR via PUR (a stop of the line) is not an adjacent pair.
        index = gen.Index([train("SN", "LBG", "PUR", "ECR", "VIC")])
        owned = gen.line_trains(index, [bml()])
        legs = gen.legs_for(index, owned["bml"], bml(), KNOWN)
        self.assertEqual(running(legs), [])
        self.assertIn(("LBG", "ECR"), breaks(legs))

    def test_route_of_prefers_the_most_common_path(self) -> None:
        """A different, rarer route never replaces the common one."""
        paths = Counter({("NXG", "SYD"): 10, ("BCY",): 9, ("NXG", "SYD", "NWD"): 1})
        self.assertEqual(gen.route_of(paths), (("NXG", "SYD"), 10))

    def test_render_is_deterministic_toml(self) -> None:
        """Sorted lines, legs in order, parseable, with a generated header."""
        legs = {
            "zeta": [gen.Leg("AAA", "BBB", ("CCC",))],
            "alpha": [
                gen.Leg("LBG", "ECR", ("NXG", "NWD")),
                gen.Leg("CLJ", "LBG", (), runs=False),
            ],
            "empty": [],
            "breaks-only": [gen.Leg("VIC", "CLJ", (), runs=False)],
        }
        dates = [dt.date(2026, 10, 7), dt.date(2026, 10, 10)]
        text = gen.render(legs, dates)
        self.assertEqual(text, gen.render(legs, dates))
        self.assertTrue(text.startswith("# GENERATED FILE"))
        parsed = tomllib.loads(text)
        self.assertEqual(parsed["source_dates"], ["2026-10-07", "2026-10-10"])
        self.assertEqual(list(parsed["lines"]), ["alpha", "zeta"])
        self.assertEqual(parsed["lines"]["alpha"]["LBG-ECR"], ["NXG", "NWD"])
        self.assertEqual(
            parsed["breaks"], {"alpha": ["CLJ-LBG"], "breaks-only": ["VIC-CLJ"]}
        )

    def test_the_checked_in_file_is_in_the_generated_format(self) -> None:
        """lines/generated/pass-through.toml parses and names only real lines."""
        text = gen.OUTPUT.read_text(encoding="utf-8")
        self.assertTrue(text.startswith("# GENERATED FILE"))
        parsed = tomllib.loads(text)
        line_ids = {line.id for line in gen.load_lines(gen.LINES_DIR)}
        self.assertLessEqual(set(parsed["lines"]), line_ids)


if __name__ == "__main__":
    unittest.main()
