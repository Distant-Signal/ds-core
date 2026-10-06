#!/usr/bin/env python3
# ruff: noqa: T201  # a CLI that reports what it wrote on stdout
"""Generate lines/generated/pass-through.toml from the CIF timetable.

  uv run scripts/generate-pass-through.py --database-url postgres://...
  uv run scripts/generate-pass-through.py --psql "kubectl ... exec -i
      distant-signal-postgres-0 -- psql -U distant_signal -d distant_signal"

For every catalogue line (lines/*.toml) and every pair of consecutive
`[[stations]]` on it, the stations the line's trains really run through
between the two, calling or passing (a fast train passes them, CIF records
the pass): the Brighton Main Line's London Bridge -> East Croydon leg runs
through New Cross Gate, Brockley, Honor Oak Park, Forest Hill, Sydenham,
Penge West, Anerley and Norwood Junction. The matcher counts them as on the
line for resolving the places an incident names (an incident "between New
Cross Gate and Norwood Junction" is on the Brighton Main Line); they are
never shown as the line's stops. See
docs/superpowers/specs/2026-10-06-incident-line-evidence-design.md.

Source: `schedule_calling_points_full` (every calling and passing point of
every schedule, by service date, as published by schedule-reference) joined
to `schedule_destination_departures` (a train's operator; passenger trains
only) and `tiploc_crs` (TIPLOC -> CRS). Read-only SELECTs. Three
representative days: by default the latest Wednesday, Saturday and Sunday
held in `schedule_calling_points_full` (it keeps a rolling fortnight);
`--date` overrides them.

Per leg (A, B): the paths, A to B or B to A, of the line's own trains (a
train belongs to the lines of its operator whose stations its route reaches
most often: `line_trains`) that call at or pass both, keeping only paths
with no other station of the line in between (so A and B really are
adjacent on the train's route). The most common path, or a fuller record
of the same route (`route_of`), gives the leg's stations, in order from A
to B. Only CRS codes in the `stations` reference table are kept (TIPLOCs
of junctions and depots that carry a CRS are not places an incident names).
A leg none of the line's trains runs (a branch boundary in the catalogue's
station order) gets nothing.

Run it after each timetable change (the December and May principal
changes) and after changing a line's stations, commit the result, and see
lines/SCHEMA.md.

The output is deterministic for the same data: lines sorted by id, legs in
catalogue order. Stdlib only; queries run through `psql` (a URL, or any
command that runs psql, such as `kubectl exec ... -- psql ...`).
"""

import argparse
import datetime as dt
import itertools
import shlex
import subprocess
import sys
import tomllib
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LINES_DIR = ROOT / "lines"
OUTPUT = LINES_DIR / "generated" / "pass-through.toml"
# The representative days `pick_dates` takes, as date.weekday() numbers.
WEEKDAYS = {"Wednesday": 2, "Saturday": 5, "Sunday": 6}
# A leg longer than this is not two adjacent stations: a catalogue gap or a
# branch boundary in the station order (lines/SCHEMA.md lists branches one
# after the other).
MAX_VIA = 40
# A path run by at least 1/MIN_SHARE_DENOMINATOR as many trains as the most
# common one may stand in for it when it records the same route more fully
# (`route_of`).
MIN_SHARE_DENOMINATOR = 5


@dataclass(frozen=True)
class Train:
    """One passenger schedule: its operator and its route as CRS codes."""

    operator: str
    path: tuple[str, ...]


@dataclass
class Line:
    """The parts of one lines/*.toml the generator uses."""

    id: str
    operators: frozenset[str]
    stations: tuple[str, ...]


@dataclass
class Leg:
    """Stations between two consecutive catalogue stations of a line.

    `runs` is false for a break: none of the line's trains runs directly
    between the two (a branch boundary in the catalogue's station order).
    """

    origin: str
    destination: str
    via: tuple[str, ...]
    trains: int = 0
    runs: bool = True


@dataclass
class Index:
    """Trains by the CRS codes on their path."""

    trains: list[Train]
    by_crs: dict[str, list[int]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Build the CRS -> train index."""
        by_crs: dict[str, list[int]] = defaultdict(list)
        for i, train in enumerate(self.trains):
            for crs in set(train.path):
                by_crs[crs].append(i)
        self.by_crs = dict(by_crs)


def load_lines(lines_dir: Path) -> list[Line]:
    """Return every catalogue line, sorted by id."""
    lines = []
    for path in sorted(lines_dir.glob("*.toml")):
        data = tomllib.loads(path.read_text(encoding="utf-8"))
        lines.append(
            Line(
                id=str(data["id"]),
                operators=frozenset(str(op) for op in data["operators"]),
                stations=tuple(str(s["crs"]) for s in data["stations"]),
            )
        )
    return sorted(lines, key=lambda line: line.id)


def line_trains(index: Index, lines: Sequence[Line]) -> dict[str, set[int]]:
    """Each line's own trains: the trains for which it is a best-fitting line.

    A train belongs to the lines of its operator whose catalogue stations its
    route reaches most often (at least two; ties: all of them). A metro
    train from Clapham Junction to London Bridge via Crystal Palace reaches
    three Brighton Main Line stations (Victoria, Clapham Junction, London
    Bridge), but more of the Crystal Palace metro line's, so it is that
    line's train, not the Brighton Main Line's.
    """
    by_operator: dict[str, list[Line]] = defaultdict(list)
    for line in lines:
        for operator in line.operators:
            by_operator[operator].append(line)
    owned: dict[str, set[int]] = defaultdict(set)
    for i, train in enumerate(index.trains):
        on_route = set(train.path)
        scores = [
            (len(on_route.intersection(line.stations)), line.id)
            for line in by_operator[train.operator]
        ]
        best = max((score for score, _ in scores), default=0)
        if best >= 2:  # noqa: PLR2004  # two stations make a route
            for score, line_id in scores:
                if score == best:
                    owned[line_id].add(i)
    return dict(owned)


def leg_paths(
    index: Index,
    trains: set[int] | None,
    line: Line,
    pair: tuple[str, str],
    known: set[str],
) -> Counter[tuple[str, ...]]:
    """Count the station paths (A to B order) of `trains` between `pair` (A, B).

    `trains` None means every train whose route reaches three of the line's
    stations (all of a two-station line's). Only paths with no other
    catalogue station of the line between A and B count, and only stations
    in `known` are kept on a path.
    """
    a, b = pair
    stations = set(line.stations)
    others = stations - {a, b}
    needed = min(3, len(stations))
    paths: Counter[tuple[str, ...]] = Counter()
    candidates = set(index.by_crs.get(a, ())) & set(index.by_crs.get(b, ()))
    if trains is not None:
        candidates &= trains
    for i in sorted(candidates):
        train = index.trains[i]
        if trains is None and len(stations.intersection(train.path)) < needed:
            continue
        i_a, i_b = train.path.index(a), train.path.index(b)
        lo, hi = sorted((i_a, i_b))
        via = train.path[lo + 1 : hi]
        if i_a > i_b:
            via = via[::-1]
        if len(via) > MAX_VIA or others.intersection(via):
            continue
        paths[tuple(crs for crs in via if crs in known)] += 1
    return paths


def route_of(paths: Counter[tuple[str, ...]]) -> tuple[tuple[str, ...], int]:
    """Pick the leg's stations: the most common path, or a fuller record of it.

    CIF records a passing point only at timing points, so a fast train's
    path omits the stations it passes between them (London Bridge to East
    Croydon: New Cross Gate, Sydenham, Norwood Junction), while a stopping
    train on the same tracks lists them all. Of the paths run by at least
    1/`MIN_SHARE_DENOMINATOR` as many trains as the most common one, the
    longest that contains every station of the most common path (the same
    route, more fully recorded) wins. Ties: the lexicographically smallest.
    """
    modal, count = min(paths.items(), key=lambda item: (-item[1], item[0]))
    fuller = [
        path
        for path, n in paths.items()
        if n * MIN_SHARE_DENOMINATOR >= count and set(modal) <= set(path)
    ]
    best = min(fuller, key=lambda path: (-len(path), path))
    return best, paths[best]


def legs_for(index: Index, owned: set[int], line: Line, known: set[str]) -> list[Leg]:
    """Each consecutive catalogue pair's route when it has stations, or break.

    Only the line's own trains ([`line_trains`]): a pair none of them runs
    between directly is a break, a branch boundary in the catalogue's
    station order (Victoria, Clapham Junction, London Bridge, East Croydon:
    no Brighton Main Line train runs from Clapham Junction to London
    Bridge). The matcher never treats a stretch across a break as track.
    A line that owns no train at all (an operator code CIF does not use)
    falls back to any train reaching three of its stations.
    """
    legs = []
    for a, b in itertools.pairwise(line.stations):
        if a == b:
            continue
        paths = leg_paths(index, owned or None, line, (a, b), known)
        if not paths:
            legs.append(Leg(origin=a, destination=b, via=(), runs=False))
            continue
        route, trains = route_of(paths)
        via = tuple(crs for crs in route if crs not in line.stations)
        if via:
            legs.append(Leg(origin=a, destination=b, via=via, trains=trains))
    return legs


def render(legs: Mapping[str, Sequence[Leg]], dates: Iterable[dt.date]) -> str:
    """Render the TOML: per line its `FROM-TO = [via...]` legs, then breaks.

    Legs go in one `[lines.<id>]` table per line; breaks in one `[breaks]`
    table, `<id> = ["FROM-TO", ...]`.
    """
    date_list = ", ".join(f'"{d.isoformat()}"' for d in dates)
    out = [
        "# GENERATED FILE -- do not edit by hand.",
        "#",
        "# Stations each catalogue line's trains run through (calling or passing)",
        "# between two of its consecutive [[stations]], from the CIF timetable.",
        "# Used ONLY by the incident matcher to resolve places named in incident",
        "# text (common::station_resolver / common::matcher); never shown as a",
        "# line's stops. Validated in CI by line-catalogue-validator.",
        "#",
        "# Regenerate after each timetable change (December and May) and after",
        "# changing a line's stations, then commit:",
        "#   uv run scripts/generate-pass-through.py --database-url <url>",
        "# (see lines/SCHEMA.md for running it against production read-only).",
        "",
        f"source_dates = [{date_list}]",
    ]
    for line_id in sorted(legs):
        running = [leg for leg in legs[line_id] if leg.runs]
        if not running:
            continue
        out.extend(["", f"[lines.{line_id}]"])
        for leg in running:
            via = ", ".join(f'"{crs}"' for crs in leg.via)
            out.append(f"{leg.origin}-{leg.destination} = [{via}]")
    breaks = {
        line_id: [
            f'"{leg.origin}-{leg.destination}"' for leg in line_legs if not leg.runs
        ]
        for line_id, line_legs in sorted(legs.items())
    }
    if any(breaks.values()):
        out.extend(
            [
                "",
                "# Consecutive catalogue stations none of the line's trains runs",
                "# between directly (branch boundaries).",
                "[breaks]",
            ]
        )
        out.extend(
            f"{line_id} = [{', '.join(pairs)}]"
            for line_id, pairs in breaks.items()
            if pairs
        )
    return "\n".join(out) + "\n"


class Psql:
    """Runs read-only queries through psql (a URL, or a command prefix)."""

    def __init__(self, database_url: str | None, command: str | None) -> None:
        """Pick the psql invocation."""
        if command:
            self.argv = shlex.split(command)
        elif database_url:
            self.argv = ["psql", database_url]
        else:
            msg = "give --database-url or --psql"
            raise SystemExit(msg)

    def rows(self, sql: str) -> list[list[str]]:
        """Run one SELECT in a read-only transaction; tab-separated rows."""
        script = f"BEGIN READ ONLY;\n{sql};\nROLLBACK;\n"
        result = subprocess.run(  # noqa: S603  # the operator's own psql command
            [*self.argv, "-X", "-q", "-At", "-F", "\t", "-v", "ON_ERROR_STOP=1"],
            input=script,
            check=True,
            capture_output=True,
            text=True,
        )
        return [line.split("\t") for line in result.stdout.splitlines() if line]


def pick_dates(psql: Psql) -> list[dt.date]:
    """Return the latest Wednesday, Saturday and Sunday the timetable holds."""
    held = [
        dt.date.fromisoformat(row[0])
        for row in psql.rows(
            "SELECT DISTINCT service_date FROM schedule_calling_points_full ORDER BY 1"
        )
    ]
    picked = []
    for name, weekday in WEEKDAYS.items():
        days = [d for d in held if d.weekday() == weekday]
        if not days:
            msg = f"no {name} in schedule_calling_points_full; pass --date"
            raise SystemExit(msg)
        picked.append(max(days))
    return sorted(picked)


def load_trains(psql: Psql, dates: Sequence[dt.date]) -> list[Train]:
    """Every passenger schedule on `dates`, as a path of CRS codes."""
    date_list = ", ".join(f"'{d.isoformat()}'" for d in dates)
    rows = psql.rows(
        "SELECT c.service_date || ' ' || c.uid, d.operator_atoc, t.crs "  # noqa: S608  # only isoformat() dates
        "FROM schedule_calling_points_full c "
        "JOIN (SELECT DISTINCT service_date, train_uid, operator_atoc "
        "      FROM schedule_destination_departures "
        f"     WHERE service_date IN ({date_list}) AND operator_atoc IS NOT NULL) d "
        "  ON d.service_date = c.service_date AND d.train_uid = c.uid "
        "JOIN tiploc_crs t ON t.tiploc = btrim(c.tiploc) "
        f"WHERE c.service_date IN ({date_list}) "
        "ORDER BY c.service_date, c.uid, c.seq"
    )
    return trains_from_rows(rows)


def trains_from_rows(rows: Iterable[Sequence[str]]) -> list[Train]:
    """Group (train key, operator, CRS) rows, in path order, into trains."""
    paths: dict[str, list[str]] = {}
    operators: dict[str, str] = {}
    for key, operator, raw_crs in rows:
        path = paths.setdefault(key, [])
        operators[key] = operator
        crs = raw_crs.strip()
        if crs and (not path or path[-1] != crs):
            path.append(crs)
    return [Train(operators[key], tuple(path)) for key, path in sorted(paths.items())]


def main(argv: Sequence[str] | None = None) -> int:
    """Write the pass-through file and report what it holds."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--database-url", help="Postgres URL holding the schedule tables"
    )
    parser.add_argument(
        "--psql", help="command that runs psql against that database instead"
    )
    parser.add_argument(
        "--date",
        action="append",
        type=dt.date.fromisoformat,
        help="service date (repeatable)",
    )
    parser.add_argument("--lines-dir", type=Path, default=LINES_DIR)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args(argv)
    psql = Psql(args.database_url, args.psql)
    dates = sorted(args.date) if args.date else pick_dates(psql)
    known = {row[0].strip() for row in psql.rows("SELECT crs FROM stations")}
    index = Index(load_trains(psql, dates))
    lines = load_lines(args.lines_dir)
    owned = line_trains(index, lines)
    legs = {
        line.id: legs_for(index, owned.get(line.id, set()), line, known)
        for line in lines
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render(legs, dates), encoding="utf-8")
    total = sum(len(leg.via) for line_legs in legs.values() for leg in line_legs)
    with_legs = sum(
        1 for line_legs in legs.values() if any(leg.via for leg in line_legs)
    )
    breaks = sum(1 for line_legs in legs.values() for leg in line_legs if not leg.runs)
    print(
        f"{args.output}: {total} pass-through stations on {with_legs} lines,", end=" "
    )
    print(f"{breaks} breaks")
    days = ", ".join(d.isoformat() for d in dates)
    print(f"source dates: {days}; {len(index.trains)} trains")
    return 0


if __name__ == "__main__":
    sys.exit(main())
