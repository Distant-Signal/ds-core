#!/usr/bin/env python3
# ruff: noqa: T201  # a CLI: the report goes to stdout or --output
r"""Back-office report: passenger delays and cancellations by recorded reason.

  uv run scripts/delay-context-report.py --database-url URL [--weeks 8]
  uv run scripts/delay-context-report.py --from-file ROWS.csv

INTERNAL ONLY (DS-513). Nothing here is served by the api or shown in the
frontend, and the report holds no operator code: `train_service_labels.toc_id`
is never read. It prints aggregate counts, per ISO week (Monday) and per
line, of passenger services that ran late or were cancelled, by the
category of the delay attribution code TRUST gave them, next to the same
counts for the non-passenger service classes (freight, ECS, light
locomotives, engineering). It is the evidence for deciding whether a
source-coded "reason given" line is worth showing on a passenger's own
train; it is not a statement about who caused a delay.

What the data can and cannot say:

  - A "service class" is the first digit of the TRUST train id's headcode
    (`train_service_labels.service_class`, else derived here from
    `trains.train_id` the same way, so weeks before the labels existed
    still report). It is a convention: 6 and 7 mix freight and engineering
    trains, 3 mixes parcels and empty coaching stock, 9 is special workings
    and counts as a passenger service only when the train has a timetable.
  - TRUST's open feed has no reason on a movement. DS stores a reason only
    for a cancellation (`train_reasons` 0002, counted while the train is
    still cancelled) and a change of origin (0006). "Late" is the train's
    stored delay, so a late service whose reason is "none" is the common
    case, and Darwin's own late reasons are not in this report until DS
    stores them. Read the categories as "late and a reason is on record".
  - A line is `trains.matched_line_id`, set when a train is matched to the
    timetable for a line; most trains have none and are counted under
    "(no line)".

Categories (`category()`), from the Network Rail delay attribution
glossary's code families. Planned possession work is split from the
unplanned causes:

  freight_operator          F*, Q0, OW (a freight operating company's cause)
  planned_possession        PA, PF, PI, QB, QP, JN (planned engineering work)
  possession_overrun_plant  I5, I7, IU, MV, J8, JG (possession overruns,
                            engineers' trains and on-track plant)
  planned_or_system_canx    PD, PG, PJ, PK, PL, PS, ZW (planned or system
                            cancellations, not a cause)
  infrastructure            the other I*, J* and P* codes
  network_operations        O* and the other Q*
  train_operator            T*
  station_operations        R*
  fleet                     M*
  external                  X*, Y*, Z* (weather, trespass, inwards stock and
                            the like)
  other                     any other code; none: the service carries no code

--database-url connects with psql in a read-only transaction. Production is
reached through kubectl instead, so --from-file reads the same rows saved
from psql (the report needs no database access beyond SELECT; run it as the
database owner, no per-service role has the labels):

  kubectl -n distant-signal exec -i distant-signal-postgres-0 -- \
    psql -U distant_signal -d distant_signal -X -q --csv \
      -c "SET default_transaction_read_only = on" -c "$(
        uv run scripts/delay-context-report.py --print-query --weeks 8)" > rows.csv
  uv run scripts/delay-context-report.py --from-file rows.csv
"""

from __future__ import annotations

import argparse
import collections
import csv
import dataclasses
import datetime as dt
import io
import pathlib
import subprocess
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

PASSENGER = "passenger"
NO_LINE = "(no line)"
NO_REASON = "none"

# The headcode digit to class mapping, mirrored in the SQL below and in
# ds-ingest's trust-backlog-consumer (`service_class_for_train_id`).
CLASS_BY_DIGIT = {
    "0": "light_loco",
    "1": "passenger_express",
    "2": "passenger_ordinary",
    "3": "ecs_or_parcels",
    "4": "freight",
    "5": "ecs",
    "6": "freight_or_engineering",
    "7": "freight_or_engineering",
    "8": "freight",
    "9": "special",
}

PLANNED_POSSESSION = frozenset({"PA", "PF", "PI", "QB", "QP", "JN"})
POSSESSION_OVERRUN_PLANT = frozenset({"I5", "I7", "IU", "MV", "J8", "JG"})
PLANNED_OR_SYSTEM_CANX = frozenset({"PD", "PG", "PJ", "PK", "PL", "PS", "ZW"})
FREIGHT_OPERATOR_EXTRA = frozenset({"Q0", "OW"})

BY_FIRST_LETTER = {
    "F": "freight_operator",
    "I": "infrastructure",
    "J": "infrastructure",
    "P": "infrastructure",
    "O": "network_operations",
    "Q": "network_operations",
    "T": "train_operator",
    "R": "station_operations",
    "M": "fleet",
    "X": "external",
    "Y": "external",
    "Z": "external",
}

# The categories the per-line table breaks out: what freight and possession
# work could explain.
CONTEXT_CATEGORIES = (
    "freight_operator",
    "planned_possession",
    "possession_overrun_plant",
)
CATEGORY_ORDER = (
    "freight_operator",
    "planned_possession",
    "possession_overrun_plant",
    "infrastructure",
    "network_operations",
    "train_operator",
    "station_operations",
    "fleet",
    "external",
    "planned_or_system_canx",
    "other",
    NO_REASON,
)


def category(code: str) -> str:
    """Return the report category of a delay attribution `code` (see above)."""
    code = code.strip().upper()
    if not code:
        return NO_REASON
    if code in PLANNED_POSSESSION:
        return "planned_possession"
    if code in POSSESSION_OVERRUN_PLANT:
        return "possession_overrun_plant"
    if code in PLANNED_OR_SYSTEM_CANX:
        return "planned_or_system_canx"
    if code in FREIGHT_OPERATOR_EXTRA:
        return "freight_operator"
    return BY_FIRST_LETTER.get(code[0], "other")


def service_class(train_id: str | None) -> str:
    """Return the class of a TRUST train id, as the SQL below derives it."""
    if train_id is None or len(train_id) < 3:  # noqa: PLR2004  # the digit is the third character
        return "unknown"
    return CLASS_BY_DIGIT.get(train_id[2], "unknown")


def service_group(klass: str, *, scheduled: bool) -> str:
    """Return `passenger` for a passenger-class service, else its class.

    Class 9 (special workings) is passenger only with a timetable.
    """
    if klass in {"passenger_express", "passenger_ordinary"}:
        return PASSENGER
    if klass == "special" and scheduled:
        return PASSENGER
    return klass


def build_query(
    since: dt.date,
    until: dt.date,
    late_minutes: int,
    *,
    labels: bool = True,
) -> str:
    """Return the read-only SELECT, one row per (week, line, group, event, code)."""
    digit_cases = "\n".join(
        f"             WHEN '{digit}' THEN '{klass}'"
        for digit, klass in CLASS_BY_DIGIT.items()
    )
    label_join = (
        "LEFT JOIN train_service_labels l ON l.trains_id = t.id" if labels else ""
    )
    label_class = "l.service_class" if labels else "NULL"
    return f"""WITH base AS (
  SELECT t.id,
         date_trunc('week', t.service_date)::date AS week,
         COALESCE(t.matched_line_id, '') AS line_id,
         t.calling_points IS NOT NULL AS scheduled,
         COALESCE({label_class},
           CASE substr(t.train_id, 3, 1)
{digit_cases}
             ELSE 'unknown' END) AS service_class
  FROM trains t
  {label_join}
  WHERE t.service_date >= DATE '{since.isoformat()}'
    AND t.service_date < DATE '{until.isoformat()}'
), grouped AS (
  SELECT id, week,
         CASE WHEN service_class IN ('passenger_express', 'passenger_ordinary')
                OR (service_class = 'special' AND scheduled)
              THEN 'passenger' ELSE service_class END AS service_group,
         CASE WHEN service_class IN ('passenger_express', 'passenger_ordinary')
                OR (service_class = 'special' AND scheduled)
              THEN line_id ELSE '' END AS line_id
  FROM base
)
SELECT g.week, g.line_id, g.service_group, 'service' AS event, '' AS reason_code, count(*) AS n
FROM grouped g GROUP BY 1, 2, 3
UNION ALL
SELECT g.week, g.line_id, g.service_group, 'late', COALESCE(r.reason_code, ''), count(*)
FROM grouped g
JOIN train_current_state s ON s.trains_id = g.id
LEFT JOIN train_reasons r ON r.trains_id = g.id AND r.msg_type = '0006'
WHERE s.status <> 'cancelled' AND s.delay_minutes >= {late_minutes}
GROUP BY 1, 2, 3, 5
UNION ALL
SELECT g.week, g.line_id, g.service_group, 'cancelled', r.reason_code, count(*)
FROM grouped g
JOIN train_current_state s ON s.trains_id = g.id AND s.status = 'cancelled'
JOIN train_reasons r ON r.trains_id = g.id AND r.msg_type = '0002'
GROUP BY 1, 2, 3, 5
UNION ALL
SELECT g.week, g.line_id, g.service_group, 'cancelled', '', count(*)
FROM grouped g
JOIN train_current_state s ON s.trains_id = g.id AND s.status = 'cancelled'
WHERE NOT EXISTS (
  SELECT 1 FROM train_reasons r WHERE r.trains_id = g.id AND r.msg_type = '0002')
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3, 4, 5"""  # noqa: S608, E501  # dates and ints only


@dataclasses.dataclass(frozen=True)
class Row:
    """One aggregate row of the query."""

    week: str
    line_id: str
    group: str
    event: str
    code: str
    n: int


def parse_rows(text: str) -> list[Row]:
    """Return the rows of `psql --csv` output (a stray command tag is skipped)."""
    lines = text.splitlines()
    start = next((i for i, line in enumerate(lines) if line.startswith("week,")), None)
    if start is None:
        if not any(line.strip() for line in lines):
            return []
        msg = "no CSV header line (week,line_id,...) in the input"
        raise ValueError(msg)
    reader = csv.DictReader(io.StringIO("\n".join(lines[start:])))
    return [
        Row(
            week=r["week"],
            line_id=r["line_id"],
            group=r["service_group"],
            event=r["event"],
            code=r["reason_code"],
            n=int(r["n"]),
        )
        for r in reader
    ]


def run_psql(database_url: str, query: str) -> str:
    """Run `query` in a read-only transaction over psql and return its CSV."""
    argv = [
        "psql",
        "-X",
        "-q",
        "--csv",
        "-v",
        "ON_ERROR_STOP=1",
        "-d",
        database_url,
        "-c",
        "SET default_transaction_read_only = on",
        "-c",
        query,
    ]
    result = subprocess.run(  # noqa: S603  # argv built here; the URL is the caller's
        argv, check=False, capture_output=True, text=True
    )
    if result.returncode != 0:
        msg = f"psql failed: {result.stderr.strip()}"
        raise RuntimeError(msg)
    return result.stdout


def table(header: Sequence[str], body: Iterable[Sequence[object]]) -> str:
    """Return a left-aligned text table."""
    rows = [[str(cell) for cell in row] for row in body]
    widths = [
        max(len(h), *(len(r[i]) for r in rows)) if rows else len(h)
        for i, h in enumerate(header)
    ]

    def fmt(cells: Sequence[str]) -> str:
        return "  ".join(
            cell.rjust(width) if i else cell.ljust(width)
            for i, (cell, width) in enumerate(zip(cells, widths, strict=True))
        )

    return "\n".join(
        [fmt(list(header)), fmt(["-" * w for w in widths]), *(fmt(r) for r in rows)]
    )


def categorised(
    rows: Iterable[Row],
) -> collections.Counter[tuple[str, str, str, str, str]]:
    """Return counts keyed by (week, line, group, event, category)."""
    counts: collections.Counter[tuple[str, str, str, str, str]] = collections.Counter()
    for row in rows:
        event_category = (
            category(row.code) if row.event in {"late", "cancelled"} else ""
        )
        counts[(row.week, row.line_id, row.group, row.event, event_category)] += row.n
    return counts


def by_category_table(
    counts: collections.Counter[tuple[str, str, str, str, str]],
    *,
    group: str,
    event: str,
    weeks: Sequence[str],
) -> str:
    """Return categories (rows) by weeks (columns) for one group and event."""
    per: collections.Counter[tuple[str, str]] = collections.Counter()
    for (week, _line, grp, evt, cat), n in counts.items():
        if grp == group and evt == event:
            per[(cat, week)] += n
    body = []
    for cat in CATEGORY_ORDER:
        values = [per[(cat, w)] for w in weeks]
        if any(values):
            body.append([cat, *values, sum(values)])
    return table(["category", *weeks, "total"], body)


def per_line_table(
    counts: collections.Counter[tuple[str, str, str, str, str]], weeks: Sequence[str]
) -> str:
    """Return passenger services by (week, line) with the context categories."""
    keys = sorted(
        {(w, ln) for (w, ln, g, _e, _c) in counts if g == PASSENGER},
        key=lambda k: (k[0], k[1]),
    )
    header = ["week", "line", "services", "late"]
    header += [f"late:{c}" for c in CONTEXT_CATEGORIES]
    header += [f"canx:{c}" for c in CONTEXT_CATEGORIES]
    body = []
    for week, line in keys:
        if week not in weeks:
            continue

        def n(
            event: str, cat: str | None = None, week: str = week, line: str = line
        ) -> int:
            return sum(
                v
                for (w, ln, g, e, c), v in counts.items()
                if (w, ln, g, e) == (week, line, PASSENGER, event)
                and (cat is None or c == cat)
            )

        body.append(
            [week, line or NO_LINE, n("service"), n("late")]
            + [n("late", c) for c in CONTEXT_CATEGORIES]
            + [n("cancelled", c) for c in CONTEXT_CATEGORIES]
        )
    return table(header, body)


def other_classes_table(
    counts: collections.Counter[tuple[str, str, str, str, str]],
) -> str:
    """Return the non-passenger classes: services, cancelled, by category."""
    totals: collections.Counter[tuple[str, str]] = collections.Counter()
    for (_w, _l, grp, evt, cat), n in counts.items():
        if grp == PASSENGER:
            continue
        key = (
            "services"
            if evt == "service"
            else f"{evt}:{cat}"
            if evt == "cancelled"
            else evt
        )
        totals[(grp, key)] += n
    groups = sorted({g for g, _ in totals})
    body = []
    for grp in groups:
        canx = sum(
            v for (g, k), v in totals.items() if g == grp and k.startswith("cancelled:")
        )
        freight = totals[(grp, "cancelled:freight_operator")]
        possession = (
            totals[(grp, "cancelled:planned_possession")]
            + totals[(grp, "cancelled:possession_overrun_plant")]
        )
        body.append(
            [
                grp,
                totals[(grp, "services")],
                totals[(grp, "late")],
                canx,
                freight,
                possession,
            ]
        )
    return table(
        ["class", "services", "late", "cancelled", "canx:freight", "canx:possession"],
        body,
    )


def render(rows: Sequence[Row], *, late_minutes: int) -> str:
    """Return the full text report."""
    counts = categorised(rows)
    weeks = sorted({w for (w, *_rest) in counts})
    out = [
        (
            "Delay context report (internal). Weeks starting: "
            f"{', '.join(weeks) or 'none'}. Late = {late_minutes}+ minutes."
        ),
        "",
    ]
    passenger_services = sum(
        v
        for (_w, _l, g, e, _c), v in counts.items()
        if g == PASSENGER and e == "service"
    )
    with_line = sum(
        v
        for (_w, ln, g, e, _c), v in counts.items()
        if g == PASSENGER and e == "service" and ln
    )
    out.append(
        f"Passenger services: {passenger_services}; "
        f"with a line on record: {with_line}. "
        "Late 'none' means no reason is on record."
    )
    for event, title in (
        ("late", "Late passenger services"),
        ("cancelled", "Cancelled passenger services"),
    ):
        out += [
            "",
            f"{title} by reason category",
            by_category_table(counts, group=PASSENGER, event=event, weeks=weeks),
        ]
    out += ["", "Passenger services by week and line", per_line_table(counts, weeks)]
    out += ["", "Other service classes (all weeks)", other_classes_table(counts)]
    return "\n".join(out) + "\n"


def render_csv(rows: Sequence[Row]) -> str:
    """Return the categorised long rows as CSV."""
    counts = categorised(rows)
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["week", "line", "group", "event", "category", "n"])
    for (week, line, grp, evt, cat), n in sorted(counts.items()):
        writer.writerow([week, line, grp, evt, cat, n])
    return buffer.getvalue()


def main(argv: Sequence[str] | None = None) -> int:
    """Run the report."""
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n", maxsplit=1)[0] if __doc__ else ""
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--database-url", help="connect with psql, read-only")
    source.add_argument(
        "--from-file", type=pathlib.Path, help="rows saved from psql --csv"
    )
    parser.add_argument(
        "--print-query", action="store_true", help="print the SQL and exit"
    )
    parser.add_argument(
        "--weeks", type=int, default=8, help="ISO weeks back from this one (default 8)"
    )
    parser.add_argument(
        "--late-minutes", type=int, default=5, help="late threshold (default 5)"
    )
    parser.add_argument(
        "--no-labels",
        action="store_true",
        help="derive every class from the train id (labels not yet deployed)",
    )
    parser.add_argument("--format", choices=("text", "csv"), default="text")
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args(argv)

    today = dt.datetime.now(dt.UTC).date()
    this_week = today - dt.timedelta(days=today.weekday())
    since = this_week - dt.timedelta(weeks=max(args.weeks, 0))
    until = this_week + dt.timedelta(weeks=1)
    query = build_query(since, until, args.late_minutes, labels=not args.no_labels)
    if args.print_query:
        print(query)
        return 0
    if args.from_file is not None:
        text = args.from_file.read_text(encoding="utf-8")
    elif args.database_url:
        text = run_psql(args.database_url, query)
    else:
        parser.error("one of --database-url, --from-file or --print-query is required")
    rows = parse_rows(text)
    report = (
        render_csv(rows)
        if args.format == "csv"
        else render(rows, late_minutes=args.late_minutes)
    )
    if args.output is not None:
        args.output.write_text(report, encoding="utf-8")
    else:
        sys.stdout.write(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
