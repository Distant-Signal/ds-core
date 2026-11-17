#!/usr/bin/env python3
"""Write the expected values for `common::zone`'s python-zoneinfo test.

  uv run scripts/gen-zone-fixtures.py \
      > crates/common/tests/fixtures/zone_python_zoneinfo.tsv

`crates/common/tests/zone_python_zoneinfo.rs` checks `common::zone` against
this file. The values come from Python's `zoneinfo`, an implementation that
shares no code with the Rust time zone libraries, reading the host's tz
database (the `tzdata` package or `/usr/share/zoneinfo`), so regenerate it
on a host whose database is at least as new as the one `jiff-tzdb` bundles
and review the diff.

Per zone it finds every clock change in a set of years (a 15 minute scan,
then a bisection to the minute) and writes, around each one, a row per
minute:

  offset  ZONE  INSTANT(Z)  LOCAL  OFFSET_SECONDS
  local   ZONE  LOCAL  single|ambiguous|gap  FIRST(Z)  SECOND(Z)  SHIFTED(Z)

`local` rows say what the wall-clock time means (`FIRST` is the earliest
instant, `SECOND` the later one of a repeated time, `-` when there is none)
and `SHIFTED` is the earliest instant of the time, or of the time an hour
later if it does not exist. A repeated or skipped time is found by
converting both readings (`fold` 0 and 1) to UTC and back. Zones with no
clock change get a few spot rows instead.
Stdlib only.
"""

import sys
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

ZONES = (
    "Europe/London",
    "Europe/Dublin",
    "America/New_York",
    "Asia/Kolkata",
    "Australia/Lord_Howe",
)
# Years whose clock changes are written with a full window: both UK change
# days of the two years the repository pins.
FULL_YEARS = (2025, 2026)
# Far past and future: the first years of British Summer Time all year, the
# end of the 32 bit era (2037, 2038) and the turn of the century (2099, 2100,
# 2101: chrono-tz stops listing transitions after 2099 and stays on winter
# time, so these are where only the zone's rule gives the right answer).
FAR_YEARS = (1970, 1971, 2037, 2038, 2099, 2100)
FULL_WINDOW_MINUTES = 62
FAR_WINDOW_MINUTES = 5
SCAN_STEP = timedelta(minutes=15)
MINUTE = timedelta(minutes=1)
HOUR = timedelta(hours=1)
SPOT_TIMES = ((1, 15), (7, 15))
SPOT_YEARS = (1970, 2026, 2037, 2099, 2101)


def iso(value: datetime) -> str:
    """Return `value` (naive) as `YYYY-MM-DDTHH:MM:SS`."""
    return value.replace(tzinfo=None).isoformat(timespec="seconds")


def iso_utc(value: datetime) -> str:
    """Return `value` (aware) as `YYYY-MM-DDTHH:MM:SSZ`."""
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def offset_seconds(zone: ZoneInfo, instant: datetime) -> int:
    """Return the zone's UTC offset at `instant` in seconds."""
    offset = instant.astimezone(zone).utcoffset()
    assert offset is not None  # noqa: S101  # zoneinfo always has one
    return int(offset.total_seconds())


def transitions(zone: ZoneInfo, year: int) -> list[datetime]:
    """Return the instants in `year` (UTC) at which the offset changes."""
    found: list[datetime] = []
    at = datetime(year, 1, 1, tzinfo=UTC)
    end = datetime(year + 1, 1, 1, tzinfo=UTC)
    while at < end:
        nxt = at + SCAN_STEP
        if offset_seconds(zone, at) != offset_seconds(zone, nxt):
            low, high = at, nxt
            while high - low > MINUTE:
                mid = low + (high - low) / 2
                mid = mid.replace(second=0, microsecond=0)
                if offset_seconds(zone, mid) == offset_seconds(zone, low):
                    low = mid
                else:
                    high = mid
            found.append(high)
        at = nxt
    return found


def readings(zone: ZoneInfo, local: datetime) -> list[datetime]:
    """Return the instants (UTC) at which `zone`'s clock shows `local`."""
    found: list[datetime] = []
    for fold in (0, 1):
        instant = local.replace(tzinfo=zone, fold=fold).astimezone(UTC)
        back = instant.astimezone(zone).replace(tzinfo=None)
        if back == local and instant not in found:
            found.append(instant)
    return sorted(found)


def local_row(zone: ZoneInfo, name: str, local: datetime) -> str:
    """Return the `local` row for the wall-clock time `local`."""
    found = readings(zone, local)
    if not found:
        kind, first, second = "gap", "-", "-"
        shifted = readings(zone, local + HOUR)[0]
    elif len(found) == 1:
        kind, first, second = "single", iso_utc(found[0]), "-"
        shifted = found[0]
    else:
        kind, first, second = "ambiguous", iso_utc(found[0]), iso_utc(found[1])
        shifted = found[0]
    return "\t".join(
        ("local", name, iso(local), kind, first, second, iso_utc(shifted)),
    )


def offset_row(zone: ZoneInfo, name: str, instant: datetime) -> str:
    """Return the `offset` row for `instant`."""
    local = instant.astimezone(zone)
    return "\t".join(
        (
            "offset",
            name,
            iso_utc(instant),
            iso(local),
            str(offset_seconds(zone, instant)),
        ),
    )


def window(
    zone: ZoneInfo,
    name: str,
    change: datetime,
    minutes: int,
) -> list[str]:
    """Return rows for every minute around the clock change at `change`."""
    rows: list[str] = []
    before = offset_seconds(zone, change - MINUTE)
    centre = (change + timedelta(seconds=before)).replace(tzinfo=None)
    steps = range(-minutes, minutes + 1)
    rows.extend(offset_row(zone, name, change + step * MINUTE) for step in steps)
    rows.extend(local_row(zone, name, centre + step * MINUTE) for step in steps)
    return rows


def spot_rows(zone: ZoneInfo, name: str) -> list[str]:
    """Return rows for a few ordinary instants of each spot year."""
    rows: list[str] = []
    for year in SPOT_YEARS:
        for month, day in SPOT_TIMES:
            instant = datetime(year, month, day, 12, 0, tzinfo=UTC)
            rows.append(offset_row(zone, name, instant))
            rows.append(
                local_row(zone, name, instant.astimezone(zone).replace(tzinfo=None))
            )
    return rows


def main() -> int:
    """Write the fixture to standard output."""
    out = [
        "# Expected values for common::zone, from Python's zoneinfo.",
        "# Generated by scripts/gen-zone-fixtures.py; do not edit by hand.",
        "# Row formats are in that script.",
    ]
    for name in ZONES:
        zone = ZoneInfo(name)
        for year, minutes in [(y, FULL_WINDOW_MINUTES) for y in FULL_YEARS] + [
            (y, FAR_WINDOW_MINUTES) for y in FAR_YEARS
        ]:
            for change in transitions(zone, year):
                out.extend(window(zone, name, change, minutes))
        out.extend(spot_rows(zone, name))
    sys.stdout.write("\n".join(out) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
