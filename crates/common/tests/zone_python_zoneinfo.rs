//! `common::zone` against Python's `zoneinfo`.
//!
//! The fixture (`tests/fixtures/zone_python_zoneinfo.tsv`, written by
//! `scripts/gen-zone-fixtures.py`) has, for London, Dublin, New York, Kolkata
//! and Lord Howe (a half-hour clock change), a row for every minute around
//! each clock change of 2025 and 2026 and around those of 1971, 2037, 2038,
//! 2099 and 2100 (2100 is past the last explicit transition some zone
//! libraries list, so it exercises the zone's rule), plus a few ordinary
//! instants. The expected values come from an implementation that shares no
//! code with the one under test.

use chrono::{DateTime, NaiveDateTime, Utc};
use common::zone::{LocalTime, Zone};

const FIXTURE: &str = include_str!("fixtures/zone_python_zoneinfo.tsv");

fn instant(text: &str) -> DateTime<Utc> {
    text.parse()
        .unwrap_or_else(|e| panic!("instant {text:?}: {e}"))
}

fn naive(text: &str) -> NaiveDateTime {
    text.parse()
        .unwrap_or_else(|e| panic!("local time {text:?}: {e}"))
}

fn zone_named(name: &str) -> Zone {
    Zone::parse(name).unwrap_or_else(|| panic!("zone {name:?}"))
}

#[test]
fn zone_matches_python_zoneinfo() {
    let mut offsets = 0_usize;
    let mut locals = 0_usize;
    let mut gaps = 0_usize;
    let mut folds = 0_usize;
    for line in FIXTURE.lines().filter(|l| !l.starts_with('#')) {
        let cols: Vec<&str> = line.split('\t').collect();
        let zone = zone_named(cols[1]);
        match cols[0] {
            "offset" => {
                let (at, local, seconds) = (instant(cols[2]), naive(cols[3]), cols[4]);
                assert_eq!(zone.naive_local(at), local, "{line}");
                assert_eq!(zone.date(at), local.date(), "{line}");
                assert_eq!(zone.time(at), local.time(), "{line}");
                assert_eq!(
                    zone.offset_at(at).local_minus_utc().to_string(),
                    seconds,
                    "{line}"
                );
                let shown = zone.local(at);
                assert_eq!(shown.naive_local(), local, "{line}");
                assert_eq!(shown.with_timezone(&Utc), at, "{line}");
                offsets += 1;
            }
            "local" => {
                let (local, kind, shifted) = (naive(cols[2]), cols[3], instant(cols[6]));
                let (resolved, earliest) = match kind {
                    "single" => (LocalTime::Single(instant(cols[4])), Some(instant(cols[4]))),
                    "ambiguous" => {
                        folds += 1;
                        (
                            LocalTime::Ambiguous {
                                earliest: instant(cols[4]),
                                latest: instant(cols[5]),
                            },
                            Some(instant(cols[4])),
                        )
                    }
                    "gap" => {
                        gaps += 1;
                        (LocalTime::Gap, None)
                    }
                    other => panic!("kind {other:?}"),
                };
                assert_eq!(zone.resolve(local), resolved, "{line}");
                assert_eq!(zone.to_utc_earliest(local), earliest, "{line}");
                assert_eq!(zone.to_utc_gap_shifted(local), Some(shifted), "{line}");
                locals += 1;
            }
            other => panic!("row {other:?}"),
        }
    }
    // The fixture was read, and covers repeated and skipped times.
    assert!(offsets > 2_000, "{offsets} offset rows");
    assert!(locals > 2_000, "{locals} local rows");
    assert!(gaps > 200, "{gaps} skipped times");
    assert!(folds > 200, "{folds} repeated times");
}

/// The bundled database is the data source, not the host's.
#[test]
fn the_london_and_dublin_constants_resolve_in_the_bundled_database() {
    for (zone, name) in [
        (Zone::LONDON, "Europe/London"),
        (Zone::DUBLIN, "Europe/Dublin"),
    ] {
        assert_eq!(zone.name(), name);
        assert_eq!(Zone::parse(name), Some(zone));
        // Summer time: the zone has rules, so the lookup worked.
        assert_eq!(
            zone.offset_at(instant("2026-07-01T12:00:00Z"))
                .local_minus_utc(),
            3600
        );
    }
}
