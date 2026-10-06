//! Train membership ("scope") against real trains.
//!
//! `fixtures/line-membership-2026-10-06.txt` holds real 2026-10-06
//! schedules (TIPLOC paths, call flags, operator, train status) read
//! read-only from production's CIF schedule data, reduced to
//! the trains named below plus up to three of each of their lines' own
//! trains (so each line has seeds to learn its route from), the TIPLOC ->
//! CRS crosswalk rows they need, and a snapshot of the catalogue lines they
//! touch (so a later catalogue edit cannot silently change these
//! expectations; regenerate the fixture when the catalogue changes on
//! purpose). Every expectation was also checked against the full day's
//! 25,127 schedules and all 243 lines -- see
//! docs/superpowers/specs/2026-10-06-line-membership-design.md, which
//! records how the fixture was generated and the measured precision and
//! recall.

#![expect(
    clippy::unwrap_used,
    reason = "test code: a panic is the right failure in a test"
)]

use std::collections::HashMap;

use schedule_query::{
    CallingPoint, DayTrains, LineScope, MembershipLine, ResolvedSchedule, RunDirection,
    StpIndicator, classify,
};

struct Fixture {
    day: DayTrains,
    lines: Vec<MembershipLine>,
    crs: HashMap<String, String>,
}

fn list(field: &str) -> Vec<String> {
    if field == "-" {
        Vec::new()
    } else {
        field.split(',').map(str::to_string).collect()
    }
}

fn calling_point(token: &str, index: usize, last: usize) -> CallingPoint {
    let (tiploc, calls) = token
        .strip_suffix('*')
        .map_or((token, false), |t| (t, true));
    let kind = if index == 0 {
        "Origin"
    } else if index == last {
        "Terminate"
    } else {
        "Intermediate"
    };
    // A call gets a booked departure (arrival at the terminus); the time
    // itself is irrelevant to membership.
    let time = calls.then_some("12:00:00");
    let (arrival, departure) = if kind == "Terminate" {
        (time, None)
    } else {
        (None, time)
    };
    serde_json::from_value(serde_json::json!({
        "tiploc": tiploc,
        "kind": kind,
        "booked_arrival": arrival,
        "booked_departure": departure,
        "is_half_minute_arrival": false,
        "is_half_minute_departure": false,
    }))
    .unwrap()
}

fn load() -> Fixture {
    let text = include_str!("fixtures/line-membership-2026-10-06.txt");
    let mut day = DayTrains::new();
    let mut lines = Vec::new();
    let mut crs = HashMap::new();
    for row in text
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
    {
        let fields: Vec<&str> = row.split(' ').collect();
        match fields[0] {
            "line" => lines.push(MembershipLine {
                id: fields[1].to_string(),
                operators: list(fields[2]),
                trunk_for: list(fields[3]),
                crs_aliases: list(fields[4])
                    .iter()
                    .map(|pair| {
                        let (from, to) = pair.split_once(':').unwrap();
                        (from.to_string(), to.to_string())
                    })
                    .collect(),
                stations: list(fields[5]),
            }),
            "crs" => {
                crs.insert(fields[1].to_string(), fields[2].to_string());
            }
            "train" => {
                let tokens = &fields[4..];
                let last = tokens.len() - 1;
                day.push(&ResolvedSchedule {
                    uid: fields[1].to_string(),
                    stp_indicator: StpIndicator::Permanent,
                    cancelled: false,
                    calling_points: tokens
                        .iter()
                        .enumerate()
                        .map(|(i, t)| calling_point(t, i, last))
                        .collect(),
                    operator_atoc: (fields[2] != "-").then(|| fields[2].to_string()),
                    headcode: None,
                    rsid: None,
                    train_status: fields[3].chars().next().filter(|c| *c != '-'),
                    train_category: None,
                });
            }
            other => panic!("unknown fixture row kind {other}"),
        }
    }
    Fixture { day, lines, crs }
}

/// `(scope, first, last, direction)` of `uid` on `line`, or `None` when the
/// train is not in that line's population at all.
type Got = Option<(
    LineScope,
    Option<String>,
    Option<String>,
    Option<RunDirection>,
)>;

fn classified() -> HashMap<(String, String), Got> {
    let fixture = load();
    let out = classify(&fixture.day, &fixture.lines, &fixture.crs);
    let mut map = HashMap::new();
    for (line, members) in fixture.lines.iter().zip(&out) {
        for (train, m) in &members.members {
            map.insert(
                (line.id.clone(), fixture.day.uid(*train).to_string()),
                Some((
                    m.scope,
                    m.run_first_crs.clone(),
                    m.run_last_crs.clone(),
                    m.direction,
                )),
            );
        }
    }
    map
}

fn assert_scopes(line: &str, cases: &[(&str, LineScope, &str)]) {
    let got = classified();
    let mut wrong = Vec::new();
    for (uid, want, why) in cases {
        let actual = got
            .get(&(line.to_string(), (*uid).to_string()))
            .cloned()
            .flatten();
        if actual.as_ref().map(|a| a.0) != Some(*want) {
            wrong.push(format!("{uid} ({why}): want {want:?}, got {actual:?}"));
        }
    }
    assert!(wrong.is_empty(), "{line}:\n{}", wrong.join("\n"));
}

fn run_of(line: &str, uid: &str) -> (Option<String>, Option<String>, Option<RunDirection>) {
    let got = classified();
    let (_, first, last, direction) = got
        .get(&(line.to_string(), uid.to_string()))
        .cloned()
        .flatten()
        .unwrap_or_else(|| panic!("{uid} not in {line}'s population"));
    (first, last, direction)
}

use LineScope::{Line, Shared, Touch};

#[test]
fn south_west_main_line() {
    assert_scopes(
        "swr-south-west-main",
        &[
            ("L80147", Line, "1W31 Waterloo-Weymouth"),
            ("L83073", Line, "2L37 Basingstoke stopper via Walton"),
            ("L81405", Line, "2F19 Waterloo-Woking"),
            (
                "L79932",
                Line,
                "1T29 Waterloo-Portsmouth Harbour via Eastleigh",
            ),
            ("L79693", Shared, "1P49: SWR's Portsmouth Direct train"),
            ("L82889", Shared, "2K17: SWR suburban"),
            ("L79355", Shared, "1L27: West of England line"),
            ("G01043", Shared, "XC 1O68 along Basingstoke-Bournemouth"),
            ("G07337", Touch, "touches a hub only"),
            ("G77279", Touch, "touches a hub only"),
            ("C48503", Touch, "touches a hub only"),
            ("L79296", Touch, "touches a hub only"),
        ],
    );
    assert_eq!(
        run_of("swr-south-west-main", "L80147"),
        (
            Some("WAT".to_string()),
            Some("WEY".to_string()),
            Some(RunDirection::Down)
        )
    );
}

#[test]
fn leeds_york() {
    assert_scopes(
        "northern-leeds-york",
        &[
            ("P28363", Line, "2T15"),
            ("G88971", Line, "1B24"),
            ("C32232", Touch, "via Harrogate"),
            ("C24924", Shared, "another operator along Leeds-York"),
            ("L89607", Shared, "2K24"),
        ],
    );
}

#[test]
fn cathcart_circle() {
    assert_scopes(
        "scotrail-cathcart-circle",
        &[
            ("W70101", Line, "Glasgow Central-Neilston"),
            ("W69152", Line, "round the circle"),
            ("W84232", Touch, "Edinburgh-Glasgow Central via Rutherglen"),
        ],
    );
    assert_eq!(
        run_of("scotrail-cathcart-circle", "W69152"),
        (
            Some("GLC".to_string()),
            Some("GLC".to_string()),
            Some(RunDirection::Loop)
        )
    );
}

#[test]
fn thameslink_core() {
    // W45556 calls at St Pancras's Thameslink platforms (STPXBOX, CRS SPL):
    // the line's `crs_aliases` make that STP.
    assert_scopes(
        "thameslink-core",
        &[("W45556", Line, "9T39"), ("C09987", Line, "9O35")],
    );
    assert_eq!(
        run_of("thameslink-core", "W45556"),
        (
            Some("STP".to_string()),
            Some("LBG".to_string()),
            Some(RunDirection::Down)
        )
    );
}

#[test]
fn elizabeth_line() {
    assert_scopes(
        "elizabeth-line",
        &[
            ("C37401", Line, "Reading-Abbey Wood through the core"),
            ("C38230", Shared, "Shenfield arm"),
            ("W10895", Shared, "Heathrow arm"),
        ],
    );
    // The core is matched through `crs_aliases` (PDX/FDX/LSX/WHX/ABX).
    assert_eq!(
        run_of("elizabeth-line", "C37401"),
        (
            Some("RDG".to_string()),
            Some("ABW".to_string()),
            Some(RunDirection::Down)
        )
    );
}

#[test]
fn east_coast_main_line_through_trunk_for() {
    // King's Cross-Leeds: its best fit is lner-leeds, which lner-ecml lists
    // in `trunk_for`.
    assert_scopes("lner-ecml", &[("G08131", Line, "KGX-LDS")]);
}

#[test]
fn windsor_branch_and_island_line() {
    assert_scopes("gwr-windsor-branch", &[("C48713", Line, "2W35")]);
    assert_scopes(
        "swr-island-line",
        &[("G21519", Line, "Ryde Pier Head-Shanklin, operator IL")],
    );
}
