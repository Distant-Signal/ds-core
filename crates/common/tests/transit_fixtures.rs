//! Golden fixtures of the transit stream bodies (spec §7.2): each file in
//! `tests/fixtures/transit/` decodes into its `common::transit` type and
//! encodes back to the same JSON. A field renamed, retyped or dropped on
//! either side breaks this before it breaks a producer and writer of
//! different releases.

#![expect(
    clippy::unwrap_used,
    reason = "test code: a panic is the right failure in a test"
)]

use common::transit::{
    TimetableCommit, TimetablePart, TimetablePartKind, TimetableRows, TransitSituations,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

fn fixture(name: &str) -> serde_json::Value {
    let path = common::manifest_dir!()
        .join("tests/fixtures/transit")
        .join(name);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    serde_json::from_str(&text).unwrap()
}

/// Decodes `name` as `T` and checks it encodes back to the same JSON.
fn round_trip<T: DeserializeOwned + Serialize>(name: &str) -> T {
    let json = fixture(name);
    let value: T = serde_json::from_value(json.clone())
        .unwrap_or_else(|err| panic!("{name} does not decode: {err}"));
    assert_eq!(serde_json::to_value(&value).unwrap(), json, "{name}");
    value
}

#[test]
fn timetable_parts_round_trip() {
    let parts: Vec<TimetablePart> = round_trip("timetable-parts.json");
    let kinds: Vec<TimetablePartKind> = parts.iter().map(|p| p.rows.kind()).collect();
    assert_eq!(kinds, TimetablePartKind::ALL);
    let TimetableRows::Trips(trips) = &parts[4].rows else {
        panic!("the fifth part is trips");
    };
    // Past midnight, and a replacement bus with no line.
    assert!(trips[0].start_s > 86_000);
    assert!(trips[1].replacement_bus && trips[1].line_id.is_none());
}

#[test]
fn the_commit_round_trips() {
    let commit: TimetableCommit = round_trip("timetable-commit.json");
    assert_eq!(commit.parts.len(), TimetablePartKind::ALL.len());
    assert_eq!(commit.network.as_str(), "metrolink");
}

#[test]
fn situations_round_trip() {
    let set: TransitSituations = round_trip("situations.json");
    assert_eq!(set.situations.len(), 2);
}

#[test]
fn a_bad_network_or_kind_is_refused() {
    let mut part = fixture("timetable-parts.json")[0].clone();
    part["network"] = "Metrolink".into();
    assert!(serde_json::from_value::<TimetablePart>(part.clone()).is_err());
    part["network"] = "metrolink".into();
    part["kind"] = "shapes".into();
    assert!(serde_json::from_value::<TimetablePart>(part).is_err());
}
