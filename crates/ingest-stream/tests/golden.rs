//! Golden fixtures for the v1 envelope (spec §7.2). They pin the
//! compatibility contract between producers and the writer, which ship
//! separately (spec §13.3):
//!
//! - `envelope-v1.json`: the JSON form (`serde`) of an envelope;
//! - `wire-v1-json.json`: the same envelope's Redis fields, byte for byte
//!   (a producer must keep writing exactly this);
//! - `wire-v1-gzip.json`: a gzipped part as a v1 producer wrote it (a writer
//!   must keep decoding it; gzip bytes are not compared on encode, since
//!   they may change with the flate2 version).
//!
//! Never edit a fixture to make a test pass: a change here is a new
//! envelope or schema version.

#![expect(
    clippy::unwrap_used,
    reason = "test code: a panic is the right failure"
)]

use std::path::PathBuf;

use chrono::{TimeZone, Utc};
use ingest_stream::{BatchPart, Encoding, Envelope, SchemaId};

fn fixture(name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn wire_fields(name: &str) -> Vec<(String, Vec<u8>)> {
    let doc: serde_json::Value = serde_json::from_str(&fixture(name)).unwrap();
    let mut fields: Vec<(String, Vec<u8>)> = doc["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|pair| {
            (
                pair[0].as_str().unwrap().to_owned(),
                pair[1].as_str().unwrap().as_bytes().to_vec(),
            )
        })
        .collect();
    if let Some(hex) = doc.get("body_hex").and_then(|v| v.as_str()) {
        fields.push(("body".to_owned(), unhex(hex)));
    }
    fields
}

fn unhex(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}

fn expected_tfl() -> Envelope {
    // Typed, like the real payload structs: field order is declaration
    // order (`json!` would sort the keys).
    #[derive(serde::Serialize)]
    struct Line {
        id: &'static str,
        severity: u8,
        reason: Option<String>,
    }
    #[derive(serde::Serialize)]
    struct Body {
        lines: Vec<Line>,
    }
    let payload = Body {
        lines: vec![Line {
            id: "elizabeth",
            severity: 10,
            reason: None,
        }],
    };
    Envelope::new(
        SchemaId::new("tfl-line-status", 1).unwrap(),
        "poller-tfl/distant-signal-poller-tfl-7c4b-xyz12",
        "tfl-line-status:2026-10-06T20:20:00Z:1/1",
        Utc.with_ymd_and_hms(2026, 10, 6, 20, 20, 0).unwrap() + chrono::Duration::milliseconds(250),
        &payload,
    )
    .unwrap()
    .with_batch(BatchPart {
        batch: "2026-10-06T20:20:00Z".into(),
        part: 1,
        parts: 1,
    })
}

#[test]
fn the_json_form_matches_the_golden_fixture() {
    let golden: serde_json::Value = serde_json::from_str(&fixture("envelope-v1.json")).unwrap();
    let ours = serde_json::to_value(expected_tfl()).unwrap();
    assert_eq!(ours, golden);
    let parsed: Envelope = serde_json::from_str(&fixture("envelope-v1.json")).unwrap();
    assert_eq!(parsed.schema, expected_tfl().schema);
    assert_eq!(parsed.produced_at, expected_tfl().produced_at);
    let payload: serde_json::Value = parsed.payload_as().unwrap();
    assert_eq!(payload, golden["payload"]);
}

#[test]
fn the_plain_wire_form_is_byte_for_byte_the_golden_fixture() {
    let golden = wire_fields("wire-v1-json.json");
    let encoded = expected_tfl().encode().unwrap();
    assert_eq!(encoded.encoding, Encoding::Json);
    let ours: Vec<(String, Vec<u8>)> = encoded
        .fields
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect();
    assert_eq!(ours, golden);
    assert_eq!(Envelope::decode(&golden).unwrap(), expected_tfl());
}

#[test]
fn a_gzipped_v1_part_still_decodes() {
    let fields = wire_fields("wire-v1-gzip.json");
    let envelope = Envelope::decode(&fields).unwrap();
    assert_eq!(envelope.schema.to_string(), "station-samples/1");
    assert_eq!(envelope.key, "station-samples:2026-10-06T20:21:00Z:3/6");
    assert_eq!(
        envelope.batch,
        Some(BatchPart {
            batch: "2026-10-06T20:21:00Z".into(),
            part: 3,
            parts: 6
        })
    );
    let rows: Vec<serde_json::Value> = (0..200)
        .map(|i| serde_json::json!({"crs": format!("S{i:03}"), "delayed": i % 5, "polled_at": "2026-10-06T20:21:00Z"}))
        .collect();
    let payload: serde_json::Value = envelope.payload_as().unwrap();
    assert_eq!(payload, serde_json::json!({ "stations": rows }));

    // Re-encoding gzips again (the payload is over 8 KiB) and round-trips.
    let again = envelope.encode().unwrap();
    assert_eq!(again.encoding, Encoding::JsonGzip);
    let owned: Vec<(String, Vec<u8>)> = again
        .fields
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect();
    assert_eq!(Envelope::decode(&owned).unwrap(), envelope);
}
