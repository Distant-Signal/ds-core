//! Regression tests for the 2026-09-25 dropped-body-line warning
//! (`src/parse.rs`'s header doc: "One narrow, deliberate exception" to its
//! otherwise log-free posture). A hand-rolled minimal [`tracing::Subscriber`]
//! -- this crate deliberately has no `tracing-subscriber`/`tracing-test` dev
//! dependency, matching its own "lib-only, no extra dependencies" convention
//! (see `Cargo.toml`) -- captures every event's fields so these tests can
//! assert on the exact train UID/record-type/position a real operator would
//! see, not just "something logged."
//!
//! # Why these live in their own test binary
//!
//! They used to be a `#[cfg(test)]` module in `src/parse.rs`, and failed
//! about one run in a hundred (DS-106). `tracing-core` caches each
//! callsite's `Interest` process-wide. While only one dispatcher is alive,
//! it computes that interest from *the registering thread's* default
//! dispatcher. The lib's other parse tests hit the same `warn!` callsite
//! with no subscriber at all, so when one of them registered it first,
//! concurrently with a test here holding the only live dispatcher, it
//! cached `Interest::never` for every thread, and this test's subscriber
//! saw zero events.
//!
//! A separate integration-test binary is a separate process with its own
//! callsite cache. **Every test in this file must emit only inside
//! [`capture`]**: then any thread that registers the callsite has a live
//! [`RecordingSubscriber`] as its default (and in the dispatcher list), so
//! the cached interest is always `always`, whatever the test order or
//! thread interleaving. Adding a test here that parses a bad body line
//! without `capture` would reintroduce the race.

#![expect(
    clippy::unwrap_used,
    reason = "test code: a panic is the right failure in a test"
)]

use std::sync::{Arc, Mutex};

use schedule_query::parse_schedule_records;
use tracing::field::{Field, Visit};

#[derive(Default, Clone, Debug)]
struct CapturedEvent {
    message: String,
    train_uid: Option<String>,
    record_type: Option<String>,
    position: Option<u64>,
}

impl Visit for CapturedEvent {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "position" {
            self.position = Some(value);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "train_uid" => self.train_uid = Some(value.to_string()),
            "record_type" => self.record_type = Some(value.to_string()),
            "message" => self.message = value.to_string(),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // `tracing::warn!("literal string")`'s message field, and a
        // `%display` field like `train_uid`, both come through here on
        // some tracing versions rather than `record_str` -- covering
        // both keeps this test robust to that detail.
        let rendered = format!("{value:?}");
        let rendered = rendered.strip_prefix('"').unwrap_or(&rendered);
        let rendered = rendered.strip_suffix('"').unwrap_or(rendered);
        match field.name() {
            "train_uid" if self.train_uid.is_none() => {
                self.train_uid = Some(rendered.to_string());
            }
            "record_type" if self.record_type.is_none() => {
                self.record_type = Some(rendered.to_string());
            }
            "message" if self.message.is_empty() => {
                self.message = rendered.to_string();
            }
            _ => {}
        }
    }
}

/// A minimal [`tracing::Subscriber`] that stores every event's fields in
/// order, in a `Mutex<Vec<_>>` shared with the test. `enabled` always
/// returns `true`: these tests want every event this parser emits, not a
/// level-filtered subset.
struct RecordingSubscriber {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl tracing::Subscriber for RecordingSubscriber {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut captured = CapturedEvent::default();
        event.record(&mut captured);
        self.events.lock().unwrap().push(captured);
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

/// Parses `text` with a [`RecordingSubscriber`] as this thread's default,
/// returning the schedules and every event the parse emitted. The only way
/// a test in this file may run the parser -- see the module doc.
fn capture(text: &str) -> (Vec<schedule_query::RawSchedule>, Vec<CapturedEvent>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = RecordingSubscriber {
        events: events.clone(),
    };
    let schedules = tracing::subscriber::with_default(subscriber, || parse_schedule_records(text));
    let captured = events.lock().unwrap().clone();
    (schedules, captured)
}

const BS_C00573_PERMANENT: &str =
    "BSNC005732605172612060000001 PXX1S003101121194800 DMU    125      S A T        P";
const LO_EUSTON: &str = "LOEUSTON  0822 08227  C      TB";
const LT_EUSTON: &str = "LTEUSTON  0804 08079     TF";

fn wrap_full_block(body: &[&str]) -> String {
    let mut lines = vec![BS_C00573_PERMANENT.to_string()];
    lines.extend(body.iter().map(ToString::to_string));
    lines.join("\n")
}

#[test]
fn an_undecodable_body_line_inside_an_open_block_warns_with_train_uid_and_position() {
    // "LISHORT" is well short of MIN_LI_LEN (20) -- undecodable, and
    // dropped from a block that is otherwise open and successfully
    // parsing (LO_EUSTON before it, LT_EUSTON after it).
    let text = wrap_full_block(&[LO_EUSTON, "LISHORT", LT_EUSTON]);

    let (schedules, captured) = capture(&text);

    // The gap really is there, silently, in the returned data -- LO and
    // LT are now adjacent with no trace of the dropped LI between them.
    // This assertion documents exactly the fabricated-adjacency shape
    // the warning exists to make visible.
    assert_eq!(schedules.len(), 1);
    assert_eq!(schedules[0].calling_points.len(), 2);

    assert_eq!(
        captured.len(),
        1,
        "exactly one warning for the one dropped line, no warning for the two good ones"
    );
    assert!(
        captured[0]
            .message
            .contains("dropped an undecodable body line"),
        "unexpected message: {:?}",
        captured[0].message
    );
    assert_eq!(captured[0].train_uid.as_deref(), Some("C00573"));
    assert_eq!(captured[0].record_type.as_deref(), Some("LI"));
    assert_eq!(
        captured[0].position,
        Some(2),
        "LO_EUSTON is body position 1, the dropped LI is position 2"
    );
}

#[test]
fn a_dropped_line_with_no_open_block_stays_silent() {
    // No BS at all -- these two body lines are orphaned from the start,
    // exactly the pre-existing, already-tested "no open block" skip
    // (`parse_schedule_records`'s own `body_lines_with_no_open_block_are_dropped_without_panicking`).
    // There is no surviving block for a dropped line here to fabricate
    // an adjacency inside, so this must not warn.
    let text = format!("{LO_EUSTON}\n{LT_EUSTON}");

    let (schedules, captured) = capture(&text);

    assert!(schedules.is_empty());
    assert!(
        captured.is_empty(),
        "an orphaned body line has no open block to fabricate a connection in"
    );
}

#[test]
fn a_fully_well_formed_block_emits_no_warnings_at_all() {
    let text = wrap_full_block(&[LO_EUSTON, LT_EUSTON]);

    let (schedules, captured) = capture(&text);

    assert_eq!(schedules[0].calling_points.len(), 2);
    assert!(
        captured.is_empty(),
        "nothing was dropped, so nothing should warn"
    );
}
