//! TRUST movement-feed message parsing. Field shapes are drawn only from
//! what docs/superpowers/specs/2026-08-28-train-tracking-design.md's
//! research pass independently confirmed (five of eight `msg_types`, by
//! name and field), plus `0005` (Reinstatement -- see the H4 finding of
//! the 2026-09-26 review, and the `Reinstatement` struct's own doc comment
//! below for why this one additional type is now modeled too). `0008` alone
//! remains unconfirmed and parses into `TrustMessage::Unknown` rather than
//! being guessed at -- per this codebase's "no invented API details"
//! convention.
//!
//! One thing that research pass got wrong: it claimed TRUST delivers a
//! JSON array of `{header, body}` envelopes per batch. A real RDM Train
//! Movements Kafka consumer run against `local.env` proved otherwise --
//! every record's payload was a single bare `{header, body}` object, which
//! made the old array-only `serde_json::from_str::<Vec<Envelope>>` fail
//! with `invalid type: map, expected a sequence` on every message. `parse_batch`
//! below accepts either shape defensively, since a single live data point
//! disproving "always an array" doesn't prove "never an array" either.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct Envelope {
    header: Header,
    body: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
struct Header {
    msg_type: String,
}

/// `train_id`/`train_uid` are the only two fields any consumer of this type
/// actually *depends* on: together they are the whole point of a `0001`
/// (bind a live TRUST identity to a CIF schedule identity), and
/// `trust-consumer`'s `by_train_uid` fast path -- the single most reliable
/// way a tracked subscription is ever attributed to a running train -- is
/// unreachable without them.
///
/// **Every other field is `Option<String>`, deliberately** (finding #7 of
/// the 2026-09-25 review). They used to be required `String`s, faithful to
/// the confirmed wire shape, even though only `toc_id` has a reader at all
/// (`full-coverage-consumer`'s `station_correlate::apply_activation`, which
/// already treats "no `toc_id` learned for this uid" as a first-class case).
/// Required fields make deserialization all-or-nothing: a single `null` or
/// absent `schedule_wtt_id` -- a feed schema tweak, a VSTP activation, a
/// TOC-specific quirk -- made `parse_envelope` drop the WHOLE Activation,
/// silently costing the `by_train_uid` fast path for that train and falling
/// the subscription through to the far weaker CRS+time departure heuristic
/// (finding #1). Trading a field this codebase never reads for the fast
/// path is not a trade worth making, so these are now all optional and a
/// partial Activation still binds its identity.
///
/// `schedule_start_date`/`schedule_end_date` in particular are the CIF
/// schedule's own multi-month VALIDITY WINDOW, not the date this specific
/// train instance is running -- see `trust-backlog-consumer`'s
/// `process.rs` module doc for the live-production bug that confirmed it.
/// Nothing may use either as "which day is this Activation for";
/// `trust-consumer` dates an Activation by the rail day it was observed on
/// (`common::rail_day::current_rail_day`) instead.
#[derive(Debug, Clone, Deserialize)]
pub struct Activation {
    pub train_id: String,
    pub train_uid: String,
    /// Read by `full-coverage-consumer` (`station_correlate::apply_activation`)
    /// -- the one non-identity field with a real consumer. `None` simply
    /// means that consumer learns no `toc_id` for this uid, which it
    /// already handles.
    pub toc_id: Option<String>,
    pub train_service_code: Option<String>,
    pub schedule_wtt_id: Option<String>,
    /// The CIF schedule's validity-window START, NOT this instance's
    /// running date -- see this struct's own doc comment. No reader.
    pub schedule_start_date: Option<String>,
    /// The CIF schedule's validity-window END, months away for a permanent
    /// schedule. Read only as a secondary pruning signal for
    /// `trust-consumer`'s parked-Activation map (see
    /// `process::prune_expired_activations`, whose primary rule is now the
    /// Activation's own observed rail day).
    pub schedule_end_date: Option<String>,
    /// The date (`YYYY-MM-DD`) this train instance departs its origin: the
    /// CIF running date, which is the `service_date` convention the rest
    /// of the system keys a train on. Unlike `schedule_start_date`, this
    /// IS per-instance. Checked against the live production feed on
    /// 2026-09-27: over 13,424 Activations it matched the London calendar
    /// date of `creation_timestamp` except for 39 overnight trains. There
    /// it was the origin date: the next day for a train activated before
    /// midnight to depart after it, and the previous day for a train
    /// activated after midnight that had departed before it. Read by
    /// `trust-backlog-consumer` to date its Activation rows (Repeater Signal
    /// M7). A plain date, not an epoch timestamp, so it is immune to the
    /// `common::trust_timestamp` corruption.
    pub tp_origin_timestamp: Option<String>,
}

// `reporting_stanox`/`toc_id` are part of `0003`'s confirmed shape but
// have no consumer yet -- see the Activation comment above for why they're
// kept rather than deleted. `gbtt_timestamp` (the public-timetable time)
// is stored by trust-consumer on `train_movement_events`.
#[derive(Debug, Clone, Deserialize)]
pub struct Movement {
    pub train_id: String,
    pub event_type: String, // ARRIVAL | DEPARTURE | PASS
    pub gbtt_timestamp: Option<String>,
    pub planned_timestamp: Option<String>,
    pub actual_timestamp: Option<String>,
    pub reporting_stanox: Option<String>,
    pub loc_stanox: Option<String>,
    pub toc_id: Option<String>,
    pub variation_status: Option<String>,
    /// TRUST's own lateness in whole minutes, as a string (`"12"`), against
    /// the working timetable at this location. Read only when
    /// `variation_status` is `LATE` -- see [`movement_delay_minutes`]. Free
    /// of the local-as-UTC timestamp skew `common::trust_timestamp` guards
    /// against, because it is a difference TRUST computed itself.
    #[serde(default)]
    pub timetable_variation: Option<String>,
}

/// A Movement's delay in minutes: TRUST's `timetable_variation` for
/// `LATE`, `0` for `ON TIME` and `EARLY` (an early train is not delayed),
/// and `None` for anything else (`OFF ROUTE`, a missing status, or a
/// `LATE` whose `timetable_variation` is missing or unparseable) -- a
/// report that says nothing about lateness.
pub fn movement_delay_minutes(movement: &Movement) -> Option<i32> {
    match movement.variation_status.as_deref() {
        Some("ON TIME" | "EARLY") => Some(0),
        Some("LATE") => movement
            .timetable_variation
            .as_deref()
            .and_then(|v| v.trim().parse::<i32>().ok())
            .map(|minutes| minutes.max(0)),
        _ => None,
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Cancellation {
    pub train_id: String,
    pub canx_timestamp: Option<String>,
    /// The delay attribution code for the cancellation (e.g. `"TG"`). Read
    /// by `trust-backlog-consumer`, which forwards it to `api`'s
    /// `train_reasons` table. Present on every `0002` in a 2026-09-27
    /// sample of the live feed; `None` is still handled.
    pub canx_reason_code: Option<String>,
    /// `"AT ORIGIN"`, `"EN ROUTE"`, `"ON CALL"` or `"OUT OF PLAN"`.
    pub canx_type: Option<String>,
    /// The planned departure (epoch milliseconds, as a string, with the
    /// same local-as-UTC skew as every TRUST timestamp -- parse it with
    /// `common::trust_timestamp`) at the location the train is cancelled
    /// FROM: the train does not run beyond it. Read by the full-coverage
    /// consumer to tell a cancellation before a line from one after it.
    #[serde(default)]
    pub dep_timestamp: Option<String>,
    /// The STANOX the train is cancelled from.
    #[serde(default)]
    pub loc_stanox: Option<String>,
}

/// TRUST `0006`, Change of Origin: the train now starts from `loc_stanox`,
/// departing at `dep_timestamp` (same encoding as
/// [`Cancellation::dep_timestamp`]).
#[derive(Debug, Clone, Deserialize)]
pub struct ChangeOfOrigin {
    pub train_id: String,
    #[serde(default)]
    pub dep_timestamp: Option<String>,
    #[serde(default)]
    pub loc_stanox: Option<String>,
    /// The delay attribution code for the change of origin (e.g. `"YI"`).
    /// Absent on some messages: 76 of 82 carried it in a 2026-09-27 sample
    /// of the live feed.
    #[serde(default)]
    pub reason_code: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChangeOfIdentity {
    pub train_id: String,
}

/// TRUST message type `0005`, Train Reinstatement: sent when a previously
/// cancelled service resumes running (the H4 finding of the 2026-09-26
/// review). This codebase's original research pass (see this module's own
/// header doc) flagged `0005` as unconfirmed, since a "community summary"
/// it found named it "Unidentified Train" rather than "Reinstatement" and
/// that wasn't checked against a primary source. Network Rail's own
/// published Train Movements message list (the same `TRAIN_MVT_ALL_TOC`
/// product this whole crate targets) names `0005` as Train Reinstatement,
/// independent of that unverified community summary -- so unlike `0008`
/// (still genuinely unresolved either way), this one type is now confirmed
/// by name.
///
/// Only `train_id` is modeled, same minimal posture as `ChangeOfOrigin`/
/// `ChangeOfIdentity` above: it is the one field any consumer of this type
/// can depend on (the join key back to the cancelled journey this
/// reinstates), and this pass has no independently-confirmed source for
/// this message's other fields (a real reinstatement timestamp, an
/// `original_loc_stanox`, etc. are plausible but would be guessing at
/// exact field names/shapes, which this codebase's "no invented API
/// details" convention rules out same as it did for `0005` as a whole
/// before this fix).
#[derive(Debug, Clone, Deserialize)]
pub struct Reinstatement {
    pub train_id: String,
    /// The planned departure at the reinstatement location (same encoding
    /// as [`Cancellation::dep_timestamp`]). Optional: nothing depends on
    /// it yet, and a body without it still parses.
    #[serde(default)]
    pub dep_timestamp: Option<String>,
    /// When the reinstatement was made (same encoding as
    /// [`Cancellation::canx_timestamp`]). Seen on every `0005` in a live
    /// production sample (`dep_timestamp division_code loc_stanox
    /// reinstatement_timestamp toc_id train_id train_service_code`). It is
    /// the field that tells two reinstatements of one train apart:
    /// `dep_timestamp` is the PLANNED departure and repeats. Fills the
    /// timestamp slot of the `0005` dedup key in both consumers.
    #[serde(default)]
    pub reinstatement_timestamp: Option<String>,
}

#[derive(Debug, Clone)]
pub enum TrustMessage {
    Activation(Activation),
    Movement(Movement),
    Cancellation(Cancellation),
    ChangeOfOrigin(ChangeOfOrigin),
    ChangeOfIdentity(ChangeOfIdentity),
    Reinstatement(Reinstatement),
    /// Any `msg_type` this pass doesn't confirm the shape of (`0008`, or
    /// anything else RDM's schema turns out to send). Carries the raw
    /// `msg_type` string for logging; the raw body is intentionally
    /// dropped here since there's no confirmed shape to hold it in.
    Unknown(String),
}

/// A real Kafka record's payload delivers a single `{header, body}` envelope
/// object -- confirmed by a live production error (see this module's header
/// doc). Defensive handling for a JSON array of envelopes is kept too, since
/// the design doc's research claimed that shape and it hasn't been *proven*
/// to never occur, e.g. on some other topic/product variant. Either way, one
/// malformed envelope inside an otherwise-good payload is logged and
/// skipped, not treated as a reason to drop everything else.
///
/// Dispatches on `serde_json::Value::is_array` rather than an
/// `#[serde(untagged)]` enum: untagged deserialization was tried first, but
/// on a genuinely malformed payload (wrong shape, not just wrong type) it
/// collapses both attempts into a single unhelpful "data did not match any
/// variant of untagged enum" with no field-level detail -- confirmed by
/// hand against this exact struct shape. Parsing to `Value` first and then
/// routing through `serde_json::from_value` keeps serde's normal, specific
/// field-level errors (e.g. "missing field `header`") for a shape that's
/// neither of the two expected ones.
pub fn parse_batch(raw: &str) -> anyhow::Result<Vec<TrustMessage>> {
    Ok(parse_batch_detailed(raw)?.messages)
}

/// One envelope [`parse_batch_detailed`] could not turn into a
/// [`TrustMessage`] (finding PL-8 of the 2026-09-27 pipelines review).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvelopeFailure {
    /// The envelope's `header.msg_type`, or [`MISSING_MSG_TYPE`] when it
    /// has none. Always one of the confirmed types or that marker, so it is
    /// safe as a metric label: an unconfirmed type is `Unknown`, never a
    /// failure.
    pub msg_type: String,
    pub error: String,
}

/// [`EnvelopeFailure::msg_type`] for an envelope inside an array payload
/// that has no `header.msg_type` at all.
pub const MISSING_MSG_TYPE: &str = "missing";

/// Every `msg_type` an [`EnvelopeFailure`] can carry: the typed TRUST
/// messages `parse_envelope` checks, plus [`MISSING_MSG_TYPE`]. Consumers
/// register their `*_errors_total{operation="parse_envelope",msg_type}`
/// series at 0 for each, so the parse-drop alert's `increase()` sees the
/// first failure too (R-097).
pub const ENVELOPE_FAILURE_MSG_TYPES: [&str; 7] = [
    "0001",
    "0002",
    "0003",
    "0005",
    "0006",
    "0007",
    MISSING_MSG_TYPE,
];

/// Every message a payload yielded, plus every envelope that was dropped.
#[derive(Debug, Default)]
pub struct ParsedBatch {
    pub messages: Vec<TrustMessage>,
    pub failures: Vec<EnvelopeFailure>,
}

/// [`parse_batch`], but reporting each dropped envelope instead of only
/// logging it (finding PL-8). A confirmed `msg_type` whose body fails its
/// typed shape used to vanish with at most a log line; a feed-wide schema
/// change looked like "trains stopped moving". Callers count
/// `failures` into their own `*_errors_total{operation="parse_envelope",
/// msg_type}`; this function logs each failing `msg_type` at WARN once per
/// process (so a feed-wide change does not flood the log at feed rate).
///
/// `Err` only when the payload as a whole is unusable: invalid JSON, or a
/// bare single envelope with no `header`. Inside an array, an envelope with
/// no `header.msg_type` is one failure, not a reason to drop the other
/// envelopes (finding PL-4).
pub fn parse_batch_detailed(raw: &str) -> anyhow::Result<ParsedBatch> {
    let value: serde_json::Value = serde_json::from_str(raw)?;
    let mut parsed = ParsedBatch::default();
    if let serde_json::Value::Array(values) = value {
        for value in values {
            match serde_json::from_value::<Envelope>(value) {
                Ok(envelope) => parsed.push(envelope),
                Err(err) => parsed.failures.push(EnvelopeFailure {
                    msg_type: MISSING_MSG_TYPE.to_string(),
                    error: err.to_string(),
                }),
            }
        }
    } else {
        parsed.push(serde_json::from_value::<Envelope>(value)?);
    }
    for failure in &parsed.failures {
        warn_once(failure);
    }
    Ok(parsed)
}

impl ParsedBatch {
    fn push(&mut self, envelope: Envelope) {
        match parse_envelope(envelope) {
            Ok(message) => self.messages.push(message),
            Err(failure) => self.failures.push(failure),
        }
    }
}

/// WARN the first failure of each `msg_type` this process sees; later ones
/// only reach the caller's counter.
fn warn_once(failure: &EnvelopeFailure) {
    use std::collections::HashSet;
    use std::sync::{Mutex, OnceLock};
    static SEEN: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let first = SEEN
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .map_or(true, |mut seen| seen.insert(failure.msg_type.clone()));
    if first {
        tracing::warn!(
            msg_type = %failure.msg_type,
            error = %failure.error,
            "a TRUST envelope failed to parse against its known shape; dropping it \
             (further failures of this msg_type are counted, not logged)"
        );
    }
}

/// What [`confirmed_envelope_bodies`] made of one Kafka record.
#[derive(Debug, Default)]
pub struct ClassifiedRecord {
    /// `(msg_type, payload)` for each confirmed envelope, payload
    /// re-serialized verbatim.
    pub envelopes: Vec<(String, String)>,
    /// Envelopes with no `header.msg_type`: skipped, for the caller to count.
    pub malformed: usize,
}

/// The `movement-relay` filtering primitive
/// (docs/superpowers/specs/2026-09-04-movement-relay-design.md Decision 1):
/// classifies each envelope in `raw` by `header.msg_type` alone against
/// the same confirmed types `parse_envelope` already encodes, and
/// re-serializes each SURVIVING envelope's own `serde_json::Value`
/// verbatim, byte-faithful, even in the rare multi-envelope-array case.
/// Returns `(msg_type, payload)` pairs -- `msg_type` is re-derived cheaply
/// here (rather than making every caller re-parse the returned payload
/// just to extract it again) since `movement-relay`'s own `EventSink`
/// needs it as a separate, redundant introspection field alongside the
/// raw payload (design doc Decision 2's field-layout choice).
///
/// Deliberately does NOT attempt to deserialize `body` into any typed
/// struct -- an envelope with a confirmed `msg_type` but a body that would
/// fail `parse_envelope`'s own typed deserialization (missing/malformed
/// fields) still survives here unchanged. That validation job stays where
/// it already lives, inside each downstream consumer's own `parse_batch`
/// call -- this function only ever looks at `header.msg_type`.
///
/// An envelope with no `header.msg_type` is skipped and counted in
/// [`ClassifiedRecord::malformed`], per envelope (finding PL-4 of the
/// 2026-09-27 pipelines review). It used to be a hard `Err` for the whole
/// record, which silently cost every other envelope in a TRUST batch (up
/// to ~200 movements) for one odd one. `Err` is now only for a record that
/// is not JSON at all, or whose top level is neither an envelope object nor
/// an array.
pub fn confirmed_envelope_bodies(raw: &str) -> anyhow::Result<ClassifiedRecord> {
    // `0005` (Reinstatement) joined this list in the H4 fix (2026-09-26
    // review): it used to parse as `Unknown` and get dropped right here,
    // before `trust-consumer`/`trust-backlog-consumer` ever saw it, which
    // silently discarded the one message TRUST sends to un-cancel a service
    // that resumes running. See `schema::Reinstatement`'s own doc comment
    // for why this type (unlike `0008`) is now confirmed.
    const CONFIRMED: [&str; 6] = ["0001", "0002", "0003", "0005", "0006", "0007"];

    let value: serde_json::Value = serde_json::from_str(raw)?;
    let envelopes = match value {
        serde_json::Value::Array(values) => values,
        value @ serde_json::Value::Object(_) => vec![value],
        other => anyhow::bail!(
            "a TRUST record must be an envelope object or an array of them, got {}",
            json_kind(&other)
        ),
    };

    let mut classified = ClassifiedRecord {
        envelopes: Vec::with_capacity(envelopes.len()),
        malformed: 0,
    };
    for envelope in envelopes {
        let Some(msg_type) = envelope
            .get("header")
            .and_then(|header| header.get("msg_type"))
            .and_then(|msg_type| msg_type.as_str())
            .map(str::to_string)
        else {
            classified.malformed += 1;
            continue;
        };
        if CONFIRMED.contains(&msg_type.as_str()) {
            let payload = serde_json::to_string(&envelope)?;
            classified.envelopes.push((msg_type, payload));
        }
    }
    Ok(classified)
}

fn json_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "a boolean",
        serde_json::Value::Number(_) => "a number",
        serde_json::Value::String(_) => "a string",
        serde_json::Value::Array(_) => "an array",
        serde_json::Value::Object(_) => "an object",
    }
}

fn parse_envelope(envelope: Envelope) -> Result<TrustMessage, EnvelopeFailure> {
    fn typed<T: serde::de::DeserializeOwned>(
        msg_type: &str,
        body: serde_json::Value,
        wrap: fn(T) -> TrustMessage,
    ) -> Result<TrustMessage, EnvelopeFailure> {
        serde_json::from_value(body)
            .map(wrap)
            .map_err(|err| EnvelopeFailure {
                msg_type: msg_type.to_string(),
                error: err.to_string(),
            })
    }
    let msg_type = envelope.header.msg_type.as_str();
    match msg_type {
        "0001" => typed(msg_type, envelope.body, TrustMessage::Activation),
        "0002" => typed(msg_type, envelope.body, TrustMessage::Cancellation),
        "0003" => typed(msg_type, envelope.body, TrustMessage::Movement),
        "0005" => typed(msg_type, envelope.body, TrustMessage::Reinstatement),
        "0006" => typed(msg_type, envelope.body, TrustMessage::ChangeOfOrigin),
        "0007" => typed(msg_type, envelope.body, TrustMessage::ChangeOfIdentity),
        other => Ok(TrustMessage::Unknown(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every `msg_type` a dropped envelope can be counted under is in
    /// `ENVELOPE_FAILURE_MSG_TYPES`, so the consumers pre-register them all.
    #[test]
    fn every_envelope_failure_msg_type_is_pre_registerable() {
        for msg_type in ["0001", "0002", "0003", "0005", "0006", "0007"] {
            let raw = format!(r#"[{{"header":{{"msg_type":"{msg_type}"}},"body":42}}]"#);
            let parsed = parse_batch_detailed(&raw).unwrap();
            assert_eq!(parsed.failures.len(), 1, "{msg_type}");
            assert!(ENVELOPE_FAILURE_MSG_TYPES.contains(&parsed.failures[0].msg_type.as_str()));
        }
        let parsed = parse_batch_detailed(r#"[{"body":{}}]"#).unwrap();
        assert_eq!(parsed.failures[0].msg_type, MISSING_MSG_TYPE);
        assert!(ENVELOPE_FAILURE_MSG_TYPES.contains(&MISSING_MSG_TYPE));
    }

    #[test]
    fn parses_an_activation_message() {
        let raw = r#"[{"header":{"msg_type":"0001"},"body":{
            "train_id":"221832406","train_uid":"C21373","toc_id":"SW",
            "train_service_code":"22345000","schedule_wtt_id":"WTT1",
            "schedule_start_date":"2026-08-28","schedule_end_date":"2026-08-28"
        }}]"#;
        let messages = parse_batch(raw).unwrap();
        assert_eq!(messages.len(), 1);
        assert!(matches!(&messages[0], TrustMessage::Activation(a) if a.train_uid == "C21373"));
    }

    /// Finding #7's regression test: an Activation carrying ONLY the two
    /// fields anything actually depends on still parses. Before these four
    /// fields became `Option<String>`, this exact payload failed the whole
    /// envelope's deserialization -- which silently cost `trust-consumer`'s
    /// `by_train_uid` fast path for that train and dropped it through to
    /// the much weaker CRS+time departure heuristic instead.
    #[test]
    fn an_activation_missing_every_field_but_the_identity_pair_still_parses() {
        let raw = r#"[{"header":{"msg_type":"0001"},"body":{
            "train_id":"221832406","train_uid":"C21373"
        }}]"#;
        let messages = parse_batch(raw).unwrap();
        assert_eq!(messages.len(), 1);
        let TrustMessage::Activation(activation) = &messages[0] else {
            panic!("expected an Activation, got {:?}", messages[0]);
        };
        assert_eq!(activation.train_uid, "C21373");
        assert_eq!(activation.toc_id, None);
        assert_eq!(activation.schedule_end_date, None);
    }

    /// Same, for an explicit `null` rather than an absent key -- the shape a
    /// feed that models these fields but has nothing to put in them sends.
    #[test]
    fn an_activation_with_explicit_nulls_for_the_optional_fields_still_parses() {
        let raw = r#"[{"header":{"msg_type":"0001"},"body":{
            "train_id":"221832406","train_uid":"C21373","toc_id":null,
            "train_service_code":null,"schedule_wtt_id":null,
            "schedule_start_date":null,"schedule_end_date":null
        }}]"#;
        let messages = parse_batch(raw).unwrap();
        assert!(matches!(&messages[0], TrustMessage::Activation(a) if a.train_uid == "C21373"));
    }

    #[test]
    fn parses_a_movement_message() {
        let raw = r#"[{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"DEPARTURE",
            "planned_timestamp":"1756400000000","actual_timestamp":"1756400060000",
            "loc_stanox":"87701","variation_status":"LATE"
        }}]"#;
        let messages = parse_batch(raw).unwrap();
        assert_eq!(messages.len(), 1);
        assert!(matches!(&messages[0], TrustMessage::Movement(m) if m.event_type == "DEPARTURE"));
    }

    /// The exact shape a real RDM Train Movements Kafka record delivers: a
    /// single bare `{header, body}` object, NOT wrapped in an array. This is
    /// the payload shape that triggered the live `invalid type: map,
    /// expected a sequence` error against `parse_batch`'s old
    /// array-only `serde_json::from_str::<Vec<Envelope>>` -- without the
    /// single/array dispatch above, this exact input reproduces that
    /// failure.
    #[test]
    fn parses_a_single_bare_envelope_object_not_wrapped_in_an_array() {
        let raw = r#"{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"DEPARTURE",
            "planned_timestamp":"1756400000000","actual_timestamp":"1756400060000",
            "loc_stanox":"87701","variation_status":"LATE"
        }}"#;
        let messages = parse_batch(raw).unwrap();
        assert_eq!(messages.len(), 1);
        assert!(matches!(&messages[0], TrustMessage::Movement(m) if m.event_type == "DEPARTURE"));
    }

    /// A payload that is neither a bare envelope object nor an array of
    /// them (e.g. missing `header` entirely) must fail with a specific,
    /// actionable field-level error -- not serde's generic untagged-enum
    /// "data did not match any variant" message, which names no field and
    /// gives no hint what's wrong.
    #[test]
    fn a_malformed_payload_produces_a_specific_field_level_error() {
        let raw = r#"{"not_an_envelope": true}"#;
        let err = parse_batch(raw).unwrap_err();
        assert!(
            err.to_string().contains("header"),
            "expected a field-level error mentioning `header`, got: {err}"
        );
    }

    #[test]
    fn unconfirmed_msg_types_become_unknown_not_a_parse_error() {
        // `0008` ("Change of Location"), not `0005`: the H4 fix confirmed
        // `0005` (Reinstatement), see `parses_a_reinstatement_message` below
        // and `Reinstatement`'s own doc comment for why.
        let raw = r#"[{"header":{"msg_type":"0008"},"body":{"anything":"goes"}}]"#;
        let messages = parse_batch(raw).unwrap();
        assert_eq!(messages.len(), 1);
        assert!(matches!(&messages[0], TrustMessage::Unknown(t) if t == "0008"));
    }

    /// The H4 finding of the 2026-09-26 review, this fix's own regression
    /// test: `0005` (Reinstatement) now parses into its own confirmed
    /// variant, not `Unknown` -- see `Reinstatement`'s doc comment for why
    /// this is a real, previously-dropped signal that a cancelled service
    /// resumed running.
    #[test]
    fn parses_a_reinstatement_message() {
        let raw = r#"[{"header":{"msg_type":"0005"},"body":{"train_id":"221832406"}}]"#;
        let messages = parse_batch(raw).unwrap();
        assert_eq!(messages.len(), 1);
        assert!(
            matches!(&messages[0], TrustMessage::Reinstatement(r) if r.train_id == "221832406")
        );
    }

    /// The fields the full-coverage consumer's windowed stats read, in
    /// the shape the live feed sends them (every value a string).
    #[test]
    fn parses_the_windowed_stats_fields_of_0002_0003_0005_and_0006() {
        let raw = r#"[
            {"header":{"msg_type":"0003"},"body":{"train_id":"722N71MW27","event_type":"ARRIVAL",
                "planned_timestamp":"1790505600000","actual_timestamp":"1790506320000",
                "loc_stanox":"87701","variation_status":"LATE","timetable_variation":"12"}},
            {"header":{"msg_type":"0002"},"body":{"train_id":"722N71MW27","canx_type":"EN ROUTE",
                "canx_reason_code":"YI","canx_timestamp":"1790506000000",
                "dep_timestamp":"1790507000000","loc_stanox":"87702"}},
            {"header":{"msg_type":"0005"},"body":{"train_id":"722N71MW27","dep_timestamp":"1790507000000"}},
            {"header":{"msg_type":"0006"},"body":{"train_id":"722N71MW27",
                "dep_timestamp":"1790508000000","loc_stanox":"87703","reason_code":"YI"}}
        ]"#;
        let messages = parse_batch(raw).unwrap();
        let TrustMessage::Movement(m) = &messages[0] else {
            panic!("{:?}", messages[0])
        };
        assert_eq!(m.timetable_variation.as_deref(), Some("12"));
        assert_eq!(movement_delay_minutes(m), Some(12));
        let TrustMessage::Cancellation(c) = &messages[1] else {
            panic!("{:?}", messages[1])
        };
        assert_eq!(c.canx_type.as_deref(), Some("EN ROUTE"));
        assert_eq!(c.dep_timestamp.as_deref(), Some("1790507000000"));
        assert_eq!(c.loc_stanox.as_deref(), Some("87702"));
        assert_eq!(c.canx_reason_code.as_deref(), Some("YI"));
        let TrustMessage::Reinstatement(r) = &messages[2] else {
            panic!("{:?}", messages[2])
        };
        assert_eq!(r.dep_timestamp.as_deref(), Some("1790507000000"));
        let TrustMessage::ChangeOfOrigin(o) = &messages[3] else {
            panic!("{:?}", messages[3])
        };
        assert_eq!(o.dep_timestamp.as_deref(), Some("1790508000000"));
        assert_eq!(o.loc_stanox.as_deref(), Some("87703"));
        assert_eq!(o.reason_code.as_deref(), Some("YI"));
    }

    /// Bodies from before these fields were read still parse, as `None`.
    #[test]
    fn bodies_without_the_windowed_stats_fields_still_parse() {
        let raw = r#"[
            {"header":{"msg_type":"0003"},"body":{"train_id":"1","event_type":"ARRIVAL","variation_status":"LATE"}},
            {"header":{"msg_type":"0002"},"body":{"train_id":"1"}},
            {"header":{"msg_type":"0006"},"body":{"train_id":"1"}}
        ]"#;
        let messages = parse_batch(raw).unwrap();
        assert_eq!(messages.len(), 3);
        let TrustMessage::Movement(m) = &messages[0] else {
            panic!()
        };
        assert_eq!(m.timetable_variation, None);
        assert_eq!(
            movement_delay_minutes(m),
            None,
            "LATE without a variation says nothing about how late"
        );
    }

    #[test]
    fn movement_delay_minutes_by_variation_status() {
        let movement = |status: Option<&str>, variation: Option<&str>| Movement {
            train_id: "1".to_string(),
            event_type: "ARRIVAL".to_string(),
            gbtt_timestamp: None,
            planned_timestamp: None,
            actual_timestamp: None,
            reporting_stanox: None,
            loc_stanox: None,
            toc_id: None,
            variation_status: status.map(str::to_string),
            timetable_variation: variation.map(str::to_string),
        };
        assert_eq!(
            movement_delay_minutes(&movement(Some("LATE"), Some("12"))),
            Some(12)
        );
        assert_eq!(
            movement_delay_minutes(&movement(Some("ON TIME"), Some("0"))),
            Some(0)
        );
        assert_eq!(
            movement_delay_minutes(&movement(Some("EARLY"), Some("3"))),
            Some(0)
        );
        assert_eq!(
            movement_delay_minutes(&movement(Some("OFF ROUTE"), Some("4"))),
            None
        );
        assert_eq!(
            movement_delay_minutes(&movement(Some("LATE"), Some("x"))),
            None
        );
    }

    #[test]
    fn a_confirmed_type_with_a_malformed_body_is_dropped_not_fatal() {
        let raw = r#"[
            {"header":{"msg_type":"0001"},"body":{"not_the_right_shape":true}},
            {"header":{"msg_type":"0001"},"body":{
                "train_id":"221832406","train_uid":"C21373","toc_id":"SW",
                "train_service_code":"22345000","schedule_wtt_id":"WTT1",
                "schedule_start_date":"2026-08-28","schedule_end_date":"2026-08-28"
            }}
        ]"#;
        let messages = parse_batch(raw).unwrap();
        assert_eq!(
            messages.len(),
            1,
            "the malformed envelope is dropped, the good one survives"
        );
    }

    #[test]
    fn a_batch_of_multiple_message_types_parses_all_of_them() {
        let raw = r#"[
            {"header":{"msg_type":"0002"},"body":{"train_id":"221832406","canx_type":"EN ROUTE"}},
            {"header":{"msg_type":"0006"},"body":{"train_id":"221832406"}},
            {"header":{"msg_type":"0007"},"body":{"train_id":"221832406"}}
        ]"#;
        let messages = parse_batch(raw).unwrap();
        assert_eq!(messages.len(), 3);
        assert!(matches!(&messages[0], TrustMessage::Cancellation(_)));
        assert!(matches!(&messages[1], TrustMessage::ChangeOfOrigin(_)));
        assert!(matches!(&messages[2], TrustMessage::ChangeOfIdentity(_)));
    }

    #[test]
    fn confirmed_envelope_bodies_keeps_confirmed_types_and_drops_unknown() {
        let raw = r#"[
            {"header":{"msg_type":"0001"},"body":{
                "train_id":"221832406","train_uid":"C21373","toc_id":"SW",
                "train_service_code":"22345000","schedule_wtt_id":"WTT1",
                "schedule_start_date":"2026-08-28","schedule_end_date":"2026-08-28"
            }},
            {"header":{"msg_type":"0008"},"body":{"anything":"goes"}},
            {"header":{"msg_type":"0003"},"body":{
                "train_id":"221832406","event_type":"DEPARTURE",
                "planned_timestamp":"1756400000000","actual_timestamp":"1756400060000",
                "loc_stanox":"87701","variation_status":"LATE"
            }}
        ]"#;
        let survivors = confirmed_envelope_bodies(raw).unwrap().envelopes;
        assert_eq!(survivors.len(), 2);
        assert_eq!(survivors[0].0, "0001");
        assert_eq!(survivors[1].0, "0003");
        for (msg_type, payload) in &survivors {
            let value: serde_json::Value = serde_json::from_str(payload).unwrap();
            assert_eq!(value["header"]["msg_type"].as_str().unwrap(), msg_type);
        }
    }

    /// The H4 fix's own regression test for the movement-relay gate: `0005`
    /// (Reinstatement) is now in `CONFIRMED` and must survive
    /// `confirmed_envelope_bodies` -- the exact function `movement-relay`
    /// calls to decide what gets published onward to
    /// `trust-consumer`/`trust-backlog-consumer`. Before this fix, `0005`
    /// was silently dropped right here, before either consumer ever saw it.
    #[test]
    fn confirmed_envelope_bodies_now_keeps_reinstatement() {
        let raw = r#"[{"header":{"msg_type":"0005"},"body":{"train_id":"221832406"}}]"#;
        let survivors = confirmed_envelope_bodies(raw).unwrap().envelopes;
        assert_eq!(survivors.len(), 1);
        assert_eq!(survivors[0].0, "0005");
    }

    /// The one the design doc's Decision 1 rationale exists to prove:
    /// `confirmed_envelope_bodies` never inspects the body, so a confirmed
    /// `msg_type` with a malformed body still survives, unlike `parse_batch`,
    /// which drops it. Both are asserted side by side against the identical
    /// input, since the two functions' different behavior on it is the point.
    #[test]
    fn confirmed_envelope_bodies_does_not_filter_on_body_shape() {
        let raw = r#"{"header":{"msg_type":"0001"},"body":{"not_the_right_shape":true}}"#;

        let survivors = confirmed_envelope_bodies(raw).unwrap().envelopes;
        assert_eq!(survivors.len(), 1, "malformed body still survives");
        assert_eq!(survivors[0].0, "0001");

        let parsed = parse_batch(raw).unwrap();
        assert_eq!(
            parsed.len(),
            0,
            "parse_batch drops the same envelope, since its body doesn't parse"
        );
    }

    #[test]
    fn confirmed_envelope_bodies_on_a_bare_single_envelope_object() {
        let raw = r#"{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"DEPARTURE",
            "planned_timestamp":"1756400000000","actual_timestamp":"1756400060000",
            "loc_stanox":"87701","variation_status":"LATE"
        }}"#;
        let survivors = confirmed_envelope_bodies(raw).unwrap().envelopes;
        assert_eq!(survivors.len(), 1);
        assert_eq!(survivors[0].0, "0003");
    }

    /// PL-4: a bare envelope with no `header` is one malformed envelope,
    /// skipped and counted -- no longer an `Err` for the record.
    #[test]
    fn confirmed_envelope_bodies_counts_a_bare_envelope_missing_header() {
        let raw = r#"{"not_an_envelope": true}"#;
        let classified = confirmed_envelope_bodies(raw).unwrap();
        assert!(classified.envelopes.is_empty());
        assert_eq!(classified.malformed, 1);
    }

    /// PL-4's regression test: one envelope without `header.msg_type` in a
    /// TRUST batch used to drop every other envelope in the record.
    #[test]
    fn one_envelope_missing_msg_type_does_not_drop_the_rest_of_the_record() {
        let raw = r#"[
            {"header":{"msg_type":"0003"},"body":{"train_id":"A","event_type":"DEPARTURE"}},
            {"header":{},"body":{"train_id":"B"}},
            {"body":{"train_id":"C"}},
            {"header":{"msg_type":7},"body":{"train_id":"D"}},
            {"header":{"msg_type":"0002"},"body":{"train_id":"E"}}
        ]"#;
        let classified = confirmed_envelope_bodies(raw).unwrap();
        let types: Vec<&str> = classified
            .envelopes
            .iter()
            .map(|(msg_type, _)| msg_type.as_str())
            .collect();
        assert_eq!(types, ["0003", "0002"]);
        assert_eq!(classified.malformed, 3);
    }

    #[test]
    fn confirmed_envelope_bodies_errors_only_on_an_unusable_top_level() {
        assert!(confirmed_envelope_bodies("not json").is_err());
        assert!(confirmed_envelope_bodies("42").is_err());
        assert!(confirmed_envelope_bodies(r#""a string""#).is_err());
        let empty = confirmed_envelope_bodies("[]").unwrap();
        assert!(empty.envelopes.is_empty());
        assert_eq!(empty.malformed, 0);
    }

    /// PL-8: a confirmed type whose body fails its shape is reported with
    /// its `msg_type` and error, not silently dropped.
    #[test]
    fn parse_batch_detailed_reports_each_dropped_envelope() {
        let raw = r#"[
            {"header":{"msg_type":"0001"},"body":{"not_the_right_shape":true}},
            {"header":{"msg_type":"0003"},"body":{"train_id":"A","event_type":"ARRIVAL"}},
            {"header":{"msg_type":"0003"},"body":{"train_id":null,"event_type":"ARRIVAL"}},
            {"header":{"msg_type":"0008"},"body":{"anything":"goes"}}
        ]"#;
        let parsed = parse_batch_detailed(raw).unwrap();
        assert_eq!(
            parsed.messages.len(),
            2,
            "the good 0003 and the Unknown 0008"
        );
        let types: Vec<&str> = parsed
            .failures
            .iter()
            .map(|f| f.msg_type.as_str())
            .collect();
        assert_eq!(types, ["0001", "0003"]);
        assert!(
            parsed.failures[0].error.contains("train_id"),
            "{:?}",
            parsed.failures[0]
        );
    }

    /// Inside an array, an envelope with no header is one failure labelled
    /// `missing`; the others still parse (PL-4 for the consumers' side).
    #[test]
    fn parse_batch_detailed_skips_an_array_envelope_without_a_header() {
        let raw = r#"[
            {"body":{"train_id":"A"}},
            {"header":{"msg_type":"0002"},"body":{"train_id":"B"}}
        ]"#;
        let parsed = parse_batch_detailed(raw).unwrap();
        assert_eq!(parsed.messages.len(), 1);
        assert_eq!(parsed.failures.len(), 1);
        assert_eq!(parsed.failures[0].msg_type, MISSING_MSG_TYPE);
    }

    #[test]
    fn a_clean_batch_reports_no_failures() {
        let raw = r#"{"header":{"msg_type":"0002"},"body":{"train_id":"B"}}"#;
        let parsed = parse_batch_detailed(raw).unwrap();
        assert_eq!(parsed.messages.len(), 1);
        assert!(parsed.failures.is_empty());
    }

    #[test]
    fn confirmed_envelope_bodies_is_byte_faithful() {
        let raw = r#"{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"DEPARTURE",
            "planned_timestamp":"1756400000000","actual_timestamp":"1756400060000",
            "loc_stanox":"87701","variation_status":"LATE",
            "an_unmodeled_field":"some real RDM data no struct declares"
        }}"#;
        let survivors = confirmed_envelope_bodies(raw).unwrap().envelopes;
        assert_eq!(survivors.len(), 1);
        let value: serde_json::Value = serde_json::from_str(&survivors[0].1).unwrap();
        assert_eq!(
            value["body"]["an_unmodeled_field"].as_str().unwrap(),
            "some real RDM data no struct declares"
        );
    }
}
