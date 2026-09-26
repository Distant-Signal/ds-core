//! TRUST movement-feed message parsing. Field shapes are drawn only from
//! what docs/superpowers/specs/2026-08-28-train-tracking-design.md's
//! research pass independently confirmed (five of eight msg_types, by
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
/// already treats "no toc_id learned for this uid" as a first-class case).
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
    #[allow(dead_code)]
    pub train_service_code: Option<String>,
    #[allow(dead_code)]
    pub schedule_wtt_id: Option<String>,
    /// The CIF schedule's validity-window START, NOT this instance's
    /// running date -- see this struct's own doc comment. No reader.
    #[allow(dead_code)]
    pub schedule_start_date: Option<String>,
    /// The CIF schedule's validity-window END, months away for a permanent
    /// schedule. Read only as a secondary pruning signal for
    /// `trust-consumer`'s parked-Activation map (see
    /// `process::prune_expired_activations`, whose primary rule is now the
    /// Activation's own observed rail day).
    pub schedule_end_date: Option<String>,
}

// `gbtt_timestamp`/`reporting_stanox`/`toc_id` are part of `0003`'s
// confirmed shape but have no consumer yet -- see the Activation comment
// above for why they're kept rather than deleted.
#[derive(Debug, Clone, Deserialize)]
pub struct Movement {
    pub train_id: String,
    pub event_type: String, // ARRIVAL | DEPARTURE | PASS
    #[allow(dead_code)]
    pub gbtt_timestamp: Option<String>,
    pub planned_timestamp: Option<String>,
    pub actual_timestamp: Option<String>,
    #[allow(dead_code)]
    pub reporting_stanox: Option<String>,
    pub loc_stanox: Option<String>,
    #[allow(dead_code)]
    pub toc_id: Option<String>,
    pub variation_status: Option<String>,
}

// `canx_reason_code`/`canx_type` are part of `0002`'s confirmed shape but
// have no consumer yet -- see the Activation comment above for why they're
// kept rather than deleted.
#[derive(Debug, Clone, Deserialize)]
pub struct Cancellation {
    pub train_id: String,
    pub canx_timestamp: Option<String>,
    #[allow(dead_code)]
    pub canx_reason_code: Option<String>,
    #[allow(dead_code)]
    pub canx_type: Option<String>, // "EN ROUTE" | "AT ORIGIN"
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChangeOfOrigin {
    pub train_id: String,
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
/// published Train Movements message list (the same TRAIN_MVT_ALL_TOC
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
    let value: serde_json::Value = serde_json::from_str(raw)?;
    let envelopes: Vec<Envelope> = if value.is_array() {
        serde_json::from_value(value)?
    } else {
        vec![serde_json::from_value(value)?]
    };
    Ok(envelopes.into_iter().filter_map(parse_envelope).collect())
}

/// The `movement-relay` filtering primitive
/// (docs/superpowers/specs/2026-09-04-movement-relay-design.md Decision 1):
/// classifies each envelope in `raw` by `header.msg_type` alone against
/// the same five confirmed types `parse_envelope` already encodes, and
/// re-serializes each SURVIVING envelope's own `serde_json::Value`
/// verbatim, byte-faithful, even in the rare multi-envelope-array case.
/// Returns `(msg_type, payload)` pairs -- `msg_type` is re-derived cheaply
/// here (rather than making every caller re-parse the returned payload
/// just to extract it again) since `movement-relay`'s own `EventSink`
/// needs it as a separate, redundant introspection field alongside the
/// raw payload (design doc Decision 2's field-layout choice) -- see
/// docs/superpowers/plans/2026-09-04-movement-relay-plan.md Task 7's own
/// note for why this deviates from this function's originally-sketched
/// `Vec<String>` signature.
///
/// Deliberately does NOT attempt to deserialize `body` into any typed
/// struct -- an envelope with a confirmed `msg_type` but a body that would
/// fail `parse_envelope`'s own typed deserialization (missing/malformed
/// fields) still survives here unchanged. That validation job stays where
/// it already lives, inside each downstream consumer's own `parse_batch`
/// call -- this function only ever looks at `header.msg_type`.
///
/// Shares `parse_batch`'s error behavior for a structurally malformed
/// payload (e.g. an envelope missing `header` entirely): that's a hard
/// `Err`, not a per-envelope skip, because `Vec<Envelope>`/`Envelope`
/// deserialization itself fails before per-envelope classification ever
/// runs -- same as `parse_batch` today.
pub fn confirmed_envelope_bodies(raw: &str) -> anyhow::Result<Vec<(String, String)>> {
    // `0005` (Reinstatement) joined this list in the H4 fix (2026-09-26
    // review): it used to parse as `Unknown` and get dropped right here,
    // before `trust-consumer`/`trust-backlog-consumer` ever saw it, which
    // silently discarded the one message TRUST sends to un-cancel a service
    // that resumes running. See `schema::Reinstatement`'s own doc comment
    // for why this type (unlike `0008`) is now confirmed.
    const CONFIRMED: [&str; 6] = ["0001", "0002", "0003", "0005", "0006", "0007"];

    let value: serde_json::Value = serde_json::from_str(raw)?;
    let envelopes: Vec<serde_json::Value> = if value.is_array() {
        serde_json::from_value(value)?
    } else {
        vec![value]
    };

    // Deliberately NOT a `filter_map` over a `?`-chained `Option` walk: that
    // shape (the plan's original sketch) conflates two different outcomes
    // that must stay distinct -- "structurally malformed envelope" (missing
    // `header`/`msg_type` entirely, a hard `Err` for the whole payload, same
    // as `parse_batch`'s own behavior on this exact input) versus
    // "well-formed envelope, unconfirmed msg_type" (a soft, per-envelope
    // skip). A bare `?` inside `filter_map`'s closure turns BOTH into a
    // silent `None`, which would make a genuinely malformed payload return
    // `Ok(vec![])` instead of `Err` -- confirmed by hand against
    // `confirmed_envelope_bodies_errors_on_a_payload_missing_header_entirely`'s
    // fixture while implementing this.
    let mut survivors = Vec::with_capacity(envelopes.len());
    for envelope in envelopes {
        let msg_type = envelope
            .get("header")
            .and_then(|header| header.get("msg_type"))
            .and_then(|msg_type| msg_type.as_str())
            .ok_or_else(|| anyhow::anyhow!("envelope missing header.msg_type"))?
            .to_string();
        if CONFIRMED.contains(&msg_type.as_str()) {
            let payload = serde_json::to_string(&envelope)?;
            survivors.push((msg_type, payload));
        }
    }
    Ok(survivors)
}

fn parse_envelope(envelope: Envelope) -> Option<TrustMessage> {
    let parsed = match envelope.header.msg_type.as_str() {
        "0001" => serde_json::from_value(envelope.body)
            .ok()
            .map(TrustMessage::Activation),
        "0002" => serde_json::from_value(envelope.body)
            .ok()
            .map(TrustMessage::Cancellation),
        "0003" => serde_json::from_value(envelope.body)
            .ok()
            .map(TrustMessage::Movement),
        "0006" => serde_json::from_value(envelope.body)
            .ok()
            .map(TrustMessage::ChangeOfOrigin),
        "0007" => serde_json::from_value(envelope.body)
            .ok()
            .map(TrustMessage::ChangeOfIdentity),
        "0005" => serde_json::from_value(envelope.body)
            .ok()
            .map(TrustMessage::Reinstatement),
        other => return Some(TrustMessage::Unknown(other.to_string())),
    };
    if parsed.is_none() {
        tracing::warn!(msg_type = %envelope.header.msg_type, "confirmed msg_type failed to parse against its known shape; dropping");
    }
    parsed
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let survivors = confirmed_envelope_bodies(raw).unwrap();
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
        let survivors = confirmed_envelope_bodies(raw).unwrap();
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

        let survivors = confirmed_envelope_bodies(raw).unwrap();
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
        let survivors = confirmed_envelope_bodies(raw).unwrap();
        assert_eq!(survivors.len(), 1);
        assert_eq!(survivors[0].0, "0003");
    }

    #[test]
    fn confirmed_envelope_bodies_errors_on_a_payload_missing_header_entirely() {
        let raw = r#"{"not_an_envelope": true}"#;
        assert!(confirmed_envelope_bodies(raw).is_err());
    }

    #[test]
    fn confirmed_envelope_bodies_is_byte_faithful() {
        let raw = r#"{"header":{"msg_type":"0003"},"body":{
            "train_id":"221832406","event_type":"DEPARTURE",
            "planned_timestamp":"1756400000000","actual_timestamp":"1756400060000",
            "loc_stanox":"87701","variation_status":"LATE",
            "an_unmodeled_field":"some real RDM data no struct declares"
        }}"#;
        let survivors = confirmed_envelope_bodies(raw).unwrap();
        assert_eq!(survivors.len(), 1);
        let value: serde_json::Value = serde_json::from_str(&survivors[0].1).unwrap();
        assert_eq!(
            value["body"]["an_unmodeled_field"].as_str().unwrap(),
            "some real RDM data no struct declares"
        );
    }
}
