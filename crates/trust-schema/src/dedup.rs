use chrono::NaiveDate;
use sha2::{Digest, Sha256};

use crate::schema::TrustMessage;

/// Stable across Kafka/Redis redelivery of the exact same TRUST message
/// (at-least-once delivery means this WILL happen). Built from the fields
/// that together identify one real-world event -- not the whole message
/// body, which may carry a redelivery-specific envelope field this pass
/// doesn't model. Mirrors `crates/common/src/text_hash.rs`'s `text_hash` in
/// shape and in the null-byte separator rationale (prevents field-boundary
/// collisions).
///
/// # Why there is a date in the key (finding #6 of the 2026-09-25 review)
///
/// TRUST `train_id`s are RECYCLED -- the same 10-character id comes back
/// around on roughly a monthly cadence. The key used to be
/// `(train_id, msg_type, event_type, loc_stanox, planned_timestamp)`, which
/// for a Movement (`0003`) is already date-unique via the raw
/// `planned_timestamp` epoch-millis value, but for every message shape that
/// carries no timestamp in the key -- a Cancellation (`0002`), an
/// Activation (`0001`), a change of origin/identity (`0006`/`0007`) --
/// collapsed to just `(train_id, msg_type)`.
///
/// That collision is not theoretical: `api`'s `trust_event_backlog` table
/// enforces `ON CONFLICT (dedup_key) DO NOTHING` GLOBALLY (not scoped per
/// train, unlike `train_movement_events`' `(trains_id, dedup_key)`), and
/// `trust-consumer`'s own config declares a 90-day retention -- well past
/// the `train_id` reuse boundary. So once retention exceeded roughly a
/// month, a genuinely new Cancellation (or Activation) for a recycled
/// `train_id` was silently discarded as a "duplicate" of the previous
/// month's completely unrelated train. `event_date` closes that: two
/// real-world events a month apart can no longer hash equal however little
/// else distinguishes them.
///
/// `event_date` must come from [`event_date`] in every live consumer:
/// the date is derived from **the message's own bytes**, falling back to
/// the processing rail day only when the message carries no usable date.
/// `trust-consumer` and `trust-backlog-consumer` both consume the same
/// live `movement-events` stream and both write `train_movement_events`
/// for the same real event; their keys agreeing is what makes
/// `ON CONFLICT (trains_id, dedup_key)` collapse the two writes into one
/// stored row.
///
/// It used to be the rail day the message was *processed* on (finding PL-3
/// of the 2026-09-27 pipelines review). A redelivery that straddled 02:00
/// London (a batch that failed at 01:59 and was reclaimed after 02:00, a
/// restart across the cutover), or the two consumers processing one
/// message either side of 02:00, produced two different keys and a
/// duplicate row. A date read from the message is the same on every
/// delivery and in every consumer. Both consumers must ship this rule in
/// the same deploy: until both run it, the old and new keys for the same
/// event differ, so expect a one-off duplicate risk at the deploy boundary.
///
/// The one deliberate exception is `api`'s own
/// `trust_event_backlog_match`, which REPLAYS stored backlog rows rather
/// than live messages and passes each row's `service_date` -- that function's
/// own doc comment already documents (and accepts) its keys differing from
/// a live consumer's.
pub fn dedup_key(
    train_id: &str,
    msg_type: &str,
    event_type: Option<&str>,
    loc_stanox: Option<&str>,
    planned_timestamp: Option<&str>,
    event_date: NaiveDate,
) -> String {
    let mut hasher = Sha256::new();
    let event_date = event_date.to_string();
    for field in [
        train_id,
        msg_type,
        event_type.unwrap_or(""),
        loc_stanox.unwrap_or(""),
        planned_timestamp.unwrap_or(""),
        event_date.as_str(),
    ] {
        hasher.update(field.as_bytes());
        hasher.update(b"\0");
    }
    let digest = hasher.finalize();
    digest.iter().map(|b| format!("{:02x}", b)).collect()
}

/// The `event_date` for [`dedup_key`]: a date read from the message itself,
/// so every delivery of it, in every consumer, hashes the same (PL-3).
///
/// Per type, the first present and parseable of:
/// - `0001`: `tp_origin_timestamp` (a `YYYY-MM-DD` date);
/// - `0002`: `canx_timestamp`, then `dep_timestamp`;
/// - `0003`: `planned_timestamp`, then `actual_timestamp`;
/// - `0005`/`0006`: `dep_timestamp`.
///
/// An epoch-millis timestamp is converted by its RAW value's UTC calendar
/// date, deliberately without `common::trust_timestamp`'s local-as-UTC
/// correction: that correction depends on the wall clock at processing
/// time, and the whole point here is a value that does not. (TRUST encodes
/// London local time as if it were UTC, so the raw UTC date is in practice
/// the London calendar date.) `0007`, `Unknown`, and a message whose field
/// is missing or unparseable fall back to `processed_rail_day`, which is
/// what every message used before.
pub fn event_date(message: &TrustMessage, processed_rail_day: NaiveDate) -> NaiveDate {
    let from_millis = |raw: Option<&str>| raw.and_then(epoch_millis_date);
    let date = match message {
        TrustMessage::Activation(a) => a
            .tp_origin_timestamp
            .as_deref()
            .and_then(|raw| NaiveDate::parse_from_str(raw.trim(), "%Y-%m-%d").ok()),
        TrustMessage::Cancellation(c) => from_millis(c.canx_timestamp.as_deref())
            .or_else(|| from_millis(c.dep_timestamp.as_deref())),
        TrustMessage::Movement(m) => from_millis(m.planned_timestamp.as_deref())
            .or_else(|| from_millis(m.actual_timestamp.as_deref())),
        TrustMessage::Reinstatement(r) => from_millis(r.dep_timestamp.as_deref()),
        TrustMessage::ChangeOfOrigin(o) => from_millis(o.dep_timestamp.as_deref()),
        TrustMessage::ChangeOfIdentity(_) | TrustMessage::Unknown(_) => None,
    };
    date.unwrap_or(processed_rail_day)
}

fn epoch_millis_date(raw: &str) -> Option<NaiveDate> {
    let millis: i64 = raw.trim().parse().ok()?;
    // A zero or negative value is a placeholder, not a date.
    if millis <= 0 {
        return None;
    }
    chrono::DateTime::from_timestamp_millis(millis).map(|at| at.date_naive())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(raw: &str) -> NaiveDate {
        raw.parse().unwrap()
    }

    #[test]
    fn identical_inputs_hash_identically() {
        assert_eq!(
            dedup_key(
                "221832406",
                "0003",
                Some("DEPARTURE"),
                Some("87701"),
                Some("1756400000000"),
                date("2026-08-28"),
            ),
            dedup_key(
                "221832406",
                "0003",
                Some("DEPARTURE"),
                Some("87701"),
                Some("1756400000000"),
                date("2026-08-28"),
            ),
        );
    }

    #[test]
    fn a_different_event_type_at_the_same_location_hashes_differently() {
        let a = dedup_key(
            "221832406",
            "0003",
            Some("ARRIVAL"),
            Some("87701"),
            Some("1756400000000"),
            date("2026-08-28"),
        );
        let b = dedup_key(
            "221832406",
            "0003",
            Some("DEPARTURE"),
            Some("87701"),
            Some("1756400000000"),
            date("2026-08-28"),
        );
        assert_ne!(a, b);
    }

    #[test]
    fn a_different_location_hashes_differently() {
        let a = dedup_key(
            "221832406",
            "0003",
            Some("PASS"),
            Some("87701"),
            None,
            date("2026-08-28"),
        );
        let b = dedup_key(
            "221832406",
            "0003",
            Some("PASS"),
            Some("11223"),
            None,
            date("2026-08-28"),
        );
        assert_ne!(a, b);
    }

    #[test]
    fn the_separator_prevents_boundary_collisions() {
        assert_ne!(
            dedup_key("AB", "0003", None, None, None, date("2026-08-28")),
            dedup_key("A", "B0003", None, None, None, date("2026-08-28")),
        );
    }

    /// Finding #6's regression test, in the exact shape that bit: TRUST
    /// recycles a `train_id` about monthly, and a Cancellation carries
    /// nothing else in the key at all -- so without a date component the
    /// SAME key came back for two completely unrelated trains a month
    /// apart, and `api`'s global `ON CONFLICT (dedup_key) DO NOTHING` on
    /// `trust_event_backlog` (90-day retention) silently dropped the newer
    /// one as a duplicate.
    #[test]
    fn a_recycled_train_id_a_month_later_does_not_collide_with_the_old_months_cancellation() {
        let august = dedup_key("221832406", "0002", None, None, None, date("2026-08-28"));
        let september = dedup_key("221832406", "0002", None, None, None, date("2026-09-28"));
        assert_ne!(
            august, september,
            "a recycled train_id's new Cancellation must not hash as a duplicate of the old \
             month's one"
        );
    }

    /// Same recycling hazard for an Activation (`0001`), whose key has never
    /// carried anything but `(train_id, msg_type)` either.
    #[test]
    fn a_recycled_train_ids_activation_a_month_later_hashes_differently() {
        assert_ne!(
            dedup_key("221832406", "0001", None, None, None, date("2026-08-28")),
            dedup_key("221832406", "0001", None, None, None, date("2026-09-28")),
        );
    }

    /// And the `0006`/`0007` passthrough shapes, which carry no timestamp of
    /// any kind.
    #[test]
    fn a_recycled_train_ids_change_of_identity_a_month_later_hashes_differently() {
        assert_ne!(
            dedup_key("221832406", "0007", None, None, None, date("2026-08-28")),
            dedup_key("221832406", "0007", None, None, None, date("2026-09-28")),
        );
    }

    /// The other half of the contract: the date must NOT make a genuine
    /// redelivery of the same event on the same rail day look new, or
    /// at-least-once redelivery would start duplicating rows.
    #[test]
    fn a_redelivery_on_the_same_rail_day_still_hashes_identically() {
        let first = dedup_key("221832406", "0002", None, None, None, date("2026-08-28"));
        let redelivered = dedup_key("221832406", "0002", None, None, None, date("2026-08-28"));
        assert_eq!(first, redelivered);
    }

    fn parse_one(raw: &str) -> TrustMessage {
        crate::schema::parse_batch(raw).unwrap().remove(0)
    }

    /// PL-3's regression test: the same message processed either side of
    /// 02:00 London (a redelivery, or the two consumers a moment apart)
    /// gets the same date, so the same key.
    #[test]
    fn a_movement_redelivered_across_the_rail_day_cutover_keeps_its_date() {
        // 2026-09-27 01:59:30 as TRUST's raw epoch millis.
        let raw = r#"{"header":{"msg_type":"0003"},"body":{"train_id":"1","event_type":"ARRIVAL",
            "planned_timestamp":"1790474370000"}}"#;
        let message = parse_one(raw);
        let before = event_date(&message, date("2026-09-26"));
        let after = event_date(&message, date("2026-09-27"));
        assert_eq!(before, after);
        assert_eq!(before, date("2026-09-27"));
    }

    #[test]
    fn event_date_per_message_type() {
        let fallback = date("2000-01-01");
        let cases = [
            (
                r#"{"header":{"msg_type":"0001"},"body":{"train_id":"1","train_uid":"C1","tp_origin_timestamp":"2026-09-28"}}"#,
                "2026-09-28",
            ),
            (
                r#"{"header":{"msg_type":"0002"},"body":{"train_id":"1","canx_timestamp":"1790474370000","dep_timestamp":"1790600000000"}}"#,
                "2026-09-27",
            ),
            (
                r#"{"header":{"msg_type":"0002"},"body":{"train_id":"1","dep_timestamp":"1790600000000"}}"#,
                "2026-09-28",
            ),
            (
                r#"{"header":{"msg_type":"0003"},"body":{"train_id":"1","event_type":"ARRIVAL","actual_timestamp":"1790600000000"}}"#,
                "2026-09-28",
            ),
            (
                r#"{"header":{"msg_type":"0005"},"body":{"train_id":"1","dep_timestamp":"1790474370000"}}"#,
                "2026-09-27",
            ),
            (
                r#"{"header":{"msg_type":"0006"},"body":{"train_id":"1","dep_timestamp":"1790474370000"}}"#,
                "2026-09-27",
            ),
        ];
        for (raw, expected) in cases {
            assert_eq!(
                event_date(&parse_one(raw), fallback),
                date(expected),
                "{raw}"
            );
        }
    }

    #[test]
    fn event_date_falls_back_to_the_processing_rail_day_without_a_usable_date() {
        let fallback = date("2026-09-27");
        for raw in [
            r#"{"header":{"msg_type":"0007"},"body":{"train_id":"1"}}"#,
            r#"{"header":{"msg_type":"0005"},"body":{"train_id":"1"}}"#,
            r#"{"header":{"msg_type":"0003"},"body":{"train_id":"1","event_type":"ARRIVAL","planned_timestamp":"garbage"}}"#,
            r#"{"header":{"msg_type":"0002"},"body":{"train_id":"1","canx_timestamp":"0"}}"#,
            r#"{"header":{"msg_type":"0001"},"body":{"train_id":"1","train_uid":"C1","tp_origin_timestamp":"28/09/2026"}}"#,
            r#"{"header":{"msg_type":"0008"},"body":{}}"#,
        ] {
            assert_eq!(event_date(&parse_one(raw), fallback), fallback, "{raw}");
        }
    }
}
