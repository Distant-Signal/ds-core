use chrono::NaiveDate;
use sha2::{Digest, Sha256};

/// Stable across Kafka/Redis redelivery of the exact same TRUST message
/// (at-least-once delivery means this WILL happen). Built from the fields
/// that together identify one real-world event -- not the whole message
/// body, which may carry a redelivery-specific envelope field this pass
/// doesn't model. Mirrors `crates/enricher/src/hash.rs`'s `text_hash` in
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
/// `event_date` must be **the Europe/London rail day the message was
/// processed on** (`common::rail_day::current_rail_day`), in every caller.
/// That specific rule matters because `trust-consumer` and
/// `trust-backlog-consumer` both consume the same live `movement-events`
/// stream and both write `train_movement_events` for the same real event;
/// their keys agreeing is what makes `ON CONFLICT (trains_id, dedup_key)`
/// collapse the two writes into one stored row. Using a per-crate notion of
/// the date (one crate's parked-Activation `service_date`, say, against the
/// other's clock) would silently double those rows for any overnight train.
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
}
