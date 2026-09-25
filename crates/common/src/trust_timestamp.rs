//! Parsing and plausibility-guarding for TRUST/RDM's millisecond-epoch
//! timestamp fields (`planned_timestamp`/`actual_timestamp`/
//! `canx_timestamp`).
//!
//! ## Background
//!
//! docs/superpowers/specs/2026-08-29-trust-schedule-delay-validation-findings.md's
//! "2026-09-11: Task 4 goes live" section documents a sustained,
//! multi-hour live-feed anomaly: the raw TRUST/RDM Kafka feed's
//! `planned_timestamp`/`actual_timestamp` fields arrive already inflated by
//! a near-constant ~59-60 minutes. This was confirmed via two
//! independently-written, byte-identical `parse_epoch_millis`
//! implementations (`crates/trust-consumer/src/process.rs` and
//! `crates/trust-backlog-consumer/src/process.rs`, both just
//! `chrono::DateTime::from_timestamp_millis(raw.parse().ok()?)`,
//! timezone-safe by construction) against a sustained live sample, and is
//! NOT explained by this codebase's own DB session timezone or column
//! types (`TIMESTAMPTZ` throughout, confirmed against
//! `crates/api/migrations/20260828120000_train_tracking.sql`).
//!
//! The best-evidenced hypothesis: whatever upstream system stamps these
//! fields is emitting Europe/London LOCAL wall-clock time (BST, currently
//! UTC+1) but labelling it as though it were already UTC -- a missed
//! timezone conversion one hop upstream of this codebase, which this
//! codebase cannot fix at the source and has NOT had vendor-confirmed.
//! [`parse_trust_epoch_millis_pair`] below applies that correction, but
//! only as a *guarded* one -- see its own doc comment for why and how it
//! falls back when the correction doesn't actually help. It also takes a
//! `correction_enabled` kill switch (each consuming crate's own
//! `trust_timestamp_correction_enabled` config flag) so an operator can
//! disable the correction outright if this hypothesis ever stops holding.
//!
//! ## Why this lives in `common`
//!
//! `trust-consumer` and `trust-backlog-consumer` each used to define their
//! own byte-identical, uncorrected `parse_epoch_millis` -- the exact
//! duplication that let the corruption above go unnoticed for as long as
//! it did (it took two independent confirmations to be believed). This
//! module exists so there is now exactly one implementation for both
//! crates to share, and it can't drift again.
//!
//! ## One decision per message, not one per field
//!
//! A single TRUST message (a Movement) carries TWO timestamp fields --
//! `planned_timestamp` and `actual_timestamp` -- that many downstream call
//! sites (`crates/api/src/data/journey.rs`'s per-stop `delay_minutes`,
//! this crate's own top-level `delay_minutes`) diff against EACH OTHER,
//! relying on both having been skewed (or not) by exactly the same amount.
//! Calling the single-field [`parse_trust_epoch_millis`] independently on
//! each field breaks that invariant: `actual_timestamp`'s own
//! `received_at` anchor makes its correction plausible far more often than
//! `planned_timestamp`'s does (a train running more than ~10 minutes EARLY
//! makes the corrected `planned_timestamp` alone look implausibly far
//! *ahead* of receipt, even though `actual_timestamp` corrects cleanly) --
//! so the two fields can independently land on OPPOSITE corrected-or-raw
//! outcomes, desyncing them by a full hour. [`parse_trust_epoch_millis_pair`]
//! exists specifically to prevent that: it makes the plausibility decision
//! ONCE, anchored on `actual_timestamp` (the only field with a meaningful
//! `received_at` anchor to judge plausibility by), and applies that same
//! outcome uniformly to both fields. Every real call site
//! (`trust-consumer`/`trust-backlog-consumer`'s Movement and Cancellation
//! handling) uses this pairwise function, never the single-field one, for
//! exactly this reason.
//!
//! Sharing just the corrected-vs-raw BRANCH is not enough on its own,
//! though: on the one UK night a year where local time is ambiguous (the
//! autumn-fallback overlap hour -- see [`reinterpret_as_london_local`]'s own
//! doc comment), "corrected" is not a single value, it is a choice between
//! two candidate instants an hour apart. If each field re-ran that
//! ambiguity choice independently, `planned` and `actual` could both
//! legitimately land on the "corrected" branch together yet still end up on
//! OPPOSITE candidates -- a full hour apart from each other despite neither
//! being wrong in isolation. [`parse_trust_epoch_millis_pair`] closes this
//! by computing the correction MAGNITUDE (`corrected - raw`) exactly once,
//! from `actual_timestamp` alone, and applying that same delta to
//! `planned_timestamp`'s raw value directly -- `planned` never calls
//! [`reinterpret_as_london_local`] itself, so it cannot independently
//! disagree with `actual` about which candidate to use.

use chrono::{DateTime, TimeZone, Utc};

/// How far ahead of the reporting message's receipt time an event
/// timestamp (`actual_timestamp`, or `parse_trust_epoch_millis`'s own
/// corrected reinterpretation of any TRUST epoch-millis field) may
/// plausibly be, before it's treated as implausible rather than trusted
/// into a matching decision.
///
/// A few seconds to a couple of minutes of clock skew between systems (the
/// feed's own clock, this process's, the database's) is normal and must
/// not be rejected. The corruption this guards against inflates
/// timestamps by ~59-60 minutes (see this module's own doc comment), so 10
/// minutes sits comfortably above ordinary clock skew and comfortably
/// below the smallest corruption actually observed -- there is no
/// evidence of any real-world value landing in between that this
/// threshold would misclassify either way.
pub const MAX_TIMESTAMP_SKEW_AHEAD_OF_RECEIPT: chrono::Duration = chrono::Duration::minutes(10);

/// `true` if `candidate` is not implausibly ahead of `received_at` -- i.e.
/// not "reported before it happened" by more than ordinary clock skew.
/// `received_at` may legitimately be far AFTER `candidate` (a delayed or
/// backlogged message is perfectly normal); only `candidate` being far
/// AFTER `received_at` is implausible, since nothing can be received
/// before it happens.
///
/// Used both as `parse_trust_epoch_millis`'s own guard on its corrected
/// value (below), and independently, as defense-in-depth, at the two pin
/// -matching boundaries that trust a TRUST `actual_timestamp` into a
/// matching decision: `trust-consumer::matching::resolve_origin_departure`
/// and `api::data::trust_event_backlog_match::find_backlog_match`. Kept as
/// a single shared predicate rather than three separate inline checks so
/// the threshold and its reasoning live in exactly one place.
pub fn is_plausible_actual_timestamp(candidate: DateTime<Utc>, received_at: DateTime<Utc>) -> bool {
    candidate <= received_at + MAX_TIMESTAMP_SKEW_AHEAD_OF_RECEIPT
}

/// Parses a raw TRUST epoch-millis string with no correction and no
/// plausibility check at all -- just the naive `i64` millis -> UTC
/// instant conversion every TRUST timestamp field starts from. Shared by
/// every function below so there is exactly one place that does this
/// bottom-layer parse.
fn parse_raw_millis(raw: &str) -> Option<DateTime<Utc>> {
    let millis: i64 = raw.parse().ok()?;
    DateTime::from_timestamp_millis(millis)
}

/// Reinterprets `raw_utc`'s wall-clock reading as a Europe/London LOCAL
/// instant (rather than the true UTC one it was parsed as) and relocalizes
/// it to a true UTC instant -- exactly the `NaiveDateTime` round-trip
/// `rail_day.rs`'s own `next_rail_day_boundary` already performs in the
/// opposite direction, via the same
/// `chrono_tz::Europe::London::from_local_datetime` this module reuses.
/// Performs NO plausibility check of its own -- callers combine this with
/// [`is_plausible_actual_timestamp`] as needed.
///
/// ## DST edge cases
///
/// `chrono_tz::Europe::London::from_local_datetime` forces two to be
/// handled explicitly, since the reinterpreted naive wall-clock reading
/// might land on the one UK night a year where local time is ambiguous or
/// missing:
///
/// - **Autumn fallback (ambiguous)**: the naive reading occurs twice, once
///   in BST and once in GMT. Disambiguated by `received_at`: whichever of
///   the two candidate UTC instants is genuinely NEARER to `received_at`
///   is chosen. (An earlier version of this function always picked the
///   earlier, BST candidate, reasoning that "the plausibility guard above
///   still catches an implausible result from either choice" -- that
///   claim was false. The guard only rejects a value AHEAD of
///   `received_at`; always picking the earlier candidate only ever moves
///   the result earlier, so a GMT-side instant in the ambiguous overlap
///   hour -- whose raw value was already correct -- could be silently,
///   undetectably shifted an hour early, with nothing catching it. Picking
///   by proximity to `received_at` instead means the guard's own
///   assumption -- "a real receipt time is close to the true event
///   time" -- is what actually decides the ambiguous case, rather than an
///   arbitrary fixed branch.)
/// - **Spring-forward (nonexistent)**: the naive reading falls in the
///   skipped 01:00-02:00 local hour and was never a real Europe/London
///   local time at all. There is no principled corrected value to compute,
///   so this returns `None` -- callers fall back to the raw interpretation.
///
/// Neither case panics.
fn reinterpret_as_london_local(
    raw_utc: DateTime<Utc>,
    received_at: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let naive_local = raw_utc.naive_utc();
    match chrono_tz::Europe::London.from_local_datetime(&naive_local) {
        chrono::LocalResult::Single(dt) => Some(dt.with_timezone(&Utc)),
        chrono::LocalResult::Ambiguous(earliest, latest) => {
            let earliest_utc = earliest.with_timezone(&Utc);
            let latest_utc = latest.with_timezone(&Utc);
            let earliest_diff = (earliest_utc - received_at).abs();
            let latest_diff = (latest_utc - received_at).abs();
            Some(if earliest_diff <= latest_diff {
                earliest_utc
            } else {
                latest_utc
            })
        }
        chrono::LocalResult::None => None,
    }
}

/// The anchor decision: parses `raw`, attempts the Europe/London-local
/// reinterpretation, and checks the result against `received_at` via
/// [`is_plausible_actual_timestamp`]. Returns `(instant_to_use,
/// was_corrected)` -- `was_corrected` is `false` whenever the correction
/// wasn't applied (implausible, no principled reinterpretation existed --
/// the spring-forward gap) OR whenever it WAS applied but changed nothing
/// (the GMT/winter case, where Europe/London-local equals UTC and
/// reinterpreting is a no-op); see the `corrected_utc != raw_utc` comment
/// below. `was_corrected` therefore means "this value was actually altered
/// by the correction", not merely "the correction logic ran" -- callers'
/// `*_timestamp_correction_total` metrics rely on that distinction. `None`
/// only if `raw` itself fails to parse as epoch millis.
///
/// This is the ONLY place that decides corrected-vs-raw for a TRUST
/// message. [`parse_trust_epoch_millis_pair`] calls this once, on
/// `actual_timestamp`, and applies its `was_corrected` outcome uniformly
/// to every other timestamp field on the same message via
/// [`apply_correction_decision`] -- see this module's own doc comment on
/// why a shared decision, not an independent one per field, is required.
fn decide_correction(raw: &str, received_at: DateTime<Utc>) -> Option<(DateTime<Utc>, bool)> {
    let raw_utc = parse_raw_millis(raw)?;

    match reinterpret_as_london_local(raw_utc, received_at) {
        Some(corrected_utc) if is_plausible_actual_timestamp(corrected_utc, received_at) => {
            // Signal Box Audit, common-crate Low finding "A BST-correction
            // metric reads ~100% 'corrected' all winter": this used to
            // unconditionally report `true` here -- "reinterpretation
            // succeeded and is plausible" -- even though `corrected_utc`
            // can legitimately equal `raw_utc` bit-for-bit. That happens
            // for every single value processed while Europe/London is on
            // GMT (winter): local wall-clock time IS UTC then, so
            // `reinterpret_as_london_local` is a mathematical no-op, yet
            // this branch (successfully reinterpreted + trivially
            // plausible, since it's identical to the untouched raw value)
            // still runs and still reported `Some(true)`. Every caller's
            // own `*_timestamp_correction_total{outcome="corrected"}`
            // metric (Finding #2's kill switch, `trust-consumer`'s and
            // `trust-backlog-consumer`'s `process.rs`) fed straight off
            // this bool, so it read ~100% "corrected" for the entire winter
            // -- unable to distinguish "the BST-mislabelling hypothesis
            // fired and changed a value" from "the hypothesis fired and
            // altered nothing", which is exactly the signal an operator
            // would need to notice if the hypothesis ever stopped holding.
            // Comparing the two instants directly reports a correction only
            // when one was actually applied.
            Some((corrected_utc, corrected_utc != raw_utc))
        }
        Some(corrected_utc) => {
            tracing::warn!(
                raw,
                raw_utc = %raw_utc,
                corrected_utc = %corrected_utc,
                received_at = %received_at,
                "TRUST timestamp correction produced an implausible result (still ahead of \
                 receipt beyond common::MAX_TIMESTAMP_SKEW_AHEAD_OF_RECEIPT); falling back to \
                 the raw, uncorrected interpretation -- the Europe/London-mislabelling \
                 hypothesis may not hold for this value"
            );
            Some((raw_utc, false))
        }
        None => {
            tracing::warn!(
                raw,
                raw_utc = %raw_utc,
                "raw TRUST timestamp's wall-clock reading falls in the Europe/London \
                 spring-forward gap and has no corrected local interpretation; using the raw, \
                 uncorrected value"
            );
            Some((raw_utc, false))
        }
    }
}

/// Applies an ALREADY-DECIDED corrected-or-raw outcome (from
/// [`decide_correction`], run once on the message's `actual_timestamp`) to
/// a companion field -- e.g. the same message's `planned_timestamp`.
/// Deliberately does NOT re-run the plausibility check itself, and -- just
/// as importantly -- does NOT re-run [`reinterpret_as_london_local`] itself
/// either: that function makes its own `received_at`-anchored choice
/// whenever the reinterpreted wall-clock reading is DST-ambiguous (the
/// autumn-fallback overlap hour), and calling it a second time,
/// independently, for this field could pick the OPPOSITE candidate from
/// the one `actual` picked, even though both fields are (correctly) on the
/// "corrected" branch together -- desyncing `planned` and `actual` by an
/// hour from each other despite neither being individually wrong. Instead,
/// `correction_delta` (the anchor field's own `corrected - raw` offset, a
/// single already-decided number) is applied directly to this field's raw
/// value, so `planned` can only ever land on the SAME side of any DST
/// transition as `actual` did -- it never independently touches
/// `reinterpret_as_london_local`'s ambiguous-or-nonexistent branches at
/// all.
///
/// Uses `checked_add_signed` rather than plain `+`: `parse_raw_millis`
/// accepts any millis value down to `DateTime::MIN_UTC` (or up to
/// `DateTime::MAX_UTC`), straight off the live TRUST Kafka feed, and
/// `chrono`'s `Add<TimeDelta>` for `DateTime<Utc>` panics on overflow. An
/// extreme-but-parseable `raw` value close to either bound, combined with a
/// non-zero `correction_delta`, could push the sum outside the
/// representable range and crash the whole processing task (Finding #1) --
/// `checked_add_signed` turns that into a clean `None` instead, which
/// `apply_correction_decision`'s own `Option` return type and every caller
/// already treat as "this field didn't parse", exactly like any other
/// unparseable value.
fn apply_correction_decision(
    raw: &str,
    was_corrected: bool,
    correction_delta: chrono::Duration,
) -> Option<DateTime<Utc>> {
    let raw_utc = parse_raw_millis(raw)?;
    if !was_corrected {
        return Some(raw_utc);
    }
    raw_utc.checked_add_signed(correction_delta)
}

/// Parses a single TRUST/RDM millisecond-epoch timestamp string, correcting
/// for the hypothesized Europe/London-local-mislabelled-as-UTC bug
/// documented in this module's own doc comment, and guarding that
/// correction against [`is_plausible_actual_timestamp`].
///
/// **Prefer [`parse_trust_epoch_millis_pair`] at any call site that also
/// has a companion timestamp field on the same message** (every real one in
/// this codebase does: `planned_timestamp`/`actual_timestamp` together, or
/// `canx_timestamp` alone). This single-field function is kept public for
/// call sites with genuinely only one TRUST timestamp to parse and for
/// `parse_trust_epoch_millis_pair`'s own internal use, but calling it
/// independently on two fields that need to agree is exactly the mistake
/// this module's doc comment warns against.
pub fn parse_trust_epoch_millis(raw: &str, received_at: DateTime<Utc>) -> Option<DateTime<Utc>> {
    decide_correction(raw, received_at).map(|(instant, _)| instant)
}

/// The result of a single message-level correction decision, applied
/// uniformly to both of a TRUST Movement's timestamp fields (or just
/// `actual` alone, for a Cancellation's `canx_timestamp`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrustTimestampPair {
    /// The companion field (`planned_timestamp`), parsed under the SAME
    /// corrected-or-raw decision as `actual`. `None` if the caller had no
    /// raw string to parse, if it failed to parse, or if applying the
    /// correction delta would overflow `DateTime<Utc>`'s representable
    /// range (Finding #1) -- treated the same as any other unparseable
    /// value rather than panicking.
    pub planned: Option<DateTime<Utc>>,
    /// The anchor field (`actual_timestamp`, or `canx_timestamp` playing
    /// the same role). `None` if the caller had no raw string to parse, or
    /// if it failed to parse -- in which case no decision could be made at
    /// all, and `planned` (if present) was parsed independently via the
    /// ordinary single-field guarded path, since there is nothing to keep
    /// it in sync with.
    pub actual: Option<DateTime<Utc>>,
    /// `Some(true)` if the correction was applied to this message AND
    /// actually changed the value, `Some(false)` if it fell back to the raw
    /// interpretation (an implausible correction, the spring-forward gap,
    /// or correction disabled entirely via `correction_enabled: false`) OR
    /// if the "corrected" value is bit-for-bit identical to the raw one
    /// (the GMT/winter case, where Europe/London-local equals UTC and
    /// reinterpreting is a no-op -- see `decide_correction`'s own doc
    /// comment), `None` if no decision could be made at all (no parseable
    /// `actual`). Feeds the caller's own
    /// `distant_signal_*_timestamp_correction_total` metric (Finding #2) --
    /// only a `Some` value should be counted, since `None` means nothing
    /// was decided. Deliberately NOT "the correction logic ran": a metric
    /// built on that weaker meaning would read ~100% "corrected" for the
    /// entire winter regardless of whether the underlying BST-mislabelling
    /// hypothesis still held (Signal Box Audit, common-crate Low finding "A
    /// BST-correction metric reads ~100% 'corrected' all winter").
    pub was_corrected: Option<bool>,
}

/// Parses a TRUST message's `actual_timestamp` (the plausibility anchor)
/// together with its optional companion `planned_timestamp`, making ONE
/// correction decision for the whole message -- anchored on `actual`,
/// since it is the only field with a `received_at` to judge plausibility
/// by -- and applying that SAME decision to `planned` too. This is what
/// keeps the two fields from independently landing on opposite
/// corrected-or-raw outcomes and desyncing by a full hour (Finding #1);
/// see this module's own top-level doc comment for the full failure mode
/// this replaces.
///
/// Also the right function for a Cancellation's lone `canx_timestamp`:
/// call with `planned: None`, `actual: cancellation.canx_timestamp.as_deref()`,
/// and read the result's `.actual` -- there is only one field, but routing
/// it through the same guarded, metrics-instrumented path keeps every
/// TRUST timestamp in this codebase flowing through one decision function.
///
/// `correction_enabled` is the kill switch from Finding #2: when `false`,
/// no correction is attempted at all (both fields are parsed as their raw,
/// uncorrected values) and `was_corrected` is `Some(false)` whenever
/// `actual` parsed -- see
/// `crates/trust-consumer/src/config.rs`/`crates/trust-backlog-consumer/src/config.rs`'s
/// `trust_timestamp_correction_enabled` field for how an operator sets
/// this at deploy time.
pub fn parse_trust_epoch_millis_pair(
    planned: Option<&str>,
    actual: Option<&str>,
    received_at: DateTime<Utc>,
    correction_enabled: bool,
) -> TrustTimestampPair {
    if !correction_enabled {
        let actual_parsed = actual.and_then(parse_raw_millis);
        return TrustTimestampPair {
            planned: planned.and_then(parse_raw_millis),
            actual: actual_parsed,
            was_corrected: actual_parsed.map(|_| false),
        };
    }

    let Some(actual_raw) = actual else {
        // No anchor to decide from at all. Nothing else on this message
        // needs to stay in sync with a field that doesn't exist, so
        // `planned` (if present) is parsed independently via the ordinary
        // guarded single-field path -- safe, since every real call site
        // that diffs `planned` against `actual` already requires both to
        // be `Some` before doing so.
        return TrustTimestampPair {
            planned: planned.and_then(|raw| parse_trust_epoch_millis(raw, received_at)),
            actual: None,
            was_corrected: None,
        };
    };

    let Some((actual_instant, was_corrected)) = decide_correction(actual_raw, received_at) else {
        // `actual` itself failed to parse -- same "no anchor" fallback as
        // above.
        return TrustTimestampPair {
            planned: planned.and_then(|raw| parse_trust_epoch_millis(raw, received_at)),
            actual: None,
            was_corrected: None,
        };
    };

    // `actual_raw` already parsed successfully above (that's how we got
    // `actual_instant`), so this can't fail. `correction_delta` is the
    // exact `corrected - raw` offset that `actual` itself landed on
    // (including, when relevant, ITS OWN DST-ambiguity disambiguation) --
    // the single number `apply_correction_decision` applies to `planned`
    // below instead of letting `planned` re-derive that disambiguation
    // independently.
    let actual_raw_utc = parse_raw_millis(actual_raw)
        .expect("actual_raw already parsed successfully in decide_correction above");
    let correction_delta = actual_instant - actual_raw_utc;

    let planned_instant =
        planned.and_then(|raw| apply_correction_decision(raw, was_corrected, correction_delta));
    TrustTimestampPair {
        planned: planned_instant,
        actual: Some(actual_instant),
        was_corrected: Some(was_corrected),
    }
}

/// How far apart a single TRUST message's own `planned_timestamp`/
/// `actual_timestamp` may plausibly sit before their difference is treated
/// as a corrupted timestamp pair rather than a real delay -- see this
/// module's own top-level doc comment for the TRUST timestamp corruption
/// this guards against (and note that corruption is NOT limited to the
/// specific ~59-60 minute Europe/London-mislabelling shape documented
/// there; any unbounded, un-plausibility-checked timestamp arithmetic is
/// exposed to it). A real train delay of even several hours is already
/// extraordinary; a full day is comfortably beyond anything a real
/// delay/early-running event could produce, so a gap beyond this is
/// treated as a bad timestamp pair, not trusted into
/// `train_current_state.delay_minutes` -- a value every subscriber to a
/// shared train sees, and which feeds Delay Repay compensation estimates.
pub const MAX_PLAUSIBLE_DELAY_MINUTES: i64 = 24 * 60;

/// Computes a plausibility-guarded `delay_minutes` from a TRUST message's
/// own `actual`/`planned` timestamps. Returns `None` -- not a clamped,
/// still-wrong value -- when `|actual - planned|` exceeds
/// [`MAX_PLAUSIBLE_DELAY_MINUTES`], logging at `warn!` so a corrupted pair
/// doesn't fail silently.
///
/// **Why `None`, not clamping to the bound:** a corrupted timestamp pair
/// doesn't degrade gracefully to "very delayed" -- `(actual - planned)`
/// having gone wrong by minutes-to-millions is exactly as wrong at
/// `MAX_PLAUSIBLE_DELAY_MINUTES` as it is at any other implausible value,
/// so clamping would just substitute one made-up number (1440) for
/// another. `None` lets both real call sites
/// (`crates/api/src/data/trust_event_backlog.rs` and
/// `crates/api/src/data/trust_event_backlog_match.rs`) fall back to the
/// coarser, `variation_status`-only `delay_minutes` estimate
/// `trust_schema::journey::apply_movement` already computed before either
/// call site overwrites it with this timestamp-derived (and here,
/// rejected) one -- an honest "we don't have a precise number" rather than
/// a precise-looking but fabricated one.
///
/// This is the fix for the truncating-cast bug the finding this guards
/// against actually found: `(actual - planned).num_minutes() as i32` with
/// no bound check at all, which one corrupt `actual_timestamp` (the same
/// class of corruption this module already guards `is_plausible_actual_timestamp`
/// against, just unfiltered at this particular call site) could turn into
/// a delay of literally millions of minutes.
pub fn plausible_delay_minutes(actual: DateTime<Utc>, planned: DateTime<Utc>) -> Option<i32> {
    let delta_minutes = (actual - planned).num_minutes();
    if delta_minutes.unsigned_abs() > MAX_PLAUSIBLE_DELAY_MINUTES as u64 {
        tracing::warn!(
            actual = %actual,
            planned = %planned,
            delta_minutes,
            "TRUST actual/planned timestamp gap is implausible (beyond \
             MAX_PLAUSIBLE_DELAY_MINUTES); dropping this delay_minutes computation rather than \
             writing a corrupted value into train_current_state"
        );
        return None;
    }
    Some(delta_minutes as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bst_period_timestamp_is_corrected_one_hour_earlier_and_is_plausible() {
        // 2026-07-15 13:00 UTC read naively -- July is BST, so the intended
        // instant is 12:00 UTC (13:00 BST local). `received_at` is set just
        // after the CORRECTED instant, matching a real-time feed: the raw
        // (uncorrected) reading would be an hour in the future relative to
        // receipt, which is exactly the implausible shape this whole fix
        // exists to catch.
        let raw = "1784120400000"; // 2026-07-15T13:00:00Z as millis
        let received_at: DateTime<Utc> = "2026-07-15T12:01:00Z".parse().unwrap();

        let corrected = parse_trust_epoch_millis(raw, received_at).unwrap();

        assert_eq!(
            corrected,
            "2026-07-15T12:00:00Z".parse::<DateTime<Utc>>().unwrap(),
            "a BST-period value must be corrected exactly one hour earlier"
        );
        assert!(
            is_plausible_actual_timestamp(corrected, received_at),
            "the corrected value must pass the plausibility check"
        );
    }

    #[test]
    fn a_gmt_period_timestamp_is_a_no_op() {
        // 2026-01-15 -- GMT period, Europe/London local == UTC, so the
        // correction must not shift the value at all.
        let raw = "1768480200000"; // 2026-01-15T12:30:00Z as millis
        let received_at: DateTime<Utc> = "2026-01-15T12:31:00Z".parse().unwrap();

        let corrected = parse_trust_epoch_millis(raw, received_at).unwrap();

        assert_eq!(
            corrected,
            "2026-01-15T12:30:00Z".parse::<DateTime<Utc>>().unwrap(),
            "a GMT-period value must not be shifted"
        );
    }

    /// Finding #8 regression ("A BST-correction metric reads ~100%
    /// 'corrected' all winter"): a GMT-period value goes through the exact
    /// same "reinterpreted + plausible" branch a BST-period value does (see
    /// `a_bst_period_timestamp_is_corrected_one_hour_earlier_and_is_plausible`
    /// above) -- the ONLY difference is that the reinterpreted value happens
    /// to equal the raw one. `was_corrected` must report `Some(false)` here,
    /// not `Some(true)`: nothing was actually corrected, so a metric built
    /// on this bool must not count it as a correction.
    #[test]
    fn a_gmt_period_value_reports_was_corrected_false_even_though_the_correction_branch_ran() {
        let raw = "1768480200000"; // 2026-01-15T12:30:00Z as millis
        let received_at: DateTime<Utc> = "2026-01-15T12:31:00Z".parse().unwrap();

        let pair = parse_trust_epoch_millis_pair(None, Some(raw), received_at, true);

        assert_eq!(
            pair.actual,
            Some("2026-01-15T12:30:00Z".parse::<DateTime<Utc>>().unwrap()),
            "the value itself is unaffected by this fix -- only the reported outcome changes"
        );
        assert_eq!(
            pair.was_corrected,
            Some(false),
            "a no-op 'correction' (GMT period) must not be counted as a correction"
        );
    }

    #[test]
    fn the_spring_forward_gap_falls_back_to_the_raw_value_without_panicking() {
        // UK clocks spring forward at 01:00 UTC on 2026-03-29, jumping local
        // time from 01:00 GMT straight to 02:00 BST -- 01:30 local never
        // exists that day. Raw millis chosen so `raw_utc`'s naive wall-clock
        // reading is exactly 2026-03-29 01:30:00 -- the nonexistent local
        // time.
        let raw = "1774747800000"; // 2026-03-29T01:30:00Z as millis
        let received_at: DateTime<Utc> = "2026-03-29T02:00:00Z".parse().unwrap();

        let corrected = parse_trust_epoch_millis(raw, received_at).unwrap();

        assert_eq!(
            corrected,
            "2026-03-29T01:30:00Z".parse::<DateTime<Utc>>().unwrap(),
            "no principled correction exists for a nonexistent local time; the raw value is \
             used unchanged"
        );
    }

    #[test]
    fn the_autumn_fallback_overlap_resolves_to_whichever_candidate_is_nearer_received_at_bst_case()
    {
        // UK clocks fall back at 02:00 BST -> 01:00 GMT on 2026-10-25 --
        // 01:30 local occurs twice: once at 00:30 UTC (BST) and once at
        // 01:30 UTC (GMT). `received_at` is set right next to the BST
        // candidate, so that's the one disambiguation must pick.
        let raw = "1792891800000"; // 2026-10-25T01:30:00Z as millis
        let received_at: DateTime<Utc> = "2026-10-25T00:31:00Z".parse().unwrap();

        let corrected = parse_trust_epoch_millis(raw, received_at).unwrap();

        assert_eq!(
            corrected,
            "2026-10-25T00:30:00Z".parse::<DateTime<Utc>>().unwrap(),
            "received_at is 1 minute from the BST candidate and 59 minutes from the GMT one"
        );
    }

    /// The counterpart to the test above, proving disambiguation genuinely
    /// depends on `received_at` rather than always picking the same
    /// (earlier/BST) branch -- the exact bug Finding #3 fixed. Same
    /// ambiguous raw value, but `received_at` now sits right next to the
    /// GMT candidate instead.
    #[test]
    fn the_autumn_fallback_overlap_resolves_to_whichever_candidate_is_nearer_received_at_gmt_case()
    {
        let raw = "1792891800000"; // 2026-10-25T01:30:00Z as millis
        let received_at: DateTime<Utc> = "2026-10-25T01:31:00Z".parse().unwrap();

        let corrected = parse_trust_epoch_millis(raw, received_at).unwrap();

        assert_eq!(
            corrected,
            "2026-10-25T01:30:00Z".parse::<DateTime<Utc>>().unwrap(),
            "received_at is 1 minute from the GMT candidate and 59 minutes from the BST one -- \
             a fixed always-pick-BST rule would have wrongly shifted this an hour early"
        );
    }

    #[test]
    fn a_correction_that_is_itself_implausible_falls_back_to_the_raw_value() {
        // The hypothesis-is-wrong case: the value corrects to 12:00:00Z (one
        // hour earlier than the raw 13:00:00Z, same as the ordinary BST
        // case above), but `received_at` is anchored an hour before even
        // THAT -- simulating a value the Europe/London-mislabelling
        // hypothesis does not actually explain, since neither the raw nor
        // the corrected reading is plausible relative to receipt. The
        // guarded correction must not trust the corrected value just
        // because it looks like the usual shape; it must fall back to the
        // raw interpretation and log loudly instead of guessing.
        let raw = "1784120400000"; // 2026-07-15T13:00:00Z as millis; corrects to 12:00:00Z
        let received_at: DateTime<Utc> = "2026-07-15T11:00:00Z".parse().unwrap(); // an hour before the correction

        let result = parse_trust_epoch_millis(raw, received_at).unwrap();

        assert_eq!(
            result,
            "2026-07-15T13:00:00Z".parse::<DateTime<Utc>>().unwrap(),
            "an implausible correction must fall back to the raw, uncorrected value"
        );
    }

    #[test]
    fn an_unparseable_value_returns_none() {
        let received_at: DateTime<Utc> = "2026-07-15T12:00:00Z".parse().unwrap();
        assert_eq!(parse_trust_epoch_millis("not-a-number", received_at), None);
    }

    #[test]
    fn is_plausible_actual_timestamp_allows_a_late_arrival_but_rejects_an_early_one() {
        let received_at: DateTime<Utc> = "2026-08-28T18:32:00Z".parse().unwrap();

        // Received well after the event -- always plausible, however late.
        assert!(is_plausible_actual_timestamp(
            "2026-08-28T10:00:00Z".parse().unwrap(),
            received_at
        ));
        // A couple of minutes ahead of receipt is ordinary clock skew.
        assert!(is_plausible_actual_timestamp(
            "2026-08-28T18:33:00Z".parse().unwrap(),
            received_at
        ));
        // ~60 minutes ahead of receipt -- the corruption this guards against.
        assert!(!is_plausible_actual_timestamp(
            "2026-08-28T19:32:00Z".parse().unwrap(),
            received_at
        ));
    }

    // --- `parse_trust_epoch_millis_pair` (Finding #1) ---

    /// The headline regression this fix exists for: a train running
    /// noticeably EARLY (the opposite of every pre-existing `LATE`-fixture
    /// test in this codebase, which is exactly why the split-correction bug
    /// shipped without any test catching it).
    ///
    /// The true, real-world event: this train's `actual` departure (18:00Z)
    /// was a full hour ahead of its `planned` departure (19:00Z) -- i.e. it
    /// left an hour early. The corrupted feed emits both fields as
    /// Europe/London-local-mislabelled-as-UTC wire values (true instant +1h):
    /// `actual` wire = 19:00:00Z, `planned` wire = 20:00:00Z. `received_at`
    /// is shortly after the TRUE actual departure (18:01Z).
    ///
    /// Calling the single-field `parse_trust_epoch_millis` independently on
    /// each field (the pre-fix behavior) reproduces the bug: `actual`'s own
    /// correction (19:00 -> 18:00) is plausible against `received_at`
    /// (18:01), so it's corrected -- but `planned`'s own correction (20:00
    /// -> 19:00) is NOT plausible against that same `received_at` (19:00 is
    /// almost an hour ahead of 18:01), so it falls back to the RAW value
    /// (20:00), desyncing the two fields by a full hour. This test proves
    /// that first, then proves `parse_trust_epoch_millis_pair` avoids it by
    /// applying `actual`'s own decision to `planned` too.
    #[test]
    fn an_early_running_train_is_corrected_or_left_raw_together_never_split() {
        let planned_raw = "1784145600000"; // 2026-07-15T20:00:00Z as millis
        let actual_raw = "1784142000000"; // 2026-07-15T19:00:00Z as millis
        let received_at: DateTime<Utc> = "2026-07-15T18:01:00Z".parse().unwrap();

        // First, confirm the bug this fix replaces would actually have
        // fired: independent single-field calls DO split.
        let independently_corrected_actual =
            parse_trust_epoch_millis(actual_raw, received_at).unwrap();
        let independently_corrected_planned =
            parse_trust_epoch_millis(planned_raw, received_at).unwrap();
        assert_eq!(
            independently_corrected_actual,
            "2026-07-15T18:00:00Z".parse::<DateTime<Utc>>().unwrap(),
            "actual's own correction is plausible on its own"
        );
        assert_eq!(
            independently_corrected_planned,
            "2026-07-15T20:00:00Z".parse::<DateTime<Utc>>().unwrap(),
            "planned's own correction (to 19:00Z) is NOT plausible against this received_at, so \
             calling parse_trust_epoch_millis independently falls back to the RAW value here -- \
             this is the split-correction bug Finding #1 fixed"
        );

        // Now the fix: the pairwise function must not split these.
        let pair =
            parse_trust_epoch_millis_pair(Some(planned_raw), Some(actual_raw), received_at, true);
        assert_eq!(pair.was_corrected, Some(true));
        assert_eq!(
            pair.actual,
            Some("2026-07-15T18:00:00Z".parse::<DateTime<Utc>>().unwrap())
        );
        assert_eq!(
            pair.planned,
            Some("2026-07-15T19:00:00Z".parse::<DateTime<Utc>>().unwrap()),
            "planned must be corrected in lockstep with actual, not independently rejected"
        );

        // And the resulting diff is self-consistent: the train really was
        // ~60 minutes early, not desynced into looking ~2 hours early or
        // anything else a split would have produced.
        let delay_minutes = (pair.actual.unwrap() - pair.planned.unwrap()).num_minutes();
        assert_eq!(delay_minutes, -60);
    }

    #[test]
    fn both_fields_stay_raw_together_when_the_anchor_correction_is_implausible() {
        // Same shape as `a_correction_that_is_itself_implausible_falls_back_to_the_raw_value`,
        // but exercised through the pair function to confirm `planned`
        // follows `actual`'s raw fallback too, not just its own.
        let planned_raw = "1784124000000"; // 2026-07-15T14:00:00Z as millis
        let actual_raw = "1784120400000"; // 2026-07-15T13:00:00Z as millis; corrects to 12:00:00Z
        let received_at: DateTime<Utc> = "2026-07-15T11:00:00Z".parse().unwrap();

        let pair =
            parse_trust_epoch_millis_pair(Some(planned_raw), Some(actual_raw), received_at, true);

        assert_eq!(pair.was_corrected, Some(false));
        assert_eq!(
            pair.actual,
            Some("2026-07-15T13:00:00Z".parse::<DateTime<Utc>>().unwrap()),
            "actual falls back to raw"
        );
        assert_eq!(
            pair.planned,
            Some("2026-07-15T14:00:00Z".parse::<DateTime<Utc>>().unwrap()),
            "planned must ALSO stay raw, following actual's decision -- not independently \
             corrected"
        );
    }

    #[test]
    fn a_missing_actual_leaves_no_decision_and_parses_planned_independently() {
        let planned_raw = "1784120400000"; // 2026-07-15T13:00:00Z as millis
        let received_at: DateTime<Utc> = "2026-07-15T12:01:00Z".parse().unwrap();

        let pair = parse_trust_epoch_millis_pair(Some(planned_raw), None, received_at, true);

        assert_eq!(pair.actual, None);
        assert_eq!(
            pair.was_corrected, None,
            "nothing to decide without an anchor"
        );
        assert_eq!(
            pair.planned,
            Some("2026-07-15T12:00:00Z".parse::<DateTime<Utc>>().unwrap()),
            "planned still gets the ordinary guarded single-field treatment"
        );
    }

    #[test]
    fn correction_disabled_leaves_every_field_raw_and_still_reports_a_decision() {
        let planned_raw = "1784145600000"; // 2026-07-15T20:00:00Z as millis
        let actual_raw = "1784142000000"; // 2026-07-15T19:00:00Z as millis
        let received_at: DateTime<Utc> = "2026-07-15T18:01:00Z".parse().unwrap();

        let pair =
            parse_trust_epoch_millis_pair(Some(planned_raw), Some(actual_raw), received_at, false);

        assert_eq!(
            pair.actual,
            Some("2026-07-15T19:00:00Z".parse::<DateTime<Utc>>().unwrap()),
            "the kill switch means the raw value is used even though it would otherwise \
             correct cleanly"
        );
        assert_eq!(
            pair.planned,
            Some("2026-07-15T20:00:00Z".parse::<DateTime<Utc>>().unwrap())
        );
        assert_eq!(
            pair.was_corrected,
            Some(false),
            "a decision was still made (correction available) -- it was just declined"
        );
    }

    /// The regression a second, independent adversarial review found in
    /// `6f58e0b` (the commit that fixed Finding #3, the fixed-BST-candidate
    /// autumn-fallback bug): sharing just the corrected-vs-raw BRANCH
    /// between `planned` and `actual` is not enough, because
    /// `apply_correction_decision` used to re-call
    /// `reinterpret_as_london_local` independently for `planned`, and that
    /// function makes its OWN `received_at`-nearest-candidate choice
    /// whenever the reinterpreted reading is DST-ambiguous. A train running
    /// 40 minutes late, with both raw readings inside the 2026-10-25
    /// 01:00-02:00 ambiguous hour, can have `actual` legitimately pick the
    /// BST candidate (nearest to `received_at`) while `planned` -- decided
    /// independently -- picks the GMT candidate instead, desyncing the two
    /// fields by a full hour even though both are correctly on the
    /// "corrected" branch together. The resulting `delay_minutes` comes out
    /// as -20 (looks 20 minutes EARLY) instead of the true +40 minutes
    /// late -- silent, undetectable corruption of exactly the kind Finding
    /// #1 exists to prevent.
    ///
    /// The fix: `parse_trust_epoch_millis_pair` computes the correction
    /// MAGNITUDE (`corrected - raw`) once, from `actual` alone, and applies
    /// that same delta to `planned`'s raw value directly, so `planned`
    /// never independently touches `reinterpret_as_london_local`'s
    /// ambiguous branch at all.
    #[test]
    fn an_autumn_fallback_ambiguous_pair_lands_on_the_same_side_together_and_delay_is_correct() {
        // Naive wall-clock reading "01:10" and "01:50" on 2026-10-25 each
        // occur twice: once in BST (00:10Z / 00:50Z) and once in GMT
        // (01:10Z / 01:50Z). The true event: planned 01:10 BST local
        // (00:10Z), actual 01:50 BST local (00:50Z) -- a 40-minutes-late
        // train, wire-corrupted (Europe/London-local-mislabelled-as-UTC) so
        // the raw fields read as if the naive wall-clock digits were
        // already UTC.
        let planned_raw = "1792890600000"; // 2026-10-25T01:10:00Z as millis
        let actual_raw = "1792893000000"; // 2026-10-25T01:50:00Z as millis
        // Realistic receipt: 1 minute after the TRUE actual event
        // (00:50Z/BST candidate) -- so `actual`'s own disambiguation
        // clearly picks the BST candidate (1 minute away) over the GMT one
        // (59 minutes away). `planned`'s OWN candidates, if disambiguated
        // independently, are roughly symmetric around this received_at (41
        // minutes to its BST candidate vs 19 minutes to its GMT one) --
        // close enough that, pre-fix, `planned` would independently pick
        // the WRONG (GMT) candidate even though `actual` correctly picked
        // BST, which is exactly the split this test guards against.
        let received_at: DateTime<Utc> = "2026-10-25T00:51:00Z".parse().unwrap();

        let pair =
            parse_trust_epoch_millis_pair(Some(planned_raw), Some(actual_raw), received_at, true);

        assert_eq!(pair.was_corrected, Some(true));
        assert_eq!(
            pair.actual,
            Some("2026-10-25T00:50:00Z".parse::<DateTime<Utc>>().unwrap()),
            "actual lands on the BST candidate, nearest received_at"
        );
        assert_eq!(
            pair.planned,
            Some("2026-10-25T00:10:00Z".parse::<DateTime<Utc>>().unwrap()),
            "planned must land on the SAME (BST) side as actual, following actual's exact \
             correction delta -- not independently re-disambiguating and landing on GMT instead"
        );

        let delay_minutes = (pair.actual.unwrap() - pair.planned.unwrap()).num_minutes();
        assert_eq!(
            delay_minutes, 40,
            "the true delay is +40 minutes late; a split (planned on GMT, actual on BST) would \
             corrupt this into -20 minutes (looking early) instead"
        );
    }

    // --- Finding #1: overflow no longer panics ---

    /// Direct regression for the panic: `apply_correction_decision` used to
    /// compute `raw_utc + correction_delta` via chrono's panicking
    /// `Add<TimeDelta>`. A raw value near `DateTime::<Utc>::MAX_UTC`,
    /// pushed further out by a positive `correction_delta`, would overflow
    /// `DateTime<Utc>`'s representable range and crash the whole
    /// processing task. `checked_add_signed` must turn this into a clean
    /// `None` instead.
    #[test]
    fn an_overflowing_correction_delta_returns_none_instead_of_panicking() {
        let near_max = DateTime::<Utc>::MAX_UTC - chrono::Duration::minutes(30);
        let raw = near_max.timestamp_millis().to_string();

        let result = apply_correction_decision(&raw, true, chrono::Duration::hours(1));

        assert_eq!(
            result, None,
            "a correction that would overflow DateTime<Utc>'s max must return None, not panic"
        );
    }

    /// The underflow counterpart, near `DateTime::<Utc>::MIN_UTC` with a
    /// negative `correction_delta` (the ordinary BST-correction direction).
    #[test]
    fn an_underflowing_correction_delta_returns_none_instead_of_panicking() {
        let near_min = DateTime::<Utc>::MIN_UTC + chrono::Duration::minutes(30);
        let raw = near_min.timestamp_millis().to_string();

        let result = apply_correction_decision(&raw, true, chrono::Duration::hours(-1));

        assert_eq!(
            result, None,
            "a correction that would underflow DateTime<Utc>'s min must return None, not panic"
        );
    }

    /// The `apply_correction_decision` overflow guard is only reached when
    /// `was_corrected` is `true` -- confirm the raw (uncorrected) branch
    /// still returns the raw value unchanged even at the extreme, since no
    /// arithmetic happens on that path at all.
    #[test]
    fn an_extreme_raw_value_is_returned_unchanged_when_not_corrected() {
        let near_max = DateTime::<Utc>::MAX_UTC - chrono::Duration::minutes(30);
        let millis = near_max.timestamp_millis();
        // `parse_raw_millis` (and this test's `raw` string) only carries
        // millisecond precision, so the expected value must be re-derived
        // from the same truncated millis rather than compared against
        // `near_max` itself, which still has its original sub-millisecond
        // component.
        let expected = DateTime::from_timestamp_millis(millis).unwrap();

        let result =
            apply_correction_decision(&millis.to_string(), false, chrono::Duration::hours(1));

        assert_eq!(result, Some(expected));
    }

    /// End-to-end regression through the real call path
    /// (`parse_trust_epoch_millis_pair`, as every TRUST consumer calls
    /// it): an ordinary, plausibly-correcting `actual_timestamp` paired
    /// with an extreme, poison `planned_timestamp` that would overflow
    /// once the same correction delta is applied to it. Must not panic --
    /// `actual` is corrected normally, `planned` comes back `None` exactly
    /// as it would for any other unparseable value, and processing
    /// continues.
    #[test]
    fn a_poison_planned_timestamp_near_the_representable_bound_does_not_panic_the_whole_message() {
        let actual_raw = "1784120400000"; // 2026-07-15T13:00:00Z as millis; corrects to 12:00:00Z (-1h delta)
        let received_at: DateTime<Utc> = "2026-07-15T12:01:00Z".parse().unwrap();
        let near_min = DateTime::<Utc>::MIN_UTC + chrono::Duration::minutes(30);
        let planned_raw = near_min.timestamp_millis().to_string();

        let pair =
            parse_trust_epoch_millis_pair(Some(&planned_raw), Some(actual_raw), received_at, true);

        assert_eq!(pair.was_corrected, Some(true));
        assert_eq!(
            pair.actual,
            Some("2026-07-15T12:00:00Z".parse::<DateTime<Utc>>().unwrap()),
            "actual corrects normally, unaffected by planned's overflow"
        );
        assert_eq!(
            pair.planned, None,
            "planned overflows DateTime<Utc>'s min applying the same -1h delta, and must come \
             back None rather than panicking the whole message"
        );
    }

    #[test]
    fn cancellation_shaped_call_with_no_planned_field_still_works() {
        // Mirrors how a Cancellation's lone canx_timestamp is parsed: no
        // `planned` at all, just the anchor.
        let actual_raw = "1784120400000"; // 2026-07-15T13:00:00Z as millis
        let received_at: DateTime<Utc> = "2026-07-15T12:01:00Z".parse().unwrap();

        let pair = parse_trust_epoch_millis_pair(None, Some(actual_raw), received_at, true);

        assert_eq!(pair.planned, None);
        assert_eq!(
            pair.actual,
            Some("2026-07-15T12:00:00Z".parse::<DateTime<Utc>>().unwrap())
        );
        assert_eq!(pair.was_corrected, Some(true));
    }

    // --- `plausible_delay_minutes` ---

    #[test]
    fn an_ordinary_delay_is_computed_normally() {
        let planned: DateTime<Utc> = "2026-09-25T18:15:00Z".parse().unwrap();
        let actual: DateTime<Utc> = "2026-09-25T18:22:00Z".parse().unwrap();
        assert_eq!(plausible_delay_minutes(actual, planned), Some(7));
    }

    #[test]
    fn an_early_departure_is_a_negative_delay() {
        let planned: DateTime<Utc> = "2026-09-25T18:15:00Z".parse().unwrap();
        let actual: DateTime<Utc> = "2026-09-25T18:10:00Z".parse().unwrap();
        assert_eq!(plausible_delay_minutes(actual, planned), Some(-5));
    }

    #[test]
    fn a_value_exactly_at_the_bound_is_still_plausible() {
        let planned: DateTime<Utc> = "2026-09-25T00:00:00Z".parse().unwrap();
        let actual = planned + chrono::Duration::minutes(MAX_PLAUSIBLE_DELAY_MINUTES);
        assert_eq!(
            plausible_delay_minutes(actual, planned),
            Some(MAX_PLAUSIBLE_DELAY_MINUTES as i32)
        );
    }

    /// The real bug this fix closes: one corrupt `actual_timestamp`
    /// (millions of minutes away from `planned`, not merely the ~60-minute
    /// shape this module's own correction targets) must not produce a
    /// fabricated `delay_minutes` -- it must be dropped (`None`), not
    /// truncating-cast into some other, still-wrong `i32`.
    #[test]
    fn a_wildly_corrupt_actual_timestamp_is_rejected_not_truncated() {
        let planned: DateTime<Utc> = "2026-09-25T18:15:00Z".parse().unwrap();
        // A plausible-looking millis value that happens to decode to a
        // timestamp decades away from `planned` -- exactly the shape of a
        // corrupted TRUST field this guards against, not a contrived
        // overflow input.
        let actual: DateTime<Utc> = "2090-01-01T00:00:00Z".parse().unwrap();
        assert_eq!(
            plausible_delay_minutes(actual, planned),
            None,
            "an implausible gap must be dropped, not written as a huge (or wrapped/truncated) \
             delay_minutes"
        );
    }

    #[test]
    fn a_wildly_corrupt_actual_timestamp_in_the_past_is_also_rejected() {
        let planned: DateTime<Utc> = "2026-09-25T18:15:00Z".parse().unwrap();
        let actual: DateTime<Utc> = "1970-01-01T00:00:00Z".parse().unwrap();
        assert_eq!(plausible_delay_minutes(actual, planned), None);
    }
}
