//! Pure position-in-journey derivation from a sequence of TRUST events.
//! Structured the way `crates/aggregator/src/matcher.rs` is pure and
//! independently testable -- no I/O, no database, just "given the
//! previous state and one new event, what's the new state."

use crate::schema::Movement;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct DerivedState {
    pub status: String, // "awaiting_activation" | "en_route" | "cancelled" | "completed"
    pub last_reported_location: Option<String>,
    pub last_event_type: Option<String>,
    pub delay_minutes: Option<i32>,
    pub next_calling_point: Option<String>,
}

impl DerivedState {
    pub fn awaiting_activation() -> Self {
        Self {
            status: "awaiting_activation".to_string(),
            ..Default::default()
        }
    }
}

/// `loc_crs` is the movement's location, already translated from STANOX by
/// the caller (a real STANOX->CRS table is out of scope for this plan --
/// see Task 14's note on where that lookup comes from); `None` if
/// untranslatable, in which case `last_reported_location` falls back to the
/// raw STANOX so nothing is silently dropped.
///
/// `destination_crs` is the train's own known final/terminus calling point
/// -- the caller's job to supply from whatever schedule source it has
/// (`trains.destination_crs` on the `crates/api` side; see this crate's
/// callers for exactly where each gets it from), `None` whenever no
/// schedule has ever matched this train and the destination is genuinely
/// unknown. Confirmed-arrival detection (below) is impossible in that
/// case, not merely skipped -- there is nothing to compare against.
///
/// Confirmed-arrival detection: an `ARRIVAL` event whose translated
/// `loc_crs` case-insensitively equals a known `destination_crs` is real,
/// positive evidence this journey has genuinely finished (not the
/// `frontend/components/TrainJourney.tsx` "no further calling points
/// reported" INFERENCE), so status becomes `"completed"` rather than
/// `"en_route"`. Deliberately narrower than "any ARRIVAL completes the
/// journey": an ARRIVAL at an intermediate calling point (`loc_crs` known,
/// but not equal to `destination_crs`) must stay `"en_route"`, and a
/// `DEPARTURE`/`PASS` at the destination (a diversion/depot move, or simply
/// routing through the terminus's own STANOX) must not be mistaken for a
/// genuine arrival either -- only `event_type == "ARRIVAL"` counts. The
/// comparison also deliberately uses the TRANSLATED `loc_crs`, never the
/// raw-STANOX `location` fallback below: an untranslatable location can
/// never be confirmed against a CRS-shaped destination, mirroring
/// `trust-consumer::process.rs`'s own "only a translated CRS can match a
/// pin" rule for origin departures.
///
/// A `PASS` still never completes the journey on its own, same as before
/// this comment -- only a confirmed terminus `ARRIVAL` does; every other
/// case (an ARRIVAL elsewhere, any DEPARTURE/PASS, or no known destination
/// at all) leaves status as `"en_route"` until an explicit Cancellation
/// ends it.
///
/// WHAT THIS DELIBERATELY CANNOT DO, and what backstops it. This function
/// sees one event at a time and knows nothing about the journey's shape, so
/// "the destination" can only ever be a CRS code here. On a service that
/// calls at its own destination CRS more than once -- every circular
/// working, e.g. South Western Railway's Kingston Loop, which departs
/// London Waterloo and terminates back at London Waterloo -- that is not
/// enough on its own to identify the FINAL calling point. The
/// requirement that only an `ARRIVAL` counts already covers the common
/// shape of that problem (a loop's first call at its own terminus CRS is a
/// DEPARTURE from the origin, never an arrival), but a mid-journey ARRIVAL
/// at a station the service later terminates at would still read as
/// completion here, and a genuine terminus arrival ingested before any
/// schedule supplied `destination_crs` is missed entirely and never
/// re-derived.
///
/// Both are backstopped on the read side, where the ordered calling-point
/// list actually exists: `api::data::journey::confirmed_final_arrival`
/// anchors the same rule to `stops.last()` -- the final scheduled calling
/// point BY POSITION, whatever CRS it carries and however many times that
/// CRS appears earlier -- and `apply_confirmed_arrival` folds the result
/// into the status the API serves. Keep the two rules ("only an ARRIVAL",
/// "at the final calling point") in step if either is ever changed.
pub fn apply_movement(
    previous: &DerivedState,
    movement: &Movement,
    loc_crs: Option<&str>,
    destination_crs: Option<&str>,
) -> DerivedState {
    let location = loc_crs
        .map(str::to_string)
        .or_else(|| movement.loc_stanox.clone());
    let delay_minutes = variation_to_minutes(movement.variation_status.as_deref());

    let confirmed_terminus_arrival = movement.event_type == "ARRIVAL"
        && match (loc_crs, destination_crs) {
            (Some(loc), Some(destination)) => loc.eq_ignore_ascii_case(destination),
            _ => false,
        };

    let naive_status = if confirmed_terminus_arrival {
        "completed"
    } else {
        "en_route"
    };

    DerivedState {
        // **Terminal-state guard (Low finding #1 of the 2026-09-25 review).**
        // Without this, a depot move or any other Movement landing AFTER the
        // confirmed terminus ARRIVAL that set `previous.status ==
        // "completed"` would unconditionally recompute `naive_status` as
        // `"en_route"` above (it is neither an ARRIVAL nor at the
        // destination) and silently regress an already-finished journey back
        // to running -- exactly the shape of bug this function's whole
        // design (a pure fold over one event at a time, per the module doc)
        // makes easy to introduce, because `apply_movement` has no memory of
        // "we already decided this is over" beyond `previous` itself.
        //
        // `status_rank` below is this crate's mirror of
        // `common::severity_rank`'s established "rank, don't compare
        // declaration/discriminant order" pattern: never let a transition
        // move a journey to a LOWER rank than it already reached. Once
        // `previous.status` is `"completed"` (or `"cancelled"` -- the other
        // terminal status this fold can be handed, e.g. if a stray Movement
        // arrives after `apply_cancellation` already ran) no Movement may
        // ever pull it back down to `"en_route"`; a `naive_status` at the
        // SAME rank (a second confirmed terminus ARRIVAL) still applies,
        // which is harmless idempotence, not a regression.
        status: if status_rank(naive_status) < status_rank(&previous.status) {
            previous.status.clone()
        } else {
            naive_status.to_string()
        },
        last_reported_location: location,
        last_event_type: Some(movement.event_type.clone()),
        delay_minutes,
        next_calling_point: previous.next_calling_point.clone(), // see module docs -- never populated ahead of time
    }
}

/// Rank of a journey `status` string for status-TRANSITION purposes only --
/// **higher means further along / more final**, mirroring
/// `common::severity_rank`'s "rank, don't compare declaration order"
/// pattern (see that function's own doc comment for the sibling reasoning).
/// `status` here is a plain `&str`, not an enum (see this module's own
/// `DerivedState::status` field comment for why), so this can't be an
/// exhaustive match on a closed type the way `severity_rank` is -- an
/// unrecognized value conservatively ranks as `0` (lowest), so it can never
/// itself block a legitimate transition.
///
/// Used by [`apply_movement`] to guard against a later event regressing an
/// already-`"completed"` (or `"cancelled"`) journey back to `"en_route"`.
/// [`apply_cancellation`] deliberately does NOT consult this: a cancellation
/// is real, independent evidence a journey has ended and must always apply,
/// even over a `"completed"` journey (e.g. a corrected/withdrawn terminus
/// arrival) -- unlike a bare Movement, it is never mistaken evidence of
/// still running.
fn status_rank(status: &str) -> u8 {
    match status {
        "awaiting_activation" => 0,
        "en_route" => 1,
        "completed" | "cancelled" => 2,
        _ => 0,
    }
}

pub fn apply_cancellation(previous: &DerivedState) -> DerivedState {
    DerivedState {
        status: "cancelled".to_string(),
        ..previous.clone()
    }
}

/// TRUST's `variation_status` is a category ("ON TIME", "LATE", "EARLY"),
/// not itself a minute count in the confirmed field list -- delay minutes
/// have to come from actual_timestamp - planned_timestamp instead, which
/// this function deliberately does NOT compute (it needs both timestamps
/// parsed, done by the caller in Task 14 where they're already in scope).
/// This function only normalizes the enum-shaped part: "ON TIME"/"EARLY"
/// clamp to zero (never negative -- a train running early isn't a
/// passenger-facing "delay"), "LATE" is left for the caller to fill in
/// with the real minute count, and anything else is `None`.
fn variation_to_minutes(variation_status: Option<&str>) -> Option<i32> {
    match variation_status {
        Some("ON TIME") | Some("EARLY") => Some(0),
        Some("LATE") => None, // caller overwrites with a real value
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn movement(event_type: &str, variation_status: Option<&str>) -> Movement {
        Movement {
            train_id: "221832406".to_string(),
            event_type: event_type.to_string(),
            gbtt_timestamp: None,
            planned_timestamp: None,
            actual_timestamp: None,
            reporting_stanox: None,
            loc_stanox: Some("87701".to_string()),
            toc_id: None,
            variation_status: variation_status.map(str::to_string),
        }
    }

    #[test]
    fn a_movement_sets_status_to_en_route() {
        let previous = DerivedState::awaiting_activation();
        let state = apply_movement(
            &previous,
            &movement("DEPARTURE", Some("ON TIME")),
            Some("WAT"),
            None,
        );
        assert_eq!(state.status, "en_route");
        assert_eq!(state.last_reported_location, Some("WAT".to_string()));
        assert_eq!(state.last_event_type, Some("DEPARTURE".to_string()));
    }

    #[test]
    fn falls_back_to_raw_stanox_when_untranslatable() {
        let previous = DerivedState::awaiting_activation();
        let state = apply_movement(&previous, &movement("PASS", None), None, None);
        assert_eq!(state.last_reported_location, Some("87701".to_string()));
    }

    #[test]
    fn on_time_and_early_clamp_delay_to_zero() {
        let previous = DerivedState::awaiting_activation();
        assert_eq!(
            apply_movement(
                &previous,
                &movement("ARRIVAL", Some("ON TIME")),
                Some("WOK"),
                None,
            )
            .delay_minutes,
            Some(0)
        );
        assert_eq!(
            apply_movement(
                &previous,
                &movement("ARRIVAL", Some("EARLY")),
                Some("WOK"),
                None,
            )
            .delay_minutes,
            Some(0)
        );
    }

    #[test]
    fn late_is_left_for_the_caller_to_fill_in() {
        let previous = DerivedState::awaiting_activation();
        assert_eq!(
            apply_movement(
                &previous,
                &movement("ARRIVAL", Some("LATE")),
                Some("WOK"),
                None,
            )
            .delay_minutes,
            None
        );
    }

    /// The headline scenario this feature exists for: an ARRIVAL at a
    /// location that translates to the train's own known `destination_crs`
    /// is real, confirmed evidence the journey has finished -- status must
    /// become `"completed"`, not the usual `"en_route"`.
    #[test]
    fn an_arrival_at_the_known_destination_marks_the_journey_completed() {
        let previous = DerivedState {
            status: "en_route".to_string(),
            last_reported_location: Some("CLJ".to_string()),
            last_event_type: Some("DEPARTURE".to_string()),
            delay_minutes: Some(2),
            next_calling_point: None,
        };
        let state = apply_movement(
            &previous,
            &movement("ARRIVAL", Some("ON TIME")),
            Some("WOK"),
            Some("WOK"),
        );
        assert_eq!(state.status, "completed");
        assert_eq!(state.last_reported_location, Some("WOK".to_string()));
    }

    /// The distinction that matters most: an ARRIVAL at an INTERMEDIATE
    /// calling point (a real, translated CRS, just not the train's own
    /// destination) must NOT be mistaken for a finished journey.
    #[test]
    fn an_arrival_at_a_non_destination_stop_stays_en_route() {
        let previous = DerivedState::awaiting_activation();
        let state = apply_movement(
            &previous,
            &movement("ARRIVAL", Some("ON TIME")),
            Some("CLJ"), // an intermediate stop
            Some("WOK"), // the train's real destination
        );
        assert_eq!(state.status, "en_route");
    }

    /// A DEPARTURE or PASS at the destination CRS (a depot move, a
    /// diversion, or simply routing back through the terminus's own
    /// STANOX) is not itself evidence of a genuine arrival -- only
    /// `event_type == "ARRIVAL"` may confirm completion.
    #[test]
    fn a_departure_at_the_destination_crs_does_not_complete_the_journey() {
        let previous = DerivedState::awaiting_activation();
        let state = apply_movement(
            &previous,
            &movement("DEPARTURE", Some("ON TIME")),
            Some("WOK"),
            Some("WOK"),
        );
        assert_eq!(state.status, "en_route");
    }

    /// No known destination at all (the common case for a train that has
    /// never been schedule-matched) means confirmed-arrival detection is
    /// impossible, not merely skipped -- status stays `"en_route"` exactly
    /// as it always did before this feature existed.
    #[test]
    fn an_arrival_with_no_known_destination_stays_en_route() {
        let previous = DerivedState::awaiting_activation();
        let state = apply_movement(
            &previous,
            &movement("ARRIVAL", Some("ON TIME")),
            Some("WOK"),
            None,
        );
        assert_eq!(state.status, "en_route");
    }

    /// The comparison is case-insensitive, same posture as every other
    /// CRS comparison in this codebase (e.g.
    /// `trust-consumer::process.rs`'s own pin matching).
    #[test]
    fn the_destination_comparison_is_case_insensitive() {
        let previous = DerivedState::awaiting_activation();
        let state = apply_movement(
            &previous,
            &movement("ARRIVAL", Some("ON TIME")),
            Some("wok"),
            Some("WOK"),
        );
        assert_eq!(state.status, "completed");
    }

    /// A circular working whose origin and terminus are the SAME station
    /// (South Western Railway's Kingston Loop: London Waterloo round via
    /// Kingston and Richmond, terminating back at London Waterloo). Both
    /// ends report `loc_crs == "WAT" == destination_crs`, so only the event
    /// type separates them -- the origin's DEPARTURE must leave the train
    /// running, and the terminus's ARRIVAL must finish it. See this
    /// function's own "WHAT THIS DELIBERATELY CANNOT DO" note for the
    /// sequence-anchored backstop that covers the shapes this rule can't.
    #[test]
    fn a_loop_service_completes_on_its_return_arrival_not_its_outbound_departure() {
        let previous = DerivedState::awaiting_activation();

        let leaving_waterloo = apply_movement(
            &previous,
            &movement("DEPARTURE", Some("ON TIME")),
            Some("WAT"),
            Some("WAT"),
        );
        assert_eq!(
            leaving_waterloo.status, "en_route",
            "the loop has only just started -- it is at its destination CRS, not its destination"
        );

        let back_at_waterloo = apply_movement(
            &leaving_waterloo,
            &movement("ARRIVAL", Some("LATE")),
            Some("WAT"),
            Some("WAT"),
        );
        assert_eq!(back_at_waterloo.status, "completed");
    }

    /// **Low finding #1 of the 2026-09-25 review, this fix's own regression
    /// test.** A depot move (or any other Movement) arriving AFTER the
    /// confirmed terminus ARRIVAL already marked the journey `"completed"`
    /// must not regress it back to `"en_route"` -- before the `status_rank`
    /// guard, this exact sequence did exactly that, because `apply_movement`
    /// recomputes status from scratch on every call and a DEPARTURE/PASS at
    /// the destination is (correctly) never itself confirmed-arrival
    /// evidence, so the naive result was `"en_route"`.
    #[test]
    fn a_later_movement_does_not_regress_a_completed_journey_to_en_route() {
        let previous = DerivedState::awaiting_activation();

        let arrived = apply_movement(
            &previous,
            &movement("ARRIVAL", Some("ON TIME")),
            Some("WOK"),
            Some("WOK"),
        );
        assert_eq!(arrived.status, "completed");

        // A depot move away from the terminus platform, reported against the
        // same STANOX/CRS -- exactly the shape TRUST emits for stabling
        // moves after a service has finished.
        let depot_move = apply_movement(
            &arrived,
            &movement("DEPARTURE", Some("ON TIME")),
            Some("WOK"),
            Some("WOK"),
        );
        assert_eq!(
            depot_move.status, "completed",
            "a movement after a confirmed terminus arrival must not regress the journey"
        );

        // A PASS somewhere else entirely (not even at the destination) must
        // be blocked the same way -- the guard is on `previous.status`, not
        // on the new event's own location.
        let stray_pass = apply_movement(
            &arrived,
            &movement("PASS", Some("ON TIME")),
            Some("CLJ"),
            Some("WOK"),
        );
        assert_eq!(
            stray_pass.status, "completed",
            "a movement anywhere else, after completion, must still not regress the journey"
        );
    }

    /// A second confirmed terminus ARRIVAL after the first (TRUST resending
    /// or correcting the same event) is a same-rank transition, not a
    /// regression -- it must still apply cleanly rather than being blocked
    /// by the guard above.
    #[test]
    fn a_repeated_confirmed_arrival_after_completion_stays_completed() {
        let previous = DerivedState::awaiting_activation();
        let arrived = apply_movement(
            &previous,
            &movement("ARRIVAL", Some("ON TIME")),
            Some("WOK"),
            Some("WOK"),
        );
        assert_eq!(arrived.status, "completed");

        let arrived_again = apply_movement(
            &arrived,
            &movement("ARRIVAL", Some("LATE")),
            Some("WOK"),
            Some("WOK"),
        );
        assert_eq!(arrived_again.status, "completed");
    }

    /// A Movement arriving after `apply_cancellation` already ran must not
    /// resurrect a cancelled journey as `"en_route"` either -- `"cancelled"`
    /// is the other terminal status `status_rank` protects.
    #[test]
    fn a_movement_after_cancellation_does_not_resurrect_the_journey() {
        let previous = DerivedState::awaiting_activation();
        let cancelled = apply_cancellation(&previous);
        assert_eq!(cancelled.status, "cancelled");

        let stray_movement = apply_movement(
            &cancelled,
            &movement("DEPARTURE", Some("ON TIME")),
            Some("WAT"),
            None,
        );
        assert_eq!(stray_movement.status, "cancelled");
    }

    #[test]
    fn cancellation_preserves_last_known_location() {
        let previous = DerivedState {
            status: "en_route".to_string(),
            last_reported_location: Some("WOK".to_string()),
            last_event_type: Some("DEPARTURE".to_string()),
            delay_minutes: Some(4),
            next_calling_point: None,
        };
        let state = apply_cancellation(&previous);
        assert_eq!(state.status, "cancelled");
        assert_eq!(state.last_reported_location, Some("WOK".to_string()));
    }
}
