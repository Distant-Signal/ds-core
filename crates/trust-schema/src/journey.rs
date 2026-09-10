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

    DerivedState {
        status: if confirmed_terminus_arrival {
            "completed".to_string()
        } else {
            "en_route".to_string()
        },
        last_reported_location: location,
        last_event_type: Some(movement.event_type.clone()),
        delay_minutes,
        next_calling_point: previous.next_calling_point.clone(), // see module docs -- never populated ahead of time
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
