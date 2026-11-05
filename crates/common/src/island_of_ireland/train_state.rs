//! What an Iarnród Éireann departure board says about one train, read the
//! same way everywhere (distant-signal's
//! docs/superpowers/specs/2026-11-05-irish-rail-train-tracking-design.md
//! §6): the api's train page and the writer's tracked-train state.
//!
//! The rule is that a train we haven't seen is unknown, not on time: every
//! state here needs an observation behind it, and a field the feed doesn't
//! vouch for is `None`, never a default. `Late` in particular is `0` on a
//! train with no report (`Status` "No Information"), which means nothing.
//!
//! The only `Status` values in our captured responses are `"En Route"` and
//! `"No Information"`; `Lastlocation` reads `"Departed <place>"` or
//! `"Arrived <place>"`, or is empty. Anything else is treated as unknown
//! text, not guessed at ([`is_known_status`] lets a caller count it).

use chrono::{DateTime, Duration, NaiveTime, Timelike, Utc};
use serde::{Deserialize, Serialize};

/// A train that was running but has been on no board for this long is
/// "not seen recently": the page says when we last saw it instead of
/// showing its old state as current. Six poll cycles of the live poller.
pub const NOT_SEEN_RECENTLY_AFTER: Duration = Duration::minutes(30);

/// The `Status` values seen in captured responses.
const EN_ROUTE: &str = "En Route";
const NO_INFORMATION: &str = "No Information";

/// Whether `status` is one of the values we have seen and read
/// ([`interpret`] still handles any other value, as unknown text).
pub fn is_known_status(status: &str) -> bool {
    let status = status.trim();
    status.eq_ignore_ascii_case(EN_ROUTE) || status.eq_ignore_ascii_case(NO_INFORMATION)
}

/// What `Lastlocation` says happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts", derive(ts_rs::TS, schemars::JsonSchema))]
pub enum LastLocationEvent {
    Departed,
    Arrived,
}

/// `Lastlocation` read: `"Departed Dundalk"` is `Departed` at `"Dundalk"`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS, schemars::JsonSchema))]
pub struct LastLocation {
    pub event: LastLocationEvent,
    pub place: String,
}

/// Reads `Lastlocation`. `None` when blank or not one of the two shapes
/// we know (the raw text stays in the history either way).
pub fn parse_last_location(raw: &str) -> Option<LastLocation> {
    let raw = raw.trim();
    let (word, place) = raw.split_once(char::is_whitespace)?;
    let event = if word.eq_ignore_ascii_case("Departed") {
        LastLocationEvent::Departed
    } else if word.eq_ignore_ascii_case("Arrived") {
        LastLocationEvent::Arrived
    } else {
        return None;
    };
    let place = place.trim();
    (!place.is_empty()).then(|| LastLocation {
        event,
        place: place.to_owned(),
    })
}

/// A train's live state, as far as the boards show it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts", derive(ts_rs::TS, schemars::JsonSchema))]
pub enum LiveState {
    /// On a board, but with no report: not running yet, or not reporting.
    NoReport,
    /// Reporting: `Status` "En Route" or a `Lastlocation`.
    Running,
    /// `Lastlocation` says it arrived at its destination.
    Arrived,
    /// Was on a board, but on none for [`NOT_SEEN_RECENTLY_AFTER`], and we
    /// never saw it arrive.
    NotSeenRecently,
}

/// The train-level fields of the newest board observation of a train.
#[derive(Debug, Clone, Copy)]
pub struct Observation<'a> {
    pub status: &'a str,
    pub late_minutes: Option<i32>,
    pub last_location: Option<&'a str>,
    /// The train's `Destination`, to tell an arrival there from one on
    /// the way.
    pub destination: &'a str,
    /// When a board last showed the train.
    pub last_seen_at: DateTime<Utc>,
}

/// [`interpret`]'s reading of an [`Observation`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interpretation {
    pub state: LiveState,
    /// The feed's `Late` (signed: negative is early), only when the train
    /// was reporting; `None` otherwise.
    pub late_minutes: Option<i32>,
    pub last_location: Option<LastLocation>,
}

/// Reads one observation at `now` (§6.2 of the design).
pub fn interpret(observation: &Observation<'_>, now: DateTime<Utc>) -> Interpretation {
    let last_location = observation.last_location.and_then(parse_last_location);
    let arrived = last_location.as_ref().is_some_and(|l| {
        l.event == LastLocationEvent::Arrived
            && l.place.eq_ignore_ascii_case(observation.destination.trim())
    });
    let reporting = arrived
        || last_location.is_some()
        || observation.status.trim().eq_ignore_ascii_case(EN_ROUTE);
    let late_minutes = observation.late_minutes.filter(|_| reporting);
    let state = if arrived {
        LiveState::Arrived
    } else if now - observation.last_seen_at >= NOT_SEEN_RECENTLY_AFTER {
        LiveState::NotSeenRecently
    } else if reporting {
        LiveState::Running
    } else {
        LiveState::NoReport
    };
    Interpretation {
        state,
        late_minutes,
        last_location,
    }
}

/// What a tracked train's `train_current_state` row says, read from an
/// [`Interpretation`] (design §6.2's last column).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentState {
    /// `awaiting_activation`, `en_route` or `completed`: never `cancelled`,
    /// which no board field tells us (design §6.4).
    pub status: &'static str,
    /// The feed's `Late`; `None` with no report.
    pub delay_minutes: Option<i32>,
    /// `Lastlocation`'s place.
    pub last_reported_location: Option<String>,
    /// `DEPARTURE` or `ARRIVAL`, from `Lastlocation`.
    pub last_event_type: Option<&'static str>,
}

/// `train_current_state.last_event_type` for a [`LastLocationEvent`], in
/// TRUST's words, as GB trains store it.
pub fn event_type(event: LastLocationEvent) -> &'static str {
    match event {
        LastLocationEvent::Departed => "DEPARTURE",
        LastLocationEvent::Arrived => "ARRIVAL",
    }
}

/// The `train_current_state` a tracked train gets from `read`, or `None`
/// for [`LiveState::NotSeenRecently`]: a train off every board keeps the
/// state it had (design §6.2: "unchanged (no write)").
pub fn current_state(read: &Interpretation) -> Option<CurrentState> {
    let located = || {
        read.last_location
            .as_ref()
            .map(|l| (l.place.clone(), event_type(l.event)))
    };
    let (status, delay_minutes, location) = match read.state {
        LiveState::NotSeenRecently => return None,
        LiveState::NoReport => ("awaiting_activation", None, None),
        LiveState::Running => ("en_route", read.late_minutes, located()),
        LiveState::Arrived => ("completed", read.late_minutes, located()),
    };
    let (last_reported_location, last_event_type) = location.unzip();
    Some(CurrentState {
        status,
        delay_minutes,
        last_reported_location,
        last_event_type,
    })
}

/// A board time as the history stores it: `HH:MM`, or `None` for a blank
/// value, a malformed one, or the feed's `00:00`, which it sends for "no
/// time" (a call with no arrival, at an origin, or no departure, at a
/// destination). A train really due at midnight is lost to that, which
/// the feed leaves no way to tell apart.
pub fn board_time(raw: Option<&str>) -> Option<String> {
    let raw = raw?.trim();
    let time = NaiveTime::parse_from_str(raw, "%H:%M").ok()?;
    (time != NaiveTime::MIN).then(|| raw.to_owned())
}

/// Minutes from the start of the train's service day to `time` (`HH:MM`),
/// for putting its calls in running order: a time earlier than the
/// train's `origin_time` is the next day's, so a train that runs past
/// midnight keeps its order. `None` when `time` doesn't parse.
pub fn running_order_minutes(time: &str, origin_time: Option<&str>) -> Option<u32> {
    let minutes = |t: &str| {
        NaiveTime::parse_from_str(t.trim(), "%H:%M")
            .ok()
            .map(|t| t.hour() * 60 + t.minute())
    };
    let at = minutes(time)?;
    match origin_time.and_then(minutes) {
        Some(start) if at < start => Some(at + 24 * 60),
        _ => Some(at),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn observation<'a>(
        status: &'a str,
        late: i32,
        last_location: Option<&'a str>,
    ) -> Observation<'a> {
        Observation {
            status,
            late_minutes: Some(late),
            last_location,
            destination: "Belfast",
            last_seen_at: at("2026-11-05T16:59:57Z"),
        }
    }

    const NOW: &str = "2026-11-05T17:02:00Z";

    #[test]
    fn last_location_reads_both_shapes_and_nothing_else() {
        assert_eq!(
            parse_last_location("Departed Dundalk"),
            Some(LastLocation {
                event: LastLocationEvent::Departed,
                place: "Dundalk".into()
            })
        );
        assert_eq!(
            parse_last_location(" Arrived  Dublin Connolly "),
            Some(LastLocation {
                event: LastLocationEvent::Arrived,
                place: "Dublin Connolly".into()
            })
        );
        assert_eq!(parse_last_location(""), None);
        assert_eq!(parse_last_location("Departed"), None);
        assert_eq!(parse_last_location("Passed Dundalk"), None);
    }

    /// The captured Belfast board's A120: en route, 2 late.
    #[test]
    fn a_reporting_train_is_running_with_its_delay() {
        let read = interpret(
            &observation("En Route", 2, Some("Departed Dundalk")),
            at(NOW),
        );
        assert_eq!(read.state, LiveState::Running);
        assert_eq!(read.late_minutes, Some(2));
        assert_eq!(
            read.last_location.map(|l| l.event),
            Some(LastLocationEvent::Departed)
        );
    }

    /// The captured A123: "No Information", `Late` 0, no location. The 0 is
    /// not "on time".
    #[test]
    fn no_information_is_no_report_with_no_delay() {
        let read = interpret(&observation("No Information", 0, None), at(NOW));
        assert_eq!(read.state, LiveState::NoReport);
        assert_eq!(read.late_minutes, None);
        assert_eq!(read.last_location, None);
    }

    #[test]
    fn early_running_keeps_its_negative_delay() {
        let read = interpret(&observation("En Route", -2, None), at(NOW));
        assert_eq!(
            (read.state, read.late_minutes),
            (LiveState::Running, Some(-2))
        );
    }

    #[test]
    fn arrived_only_at_the_destination() {
        let at_destination = interpret(
            &observation("En Route", 4, Some("Arrived belfast")),
            at(NOW),
        );
        assert_eq!(at_destination.state, LiveState::Arrived);
        assert_eq!(at_destination.late_minutes, Some(4));
        let on_the_way = interpret(&observation("En Route", 4, Some("Arrived Newry")), at(NOW));
        assert_eq!(on_the_way.state, LiveState::Running);
    }

    /// An arrival is final; anything else seen 30 minutes ago is not
    /// current any more.
    #[test]
    fn a_train_off_every_board_is_not_seen_recently_unless_it_arrived() {
        let later = at("2026-11-05T17:29:57Z");
        let running = observation("En Route", 6, Some("Departed Newry"));
        assert_eq!(interpret(&running, later).state, LiveState::NotSeenRecently);
        assert_eq!(interpret(&running, later).late_minutes, Some(6));
        let quiet = observation("No Information", 0, None);
        assert_eq!(interpret(&quiet, later).state, LiveState::NotSeenRecently);
        assert_eq!(interpret(&quiet, later).late_minutes, None);
        let arrived = observation("En Route", 6, Some("Arrived Belfast"));
        assert_eq!(interpret(&arrived, later).state, LiveState::Arrived);
        let just_before = at("2026-11-05T17:29:56Z");
        assert_eq!(interpret(&running, just_before).state, LiveState::Running);
    }

    /// An unknown `Status` with no location tells us nothing; with one, the
    /// location says the train is moving.
    #[test]
    fn an_unknown_status_falls_back_on_the_location() {
        assert!(!is_known_status("Terminated"));
        assert!(is_known_status(" en route "));
        let quiet = interpret(&observation("Terminated", 3, None), at(NOW));
        assert_eq!(
            (quiet.state, quiet.late_minutes),
            (LiveState::NoReport, None)
        );
        let moving = interpret(
            &observation("Terminated", 3, Some("Departed Drogheda")),
            at(NOW),
        );
        assert_eq!(
            (moving.state, moving.late_minutes),
            (LiveState::Running, Some(3))
        );
    }

    /// Design §6.2's last column, row by row.
    #[test]
    fn current_state_follows_the_interpretation() {
        let now = at(NOW);
        let quiet = interpret(&observation("No Information", 0, None), now);
        assert_eq!(
            current_state(&quiet),
            Some(CurrentState {
                status: "awaiting_activation",
                delay_minutes: None,
                last_reported_location: None,
                last_event_type: None,
            })
        );
        let running = interpret(&observation("En Route", 16, Some("Departed Dundalk")), now);
        assert_eq!(
            current_state(&running),
            Some(CurrentState {
                status: "en_route",
                delay_minutes: Some(16),
                last_reported_location: Some("Dundalk".into()),
                last_event_type: Some("DEPARTURE"),
            })
        );
        let at_newry = interpret(&observation("En Route", 3, Some("Arrived Newry")), now);
        assert_eq!(
            current_state(&at_newry).map(|s| (s.status, s.last_event_type)),
            Some(("en_route", Some("ARRIVAL")))
        );
        let no_location = interpret(&observation("En Route", 1, None), now);
        assert_eq!(
            current_state(&no_location),
            Some(CurrentState {
                status: "en_route",
                delay_minutes: Some(1),
                last_reported_location: None,
                last_event_type: None,
            })
        );
        let arrived = interpret(&observation("En Route", 4, Some("Arrived Belfast")), now);
        assert_eq!(
            current_state(&arrived),
            Some(CurrentState {
                status: "completed",
                delay_minutes: Some(4),
                last_reported_location: Some("Belfast".into()),
                last_event_type: Some("ARRIVAL"),
            })
        );
        let gone = interpret(
            &observation("En Route", 6, Some("Departed Newry")),
            at("2026-11-05T17:40:00Z"),
        );
        assert_eq!(current_state(&gone), None);
    }

    #[test]
    fn board_times_drop_the_feeds_none_value() {
        assert_eq!(board_time(Some("17:58")), Some("17:58".into()));
        assert_eq!(board_time(Some(" 07:05 ")), Some("07:05".into()));
        assert_eq!(board_time(Some("00:00")), None);
        assert_eq!(board_time(Some("")), None);
        assert_eq!(board_time(Some("25:00")), None);
        assert_eq!(board_time(None), None);
    }

    #[test]
    fn running_order_wraps_past_midnight_from_the_origin_time() {
        assert_eq!(running_order_minutes("23:55", Some("23:30")), Some(1435));
        assert_eq!(running_order_minutes("00:20", Some("23:30")), Some(1460));
        assert_eq!(running_order_minutes("08:00", Some("07:30")), Some(480));
        assert_eq!(running_order_minutes("08:00", None), Some(480));
        assert_eq!(running_order_minutes("bad", Some("07:30")), None);
    }
}
