//! A whole-day connections array: one entry per consecutive pair of a
//! resolved, non-cancelled schedule's calling points, sorted by departure.
//! The shared input both Connection Scan (Phase 3) and RAPTOR (Phase 4)
//! sweep -- see this codebase's
//! docs/superpowers/plans/2026-09-22-dynamic-trip-planning-phase2-connections-array-plan.md
//! for why the two algorithms must consume the SAME array, unmodified by
//! either, for their later agreement to be meaningful.
//!
//! No I/O, no dependency on [`crate::resolve::ScheduleIndex`] -- takes
//! [`CallingPointForConnections`] slices grouped by schedule identity,
//! deliberately decoupled from HOW those calling points were produced (an
//! in-memory `ScheduleIndex` resolve, in this crate's own tests, or a
//! Postgres row set re-hydrated by `crates/api`'s `data::trip_planning`,
//! in production) -- same "pure function over already-shaped data"
//! convention every other function in this crate already follows.

use chrono::NaiveTime;

/// One calling point, reduced to exactly what `build_connections` needs --
/// deliberately NOT [`crate::records::CallingPoint`] itself, so this module
/// has no dependency on how the caller obtained the data (a resolved
/// `ScheduleIndex` schedule, or a Postgres row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallingPointForConnections {
    pub tiploc: String,
    pub booked_arrival: Option<NaiveTime>,
    pub booked_departure: Option<NaiveTime>,
    /// Calendar days past this schedule's own service date -- see
    /// [`crate::records::CallingPoint::day_offset`]'s own doc comment for
    /// the real overnight case this exists to handle. `build_connections`
    /// folds this directly into `departure_min`/`arrival_min` rather than
    /// re-deriving a rollover from adjacent-pair comparison (an
    /// improvement over a same-pair-only heuristic -- see this plan's
    /// Judgment Call 5).
    pub day_offset: u8,
}

impl From<&crate::records::CallingPoint> for CallingPointForConnections {
    fn from(cp: &crate::records::CallingPoint) -> Self {
        Self {
            tiploc: cp.tiploc.clone(),
            booked_arrival: cp.booked_arrival,
            booked_departure: cp.booked_departure,
            day_offset: cp.day_offset,
        }
    }
}

/// One consecutive pair of public calling points on one schedule -- the
/// edge type both search algorithms walk. `uid` is this app's own train
/// UID, doubling as "which physical working is this" (see this plan's
/// Judgment Call 4 for why this app needs no separate synthetic id, unlike
/// the sibling `Distant-Signal-MCP` project's own `scheduleId`/
/// `sourceScheduleId` split).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connection {
    pub uid: String,
    pub from_tiploc: String,
    pub to_tiploc: String,
    /// Minutes from midnight of the schedule's OWN service date --
    /// `day_offset * 1440 + minutes-since-midnight`, so a connection whose
    /// calling points fall on a later calendar day than the schedule's own
    /// service date is already correctly ordered against same-day
    /// connections, with no adjacent-pair rollover heuristic needed.
    pub departure_min: u32,
    pub arrival_min: u32,
}

fn minutes_from_midnight(time: NaiveTime, day_offset: u8) -> u32 {
    use chrono::Timelike;
    time.num_seconds_from_midnight() / 60 + u32::from(day_offset) * 1440
}

/// Builds and sorts the whole connections array. `schedules` is every
/// resolved, non-cancelled schedule to include, as `(uid, calling points in
/// stopping order)` pairs -- the caller (a `ScheduleIndex`-driven test
/// fixture, or `crates/api`'s Postgres-row hydration) is responsible for
/// having already excluded cancelled schedules and having each schedule's
/// own calling points already in seq order; this function does no
/// resolution or reordering of its own.
///
/// A connection joins the earlier point with a `booked_departure` to the
/// NEAREST later point with a `booked_arrival` -- not necessarily its
/// immediate slice neighbour. Real CIF schedules interleave genuine calling
/// points with non-stopping junction/pass-point TIPLOCs (e.g. `CMDNJN`,
/// `WATFDJ` used as a pass rather than a call, `RUGBY`, `STOKOTJ`) that
/// carry no booked time at all -- CIF gives those a separate "Scheduled
/// Pass" time this crate does not decode (see
/// `crate::parse::parse_calling_point`'s own doc comment), so they
/// correctly arrive here as `CallingPointForConnections` with both fields
/// `None`. `schedule_calling_points_full_rows`
/// (`crates/schedule-reference/src/main.rs`) does not filter these rows out
/// -- a blank CIF Activity code deliberately "fails open" as boardable
/// (see `crate::records::CallingPoint::is_public_pickup`'s own doc
/// comment), and a real pass point has both a blank Activity code and no
/// booked time, so it is published here indistinguishable from a genuine
/// calling point except for carrying no time. Requiring strict pairwise
/// adjacency (as this function used to) treats every one of these untimed
/// rows as a hard break in the graph, severing it at almost every junction
/// on almost every real multi-station schedule -- confirmed live
/// 2026-09-27 against UID `W33128` (EUSTON -> Manchester Piccadilly),
/// which produced zero trip-planner itineraries despite the direct service
/// genuinely existing. Walking forward past any untimed row(s) instead
/// connects the two REAL timed calling points directly, carrying the
/// correct transit time across the skipped gap (the skipped rows have no
/// timing data to contribute either way, so nothing is fabricated or
/// interpolated -- the edge is exactly the timed "from" point's own
/// `booked_departure` to the timed "to" point's own `booked_arrival`,
/// precisely as it would be for a genuinely adjacent pair).
///
/// An `Origin` point has no arrival; a `Terminate` point has no departure
/// (see [`crate::records::CallingPointKind`]) -- those simply cannot start
/// (`Terminate`) or cannot be the `to` of (`Origin`) a connection, never a
/// fabricated one. A schedule with fewer than two calling points, or one
/// whose remaining timed points are fewer than two, contributes nothing.
///
/// Sorted by `(departure_min, uid, from_tiploc)`, not `departure_min`
/// alone: `schedules` is commonly driven by a `HashMap` at the call site
/// (e.g. `crates/api/src/data/trip_planning.rs`'s `build_connections_for_date`,
/// grouping by `uid`), whose iteration order is randomized per-process, and
/// hundreds of connections can share the same `departure_min` at
/// whole-network scale. `sort_by_key` is a stable sort, so without a fully
/// deterministic key, same-minute connections would tie-break on whatever
/// order the random `HashMap` walk happened to produce -- nondeterministic
/// across runs. Both Connection Scan (Phase 3) and RAPTOR (Phase 4) must
/// see the SAME array, including the same tie-break order, for their later
/// agreement to be meaningful (see this module's own doc comment).
pub fn build_connections<'a>(
    schedules: impl IntoIterator<Item = (&'a str, &'a [CallingPointForConnections])>,
) -> Vec<Connection> {
    let mut connections = Vec::new();
    for (uid, calling_points) in schedules {
        for (i, from) in calling_points.iter().enumerate() {
            let Some(departure) = from.booked_departure else {
                continue;
            };
            let Some(to) = calling_points[i + 1..]
                .iter()
                .find(|cp| cp.booked_arrival.is_some())
            else {
                continue;
            };
            let arrival = to.booked_arrival.expect("just checked is_some");
            connections.push(Connection {
                uid: uid.to_string(),
                from_tiploc: from.tiploc.clone(),
                to_tiploc: to.tiploc.clone(),
                departure_min: minutes_from_midnight(departure, from.day_offset),
                arrival_min: minutes_from_midnight(arrival, to.day_offset),
            });
        }
    }
    connections.sort_by(|a, b| {
        (a.departure_min, &a.uid, &a.from_tiploc).cmp(&(b.departure_min, &b.uid, &b.from_tiploc))
    });
    connections
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cp(
        tiploc: &str,
        arrival: Option<&str>,
        departure: Option<&str>,
        day_offset: u8,
    ) -> CallingPointForConnections {
        CallingPointForConnections {
            tiploc: tiploc.to_string(),
            booked_arrival: arrival.map(|t| t.parse().unwrap()),
            booked_departure: departure.map(|t| t.parse().unwrap()),
            day_offset,
        }
    }

    #[test]
    fn a_three_stop_schedule_produces_two_connections_in_departure_order() {
        let points = vec![
            cp("EUSTON", None, Some("08:00:00"), 0),
            cp("WATFDJ", Some("08:20:00"), Some("08:21:00"), 0),
            cp("MKC", Some("08:50:00"), None, 0),
        ];
        let connections = build_connections([("C11052", points.as_slice())]);
        assert_eq!(connections.len(), 2);
        assert_eq!(connections[0].from_tiploc, "EUSTON");
        assert_eq!(connections[0].to_tiploc, "WATFDJ");
        assert_eq!(connections[0].departure_min, 480);
        assert_eq!(connections[0].arrival_min, 500);
        assert_eq!(connections[1].from_tiploc, "WATFDJ");
        assert_eq!(connections[1].to_tiploc, "MKC");
    }

    #[test]
    fn a_single_calling_point_schedule_produces_no_connections() {
        let points = vec![cp("EUSTON", None, Some("08:00:00"), 0)];
        assert!(build_connections([("C11052", points.as_slice())]).is_empty());
    }

    #[test]
    fn an_overnight_calling_point_sorts_correctly_via_day_offset_not_a_rollover_heuristic() {
        // Real live-confirmed shape (schedule_query::resolve's own
        // day_offset tests): Liverpool St 23:48 -> Barking 00:06 next day.
        let points = vec![
            cp("LIVST", None, Some("23:48:00"), 0),
            cp("BARKING", Some("00:06:00"), None, 1),
        ];
        let connections = build_connections([("F49687", points.as_slice())]);
        assert_eq!(connections.len(), 1);
        assert_eq!(connections[0].departure_min, 23 * 60 + 48);
        assert_eq!(connections[0].arrival_min, 1440 + 6);
        assert!(
            connections[0].arrival_min > connections[0].departure_min,
            "day_offset must make this a positive-duration connection, not a negative one"
        );
    }

    #[test]
    fn a_terminate_point_followed_by_nothing_boardable_contributes_no_connection() {
        // An Origin-kind point with no booked_departure at all (a real,
        // if rare, malformed-looking but non-panicking shape) must not
        // fabricate a connection.
        let points = vec![
            cp("EUSTON", None, None, 0),
            cp("MKC", Some("08:50:00"), None, 0),
        ];
        assert!(build_connections([("C1", points.as_slice())]).is_empty());
    }

    #[test]
    fn results_are_sorted_by_departure_across_multiple_schedules() {
        let early = vec![
            cp("A", None, Some("06:00:00"), 0),
            cp("B", Some("06:10:00"), None, 0),
        ];
        let late = vec![
            cp("A", None, Some("09:00:00"), 0),
            cp("B", Some("09:10:00"), None, 0),
        ];
        let connections =
            build_connections([("LATE", late.as_slice()), ("EARLY", early.as_slice())]);
        assert_eq!(connections[0].uid, "EARLY");
        assert_eq!(connections[1].uid, "LATE");
    }

    #[test]
    fn untimed_junction_rows_between_two_timed_stops_still_produce_a_connection() {
        // Real-world shape confirmed live 2026-09-27 against UID `W33128`
        // (EUSTON -> Manchester Piccadilly): `schedule_calling_points_full`
        // carries genuine, non-stopping junction/pass-point TIPLOCs
        // (CMDNJN, WATFDJ used here as a pass rather than a call, RUGBY,
        // STOKOTJ) interleaved between real calling points, each with
        // neither a booked arrival nor departure. Before this fix,
        // `build_connections`'s strict `windows(2)` adjacency produced ZERO
        // edges across a gap like this, severing the whole graph at this
        // junction even though EUSTON and MAN are both real, genuinely
        // timed calling points of the same schedule.
        let points = vec![
            cp("EUSTON", None, Some("08:00:00"), 0),
            cp("CMDNJN", None, None, 0),
            cp("WATFDJ", None, None, 0),
            cp("RUGBY", None, None, 0),
            cp("STOKOTJ", None, None, 0),
            cp("MAN", Some("10:50:00"), None, 0),
        ];
        let connections = build_connections([("W33128", points.as_slice())]);
        assert_eq!(
            connections.len(),
            1,
            "the four untimed junction rows must be skipped over, not treated as breaks"
        );
        assert_eq!(connections[0].from_tiploc, "EUSTON");
        assert_eq!(connections[0].to_tiploc, "MAN");
        assert_eq!(connections[0].departure_min, 8 * 60);
        assert_eq!(connections[0].arrival_min, 10 * 60 + 50);
    }

    #[test]
    fn untimed_junction_rows_do_not_merge_two_separate_real_segments() {
        // A schedule with real timed stops on BOTH sides of an untimed
        // junction run must still produce two separate connections (one per
        // real segment), not one long connection that skips the middle
        // timed stop entirely.
        let points = vec![
            cp("EUSTON", None, Some("08:00:00"), 0),
            cp("CMDNJN", None, None, 0),
            cp("WATFDJ", Some("08:20:00"), Some("08:21:00"), 0),
            cp("RUGBY", None, None, 0),
            cp("MAN", Some("10:50:00"), None, 0),
        ];
        let connections = build_connections([("W33128", points.as_slice())]);
        assert_eq!(connections.len(), 2);
        assert_eq!(connections[0].from_tiploc, "EUSTON");
        assert_eq!(connections[0].to_tiploc, "WATFDJ");
        assert_eq!(connections[0].departure_min, 8 * 60);
        assert_eq!(connections[0].arrival_min, 8 * 60 + 20);
        assert_eq!(connections[1].from_tiploc, "WATFDJ");
        assert_eq!(connections[1].to_tiploc, "MAN");
        assert_eq!(connections[1].departure_min, 8 * 60 + 21);
        assert_eq!(connections[1].arrival_min, 10 * 60 + 50);
    }
}
