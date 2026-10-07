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

use std::collections::HashMap;

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
    /// A passenger may board here ([`crate::records::CallingPoint::can_board`]).
    /// `false` at a set-down-only (`D`) stop: the train calls, and the
    /// connection leaving it exists, but nobody may get on there.
    pub can_board: bool,
    /// A passenger may alight here ([`crate::records::CallingPoint::can_alight`]).
    /// `false` at a pick-up-only (`U`) stop.
    pub can_alight: bool,
    /// The public (GBTT) arrival and departure
    /// ([`crate::records::CallingPoint::public_arrival`]/`public_departure`):
    /// what [`build_connections`] plans on. `None` where the call has no
    /// public time in that direction (or the row predates the column), and
    /// the working time is used instead.
    pub public_arrival: Option<NaiveTime>,
    pub public_departure: Option<NaiveTime>,
}

impl From<&crate::records::CallingPoint> for CallingPointForConnections {
    fn from(cp: &crate::records::CallingPoint) -> Self {
        Self {
            tiploc: cp.tiploc.to_string(),
            booked_arrival: cp.booked_arrival,
            booked_departure: cp.booked_departure,
            day_offset: cp.day_offset,
            can_board: cp.can_board(),
            can_alight: cp.can_alight(),
            public_arrival: cp.public_arrival,
            public_departure: cp.public_departure,
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
    ///
    /// These are the PUBLIC (GBTT) times the passenger is sold, which every
    /// search plans on, minimum change times included (design doc §10, P6);
    /// a call with no public time in that direction falls back to its
    /// working time. See [`build_connections`].
    pub departure_min: u32,
    pub arrival_min: u32,
    /// The same two times on the WORKING timetable (truncated to the
    /// minute): what `scheduledDeparture`/`scheduledArrival` still serve
    /// for one release, and what the live overlay matches TRUST and Darwin
    /// reports against.
    pub working_departure_min: u32,
    pub working_arrival_min: u32,
    /// A passenger may board this train at `from_tiploc`. Riding through a
    /// stop is always allowed; this only gates a fresh boarding there.
    pub can_board: bool,
    /// A passenger may get off this train at `to_tiploc`. When `false` the
    /// searches carry on along the train but never record an arrival there.
    pub can_alight: bool,
}

fn minutes_from_midnight(time: NaiveTime, day_offset: u8) -> u32 {
    use chrono::Timelike;
    time.num_seconds_from_midnight() / 60 + u32::from(day_offset) * 1440
}

/// The public minute for a call whose working minute is `working_min`
/// (`working` being that time of day): the public time read as the nearest
/// one to the working time across midnight (a 23:59H working arrival is a
/// 00:00 public one, the next day), or the working minute itself when there
/// is no public time.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "clamped to >= 0; within a minute or two of a u32 minute value"
)]
fn public_minute(working_min: u32, working: NaiveTime, public: Option<NaiveTime>) -> u32 {
    use chrono::Timelike;
    let Some(public) = public else {
        return working_min;
    };
    let mut gap = i64::from(public.num_seconds_from_midnight() / 60)
        - i64::from(working.num_seconds_from_midnight() / 60);
    if gap > 720 {
        gap -= 1440;
    } else if gap < -720 {
        gap += 1440;
    }
    (i64::from(working_min) + gap).max(0) as u32
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
/// **Public times.** A connection's `departure_min`/`arrival_min` are the
/// public times of its two calls, falling back to the working time per call
/// and direction where there is none (a set-down-only stop's departure, a
/// pick-up-only stop's arrival, a row published before public times were);
/// `working_departure_min`/`working_arrival_min` keep the working ones. The
/// public times are kept consistent along the train: a departure is never
/// before the train's public arrival at the same call, and an arrival never
/// before the departure it follows, so a same-train continuation is never
/// sorted ahead of the connection it continues.
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
    build(schedules, false).0
}

/// For each TIPLOC, the connections (indices into the array
/// [`build_connections_with_passes`] returns) whose train runs past it
/// WITHOUT calling -- the untimed rows [`build_connections`] walks over. What
/// `/Trips/plan`'s pass-through `avoid` needs: a connection alone only names
/// its two calls.
///
/// Each entry also records where among the connection's skipped rows the
/// TIPLOC lies (0 = the first row after the departure call), so the order in
/// which a train passes two places between the same pair of calls is known
/// (`/Trips/plan`'s ordered pass-through `via`).
#[derive(Debug, Clone, Default)]
pub struct PassIndex {
    by_tiploc: HashMap<String, Vec<PassRow>>,
}

/// One entry of a [`PassIndex`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassRow {
    /// Index into the connections array.
    pub connection: u32,
    /// Position among the connection's skipped rows, from 0.
    pub position: u16,
}

impl PassIndex {
    /// Indices of the connections running past `tiploc` (normalized here),
    /// ascending.
    pub fn connections_passing(&self, tiploc: &str) -> impl Iterator<Item = u32> + '_ {
        self.passes_at(tiploc).iter().map(|row| row.connection)
    }

    /// Every pass of `tiploc` (normalized here), by ascending connection.
    pub fn passes_at(&self, tiploc: &str) -> &[PassRow] {
        self.by_tiploc
            .get(crate::normalize_tiploc(tiploc))
            .map_or(&[], Vec::as_slice)
    }

    /// How many (TIPLOC, connection) entries the index holds.
    pub fn len(&self) -> usize {
        self.by_tiploc.values().map(Vec::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.by_tiploc.is_empty()
    }
}

/// [`build_connections`] plus the [`PassIndex`] over the same array. The
/// rows skipped between a connection's two calls are exactly the ones
/// [`build_connections`]'s doc describes (untimed pass points, or anything
/// else with no booked arrival).
pub fn build_connections_with_passes<'a>(
    schedules: impl IntoIterator<Item = (&'a str, &'a [CallingPointForConnections])>,
) -> (Vec<Connection>, PassIndex) {
    build(schedules, true)
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::expect_used,
    reason = "a day's connection count is far below u32::MAX; the invariant is established just above; the expect message names it"
)]
fn build<'a>(
    schedules: impl IntoIterator<Item = (&'a str, &'a [CallingPointForConnections])>,
    with_passes: bool,
) -> (Vec<Connection>, PassIndex) {
    let mut connections: Vec<(Connection, Vec<&'a str>)> = Vec::new();
    for (uid, calling_points) in schedules {
        // The public arrival minute at the call the previous connection
        // ended at (the one the next connection leaves from).
        let mut arrived_at: Option<(usize, u32)> = None;
        for (i, from) in calling_points.iter().enumerate() {
            let Some(departure) = from.booked_departure else {
                continue;
            };
            let Some(offset) = calling_points[i + 1..]
                .iter()
                .position(|cp| cp.booked_arrival.is_some())
            else {
                continue;
            };
            let to = &calling_points[i + 1 + offset];
            let arrival = to.booked_arrival.expect("just checked is_some");
            let passes = if with_passes {
                calling_points[i + 1..i + 1 + offset]
                    .iter()
                    .map(|cp| crate::normalize_tiploc(&cp.tiploc))
                    .collect()
            } else {
                Vec::new()
            };
            // The departure's own day: a stop dwelling across midnight
            // departs a day after it arrives (R-043).
            let working_departure_min = minutes_from_midnight(
                departure,
                crate::records::departure_day_offset(
                    from.booked_arrival,
                    Some(departure),
                    from.day_offset,
                ),
            );
            let working_arrival_min = minutes_from_midnight(arrival, to.day_offset);
            let mut departure_min =
                public_minute(working_departure_min, departure, from.public_departure);
            if let Some((_, arrived)) = arrived_at.filter(|(call, _)| *call == i) {
                departure_min = departure_min.max(arrived);
            }
            let arrival_min =
                public_minute(working_arrival_min, arrival, to.public_arrival).max(departure_min);
            arrived_at = Some((i + 1 + offset, arrival_min));
            connections.push((
                Connection {
                    uid: uid.to_string(),
                    from_tiploc: from.tiploc.clone(),
                    to_tiploc: to.tiploc.clone(),
                    departure_min,
                    arrival_min,
                    working_departure_min,
                    working_arrival_min,
                    can_board: from.can_board,
                    can_alight: to.can_alight,
                },
                passes,
            ));
        }
    }
    connections.sort_by(|(a, _), (b, _)| {
        (a.departure_min, &a.uid, &a.from_tiploc).cmp(&(b.departure_min, &b.uid, &b.from_tiploc))
    });
    let mut index = PassIndex::default();
    let connections = connections
        .into_iter()
        .enumerate()
        .map(|(position, (connection, passes))| {
            for (row, tiploc) in passes.into_iter().enumerate() {
                let entry = index.by_tiploc.entry(tiploc.to_string()).or_default();
                // A train passing the same place twice in one span: the first.
                if entry.last().map(|last| last.connection) != Some(position as u32) {
                    entry.push(PassRow {
                        connection: position as u32,
                        position: u16::try_from(row).unwrap_or(u16::MAX),
                    });
                }
            }
            connection
        })
        .collect();
    (connections, index)
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
            can_board: true,
            can_alight: true,
            public_arrival: None,
            public_departure: None,
        }
    }

    fn public(
        mut point: CallingPointForConnections,
        arrival: Option<&str>,
        departure: Option<&str>,
    ) -> CallingPointForConnections {
        point.public_arrival = arrival.map(|t| t.parse().unwrap());
        point.public_departure = departure.map(|t| t.parse().unwrap());
        point
    }

    /// Design doc §4: Avanti 9G44, WTT 20:31 from Watford Junction to
    /// Milton Keynes 20:50H, public 20:51. The planner arrives at 20:51;
    /// the working minutes are kept alongside.
    #[test]
    fn connections_plan_on_public_times_and_keep_the_working_ones() {
        let points = vec![
            public(
                cp("WATFDJ", None, Some("20:31:00"), 0),
                None,
                Some("20:31:00"),
            ),
            public(
                cp("MKNSCEN", Some("20:50:00"), Some("20:52:00"), 0),
                Some("20:51:00"),
                Some("20:52:00"),
            ),
            public(
                cp("WVRMPTN", Some("22:03:00"), None, 0),
                Some("22:04:00"),
                None,
            ),
        ];
        let connections = build_connections([("C01355", points.as_slice())]);
        let minutes: Vec<(u32, u32, u32, u32)> = connections
            .iter()
            .map(|c| {
                (
                    c.departure_min,
                    c.arrival_min,
                    c.working_departure_min,
                    c.working_arrival_min,
                )
            })
            .collect();
        assert_eq!(
            minutes,
            vec![
                (20 * 60 + 31, 20 * 60 + 51, 20 * 60 + 31, 20 * 60 + 50),
                (20 * 60 + 52, 22 * 60 + 4, 20 * 60 + 52, 22 * 60 + 3),
            ]
        );
    }

    /// A set-down-only stop has no public departure and a pick-up-only one
    /// no public arrival: each falls back to the working time on that side.
    #[test]
    fn a_missing_public_time_falls_back_to_the_working_time_per_direction() {
        let points = vec![
            public(
                cp("EUSTON", None, Some("15:40:00"), 0),
                None,
                Some("15:39:00"),
            ),
            public(
                cp("MOTHRWL", Some("17:00:00"), Some("17:02:00"), 0),
                Some("17:01:00"),
                None,
            ),
            cp("GLGC", Some("18:00:00"), None, 0),
        ];
        let connections = build_connections([("C01372", points.as_slice())]);
        assert_eq!(connections[0].departure_min, 15 * 60 + 39);
        assert_eq!(connections[0].arrival_min, 17 * 60 + 1);
        assert_eq!(connections[1].departure_min, 17 * 60 + 2);
        assert_eq!(connections[1].arrival_min, 18 * 60);
    }

    /// A 23:59H working arrival is a 00:00 public one, the next day; and a
    /// public departure is never before the public arrival at the same call.
    #[test]
    fn public_times_cross_midnight_and_stay_in_order_along_the_train() {
        let points = vec![
            cp("A", None, Some("23:40:00"), 0),
            public(
                cp("B", Some("23:59:00"), Some("23:59:00"), 0),
                Some("00:00:00"),
                Some("23:59:00"),
            ),
            cp("C", Some("00:10:00"), None, 1),
        ];
        let connections = build_connections([("U", points.as_slice())]);
        assert_eq!(connections[0].arrival_min, 1440);
        assert_eq!(connections[1].departure_min, 1440);
        assert_eq!(connections[1].working_departure_min, 23 * 60 + 59);
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

    /// A connection carries boarding from its `from` call and alighting from
    /// its `to` call, so a set-down-only stop still links the train through.
    #[test]
    fn each_connection_carries_its_ends_direction_flags() {
        let mut set_down_only = cp("MOTHRWL", Some("17:00:00"), Some("17:02:00"), 0);
        set_down_only.can_board = false;
        let mut pick_up_only = cp("WATFDJ", Some("16:00:00"), Some("16:01:00"), 0);
        pick_up_only.can_alight = false;
        let points = vec![
            cp("EUSTON", None, Some("15:40:00"), 0),
            pick_up_only,
            set_down_only,
            cp("GLGC", Some("18:00:00"), None, 0),
        ];
        let connections = build_connections([("C01372", points.as_slice())]);
        let flags: Vec<(&str, &str, bool, bool)> = connections
            .iter()
            .map(|c| {
                (
                    c.from_tiploc.as_str(),
                    c.to_tiploc.as_str(),
                    c.can_board,
                    c.can_alight,
                )
            })
            .collect();
        assert_eq!(
            flags,
            vec![
                ("EUSTON", "WATFDJ", true, false),
                ("WATFDJ", "MOTHRWL", true, true),
                ("MOTHRWL", "GLGC", false, true),
            ]
        );
    }

    /// R-043: C22645 arrives at Blackfriars 23:55 and departs 00:02 (both
    /// stored with the arrival's `day_offset` 0). The departure is the next
    /// day, so the connection on to Farringdon (00:05) is 3 minutes long,
    /// not -1437.
    #[test]
    fn a_midnight_dwell_departs_on_the_next_day() {
        let points = vec![
            cp("BLFR", Some("23:55:00"), Some("00:02:00"), 0),
            cp("FRNDNLT", Some("00:05:00"), Some("00:06:00"), 1),
        ];
        let connections = build_connections([("C22645", points.as_slice())]);
        assert_eq!(connections.len(), 1);
        assert_eq!(connections[0].departure_min, 1440 + 2);
        assert_eq!(connections[0].arrival_min, 1440 + 5);
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

    #[test]
    fn the_pass_index_names_the_connections_running_past_an_untimed_row() {
        let schedule = vec![
            cp("EUSTON", None, Some("08:00"), 0),
            cp("CMDNJN ", None, None, 0),
            cp("WATFDJ", None, None, 0),
            cp("MKC", Some("08:40"), Some("08:42"), 0),
            cp("RUGBY", Some("09:00"), None, 0),
        ];
        let (connections, passes) = build_connections_with_passes([("U1", schedule.as_slice())]);
        assert_eq!(
            connections,
            build_connections([("U1", schedule.as_slice())])
        );
        assert_eq!(passes.len(), 2);
        let passing: Vec<&Connection> = passes
            .connections_passing("CMDNJN")
            .map(|i| &connections[i as usize])
            .collect();
        assert_eq!(passing.len(), 1);
        assert_eq!(
            (
                passing[0].from_tiploc.as_str(),
                passing[0].to_tiploc.as_str()
            ),
            ("EUSTON", "MKC")
        );
        assert_eq!(
            passes.connections_passing("WATFDJ").collect::<Vec<_>>(),
            passes.connections_passing("CMDNJN").collect::<Vec<_>>()
        );
        // The order the train runs past them in.
        assert_eq!(passes.passes_at("CMDNJN")[0].position, 0);
        assert_eq!(passes.passes_at("WATFDJ")[0].position, 1);
        assert!(passes.passes_at("MKC").is_empty());
    }
}
