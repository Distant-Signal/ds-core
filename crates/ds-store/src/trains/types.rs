//! The journey stop domain types (`JourneyStop`, `StopStatus`,
//! `StopTimetable` and what they carry), moved from the api's `journey.rs`
//! (which re-exports them), with the stored calling-point shape they are
//! built from. `StopBoard` comes from the api's `stop_board.rs`.

use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::trains::london_to_utc;

/// Mirrors `schedule_matching::ScheduleCallingPointDto`'s exact camelCase
/// wire shape (the format `trains.calling_points` is stored in) -- a
/// separate, `Deserialize`-only type rather than importing that module's
/// private struct, matching this codebase's "each layer owns its own wire
/// shape" posture (the same relationship `frontend/lib/types.ts`'s
/// `ScheduleCallingPoint` already has to it, just on the Rust side).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawCallingPoint {
    pub tiploc: String,
    pub kind: schedule_query::CallingPointKind,
    pub booked_arrival: Option<chrono::NaiveTime>,
    pub booked_departure: Option<chrono::NaiveTime>,
    /// Mirrors `schedule_matching::ScheduleCallingPointDto::day_offset` /
    /// `schedule_query::CallingPoint::day_offset` -- how many calendar days
    /// past `service_date` this calling point's booked times actually fall
    /// on (a real overnight service crosses midnight mid-schedule; see that
    /// field's own doc comment). `#[serde(default)]` so a `trains.calling_points`
    /// row written before this field existed still deserializes, as `0`
    /// (the previous, buggy "always same day" behavior) rather than
    /// failing outright.
    #[serde(default)]
    pub day_offset: u8,
    /// Mirrors `schedule_matching::ScheduleCallingPointDto::platform` /
    /// `schedule_query::CallingPoint::platform` -- the CIF booked platform.
    /// `#[serde(default)]` for the same pre-existing-row reason as
    /// `day_offset` above: an older row reads as `None` ("not known").
    #[serde(default)]
    pub platform: Option<String>,
    /// Public times, exact working times and direction. Every field
    /// defaults, so a stored blob that predates them still deserializes.
    #[serde(flatten)]
    pub timetable: RawTimetable,
}

/// The public-time, working-time and direction fields of a
/// [`RawCallingPoint`] -- mirrors `schedule_matching::ScheduleCallingPointDto`
/// and the matching `schedule_calling_points_full` columns. See
/// docs/superpowers/specs/2026-10-01-working-vs-public-times-design.md.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RawTimetable {
    pub public_arrival: Option<chrono::NaiveTime>,
    pub public_departure: Option<chrono::NaiveTime>,
    /// Exact WTT times (`:30` for a half-minute). When absent (an older
    /// blob), the WTT time is rebuilt from `booked_*` plus the stored
    /// half-minute flags.
    pub working_arrival: Option<chrono::NaiveTime>,
    pub working_departure: Option<chrono::NaiveTime>,
    pub working_pass: Option<chrono::NaiveTime>,
    pub is_half_minute_arrival: bool,
    pub is_half_minute_departure: bool,
    /// `None` = not known (a row published before direction was); see
    /// [`direction_or_default`].
    pub can_board: Option<bool>,
    pub can_alight: Option<bool>,
    pub request_stop: Option<bool>,
}

/// `JourneyStop`'s public-time, working-time and direction fields,
/// flattened into it on the wire (`publicArrival`, `publicDeparture`,
/// `workingArrival`, `workingDeparture`, `workingPass`, `canBoard`,
/// `canAlight`, `requestStop`).
///
/// * `public*`: the public (GBTT) times -- what a passenger timetable and
///   the station screens show. `null` when there is no public call in that
///   direction (e.g. the departure of a set-down-only stop), and until the
///   next schedule publish for a schedule stored before these existed.
/// * `working*`: the exact working-timetable (WTT) times, with `:30`
///   seconds for a half-minute. `workingPass` is set only on a passing point
///   (the train runs through without stopping), which has no other time.
///   `scheduledArrival`/`scheduledDeparture` are the WTT time truncated to
///   the minute: kept for one release, then switched to public or removed.
/// * `canBoard`/`canAlight`/`requestStop`: from the CIF Activity field. A
///   set-down-only stop has `canBoard: false`, a pick-up-only stop
///   `canAlight: false`.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StopTimetable {
    pub public_arrival: Option<DateTime<Utc>>,
    pub public_departure: Option<DateTime<Utc>>,
    pub working_arrival: Option<DateTime<Utc>>,
    pub working_departure: Option<DateTime<Utc>>,
    pub working_pass: Option<DateTime<Utc>>,
    pub can_board: bool,
    pub can_alight: bool,
    pub request_stop: bool,
}

/// `booked` plus 30 seconds when `half_minute`.
fn with_half_minute(
    booked: Option<chrono::NaiveTime>,
    half_minute: bool,
) -> Option<chrono::NaiveTime> {
    booked.map(|time| {
        if half_minute {
            time + Duration::seconds(30)
        } else {
            time
        }
    })
}

/// The calendar date a public (or working) time falls on, given the WTT time
/// of the same call and that call's date. The two are minutes apart, so a
/// gap of more than 12 hours means they straddle midnight: a 23:59H WTT
/// arrival rounds up to a 00:00 public arrival on the next day.
fn date_near(
    reference: Option<chrono::NaiveTime>,
    date: NaiveDate,
    time: chrono::NaiveTime,
) -> NaiveDate {
    let Some(reference) = reference else {
        return date;
    };
    let gap = time.signed_duration_since(reference);
    if gap < -Duration::hours(12) {
        date + Duration::days(1)
    } else if gap > Duration::hours(12) {
        date - Duration::days(1)
    } else {
        date
    }
}

/// `(can_board, can_alight)` for a stop, defaulting what a row published
/// before direction existed does not say: the origin is boardable only, the
/// terminus alightable only, an untimed passing point neither, and any other
/// call both (the behaviour before direction was published).
fn direction_or_default(cp: &RawCallingPoint) -> (bool, bool) {
    let untimed = cp.booked_arrival.is_none() && cp.booked_departure.is_none();
    let (board, alight) = match cp.kind {
        _ if untimed => (false, false),
        schedule_query::CallingPointKind::Origin => (true, false),
        schedule_query::CallingPointKind::Terminate => (false, true),
        schedule_query::CallingPointKind::Intermediate => (true, true),
    };
    (
        cp.timetable.can_board.unwrap_or(board),
        cp.timetable.can_alight.unwrap_or(alight),
    )
}

impl StopTimetable {
    /// `arrival_date` is the stop's own date (its arrival's day);
    /// `departure_date` is its departure's, a day later for a stop that
    /// dwells across midnight (R-043). Each public/working time is dated
    /// against the WTT time on the same side.
    pub fn from_calling_point(
        cp: &RawCallingPoint,
        arrival_date: NaiveDate,
        departure_date: NaiveDate,
    ) -> Self {
        let t = &cp.timetable;
        let instant = |date: NaiveDate,
                       reference: Option<chrono::NaiveTime>,
                       time: Option<chrono::NaiveTime>| {
            time.and_then(|time| london_to_utc(date_near(reference, date, time).and_time(time)))
        };
        let working_arrival = t
            .working_arrival
            .or_else(|| with_half_minute(cp.booked_arrival, t.is_half_minute_arrival));
        let working_departure = t
            .working_departure
            .or_else(|| with_half_minute(cp.booked_departure, t.is_half_minute_departure));
        let (can_board, can_alight) = direction_or_default(cp);
        Self {
            public_arrival: instant(arrival_date, cp.booked_arrival, t.public_arrival),
            public_departure: instant(departure_date, cp.booked_departure, t.public_departure),
            working_arrival: instant(arrival_date, cp.booked_arrival, working_arrival),
            working_departure: instant(departure_date, cp.booked_departure, working_departure),
            working_pass: instant(arrival_date, None, t.working_pass),
            can_board,
            can_alight,
            request_stop: t.request_stop.unwrap_or(false),
        }
    }
}

/// The single lookup key both sides of the TIPLOC->CRS join must agree on:
/// trimmed (via [`schedule_query::normalize_tiploc`]) and uppercased.
///
/// Both halves are load-bearing, and getting either wrong fails silently:
///
/// - **Trimming.** `trains.calling_points`'s `tiploc` is the CIF schedule
///   body's **fixed 7-character, space-padded** field, stored verbatim --
///   `schedule_query`'s `parse_calling_point` does `line[2..9].to_string()`
///   with no trim, `schedule_matching::ScheduleCallingPointDto` clones it
///   unchanged, and `schedule_query::CallingPoint::tiploc`'s own doc
///   comment says so explicitly ("Stored here exactly as decoded, still
///   padded; see `normalize_tiploc` for trimming it at query time, not
///   parse time"). `stanox_crs.tiploc`, by contrast, holds the **trimmed**
///   value -- `schedule-reference`'s `parse_ti_lines` does
///   `line[2..9].trim().to_string()`. So a TIPLOC shorter than 7
///   characters (`"PUTNEY "`, `"WDON   "`) never equalled its own
///   `stanox_crs` row, the stop came back with `crs: None` and therefore
///   `name: None`, and the train detail page rendered it as "Unknown
///   location". That is not a rare edge case: in a real CORPUS extract
///   roughly a third of CRS-bearing station TIPLOCs are shorter than 7
///   characters, so this silently hit about one calling point in three,
///   nationwide. Confirmed against live production data for train `L82877`
///   on 2026-09-14 (SWR's Kingston loop): of that journey's 30 stops, all
///   11 whose TIPLOC is genuinely 7 characters resolved (`WATRLMN`,
///   `RAYNSPK`, `NEWMLDN`, `NRBITON`, `HAMWICK`, `TEDNGTN`, `STRWBYH`,
///   `TWCKNHM`, `RICHMND`, `WDWTOWN`, `QTRDBAT`) and all 8 padded ones did
///   not (`"ERLFLD "` Earlsfield, `"WDON   "` Wimbledon, `"KGSTON "`
///   Kingston, `"STMGTS "` St Margarets, `"NSHEEN "` North Sheen,
///   `"MRTLKE "` Mortlake, `"BARNES "` Barnes, `"PUTNEY "` Putney) --
///   every one of them a large, obviously-real station, which is exactly
///   why the symptom read as missing reference data rather than as a
///   key-format bug.
/// - **Uppercasing.** [`queries::crs_for_tiplocs_batch`] keys its returned
///   map on the stored, `normalize_code`d (trimmed, upper-cased) TIPLOC, so
///   the Rust-side `get` has to produce the identical string.
///
/// Every other TIPLOC comparison in this codebase already normalizes
/// (`schedule_matching`'s own `crs_for_tiploc` call site,
/// `schedule_query::resolve`'s call sites, `schedule-reference`'s two) --
/// this module was the sole outlier.
///
/// **What this does NOT fix, and deliberately so.** Two further classes of
/// unresolved calling point remain, both visible in the same live journey
/// and neither one a key-format problem:
///
/// 1. **CLOSED, 2026-09-24: a real station reached via a line-group TIPLOC
///    the crosswalk does not hold.** `stanox_crs`'s primary key is `stanox`
///    (`migrations/20260901150000_stanox_crs.sql`), so that table stores at
///    most ONE TIPLOC per STANOX -- whichever one `schedule-reference`'s
///    `resolve` happened to pick as the CRS-bearing candidate. A station
///    whose STANOX covers several TIPLOCs therefore resolved for one of
///    them and silently missed for the rest. On `L82877` that was Vauxhall
///    (`VAUXHLM`, "Vauxhall Main Lines", STANOX 87214, called at twice) and
///    Clapham Junction (`CLPHMJM` main lines and `CLPHMJW` Windsor lines,
///    STANOX 87219) -- all 7 characters, so trimming could not help them.
///    Closing this needed a TIPLOC-keyed crosswalk (an ingestion and
///    reference-data-schema change, not a query fix), and this codebase's
///    own investigation found it needed no STANOX-inheritance policy at
///    all -- a naive "any TIPLOC sharing a STANOX inherits that STANOX's
///    one resolved CRS" design would indeed have wrongly handed Waterloo's
///    CRS to the junction TIPLOCs in case 2 below, but Vauxhall's and
///    Clapham Junction's siblings don't need inheritance: each TIPLOC
///    already carries its own CRS directly (its own `TI` record, or that
///    same TIPLOC's own `MSN` `A` record when `TI`'s CRS is blank), so a
///    crosswalk keyed on TIPLOC rather than STANOX resolves every one of
///    them with no tiebreaker or inheritance step required. The fix: a new
///    `tiploc_crs` table (`PRIMARY KEY (tiploc)`,
///    `crates/ds-store/migrations/20260924130000_tiploc_crs.sql`), populated by
///    a new `crates/schedule-reference::parser::resolve_tiploc_crs`
///    function that keeps EVERY TIPLOC with a resolvable CRS as its own
///    row -- no STANOX-based grouping or exclusion -- alongside the
///    existing `stanox_crs` table/`resolve` function, which are completely
///    unchanged. `queries::crs_for_tiploc`, `crs_for_tiplocs_batch`, and
///    `list_stanox_crs_for_crs` now read the UNION of both tables
///    (preferring a `tiploc_crs` row when a TIPLOC is present in both), so
///    every existing `stanox_crs`-only fixture still resolves exactly as
///    before and BOTH of Vauxhall's real TIPLOCs (`VAUXHLM`/`VAUXHLW`) and
///    BOTH of Clapham Junction's real TIPLOCs (`CLPHMJM`/`CLPHMJW`) now
///    resolve on the same journey. See
///    docs/superpowers/plans/2026-09-24-tiploc-crs-crosswalk-plan.md for
///    the full design and investigation findings, and
///    `both_of_vauxhalls_real_tiplocs_resolve_when_the_crosswalk_holds_both`/
///    `both_of_clapham_junctions_real_tiplocs_resolve_when_the_crosswalk_holds_both`
///    below for the regression tests proving it at this module's level.
/// 2. **A genuine non-station timing point.** A CIF `LI` passing record
///    carries its time in the pass field (bytes `20..24`), which
///    `schedule_query::parse_calling_point` does not decode, so such a stop
///    arrives here with no booked arrival AND no booked departure, and its
///    TIPLOC has no CRS anywhere because the location is a junction, not a
///    station. On `L82877` these are `SHCKLGJ` (Shacklegate Junction),
///    `TWCKNMJ` (Twickenham Junction), `NINELMJ` (Nine Elms Junction),
///    `WLNDNJW` and `WATRLWC`. These are correctly unresolvable; how (or
///    whether) they should appear in a passenger-facing calling-point list
///    is a product question, not a data-quality one.
pub fn tiploc_key(raw_tiploc: &str) -> String {
    schedule_query::normalize_tiploc(raw_tiploc).to_uppercase()
}

/// Whether -- and to what degree of confidence -- a stop was actually
/// called at, independent of (but computed from) `actual_arrival`/
/// `actual_departure`/`last_event_type` on [`JourneyStop`]. Exists because
/// those fields alone can no longer safely distinguish, for a genuine
/// booked calling point, "not yet reached" from "the train ran through
/// without calling" -- both now leave `actual_arrival`/`actual_departure`
/// `None` (see `overlay_movement_events`'s own doc comment on its `"PASS"`
/// arm, the fix this type is the planned follow-up to). `apply_stop_status`
/// (below) is the only place this is computed.
///
/// `Unknown` covers every stop this whole distinction does not apply to at
/// all: an `Origin`/`Terminate` call (no two-sided booked stop to begin
/// with) or an `Intermediate` entry missing a scheduled arrival or
/// departure (a CIF timing point with a blank public time, never a real
/// calling point) -- the exact same `booked_calling_point` gate
/// `overlay_movement_events`'s `"PASS"` arm already uses, so the two can
/// never disagree about which stops this applies to. Whether such a stop
/// was "reached" is still answered by `actual_arrival`/`actual_departure`
/// alone, exactly as before this type existed.
///
/// Plain `PascalCase` variant names on the wire, no `rename_all` override --
/// matching `schedule_query::CallingPointKind`'s own convention, the field
/// this one sits right next to on every `JourneyStop`
/// (`frontend/lib/types.ts`'s `JourneyStopKind` mirrors it the same way).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum StopStatus {
    /// Not a genuine booked calling point -- see this type's own doc
    /// comment.
    Unknown,
    /// A booked calling point, not yet reached, with no signal saying it
    /// will be skipped -- the ordinary state of every future booked stop.
    Scheduled,
    /// A booked calling point the train genuinely called at (a real
    /// ARRIVAL and/or DEPARTURE was reported here).
    Called,
    /// A booked calling point the train did NOT call at today. See
    /// `JourneyStop::skip_source` for which signal(s) say so.
    Skipped,
}

/// Which signal(s) support a [`StopStatus::Skipped`] verdict -- carried as
/// a SIBLING field on [`JourneyStop`] (`skip_source`), never nested inside
/// `StopStatus` itself. That mirrors this app's existing "surface
/// provenance as its own field, never collapse it into the primary value"
/// convention -- `EtaBadge.tsx`'s `etaSource` badge, shown alongside (never
/// instead of) the ETA it qualifies, is the precedent this follows -- so a
/// `Skipped` stop's `stop_status` always serializes as the same flat
/// string regardless of source, and a consumer that only cares "was this
/// stop skipped" never has to pattern-match a nested value to find out.
///
/// The two sources are not equally trustworthy, and this type exists so
/// that difference is never silently flattened away:
///
/// - `Darwin` is Darwin/LDBWS's own explicit per-calling-point
///   `isCancelled` flag for this specific service
///   (`common::StationDeparture.skipped_stations`) -- the operator's own
///   timetable system stating outright that this call will not happen
///   today. Treated as authoritative.
/// - `Trust` is inferred purely from a reported TRUST `PASS` event at a
///   booked stop -- real running data, but an INFERENCE about what a
///   `PASS` message means for a public calling point, not a first-party
///   "this call was withdrawn" signal (see
///   docs/superpowers/specs/2026-09-04-option-b-live-consumer-design.md's
///   still-open PASS-mapping caveat). A consumer must word this more
///   softly than the `Darwin` case.
/// - `Both` is the two signals independently agreeing -- as confident as
///   `Darwin` alone, just worth surfacing that TRUST's own running data
///   corroborates it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum SkipSource {
    Darwin,
    Trust,
    Both,
}

/// `JourneyStop.platformStatus`: what the stop's `platform` means for a
/// passenger. Serialized lowercase (`"active"`, `"cancelled"`); `null` when
/// there is no platform.
///
/// A string enum rather than a `platformCancelled` boolean so that further
/// states (e.g. "provisional" before Darwin confirms an allocation) can be
/// added without another field; clients should render an unknown value as
/// `active`. "Changed" is deliberately not a state: `platformChanged` is
/// orthogonal (a cancelled call can also have changed platform) and is
/// already served.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PlatformStatus {
    /// The train is expected to call at this platform.
    Active,
    /// Darwin's board lists this train as cancelled at this stop: this is
    /// the platform it was allocated, but it is no longer calling there.
    /// Served (and shown struck through) rather than hidden, so a passenger
    /// waiting on that platform can see that it is their train that is gone.
    Cancelled,
}

/// One calling point of a train's journey, booked schedule merged with the
/// latest reported live data for that location -- see this module's own
/// doc comment and the design doc §2/§3.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JourneyStop {
    pub crs: Option<String>,
    /// The stop's display name: the station's name for a station, else the
    /// `tiploc_locations` (or CORPUS) name -- `Heathrow Terminal 3 (bus
    /// stop)`, `Marylebone 10 Signal`. `None` only for a TIPLOC no source
    /// knows.
    pub name: Option<String>,
    /// What kind of place this is (`station`, `bus_stop`, `ferry_terminal`,
    /// `junction`, `siding`, `passing_point`, `other`); `station` whenever
    /// `crs` is set, `None` when nothing is known.
    pub location_type: Option<common::location_naming::LocationType>,
    /// For a bus stop or ferry terminal, the station it belongs to (a
    /// `stations` CRS), to link to instead of a page of its own.
    pub parent_crs: Option<String>,
    pub tiploc: Option<String>,
    pub kind: Option<schedule_query::CallingPointKind>,
    /// The WORKING-timetable time, truncated to the minute. Deprecated on
    /// the wire (docs/api-changelog.md): kept for one release, then switched
    /// to the public time or removed. Read `publicArrival`/`publicDeparture`
    /// (or `workingArrival`/`workingDeparture`) instead.
    pub scheduled_arrival: Option<DateTime<Utc>>,
    pub scheduled_departure: Option<DateTime<Utc>>,
    pub actual_arrival: Option<DateTime<Utc>>,
    pub actual_departure: Option<DateTime<Utc>>,
    /// Scheduled time + the train's current overall delay, filled in by
    /// `apply_delay_estimates` (below) ONLY while `actual_arrival` is still
    /// `None` -- i.e. only for a stop live movement data hasn't reported an
    /// actual arrival for yet. Always `None` on a freshly-built stop, same
    /// as `actual_arrival`/`actual_departure` above, until that pass runs.
    pub estimated_arrival: Option<DateTime<Utc>>,
    /// See `estimated_arrival`'s own doc comment -- same contract, gated on
    /// `actual_departure` instead.
    pub estimated_departure: Option<DateTime<Utc>>,
    pub last_event_type: Option<String>,
    pub variation_status: Option<String>,
    /// How late the train was at this stop, in whole minutes (negative is
    /// early), measured against the PUBLIC timetable on the side the stop's
    /// latest TRUST report was for (an arrival against the public arrival,
    /// a departure against the public departure). `delay_basis` says which
    /// public time was used, or that none was known and the working
    /// timetable was used instead; see `common::public_delay` (design doc
    /// §9 decision 2, §11). `None` until the train reports here.
    ///
    /// See `apply_stop_status`'s own doc comment for the one exception:
    /// cleared to `None` for a [`StopStatus::Skipped`] stop even when a
    /// TRUST `PASS` event supplied a value here first.
    pub delay_minutes: Option<i32>,
    /// The baseline `delay_minutes` was measured against: `public` (TRUST's
    /// own public time), `publicSchedule` (the CIF public time, when TRUST
    /// sent none, e.g. a movement matched from the backlog) or `working`
    /// (no public time in that direction: a pass, or the departure of a
    /// set-down-only stop). `None` exactly when `delay_minutes` is.
    pub delay_basis: Option<common::public_delay::DelayBasis>,
    /// See [`StopStatus`]'s own doc comment. Computed by `apply_stop_status`,
    /// after the live movement overlay -- always `StopStatus::Unknown` on a
    /// freshly-built stop, same "None/default until the relevant pass runs"
    /// contract every other computed field on this struct already has.
    pub stop_status: StopStatus,
    /// `Some` only when `stop_status` is [`StopStatus::Skipped`] -- see
    /// [`SkipSource`]'s own doc comment for why this lives here rather than
    /// nested inside `stop_status` itself.
    pub skip_source: Option<SkipSource>,
    /// The CURRENT (live/expected) Darwin platform for this stop, from two
    /// sources: the origin's pin-time snapshot (`apply_origin_platform`)
    /// and, for ANY departing calling point whose station `poller-ldbws`
    /// samples, this train's row on that station's current departure board
    /// (`stop_board::apply_station_sample_board`, the same unique match as
    /// `board`, which wins when its row has a platform; a cancelled row's
    /// platform is served with `platform_status` `Cancelled`).
    /// Darwin/LDBWS's board only ever reports a station's OWN platform
    /// for a service departing FROM it
    /// (`poller-ldbws/src/schema.rs`'s `RdmCallingPoint` carries no
    /// platform field at all), so a terminating stop, a stop at an
    /// unsampled station, a stop whose board row is ambiguous or more than
    /// 10 minutes old, or a service that has already left a station's
    /// board is `None` -- exactly "not known", never a fabricated value and
    /// never another train's platform.
    pub platform: Option<String>,
    /// The EARLIEST Darwin platform observed for this stop, reconstructing
    /// "planned" the same way `common::StationDeparture.planned_platform`
    /// does -- see that field's own doc comment. NOT the CIF timetable's
    /// booked platform. `None` under the same conditions as `platform`
    /// above.
    pub planned_platform: Option<String>,
    /// `true` only when both `platform` and `planned_platform` are known
    /// AND differ -- see `api::render::station_departure_json`'s identical
    /// derivation for `StationDeparture`. Always `false` for a stop with no
    /// platform signal at all (there is nothing to have changed).
    pub platform_changed: bool,
    /// Whether the train still calls at `platform`: `Some` exactly when
    /// `platform` is, `None` otherwise. See [`PlatformStatus`].
    pub platform_status: Option<PlatformStatus>,
    /// The TIMETABLED platform from the CIF schedule itself
    /// (`schedule_query::CallingPoint::platform`: `LO`/`LT` `19..22`, `LI`
    /// `33..36`), independent of any Darwin data -- known for every calling
    /// point whose CIF record carries one, including stops at stations
    /// `poller-ldbws` never samples and the terminating stop. Deliberately a
    /// SEPARATE field from `planned_platform` (Darwin's earliest-observed
    /// reconstruction) and never folded into `platform_changed`: the two
    /// "planned" notions come from different systems, and a CIF platform
    /// can legitimately differ in form from Darwin's (e.g. a sub-platform
    /// suffix), so comparing them would risk a false "platform changed".
    /// `None` when the CIF field is blank, or the schedule row predates this
    /// field -- "not known", never guessed.
    pub booked_platform: Option<String>,
    /// LDBWS-style live status of this stop (`OnTime`, `Late`, `Cancelled`,
    /// `NoReport`, `Arrived`, `Departed`, `Scheduled`), served as `status`.
    /// See `data::stop_live_status` for the mapping. Computed last, by the
    /// train-detail routes once the train's own status is known, so it is
    /// `None` on a freshly built stop.
    #[serde(rename = "status")]
    pub live_status: Option<crate::trains::stop_live_status::LiveStopStatus>,
    /// How many minutes late this stop is expected to be; `Some` only when
    /// `live_status` is `Late`.
    pub late_minutes: Option<i32>,
    /// This train's row on the stop's current LDBWS departure board
    /// (Darwin's `delayReason`/`cancelReason`/`isCancelled`/`etd`), or
    /// `None` when there is no unique fresh match. Set by
    /// `stop_board::apply_station_sample_board`; see that module's doc for
    /// the matching rules and when it is null. Serialized as `null`, never
    /// omitted.
    pub board: Option<StopBoard>,
    /// Public and exact working times, and direction -- see
    /// [`StopTimetable`]. Flattened: its fields sit directly on the stop.
    #[serde(flatten)]
    pub timetable: StopTimetable,
}

impl JourneyStop {
    pub fn from_calling_point(
        cp: &RawCallingPoint,
        crs: Option<String>,
        service_date: NaiveDate,
    ) -> Self {
        // `day_offset` calendar days past `service_date` -- see
        // `RawCallingPoint::day_offset`'s own doc comment for why this
        // can't just be `service_date` unconditionally: a real overnight
        // service's post-midnight calling points are really the NEXT
        // calendar day.
        let calling_point_date = service_date + Duration::days(i64::from(cp.day_offset));
        // The departure's own day: a stop that dwells across midnight
        // (arrive 23:55, depart 00:02) departs the day after its stored
        // `day_offset`, which is its arrival's (R-043).
        let departure_date = service_date
            + Duration::days(i64::from(schedule_query::records::departure_day_offset(
                cp.booked_arrival,
                cp.booked_departure,
                cp.day_offset,
            )));
        Self {
            crs,
            name: None, // filled in by a batch station-name pass in `build_journey_stops`
            // Both filled in by `apply_locations`, after the name pass.
            location_type: None,
            parent_crs: None,
            // Normalized, not the raw padded field: `JourneyStop` is a wire
            // type (`frontend/lib/types.ts`'s `JourneyStop.tiploc`), and
            // emitting `"PUTNEY "` where every other TIPLOC-shaped value
            // this API serves is bare would just re-export the padding trap
            // `tiploc_key` exists to close. The raw, padded form stays
            // available verbatim on the separate `callingPoints` relay
            // (`render.rs`), which is documented as a pass-through of
            // exactly what was stored.
            tiploc: Some(tiploc_key(&cp.tiploc)),
            kind: Some(cp.kind),
            scheduled_arrival: cp
                .booked_arrival
                .and_then(|t| london_to_utc(calling_point_date.and_time(t))),
            scheduled_departure: cp
                .booked_departure
                .and_then(|t| london_to_utc(departure_date.and_time(t))),
            actual_arrival: None,
            actual_departure: None,
            estimated_arrival: None,
            estimated_departure: None,
            last_event_type: None,
            variation_status: None,
            delay_minutes: None,
            delay_basis: None,
            // Overwritten by `apply_stop_status`, later in
            // `build_journey_stops` -- see that field's own doc comment.
            stop_status: StopStatus::Unknown,
            skip_source: None,
            // Overwritten for the origin stop by `apply_origin_platform`,
            // later in `build_journey_stops` -- see that function's own
            // doc comment.
            platform: None,
            planned_platform: None,
            platform_changed: false,
            platform_status: None,
            booked_platform: cp.platform.clone(),
            live_status: None,
            late_minutes: None,
            board: None,
            timetable: StopTimetable::from_calling_point(cp, calling_point_date, departure_date),
        }
    }
}

/// `journeyStops[].board`. See the module doc.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StopBoard {
    /// Darwin's passenger delay text, verbatim.
    pub delay_reason: Option<String>,
    /// Darwin's passenger cancellation text, verbatim.
    pub cancel_reason: Option<String>,
    /// The whole service is cancelled at this station.
    pub is_cancelled: bool,
    /// `etd - std` in minutes (0 for early), when `etd` is a time or
    /// "On time"; `null` when it is a status word ("Delayed",
    /// "Cancelled"), so an unknown delay never reads as on time.
    pub delay_minutes: Option<i32>,
    /// LDBWS `etd`, verbatim: `"HH:MM"` (London local), `"On time"`,
    /// `"Delayed"` or `"Cancelled"`.
    pub estimated: String,
    /// When the board was polled (`station_samples.polled_at`).
    pub observed_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiploc_key_trims_the_fixed_seven_char_schedule_body_padding() {
        // The real shape `schedule_query::parse_calling_point` produces and
        // `ScheduleCallingPointDto` stores verbatim, for a TIPLOC shorter
        // than the fixed 7-character field.
        assert_eq!(tiploc_key("PUTNEY "), "PUTNEY");
        assert_eq!(tiploc_key("WDON   "), "WDON");
        assert_eq!(tiploc_key("BARNES "), "BARNES");
    }

    #[test]
    fn tiploc_key_is_unchanged_for_an_exactly_seven_char_tiploc() {
        // The case that masked the bug in casual testing: a TIPLOC that
        // happens to be exactly 7 characters needs no padding, so it
        // resolved correctly even before this fix.
        assert_eq!(tiploc_key("WATRLMN"), "WATRLMN");
        assert_eq!(tiploc_key("RAYNSPK"), "RAYNSPK");
    }

    #[test]
    fn tiploc_key_uppercases_so_it_matches_the_sql_sides_upper_trim() {
        assert_eq!(tiploc_key("putney "), "PUTNEY");
    }
}
