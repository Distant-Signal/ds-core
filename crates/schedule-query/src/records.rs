//! Record/struct shapes decoded from CIF `SCHEDULE` (`MCA`) records.
//!
//! Every byte offset documented below was independently re-verified in
//! this crate's own tests against real bytes already quoted in
//! `docs/superpowers/specs/2026-08-29-trust-schedule-delay-validation-findings.md`
//! and
//! `docs/superpowers/specs/2026-08-29-trust-schedule-delay-inference-timetable-verification.md`
//! -- not re-derived from memory of the published CIF User Spec (RSPS5046),
//! per this repo's "no invented API details" convention. Fields this crate
//! has no real-data-verified use for (Transaction Type, Train Status,
//! Platform, Line) are left undecoded rather than guessed at --
//! see this plan's Non-goals.
//!
//! **The Activity field is no longer on that list (2026-09-25).** Leaving it
//! undecoded was not a neutral scope decision: without it, every calling point
//! carrying a booked departure was published as a boardable departure, so
//! set-down-only, operational and not-advertised-to-the-public stops were
//! offered to users as places to board. Its byte range was verified per record
//! type against this crate's own real fixtures the same way every other offset
//! here was -- see [`CallingPoint::activity`] and
//! [`CallingPoint::is_public_pickup`]. The public arrival/departure times were
//! decoded alongside it, at offsets verified the same way.
//!
//! The `BX` record is no longer entirely
//! undecoded: its ATOC Code field is now decoded (see
//! [`BasicSchedule::operator_atoc`]), and so is its Retail Service ID
//! (see [`BasicSchedule::rsid`]); every other `BX` field remains
//! undecoded for the same no-real-fixture-need reason as above.

use chrono::{NaiveDate, NaiveTime};
use serde::{Deserialize, Serialize};

use crate::compact::SmallStr;

/// A CIF TIPLOC as stored on a [`CallingPoint`]: the fixed 7-byte field,
/// inline. See [`crate::compact::SmallStr`].
pub type Tiploc = SmallStr<7>;
/// A [`CallingPoint::activity`] field: CIF's 12-byte packed activity codes,
/// inline.
pub type Activity = SmallStr<12>;

/// A working-timetable (WTT) time to the half-minute, stored as
/// half-minutes since midnight so it costs 2 bytes (an `Option` of it, 4)
/// on a [`CallingPoint`] -- see [`crate::compact`] for why that struct's
/// size matters.
///
/// Serializes as an `"HH:MM:SS"` string, the same shape `chrono::NaiveTime`
/// uses, with `:30` seconds for a half-minute (`"20:50:30"` is CIF's
/// `2050H`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HalfMinuteTime(u16);

impl HalfMinuteTime {
    /// `time` (whole minutes; seconds are ignored) plus 30 seconds when
    /// `half_minute`.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "minutes since midnight are at most 1439"
    )]
    pub fn new(time: NaiveTime, half_minute: bool) -> Self {
        use chrono::Timelike;
        let minutes = (time.hour() * 60 + time.minute()) as u16;
        Self(minutes * 2 + u16::from(half_minute))
    }

    /// The exact WTT time, with `:30` seconds for a half-minute.
    pub fn time(self) -> NaiveTime {
        let half_minutes = u32::from(self.0);
        NaiveTime::from_num_seconds_from_midnight_opt(half_minutes * 30, 0)
            .unwrap_or(NaiveTime::MIN)
    }

    /// The time truncated to the whole minute -- how every other WTT time
    /// in this crate (`booked_arrival`/`booked_departure`) is stored.
    pub fn whole_minute(self) -> NaiveTime {
        use chrono::Timelike;
        self.time().with_second(0).unwrap_or(NaiveTime::MIN)
    }

    pub fn is_half_minute(self) -> bool {
        self.0 % 2 == 1
    }
}

impl Serialize for HalfMinuteTime {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.time().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for HalfMinuteTime {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use chrono::Timelike;
        let time = NaiveTime::deserialize(deserializer)?;
        Ok(Self::new(time, time.second() >= 30))
    }
}

/// `booked` plus 30 seconds when `half_minute` -- the exact WTT time.
fn working_time(booked: Option<NaiveTime>, half_minute: bool) -> Option<NaiveTime> {
    booked.map(|time| HalfMinuteTime::new(time, half_minute).time())
}
/// A [`CallingPoint::platform`] value: CIF's 3-byte platform field, inline.
pub type Platform = SmallStr<3>;

/// A CIF `BS` (Basic Schedule) record's STP (Short Term Planning) overlay
/// indicator -- CIF User Spec column 80 (1-based), the fixed last byte of
/// the 80-byte `BS` line (`parse::STP_INDICATOR_COL`, 0-based 79).
///
/// **Read from that fixed column, not "the line's last significant
/// character," as of 2026-09-25.** The two are equivalent on every
/// well-formed real line quoted below (each is exactly 80 bytes, and
/// nothing follows the STP indicator for right-trimming to strip), but they
/// diverge on a line truncated partway through its own free-text tail --
/// see [`crate::parse::parse_basic_schedule`]'s own doc comment for the
/// decode-correctness hazard that divergence caused and this fix closes.
///
/// Confirmed real and populated with all four values in the same real
/// `RJTTF942MCA.txt` extract (verification doc, "Claim 1"): `81162 C /
/// 122230 N / 149201 O / 136205 P` (of 488,798 total `BS` records).
///
/// Ordered so that "lowest STP letter wins" (the design spec's and
/// findings doc's independently-confirmed resolution rule -- `C` beats `N`
/// beats `O` beats `P`) is just `Ord`/`min_by_key` on this type directly,
/// via the variants' declaration order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum StpIndicator {
    /// `C` -- Cancellation. The schedule is withdrawn for the days this
    /// record covers; a `C`-indicator record carries no `LO`/`LI`/`LT`
    /// body at all (verification doc, "Claim 1": a real quoted `C` record
    /// is "immediately followed in the file by the next `BS` line, with
    /// **no** `BX`/`LO`/`LI`/`LT` body at all" -- confirmed arithmetically,
    /// `488,798 (BS) - 407,636 (LO/BX/LT) = 81,162`, exactly the `C` count).
    Cancellation,
    /// `N` -- New. Used for real Bank Holiday replacement schedules under
    /// their own UID (findings doc, 2026-08-31/09-01 section: `F26094`,
    /// `Q98537`, `Q97575`, `Q97539`).
    New,
    /// `O` -- Overlay. A variation of an existing base schedule for a
    /// sub-range of dates.
    Overlay,
    /// `P` -- Permanent. The base, unconditional schedule.
    Permanent,
}

impl TryFrom<char> for StpIndicator {
    type Error = ();

    fn try_from(value: char) -> Result<Self, Self::Error> {
        match value {
            'C' => Ok(Self::Cancellation),
            'N' => Ok(Self::New),
            'O' => Ok(Self::Overlay),
            'P' => Ok(Self::Permanent),
            _ => Err(()),
        }
    }
}

/// One `BS` (Basic Schedule) record.
///
/// Byte layout (0-based, half-open ranges, verified against the real
/// `BSNC005732605172612060000001 PXX1S003101121194800 DMU    125      S A T        P`
/// line -- findings doc, 2026-08-29 "Task 3" section -- decoding to UID
/// `C00573`, Date Runs From `260517`, Date Runs To `261206`, and
/// cross-checked against the real `BSNG007042605172608300000001 ... C`
/// Cancellation line and `BSNW684682605172610180000001 ... O` Overlay line,
/// both quoted verbatim in the verification doc's "Claim 1" section):
///
/// - `0..2` record identity `"BS"` (not stored)
/// - `2..3` Transaction Type (not decoded; no real fixture needed it)
/// - `3..9` Train UID (6 chars, e.g. `"C00573"`)
/// - `9..15` Date Runs From, `YYMMDD`
/// - `15..21` Date Runs To, `YYMMDD`
/// - `21..28` Days Run, a 7-char `'0'`/`'1'` bitmask. Index 0 = Monday
///   .. index 6 = Sunday -- confirmed directly from the findings doc's
///   real 2026-08-31/09-01 Bank Holiday cross-check: UID `C11052`'s real
///   `STP=C` override is dated `from=260831 to=260831 days=1000000`, and
///   2026-08-31 is independently confirmed a Monday in the same section
///   ("2026-08-31 is a Monday, and turned out to be the UK August Bank
///   Holiday") -- so bit index 0 set alone means "Monday only".
/// - `28` Bank Holiday Running (not decoded)
/// - `29` Train Status (CIF column 30) -- see [`BasicSchedule::train_status`]
/// - `30..32` Train Category (not decoded)
/// - `32..36` Train Identity (the 4-character signalling headcode, e.g.
///   `"1S00"`) -- see [`BasicSchedule::headcode`]
/// - `36..40` CIF's separately-named "Headcode" field (not decoded; NOT
///   the signalling headcode despite its name)
/// - `79` (the record's last byte, CIF column 80 1-based) the STP
///   indicator -- see [`StpIndicator`]'s own doc comment for why this is a
///   fixed offset rather than "the line's last significant character" as of
///   2026-09-25.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BasicSchedule {
    pub uid: String,
    pub stp_indicator: StpIndicator,
    pub date_from: NaiveDate,
    pub date_to: NaiveDate,
    /// Index 0 = Monday .. index 6 = Sunday.
    pub days_of_week: [bool; 7],
    /// The ATOC/TOC operator code, decoded from the `BX` (Basic Schedule
    /// Extra Details) line's `11..13` byte range (0-based, half-open) --
    /// verified against the real `BX         SRYSR408800` line quoted in
    /// `docs/superpowers/specs/2026-08-29-trust-schedule-delay-inference-timetable-verification.md`
    /// ("Claim 1" section), which decodes to `"SR"` (`ScotRail`). `None` when
    /// no `BX` line follows the `BS` (or the `BX` line is too short/
    /// non-ASCII to decode, or its ATOC Code field is blank) -- this is the
    /// only `BX` field this crate decodes; every other `BX` field remains
    /// undecoded, per this module's own header comment.
    pub operator_atoc: Option<String>,
    /// The `BS` record's Train Identity field -- the 4-character signalling
    /// headcode / train reporting number (e.g. `"1S00"`), decoded from the
    /// `32..36` byte range (0-based, half-open; CIF columns 33-36). Verified
    /// against the real, byte-verbatim `BS` lines quoted above:
    /// `...PXX1S003101...` (C00573) -> `"1S00"`, `...PXX1P033104...`
    /// (C00574) -> `"1P03"`, `...POO2E88    1...` (W68468) -> `"2E88"`;
    /// the field sits right after the 2-char Train Category (`XX`/`OO`)
    /// and right before CIF's separately-named 4-char "Headcode" field
    /// (`3101`/`3104`/blank, NOT decoded) and the 1-char Course Indicator
    /// (`1` on all three lines, including the real `G00704` Cancellation
    /// line, which pins the column alignment independently).
    ///
    /// `None` when the field is blank (real `C`-indicator lines leave it
    /// space-filled) or not ASCII alphanumeric. NOT the TRUST 10-character
    /// `train_id` (`trains.train_id`), which is a movement-feed identifier
    /// that merely embeds a headcode.
    #[serde(default)]
    pub headcode: Option<String>,
    /// The `BX` record's Retail Service ID -- the 8-character ID
    /// customer-facing systems show for this train (LDBWS/Darwin's `rsid`,
    /// e.g. `"SR408800"`: ATOC prefix, 4-digit service number, 2-digit
    /// portion suffix), decoded from the `BX` line's `14..22` byte range
    /// (0-based, half-open; CIF columns 15-22). Verified against the real
    /// `BX         SRYSR408800` line (see `operator_atoc` above).
    ///
    /// `None` when no `BX` line follows the `BS`, the line is too short to
    /// carry the whole field, or the field is blank / not ASCII
    /// alphanumeric. NOT unique per train: measured on the real
    /// `RJTTF971MCA` extract (2026-09-26), a handful of RSIDs are shared by
    /// several UIDs running on the same day (Heathrow Express uses one RSID
    /// per service group), but no two UIDs share an RSID at the same
    /// calling point and working time.
    #[serde(default)]
    pub rsid: Option<String>,
    /// The `BS` record's Train Status (byte 29, 0-based; CIF column 30):
    /// `P`/`1` passenger (permanent/STP), `B`/`5` bus, `S`/`4` ship,
    /// `F`/`2` freight, `T`/`3` trip. `None` when the byte is blank (a real
    /// `C`-indicator line leaves it space-filled) or not ASCII
    /// alphanumeric. TRUST never reports a bus or a ship, which is why the
    /// full-coverage consumer needs it (see
    /// `docs/superpowers/specs/2026-09-27-full-coverage-windowed-stats-design.md`
    /// section 4.1). `#[serde(default)]` so a record serialized before this
    /// field existed still deserializes.
    #[serde(default)]
    pub train_status: Option<char>,
}

/// Whether a CIF Train Status is a road vehicle or a ship (`B`/`5` bus,
/// `S`/`4` ship) -- never a train TRUST can report on.
pub fn is_bus_or_ship(train_status: Option<char>) -> bool {
    matches!(train_status, Some('B' | '5' | 'S' | '4'))
}

/// Which of `LO`/`LI`/`LT` a [`CallingPoint`] was decoded from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CallingPointKind {
    /// `LO` -- Origin. Departure only, no arrival.
    Origin,
    /// `LI` -- Intermediate. Both arrival and departure.
    Intermediate,
    /// `LT` -- Terminate. Arrival only, no departure.
    Terminate,
}

/// One calling point, decoded from an `LO`/`LI`/`LT` schedule-body line.
///
/// Byte layout (0-based, half-open ranges, verified against the real
/// `LOEUSTON  0822 08227  C      TB`, `LTEUSTON  0804 08079     TF`,
/// `LICARLILE 1202 1213      120212131        T`, and
/// `LOWATRLMN 0754 075315 MFL    TB` lines -- verification doc, "Claim 2"
/// section):
///
/// - `0..2` record identity (`"LO"`/`"LI"`/`"LT"`, not stored, determines
///   [`CallingPointKind`])
/// - `2..9` TIPLOC, **fixed 7-character, space-padded** (`"EUSTON "`,
///   `"CARLILE"` -- no padding needed since it's exactly 7). Stored here
///   exactly as decoded, still padded; see [`crate::tiploc::normalize_tiploc`]
///   for trimming it at query time, not parse time.
/// - `9..10` Location Suffix (not decoded)
/// - For `LO`/`LT` (one time only): `10..14` scheduled time `HHMM`,
///   `14..15` half-minute flag (`'H'` or space)
/// - For `LI` (both times): `10..14` scheduled arrival `HHMM`, `14..15`
///   arrival half-minute flag; `15..19` scheduled departure `HHMM`,
///   `19..20` departure half-minute flag
///
/// None of the four real quoted lines above happen to carry an `'H'`
/// half-minute flag -- that marker is independently confirmed real only in
/// the findings doc's paraphrased summary form (e.g. `MKC@0750H`,
/// `LIHTCHEND 1135/1136H`), not in a raw byte quote. This crate's own
/// tests (`tests/real_cif_fixtures.rs`) cover it with a clearly-labeled
/// synthetic-but-byte-layout-correct line built at these same
/// real-byte-verified offsets, per this plan's Non-goals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallingPoint {
    /// Inline, no heap allocation: see [`crate::compact::SmallStr`].
    pub tiploc: Tiploc,
    pub kind: CallingPointKind,
    pub booked_arrival: Option<NaiveTime>,
    pub booked_departure: Option<NaiveTime>,
    pub is_half_minute_arrival: bool,
    pub is_half_minute_departure: bool,
    /// How many calendar days past the SCHEDULE'S OWN `service_date`
    /// (never past this calling point's own local midnight -- there is no
    /// such concept here) this calling point's `booked_arrival`/
    /// `booked_departure` actually fall on. CIF times are bare `HH:MM` with
    /// no day marker of their own; a real overnight service (e.g. a real
    /// c2c Liverpool Street -> Shoeburyness working confirmed live on
    /// 2026-09-05, UID `F49687`: `23:48` at Liverpool Street, `23:54/23:55`
    /// at Stratford, then `00:06/00:07` at Barking and `01:01` at
    /// Shoeburyness) genuinely crosses midnight mid-schedule, so every
    /// calling point from Barking onward is really the NEXT calendar day.
    /// Set once, for every non-cancelled resolved schedule, by
    /// [`crate::resolve::assign_day_offsets`] -- NOT by this crate's parser
    /// (`crate::parse::parse_calling_point`), which only ever writes `0`
    /// here, since a single `BS`(+`BX`)/`LO`/`LI`*/`LT` block is decoded in
    /// isolation and has no reason to own this cross-calling-point
    /// bookkeeping itself; see that resolver's own doc comment for the
    /// algorithm. `#[serde(default)]` so a `schedule_line_population`/
    /// `trains.calling_points` JSONB blob published before this field
    /// existed still deserializes (as `0`, i.e. "assume same day", the
    /// previous -- buggy -- behavior, never a hard failure).
    #[serde(default)]
    pub day_offset: u8,
    /// The CIF Activity field, right-trimmed, exactly as decoded -- a packed
    /// run of up to six TWO-CHARACTER activity codes (`"TB"`, `"T"`, `"TF"`,
    /// `"D"`, `"U"`, `"R"`, `"N"`, `"OP"`, `"-T"`, ...). `LO` `29..41`, `LT`
    /// `25..37`, `LI` `42..54`; all three verified against this crate's own
    /// real byte-verbatim fixtures (`LOEUSTON  0822 08227  C      TB` ->
    /// `"TB"`, `LTEUSTON  0804 08079     TF` -> `"TF"`,
    /// `LICARLILE 1202 1213      120212131        T` -> `"T"`).
    ///
    /// **Left undecoded until 2026-09-25, and that was a real
    /// correctness bug, not just a missing feature.** Without it, every
    /// calling point carrying a booked departure was published as a boardable
    /// departure -- so a set-down-only stop (`D`, passengers may only get
    /// OFF), a pickup-only stop, an operational stop (`OP`), and a stop not
    /// advertised to the public at all (`N`) were all offered to users as
    /// places to board, on station boards and in the trip planner. See
    /// [`Self::is_public_pickup`].
    ///
    /// Stored as the raw field rather than a parsed enum set: this is a
    /// packed, open-ended code list (RSPS5046 defines more codes than this
    /// app has real-data evidence for), and the one question this codebase
    /// actually asks of it -- "can a passenger board here?" -- is answered by
    /// [`Self::is_public_pickup`] without needing to model every code.
    /// `#[serde(default)]` (empty string) so a `schedule_line_population` /
    /// `trains.calling_points` JSONB blob published before this field existed
    /// still deserializes, and deliberately reads as "unknown, assume
    /// boardable" -- see [`Self::is_public_pickup`] for why that direction is
    /// the safe one.
    ///
    /// Stored inline ([`Activity`]) rather than as a `String` -- see
    /// [`crate::compact`] for the production OOM that motivated it.
    #[serde(default)]
    pub activity: Activity,
    /// The CIF Public Arrival time (`LT`/`LO` `15..19`, `LI` `25..29`) --
    /// what a passenger timetable shows, as opposed to the working
    /// (`booked_arrival`) time. `None` when the field is blank or the CIF
    /// "no public time" sentinel `0000`, which is what a non-public stop
    /// carries.
    #[serde(default)]
    pub public_arrival: Option<NaiveTime>,
    /// The CIF Public Departure time (`LO` `15..19`, `LI` `29..33`). Same
    /// blank/`0000` handling as [`Self::public_arrival`].
    #[serde(default)]
    pub public_departure: Option<NaiveTime>,
    /// The CIF booked (timetabled) Platform field -- `LO`/`LT` `19..22`,
    /// `LI` `33..36` -- trimmed; `None` when blank (most calling points at
    /// single-platform or non-platformed locations carry no value). This is
    /// the TIMETABLE's platform as published in the CIF extract, not a live
    /// one: a later Darwin platform alteration is never reflected here.
    /// `#[serde(default)]` so a stored/serialized `CallingPoint` written
    /// before this field existed still deserializes, as `None` ("not known").
    ///
    /// Stored inline ([`Platform`]) rather than as a `String` -- see
    /// [`crate::compact`].
    #[serde(default)]
    pub platform: Option<Platform>,
    /// The CIF Scheduled Pass time (`LI` `20..25`, `HHMM` plus the `H`
    /// half-minute flag): set only on a passing point, an `LI` the train
    /// runs through without stopping, which carries no arrival or
    /// departure. `None` on every other calling point, and on a
    /// `schedule_line_population`/`trains.calling_points` blob written
    /// before this field existed (`#[serde(default)]`). Not serialized
    /// when `None`, so a stored blob for a non-passing point is unchanged.
    ///
    /// Shown only by the train page's optional "detailed" working-timetable
    /// view; passing points are never offered as stops.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub booked_pass: Option<HalfMinuteTime>,
}

/// Two-character CIF Activity codes that mean a passenger may BOARD at this
/// calling point.
///
/// * `T` -- stops to take up and set down passengers.
/// * `TB` -- train begins (the origin of a passenger service).
/// * `U` -- stops to take up passengers only.
/// * `R` -- request stop (the train calls on request; a passenger can board).
///
/// Deliberately NOT included, and each one a real reason this list exists:
/// `D` (stops to SET DOWN passengers only -- you may get off, never on),
/// `TF` (train finishes, a terminus), `OP` (operational stop), `N` (stop not
/// advertised to the public), and every engineering/shunting code (`-T`,
/// `-U`, `-D`, `W`, ...).
const PICKUP_ACTIVITY_CODES: [&str; 4] = ["T", "TB", "U", "R"];

/// The CIF Activity code for "stop not advertised to the public". Checked
/// separately from [`PICKUP_ACTIVITY_CODES`] because it can legitimately
/// appear ALONGSIDE a passenger code, and when it does it wins: an
/// unadvertised stop is not a place this app may tell a user to board.
const NOT_ADVERTISED_ACTIVITY_CODE: &str = "N";

/// Two-character CIF Activity codes that mean a passenger may ALIGHT at
/// this calling point -- the mirror of [`PICKUP_ACTIVITY_CODES`].
///
/// * `T` -- stops to take up and set down passengers.
/// * `TF` -- train finishes (the passenger service's destination).
/// * `D` -- stops to set down passengers only.
/// * `R` -- request stop.
///
/// Deliberately NOT included: `U` (take up only -- you may get on, never
/// off), `TB` (train begins), `OP`, `N` and the engineering codes.
const SET_DOWN_ACTIVITY_CODES: [&str; 4] = ["T", "TF", "D", "R"];

/// The CIF Activity code for a request stop.
const REQUEST_STOP_ACTIVITY_CODE: &str = "R";

/// The calendar-day offset of a stop's `booked_departure`, given the offset
/// [`crate::resolve::assign_day_offsets`] stored for the stop (its
/// ARRIVAL's day). A stop that dwells across midnight (arrival 23:55,
/// departure 00:02) departs one day later than it arrives; every other
/// stop departs on its stored day. A free function so a caller holding
/// only the three columns (a `schedule_calling_points_full` row) applies
/// the same rule (R-043).
pub fn departure_day_offset(
    booked_arrival: Option<NaiveTime>,
    booked_departure: Option<NaiveTime>,
    day_offset: u8,
) -> u8 {
    match (booked_arrival, booked_departure) {
        (Some(arrival), Some(departure)) if departure < arrival => day_offset.saturating_add(1),
        _ => day_offset,
    }
}

impl CallingPoint {
    /// The day offset of this stop's `booked_departure` -- see
    /// [`departure_day_offset`]. `day_offset` itself is the arrival's.
    pub fn departure_day_offset(&self) -> u8 {
        departure_day_offset(self.booked_arrival, self.booked_departure, self.day_offset)
    }

    /// The two-character Activity codes packed into [`Self::activity`], each
    /// trimmed, blanks dropped. `"TB          "` yields `["TB"]`;
    /// `"T           "` yields `["T"]` (single-character codes are
    /// left-justified in their own two-character slot).
    pub fn activity_codes(&self) -> impl Iterator<Item = &str> {
        self.activity
            .as_bytes()
            .chunks(2)
            .filter_map(|chunk| std::str::from_utf8(chunk).ok())
            .map(str::trim)
            .filter(|code| !code.is_empty())
    }

    /// Can a passenger genuinely BOARD a train at this calling point -- i.e.
    /// may this app publish it as a departure a user can catch?
    ///
    /// **This is the predicate that was missing.** Every consumer of
    /// calling-point data treated "has a `booked_departure`" as "is a
    /// boardable departure", which is not the same thing: a set-down-only
    /// (`D`) stop, an operational (`OP`) stop and a stop not advertised to the
    /// public (`N`) all carry a booked departure time and none of them is a
    /// place a passenger may board. `crate::resolve::departures_by_crs` and
    /// `departures_by_destination_crs` -- which produce the station-board and
    /// whole-network-search "you can board here" rows -- now filter on this.
    ///
    /// **An empty `activity` reads as boardable, on purpose.** That is the
    /// value for a line too short to carry the field, for a
    /// `schedule_line_population` blob published before this field existed,
    /// and for any future decode gap. Failing open keeps a real departure on
    /// the board when the evidence is simply absent; failing closed would
    /// silently empty station boards the first time an offset or a
    /// deployment ordering surprised us, which is precisely the
    /// "product silently stops updating" class this codebase keeps getting
    /// bitten by. Deliberately NOT gated on
    /// [`Self::public_departure`] being `Some` for the same reason: the
    /// Activity field is one well-specified field at one verified offset,
    /// whereas requiring a public time would turn any offset surprise in a
    /// layout variant into a wholesale board outage.
    pub fn is_public_pickup(&self) -> bool {
        if self.activity.trim().is_empty() {
            return true;
        }
        let mut boardable = false;
        for code in self.activity_codes() {
            if code == NOT_ADVERTISED_ACTIVITY_CODE {
                return false;
            }
            if PICKUP_ACTIVITY_CODES.contains(&code) {
                boardable = true;
            }
        }
        boardable
    }

    /// A passing point: the train runs through without calling (a Scheduled
    /// Pass time and no arrival or departure). Never a stop for a
    /// passenger.
    pub fn is_pass(&self) -> bool {
        self.booked_pass.is_some()
            && self.booked_arrival.is_none()
            && self.booked_departure.is_none()
    }

    /// Can a passenger BOARD here? [`Self::is_public_pickup`], except that a
    /// passing point and a terminating stop are never boardable. Published
    /// per stop as `can_board`, and what the trip planner checks before
    /// boarding a train here.
    pub fn can_board(&self) -> bool {
        self.kind != CallingPointKind::Terminate && !self.is_pass() && self.is_public_pickup()
    }

    /// Can a passenger ALIGHT here? The mirror of [`Self::can_board`]: `T`,
    /// `TF`, `D` or `R`; never `N` (not advertised), never a passing point
    /// and never the origin. A pick-up-only (`U`) stop is a stop where the
    /// train calls but nobody may get off -- SWR's down trains at Clapham
    /// Junction are the commonest real case.
    ///
    /// An empty `activity` reads as alightable, for the same fail-open
    /// reason [`Self::is_public_pickup`] gives.
    pub fn can_alight(&self) -> bool {
        if self.kind == CallingPointKind::Origin || self.is_pass() {
            return false;
        }
        if self.activity.trim().is_empty() {
            return true;
        }
        let mut alightable = false;
        for code in self.activity_codes() {
            if code == NOT_ADVERTISED_ACTIVITY_CODE {
                return false;
            }
            if SET_DOWN_ACTIVITY_CODES.contains(&code) {
                alightable = true;
            }
        }
        alightable
    }

    /// A request stop (`R`): the train calls only if a passenger asks the
    /// conductor, or signals the driver from the platform.
    pub fn is_request_stop(&self) -> bool {
        self.activity_codes()
            .any(|code| code == REQUEST_STOP_ACTIVITY_CODE)
    }

    /// The exact WTT arrival, with `:30` seconds for a half-minute.
    pub fn working_arrival(&self) -> Option<NaiveTime> {
        working_time(self.booked_arrival, self.is_half_minute_arrival)
    }

    /// The exact WTT departure, with `:30` seconds for a half-minute.
    pub fn working_departure(&self) -> Option<NaiveTime> {
        working_time(self.booked_departure, self.is_half_minute_departure)
    }

    /// The exact WTT pass time of a passing point.
    pub fn working_pass(&self) -> Option<NaiveTime> {
        self.booked_pass.map(HalfMinuteTime::time)
    }
}

/// One UID's resolved calling points, as published over the wire between
/// `crates/schedule-reference` (writer, via `POST
/// /private/schedule-line-population`) and `crates/full-coverage-consumer`
/// (reader, via `GET /private/schedule-line-population`) -- see
/// docs/superpowers/specs/2026-09-04-option-b-live-consumer-design.md
/// Decision 2a/2b. Deliberately NOT `ResolvedSchedule` itself (which
/// carries `stp_indicator`/`cancelled`, neither of which either producer
/// or consumer needs on the wire -- `schedules_touching` already filters
/// to non-cancelled results before this type is ever constructed).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinePopulationEntry {
    pub uid: String,
    pub calling_points: Vec<CallingPoint>,
    /// The winning schedule's ATOC operator code (see
    /// [`BasicSchedule::operator_atoc`]). Optional on the wire, and omitted
    /// when `None`, so an old reader ignores it and an old writer's body
    /// (without it) still deserializes -- the full-coverage consumer then
    /// falls back to `relevance = 'stops_only'` (windowed-stats design
    /// section 4.1, version skew).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator_atoc: Option<String>,
    /// The winning schedule's CIF Train Status (see
    /// [`BasicSchedule::train_status`]), serialized as a one-character
    /// string. Same compatibility posture as `operator_atoc`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub train_status: Option<char>,
}

impl From<crate::resolve::ResolvedSchedule> for LinePopulationEntry {
    fn from(resolved: crate::resolve::ResolvedSchedule) -> Self {
        Self {
            uid: resolved.uid,
            calling_points: resolved.calling_points,
            operator_atoc: resolved.operator_atoc,
            train_status: resolved.train_status,
        }
    }
}

/// One CIF-derived departure -- the whole-network trip-search fallback
/// picker's wire shape between `crates/schedule-reference` (writer, via
/// `POST /private/schedule-network-departures`) and `crates/api` (reader,
/// opaque-JSONB storage only -- `api` does NOT depend on this crate, see
/// docs/superpowers/plans/2026-09-04-whole-network-trip-search-plan.md's
/// own Corrections section). Deliberately narrower than
/// [`LinePopulationEntry`]: no `calling_points`, no full stopping pattern
/// -- see
/// docs/superpowers/specs/2026-09-04-whole-network-trip-search-design.md's
/// Explicitly out of scope section for why a full pattern per departure
/// per station was ruled out (row-size blowup for a feature this slice
/// doesn't need).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleDeparture {
    pub uid: String,
    pub scheduled: NaiveTime,
    /// The departing calling point's own [`CallingPoint::day_offset`] --
    /// how many calendar days past `service_date` `scheduled` actually
    /// falls on. Copied verbatim from the calling point this entry
    /// represents (see [`crate::resolve::departures_by_crs`]), never
    /// recomputed -- the exact same "copy, don't recompute" posture
    /// [`DestinationDeparture::day_offset`] already documents for its own
    /// sibling bucket. `#[serde(default)]` for the same deploy-in-flight/
    /// pre-existing-row reason [`CallingPoint::day_offset`] documents: a
    /// `schedule_network_departures` row published before this field
    /// existed still deserializes, as `0` ("assume same day", the previous
    /// -- buggy -- behavior).
    #[serde(default)]
    pub day_offset: u8,
    pub destination_crs: Option<String>,
    /// The CIF public departure for this call (see
    /// [`CallingPoint::public_departure`]) -- what a passenger timetable
    /// shows, where `scheduled` is the working (WTT) time. `None` when the
    /// CIF carries none, and on a row published before this field existed
    /// (`#[serde(default)]`); not serialized when `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_departure: Option<NaiveTime>,
}

/// One departure-bearing calling point of a schedule that TERMINATES at
/// some destination CRS, as bucketed by
/// [`crate::resolve::departures_by_destination_crs`]. The destination CRS
/// itself is deliberately absent from this struct: it is the bucket key
/// (identical for every entry in a bucket), exactly as the origin CRS is
/// the bucket key for [`ScheduleDeparture`]/`departures_by_crs`.
///
/// `origin_crs` means "the station this train departs FROM", which is the
/// calling point's own CRS -- an `Origin` calling point for the first
/// entry, an `Intermediate` one for every later entry of the same
/// schedule. It is NOT necessarily the schedule's own first station, and
/// is deliberately not the same concept as `trains.origin_crs` in
/// `crates/api`, which always is. This is the field the calling-point-first
/// train search (`GET /public/trains/search?station=`) filters its
/// PRIMARY, required key on.
///
/// `true_origin_crs` is the schedule's REAL first calling point's CRS --
/// the one this struct's own `origin_crs` field doc explicitly says
/// `origin_crs` is NOT. It is computed once per schedule (via
/// `resolved.calling_points.first()`, the exact mirror of how the bucket
/// key is computed via `.last()`) and is IDENTICAL across every entry that
/// schedule contributes, unlike `origin_crs` which varies per entry. `None`
/// when the schedule's first calling point's TIPLOC doesn't resolve via
/// `tiploc_to_crs` -- a plain filter-field degrade, not a dropped row (see
/// [`crate::resolve::departures_by_destination_crs`]'s own doc comment for
/// why this differs from how an unresolved bucket-key destination is
/// treated). Backs the OPTIONAL "originating at" filter on
/// `GET /public/trains/search?origin=`, which is deliberately independent
/// of `station=`/`origin_crs` above -- see
/// docs/superpowers/specs/2026-09-08-calling-point-train-search-design.md
/// §0.2 for why these needed to become two different columns.
///
/// `scheduled` is Europe/London LOCAL civil time, straight off the CIF
/// body, same as [`ScheduleDeparture::scheduled`] -- never UTC. See
/// `crates/schedule-reference/src/main.rs`'s `london_local_time_at` for
/// the one place that distinction is handled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationDeparture {
    pub uid: String,
    pub origin_crs: String,
    pub scheduled: NaiveTime,
    /// The departing calling point's own [`CallingPoint::day_offset`] --
    /// i.e. how many calendar days past `service_date` `scheduled` actually
    /// falls on. Copied verbatim from the calling point this entry
    /// represents (see [`crate::resolve::departures_by_destination_crs`]),
    /// never recomputed. `#[serde(default)]` for the same
    /// deploy-in-flight/pre-existing-row reason [`CallingPoint::day_offset`]
    /// documents.
    #[serde(default)]
    pub day_offset: u8,
    pub true_origin_crs: Option<String>,
    /// This entry's OWN calling point's `booked_arrival` -- NOT the
    /// schedule's true destination's arrival (see `destination_arrival`
    /// below for that). Read per-calling-point, unlike `true_origin_crs`/
    /// `destination_arrival`/`destination_arrival_day_offset`, which are
    /// all computed once per schedule and copied verbatim onto every entry
    /// -- this one genuinely varies entry to entry, exactly like
    /// `scheduled`/`origin_crs`/`day_offset` above. `None` for the
    /// schedule's own true origin (an `Origin` calling point has no
    /// `booked_arrival` at all -- see `CallingPointKind::Origin`), `Some`
    /// for a genuine `Intermediate` calling point. Backs
    /// `GET /public/trains/search`'s `arrival_from`/`arrival_to` filter,
    /// which only applies when `stops_at` names exactly one station --
    /// see docs/superpowers/specs/2026-09-09-stops-at-search-filter-design.md.
    pub calling_point_arrival: Option<NaiveTime>,
    /// `destination_arrival` is the schedule's REAL final calling point's
    /// (the `Terminate` one) `booked_arrival` -- the mirror of
    /// `true_origin_crs` above, but reading the LAST calling point's
    /// arrival instead of the FIRST's departure, because
    /// `CallingPointKind::Terminate` is "arrival only, no departure"
    /// (this crate's own `records.rs`). Computed once per schedule and
    /// IDENTICAL across every entry that schedule contributes, exactly
    /// like `true_origin_crs`. `None` when the terminating calling
    /// point's own `booked_arrival` is absent from a real published
    /// schedule -- a plain filter-field degrade, not a dropped row.
    /// Rendered on every `GET /public/trains/search` row as
    /// `destinationArrival`/`destinationArrivalDayOffset` -- informational
    /// only; it is NOT used as a filter any more (superseded by
    /// `calling_point_arrival` above, since `stops_at`'s single-entry
    /// arrival filter is scoped to whichever calling point was searched
    /// for, not necessarily the schedule's true destination).
    pub destination_arrival: Option<NaiveTime>,
    /// How many calendar days past `service_date` `destination_arrival`
    /// actually falls on -- the TERMINATING calling point's own
    /// [`CallingPoint::day_offset`], read via `resolved.calling_points.last()`
    /// exactly like `destination_arrival` itself, NOT copied from this row's
    /// own [`day_offset`](Self::day_offset) above (that field describes the
    /// DEPARTING calling point this row represents, a different calling
    /// point on a genuine overnight schedule -- see
    /// [`crate::resolve::assign_day_offsets`]'s own doc comment for the
    /// live-confirmed c2c UID `F49687` example both fields ultimately trace
    /// back to). Computed once per schedule and IDENTICAL across every
    /// entry that schedule contributes, exactly like `destination_arrival`.
    /// NOT guaranteed to be `0` when `destination_arrival` itself is `None`:
    /// this is still the terminating calling point's own `day_offset` from
    /// `assign_day_offsets`, which reflects whichever day that stop is
    /// genuinely on regardless of whether it has a `booked_arrival`/
    /// `booked_departure` recorded at all -- a terminus with no arrival time
    /// that sits after an earlier midnight crossing in the same schedule
    /// still carries that crossing's nonzero offset. Added after
    /// `destination_arrival` itself shipped with no day-offset of its own --
    /// the same architectural gap `day_offset` above was added to close on
    /// the DEPARTURE side.
    pub destination_arrival_day_offset: u8,
    /// The schedule's `BX` ATOC Code (see [`BasicSchedule::operator_atoc`]),
    /// copied verbatim from [`crate::resolve::ResolvedSchedule::operator_atoc`].
    /// It is computed once per schedule and attached unchanged to every
    /// entry that schedule contributes -- exactly like `true_origin_crs`
    /// above, not recomputed per calling point. `None` when the schedule's
    /// `BX` record is absent or its ATOC Code field was blank (see
    /// `BasicSchedule::operator_atoc`'s own doc comment).
    pub operator_atoc: Option<String>,
    /// The schedule's `BS` Train Identity (see [`BasicSchedule::headcode`]),
    /// copied verbatim from [`crate::resolve::ResolvedSchedule::headcode`]
    /// and attached unchanged to every entry the schedule contributes,
    /// exactly like `operator_atoc` above.
    #[serde(default)]
    pub headcode: Option<String>,
    /// The schedule's `BX` Retail Service ID (see [`BasicSchedule::rsid`]),
    /// copied verbatim from [`crate::resolve::ResolvedSchedule::rsid`] --
    /// the STP-resolved winner's own value, so an overlay that changes the
    /// RSID wins -- and attached unchanged to every entry the schedule
    /// contributes, exactly like `operator_atoc`/`headcode` above.
    #[serde(default)]
    pub rsid: Option<String>,
    /// The public departure for this entry's own call (`scheduled` is the
    /// WTT one). See [`CallingPoint::public_departure`].
    #[serde(default)]
    pub public_departure: Option<NaiveTime>,
    /// The public arrival for this entry's own call -- the public
    /// counterpart of `calling_point_arrival`.
    #[serde(default)]
    pub public_calling_point_arrival: Option<NaiveTime>,
    /// The public arrival at the schedule's terminus -- the public
    /// counterpart of `destination_arrival`, and on the same day
    /// (`destination_arrival_day_offset`). This is where the terminus
    /// recovery margin shows: 22% of terminating arrivals are 0.5-4 minutes
    /// later than the WTT one.
    #[serde(default)]
    pub public_destination_arrival: Option<NaiveTime>,
}

/// One `BS`(+`BX`)/`LO`/`LI`*/`LT` block, pre-STP-resolution.
///
/// A [`StpIndicator::Cancellation`] `RawSchedule` has an empty
/// `calling_points` -- see [`StpIndicator::Cancellation`]'s own doc
/// comment for the real evidence this reflects a genuine CIF property, not
/// an assumption.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawSchedule {
    pub basic: BasicSchedule,
    pub calling_points: Vec<CallingPoint>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolve::ResolvedSchedule;

    #[test]
    fn line_population_entry_from_resolved_schedule_drops_stp_fields() {
        let resolved = ResolvedSchedule {
            uid: "C11052".to_string(),
            stp_indicator: StpIndicator::Permanent,
            cancelled: false,
            calling_points: vec![CallingPoint {
                tiploc: "EUSTON ".into(),
                kind: CallingPointKind::Origin,
                booked_arrival: None,
                booked_departure: NaiveTime::from_hms_opt(8, 22, 0),
                is_half_minute_arrival: false,
                is_half_minute_departure: false,
                day_offset: 0,
                activity: SmallStr::default(),
                public_arrival: None,
                public_departure: None,
                platform: None,
                booked_pass: None,
            }],
            operator_atoc: Some("LM".to_string()),
            headcode: None,
            rsid: None,
            train_status: Some('P'),
        };
        let entry: LinePopulationEntry = resolved.clone().into();
        assert_eq!(entry.uid, "C11052");
        assert_eq!(entry.calling_points, resolved.calling_points);
        assert_eq!(entry.operator_atoc.as_deref(), Some("LM"));
        assert_eq!(entry.train_status, Some('P'));
    }

    /// The population wire in both skew directions: a new entry carries
    /// the two optional fields; an old body without them still
    /// deserializes (as `None`), and a `None` is not serialized at all, so
    /// the body an old writer would have produced is unchanged.
    #[test]
    fn line_population_entry_round_trips_with_and_without_schedule_facts() {
        let new = LinePopulationEntry {
            uid: "C11052".to_string(),
            calling_points: vec![],
            operator_atoc: Some("LM".to_string()),
            train_status: Some('5'),
        };
        let json = serde_json::to_string(&new).unwrap();
        assert!(json.contains(r#""operator_atoc":"LM""#), "{json}");
        assert!(json.contains(r#""train_status":"5""#), "{json}");
        assert_eq!(
            serde_json::from_str::<LinePopulationEntry>(&json).unwrap(),
            new
        );

        let old_body = r#"{"uid": "C11052", "calling_points": []}"#;
        let old: LinePopulationEntry = serde_json::from_str(old_body).unwrap();
        assert_eq!(old.operator_atoc, None);
        assert_eq!(old.train_status, None);
        assert_eq!(
            serde_json::to_string(&old).unwrap(),
            r#"{"uid":"C11052","calling_points":[]}"#
        );
    }

    /// The half-minute time round-trips through its `"HH:MM:SS"` JSON form,
    /// and a blob without `booked_pass` still deserializes.
    #[test]
    fn half_minute_time_serializes_as_a_time_string() {
        let time = HalfMinuteTime::new(NaiveTime::from_hms_opt(20, 50, 0).unwrap(), true);
        let json = serde_json::to_string(&time).unwrap();
        assert_eq!(json, r#""20:50:30""#);
        assert_eq!(serde_json::from_str::<HalfMinuteTime>(&json).unwrap(), time);
        let whole: HalfMinuteTime = serde_json::from_str(r#""23:59:00""#).unwrap();
        assert!(!whole.is_half_minute());
        assert_eq!(whole.time(), NaiveTime::from_hms_opt(23, 59, 0).unwrap());
    }

    /// Adding the pass time must not grow the struct the schedule index holds
    /// ~7.9M of (see `crate::compact`).
    #[test]
    fn calling_point_stays_104_bytes() {
        assert_eq!(size_of::<CallingPoint>(), 104);
    }

    #[test]
    fn bus_and_ship_statuses_are_recognised() {
        for status in ['B', '5', 'S', '4'] {
            assert!(is_bus_or_ship(Some(status)), "{status}");
        }
        for status in [Some('P'), Some('1'), Some('F'), None] {
            assert!(!is_bus_or_ship(status), "{status:?}");
        }
    }
}
