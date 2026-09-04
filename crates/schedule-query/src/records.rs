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
//! Platform, Line, Activity, and every `BX`-record field) are left
//! undecoded rather than guessed at -- see this plan's Non-goals.

use chrono::{NaiveDate, NaiveTime};
use serde::{Deserialize, Serialize};

/// A CIF `BS` (Basic Schedule) record's STP (Short Term Planning) overlay
/// indicator -- the final significant (non-space, after right-trimming)
/// character of the 80-byte `BS` line.
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
/// - the record's final significant character (after right-trimming
///   trailing spaces) is the STP indicator, not a fixed offset within this
///   struct's own decoded range -- see [`StpIndicator`]'s own doc comment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BasicSchedule {
    pub uid: String,
    pub stp_indicator: StpIndicator,
    pub date_from: NaiveDate,
    pub date_to: NaiveDate,
    /// Index 0 = Monday .. index 6 = Sunday.
    pub days_of_week: [bool; 7],
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
    pub tiploc: String,
    pub kind: CallingPointKind,
    pub booked_arrival: Option<NaiveTime>,
    pub booked_departure: Option<NaiveTime>,
    pub is_half_minute_arrival: bool,
    pub is_half_minute_departure: bool,
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
}

impl From<crate::resolve::ResolvedSchedule> for LinePopulationEntry {
    fn from(resolved: crate::resolve::ResolvedSchedule) -> Self {
        Self {
            uid: resolved.uid,
            calling_points: resolved.calling_points,
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
    pub destination_crs: Option<String>,
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
                tiploc: "EUSTON ".to_string(),
                kind: CallingPointKind::Origin,
                booked_arrival: None,
                booked_departure: chrono::NaiveTime::from_hms_opt(8, 22, 0),
                is_half_minute_arrival: false,
                is_half_minute_departure: false,
            }],
        };
        let entry: LinePopulationEntry = resolved.clone().into();
        assert_eq!(entry.uid, "C11052");
        assert_eq!(entry.calling_points, resolved.calling_points);
    }
}
