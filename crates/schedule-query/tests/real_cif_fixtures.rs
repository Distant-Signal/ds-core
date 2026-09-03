//! Integration tests against real, quoted CIF fixture data (Task 5 of
//! `docs/superpowers/plans/2026-09-03-option-b-consumer-first-slice-plan.md`).
//!
//! Per that plan's Non-goals, every fixture below is either a byte-verbatim
//! real CIF line already quoted in
//! `docs/superpowers/specs/2026-08-29-trust-schedule-delay-validation-findings.md`
//! or
//! `docs/superpowers/specs/2026-08-29-trust-schedule-delay-inference-timetable-verification.md`,
//! or is **reconstructed** from a real value the findings doc quotes only
//! in paraphrased/human-summary form (UID, STP, dates, TIPLOCs, times --
//! never invented), rebuilt at the same real-byte-verified field offsets
//! `src/records.rs`/`src/parse.rs`'s own unit tests already pin down
//! against raw byte quotes. Every reconstructed block says so in its own
//! comment, per this plan's "synthetic lines must be clearly commented as
//! such, not presented as real" rule. Fully synthetic edge-case lines
//! (a minimal two-point block, a malformed line) are labeled the same way.

use chrono::NaiveDate;
use schedule_query::{CallingPointKind, ScheduleIndex};

fn date(s: &str) -> NaiveDate {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
}

/// Real byte-verbatim `BS`/`LO` lines quoted directly in the verification
/// doc's "Claim 1" section (an `O`-indicator schedule with a full body) and
/// "Claim 2" section (the `LOWATRLMN`/`LICARLILE`/`LTEUSTON` real body
/// lines), combined into one real multi-line block to exercise
/// `ScheduleIndex::from_text` end to end.
const REAL_MIXED_BLOCK: &str = "\
BSNW684682605172610180000001 POO2E88    113560015 EMU    075D     S            O
BX         SRYSR408800
LOWATRLMN 0754 075315 MFL    TB
LICARLILE 1202 1213      120212131        T
LTEUSTON  0804 08079     TF";

#[test]
fn a_real_mixed_block_parses_end_to_end_through_the_index() {
    let index = ScheduleIndex::from_text(REAL_MIXED_BLOCK);
    // W68468's real days-of-week bitmask (positions 21..28 of the real
    // quoted BS line) is "0000001" -- Sunday only -- so the query date
    // must be a real Sunday within its real 260517..261018 range.
    let resolved = index
        .schedule_for_uid("W68468", date("2026-05-24"))
        .expect("W68468 covers 260517..261018 and runs Sundays only; 2026-05-24 is a Sunday");
    assert!(!resolved.cancelled);
    assert_eq!(resolved.calling_points.len(), 3);
    assert_eq!(resolved.calling_points[0].tiploc, "WATRLMN");
    assert_eq!(resolved.calling_points[1].tiploc, "CARLILE");
    assert_eq!(resolved.calling_points[2].tiploc, "EUSTON ");
}

// --- The real C11052 STP=P/STP=C Bank Holiday pair -------------------------
//
// UID/STP/date-range/days values transcribed from the findings doc's real
// 2026-08-31/09-01 Bank Holiday cross-check (paraphrased summary form, not
// raw bytes -- the doc itself only shows this pair as
// `UID=C11052 stp=P from=260518 to=261211 days=1111100` and
// `UID=C11052 stp=C from=260831 to=260831 days=1000000`), rebuilt at the
// real-byte-verified BS field offsets. `records.rs`/`resolve.rs`'s own
// tests already cover this UID's resolution logic directly; this test adds
// the "not present at all" -> `None` case Task 5 calls out separately.
const C11052_BASE_AND_OVERRIDE: &str = "\
BSNC110522605182612111111100           P
BSNC110522608312608311000000           C";

#[test]
fn schedule_for_uid_for_a_uid_date_not_covered_by_any_record_returns_none() {
    let index = ScheduleIndex::from_text(C11052_BASE_AND_OVERRIDE);
    // A real UID, but a date far outside every record's real date range.
    assert_eq!(index.schedule_for_uid("C11052", date("2027-06-01")), None);
    // A UID never present in the index at all.
    assert_eq!(index.schedule_for_uid("Z00000", date("2026-09-01")), None);
}

// --- The real F26094 STP=N Bank Holiday replacement body --------------------
//
// Reconstructed from the findings doc's real, quoted (paraphrased, not
// raw-byte) Task 5 output (2026-08-31/09-01 section):
//   "UID F26094 [STP=N, 260831 only]: LOEUSTON 1130 -> ... ->
//    LIHTCHEND 1135/1136H -> ... -> LIBUSHEY 1148/1149 -> ..."
// UID, STP, date, and every TIPLOC/time/half-minute value below is real,
// taken directly from that quote; the exact byte positions are the same
// ones independently verified against the raw BS/LO/LI/LT byte quotes in
// records.rs/parse.rs's own tests -- not verified against F26094's own raw
// bytes specifically, since the findings doc never quotes those in raw
// form. The block is deliberately left open after BUSHEY (no LT), matching
// the real quote's own trailing "-> ..." -- this crate has no real quoted
// terminus for this UID to reconstruct.
const F26094_BANK_HOLIDAY_BODY: &str = "\
BSNF260942608312608311000000           N
LOEUSTON  1130         TB
LIHTCHEND 1135 1136H        T
LIBUSHEY  1148 1149         T";

#[test]
fn f26094_real_bank_holiday_body_decodes_calling_points_and_the_half_minute_marker() {
    // Pin 13's real observed HRW -> BSH sequence (findings doc,
    // 2026-08-31/09-01 section, Task 4 table) lines up directly against
    // this UID's real CIF body -- cited here as corroborating narrative
    // only, not asserted programmatically, since this crate has no TRUST
    // dependency.
    let index = ScheduleIndex::from_text(F26094_BANK_HOLIDAY_BODY);
    let resolved = index
        .schedule_for_uid("F26094", date("2026-08-31"))
        .expect("F26094 is a real STP=N Bank Holiday replacement for 260831");
    assert!(!resolved.cancelled);
    assert_eq!(resolved.calling_points.len(), 3);

    let euston = &resolved.calling_points[0];
    assert_eq!(euston.tiploc, "EUSTON ");
    assert_eq!(euston.kind, CallingPointKind::Origin);
    assert_eq!(
        euston.booked_departure,
        chrono::NaiveTime::from_hms_opt(11, 30, 0)
    );
    assert!(!euston.is_half_minute_departure);

    let htchend = &resolved.calling_points[1];
    assert_eq!(htchend.tiploc, "HTCHEND");
    assert_eq!(
        htchend.booked_arrival,
        chrono::NaiveTime::from_hms_opt(11, 35, 0)
    );
    assert!(!htchend.is_half_minute_arrival);
    assert_eq!(
        htchend.booked_departure,
        chrono::NaiveTime::from_hms_opt(11, 36, 0)
    );
    assert!(
        htchend.is_half_minute_departure,
        "the real H suffix (1136H) must be captured, not dropped"
    );

    let bushey = &resolved.calling_points[2];
    assert_eq!(bushey.tiploc, "BUSHEY ");
    assert_eq!(
        bushey.booked_arrival,
        chrono::NaiveTime::from_hms_opt(11, 48, 0)
    );
    assert_eq!(
        bushey.booked_departure,
        chrono::NaiveTime::from_hms_opt(11, 49, 0)
    );
}

// --- The real C01370/C17755/C17798 WCML multi-station examples --------------
//
// Reconstructed from the findings doc's real, quoted (paraphrased) Task 5
// output (2026-08-29, "Task 5" section):
//   "UID C01370 STP=P [260523-261212]: EUS@0716 -> MKC@0750H -> CRE@1006H
//    -> CAR@1200H"
//   "UID C17755 STP=P [260523-261212]: EUS@1940 -> MKC@2022 -> CRE@2157"
//   "UID C17798 STP=P [260523-261212]: EUS@0756 -> MKC@0837"
// UID, STP, date range, and every short-code/time value is real, taken
// directly from that quote; the short station codes (EUS/MKC/CRE/CAR) are
// the doc's own write-up shorthand, mapped here to the real TIPLOCs the
// plan names for this same join (EUSTON/MKNSCEN/CREWE/CARLILE). The
// paraphrase prints one time per intermediate stop rather than a separate
// arrival/departure pair, so this reconstruction uses that same time
// (with its H flag, where shown) for both -- a labeled simplification,
// not a byte-exact quote. days-of-week is not given by the paraphrase at
// all; set here to run daily so the date-range coverage the doc does
// state is exercised without guessing a specific weekday pattern.
const WCML_MULTI_STATION_SCHEDULES: &str = "\
BSNC013702605232612121111111           P
LOEUSTON  0716         TB
LIMKNSCEN 0750H0750H        T
LICREWE   1006H1006H        T
LTCARLILE 1200H        TF
BSNC177552605232612121111111           P
LOEUSTON  1940         TB
LIMKNSCEN 2022 2022         T
LTCREWE   2157         TF
BSNC177982605232612121111111           P
LOEUSTON  0756         TB
LTMKNSCEN 0837         TF";

#[test]
fn schedules_touching_the_five_real_wcml_sample_tiplocs_finds_the_three_real_uids() {
    let index = ScheduleIndex::from_text(WCML_MULTI_STATION_SCHEDULES);
    // The plan's own real five WCML sample TIPLOCs (findings doc, "Task 5"
    // section): EUSTON, MKNSCEN, CREWE, PRSTON, CARLILE. PRSTON is not
    // touched by any of these three real UIDs' routes -- included anyway
    // to exercise "not every listed TIPLOC needs a hit".
    let tiplocs = ["EUSTON", "MKNSCEN", "CREWE", "PRSTON", "CARLILE"];
    let mut resolved = schedule_query::schedules_touching(&index, &tiplocs, date("2026-08-29"));
    resolved.sort_by(|a, b| a.uid.cmp(&b.uid));

    let uids: Vec<&str> = resolved.iter().map(|r| r.uid.as_str()).collect();
    assert_eq!(uids, vec!["C01370", "C17755", "C17798"]);
}

// --- Synthetic edge cases, clearly labeled -----------------------------

/// Fully synthetic (not derived from any real quote): the smallest
/// possible valid schedule block, `BS` + `BX` + `LO` + `LT`, two calling
/// points, to pin down parsing without real-data noise. Runs every day of
/// the week so a query against it exercises a real match, not just a
/// parse.
const SYNTHETIC_MINIMAL_BLOCK: &str = "\
BSNZ000012601012612311111111           P
BX         SRYSR000000
LOABC     1000         TB
LTXYZ     1010         TF";

#[test]
fn a_synthetic_minimal_two_point_block_parses_and_resolves_cleanly() {
    let index = ScheduleIndex::from_text(SYNTHETIC_MINIMAL_BLOCK);
    let resolved = index
        .schedule_for_uid("Z00001", date("2026-06-15"))
        .expect("Z00001 covers 260101..261231 and runs every day");
    assert!(!resolved.cancelled);
    assert_eq!(resolved.calling_points.len(), 2);
    assert_eq!(resolved.calling_points[0].tiploc, "ABC    ");
    assert_eq!(resolved.calling_points[1].tiploc, "XYZ    ");
}

/// Fully synthetic: identical to [`SYNTHETIC_MINIMAL_BLOCK`] but with an
/// all-zero days-of-week bitmask, to confirm a schedule with no running
/// day at all never resolves for any date.
const SYNTHETIC_BLOCK_NO_RUNNING_DAYS: &str = "\
BSNZ000022601012612310000000           P
LOABC     1000         TB
LTXYZ     1010         TF";

#[test]
fn a_synthetic_block_with_an_all_zero_days_bitmask_never_resolves() {
    let index = ScheduleIndex::from_text(SYNTHETIC_BLOCK_NO_RUNNING_DAYS);
    assert_eq!(
        index.schedule_for_uid("Z00002", date("2026-06-15")),
        None,
        "a real CIF days-of-week bitmask with no bit set should never match any date"
    );
}

/// Fully synthetic: a too-short, unrecognized-prefix line mixed into an
/// otherwise-real block, to confirm the "skip, don't abort" behavior at
/// the integration level (unit-level coverage already lives in
/// `src/parse.rs`'s own tests).
const BLOCK_WITH_A_MALFORMED_LINE: &str = "\
BSNC110522605182612111111100           P
LOEUSTON  0716         TB
XX
LTCARLILE 1200H        TF";

#[test]
fn a_malformed_line_mixed_into_a_real_block_is_skipped_not_a_hard_failure() {
    let index = ScheduleIndex::from_text(BLOCK_WITH_A_MALFORMED_LINE);
    let resolved = index
        .schedule_for_uid("C11052", date("2026-06-01"))
        .expect("the surrounding real block still parses despite the bad XX line");
    assert_eq!(resolved.calling_points.len(), 2);
}
