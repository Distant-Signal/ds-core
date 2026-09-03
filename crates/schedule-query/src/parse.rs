//! Streams raw CIF `SCHEDULE` text into [`RawSchedule`] blocks.
//!
//! No I/O here -- the caller already read the file (or a fixture) into a
//! `&str`. A single malformed/too-short line is skipped, never a hard
//! parse failure for the whole extraction -- mirroring
//! `crates/schedule-reference/src/parser.rs::parse_ti_lines`'s own
//! documented "a single malformed line must not abort the whole
//! extraction" posture. That sibling module is also fully log-free (no
//! `tracing` call, no skip-count return -- just a silently-shorter `Vec`)
//! despite its crate depending on `tracing` for its own `main.rs`; this
//! crate has no `main.rs` and matches that same log-free posture rather
//! than inventing a fresh one, per this plan's Task 2 "decide during
//! implementation, matching whichever posture `schedule-reference` already
//! established" guidance.

use chrono::{NaiveDate, NaiveTime};

use crate::records::{BasicSchedule, CallingPoint, CallingPointKind, RawSchedule, StpIndicator};

/// Minimum length of a `BS` line this parser can decode: needs bytes
/// `0..28` (record identity through the days-of-week bitmask).
const MIN_BS_LEN: usize = 28;
/// Minimum length of an `LO`/`LT` line: needs bytes `0..15` (record
/// identity, TIPLOC, suffix, one time field, its half-minute flag).
const MIN_LO_LT_LEN: usize = 15;
/// Minimum length of an `LI` line: needs bytes `0..20` (as above, plus a
/// second time field and its half-minute flag).
const MIN_LI_LEN: usize = 20;

/// Parses every `BS`(+`BX`)/`LO`/`LI`*/`LT` block out of `text`, matching
/// the real CIF block structure a full `MCA` extract has: `BS` starts a
/// block; an optional `BX` line extends it (recognized so it doesn't get
/// mistaken for an unrelated/malformed line, but not decoded -- no real
/// fixture in this plan's scope needed a `BX` field); `LO`/`LI`*/`LT` are
/// its body; the block is implicitly terminated by the next `BS` (or by
/// end of file, or by `LT` itself for a well-formed block). A
/// `Cancellation`-indicator `BS` line has no body at all (see
/// [`StpIndicator::Cancellation`]'s doc comment) and is pushed as a
/// complete, empty-`calling_points` block immediately.
///
/// Any other record type (`TI`, `CR`, `AA`, `HD`, `ZZ`, ...) is ignored --
/// this crate only decodes the schedule-body record family, per this
/// plan's Non-goals. A `CR` (Change en Route) line, which can appear
/// mid-block, is likewise ignored without disturbing the open block, since
/// this plan decodes no field from it.
pub fn parse_schedule_records(text: &str) -> Vec<RawSchedule> {
    let mut out = Vec::new();
    let mut current: Option<RawSchedule> = None;

    for line in text.lines() {
        if line.len() < 2 {
            continue;
        }
        match &line[0..2] {
            "BS" => {
                if let Some(prev) = current.take() {
                    out.push(prev);
                }
                if let Some(basic) = parse_basic_schedule(line) {
                    let cancelled = basic.stp_indicator == StpIndicator::Cancellation;
                    let schedule = RawSchedule {
                        basic,
                        calling_points: Vec::new(),
                    };
                    if cancelled {
                        out.push(schedule);
                    } else {
                        current = Some(schedule);
                    }
                }
                // A malformed BS line is skipped; `current` stays `None`
                // until the next real BS line starts a new block.
            }
            "LO" => {
                if let Some(cp) = parse_calling_point(line, CallingPointKind::Origin)
                    && let Some(schedule) = current.as_mut()
                {
                    schedule.calling_points.push(cp);
                }
            }
            "LI" => {
                if let Some(cp) = parse_calling_point(line, CallingPointKind::Intermediate)
                    && let Some(schedule) = current.as_mut()
                {
                    schedule.calling_points.push(cp);
                }
            }
            "LT" => {
                if let Some(cp) = parse_calling_point(line, CallingPointKind::Terminate)
                    && let Some(schedule) = current.as_mut()
                {
                    schedule.calling_points.push(cp);
                }
                if let Some(done) = current.take() {
                    out.push(done);
                }
            }
            _ => {}
        }
    }

    if let Some(leftover) = current.take() {
        out.push(leftover);
    }

    out
}

fn parse_basic_schedule(line: &str) -> Option<BasicSchedule> {
    if line.len() < MIN_BS_LEN {
        return None;
    }
    let uid = line[3..9].trim().to_string();
    if uid.is_empty() {
        return None;
    }
    let date_from = NaiveDate::parse_from_str(&line[9..15], "%y%m%d").ok()?;
    let date_to = NaiveDate::parse_from_str(&line[15..21], "%y%m%d").ok()?;

    let mut days_of_week = [false; 7];
    for (i, c) in line[21..28].chars().enumerate() {
        days_of_week[i] = c == '1';
    }

    let stp_char = line.trim_end().chars().next_back()?;
    let stp_indicator = StpIndicator::try_from(stp_char).ok()?;

    Some(BasicSchedule {
        uid,
        stp_indicator,
        date_from,
        date_to,
        days_of_week,
    })
}

fn parse_time_field(field: &str) -> Option<NaiveTime> {
    if field.len() != 4 || !field.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let hour: u32 = field[0..2].parse().ok()?;
    let minute: u32 = field[2..4].parse().ok()?;
    NaiveTime::from_hms_opt(hour, minute, 0)
}

fn parse_calling_point(line: &str, kind: CallingPointKind) -> Option<CallingPoint> {
    let min_len = match kind {
        CallingPointKind::Origin | CallingPointKind::Terminate => MIN_LO_LT_LEN,
        CallingPointKind::Intermediate => MIN_LI_LEN,
    };
    if line.len() < min_len {
        return None;
    }

    let tiploc = line[2..9].to_string();
    let first_time = parse_time_field(&line[10..14]);
    let first_half_minute = line.as_bytes()[14] == b'H';

    let (booked_arrival, is_half_minute_arrival, booked_departure, is_half_minute_departure) =
        match kind {
            CallingPointKind::Origin => (None, false, first_time, first_half_minute),
            CallingPointKind::Terminate => (first_time, first_half_minute, None, false),
            CallingPointKind::Intermediate => {
                let second_time = parse_time_field(&line[15..19]);
                let second_half_minute = line.as_bytes()[19] == b'H';
                (
                    first_time,
                    first_half_minute,
                    second_time,
                    second_half_minute,
                )
            }
        };

    Some(CallingPoint {
        tiploc,
        kind,
        booked_arrival,
        booked_departure,
        is_half_minute_arrival,
        is_half_minute_departure,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::StpIndicator;

    // Real `BS` lines, byte-verbatim, quoted in
    // docs/superpowers/specs/2026-08-29-trust-schedule-delay-validation-findings.md
    // ("Task 3" section, Step 2) and
    // docs/superpowers/specs/2026-08-29-trust-schedule-delay-inference-timetable-verification.md
    // ("Claim 1" section).
    const BS_C00573_PERMANENT: &str =
        "BSNC005732605172612060000001 PXX1S003101121194800 DMU    125      S A T        P";
    const BS_C00574_PERMANENT: &str =
        "BSNC005742605172612060000001 PXX1P033104121194800 DMU    125      S A T        P";
    const BS_G00704_CANCELLATION: &str =
        "BSNG007042605172608300000001            1                                      C";
    const BS_W68468_OVERLAY: &str =
        "BSNW684682605172610180000001 POO2E88    113560015 EMU    075D     S            O";

    #[test]
    fn decodes_uid_dates_and_permanent_stp_from_a_real_bs_line() {
        let schedules = parse_schedule_records(BS_C00573_PERMANENT);
        assert_eq!(schedules.len(), 1);
        let basic = &schedules[0].basic;
        assert_eq!(basic.uid, "C00573");
        assert_eq!(basic.stp_indicator, StpIndicator::Permanent);
        assert_eq!(
            basic.date_from,
            NaiveDate::from_ymd_opt(2026, 5, 17).unwrap()
        );
        assert_eq!(basic.date_to, NaiveDate::from_ymd_opt(2026, 12, 6).unwrap());
        assert_eq!(
            basic.days_of_week,
            [false, false, false, false, false, false, true]
        );
    }

    #[test]
    fn a_second_real_bs_line_with_a_different_uid_decodes_consistently() {
        let schedules = parse_schedule_records(BS_C00574_PERMANENT);
        assert_eq!(schedules[0].basic.uid, "C00574");
        assert_eq!(schedules[0].basic.stp_indicator, StpIndicator::Permanent);
    }

    #[test]
    fn a_real_cancellation_bs_line_has_no_body_and_the_final_char_decodes_as_c() {
        let schedules = parse_schedule_records(BS_G00704_CANCELLATION);
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].basic.uid, "G00704");
        assert_eq!(schedules[0].basic.stp_indicator, StpIndicator::Cancellation);
        assert!(schedules[0].calling_points.is_empty());
    }

    #[test]
    fn a_real_overlay_bs_line_with_full_body_decodes_lo_and_the_o_indicator() {
        let text =
            format!("{BS_W68468_OVERLAY}\nBX         SRYSR408800\nLOBALLOCH 2308 2308          TB");
        let schedules = parse_schedule_records(&text);
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].basic.uid, "W68468");
        assert_eq!(schedules[0].basic.stp_indicator, StpIndicator::Overlay);
        assert_eq!(schedules[0].calling_points.len(), 1);
        let cp = &schedules[0].calling_points[0];
        assert_eq!(cp.tiploc, "BALLOCH");
        assert_eq!(cp.kind, CallingPointKind::Origin);
        assert_eq!(cp.booked_departure, NaiveTime::from_hms_opt(23, 8, 0));
        assert!(!cp.is_half_minute_departure);
    }

    // Real LO/LT/LI body lines, byte-verbatim, quoted in the verification
    // doc's "Claim 2" section.
    const LO_EUSTON: &str = "LOEUSTON  0822 08227  C      TB";
    const LT_EUSTON: &str = "LTEUSTON  0804 08079     TF";
    const LI_CARLILE: &str = "LICARLILE 1202 1213      120212131        T";
    const LO_WATRLMN: &str = "LOWATRLMN 0754 075315 MFL    TB";

    fn wrap_full_block(body: &[&str]) -> String {
        let mut lines = vec![BS_C00573_PERMANENT.to_string()];
        lines.extend(body.iter().map(|s| s.to_string()));
        lines.join("\n")
    }

    #[test]
    fn a_real_lo_line_decodes_tiploc_and_scheduled_departure_only() {
        let text = wrap_full_block(&[LO_EUSTON, LT_EUSTON]);
        let schedules = parse_schedule_records(&text);
        let cp = &schedules[0].calling_points[0];
        assert_eq!(cp.tiploc, "EUSTON ");
        assert_eq!(cp.kind, CallingPointKind::Origin);
        assert_eq!(cp.booked_arrival, None);
        assert_eq!(cp.booked_departure, NaiveTime::from_hms_opt(8, 22, 0));
        assert!(!cp.is_half_minute_departure);
    }

    #[test]
    fn a_real_lt_line_decodes_tiploc_and_scheduled_arrival_only() {
        let text = wrap_full_block(&[LO_EUSTON, LT_EUSTON]);
        let schedules = parse_schedule_records(&text);
        let cp = &schedules[0].calling_points[1];
        assert_eq!(cp.tiploc, "EUSTON ");
        assert_eq!(cp.kind, CallingPointKind::Terminate);
        assert_eq!(cp.booked_arrival, NaiveTime::from_hms_opt(8, 4, 0));
        assert_eq!(cp.booked_departure, None);
    }

    #[test]
    fn a_real_li_line_decodes_tiploc_arrival_and_departure() {
        let text = wrap_full_block(&[LO_WATRLMN, LI_CARLILE, LT_EUSTON]);
        let schedules = parse_schedule_records(&text);
        let cp = &schedules[0].calling_points[1];
        assert_eq!(cp.tiploc, "CARLILE");
        assert_eq!(cp.kind, CallingPointKind::Intermediate);
        assert_eq!(cp.booked_arrival, NaiveTime::from_hms_opt(12, 2, 0));
        assert_eq!(cp.booked_departure, NaiveTime::from_hms_opt(12, 13, 0));
        assert!(!cp.is_half_minute_arrival);
        assert!(!cp.is_half_minute_departure);
    }

    #[test]
    fn a_real_lo_line_with_padded_tiploc_matches_the_seven_char_field() {
        let text = wrap_full_block(&[LO_WATRLMN, LT_EUSTON]);
        let schedules = parse_schedule_records(&text);
        assert_eq!(schedules[0].calling_points[0].tiploc, "WATRLMN");
    }

    #[test]
    fn an_unrecognized_record_type_prefix_is_skipped_not_a_parse_error() {
        // Synthetic: an obviously-unrecognized two-char prefix.
        let text = wrap_full_block(&["ZZ this is not a real body line", LO_EUSTON, LT_EUSTON]);
        let schedules = parse_schedule_records(&text);
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].calling_points.len(), 2);
    }

    #[test]
    fn a_line_shorter_than_its_record_types_minimum_width_is_skipped() {
        // Synthetic: a truncated LO line, too short to carry a departure
        // time field at all.
        let text = wrap_full_block(&["LOSHORT", LT_EUSTON]);
        let schedules = parse_schedule_records(&text);
        assert_eq!(schedules[0].calling_points.len(), 1);
        assert_eq!(
            schedules[0].calling_points[0].kind,
            CallingPointKind::Terminate
        );
    }

    #[test]
    fn a_synthetic_half_minute_marker_is_captured_as_a_boolean_not_dropped() {
        // Synthetic, but built at the same real-byte-verified offsets as
        // LO_EUSTON above (10..14 time, 14 half-minute flag) -- the H
        // suffix itself is confirmed real only in the findings doc's
        // paraphrased summary form (e.g. "MKC@0750H"), never in a raw byte
        // quote, so this line's exact byte content is synthetic per this
        // plan's Non-goals, not presented as a real quote.
        let li_half_minute = "LIHTCHEND 1135H1136H     ";
        let text = wrap_full_block(&[LO_EUSTON, li_half_minute, LT_EUSTON]);
        let schedules = parse_schedule_records(&text);
        let cp = &schedules[0].calling_points[1];
        assert_eq!(cp.tiploc, "HTCHEND");
        assert_eq!(cp.booked_arrival, NaiveTime::from_hms_opt(11, 35, 0));
        assert!(cp.is_half_minute_arrival);
        assert_eq!(cp.booked_departure, NaiveTime::from_hms_opt(11, 36, 0));
        assert!(cp.is_half_minute_departure);
    }

    #[test]
    fn a_block_terminated_implicitly_by_the_next_bs_line_is_still_captured() {
        // A well-formed file always terminates a body with LT, but the
        // grouping logic must not *require* that -- the next BS line
        // implicitly ends whatever came before it, per real CIF structure.
        let text =
            format!("{BS_C00573_PERMANENT}\n{LO_EUSTON}\n{BS_C00574_PERMANENT}\n{LT_EUSTON}");
        let schedules = parse_schedule_records(&text);
        assert_eq!(schedules.len(), 2);
        assert_eq!(schedules[0].basic.uid, "C00573");
        assert_eq!(schedules[0].calling_points.len(), 1);
        assert_eq!(schedules[1].basic.uid, "C00574");
        assert_eq!(schedules[1].calling_points.len(), 1);
    }
}
