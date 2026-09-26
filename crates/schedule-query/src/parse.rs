//! Streams raw CIF `SCHEDULE` text into [`RawSchedule`] blocks.
//!
//! No I/O here -- the caller already read the file (or a fixture) into a
//! `&str`. A single malformed/too-short/non-ASCII line is skipped (though
//! a `BS`/`LT` one still closes whatever block it follows, exactly as a
//! well-formed one would -- see `is_fixed_width_decodable`, private, for
//! what counts as decodable here), never a
//! hard parse failure for the whole extraction -- mirroring
//! `crates/schedule-reference/src/parser.rs::parse_ti_lines`'s own
//! documented "a single malformed line must not abort the whole
//! extraction" posture. That sibling module is also fully log-free (no
//! `tracing` call, no skip-count return -- just a silently-shorter `Vec`)
//! despite its crate depending on `tracing` for its own `main.rs`; this
//! crate matched that same log-free posture for a long time rather than
//! inventing a fresh one, per this plan's Task 2 "decide during
//! implementation, matching whichever posture `schedule-reference` already
//! established" guidance.
//!
//! **One narrow, deliberate exception to that posture, as of 2026-09-25.**
//! An undecodable `LO`/`LI`/`LT` line dropped from an otherwise-successfully-parsing,
//! still-open block is not just "one fewer calling point" -- the block
//! stays open around the gap, so the calling points immediately before and
//! after the dropped one end up adjacent in [`RawSchedule::calling_points`]
//! with nothing recording that a real intermediate stop used to sit between
//! them. Every downstream consumer of this crate treats adjacency in that
//! `Vec` as "these two stops are directly connected" (this is the exact
//! shape the dynamic trip-planning connections graph -- `crates/schedule-query::connections`
//! -- builds edges from), so this one failure mode can silently fabricate a
//! journey leg that was never actually timetabled, which is a materially
//! worse and harder-to-notice failure than "this train's schedule is a
//! little shorter than it should be." Every OTHER malformed-line skip in
//! this module stays silent (a bad `BS`, a stray orphaned body line with no
//! open block, a too-short `BX`): none of those can splice two real,
//! non-adjacent stops together, so the original "skip malformed, never
//! abort, never log" posture is left alone for them. See
//! [`parse_schedule_records`]'s own doc comment for why the block is still
//! kept open (rather than discarded) even now that the gap is visible.

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
/// Minimum length of a `BX` line this parser can decode: needs bytes
/// `0..13` (record identity through the ATOC Code).
const MIN_BX_LEN: usize = 13;
/// 0-based byte offset of the STP (Short Term Planning) indicator within a
/// full-width `BS` line -- CIF User Spec column 80 (1-based), the record's
/// own LAST column. Fixed on purpose, not derived from the line's own
/// length: see [`parse_basic_schedule`]'s own doc comment (2026-09-25 fix)
/// for why reading "the last significant character" instead let a
/// truncated line decode into a plausible-but-wrong STP value.
const STP_INDICATOR_COL: usize = 79;

/// Is `line` safe to decode with this module's fixed-offset byte slices?
///
/// Every field in this module is read as `&line[a..b]` at a fixed byte
/// offset, because CIF is a fixed-width format. A `&str` byte slice panics
/// on **two** distinct conditions: an out-of-range index (covered by each
/// caller's own `MIN_*_LEN` length check) *and* an index that falls inside
/// a multi-byte UTF-8 character -- which a length check does **not** cover.
/// CIF is ASCII by specification, so on real feed bytes the second case
/// never arises; on a corrupted, truncated-mid-character, re-encoded or
/// hostile file it does, and every fixed-offset slice below would panic
/// (`"byte index N is not a char boundary"`) rather than skip the line.
///
/// One `is_ascii()` check per decoded line, taken before any slicing,
/// removes that whole panic class at the source: once a line is known to
/// be ASCII, every byte index in it is a char boundary by construction, so
/// each later fixed-offset slice is boundary-safe and only the length
/// check is left to do. A non-ASCII line is not valid CIF, so this
/// function's caller skips it, the same as any other malformed line --
/// never decoding it into a truncated or garbage value that would look
/// like a successful parse.
///
/// Note what this deliberately does NOT do: it is not scoped to the field
/// ranges actually sliced, so a stray non-ASCII byte anywhere on the line
/// -- including in the free-text region past the last field this crate
/// decodes -- rejects the whole record rather than just the field it sits
/// in. That is a real widening of "malformed", accepted on purpose: CIF is
/// ASCII by specification, so a line carrying non-ASCII bytes anywhere has
/// already failed the format, and a per-field check would be both slower
/// and easier to leave a hole in. Rejecting a `BS` line this way discards
/// its whole `LO`/`LI`/`LT` body too, since no block opens for the body to
/// attach to -- silently, like every other skip in this log-free module.
///
/// It is also not a content check in the other direction: `is_ascii()` is
/// true of the C0 controls, so a `NUL`-filled or otherwise control-byte
/// corrupted line passes and decodes into a plausible-looking record (a
/// TIPLOC of seven `NUL`s, say). That asymmetry is inherent to using an
/// encoding check as a corruption check, and is left as-is deliberately:
/// the job here is the char-boundary panic class, and tightening what
/// counts as a valid field VALUE is a separate decode-correctness
/// question this crate has no real-data evidence to settle.
///
/// This is deliberately NOT applied to the record-type dispatch in
/// [`parse_schedule_records`], which reads the two identity bytes through
/// `line.as_bytes()` instead -- byte indexing has no char-boundary hazard
/// at all, so the dispatch stays panic-free without an ASCII check, and a
/// non-ASCII `BS`/`LT` line still correctly TERMINATES the block it
/// follows instead of silently letting the next block's calling points
/// accumulate onto the previous schedule.
///
/// `fuzz/fuzz_targets/parse_schedule_records.rs` is the coverage-guided
/// harness this was found and verified with; its own doc comment records
/// the exact pre-fix crash triage and the post-fix clean run.
fn is_fixed_width_decodable(line: &str, min_len: usize) -> bool {
    line.len() >= min_len && line.is_ascii()
}

/// Parses every `BS`(+`BX`)/`LO`/`LI`*/`LT` block out of `text`, matching
/// the real CIF block structure a full `MCA` extract has: `BS` starts a
/// block; an optional `BX` line extends it (its ATOC Code field is decoded
/// into [`BasicSchedule::operator_atoc`] -- see [`parse_bx_operator`] --
/// every other `BX` field is recognized only so the line doesn't get
/// mistaken for an unrelated/malformed line); `LO`/`LI`*/`LT` are
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
///
/// **An undecodable `LO`/`LI`/`LT` line inside an open block is skipped, not
/// the whole block.** This was a deliberate choice, not the only option
/// considered -- see this module's own header doc for why the skip now
/// also emits a `tracing::warn!` (2026-09-25). Discarding the entire block
/// on one bad body line was rejected for two reasons: (1) it would make a
/// single corrupted byte -- the same class of real-world glitch this
/// module's char-boundary/ASCII guards already exist to survive -- delete a
/// whole train's timetable from every downstream product (station boards,
/// destination search, the connections graph) instead of costing it one
/// calling point, which is a strictly worse outcome for trip-planning
/// correctness than a visible gap; and (2) it would widen "malformed" well
/// past what this fix's own scope covers, re-litigating the fuzz-verified
/// "reject only the line that is actually bad" behaviour this module's own
/// `an_undecodable_bs_line_still_terminates_the_previous_block`/
/// `an_undecodable_lt_line_still_terminates_its_own_block` tests exist to
/// pin. Keeping the line-level skip but making it LOUD -- a `tracing::warn!`
/// naming the train UID and the line's position in the block -- gives an
/// operator a real signal to investigate a systematically corrupt feed
/// without trading a rare visible gap for a common invisible one.
pub fn parse_schedule_records(text: &str) -> Vec<RawSchedule> {
    let mut out = Vec::new();
    let mut current: Option<RawSchedule> = None;
    // 1-based position of the body line about to be processed within the
    // CURRENTLY OPEN block (reset whenever a real `BS` line opens a new
    // one) -- purely diagnostic context for the drop warning below, so a
    // log line can say WHERE in the block the gap is, not just which train.
    let mut body_line_position: u32 = 0;

    for line in text.lines() {
        // Dispatch on the two record-identity BYTES, not on `&line[0..2]`.
        // A `&str` slice would panic when byte index 2 falls inside a
        // multi-byte character (the very first of this parser's eight
        // fuzzer-found panic sites); a byte slice pattern cannot, and it
        // needs no length check either -- a shorter line simply matches
        // none of the arms. Crucially this also keeps a line whose
        // identity bytes ARE `BS`/`LT` but whose body is undecodable
        // (non-ASCII, too short, bad date, ...) flowing into the arms
        // below, so it still TERMINATES the open block, exactly as an
        // ASCII-but-malformed one always has. Rejecting such a line before
        // the dispatch instead would silently append the NEXT block's
        // calling points to the PREVIOUS schedule.
        match line.as_bytes() {
            [b'B', b'S', ..] => {
                if let Some(prev) = current.take() {
                    out.push(prev);
                }
                body_line_position = 0;
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
            [b'L', b'O', ..] => {
                body_line_position += 1;
                record_calling_point(
                    parse_calling_point(line, CallingPointKind::Origin),
                    current.as_mut(),
                    "LO",
                    body_line_position,
                );
            }
            [b'L', b'I', ..] => {
                body_line_position += 1;
                record_calling_point(
                    parse_calling_point(line, CallingPointKind::Intermediate),
                    current.as_mut(),
                    "LI",
                    body_line_position,
                );
            }
            [b'L', b'T', ..] => {
                body_line_position += 1;
                record_calling_point(
                    parse_calling_point(line, CallingPointKind::Terminate),
                    current.as_mut(),
                    "LT",
                    body_line_position,
                );
                if let Some(done) = current.take() {
                    out.push(done);
                }
            }
            [b'B', b'X', ..] => {
                if let Some(schedule) = current.as_mut() {
                    schedule.basic.operator_atoc = parse_bx_operator(line);
                }
                // A stray BX with no open block (current is None) is
                // dropped, exactly as a stray LO/LI/LT already is.
            }
            _ => {}
        }
    }

    if let Some(leftover) = current.take() {
        out.push(leftover);
    }

    out
}

/// Pushes a successfully-decoded calling point onto the currently open
/// block, or -- when decode failed AND a block is genuinely open -- emits a
/// `tracing::warn!` naming the train UID and the line's position in the
/// block before dropping it. See this module's own header doc for why this
/// is the one skip in this otherwise log-free parser that is not silent.
///
/// A decode failure with NO open block (`current` is `None`) stays silent:
/// an orphaned body line has no adjacent real stops for the gap to
/// fabricate a connection between, so it is exactly the same harmless shape
/// as every other malformed-line skip this module already treats quietly.
fn record_calling_point(
    cp: Option<CallingPoint>,
    current: Option<&mut RawSchedule>,
    record_type: &str,
    position: u32,
) {
    match (cp, current) {
        (Some(cp), Some(schedule)) => schedule.calling_points.push(cp),
        (None, Some(schedule)) => {
            tracing::warn!(
                train_uid = %schedule.basic.uid,
                record_type,
                position,
                calling_points_so_far = schedule.calling_points.len(),
                "dropped an undecodable body line inside an open schedule block; the block \
                 stays open, so the calling points immediately before and after this gap can \
                 look like a direct connection that was never actually timetabled"
            );
        }
        (_, None) => {}
    }
}

fn parse_basic_schedule(line: &str) -> Option<BasicSchedule> {
    // Guards every fixed-offset slice below (`3..9`, `9..15`, `15..21`,
    // `21..28`) against BOTH panic conditions at once -- see
    // [`is_fixed_width_decodable`]. `21..28` being exactly 7 ASCII
    // characters is also what bounds the `days_of_week[i]` write below to
    // `i <= 6` with no length check of its own.
    if !is_fixed_width_decodable(line, MIN_BS_LEN) {
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

    // **2026-09-25 fix.** This used to be `line.trim_end().chars().next_back()`
    // -- "the last significant character of the line" -- which reads the STP
    // indicator from wherever the line happens to END rather than from its
    // documented fixed CIF column (80, 1-based; `STP_INDICATOR_COL` = 79
    // 0-based). Both of this module's own real fixture lines are exactly 80
    // bytes and their last character IS the real STP indicator, so the two
    // approaches agree on well-formed input -- the divergence only shows up
    // on a TRUNCATED line, which is exactly the case that matters: a `BS`
    // line cut short partway through its free-text tail can, by pure
    // coincidence, end on a byte that happens to be `C`/`N`/`O`/`P`, and the
    // old logic would decode that as a complete, plausible, WRONG record
    // (this exact hazard was previously identified and deliberately left
    // unfixed here -- see this module's own `tests::a_bs_line_truncated_inside_every_fixed_field_is_skipped`
    // for the worked example, e.g. `&BS_C00573_PERMANENT[..30]` decoding as
    // a bogus Permanent schedule). Requiring the real fixed column instead
    // means a line too short to reach it fails cleanly (`None`), matching
    // this parser's own "skip malformed, never guess" posture for every
    // other fixed-offset field.
    if line.len() <= STP_INDICATOR_COL {
        return None;
    }
    // Safe: `is_fixed_width_decodable` (checked above) already confirmed
    // `line` is ASCII, so every byte index -- including this one -- is a
    // char boundary, and an ASCII byte can always be widened to `char`
    // directly.
    let stp_char = line.as_bytes()[STP_INDICATOR_COL] as char;
    let stp_indicator = StpIndicator::try_from(stp_char).ok()?;

    Some(BasicSchedule {
        uid,
        stp_indicator,
        date_from,
        date_to,
        days_of_week,
        // Filled in later by the BX arm in `parse_schedule_records`, if a
        // BX line follows this BS line; a Cancellation-indicator BS line
        // (see `StpIndicator::Cancellation`) or one with no BX body at all
        // keeps this `None`.
        operator_atoc: None,
    })
}

/// Decodes the ATOC/TOC operator code from a `BX` (Basic Schedule Extra
/// Details) line's `11..13` byte range (0-based, half-open) -- verified
/// against the real `BX         SRYSR408800` line quoted in
/// `docs/superpowers/specs/2026-08-29-trust-schedule-delay-inference-timetable-verification.md`
/// ("Claim 1" section), which decodes to `"SR"`.
///
/// Guarded by [`is_fixed_width_decodable`] against the same two panic
/// conditions every other fixed-offset slice in this module guards
/// against. `None` for a too-short/non-ASCII line (mirroring this module's
/// silent-skip posture for every other malformed line) and for a
/// space-filled ATOC Code field, if that ever occurs in real data.
fn parse_bx_operator(line: &str) -> Option<String> {
    if !is_fixed_width_decodable(line, MIN_BX_LEN) {
        return None;
    }
    let trimmed = line[11..13].trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn parse_time_field(field: &str) -> Option<NaiveTime> {
    if field.len() != 4 || !field.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let hour: u32 = field[0..2].parse().ok()?;
    let minute: u32 = field[2..4].parse().ok()?;
    NaiveTime::from_hms_opt(hour, minute, 0)
}

/// The CIF PUBLIC arrival/departure fields, which differ from
/// [`parse_time_field`]'s working-timetable ones in one way that matters:
/// `0000` is not midnight, it is the "this stop has no public time" sentinel,
/// which is what a non-public (set-down-only, operational, unadvertised)
/// calling point carries. A blank field is likewise `None`.
fn parse_public_time_field(field: &str) -> Option<NaiveTime> {
    if field == "0000" {
        return None;
    }
    parse_time_field(field)
}

/// A fixed-offset field that may run past the end of the line, clamped rather
/// than panicking or rejecting the whole record.
///
/// Every caller is past [`is_fixed_width_decodable`], so `line` is ASCII and
/// any byte index is a char boundary -- the only hazard left is length, and
/// unlike the working-time fields these later fields genuinely are missing
/// from real short lines: `LTEUSTON  0804 08079     TF` is 27 bytes and its
/// Activity field is specified as `25..37`. Raising `MIN_LO_LT_LEN` to 37 to
/// slice it "safely" would reject that real line outright, so the field is
/// clamped instead and simply comes back shorter (or empty).
fn ascii_field(line: &str, start: usize, end: usize) -> &str {
    if start >= line.len() {
        return "";
    }
    &line[start..end.min(line.len())]
}

/// The CIF Activity field's byte range for each record type -- `LO` `29..41`,
/// `LT` `25..37`, `LI` `42..54` -- all three independently verified against
/// this module's own real byte-verbatim fixtures by decomposing them field by
/// field from this crate's already-verified TIPLOC/time offsets:
///
/// ```text
/// LOEUSTON  0822 08227  C      TB
///           ^10..15     ^19..22 platform      ^29..41 activity = "TB"
///                ^15..19 public departure
/// LTEUSTON  0804 08079     TF
///           ^10..15   ^19..22 platform  ^25..37 activity = "TF"
///                ^15..19 public arrival
/// LICARLILE 1202 1213      120212131        T
///           ^10..15        ^25..29 public arr    ^42..54 activity = "T"
///                ^15..20     ^29..33 public dep
///                     ^20..25 pass
/// ```
fn activity_range(kind: CallingPointKind) -> (usize, usize) {
    match kind {
        CallingPointKind::Origin => (29, 41),
        CallingPointKind::Terminate => (25, 37),
        CallingPointKind::Intermediate => (42, 54),
    }
}

fn parse_calling_point(line: &str, kind: CallingPointKind) -> Option<CallingPoint> {
    let min_len = match kind {
        CallingPointKind::Origin | CallingPointKind::Terminate => MIN_LO_LT_LEN,
        CallingPointKind::Intermediate => MIN_LI_LEN,
    };
    // Guards every fixed-offset read below against BOTH panic conditions
    // at once -- see [`is_fixed_width_decodable`]. That is the `2..9` and
    // `10..14` slices plus the `as_bytes()[14]` half-minute byte (which is
    // why `MIN_LO_LT_LEN` is 15 and not 14), and, for an `LI` line, the
    // `15..19` slice plus `as_bytes()[19]` (likewise why `MIN_LI_LEN` is 20
    // and not 19). The byte reads carry no char-boundary hazard of their
    // own; they are the reason for the length half of the guard.
    if !is_fixed_width_decodable(line, min_len) {
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

    // Public times: the passenger-timetable times, distinct from the working
    // times above. `LO` carries only a public DEPARTURE and `LT` only a public
    // ARRIVAL, both at `15..19` -- the same offset an `LI`'s WORKING departure
    // occupies, which is why this is matched per-kind and not hoisted.
    let (public_arrival, public_departure) = match kind {
        CallingPointKind::Origin => (None, parse_public_time_field(ascii_field(line, 15, 19))),
        CallingPointKind::Terminate => (parse_public_time_field(ascii_field(line, 15, 19)), None),
        CallingPointKind::Intermediate => (
            parse_public_time_field(ascii_field(line, 25, 29)),
            parse_public_time_field(ascii_field(line, 29, 33)),
        ),
    };
    // CIF Platform field: `LO`/`LT` `19..22`, `LI` `33..36` -- see
    // `activity_range`'s own byte-layout diagram, which already marks both.
    let (platform_start, platform_end) = match kind {
        CallingPointKind::Origin | CallingPointKind::Terminate => (19, 22),
        CallingPointKind::Intermediate => (33, 36),
    };
    let platform = Some(ascii_field(line, platform_start, platform_end).trim())
        .filter(|p| !p.is_empty())
        .map(str::to_string);
    let (activity_start, activity_end) = activity_range(kind);
    let activity = ascii_field(line, activity_start, activity_end)
        .trim_end()
        .to_string();

    Some(CallingPoint {
        tiploc,
        kind,
        booked_arrival,
        booked_departure,
        is_half_minute_arrival,
        is_half_minute_departure,
        activity,
        public_arrival,
        public_departure,
        platform,
        // Always 0 here: a single BS(+BX)/LO/LI*/LT block is decoded in
        // isolation and has no reason to own cross-calling-point
        // day-rollover bookkeeping. The real value is computed once, over
        // the WINNING resolved schedule's whole calling-point sequence, by
        // `crate::resolve::assign_day_offsets` -- see `CallingPoint::day_offset`'s
        // own doc comment.
        day_offset: 0,
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
        // Real quoted BX line above ("BX         SRYSR408800") decodes its
        // `11..13` ATOC Code field to "SR" (ScotRail) -- verification doc,
        // "Claim 1" section.
        assert_eq!(schedules[0].basic.operator_atoc, Some("SR".to_string()));
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

    // --- Malformed-input tests ------------------------------------------
    //
    // An external coverage-guided fuzzing campaign (cargo-fuzz/libFuzzer +
    // AddressSanitizer) found eight panic sites in this module, all one
    // root cause: fixed-offset `&line[a..b]` slices guarded only by
    // BYTE-LENGTH checks, where a `&str` byte slice ALSO panics when an
    // index falls INSIDE a multi-byte UTF-8 character. CIF is ASCII by
    // spec, so real feed bytes never hit it; a corrupted,
    // truncated-mid-character, re-encoded or hostile file does. The eight:
    // `line[0..2]` in `parse_schedule_records` (since replaced by a byte
    // dispatch); `line[3..9]`, `line[9..15]`, `line[15..21]`,
    // `line[21..28]` in `parse_basic_schedule`; `line[2..9]`,
    // `line[10..14]`, `line[15..19]` in `parse_calling_point`.
    //
    // Six of the tests below are true regression guards -- they were
    // each measured failing against the unfixed parser, not assumed to:
    //
    // - `non_ascii_lines_do_not_panic` and
    //   `a_long_run_of_unknown_and_malformed_record_types_is_inert` guard
    //   the record-type DISPATCH (they still pass if only the decoders'
    //   `is_ascii()` half is reverted);
    // - the two `a_non_ascii_byte_at_every_fixed_field_boundary_of_*`
    //   tests guard the decoders' own guard as well;
    // - `an_undecodable_bs_line_still_terminates_the_previous_block` and
    //   `an_undecodable_lt_line_still_terminates_its_own_block` guard
    //   against a defect an earlier DRAFT of this fix introduced -- see
    //   their own comments -- and are the only two that fail against that
    //   draft specifically.
    //
    // The remaining seven are characterization tests for malformed-input
    // behaviour this parser already had (truncation, bad dates, bad
    // times, blank UID, unknown STP indicator, orphaned body lines, empty
    // input), pinned here because the audit that produced the fix had to
    // reason about each of them to be sure none was a NINTH panic site,
    // and pinning is what stops that reasoning from having to be redone.
    // They pass with the fix fully reverted, so they guard behaviour, not
    // this fix.
    //
    // A `#[test]` that merely RETURNS is already proof of no panic, but
    // each one also asserts the honest outcome (the bad line is skipped,
    // never decoded into a truncated/garbage value that looks like a
    // success, and never silently folded into a neighbouring record).

    #[test]
    fn non_ascii_lines_do_not_panic() {
        // Each of these is a real minimized fuzzer reproducer, or the same
        // shape as one. "\u{20AC}X" is the smallest: '\u{20AC}' is 3 bytes,
        // so the line is long enough for `&line[0..2]` to be in range while
        // byte index 2 sits inside the character -- the exact condition the
        // old `line.len() < 2` guard could not see. '\u{0BBF}' (Tamil vowel
        // sign, 3 bytes) is a second, independently-reported reproducer for
        // the same site.
        for line in [
            "\u{20AC}X",
            "\u{1F600}",
            "B\u{20AC}rest",
            "\u{061E}W",
            "\u{0BBF}",
            "\u{0BBF}BS",
            "B\u{0BBF}S",
        ] {
            let schedules = parse_schedule_records(line);
            assert!(
                schedules.is_empty(),
                "non-ASCII line {line:?} must be skipped, not decoded"
            );
        }
    }

    #[test]
    fn a_non_ascii_byte_at_every_fixed_field_boundary_of_a_bs_line_is_skipped() {
        // Walks a multi-byte character across a real BS line so that it
        // straddles each of `parse_basic_schedule`'s four fixed-offset
        // slice boundaries (3, 9, 15, 21, 28) in turn, plus the offsets
        // just inside each field. Offsets 0 and 2 are in the list for
        // completeness, not because they reach `parse_basic_schedule`:
        // they corrupt the record identity itself, so the dispatch
        // handles them.
        for at in [0, 2, 3, 8, 9, 14, 15, 20, 21, 27, 28] {
            let mut line = BS_C00573_PERMANENT.to_string();
            line.replace_range(at..at + 1, "\u{20AC}");
            let schedules = parse_schedule_records(&line);
            assert!(
                schedules.is_empty(),
                "BS line with a multi-byte char at offset {at} must be skipped"
            );
        }
    }

    #[test]
    fn a_non_ascii_byte_at_every_fixed_field_boundary_of_a_body_line_is_skipped() {
        // Same walk for `parse_calling_point`'s own slice boundaries (2, 9,
        // 10, 14 for LO/LT; plus 15, 19 for LI). The enclosing block is
        // well-formed, so a surviving panic-free parse must keep the BS
        // block and drop only the poisoned body line.
        for body in [LO_EUSTON, LT_EUSTON, LI_CARLILE] {
            for at in [2, 8, 9, 10, 13, 14, 15, 18, 19] {
                let mut line = body.to_string();
                line.replace_range(at..at + 1, "\u{20AC}");
                let text = wrap_full_block(&[line.as_str()]);
                let schedules = parse_schedule_records(&text);
                assert_eq!(schedules.len(), 1, "the BS block itself must survive");
                assert!(
                    schedules[0].calling_points.is_empty(),
                    "{body} with a multi-byte char at offset {at} must be skipped"
                );
            }
        }
    }

    #[test]
    fn empty_and_whitespace_only_input_parses_to_nothing() {
        for text in ["", "\n", "\r\n", "  ", "\n\n\n", "B", "L", "\0", "\0\0"] {
            assert!(parse_schedule_records(text).is_empty());
        }
    }

    #[test]
    fn a_bs_line_truncated_inside_every_fixed_field_is_skipped() {
        // Truncation at any byte before MIN_BS_LEN leaves at least one
        // fixed-offset field un-sliceable; each must skip, not panic.
        for len in 0..MIN_BS_LEN {
            let truncated = &BS_C00573_PERMANENT[..len];
            assert!(
                parse_schedule_records(truncated).is_empty(),
                "BS line truncated to {len} bytes must be skipped"
            );
        }
        // The untruncated line still decodes, so the guard is a real
        // boundary and not a blanket reject.
        assert_eq!(parse_schedule_records(BS_C00573_PERMANENT).len(), 1);

        // **This is the "separate pass" the comment below used to point
        // at, now done (2026-09-25).** Before the STP-indicator fixed-column
        // fix, a truncation past `MIN_BS_LEN` (28) but short of the full
        // 80-byte line decoded the STP indicator from wherever the line
        // happened to END, since `parse_basic_schedule` read it as "the
        // line's last significant character" rather than a fixed offset --
        // so `&BS_C00573_PERMANENT[..30]` used to decode as a complete,
        // plausible, WRONG Permanent schedule, picking its `P` out of the
        // Train Status column instead of a real STP field. Now that the STP
        // indicator is read from its real fixed column (80, 1-based), any
        // line shorter than that column -- including every one of these
        // truncations -- fails to decode at all.
        for len in MIN_BS_LEN..BS_C00573_PERMANENT.len() {
            assert!(
                parse_schedule_records(&BS_C00573_PERMANENT[..len]).is_empty(),
                "a BS line truncated to {len} bytes is short of the real STP column (80) and \
                 must be skipped, not decoded with a wrong STP guessed from wherever it ends"
            );
        }
    }

    #[test]
    fn a_body_line_truncated_inside_every_fixed_field_is_skipped() {
        for (body, min_len) in [
            (LO_EUSTON, MIN_LO_LT_LEN),
            (LT_EUSTON, MIN_LO_LT_LEN),
            (LI_CARLILE, MIN_LI_LEN),
        ] {
            for len in 0..min_len {
                let text = wrap_full_block(&[&body[..len]]);
                let schedules = parse_schedule_records(&text);
                assert_eq!(schedules.len(), 1);
                assert!(
                    schedules[0].calling_points.is_empty(),
                    "{body} truncated to {len} bytes must be skipped"
                );
            }
        }
    }

    /// A copy of the real permanent `BS` line with the bytes at `range`
    /// overwritten by `with` -- so each malformed-field case below differs
    /// from a known-good line in exactly one documented field, rather than
    /// being retyped by hand (where a miscounted space would make the test
    /// pass for the wrong reason).
    fn bs_line_with(range: std::ops::Range<usize>, with: &str) -> String {
        let mut line = BS_C00573_PERMANENT.to_string();
        line.replace_range(range, with);
        line
    }

    #[test]
    fn non_numeric_and_blank_date_fields_are_a_skip_not_a_panic() {
        // `parse_basic_schedule` reads two `YYMMDD` dates at fixed offsets
        // (`9..15` Date Runs From, `15..21` Date Runs To). Garbage there
        // must fail the date parse and skip the record.
        let cases = [
            ("non-numeric date-from", bs_line_with(9..15, "XXXXXX")),
            ("non-numeric date-to", bs_line_with(15..21, "XXXXXX")),
            ("blank date-from", bs_line_with(9..15, "      ")),
            ("blank date-to", bs_line_with(15..21, "      ")),
            ("month 99 in date-from", bs_line_with(9..15, "269999")),
            ("day 00 in date-to", bs_line_with(15..21, "261200")),
            ("negative-looking date", bs_line_with(9..15, "-10517")),
        ];
        for (what, line) in cases {
            assert!(
                parse_schedule_records(&line).is_empty(),
                "BS line with an undecodable date field ({what}) must be skipped"
            );
        }
    }

    #[test]
    fn a_blank_uid_or_unknown_stp_indicator_is_a_skip_not_a_panic() {
        // UID field (`3..9`) all spaces -> trims to empty -> skipped.
        assert!(parse_schedule_records(&bs_line_with(3..9, "      ")).is_empty());

        // Final significant character is not one of C/N/O/P.
        let len = BS_C00573_PERMANENT.len();
        assert!(parse_schedule_records(&bs_line_with(len - 1..len, "Z")).is_empty());

        // Truncated to exactly the fixed fields this parser decodes and
        // then space-padded out to the real 80-byte width: the fixed STP
        // column now lands on a space (part of the padding, not real data),
        // which is not a valid STP indicator either.
        let padded = format!("{}{}", &BS_C00573_PERMANENT[..MIN_BS_LEN], " ".repeat(52));
        assert!(parse_schedule_records(&padded).is_empty());

        // And a line that is nothing but its two-character record identity
        // plus spaces: no significant character after trimming at all.
        assert!(parse_schedule_records(&format!("BS{}", " ".repeat(78))).is_empty());
    }

    #[test]
    fn non_numeric_time_fields_decode_to_none_rather_than_panicking() {
        // A body line whose HHMM fields are garbage is still a structurally
        // valid calling point -- the time simply doesn't decode. This is
        // the one malformed-field class this parser keeps rather than
        // skips, matching its pre-existing behaviour for an absent time.
        let text = wrap_full_block(&["LOEUSTON  ABCD X", "LIEUSTON  99991-1  X", "LTEUSTON  ::::"]);
        let schedules = parse_schedule_records(&text);
        assert_eq!(schedules.len(), 1);
        let cps = &schedules[0].calling_points;
        assert_eq!(cps.len(), 2, "the too-short LT line is skipped");
        assert_eq!(cps[0].booked_departure, None);
        assert_eq!(cps[1].booked_arrival, None);
        assert_eq!(cps[1].booked_departure, None);
    }

    #[test]
    fn body_lines_with_no_open_block_are_dropped_without_panicking() {
        // Orphan LO/LI/LT lines before any BS line: `current` is `None`,
        // so there is nothing to push onto and nothing to index into.
        let text = format!("{LO_EUSTON}\n{LI_CARLILE}\n{LT_EUSTON}\n{LT_EUSTON}");
        assert!(parse_schedule_records(&text).is_empty());
    }

    #[test]
    fn a_long_run_of_unknown_and_malformed_record_types_is_inert() {
        // Two separate hazards interleaved: unknown-but-ASCII two-letter
        // record identities (which must reach the dispatch and fall
        // through its catch-all), and non-ASCII junk (which must not reach
        // any decoder). Neither may disturb the real block that follows.
        let mut lines = Vec::new();
        for (i, prefix) in ["TI", "AA", "ZZ", "HD", "QQ", "L!", "B ", "  "]
            .into_iter()
            .cycle()
            .take(200)
            .enumerate()
        {
            lines.push(format!("{prefix}unknown record {i}"));
            lines.push(format!("\u{20AC}\u{1F600} junk {i}"));
        }
        lines.push(BS_C00573_PERMANENT.to_string());
        lines.push(LO_EUSTON.to_string());
        lines.push(LT_EUSTON.to_string());
        let schedules = parse_schedule_records(&lines.join("\n"));
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].calling_points.len(), 2);
    }

    #[test]
    fn an_undecodable_bs_line_still_terminates_the_previous_block() {
        // Regression guard for a defect an earlier draft of the
        // char-boundary fix introduced: rejecting a non-ASCII line BEFORE
        // the record-type dispatch meant a `BS` line whose body happened
        // to carry a non-ASCII byte never ran the dispatch's
        // `current.take()`, so the NEXT block's calling points were
        // silently appended to the PREVIOUS schedule -- two real trains
        // merged into one, which is far worse than a dropped record.
        //
        // Byte 40 of a `BS` line is in the free-text region this crate
        // decodes no field from, so this line's every decoded field is
        // intact and only the ASCII rule rejects it. Whatever the reason a
        // `BS` line fails to decode, the block before it must close.
        let mut poisoned = BS_C00574_PERMANENT.to_string();
        poisoned.replace_range(40..41, "\u{00A3}");

        for second_bs in [
            poisoned.as_str(),
            // The same expectation for every OTHER way a BS line can fail
            // to decode -- the behaviour non-ASCII must not diverge from.
            "BS",
            &bs_line_with(9..15, "XXXXXX"),
            &bs_line_with(3..9, "      "),
        ] {
            let text = format!("{BS_C00573_PERMANENT}\n{LO_EUSTON}\n{second_bs}\n{LT_EUSTON}");
            let schedules = parse_schedule_records(&text);
            assert_eq!(
                schedules.len(),
                1,
                "only the first block decodes: {second_bs}"
            );
            assert_eq!(schedules[0].basic.uid, "C00573");
            assert_eq!(
                schedules[0].calling_points.len(),
                1,
                "the orphaned LT after an undecodable BS must NOT join C00573: {second_bs}"
            );
        }
    }

    #[test]
    fn an_undecodable_lt_line_still_terminates_its_own_block() {
        // The `LT` half of the same defect: `LT` closes a block whether or
        // not its own calling point decodes, so a non-ASCII `LT` must not
        // leave the block open for the next one's body to fall into.
        let mut poisoned = LT_EUSTON.to_string();
        poisoned.replace_range(24..25, "\u{00A3}");

        // The body lines AFTER the terminator are deliberately orphaned --
        // no `BS` follows to close the block for it. That is what makes
        // the `LT`'s own termination observable: leave the block open and
        // these stops get attributed to C00573.
        for terminator in [poisoned.as_str(), "LT", "LTEUSTON  ::::"] {
            let text = format!(
                "{BS_C00573_PERMANENT}\n{LO_EUSTON}\n{terminator}\n{LO_WATRLMN}\n{LI_CARLILE}"
            );
            let schedules = parse_schedule_records(&text);
            assert_eq!(schedules.len(), 1, "one block: {terminator}");
            assert_eq!(schedules[0].basic.uid, "C00573");
            assert_eq!(
                schedules[0].calling_points.len(),
                1,
                "C00573 keeps only its own LO, not the orphans after an \
                 undecodable LT: {terminator}"
            );
        }
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

/// Regression tests for the 2026-09-25 Activity/public-time decode.
///
/// The failure being fixed: `parse_calling_point` read only the WORKING
/// arrival/departure times, so a calling point that stops to SET DOWN
/// passengers only, or for operational reasons, or that is not advertised to
/// the public at all, was indistinguishable from a genuine boardable
/// departure -- and was published as one, on station boards and in the trip
/// planner.
///
/// Every fixture here is either a real byte-verbatim line already quoted in
/// this module's own tests, or that same line with ONLY its Activity field
/// overwritten in place (byte length unchanged, so the offsets under test are
/// the real ones) -- clearly marked where that is the case, per this crate's
/// "quote real bytes when available, mark anything else synthetic" convention.
#[cfg(test)]
mod activity_tests {
    use super::*;

    const LO_EUSTON: &str = "LOEUSTON  0822 08227  C      TB";
    const LT_EUSTON: &str = "LTEUSTON  0804 08079     TF";
    const LI_CARLILE: &str = "LICARLILE 1202 1213      120212131        T";

    /// `line` with ONLY its Activity field replaced by `activity`, space-padded
    /// out to the field's real end offset -- every byte before the field is the
    /// real, byte-verbatim fixture, so the offsets under test are the real,
    /// verified ones and the only thing synthetic is the field's own content.
    /// (Real CIF records are 80 bytes; these fixtures are quoted
    /// right-trimmed, which is why padding out to the field end is needed at
    /// all.)
    fn with_activity(line: &str, kind: CallingPointKind, activity: &str) -> String {
        let (start, end) = activity_range(kind);
        assert!(
            activity.len() <= end - start,
            "an Activity field holds at most {} bytes",
            end - start
        );
        let mut out = String::new();
        out.push_str(&line[..start]);
        out.push_str(activity);
        while out.len() < end {
            out.push(' ');
        }
        assert_eq!(
            &out[..start],
            &line[..start],
            "every byte before the Activity field must stay byte-verbatim real"
        );
        out
    }

    fn cp(line: &str, kind: CallingPointKind) -> CallingPoint {
        parse_calling_point(line, kind).expect("real fixture line must decode")
    }

    #[test]
    fn the_real_fixture_lines_decode_the_booked_platform_field_at_its_verified_offset() {
        assert_eq!(
            cp(LO_EUSTON, CallingPointKind::Origin).platform.as_deref(),
            Some("7")
        );
        assert_eq!(
            cp(LT_EUSTON, CallingPointKind::Terminate)
                .platform
                .as_deref(),
            Some("9")
        );
        assert_eq!(
            cp(LI_CARLILE, CallingPointKind::Intermediate)
                .platform
                .as_deref(),
            Some("1")
        );
        // Two-character platform immediately followed by the Line field.
        assert_eq!(
            cp("LOWATRLMN 0754 075315 MFL    TB", CallingPointKind::Origin)
                .platform
                .as_deref(),
            Some("15")
        );
    }

    #[test]
    fn a_blank_or_truncated_platform_field_decodes_to_none_not_an_empty_string() {
        // Real LI shape with the platform bytes blanked out.
        let blank = "LICARLILE 1202 1213      12021213         T";
        assert_eq!(cp(blank, CallingPointKind::Intermediate).platform, None);
        // A line that ends before the platform field at all.
        assert_eq!(
            cp("LTEUSTON  0804 0807", CallingPointKind::Terminate).platform,
            None
        );
    }

    #[test]
    fn the_real_fixture_lines_decode_the_activity_field_at_its_verified_offset() {
        assert_eq!(cp(LO_EUSTON, CallingPointKind::Origin).activity, "TB");
        assert_eq!(cp(LT_EUSTON, CallingPointKind::Terminate).activity, "TF");
        assert_eq!(cp(LI_CARLILE, CallingPointKind::Intermediate).activity, "T");
    }

    #[test]
    fn the_real_fixture_lines_decode_their_public_times() {
        let lo = cp(LO_EUSTON, CallingPointKind::Origin);
        assert_eq!(lo.public_departure, NaiveTime::from_hms_opt(8, 22, 0));
        assert_eq!(lo.public_arrival, None, "an LO has no public arrival");

        let lt = cp(LT_EUSTON, CallingPointKind::Terminate);
        assert_eq!(lt.public_arrival, NaiveTime::from_hms_opt(8, 7, 0));
        assert_eq!(lt.public_departure, None, "an LT has no public departure");

        let li = cp(LI_CARLILE, CallingPointKind::Intermediate);
        assert_eq!(li.public_arrival, NaiveTime::from_hms_opt(12, 2, 0));
        assert_eq!(li.public_departure, NaiveTime::from_hms_opt(12, 13, 0));
    }

    #[test]
    fn activity_codes_splits_the_packed_field_into_two_character_codes() {
        let line = with_activity(LI_CARLILE, CallingPointKind::Intermediate, "T RM");
        let parsed = cp(&line, CallingPointKind::Intermediate);
        assert_eq!(
            parsed.activity_codes().collect::<Vec<_>>(),
            vec!["T", "RM"],
            "single-character codes are left-justified in their own 2-char slot"
        );
    }

    /// **The core of the fix.** A real `LI` line whose only Activity code is
    /// `D` -- stops to SET DOWN passengers only. It has a booked departure, so
    /// before this change it was published as a boardable departure.
    #[test]
    fn a_set_down_only_stop_is_not_a_public_pickup() {
        let line = with_activity(LI_CARLILE, CallingPointKind::Intermediate, "D");
        let parsed = cp(&line, CallingPointKind::Intermediate);
        assert_eq!(parsed.activity, "D");
        assert!(
            parsed.booked_departure.is_some(),
            "sanity check: it really does carry a booked departure, which is what made it \
             indistinguishable from a boardable one"
        );
        assert!(!parsed.is_public_pickup());
    }

    #[test]
    fn an_operational_stop_is_not_a_public_pickup() {
        let line = with_activity(LI_CARLILE, CallingPointKind::Intermediate, "OP");
        assert!(!cp(&line, CallingPointKind::Intermediate).is_public_pickup());
    }

    /// `N` wins even when a passenger code sits alongside it: a stop not
    /// advertised to the public is not somewhere this app may tell a user to
    /// board, whatever else the field says.
    #[test]
    fn a_not_advertised_stop_is_not_a_public_pickup_even_next_to_a_passenger_code() {
        let line = with_activity(LI_CARLILE, CallingPointKind::Intermediate, "T N");
        let parsed = cp(&line, CallingPointKind::Intermediate);
        assert_eq!(parsed.activity_codes().collect::<Vec<_>>(), vec!["T", "N"]);
        assert!(!parsed.is_public_pickup());
    }

    #[test]
    fn ordinary_pickup_codes_are_public_pickups() {
        for code in ["T", "TB", "U", "R"] {
            let line = with_activity(LI_CARLILE, CallingPointKind::Intermediate, code);
            assert!(
                cp(&line, CallingPointKind::Intermediate).is_public_pickup(),
                "activity {code} must be treated as boardable"
            );
        }
    }

    /// A real origin: `TB` (train begins) must always be boardable -- if this
    /// regressed, every schedule's own first stop would vanish from every
    /// station board.
    #[test]
    fn a_real_origin_train_begins_stop_is_a_public_pickup() {
        assert!(cp(LO_EUSTON, CallingPointKind::Origin).is_public_pickup());
    }

    /// **The fail-open property, and why it is deliberate.** A line too short
    /// to carry the Activity field at all (the real `LT` fixture is 27 bytes;
    /// its field is specified at `25..37`) must still decode, and an absent
    /// Activity must read as boardable rather than silently emptying a board.
    #[test]
    fn a_line_too_short_to_carry_the_activity_field_still_decodes_and_fails_open() {
        let truncated = &LI_CARLILE[..25];
        let parsed = parse_calling_point(truncated, CallingPointKind::Intermediate)
            .expect("a line past MIN_LI_LEN must still decode");
        assert_eq!(parsed.activity, "");
        assert_eq!(parsed.public_arrival, None);
        assert!(
            parsed.is_public_pickup(),
            "an absent Activity field must fail OPEN -- failing closed would silently empty \
             station boards on any future decode gap"
        );
    }

    /// The CIF "no public time" sentinel. `0000` is not midnight; it is what a
    /// non-public stop carries, and it must not decode as `00:00`.
    #[test]
    fn a_zero_public_time_is_absent_not_midnight() {
        assert_eq!(parse_public_time_field("0000"), None);
        assert_eq!(parse_public_time_field("    "), None);
        assert_eq!(
            parse_public_time_field("0822"),
            NaiveTime::from_hms_opt(8, 22, 0)
        );
    }
}

/// Regression tests for the 2026-09-25 dropped-body-line warning (this
/// module's own header doc: "One narrow, deliberate exception" to its
/// otherwise log-free posture). A hand-rolled minimal [`tracing::Subscriber`]
/// -- this crate deliberately has no `tracing-subscriber`/`tracing-test` dev
/// dependency, matching its own "lib-only, no extra dependencies" convention
/// (see `Cargo.toml`) -- captures every event's fields so these tests can
/// assert on the exact train UID/record-type/position a real operator would
/// see, not just "something logged."
#[cfg(test)]
mod drop_warning_tests {
    use std::sync::{Arc, Mutex};

    use tracing::field::{Field, Visit};

    use super::*;

    #[derive(Default, Clone, Debug)]
    struct CapturedEvent {
        message: String,
        train_uid: Option<String>,
        record_type: Option<String>,
        position: Option<u64>,
    }

    impl Visit for CapturedEvent {
        fn record_u64(&mut self, field: &Field, value: u64) {
            if field.name() == "position" {
                self.position = Some(value);
            }
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            match field.name() {
                "train_uid" => self.train_uid = Some(value.to_string()),
                "record_type" => self.record_type = Some(value.to_string()),
                "message" => self.message = value.to_string(),
                _ => {}
            }
        }

        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            // `tracing::warn!("literal string")`'s message field, and a
            // `%display` field like `train_uid`, both come through here on
            // some tracing versions rather than `record_str` -- covering
            // both keeps this test robust to that detail.
            let rendered = format!("{value:?}");
            let rendered = rendered.strip_prefix('"').unwrap_or(&rendered);
            let rendered = rendered.strip_suffix('"').unwrap_or(rendered);
            match field.name() {
                "train_uid" if self.train_uid.is_none() => {
                    self.train_uid = Some(rendered.to_string());
                }
                "record_type" if self.record_type.is_none() => {
                    self.record_type = Some(rendered.to_string());
                }
                "message" if self.message.is_empty() => {
                    self.message = rendered.to_string();
                }
                _ => {}
            }
        }
    }

    /// A minimal [`tracing::Subscriber`] that stores every event's fields in
    /// order, in a `Mutex<Vec<_>>` shared with the test. `enabled` always
    /// returns `true`: these tests want every event this parser emits, not a
    /// level-filtered subset.
    struct RecordingSubscriber {
        events: Arc<Mutex<Vec<CapturedEvent>>>,
    }

    impl tracing::Subscriber for RecordingSubscriber {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            let mut captured = CapturedEvent::default();
            event.record(&mut captured);
            self.events.lock().unwrap().push(captured);
        }

        fn enter(&self, _span: &tracing::span::Id) {}

        fn exit(&self, _span: &tracing::span::Id) {}
    }

    const BS_C00573_PERMANENT: &str =
        "BSNC005732605172612060000001 PXX1S003101121194800 DMU    125      S A T        P";
    const LO_EUSTON: &str = "LOEUSTON  0822 08227  C      TB";
    const LT_EUSTON: &str = "LTEUSTON  0804 08079     TF";

    fn wrap_full_block(body: &[&str]) -> String {
        let mut lines = vec![BS_C00573_PERMANENT.to_string()];
        lines.extend(body.iter().map(|s| s.to_string()));
        lines.join("\n")
    }

    #[test]
    fn an_undecodable_body_line_inside_an_open_block_warns_with_train_uid_and_position() {
        // "LISHORT" is well short of MIN_LI_LEN (20) -- undecodable, and
        // dropped from a block that is otherwise open and successfully
        // parsing (LO_EUSTON before it, LT_EUSTON after it).
        let text = wrap_full_block(&[LO_EUSTON, "LISHORT", LT_EUSTON]);

        let events = Arc::new(Mutex::new(Vec::new()));
        let subscriber = RecordingSubscriber {
            events: events.clone(),
        };
        let schedules =
            tracing::subscriber::with_default(subscriber, || parse_schedule_records(&text));

        // The gap really is there, silently, in the returned data -- LO and
        // LT are now adjacent with no trace of the dropped LI between them.
        // This assertion documents exactly the fabricated-adjacency shape
        // the warning exists to make visible.
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].calling_points.len(), 2);

        let captured = events.lock().unwrap();
        assert_eq!(
            captured.len(),
            1,
            "exactly one warning for the one dropped line, no warning for the two good ones"
        );
        assert!(
            captured[0]
                .message
                .contains("dropped an undecodable body line"),
            "unexpected message: {:?}",
            captured[0].message
        );
        assert_eq!(captured[0].train_uid.as_deref(), Some("C00573"));
        assert_eq!(captured[0].record_type.as_deref(), Some("LI"));
        assert_eq!(
            captured[0].position,
            Some(2),
            "LO_EUSTON is body position 1, the dropped LI is position 2"
        );
    }

    #[test]
    fn a_dropped_line_with_no_open_block_stays_silent() {
        // No BS at all -- these two body lines are orphaned from the start,
        // exactly the pre-existing, already-tested "no open block" skip
        // (`parse_schedule_records`'s own `body_lines_with_no_open_block_are_dropped_without_panicking`).
        // There is no surviving block for a dropped line here to fabricate
        // an adjacency inside, so this must not warn.
        let text = format!("{LO_EUSTON}\n{LT_EUSTON}");

        let events = Arc::new(Mutex::new(Vec::new()));
        let subscriber = RecordingSubscriber {
            events: events.clone(),
        };
        let schedules =
            tracing::subscriber::with_default(subscriber, || parse_schedule_records(&text));

        assert!(schedules.is_empty());
        assert!(
            events.lock().unwrap().is_empty(),
            "an orphaned body line has no open block to fabricate a connection in"
        );
    }

    #[test]
    fn a_fully_well_formed_block_emits_no_warnings_at_all() {
        let text = wrap_full_block(&[LO_EUSTON, LT_EUSTON]);

        let events = Arc::new(Mutex::new(Vec::new()));
        let subscriber = RecordingSubscriber {
            events: events.clone(),
        };
        let schedules =
            tracing::subscriber::with_default(subscriber, || parse_schedule_records(&text));

        assert_eq!(schedules[0].calling_points.len(), 2);
        assert!(
            events.lock().unwrap().is_empty(),
            "nothing was dropped, so nothing should warn"
        );
    }
}
