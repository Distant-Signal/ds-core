//! TIPLOC padding/matching helper.
//!
//! Confirmed real and load-bearing by the verification doc's "Claim 2"
//! section: the schedule-body `LO`/`LI`/`LT` TIPLOC field is a **fixed
//! 7-character, space-padded** string (`"EUSTON "`, `"WATFDJ "` -- no
//! padding needed for `"CARLILE"`, which happens to be exactly 7
//! characters, "masking the bug in casual testing" per that section's own
//! wording), while this app's `lines/*.toml` stores the bare, unpadded
//! TIPLOC (`"EUSTON"`, 6 chars). A naive substring compare between the two
//! representations silently fails for every TIPLOC shorter than 7
//! characters.

/// Trims the fixed 7-character space-padding a CIF schedule-body TIPLOC
/// field carries, so it compares equal to `lines/*.toml`'s bare,
/// already-curated `Station.tiploc` value. Idempotent on already-trimmed
/// input, since a caller may pass either shape.
///
/// Returns `&str` rather than `String`: every real caller in this crate
/// ([`crate::resolve::schedules_touching`]) only needs the trimmed value
/// for an equality comparison, never an owned copy, so borrowing avoids an
/// unnecessary allocation at every call site.
pub fn normalize_tiploc(raw: &str) -> &str {
    raw.trim()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trims_the_real_seven_char_space_padded_schedule_body_field() {
        assert_eq!(normalize_tiploc("EUSTON "), "EUSTON");
    }

    #[test]
    fn is_idempotent_on_an_already_bare_tiploc() {
        assert_eq!(normalize_tiploc("EUSTON"), "EUSTON");
    }

    #[test]
    fn a_seven_char_tiploc_needing_no_padding_is_unchanged() {
        // The real CARLILE case the verification doc's own "Claim 2"
        // section flags as exactly 7 characters already.
        assert_eq!(normalize_tiploc("CARLILE"), "CARLILE");
    }
}
