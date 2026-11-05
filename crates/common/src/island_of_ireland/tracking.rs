//! Identity of a tracked Iarnród Éireann train (distant-signal's
//! docs/superpowers/specs/2026-11-05-irish-rail-train-tracking-design.md
//! §3): its `trains.train_uid` is `ie:<TRAINCODE>`, and a subscription's
//! station refs are `ie:<StationCode>`.
//!
//! A CIF uid, a TRUST id or a Darwin RID never contains `:`, so these never
//! collide with a GB train. The trip planner's transit uids
//! (`<network>:<n>`) also use a colon; the `ie` prefix is reserved and must
//! never be a transit network id, and code that tells transit uids apart
//! asks [`is_train_uid`] first.
//!
//! The prefix is always the lower-case `ie:` and the code upper-case: the
//! writer looks a train up by `'ie:' || train_code`, so a uid built any
//! other way would never get a state. Build one only with [`train_uid`].

/// The `trains.train_uid` prefix of an Iarnród Éireann train.
pub const TRAIN_UID_PREFIX: &str = "ie:";

/// The `train_subscriptions.pin_*_ref` prefix of an Iarnród Éireann
/// station (its live `StationCode`).
pub const STATION_REF_PREFIX: &str = "ie:";

/// The operator's name, shown where a GB train shows its operator from CIF.
pub const OPERATOR_NAME: &str = "Iarnród Éireann";

/// A train code as the history stores it: trimmed and upper-cased, and
/// `^[A-Z0-9]{1,8}$` (the history tables' `CHECK`). `None` otherwise.
pub fn normalise_train_code(raw: &str) -> Option<String> {
    let code = raw.trim().to_ascii_uppercase();
    is_train_code(&code).then_some(code)
}

/// Whether `code` is already a normalised train code.
pub fn is_train_code(code: &str) -> bool {
    (1..=8).contains(&code.len())
        && code
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

/// The `trains.train_uid` of train `raw_code` (`"a120 "` is `"ie:A120"`).
/// `None` when the code isn't valid.
pub fn train_uid(raw_code: &str) -> Option<String> {
    normalise_train_code(raw_code).map(|code| format!("{TRAIN_UID_PREFIX}{code}"))
}

/// The train code of an `ie:` uid, exactly as [`train_uid`] builds them:
/// `"ie:A120"` is `Some("A120")`; `"IE:A120"`, `"ie:a120"` and GB uids are
/// `None`.
pub fn train_code_from_uid(uid: &str) -> Option<&str> {
    uid.strip_prefix(TRAIN_UID_PREFIX)
        .filter(|code| is_train_code(code))
}

/// Whether `uid` is an Iarnród Éireann train's uid. Any uid starting with
/// `ie:` counts, even a malformed one, so a caller that sorts uids into GB,
/// transit and Irish never sends an Irish-looking uid down another path.
pub fn is_train_uid(uid: &str) -> bool {
    uid.get(..TRAIN_UID_PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(TRAIN_UID_PREFIX))
}

/// Whether `code` fits a station ref's code part (`[A-Z0-9]{1,12}`, the
/// `train_subscriptions_pin_ref_check`).
fn is_station_code(code: &str) -> bool {
    (1..=12).contains(&code.len())
        && code
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

/// The `pin_origin_ref` of live station `raw_code` (`"cnlly"` is
/// `"ie:CNLLY"`). `None` when the code isn't valid.
pub fn station_ref(raw_code: &str) -> Option<String> {
    let code = raw_code.trim().to_ascii_uppercase();
    is_station_code(&code).then(|| format!("{STATION_REF_PREFIX}{code}"))
}

/// The live `StationCode` of an `ie:` station ref. `None` for any other
/// ref.
pub fn station_code_from_ref(station_ref: &str) -> Option<&str> {
    station_ref
        .strip_prefix(STATION_REF_PREFIX)
        .filter(|code| is_station_code(code))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn train_uids_are_lower_prefix_upper_code() {
        assert_eq!(train_uid(" a120 ").as_deref(), Some("ie:A120"));
        assert_eq!(train_uid("E901").as_deref(), Some("ie:E901"));
        assert_eq!(train_uid(""), None);
        assert_eq!(train_uid("A12345678"), None);
        assert_eq!(train_uid("A-1"), None);
        assert_eq!(train_uid("ie:A120"), None);
    }

    #[test]
    fn a_code_comes_back_only_from_a_uid_built_here() {
        assert_eq!(train_code_from_uid("ie:A120"), Some("A120"));
        assert_eq!(train_code_from_uid("IE:A120"), None);
        assert_eq!(train_code_from_uid("ie:a120"), None);
        assert_eq!(train_code_from_uid("C12345"), None);
        assert_eq!(train_code_from_uid("ie:"), None);
    }

    /// Any case of the prefix is Irish, so it never passes for a GB or
    /// transit uid; only a well-formed one has a code.
    #[test]
    fn any_ie_prefix_is_irish() {
        assert!(is_train_uid("ie:A120"));
        assert!(is_train_uid("IE:A120"));
        assert!(is_train_uid("ie:"));
        assert!(!is_train_uid("C12345"));
        assert!(!is_train_uid("luas:12"));
        assert!(!is_train_uid("i"));
        assert!(!is_train_uid("Éi"));
    }

    #[test]
    fn station_refs_round_trip() {
        assert_eq!(station_ref(" cnlly").as_deref(), Some("ie:CNLLY"));
        assert_eq!(station_code_from_ref("ie:CNLLY"), Some("CNLLY"));
        assert_eq!(station_ref(""), None);
        assert_eq!(station_ref("TOOLONGSTATION"), None);
        assert_eq!(station_code_from_ref("gb:WAT"), None);
        assert_eq!(station_code_from_ref("ie:cnlly"), None);
    }
}
