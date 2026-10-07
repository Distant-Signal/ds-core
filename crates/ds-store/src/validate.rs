//! Pure input checks shared by the api's routes and the ingest half of
//! `train_tracking`: `validate_short_text`, `validate_code_list`,
//! `is_ascii_code` and `is_crs_code`, from `api/src/routes/mod.rs` (which
//! re-exports them).

/// The one shared bound for short free-text fields on authenticated writes
/// (API-7). Without it a logged-in user could park multi-megabyte strings
/// in TEXT columns (the default body limit is 2MB, 8MB on the train
/// router). Counts characters, not bytes, like `validate_custom_name`, and
/// rejects control characters, which no short display field needs.
///
/// `label` is user-facing copy (these 400 bodies are rendered verbatim by
/// the frontend), so it is plain words such as "The operator", never a
/// field name.
pub fn validate_short_text(label: &str, value: &str, max_chars: usize) -> Result<(), String> {
    if value.chars().count() > max_chars {
        return Err(format!(
            "{label} is too long. Keep it to {max_chars} characters or fewer."
        ));
    }
    if value.chars().any(char::is_control) {
        return Err(format!("{label} contains characters that aren't allowed."));
    }
    Ok(())
}

/// [`validate_short_text`] for a list field: at most `max_items` entries,
/// and every entry must pass `is_valid`. `describe` is the user-facing
/// shape, for example "three-letter station codes".
pub fn validate_code_list(
    label: &str,
    values: &[String],
    max_items: usize,
    describe: &str,
    is_valid: impl Fn(&str) -> bool,
) -> Result<(), String> {
    if values.len() > max_items {
        return Err(format!("{label} can have at most {max_items} entries."));
    }
    if let Some(bad) = values.iter().find(|v| !is_valid(v)) {
        // Bounded before echoing, so an oversized entry never comes back
        // whole in the error body.
        let shown: String = bad.chars().take(16).filter(|c| !c.is_control()).collect();
        return Err(format!("{label} must be {describe}; '{shown}' isn't one."));
    }
    Ok(())
}

/// `len` ASCII letters or digits, case-insensitive. The shape of an ATOC
/// operator code (2) and of a CRS code when `letters_only`.
pub fn is_ascii_code(value: &str, min_len: usize, max_len: usize, letters_only: bool) -> bool {
    let n = value.len();
    (min_len..=max_len).contains(&n)
        && value.bytes().all(|b| {
            if letters_only {
                b.is_ascii_alphabetic()
            } else {
                b.is_ascii_alphanumeric()
            }
        })
}

/// A three-letter CRS code (untrimmed; callers trim first if they accept
/// padding).
pub fn is_crs_code(value: &str) -> bool {
    is_ascii_code(value, 3, 3, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_short_text_counts_characters_and_rejects_control_characters() {
        assert!(validate_short_text("The name", "abc", 3).is_ok());
        assert!(validate_short_text("The name", "äöü", 3).is_ok());
        assert!(
            validate_short_text("The name", "abcd", 3)
                .unwrap_err()
                .contains("3 characters")
        );
        assert!(validate_short_text("The name", "a\nb", 10).is_err());
    }

    #[test]
    fn validate_code_list_bounds_count_and_shape_without_echoing_the_whole_value() {
        let ok = vec!["WAT".to_string(), "wok".to_string()];
        assert!(validate_code_list("Stops", &ok, 2, "codes", is_crs_code).is_ok());
        assert!(validate_code_list("Stops", &ok, 1, "codes", is_crs_code).is_err());
        let bad = vec!["W".repeat(10_000)];
        let err = validate_code_list("Stops", &bad, 5, "codes", is_crs_code).unwrap_err();
        assert!(err.len() < 100, "{err}");
    }

    #[test]
    fn is_ascii_code_checks_length_and_charset() {
        assert!(is_ascii_code("SW", 2, 2, false));
        assert!(is_ascii_code("1P", 1, 4, false));
        assert!(!is_ascii_code("1P", 1, 4, true));
        assert!(!is_ascii_code("S W", 2, 3, false));
        assert!(!is_ascii_code("ÄB", 2, 3, false));
        assert!(is_crs_code("wat"));
        assert!(!is_crs_code("WA1"));
    }
}
