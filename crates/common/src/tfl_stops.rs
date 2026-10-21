//! `TfL`'s Tube, DLR and tram stops (docs/superpowers/specs/2026-10-20-tfl-stop-lookup-design.md):
//! the `tfl-stops/1` row poller-tfl publishes on `ds:ingest:tfl-stops` and the
//! ingest-writer applies to `tfl_stops`, plus the name rules both ends and
//! the api share.

use serde::{Deserialize, Serialize};

/// The stream schema's name (`tfl-stops/1`).
pub const SCHEMA: &str = "tfl-stops";

/// `ingest_freshness.source` of the stops snapshot.
pub const FRESHNESS_SOURCE: &str = "tfl-stops";

/// The modes this lookup covers, in display order. Buses are a later
/// card (DS-77); Overground and the Elizabeth line stay on the National
/// Rail station search.
pub const MODES: [&str; 3] = ["tube", "dlr", "tram"];

/// The longest stop name kept (`tfl_stops.name`'s CHECK).
pub const NAME_MAX_CHARS: usize = 100;

/// One line serving a stop.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TflStopLine {
    /// `TfL`'s line id (`victoria`, `dlr`, `tram`).
    pub id: String,
    /// `TfL`'s line name (`Victoria`).
    pub name: String,
}

/// One stop of a `tfl-stops/1` snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TflStop {
    /// The `NaPTAN` stop-area id (`940GZZLUEUS`).
    pub id: String,
    /// The cleaned name ([`clean_stop_name`]): `Euston`.
    pub name: String,
    /// The in-scope modes ([`MODES`]) the stop serves.
    pub modes: Vec<String>,
    /// Its lines in those modes.
    #[serde(default)]
    pub lines: Vec<TflStopLine>,
    /// `TfL`'s interchange hub (`HUBEUS`), when it is part of one.
    #[serde(default)]
    pub hub_code: Option<String>,
    #[serde(default)]
    pub lat: Option<f64>,
    #[serde(default)]
    pub lon: Option<f64>,
    /// The National Rail station(s) at the same hub, by CRS, as far as the
    /// producer can tell (the hub code's own letters).
    #[serde(default)]
    pub crs: Vec<String>,
    /// The `TIPLOCs` of the National Rail stations at the same hub (`TfL`'s
    /// `910G<TIPLOC>` ids), which the writer resolves to CRS codes through
    /// `tiploc_crs`.
    #[serde(default)]
    pub rail_tiplocs: Vec<String>,
}

/// Whether `id` looks like a stop id this lookup stores: `NaPTAN` ids are
/// upper-case letters and digits.
pub fn is_stop_id(id: &str) -> bool {
    (4..=20).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

/// `TfL`'s `commonName` without its mode suffix: `Euston Underground
/// Station` -> `Euston`, `Canary Wharf DLR Station` -> `Canary Wharf`,
/// `Wimbledon Tram Stop` -> `Wimbledon`. A name that would be left empty
/// is kept whole.
pub fn clean_stop_name(common_name: &str) -> String {
    const SUFFIXES: [&str; 6] = [
        " Underground Station",
        " DLR Station",
        " Tram Stop",
        " Tram Station",
        " Rail Station",
        " Station",
    ];
    let name = common_name.split_whitespace().collect::<Vec<_>>().join(" ");
    for suffix in SUFFIXES {
        if let Some(stripped) = strip_suffix_ignore_case(&name, suffix) {
            let stripped = stripped.trim();
            if !stripped.is_empty() {
                return stripped.to_owned();
            }
        }
    }
    name
}

fn strip_suffix_ignore_case<'a>(name: &'a str, suffix: &str) -> Option<&'a str> {
    let cut = name.len().checked_sub(suffix.len())?;
    let tail = name.get(cut..)?;
    tail.eq_ignore_ascii_case(suffix).then(|| &name[..cut])
}

/// The search form of a name or query: lower case, `&` as `and`, `St.` as
/// `st`, every other run of punctuation or space one space. So "kings
/// cross" finds "King's Cross St. Pancras" and "elephant and castle" finds
/// "Elephant & Castle".
pub fn search_key(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut pending_space = false;
    for ch in text.chars() {
        if ch == '\'' || ch == '\u{2019}' || ch == '.' {
            // Dropped, not a word break: "King's" is "kings".
            continue;
        }
        if ch == '&' {
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str("and");
            pending_space = true;
            continue;
        }
        if ch.is_alphanumeric() {
            if pending_space && !out.is_empty() {
                out.push(' ');
            }
            pending_space = false;
            out.extend(ch.to_lowercase());
        } else {
            pending_space = true;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_lose_their_mode_suffix() {
        assert_eq!(clean_stop_name("Euston Underground Station"), "Euston");
        assert_eq!(clean_stop_name("Canary Wharf DLR Station"), "Canary Wharf");
        assert_eq!(clean_stop_name("Wimbledon Tram Stop"), "Wimbledon");
        assert_eq!(
            clean_stop_name("King's Cross St. Pancras Underground Station"),
            "King's Cross St. Pancras"
        );
        assert_eq!(clean_stop_name("  Bank   DLR station "), "Bank");
        // Nothing to strip, or nothing left: kept whole.
        assert_eq!(
            clean_stop_name("Shepherd's Bush Market"),
            "Shepherd's Bush Market"
        );
        assert_eq!(clean_stop_name("Station"), "Station");
    }

    #[test]
    fn search_keys_ignore_punctuation_and_case() {
        assert_eq!(
            search_key("King's Cross St. Pancras"),
            "kings cross st pancras"
        );
        assert_eq!(search_key("kings cross"), "kings cross");
        assert_eq!(search_key("Elephant & Castle"), "elephant and castle");
        assert_eq!(
            search_key("Heathrow Terminals 2 & 3"),
            "heathrow terminals 2 and 3"
        );
        assert_eq!(search_key("  Harrow-on-the-Hill "), "harrow on the hill");
        assert_eq!(search_key("&"), "and");
        assert_eq!(search_key("!!"), "");
    }

    #[test]
    fn stop_ids_are_upper_case_naptan_codes() {
        assert!(is_stop_id("940GZZLUEUS"));
        assert!(is_stop_id("940GZZCRWMB"));
        assert!(!is_stop_id("940gzzluesu"));
        assert!(!is_stop_id("EUS"));
        assert!(!is_stop_id("940G ZZLU"));
        assert!(!is_stop_id("940GZZLUEUS940GZZLUEUS"));
    }

    #[test]
    fn a_row_round_trips_and_tolerates_missing_optionals() {
        let row: TflStop =
            serde_json::from_str(r#"{"id":"940GZZLUXXA","name":"Testbury","modes":["tube"]}"#)
                .unwrap();
        assert!(row.lines.is_empty());
        assert_eq!(row.hub_code, None);
        assert!(row.crs.is_empty());
        let json = serde_json::to_string(&row).unwrap();
        assert_eq!(serde_json::from_str::<TflStop>(&json).unwrap(), row);
    }
}
