//! Location types and display names for TIPLOCs that are not (or not only)
//! National Rail stations: bus stops, ferry terminals, junctions, sidings and
//! timing points.
//!
//! `schedule-reference` derives a [`LocationType`] and a display name for
//! every CIF `TI` record and publishes them as `tiploc_locations`; `api`
//! uses the same functions for its CORPUS fallback (a TIPLOC with no
//! `tiploc_locations` row). Both sides must agree, so the rules live here.
//! See docs/superpowers/specs/2026-10-06-tiploc-locations-design.md.
//!
//! Everything here is pure and deterministic: the same raw name always
//! gives the same output, whatever the order the records arrive in.

use serde::{Deserialize, Serialize};

/// What kind of place a TIPLOC is, as far as a passenger cares.
///
/// Derived by `schedule-reference` from, in order: whether the TIPLOC is a
/// station (a STANOX and a bookable CRS), which kinds of service call there
/// (CIF train status: bus or ship services only), then name heuristics
/// ([`classify_name`]), then whether rail services only ever pass it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocationType {
    Station,
    BusStop,
    FerryTerminal,
    Junction,
    Siding,
    PassingPoint,
    Other,
}

impl LocationType {
    pub const ALL: [LocationType; 7] = [
        LocationType::Station,
        LocationType::BusStop,
        LocationType::FerryTerminal,
        LocationType::Junction,
        LocationType::Siding,
        LocationType::PassingPoint,
        LocationType::Other,
    ];

    /// The stored and wire form: `station`, `bus_stop`, ... (the database's
    /// `CHECK` constraint lists the same values).
    pub fn as_str(self) -> &'static str {
        match self {
            LocationType::Station => "station",
            LocationType::BusStop => "bus_stop",
            LocationType::FerryTerminal => "ferry_terminal",
            LocationType::Junction => "junction",
            LocationType::Siding => "siding",
            LocationType::PassingPoint => "passing_point",
            LocationType::Other => "other",
        }
    }

    /// The inverse of [`Self::as_str`].
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == value)
    }

    /// A stop served by road or water rather than rail: a journey-planner
    /// end point, and the kinds a change to or from costs extra time.
    pub fn is_road_or_water(self) -> bool {
        matches!(self, LocationType::BusStop | LocationType::FerryTerminal)
    }

    /// The short suffix the planner's location search shows: `(bus)` or
    /// `(ferry)`. `None` for every other kind.
    pub fn search_suffix(self) -> Option<&'static str> {
        match self {
            LocationType::BusStop => Some("(bus)"),
            LocationType::FerryTerminal => Some("(ferry)"),
            _ => None,
        }
    }
}

/// Upper-cased words of `raw`, split on anything that is not a letter or
/// digit. `"LEIGH (FLEUR-DE-LIS P.H.)"` -> `LEIGH FLEUR DE LIS P H`.
fn words(raw: &str) -> Vec<String> {
    raw.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(str::to_ascii_uppercase)
        .collect()
}

const BUS_WORDS: &[&str] = &["BUS", "BUSES", "COACH", "BUSTRAM"];
const FERRY_WORDS: &[&str] = &[
    "FERRY",
    "FERRYPORT",
    "PIER",
    "QUAY",
    "HARBOUR",
    "PORT",
    "EUROPORT",
    "SLIP",
    "SLIPWAY",
];
const JUNCTION_WORDS: &[&str] = &["JN", "JCN", "JNC", "JUNC", "JUNCTION", "JNS"];
const SIDING_WORDS: &[&str] = &[
    "SIDINGS", "SIDING", "SDGS", "SDG", "CS", "DEPOT", "DEP", "DPT", "TMD", "LMD", "CARMD",
    "TRSMD", "EMUD", "DMUD", "MPD", "YARD", "YD", "GOODS", "FREIGHT", "FLT", "DOCK", "DOCKS",
    "WORKS", "QUARRY",
];
const PASSING_WORDS: &[&str] = &[
    "SIGNAL",
    "SIG",
    "SIGS",
    "LOOP",
    "LOOPS",
    "LP",
    "SB",
    "GF",
    "LC",
    "XOVER",
    "XOVERS",
    "CROSSOVER",
    "TUNNEL",
    "VIADUCT",
    "SUMMIT",
    "BOX",
];

/// The [`LocationType`] a name alone suggests, or `None` when it says
/// nothing. Whole words only (`BUSHEY` is not a bus stop), first match in
/// this order: bus, ferry (`LANDING STAGE` included), junction, siding,
/// passing point.
///
/// Never returns [`LocationType::Station`]: whether a TIPLOC is a station is
/// a data question (a STANOX and a bookable CRS), not a naming one, and
/// callers decide it first -- `CLAPHAM JUNCTION` and `PORTSMOUTH HARBOUR`
/// are stations.
pub fn classify_name(raw: &str) -> Option<LocationType> {
    classify_words(raw, true)
}

/// [`classify_name`] without the bus and ferry rules: for a TIPLOC rail
/// services call at or pass, where `IMMINGHAM PORT` is a freight terminal,
/// not a ferry terminal.
pub fn classify_rail_name(raw: &str) -> Option<LocationType> {
    classify_words(raw, false)
}

fn classify_words(raw: &str, road_or_water: bool) -> Option<LocationType> {
    let words = words(raw);
    let has = |list: &[&str]| words.iter().any(|word| list.contains(&word.as_str()));
    let landing_stage = words
        .windows(2)
        .any(|pair| pair[0] == "LANDING" && pair[1] == "STAGE");
    if road_or_water && has(BUS_WORDS) {
        Some(LocationType::BusStop)
    } else if road_or_water && (has(FERRY_WORDS) || landing_stage) {
        Some(LocationType::FerryTerminal)
    } else if has(JUNCTION_WORDS) {
        Some(LocationType::Junction)
    } else if has(SIDING_WORDS) {
        Some(LocationType::Siding)
    } else if has(PASSING_WORDS) {
        Some(LocationType::PassingPoint)
    } else {
        None
    }
}

/// Words kept lower case inside a name (never as its first word, or the
/// first word inside brackets): `Stow-on-the-Wold`, `Isle of Man`,
/// `Ashchurch for Tewkesbury`, `Pen-y-Bont`.
const SMALL_WORDS: &[&str] = &[
    "OF", "THE", "ON", "IN", "UPON", "UNDER", "AND", "BY", "AT", "FOR", "DE", "LA", "LE", "Y",
];

/// Common abbreviations with a conventional mixed-case form. Checked before
/// the "no vowels means an acronym" rule, which would otherwise keep them
/// upper case.
const ABBREVIATIONS: &[(&str, &str)] = &[
    ("ST", "St"),
    ("MT", "Mt"),
    ("RD", "Rd"),
    ("JN", "Jn"),
    ("JCN", "Jn"),
    ("SQ", "Sq"),
    ("PK", "Pk"),
    ("LN", "Ln"),
    ("STN", "Stn"),
    ("GDNS", "Gdns"),
    ("CT", "Ct"),
    ("PL", "Pl"),
];

/// Known acronyms that contain a vowel, so the no-vowel rule misses them.
const ACRONYMS: &[&str] = &[
    "LUL", "DLR", "NEC", "IOW", "IOM", "UK", "NHS", "RAF", "YHA", "EMU", "DMU",
];

/// Cases one alphabetic-or-numeric word (no separators). `first` is whether
/// it starts the name, or a bracketed or slash-separated part of it.
fn case_word(word: &str, first: bool) -> String {
    let upper = word.to_ascii_uppercase();
    if upper.chars().any(|c| c.is_ascii_digit()) {
        return upper;
    }
    if let Some((_, cased)) = ABBREVIATIONS.iter().find(|(raw, _)| *raw == upper) {
        return (*cased).to_string();
    }
    if ACRONYMS.contains(&upper.as_str()) {
        return upper;
    }
    if !first && SMALL_WORDS.contains(&upper.as_str()) {
        return upper.to_ascii_lowercase();
    }
    let letters: Vec<char> = upper.chars().filter(char::is_ascii_alphabetic).collect();
    let has_vowel = letters.iter().any(|c| "AEIOUYW".contains(*c));
    if letters.len() == 1 || (!has_vowel && letters.len() <= 4) {
        // `D R`, `GN`, `TMD`, `CS`: initials and acronyms.
        return upper;
    }
    let mut out = String::with_capacity(upper.len());
    let lower = upper.to_ascii_lowercase();
    let mut capitalise_next = true;
    let mut previous: Option<char> = None;
    for (index, c) in lower.chars().enumerate() {
        if capitalise_next && c.is_ascii_alphabetic() {
            out.push(c.to_ascii_uppercase());
            capitalise_next = false;
        } else {
            out.push(c);
        }
        // `O'Brien`, `D'Arcy`: a one-letter prefix before an apostrophe.
        if c == '\'' && index == 1 {
            capitalise_next = true;
        }
        // `McDonald`: `Mc` followed by at least three letters.
        if index == 1 && previous == Some('m') && c == 'c' && letters.len() > 4 {
            capitalise_next = true;
        }
        previous = Some(c);
    }
    out
}

/// Title-cases an upper-case CIF/MSN/CORPUS location name.
///
/// Rules, in order, per word (words split on spaces, `-`, `/`, brackets,
/// `&` and `,`, which are all kept as they are):
///
/// - a word with a digit is kept as is (`A62`, `T5`, `10`);
/// - a dotted abbreviation keeps its capitals (`I.O.W.`, `P.H.`), except
///   `ST.`, which becomes `St`;
/// - `ST`, `MT`, `RD`, `JN`, `SQ`, ... get their usual form (`St`, `Mt`);
/// - a few known acronyms stay upper case (`LUL`, `NEC`, `IOW`);
/// - small words (`of`, `the`, `on`, `and`, `y`, ...) are lower case unless
///   they start the name or a bracketed part (`Isle of Man`,
///   `Stow-on-the-Wold`);
/// - a single letter, or a word of up to four letters with no vowel
///   (counting `W` and `Y`, for Welsh), stays upper case (`D R`, `TMD`);
/// - `CO-OP` is `Co-op`;
/// - everything else is capitalised (`Mary's`, `O'Brien`, `McDonald`).
///
/// Whitespace is collapsed to single spaces. Deterministic and idempotent
/// on its own output's upper-cased form.
pub fn title_case(raw: &str) -> String {
    let collapsed = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    let upper = collapsed.to_ascii_uppercase().replace("CO-OP", "\u{1}");
    let mut out = String::with_capacity(upper.len());
    let mut word = String::new();
    let mut first = true;
    let flush = |word: &mut String, out: &mut String, first: &mut bool| {
        if word.is_empty() {
            return;
        }
        if word.contains('.') {
            let bare = word.trim_end_matches('.');
            if bare == "ST" {
                out.push_str("St");
            } else if bare.contains('.') || word.ends_with('.') && bare.len() == 1 {
                out.push_str(word);
            } else {
                // `RD.` and the like: case the word, drop nothing.
                let dots = &word[bare.len()..];
                out.push_str(&case_word(bare, *first));
                out.push_str(dots);
            }
        } else {
            out.push_str(&case_word(word, *first));
        }
        *first = false;
        word.clear();
    };
    for c in upper.chars() {
        match c {
            '\u{1}' => {
                flush(&mut word, &mut out, &mut first);
                out.push_str("Co-op");
                first = false;
            }
            c if c.is_ascii_alphanumeric() || c == '\'' || c == '.' => word.push(c),
            '(' | '/' => {
                flush(&mut word, &mut out, &mut first);
                out.push(c);
                first = true;
            }
            _ => {
                flush(&mut word, &mut out, &mut first);
                out.push(c);
            }
        }
    }
    flush(&mut word, &mut out, &mut first);
    out
}

/// Closes a bracket a fixed-width field cut off: CIF `TI` names are 26
/// characters, so `EAST MIDLANDS AIRPORT (BUS)` arrives as
/// `EAST MIDLANDS AIRPORT (BUS`.
fn close_brackets(raw: &str) -> String {
    let open = raw.matches('(').count();
    let close = raw.matches(')').count();
    let mut out = raw.trim().to_string();
    for _ in close..open {
        out.push(')');
    }
    out
}

/// Bus markers stripped from the end of a bus stop's name, longest first,
/// and whether each one names a bus station rather than a stop.
const BUS_STATION_SUFFIXES: &[&str] = &[
    "(BUS STATION)",
    "BUS STATION",
    "(BUS STN)",
    "BUS STN",
    "BUS STANCE",
    "BUS INTERCHANGE",
    "COACH STATION",
    "(COACH)",
    "COACH",
];
const BUS_STOP_SUFFIXES: &[&str] = &["(BUS STOP)", "BUS STOP", "BUSES ONLY", "(BUS)", "BUS", "BS"];

/// Strips one trailing whole-word marker from `upper` (already upper case),
/// returning the rest when something was stripped and something is left.
fn strip_suffix_word<'a>(upper: &'a str, suffixes: &[&str]) -> Option<&'a str> {
    for suffix in suffixes {
        let Some(rest) = upper.strip_suffix(suffix) else {
            continue;
        };
        let starts_word = suffix.starts_with('(') || rest.is_empty() || rest.ends_with([' ', '-']);
        let rest = rest.trim_end_matches([' ', '-', ',']);
        if starts_word && !rest.is_empty() {
            return Some(rest);
        }
    }
    None
}

/// A location's place name and display name, derived from its raw name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationName {
    /// The title-cased place name, with any bus marker stripped:
    /// `Heathrow Terminal 3`, `Keswick`, `Brodick`, `Abbeyhill Jn`.
    pub name: String,
    /// What the passenger-facing pages show: [`Self::name`] plus the kind of
    /// stop for a bus stop or ferry terminal (`Heathrow Terminal 3 (bus
    /// stop)`, `Keswick (bus station)`, `Brodick (ferry terminal)`), and
    /// [`Self::name`] alone for everything else.
    pub display_name: String,
}

/// Derives the [`LocationName`] for a raw upper-case name of a location of
/// `location_type`. An empty or whitespace-only `raw` gives empty names.
///
/// For a bus stop, one trailing bus marker is removed from the place name
/// and turned into the suffix: `(BUS STATION)`/`BUS STATION`/`BUS STN`/
/// `COACH`... give `(bus station)`, `(BUS)`/`BUS`/`BUS STOP`/`BUSES ONLY`
/// or nothing give `(bus stop)`. A ferry terminal's name is kept whole and
/// gets `(ferry terminal)`.
pub fn location_name(raw: &str, location_type: LocationType) -> LocationName {
    let closed = close_brackets(raw);
    let upper = closed.to_ascii_uppercase();
    let (place, suffix) = match location_type {
        LocationType::BusStop => {
            if let Some(rest) = strip_suffix_word(&upper, BUS_STATION_SUFFIXES) {
                (rest.to_string(), Some("(bus station)"))
            } else if let Some(rest) = strip_suffix_word(&upper, BUS_STOP_SUFFIXES) {
                (rest.to_string(), Some("(bus stop)"))
            } else {
                (upper.clone(), Some("(bus stop)"))
            }
        }
        LocationType::FerryTerminal => (upper.clone(), Some("(ferry terminal)")),
        _ => (upper.clone(), None),
    };
    let name = title_case(&place);
    let display_name = match suffix {
        Some(suffix) if !name.is_empty() => format!("{name} {suffix}"),
        _ => name.clone(),
    };
    LocationName { name, display_name }
}

/// The planner search's label for a bus stop or ferry terminal: its place
/// name plus [`LocationType::search_suffix`] (`Keswick (bus)`,
/// `Brodick (ferry)`); `name` unchanged for any other kind.
pub fn search_label(name: &str, location_type: LocationType) -> String {
    match location_type.search_suffix() {
        Some(suffix) => format!("{name} {suffix}"),
        None => name.to_string(),
    }
}

/// The planner's identifier for a location that has no CRS of its own:
/// `tiploc:` and the upper-case TIPLOC. See
/// docs/superpowers/specs/2026-10-06-tiploc-locations-design.md
/// ("Identifier scheme") for why a TIPLOC rather than an MSN code.
pub const TIPLOC_CODE_PREFIX: &str = "tiploc:";

/// `tiploc:SANWBUS` for `SANWBUS` (trimmed and upper-cased).
pub fn tiploc_code(tiploc: &str) -> String {
    format!("{TIPLOC_CODE_PREFIX}{}", tiploc.trim().to_ascii_uppercase())
}

/// The TIPLOC in a `tiploc:` code (any case of the prefix), upper-cased;
/// `None` for anything else, a CRS included.
pub fn tiploc_from_code(code: &str) -> Option<String> {
    let code = code.trim();
    let prefix_len = TIPLOC_CODE_PREFIX.len();
    if code.len() <= prefix_len
        || !code.is_char_boundary(prefix_len)
        || !code[..prefix_len].eq_ignore_ascii_case(TIPLOC_CODE_PREFIX)
    {
        return None;
    }
    let tiploc = code[prefix_len..].trim().to_ascii_uppercase();
    (!tiploc.is_empty() && tiploc.len() <= 7 && tiploc.chars().all(|c| c.is_ascii_alphanumeric()))
        .then_some(tiploc)
}

/// Normalises a station-or-location code a caller typed: a `tiploc:` code
/// to its canonical `tiploc:UPPER` form, anything else upper-cased and
/// trimmed (a CRS). A malformed `tiploc:` code is upper-cased whole, so it
/// matches nothing rather than being mistaken for a CRS.
pub fn normalize_location_code(code: &str) -> String {
    tiploc_from_code(code).map_or_else(
        || code.trim().to_ascii_uppercase(),
        |tiploc| tiploc_code(&tiploc),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn location_types_round_trip_through_their_stored_form() {
        for kind in LocationType::ALL {
            assert_eq!(LocationType::parse(kind.as_str()), Some(kind));
            assert_eq!(
                serde_json::to_value(kind).unwrap(),
                serde_json::Value::String(kind.as_str().to_string())
            );
        }
        assert_eq!(LocationType::parse("tram_stop"), None);
    }

    #[test]
    fn title_case_handles_small_words_hyphens_and_brackets() {
        for (raw, expected) in [
            ("ST ANDREWS BUS STATION", "St Andrews Bus Station"),
            ("STOW-ON-THE-WOLD", "Stow-on-the-Wold"),
            ("BOURTON-ON-THE-WATER (BUS)", "Bourton-on-the-Water (Bus)"),
            ("DOUGLAS (ISLE OF MAN)", "Douglas (Isle of Man)"),
            ("ASHCHURCH FOR TEWKESBURY", "Ashchurch for Tewkesbury"),
            ("LEIGH (FLEUR-DE-LIS P.H.)", "Leigh (Fleur-de-Lis P.H.)"),
            ("SWANSCOMBE-GEORGE & DRAGON", "Swanscombe-George & Dragon"),
            ("STONEHOUSE HIGH ST/BATH RD", "Stonehouse High St/Bath Rd"),
            ("WEDGWOOD, WEDGWOOD LANE", "Wedgwood, Wedgwood Lane"),
            ("THE HIVE", "The Hive"),
            ("PEN-Y-BONT", "Pen-y-Bont"),
        ] {
            assert_eq!(title_case(raw), expected, "{raw}");
        }
    }

    #[test]
    fn title_case_keeps_numbers_initials_and_acronyms() {
        for (raw, expected) in [
            ("HEATHROW TERMINAL 3 BUS", "Heathrow Terminal 3 Bus"),
            ("MARYLEBONE 10 SIGNAL", "Marylebone 10 Signal"),
            ("MARSDEN BUS A62", "Marsden Bus A62"),
            ("LONDON ROAD D R", "London Road D R"),
            ("KINGS LYNN BUS STATION GN", "Kings Lynn Bus Station GN"),
            ("PADDINGTON BAKERLOO LUL", "Paddington Bakerloo LUL"),
            ("YARMOUTH (I.O.W.)", "Yarmouth (I.O.W.)"),
            ("OLD OAK COMMON TMD", "Old Oak Common TMD"),
            ("WESTBOURNE PARK CS", "Westbourne Park CS"),
            ("CWM BARGOED", "Cwm Bargoed"),
        ] {
            assert_eq!(title_case(raw), expected, "{raw}");
        }
    }

    #[test]
    fn title_case_handles_abbreviations_apostrophes_and_prefixes() {
        for (raw, expected) in [
            ("ST. MARYS QUAY", "St Marys Quay"),
            ("ST MARY'S QUAY", "St Mary's Quay"),
            ("O'BRIEN STREET", "O'Brien Street"),
            ("MCDONALD ROAD", "McDonald Road"),
            ("ABBEYHILL JN", "Abbeyhill Jn"),
            ("PENYGROES CO-OP", "Penygroes Co-op"),
            ("CO-OP CORNER", "Co-op Corner"),
            ("  READING   BUS ", "Reading Bus"),
            ("Bodmin Mount Folly (Bus)", "Bodmin Mount Folly (Bus)"),
        ] {
            assert_eq!(title_case(raw), expected, "{raw}");
        }
    }

    #[test]
    fn title_case_is_idempotent() {
        for raw in [
            "STOW-ON-THE-WOLD",
            "LEIGH (FLEUR-DE-LIS P.H.)",
            "ST MARY'S QUAY",
            "PENYGROES CO-OP",
            "LONDON ROAD D R",
        ] {
            let once = title_case(raw);
            assert_eq!(title_case(&once), once, "{raw}");
        }
    }

    /// Real CIF `TI` names (RJTTF980MCA.txt, 2026-10-05) and the display
    /// names they get as bus stops.
    #[test]
    fn bus_stop_names_lose_their_marker_and_gain_a_suffix() {
        for (raw, name, display) in [
            (
                "HEATHROW TERMINAL 3 BUS",
                "Heathrow Terminal 3",
                "Heathrow Terminal 3 (bus stop)",
            ),
            ("KESWICK (BUS STATION)", "Keswick", "Keswick (bus station)"),
            (
                "ST ANDREWS BUS STATION",
                "St Andrews",
                "St Andrews (bus station)",
            ),
            ("READING BUS", "Reading", "Reading (bus stop)"),
            ("BUDE (BUS)", "Bude", "Bude (bus stop)"),
            (
                "EAST MIDLANDS AIRPORT (BUS",
                "East Midlands Airport",
                "East Midlands Airport (bus stop)",
            ),
            (
                "EDINBURGH AIRPORT",
                "Edinburgh Airport",
                "Edinburgh Airport (bus stop)",
            ),
            ("BRECON BUS INTERCHANGE", "Brecon", "Brecon (bus station)"),
            ("WISBECH (COACH)", "Wisbech", "Wisbech (bus station)"),
            (
                "HEATHROW TERMINAL 5 BUSES ONLY",
                "Heathrow Terminal 5",
                "Heathrow Terminal 5 (bus stop)",
            ),
            (
                "BARNSTAPLE STATION BS",
                "Barnstaple Station",
                "Barnstaple Station (bus stop)",
            ),
            ("BUSBY", "Busby", "Busby (bus stop)"),
            ("BUS", "Bus", "Bus (bus stop)"),
        ] {
            let derived = location_name(raw, LocationType::BusStop);
            assert_eq!(derived.name, name, "{raw}");
            assert_eq!(derived.display_name, display, "{raw}");
        }
    }

    #[test]
    fn ferry_and_other_names_are_kept_whole() {
        assert_eq!(
            location_name("BRODICK", LocationType::FerryTerminal).display_name,
            "Brodick (ferry terminal)"
        );
        assert_eq!(
            location_name("GOUROCK PIER", LocationType::FerryTerminal).name,
            "Gourock Pier"
        );
        let signal = location_name("MARYLEBONE 10 SIGNAL", LocationType::PassingPoint);
        assert_eq!(signal.name, "Marylebone 10 Signal");
        assert_eq!(signal.display_name, "Marylebone 10 Signal");
        assert_eq!(
            location_name("ERITH LOOP", LocationType::PassingPoint).display_name,
            "Erith Loop"
        );
        assert_eq!(
            location_name("  ", LocationType::BusStop),
            LocationName {
                name: String::new(),
                display_name: String::new()
            }
        );
    }

    #[test]
    fn classify_name_uses_whole_words_in_priority_order() {
        for (raw, expected) in [
            ("HEATHROW TERMINAL 3 BUS", Some(LocationType::BusStop)),
            ("BOURTON-ON-THE-WATER (BUS)", Some(LocationType::BusStop)),
            ("SWAFFHAM (COACH)", Some(LocationType::BusStop)),
            ("BUSHEY", None),
            ("CAMBUSLANG", None),
            ("GOUROCK PIER", Some(LocationType::FerryTerminal)),
            ("DUBLIN FERRYPORT", Some(LocationType::FerryTerminal)),
            ("LIVERPOOL LANDING STAGE", Some(LocationType::FerryTerminal)),
            ("PENZANCE QUAY", Some(LocationType::FerryTerminal)),
            ("ABBEYHILL JN", Some(LocationType::Junction)),
            ("WATERLOO WINDSOR JN", Some(LocationType::Junction)),
            ("WESTBOURNE PARK CS", Some(LocationType::Siding)),
            ("LONDON ROAD DEPOT", Some(LocationType::Siding)),
            ("MARYLEBONE 10 SIGNAL", Some(LocationType::PassingPoint)),
            ("ERITH LOOP", Some(LocationType::PassingPoint)),
            (
                "VIRGINIA WATER SIGNAL 2217",
                Some(LocationType::PassingPoint),
            ),
            ("CANNA", None),
            ("PRINCES ST GARDENS", None),
        ] {
            assert_eq!(classify_name(raw), expected, "{raw}");
        }
        assert_eq!(classify_rail_name("IMMINGHAM PORT"), None);
        assert_eq!(classify_rail_name("HEATHROW TERMINAL 3 BUS"), None);
        assert_eq!(
            classify_rail_name("FELIXSTOWE NORTH FLT"),
            Some(LocationType::Siding)
        );
    }

    #[test]
    fn search_labels_name_the_mode() {
        assert_eq!(
            search_label("Keswick", LocationType::BusStop),
            "Keswick (bus)"
        );
        assert_eq!(
            search_label("Brodick", LocationType::FerryTerminal),
            "Brodick (ferry)"
        );
        assert_eq!(search_label("York", LocationType::Station), "York");
    }

    #[test]
    fn tiploc_codes_round_trip_and_never_match_a_crs() {
        assert_eq!(tiploc_code(" sanwbus "), "tiploc:SANWBUS");
        assert_eq!(
            tiploc_from_code("tiploc:SANWBUS").as_deref(),
            Some("SANWBUS")
        );
        assert_eq!(
            tiploc_from_code("TIPLOC:sanwbus").as_deref(),
            Some("SANWBUS")
        );
        assert_eq!(tiploc_from_code("SAO"), None);
        assert_eq!(tiploc_from_code("tiploc:"), None);
        assert_eq!(tiploc_from_code("tiploc:TOOLONGX"), None);
        assert_eq!(tiploc_from_code("tiploc:A-B"), None);
        assert_eq!(normalize_location_code(" Tiploc:keswick"), "tiploc:KESWICK");
        assert_eq!(normalize_location_code(" edb "), "EDB");
        assert_eq!(normalize_location_code("tiploc:a-b"), "TIPLOC:A-B");
    }
}
