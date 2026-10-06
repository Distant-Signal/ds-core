//! Places named in a Knowledgebase incident's prose, resolved to CRS codes.
//!
//! RDM's Knowledgebase feed has no structured station field
//! (`poller-incidents` leaves `affected_stations` empty), so until
//! 2026-10-06 the matcher's station tier never fired and every incident
//! without a line keyword was shown "operator-wide" on every line of its
//! operator. A 30-day production study (2026-10-06) found that about 98% of
//! unplanned incidents are local and name their stations in the summary
//! ("Disruption between Purley and Gatwick Airport"), and that resolving
//! those names against the `stations` reference table, scoped to the
//! operator's own catalogue lines, placed 603 of 603 resolvable local
//! incidents on at least one line. This module is that resolver.
//!
//! [`StationGazetteer`] holds every station's normalised name plus a few
//! derived and hand-written aliases. [`StationGazetteer::stations_in`] scans
//! a text for the longest station names it contains, guarding against the
//! usual false hits: lower-case prose ("reading the notice"), station names
//! that are also common words ("Reading the timetable", "from March"), and
//! line names ("Brighton Main Line", "Hull Trains").
//!
//! [`has_network_scope_marker`] recognises the text of a genuinely
//! network-wide notice (industrial action, "across the ... network", a
//! reduced timetable). [`effective_operator`] maps operator codes RDM uses
//! that the catalogue spells differently. Both feed the matching rule in
//! [`crate::matcher`]: see that module and
//! docs/superpowers/specs/2026-10-06-incident-line-evidence-design.md.

use std::collections::{HashMap, HashSet};

/// RDM operator codes the catalogue spells differently (2026-10-06): West
/// Midlands Trains' two brands are separate codes in the Knowledgebase TOC
/// list (`LN` London Northwestern Railway, `WM` West Midlands Railway), but
/// every `lines/*.toml` file for them uses the combined `LM`. Without this,
/// 25 incidents in 30 days matched no line at all.
const OPERATOR_ALIASES: [(&str, &str); 2] = [("LN", "LM"), ("WM", "LM")];

/// "National Rail" in RDM's TOC list: an incident not attributed to any one
/// operator. Its lines can only come from its text, across every operator.
pub const NATIONAL_RAIL_OPERATOR: &str = "ZN";

/// The catalogue's code for an RDM operator code.
pub fn effective_operator(code: &str) -> &str {
    let code = code.trim();
    OPERATOR_ALIASES
        .iter()
        .find(|(from, _)| from.eq_ignore_ascii_case(code))
        .map_or(code, |(_, to)| to)
}

/// Hand-written aliases, `(CRS, name as written in incident text)`, for
/// spellings that neither the reference name nor the derived aliases
/// (below) cover. Kept short on purpose: each one was seen in production
/// text, or is the everyday form of a terminus name. An alias listed for
/// several codes is one place that may mean any of them ("Heathrow
/// Airport" is all three Heathrow stations).
///
/// City names (2026-10-06 misses study): RDM writes "between Birmingham and
/// Bristol" and "Manchester to Glasgow", and those cities' stations all
/// carry a suffix in the reference table. Each maps to the city's main
/// terminal(s), the big-city terminals of the catalogue; a longer name in
/// the text still wins ("Birmingham International", "Manchester Airport").
const EXTRA_ALIASES: [(&str, &str); 33] = [
    ("LVJ", "James Street"),
    ("LIV", "Lime Street"),
    ("STP", "London St Pancras"),
    ("STP", "St Pancras"),
    ("HUL", "Hull Paragon"),
    ("MKC", "Milton Keynes"),
    // 7CE5A87E: "Heathrow Terminals", "Heathrow Airport Terminal 5".
    ("HXX", "Heathrow Terminals"),
    ("HAF", "Heathrow Terminals"),
    ("HWV", "Heathrow Terminals"),
    ("HXX", "Heathrow Airport"),
    ("HAF", "Heathrow Airport"),
    ("HWV", "Heathrow Airport"),
    ("HXX", "Heathrow"),
    ("HAF", "Heathrow"),
    ("HWV", "Heathrow"),
    ("HXX", "Heathrow Airport Terminals 2 & 3"),
    ("HXX", "Heathrow Terminals 2 and 3"),
    ("HAF", "Heathrow Airport Terminal 4"),
    ("HWV", "Heathrow Airport Terminal 5"),
    ("BHM", "Birmingham"),
    ("MAN", "Manchester"),
    ("GLC", "Glasgow"),
    ("GLQ", "Glasgow"),
    ("BRI", "Bristol"),
    ("CDF", "Cardiff"),
    ("LIV", "Liverpool"),
    ("EXD", "Exeter"),
    ("SOU", "Southampton"),
    ("BTH", "Bath"),
    ("EDB", "Edinburgh Waverley"),
    ("BDI", "Bradford"),
    ("BDQ", "Bradford"),
    ("WKF", "Wakefield"),
];

/// The London termini a bare "London" stands for when it is the end of an
/// explicit section ("between London and Stevenage"): one place, which on
/// each line is that line's own London terminus.
const LONDON_TERMINI: [&str; 15] = [
    "PAD", "MYB", "EUS", "STP", "KGX", "LST", "FST", "CST", "CHX", "LBG", "WAT", "VIC", "BFR",
    "WAE", "MOG",
];

/// Trailing county words some reference names carry without parentheses
/// ("Seaford Sussex"), dropped to give an alias ("Seaford").
const COUNTY_QUALIFIERS: [&str; 24] = [
    "sussex",
    "kent",
    "surrey",
    "essex",
    "yorks",
    "yorkshire",
    "lancs",
    "herts",
    "berks",
    "bucks",
    "glos",
    "wilts",
    "hants",
    "oxon",
    "devon",
    "cornwall",
    "cumbria",
    "cheshire",
    "staffs",
    "salop",
    "notts",
    "derbys",
    "leics",
    "northants",
];

/// One-word "London X" names whose bare form is unambiguous in rail prose
/// ("trains to Victoria"). "London Bridge", "London Fields" and "London
/// Road" stay full-name only: "bridge", "fields" and "road" are words.
const LONDON_SHORT_FORMS: [&str; 6] = [
    "waterloo",
    "victoria",
    "euston",
    "paddington",
    "marylebone",
    "blackfriars",
];

/// Station names that are also everyday words. Such a name counts only in
/// a place context: right after "between", "and", "at", "to", "from",
/// "via", "near", "serving", "towards", "of", "for" or a "/", and not next
/// to a number ("from March 2027", "3 March"). This is what keeps "Reading
/// the timetable" at the start of a sentence from resolving to Reading.
const COMMON_WORD_NAMES: [&str; 16] = [
    "reading", "march", "hope", "battle", "deal", "wool", "rye", "fleet", "ford", "stone", "ash",
    "hale", "ware", "lake", "street", "church",
];

const PLACE_CONTEXT_WORDS: [&str; 12] = [
    "between", "and", "at", "to", "from", "via", "near", "serving", "towards", "of", "for", "or",
];

/// Words before a place that put the disruption at it ("a signalling fault
/// at Lewisham", "near Gloucester"); "<place> area" does too. See
/// [`PlaceMention::localised`].
const LOCALISING_WORDS: [&str; 4] = ["at", "near", "outside", "through"];

/// One place named in a text: every CRS code the name may mean (in scope),
/// and whether the text puts the disruption at it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaceMention {
    /// The station(s) the name means, in scope. Several for an alias of
    /// more than one station ("Heathrow Airport"), a name several stations
    /// share once their qualifiers are dropped ("Bramley"), or a bare
    /// "London" section end (every London terminus).
    pub crs: Vec<String>,
    /// "at X", "near X", "outside X", "through X" or "the X area": the
    /// disruption is at this place, not merely somewhere on a section
    /// ending at it.
    pub localised: bool,
}

/// Lowercase; apostrophes dropped ("Shepherd's" = "Shepherds"); a
/// parenthesised qualifier dropped ("Richmond (London)"); "&" read as "and";
/// every other non-alphanumeric a space; spaces collapsed. The same
/// normalisation the "no trains between" parser always used (moved here
/// from `aggregator::no_trains` on 2026-10-06 so both share it).
pub fn normalise_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut depth = 0u32;
    for c in name.chars() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ if depth > 0 => {}
            '\'' | '\u{2019}' => {}
            '&' => out.push_str(" and "),
            c if c.is_alphanumeric() => out.extend(c.to_lowercase()),
            _ => out.push(' '),
        }
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Every station name (and alias) in the `stations` reference table,
/// normalised, for resolving places named in incident text.
#[derive(Debug, Clone, Default)]
pub struct StationGazetteer {
    /// Normalised name or alias -> every CRS it may mean (several stations
    /// share a bare name once their qualifiers are dropped: "Bramley").
    by_name: HashMap<String, Vec<String>>,
    /// CRS -> its normalised names, the reference name first.
    names: HashMap<String, Vec<String>>,
    /// The most words in any name, bounding the longest-match scan.
    max_words: usize,
}

impl StationGazetteer {
    /// Builds the gazetteer from `(crs, reference name)` pairs, adding the
    /// derived aliases (the name without a trailing county word, a London
    /// terminus without "London") and [`EXTRA_ALIASES`] for CRS codes
    /// present in `names`.
    pub fn new<I, C, N>(names: I) -> Self
    where
        I: IntoIterator<Item = (C, N)>,
        C: AsRef<str>,
        N: AsRef<str>,
    {
        let mut gazetteer = Self::default();
        for (crs, name) in names {
            let crs = crs.as_ref().trim().to_uppercase();
            let normalised = normalise_name(name.as_ref());
            if crs.is_empty() || normalised.is_empty() {
                continue;
            }
            gazetteer.add(&crs, normalised.clone());
            for alias in derived_aliases(&normalised) {
                gazetteer.add(&crs, alias);
            }
        }
        for (crs, alias) in EXTRA_ALIASES {
            if gazetteer.names.contains_key(crs) {
                gazetteer.add(crs, normalise_name(alias));
            }
        }
        gazetteer
    }

    fn add(&mut self, crs: &str, name: String) {
        let names = self.names.entry(crs.to_string()).or_default();
        if names.contains(&name) {
            return;
        }
        names.push(name.clone());
        self.max_words = self.max_words.max(name.split(' ').count());
        let crs_list = self.by_name.entry(name).or_default();
        if !crs_list.iter().any(|c| c == crs) {
            crs_list.push(crs.to_string());
        }
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    /// `crs`'s normalised names, the reference name first; empty for an
    /// unknown code.
    pub fn names_of(&self, crs: &str) -> &[String] {
        self.names
            .get(&crs.trim().to_uppercase())
            .map_or(&[], Vec::as_slice)
    }

    /// The CRS codes of every station `text` names, in order of first
    /// mention, keeping only codes `in_scope` accepts: [`Self::places_in`]
    /// flattened, as for a description (no bare "London").
    pub fn stations_in(&self, text: &str, in_scope: impl Fn(&str) -> bool) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for place in self.places_in(text, in_scope, false) {
            for crs in place.crs {
                if !out.contains(&crs) {
                    out.push(crs);
                }
            }
        }
        out
    }

    /// Every place `text` names, in order, one [`PlaceMention`] per mention
    /// with at least one code `in_scope` accepts (a place named twice is
    /// listed twice).
    ///
    /// Longest match first: "Purley Oaks" is Purley Oaks, not Purley, and
    /// a longer name consumes its words even when its station is out of
    /// scope ("Reading West" never falls back to Reading). A name must start
    /// with a capital letter in `text` (station names are proper nouns in
    /// RDM's prose), a common-word name needs a place context
    /// ([`COMMON_WORD_NAMES`]), and a name followed by "line", "lines",
    /// "branch", "route", "main line" or "Trains" is a line or operator
    /// name, not a station.
    ///
    /// `headline` is true for an incident's summary: only there is a bare
    /// "London" read, and only as the end of an explicit section ("between
    /// London and Stevenage", "between Stevenage and London"), as every
    /// London terminus in scope (2026-10-06 decision 4): on each line, that
    /// line's own London terminus. Never inside parentheses ("Stratford
    /// (London)") and never in a description, where "London" is mostly
    /// ticket acceptance and travel advice.
    pub fn places_in(
        &self,
        text: &str,
        in_scope: impl Fn(&str) -> bool,
        headline: bool,
    ) -> Vec<PlaceMention> {
        if self.is_empty() {
            return Vec::new();
        }
        let cleaned = strip_markup(text);
        let tokens = tokenize(&cleaned);
        let mut out: Vec<PlaceMention> = Vec::new();
        let mut i = 0;
        while i < tokens.len() {
            if !tokens[i].capitalised {
                i += 1;
                continue;
            }
            let longest = (1..=self.max_words.min(tokens.len() - i))
                .rev()
                .find_map(|n| {
                    let key = tokens[i..i + n]
                        .iter()
                        .map(|t| t.word.as_str())
                        .collect::<Vec<_>>()
                        .join(" ");
                    self.by_name.get(&key).map(|crs| (n, key, crs))
                });
            let Some((n, key, crs_list)) = longest else {
                if headline && is_bare_london_section_end(&cleaned, &tokens, i) {
                    let crs: Vec<String> = LONDON_TERMINI
                        .iter()
                        .filter(|crs| self.names.contains_key(**crs) && in_scope(crs))
                        .map(ToString::to_string)
                        .collect();
                    if !crs.is_empty() {
                        out.push(PlaceMention {
                            crs,
                            localised: false,
                        });
                    }
                }
                i += 1;
                continue;
            };
            let accepted = !names_a_line(&cleaned, &tokens, i + n)
                && (!COMMON_WORD_NAMES.contains(&key.as_str())
                    || in_place_context(&cleaned, &tokens, i, i + n));
            if accepted {
                let crs: Vec<String> = crs_list.iter().filter(|c| in_scope(c)).cloned().collect();
                if !crs.is_empty() {
                    out.push(PlaceMention {
                        crs,
                        localised: is_localised(&cleaned, &tokens, i, i + n),
                    });
                }
            }
            i += n;
        }
        out
    }
}

/// Whether the capitalised "London" at `tokens[at]` (where no station name
/// starts) ends an explicit section: "between London and X", or "between
/// X and London" within one clause. Not inside parentheses.
fn is_bare_london_section_end(text: &str, tokens: &[Token], at: usize) -> bool {
    if tokens[at].word != "london" || at == 0 {
        return false;
    }
    let space_only = |a: &Token, b: &Token| text[a.end..b.start].trim().is_empty();
    let before = &text[..tokens[at].start];
    if before.rfind('(') > before.rfind(')') {
        return false;
    }
    let previous = &tokens[at - 1];
    if !space_only(previous, &tokens[at]) {
        return false;
    }
    if previous.word == "between" {
        return tokens
            .get(at + 1)
            .is_some_and(|next| next.word == "and" && space_only(&tokens[at], next));
    }
    if previous.word != "and" {
        return false;
    }
    let clause_start = before.rfind(['.', ';', ':', '!', '?']).map_or(0, |i| i + 1);
    tokens[..at]
        .iter()
        .any(|t| t.start >= clause_start && t.word == "between")
}

/// Whether the place at `tokens[start..end]` is where the disruption is:
/// right after "at", "near", "outside" or "through", or right before
/// "area".
fn is_localised(text: &str, tokens: &[Token], start: usize, end: usize) -> bool {
    let space_only = |from: usize, to: usize| text[from..to].chars().all(char::is_whitespace);
    let before = start.checked_sub(1).is_some_and(|i| {
        LOCALISING_WORDS.contains(&tokens[i].word.as_str())
            && space_only(tokens[i].end, tokens[start].start)
    });
    let after = tokens
        .get(end)
        .is_some_and(|t| t.word == "area" && space_only(tokens[end - 1].end, t.start));
    before || after
}

fn derived_aliases(normalised: &str) -> Vec<String> {
    let mut out = Vec::new();
    let words: Vec<&str> = normalised.split(' ').collect();
    if words.len() >= 2
        && let Some(last) = words.last()
        && COUNTY_QUALIFIERS.contains(last)
    {
        out.push(words[..words.len() - 1].join(" "));
    }
    if words.len() >= 2 && words[0] == "london" {
        let rest = &words[1..];
        if rest.len() >= 2 || LONDON_SHORT_FORMS.contains(&rest[0]) {
            out.push(rest.join(" "));
        }
    }
    out
}

/// One word of incident text: normalised like a station name, with where it
/// sits in the (markup-stripped) text and whether it was capitalised.
struct Token {
    word: String,
    start: usize,
    end: usize,
    capitalised: bool,
}

/// Splits `text` into words the way [`normalise_name`] splits a station
/// name: alphanumeric runs, apostrophes inside a word dropped, "&" its own
/// word "and".
fn tokenize(text: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut current: Option<Token> = None;
    for (offset, c) in text.char_indices() {
        if c.is_alphanumeric() {
            match &mut current {
                Some(token) => {
                    token.word.extend(c.to_lowercase());
                    token.end = offset + c.len_utf8();
                }
                None => {
                    current = Some(Token {
                        word: c.to_lowercase().collect(),
                        start: offset,
                        end: offset + c.len_utf8(),
                        capitalised: c.is_uppercase() || c.is_numeric(),
                    });
                }
            }
        } else if (c == '\'' || c == '\u{2019}') && current.is_some() {
            // "Shepherd's" reads as one word, like the normalised name.
        } else {
            if let Some(token) = current.take() {
                tokens.push(token);
            }
            if c == '&' {
                tokens.push(Token {
                    word: "and".to_string(),
                    start: offset,
                    end: offset + 1,
                    capitalised: false,
                });
            }
        }
    }
    if let Some(token) = current {
        tokens.push(token);
    }
    tokens
}

/// Whether the words right after a match make it a line or operator name:
/// "Brighton Main Line", "Uckfield line", "Windsor lines", "Hull Trains".
/// Only when nothing but spaces separates them.
fn names_a_line(text: &str, tokens: &[Token], next: usize) -> bool {
    let Some(first) = tokens.get(next) else {
        return false;
    };
    let gap_is_space = |from: usize, to: usize| text[from..to].chars().all(char::is_whitespace);
    if !gap_is_space(tokens[next - 1].end, first.start) {
        return false;
    }
    match first.word.as_str() {
        "line" | "lines" | "branch" | "route" => true,
        // "Hull Trains", "Heathrow Express" (an operator brand, now that
        // "Heathrow" alone is a place).
        "trains" => text[first.start..].starts_with('T'),
        "express" => text[first.start..].starts_with('E'),
        "main" => tokens
            .get(next + 1)
            .is_some_and(|t| t.word == "line" && gap_is_space(first.end, t.start)),
        _ => false,
    }
}

/// Whether a common-word name at `tokens[start..end]` reads as a place:
/// after a place word or a "/", and not beside a number.
///
/// The "/" is checked before the number in front of it (2026-10-06): in
/// "Paddington and Heathrow Terminal 5 / Reading" (7CE5A87E) the "5" ends
/// the previous place and "/ Reading" lists another. A number after the
/// name still rules it out ("from March 2027").
fn in_place_context(text: &str, tokens: &[Token], start: usize, end: usize) -> bool {
    let is_number = |t: &Token| t.word.chars().all(|c| c.is_ascii_digit());
    if tokens.get(end).is_some_and(is_number) {
        return false;
    }
    let Some(previous) = start.checked_sub(1).map(|i| &tokens[i]) else {
        return false;
    };
    let gap = &text[previous.end..tokens[start].start];
    if gap.contains('/') {
        return true;
    }
    if is_number(previous) {
        return false;
    }
    gap.chars().all(char::is_whitespace) && PLACE_CONTEXT_WORDS.contains(&previous.word.as_str())
}

/// Removes HTML tags (RDM descriptions are HTML) and decodes the few
/// entities that matter for word boundaries.
fn strip_markup(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut in_tag = false;
    for c in text.chars() {
        match c {
            '<' => {
                in_tag = true;
                out.push(' ');
            }
            '>' if in_tag => in_tag = false,
            _ if in_tag => {}
            _ => out.push(c),
        }
    }
    out.replace("&amp;", "&")
        .replace("&nbsp;", " ")
        .replace("&#39;", "'")
        .replace("&rsquo;", "'")
        .replace("&quot;", "\"")
}

/// Whether an incident's text says it is network-wide rather than local
/// (2026-10-06 user decision 2): industrial action, "across the ...
/// network", a reduced timetable, "all ... services", "Intercity routes",
/// or "reduced <operator> service". Only such an incident (or one naming
/// no place at all) is shown on every line of its operator.
///
/// "All ... services" and "reduced ... service" count only when no place
/// follows ("all services between Leeds and York are cancelled" is local),
/// and "reduced ... service" not when it names a line ("Reduced Mildmay
/// line service" is that line's). A bridge or lightning strike is not
/// industrial action.
///
/// Industrial action counts anywhere in the text; every other marker only
/// in the summary. Descriptions of local incidents routinely say "a
/// severely reduced service is running between X and Y" or "delays may
/// spread across the network" (6C20E627, 2026-10-06 replay), while a
/// genuinely network-wide notice says so in its headline.
pub fn has_network_scope_marker(summary: &str, description: &str) -> bool {
    if is_industrial_action(&words_of(summary, description)) {
        return true;
    }
    let words = words_of(summary, "");
    if has_phrase(&words, &["intercity", "routes"])
        || has_phrase(&words, &["network", "wide"])
        || has_phrase(&words, &["across", "the", "network"])
    {
        return true;
    }
    for (i, word) in words.iter().enumerate() {
        // "across the <up to 4 words> network".
        if word == "across"
            && words.get(i + 1).is_some_and(|w| w == "the")
            && words
                .iter()
                .skip(i + 2)
                .take(5)
                .any(|w| w == "network" || w == "networks")
        {
            return true;
        }
        // "reduced <up to 4 words> timetable".
        if word == "reduced"
            && words
                .iter()
                .skip(i + 1)
                .take(5)
                .any(|w| w == "timetable" || w == "timetables")
        {
            return true;
        }
        // "all <1-4 words> services" / "reduced <1-4 words> service", with
        // no place after it and (for "reduced") no line named in between.
        if word == "all" || word == "reduced" {
            let noun = if word == "all" { "services" } else { "service" };
            let window: Vec<&String> = words.iter().skip(i + 1).take(5).collect();
            if let Some(pos) = window
                .iter()
                .position(|w| *w == noun || (word == "reduced" && *w == "services"))
                && pos >= 1
                && !window[..pos]
                    .iter()
                    .any(|w| *w == "line" || *w == "lines" || *w == "branch")
                && !words
                    .get(i + 2 + pos)
                    .is_some_and(|next| LOCAL_FOLLOWERS.contains(&next.as_str()))
            {
                return true;
            }
        }
    }
    false
}

/// Whether the text is about industrial action: "industrial action",
/// "strike action", "on strike", "strike day(s)", or a union's strike ("RMT
/// strike"). Not a bridge or lightning strike.
pub fn mentions_industrial_action(summary: &str, description: &str) -> bool {
    is_industrial_action(&words_of(summary, description))
}

fn words_of(summary: &str, description: &str) -> Vec<String> {
    let text = format!("{} {}", strip_markup(summary), strip_markup(description));
    tokenize(&text).into_iter().map(|t| t.word).collect()
}

fn has_phrase(words: &[String], phrase: &[&str]) -> bool {
    words
        .windows(phrase.len())
        .any(|w| w.iter().zip(phrase).all(|(a, b)| a == b))
}

fn is_industrial_action(words: &[String]) -> bool {
    has_phrase(words, &["industrial", "action"])
        || has_phrase(words, &["strike", "action"])
        || has_phrase(words, &["on", "strike"])
        || has_phrase(words, &["strike", "day"])
        || has_phrase(words, &["strike", "days"])
        || words.windows(2).any(|pair| {
            ["rmt", "aslef", "tssa", "unite"].contains(&pair[0].as_str())
                && (pair[1] == "strike" || pair[1] == "strikes")
        })
}

/// Words that, right after "all ... services" or "reduced ... service",
/// show it is about a place: "between Leeds and York", "to Hull".
const LOCAL_FOLLOWERS: [&str; 11] = [
    "between", "to", "from", "at", "via", "through", "calling", "in", "on", "towards", "serving",
];

/// Whether every one of `operators` (effective codes) is unknown to the
/// catalogue or is [`NATIONAL_RAIL_OPERATOR`], so the incident's lines can
/// only come from its text, searched across every operator. Also true for
/// an incident with no operators at all.
#[expect(
    clippy::implicit_hasher,
    reason = "callers always use the default hasher"
)]
pub fn needs_all_operator_scope(operators: &[String], known: &HashSet<&str>) -> bool {
    operators.iter().all(|op| {
        let op = effective_operator(op);
        op == NATIONAL_RAIL_OPERATOR || !known.contains(op)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gazetteer() -> StationGazetteer {
        StationGazetteer::new([
            ("RDG", "Reading"),
            ("RDW", "Reading West"),
            ("PUR", "Purley"),
            ("PUO", "Purley Oaks"),
            ("GTW", "Gatwick Airport"),
            ("SEF", "Seaford Sussex"),
            ("LVJ", "Liverpool James Street"),
            ("SJS", "St James Street (Walthamstow)"),
            ("VIC", "London Victoria"),
            ("MCV", "Manchester Victoria"),
            ("LBG", "London Bridge"),
            ("BTN", "Brighton"),
            ("HUL", "Hull"),
            ("MCH", "March"),
            ("ORE", "Ore"),
            ("EBN", "Eastbourne"),
            ("SPB", "Shepherd's Bush"),
            ("BMR", "Bromley (Kent)"),
            ("BLE", "Bramley (West Yorkshire)"),
            ("BMY", "Bramley (Hampshire)"),
            ("STP", "London St Pancras International"),
            ("SPX", "London St Pancras (Eurostar)"),
            ("PAD", "London Paddington"),
            ("KGX", "London Kings Cross"),
            ("HXX", "Heathrow Terminals 2 & 3 (Rail Station Only)"),
            ("HAF", "Heathrow Terminal 4 (Rail Station Only)"),
            ("HWV", "Heathrow Terminal 5 (Rail Station Only)"),
            ("BHM", "Birmingham New Street"),
            ("BHI", "Birmingham International"),
            ("MAN", "Manchester Piccadilly"),
            ("SRA", "Stratford (London)"),
            ("SVG", "Stevenage"),
            ("GCR", "Gloucester"),
            ("LEW", "Lewisham"),
        ])
    }

    fn places(text: &str, headline: bool) -> Vec<(Vec<String>, bool)> {
        gazetteer()
            .places_in(text, |_| true, headline)
            .into_iter()
            .map(|p| (p.crs, p.localised))
            .collect()
    }

    #[test]
    fn a_slash_lists_a_common_word_place_even_after_a_number() {
        // 7CE5A87E: Reading was dropped as "next to a number".
        assert_eq!(
            all("Disruption between London Paddington and Heathrow Terminal 5 / Reading"),
            ["PAD", "HWV", "RDG"]
        );
        // A number after the name still rules it out.
        assert!(all("Works from March 2027").is_empty());
    }

    #[test]
    fn heathrow_and_city_aliases() {
        let heathrow = vec!["HXX".to_string(), "HAF".to_string(), "HWV".to_string()];
        assert_eq!(
            places("Disruption at Heathrow Airport", true),
            [(heathrow.clone(), true)]
        );
        assert_eq!(
            places("Trains to Heathrow Terminals are delayed", true),
            [(heathrow, false)]
        );
        // 7CE5A87E: "Heathrow Airport Terminal 5" is Terminal 5 alone.
        assert_eq!(
            all("Trains will not serve Heathrow Airport Terminal 5"),
            ["HWV"]
        );
        // The operator brand is not a place.
        assert!(all("Tickets valid on Heathrow Express").is_empty());
        assert_eq!(
            all("Disruption between Birmingham and Manchester"),
            ["BHM", "MAN"]
        );
        // A longer name still wins over the city alias.
        assert_eq!(all("Disruption at Birmingham International"), ["BHI"]);
    }

    #[test]
    fn an_area_is_its_place_and_localises_it() {
        assert_eq!(
            places("Major disruption in the Gloucester area", true),
            [(vec!["GCR".to_string()], true)]
        );
        assert_eq!(
            places("A signalling fault at Lewisham", false),
            [(vec!["LEW".to_string()], true)]
        );
        assert_eq!(
            places("Disruption between Lewisham and Gloucester", true),
            [
                (vec!["LEW".to_string()], false),
                (vec!["GCR".to_string()], false)
            ]
        );
    }

    #[test]
    fn bare_london_only_ends_an_explicit_section_in_a_summary() {
        let london = |found: &[(Vec<String>, bool)]| {
            found
                .iter()
                .any(|(crs, _)| crs.contains(&"KGX".to_string()) && crs.len() > 1)
        };
        // 0BD0471E: "between London and Stevenage".
        let found = places(
            "Lines reopened: disruption between London and Stevenage",
            true,
        );
        assert!(london(&found), "{found:?}");
        assert_eq!(found.len(), 2);
        let found = places(
            "Disruption between Stevenage and London expected until 18:00",
            true,
        );
        assert!(london(&found), "{found:?}");
        // Not in a description, not without a section, not in parentheses.
        let found = places("Disruption between London and Stevenage", false);
        assert!(!london(&found), "{found:?}");
        let found = places("Trains to London are delayed at Stevenage", true);
        assert!(!london(&found), "{found:?}");
        let found = places("Disruption between Stratford (London) and Lewisham", true);
        assert_eq!(found.len(), 2, "{found:?}");
        assert!(!london(&found), "{found:?}");
        // A named London station is itself.
        assert_eq!(
            all("Disruption between London Kings Cross and Stevenage"),
            ["KGX", "SVG"]
        );
    }

    fn all(text: &str) -> Vec<String> {
        gazetteer().stations_in(text, |_| true)
    }

    #[test]
    fn longest_name_wins() {
        assert_eq!(
            all("Disruption between Purley and Gatwick Airport"),
            ["PUR", "GTW"]
        );
        assert_eq!(all("Delays at Purley Oaks"), ["PUO"]);
        assert_eq!(
            all("No trains between Reading West and Reading"),
            ["RDW", "RDG"]
        );
    }

    #[test]
    fn a_longer_out_of_scope_name_does_not_fall_back_to_a_shorter_one() {
        let g = gazetteer();
        assert_eq!(
            g.stations_in("Delays at Reading West", |crs| crs == "RDG"),
            Vec::<String>::new()
        );
    }

    #[test]
    fn aliases_county_qualifiers_and_london_short_forms() {
        assert_eq!(all("Reduced service to Seaford"), ["SEF"]);
        assert_eq!(all("Disruption at Seaford Sussex"), ["SEF"]);
        assert_eq!(
            all("Trains between James Street and Hamilton Square"),
            ["LVJ"]
        );
        assert_eq!(all("Liverpool James Street is closed"), ["LVJ"]);
        // "St James Street" is a different station: the longer name wins.
        assert_eq!(all("Delays at St James Street"), ["SJS"]);
        assert_eq!(all("Trains to Victoria are delayed"), ["VIC"]);
        assert_eq!(all("Delays at Manchester Victoria"), ["MCV"]);
        assert_eq!(all("Disruption at Bromley"), ["BMR"]);
        // "London Bridge" has no bare form: "bridge" is a word.
        assert_eq!(all("a bridge strike near London Bridge"), ["LBG"]);
        assert_eq!(all("Shepherd’s Bush and Shepherds Bush"), ["SPB"]);
        // Ambiguous once qualifiers are dropped: both, for the scope to pick.
        assert_eq!(all("Disruption at Bramley"), ["BLE", "BMY"]);
        // RDM's own spelling of St Pancras.
        assert_eq!(all("Disruption at London St Pancras"), ["SPX", "STP"]);
        assert_eq!(
            all("Disruption at London St Pancras International"),
            ["STP"]
        );
    }

    #[test]
    fn common_words_need_a_place_context() {
        assert!(all("Reading the timetable before you travel").is_empty());
        assert!(all("reading the timetable").is_empty());
        assert_eq!(all("Disruption between Oxford and Reading"), ["RDG"]);
        assert_eq!(all("Trains to Reading are delayed"), ["RDG"]);
        assert!(all("Works from March 2027").is_empty());
        assert!(all("Until 3 March").is_empty());
        assert_eq!(all("Buses between Ely and March"), ["MCH"]);
        // Not a common word: any capitalised mention counts.
        assert_eq!(all("Ore / Eastbourne"), ["ORE", "EBN"]);
    }

    #[test]
    fn line_and_operator_names_are_not_stations() {
        assert!(all("Disruption on the Brighton Main Line").is_empty());
        assert!(all("Tickets valid on Hull Trains services").is_empty());
        assert!(all("Victoria line closed").is_empty());
        assert_eq!(all("Trains between Brighton and Hull"), ["BTN", "HUL"]);
        // Punctuation between the name and "Lines" ends the name.
        assert_eq!(
            all("Disruption at Brighton. Lines reopen at 10:00"),
            ["BTN"]
        );
    }

    #[test]
    fn html_descriptions_are_read_as_text() {
        assert_eq!(
            all("<p>Signalling fault at <strong>Purley</strong>&nbsp;today</p>"),
            ["PUR"]
        );
    }

    #[test]
    fn operator_aliases() {
        assert_eq!(effective_operator("LN"), "LM");
        assert_eq!(effective_operator("WM"), "LM");
        assert_eq!(effective_operator("SN"), "SN");
        let known: HashSet<&str> = ["LM", "SN"].into_iter().collect();
        assert!(needs_all_operator_scope(&["ZN".to_string()], &known));
        assert!(needs_all_operator_scope(&["QQ".to_string()], &known));
        assert!(needs_all_operator_scope(&[], &known));
        assert!(!needs_all_operator_scope(&["WM".to_string()], &known));
        assert!(!needs_all_operator_scope(
            &["ZN".to_string(), "SN".to_string()],
            &known
        ));
    }

    #[test]
    fn network_scope_markers() {
        for text in [
            "Industrial action to affect TransPennine Express services on Sunday 11 October",
            "Temporary reduced timetable on East Midlands Railway Intercity routes until further notice",
            "Disruption across the Southern network",
            "Reduced c2c service",
            "Reduced Northern service today",
            "All Chiltern Railways services are suspended",
            "RMT strike",
            "No service on strike days",
        ] {
            assert!(has_network_scope_marker(text, ""), "{text}");
        }
        for text in [
            "Disruption between Purley and Gatwick Airport",
            "Reduced service between Uckfield and Oxted",
            "Reduced Mildmay line service",
            "Reduced Windrush line service",
            "All services between Leeds and York are cancelled",
            "Reduced Northern service between Leeds and York",
            "Disruption caused by a bridge strike at Woking",
            "a lightning strike damaged signalling",
        ] {
            assert!(!has_network_scope_marker(text, ""), "{text}");
        }
        assert!(has_network_scope_marker(
            "Disruption to services",
            "<p>The RMT union has announced industrial action.</p>"
        ));
        // Other markers only count in the summary (6C20E627's description).
        assert!(!has_network_scope_marker(
            "Reduced service between Uckfield and Oxted today",
            "<p>Services will be affected all day, with a severely reduced service running \
             directly between Uckfield and London Bridge.</p>"
        ));
        assert!(!has_network_scope_marker(
            "Disruption between Purley and Gatwick Airport",
            "<p>Delays may spread across the Southern network.</p>"
        ));
    }
}
