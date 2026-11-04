//! Joins the GTFS-sourced station catalogue (`IslandOfIrelandStation.id`, a
//! GTFS `stop_id` such as `7020IR2162`) to Iarnród Éireann's live API
//! (`IslandOfIrelandLiveStation.code`, a `StationCode` such as `BFSTC`), the
//! key its departure boards are stored under. The two schemes share
//! nothing, so stations are matched on name and position, in this order:
//!
//! 1. **Override** ([`OVERRIDES`]): an explicit stop id to code (or to no
//!    code). Wins over everything else.
//! 2. **Name**: the normalised catalogue name equals a live station's
//!    normalised name or alias, and the two are within
//!    [`NAME_MAX_METRES`] of each other (or either has no position).
//! 3. **Name prefix**: one normalised name is a whole-word prefix of the
//!    other ("Cork Kent" and "Cork", "Park West and Cherry Orchard" and
//!    "Park West"), within [`PREFIX_MAX_METRES`]. Both need a position.
//! 4. **Coordinates**: a single live station within
//!    [`COORDINATES_MAX_METRES`] and no other within
//!    [`COORDINATES_CLEAR_METRES`].
//!
//! A step with several candidates takes the nearest only when it is less
//! than half as far as the next; otherwise the station is reported as
//! ambiguous and left without a code. Nothing is guessed: a station no step
//! matches is reported ([`LiveCodeReport::unmatched`]) and keeps no code,
//! and an override fixes it.

use std::collections::HashMap;

use super::{IslandOfIrelandLiveStation, IslandOfIrelandStation};

/// Explicit `(catalogue stop id, live code)` pairs, checked before any
/// matching. A `None` code says "this station has no live board" and
/// stops a wrong name or position match. Add one for every station the
/// writer reports as unmatched or ambiguous once its code is confirmed.
///
/// - `7020IR2162` is GTFS's single Belfast stop and `BFSTC` the live API's
///   Belfast (friction research §4). Their published positions are about
///   2 km apart, so this is pinned rather than left to the name step's
///   distance check.
pub const OVERRIDES: &[(&str, Option<&str>)] = &[("7020IR2162", Some("BFSTC"))];

/// Furthest apart an exact name match may be. Generous: the live API's
/// positions are coarse (Belfast's is about 2 km from GTFS's), and the
/// name already agrees.
pub const NAME_MAX_METRES: f64 = 5_000.0;
/// Furthest apart a name-prefix match may be.
pub const PREFIX_MAX_METRES: f64 = 2_000.0;
/// Furthest apart a position-only match may be.
pub const COORDINATES_MAX_METRES: f64 = 150.0;
/// How far the next live station must be for a position-only match.
pub const COORDINATES_CLEAR_METRES: f64 = 500.0;

/// How a station got its live code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MatchMethod {
    Override,
    Name,
    NamePrefix,
    Coordinates,
}

impl MatchMethod {
    /// A metric label value.
    pub fn label(self) -> &'static str {
        match self {
            Self::Override => "override",
            Self::Name => "name",
            Self::NamePrefix => "name_prefix",
            Self::Coordinates => "coordinates",
        }
    }
}

/// Why a station has no live code.
#[derive(Debug, Clone, PartialEq)]
pub enum Unmatched {
    /// No step found any candidate.
    NoCandidate,
    /// A step found several candidates and none clearly nearest.
    Ambiguous(Vec<String>),
    /// An override names a code the live list does not have.
    OverrideCodeMissing(String),
}

/// The outcome for one catalogue station.
#[derive(Debug, Clone, PartialEq)]
pub enum LiveCodeOutcome {
    Matched {
        code: String,
        method: MatchMethod,
    },
    /// An override says the station has no live board.
    NoBoard,
    Unmatched(Unmatched),
}

/// Every catalogue station's outcome, by station id.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LiveCodeReport {
    pub outcomes: Vec<(String, LiveCodeOutcome)>,
}

impl LiveCodeReport {
    /// `(station id, live code)` for every matched station.
    pub fn codes(&self) -> impl Iterator<Item = (&str, &str)> {
        self.outcomes
            .iter()
            .filter_map(|(id, outcome)| match outcome {
                LiveCodeOutcome::Matched { code, .. } => Some((id.as_str(), code.as_str())),
                _ => None,
            })
    }

    /// Stations left without a code that should have one.
    pub fn unmatched(&self) -> impl Iterator<Item = (&str, &Unmatched)> {
        self.outcomes
            .iter()
            .filter_map(|(id, outcome)| match outcome {
                LiveCodeOutcome::Unmatched(why) => Some((id.as_str(), why)),
                _ => None,
            })
    }

    /// How many stations each method matched.
    pub fn counts(&self) -> HashMap<MatchMethod, usize> {
        let mut counts = HashMap::new();
        for (_, outcome) in &self.outcomes {
            if let LiveCodeOutcome::Matched { method, .. } = outcome {
                *counts.entry(*method).or_default() += 1;
            }
        }
        counts
    }
}

/// Matches every catalogue station to a live code ([module docs](self)).
/// `stations` should be the GTFS-sourced (Republic of Ireland) catalogue:
/// the live API covers only Iarnród Éireann's stations.
pub fn match_live_codes(
    stations: &[IslandOfIrelandStation],
    live: &[IslandOfIrelandLiveStation],
) -> LiveCodeReport {
    match_live_codes_with(stations, live, OVERRIDES)
}

/// [`match_live_codes`] with the caller's overrides (tests).
pub fn match_live_codes_with(
    stations: &[IslandOfIrelandStation],
    live: &[IslandOfIrelandLiveStation],
    overrides: &[(&str, Option<&str>)],
) -> LiveCodeReport {
    let prepared: Vec<Prepared<'_>> = live.iter().map(Prepared::new).collect();
    let outcomes = stations
        .iter()
        .map(|station| {
            let outcome = match overrides.iter().find(|(id, _)| *id == station.id) {
                Some((_, None)) => LiveCodeOutcome::NoBoard,
                Some((_, Some(code))) => {
                    if live.iter().any(|l| l.code == *code) {
                        LiveCodeOutcome::Matched {
                            code: (*code).to_owned(),
                            method: MatchMethod::Override,
                        }
                    } else {
                        LiveCodeOutcome::Unmatched(Unmatched::OverrideCodeMissing(
                            (*code).to_owned(),
                        ))
                    }
                }
                None => match_one(station, &prepared),
            };
            (station.id.clone(), outcome)
        })
        .collect();
    LiveCodeReport { outcomes }
}

struct Prepared<'a> {
    station: &'a IslandOfIrelandLiveStation,
    names: Vec<Vec<String>>,
}

impl<'a> Prepared<'a> {
    fn new(station: &'a IslandOfIrelandLiveStation) -> Self {
        let mut names = vec![normalise(&station.name)];
        if let Some(alias) = station.alias.as_deref() {
            let alias = normalise(alias);
            if !alias.is_empty() && !names.contains(&alias) {
                names.push(alias);
            }
        }
        names.retain(|n| !n.is_empty());
        Self { station, names }
    }
}

fn match_one(station: &IslandOfIrelandStation, live: &[Prepared<'_>]) -> LiveCodeOutcome {
    let name = normalise(&station.name);
    let here = position(station.latitude, station.longitude);
    let distance_to = |l: &Prepared<'_>| {
        here.zip(position(l.station.latitude, l.station.longitude))
            .map(|(a, b)| metres(a, b))
    };

    // Each step: the live stations it accepts, with their distance.
    let exact: Vec<(&Prepared<'_>, Option<f64>)> = live
        .iter()
        .filter(|l| !name.is_empty() && l.names.contains(&name))
        .map(|l| (l, distance_to(l)))
        .filter(|(_, d)| d.is_none_or(|d| d <= NAME_MAX_METRES))
        .collect();
    if let Some(outcome) = pick(&exact, MatchMethod::Name) {
        return outcome;
    }

    let prefix: Vec<(&Prepared<'_>, Option<f64>)> = live
        .iter()
        .filter(|l| l.names.iter().any(|n| word_prefix(n, &name)))
        .map(|l| (l, distance_to(l)))
        .filter(|(_, d)| d.is_some_and(|d| d <= PREFIX_MAX_METRES))
        .collect();
    if let Some(outcome) = pick(&prefix, MatchMethod::NamePrefix) {
        return outcome;
    }

    let mut near: Vec<(&Prepared<'_>, f64)> = live
        .iter()
        .filter_map(|l| distance_to(l).map(|d| (l, d)))
        .filter(|(_, d)| *d <= COORDINATES_CLEAR_METRES)
        .collect();
    near.sort_by(|a, b| a.1.total_cmp(&b.1));
    match near.as_slice() {
        [(only, d)] if *d <= COORDINATES_MAX_METRES => LiveCodeOutcome::Matched {
            code: only.station.code.clone(),
            method: MatchMethod::Coordinates,
        },
        [] | [_] => LiveCodeOutcome::Unmatched(Unmatched::NoCandidate),
        several if several[0].1 <= COORDINATES_MAX_METRES => {
            LiveCodeOutcome::Unmatched(Unmatched::Ambiguous(
                several
                    .iter()
                    .map(|(l, _)| l.station.code.clone())
                    .collect(),
            ))
        }
        _ => LiveCodeOutcome::Unmatched(Unmatched::NoCandidate),
    }
}

/// The one candidate, or the nearest when it is less than half as far as
/// the next; `None` when there are no candidates (try the next step).
fn pick(
    candidates: &[(&Prepared<'_>, Option<f64>)],
    method: MatchMethod,
) -> Option<LiveCodeOutcome> {
    let matched = |l: &Prepared<'_>| LiveCodeOutcome::Matched {
        code: l.station.code.clone(),
        method,
    };
    match candidates {
        [] => None,
        [(only, _)] => Some(matched(only)),
        several => {
            let mut by_distance: Vec<(&Prepared<'_>, f64)> = several
                .iter()
                .filter_map(|(l, d)| d.map(|d| (*l, d)))
                .collect();
            by_distance.sort_by(|a, b| a.1.total_cmp(&b.1));
            let clear =
                by_distance.len() == several.len() && by_distance[0].1 * 2.0 < by_distance[1].1;
            Some(if clear {
                matched(by_distance[0].0)
            } else {
                let mut codes: Vec<String> = several
                    .iter()
                    .map(|(l, _)| l.station.code.clone())
                    .collect();
                codes.sort();
                codes.dedup();
                LiveCodeOutcome::Unmatched(Unmatched::Ambiguous(codes))
            })
        }
    }
}

/// Whether one name's words begin the other's (and they differ).
fn word_prefix(a: &[String], b: &[String]) -> bool {
    let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    !short.is_empty() && short.len() < long.len() && long.starts_with(short)
}

/// Lowercase ASCII words: accents folded (`Dún Laoghaire` is `dun
/// laoghaire`), apostrophes dropped, `&` read as `and`, other punctuation a
/// word break, and a trailing `station` dropped.
pub fn normalise(name: &str) -> Vec<String> {
    let mut folded = String::with_capacity(name.len());
    for ch in name.chars() {
        match ch {
            '\'' | '\u{2019}' | '.' => {}
            '&' => folded.push_str(" and "),
            _ => folded.push(fold(ch)),
        }
    }
    let mut words: Vec<String> = folded
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(str::to_owned)
        .collect();
    if words.len() > 1 && words.last().is_some_and(|w| w == "station") {
        words.pop();
    }
    words
}

fn fold(ch: char) -> char {
    match ch.to_lowercase().next().unwrap_or(ch) {
        'á' | 'à' | 'â' | 'ä' => 'a',
        'é' | 'è' | 'ê' | 'ë' => 'e',
        'í' | 'ì' | 'î' | 'ï' => 'i',
        'ó' | 'ò' | 'ô' | 'ö' => 'o',
        'ú' | 'ù' | 'û' | 'ü' => 'u',
        other => other,
    }
}

fn position(latitude: Option<f64>, longitude: Option<f64>) -> Option<(f64, f64)> {
    latitude.zip(longitude)
}

/// Great-circle distance in metres (haversine).
fn metres((lat1, lon1): (f64, f64), (lat2, lon2): (f64, f64)) -> f64 {
    const EARTH_RADIUS_METRES: f64 = 6_371_000.0;
    let (phi1, phi2) = (lat1.to_radians(), lat2.to_radians());
    let d_phi = (lat2 - lat1).to_radians();
    let d_lambda = (lon2 - lon1).to_radians();
    let a = (d_phi / 2.0).sin().powi(2) + phi1.cos() * phi2.cos() * (d_lambda / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_METRES * a.sqrt().asin()
}

#[cfg(test)]
mod tests {
    use super::super::IslandOfIrelandNetwork;
    use super::*;

    fn stop(id: &str, name: &str, at: Option<(f64, f64)>) -> IslandOfIrelandStation {
        IslandOfIrelandStation {
            id: id.into(),
            name: name.into(),
            network: IslandOfIrelandNetwork::RepublicOfIreland,
            latitude: at.map(|p| p.0),
            longitude: at.map(|p| p.1),
            raw_name: None,
        }
    }

    fn live(
        code: &str,
        name: &str,
        alias: Option<&str>,
        at: (f64, f64),
    ) -> IslandOfIrelandLiveStation {
        IslandOfIrelandLiveStation {
            code: code.into(),
            name: name.into(),
            alias: alias.map(Into::into),
            latitude: Some(at.0),
            longitude: Some(at.1),
        }
    }

    /// A synthetic slice of the live list, shaped like `getAllStationsXML`
    /// (positions rounded from the real feed): passenger stations, an alias,
    /// two stations sharing a town name and a junction.
    fn live_list() -> Vec<IslandOfIrelandLiveStation> {
        vec![
            live("BFSTC", "Belfast", None, (54.6123, -5.91744)),
            live(
                "CNLLY",
                "Dublin Connolly",
                Some("Connolly"),
                (53.3531, -6.24591),
            ),
            live("CORK", "Cork", None, (51.9018, -8.4582)),
            live("LMRCK", "Limerick", None, (52.6587, -8.6243)),
            live("LMRKJ", "Limerick Junction", None, (52.5009, -8.2000)),
            live("DLERY", "Dun Laoghaire", None, (53.2951, -6.1350)),
            live(
                "CTJN",
                "City Junction",
                Some("Dublin Belfast"),
                (54.5900, -5.9300),
            ),
            live("HAZEL", "Hazelhatch", None, (53.3226, -6.5232)),
        ]
    }

    fn outcome<'a>(report: &'a LiveCodeReport, id: &str) -> &'a LiveCodeOutcome {
        &report.outcomes.iter().find(|(i, _)| i == id).expect(id).1
    }

    fn matched(code: &str, method: MatchMethod) -> LiveCodeOutcome {
        LiveCodeOutcome::Matched {
            code: code.into(),
            method,
        }
    }

    #[test]
    fn normalise_folds_accents_punctuation_and_a_trailing_station() {
        assert_eq!(normalise("Dún Laoghaire"), ["dun", "laoghaire"]);
        assert_eq!(normalise("Helen's Bay Station"), ["helens", "bay"]);
        assert_eq!(
            normalise("Park West & Cherry Orchard"),
            ["park", "west", "and", "cherry", "orchard"]
        );
        assert_eq!(normalise("Station"), ["station"]);
        assert!(normalise("  ").is_empty());
    }

    #[test]
    fn exact_names_and_aliases_match() {
        let stations = [
            stop("8220IR0132", "Dublin Connolly", Some((53.3509, -6.2502))),
            stop("A", "Connolly", Some((53.3509, -6.2502))),
            stop("B", "Dún Laoghaire", Some((53.2950, -6.1351))),
            stop("C", "Limerick Junction", Some((52.5010, -8.2001))),
        ];
        let report = match_live_codes_with(&stations, &live_list(), &[]);
        assert_eq!(
            outcome(&report, "8220IR0132"),
            &matched("CNLLY", MatchMethod::Name)
        );
        assert_eq!(outcome(&report, "A"), &matched("CNLLY", MatchMethod::Name));
        assert_eq!(outcome(&report, "B"), &matched("DLERY", MatchMethod::Name));
        // The exact name wins over "Limerick", a word prefix of it.
        assert_eq!(outcome(&report, "C"), &matched("LMRKJ", MatchMethod::Name));
    }

    #[test]
    fn a_name_prefix_matches_only_nearby() {
        let stations = [
            stop("K", "Cork Kent", Some((51.9019, -8.4580))),
            stop("L", "Limerick Colbert", Some((52.6588, -8.6244))),
            // "Cork" again, but 60 km away: no prefix match.
            stop("FAR", "Cork Airport Halt", Some((52.3000, -8.9000))),
        ];
        let report = match_live_codes_with(&stations, &live_list(), &[]);
        assert_eq!(
            outcome(&report, "K"),
            &matched("CORK", MatchMethod::NamePrefix)
        );
        assert_eq!(
            outcome(&report, "L"),
            &matched("LMRCK", MatchMethod::NamePrefix)
        );
        assert_eq!(
            outcome(&report, "FAR"),
            &LiveCodeOutcome::Unmatched(Unmatched::NoCandidate)
        );
    }

    #[test]
    fn an_exact_name_too_far_away_does_not_match() {
        let stations = [stop("X", "Hazelhatch", Some((52.0, -7.0)))];
        let report = match_live_codes_with(&stations, &live_list(), &[]);
        assert_eq!(
            outcome(&report, "X"),
            &LiveCodeOutcome::Unmatched(Unmatched::NoCandidate)
        );
    }

    #[test]
    fn an_exact_name_without_a_position_still_matches() {
        let stations = [stop("X", "Hazelhatch", None)];
        let report = match_live_codes_with(&stations, &live_list(), &[]);
        assert_eq!(outcome(&report, "X"), &matched("HAZEL", MatchMethod::Name));
    }

    #[test]
    fn coordinates_match_a_lone_close_station_only() {
        let stations = [
            // 40 m from Hazelhatch, under another name.
            stop("NEAR", "Celbridge", Some((53.3229, -6.5229))),
            // 400 m away: not close enough.
            stop("MID", "Somewhere", Some((53.3262, -6.5232))),
        ];
        let report = match_live_codes_with(&stations, &live_list(), &[]);
        assert_eq!(
            outcome(&report, "NEAR"),
            &matched("HAZEL", MatchMethod::Coordinates)
        );
        assert_eq!(
            outcome(&report, "MID"),
            &LiveCodeOutcome::Unmatched(Unmatched::NoCandidate)
        );
    }

    #[test]
    fn two_close_candidates_are_reported_not_guessed() {
        let mut list = live_list();
        list.push(live("CRKA", "Cork", None, (51.9030, -8.4600)));
        let stations = [stop("K", "Cork", Some((51.9024, -8.4591)))];
        let report = match_live_codes_with(&stations, &list, &[]);
        assert_eq!(
            outcome(&report, "K"),
            &LiveCodeOutcome::Unmatched(Unmatched::Ambiguous(vec!["CORK".into(), "CRKA".into()]))
        );
        assert_eq!(report.unmatched().count(), 1);
    }

    #[test]
    fn of_two_named_candidates_a_clearly_nearer_one_wins() {
        let mut list = live_list();
        list.push(live("CRKB", "Cork", None, (51.9300, -8.4900)));
        let stations = [stop("K", "Cork", Some((51.9019, -8.4583)))];
        let report = match_live_codes_with(&stations, &list, &[]);
        assert_eq!(outcome(&report, "K"), &matched("CORK", MatchMethod::Name));
    }

    #[test]
    fn overrides_win_and_can_say_no_board() {
        let stations = [
            stop(
                "7020IR2162",
                "Belfast Grand Central",
                Some((54.594_684, -5.939_831)),
            ),
            stop("NB", "Dublin Connolly", Some((53.3509, -6.2502))),
            stop("GONE", "Hazelhatch", None),
        ];
        let report = match_live_codes_with(
            &stations,
            &live_list(),
            &[
                ("7020IR2162", Some("BFSTC")),
                ("NB", None),
                ("GONE", Some("NOPE")),
            ],
        );
        assert_eq!(
            outcome(&report, "7020IR2162"),
            &matched("BFSTC", MatchMethod::Override)
        );
        assert_eq!(outcome(&report, "NB"), &LiveCodeOutcome::NoBoard);
        assert_eq!(
            outcome(&report, "GONE"),
            &LiveCodeOutcome::Unmatched(Unmatched::OverrideCodeMissing("NOPE".into()))
        );
        let codes: Vec<_> = report.codes().collect();
        assert_eq!(codes, [("7020IR2162", "BFSTC")]);
    }

    #[test]
    fn the_shipped_overrides_name_belfast() {
        assert!(OVERRIDES.contains(&("7020IR2162", Some("BFSTC"))));
        let report = match_live_codes(
            &[stop(
                "7020IR2162",
                "Belfast",
                Some((54.594_684, -5.939_831)),
            )],
            &live_list(),
        );
        assert_eq!(report.counts().get(&MatchMethod::Override), Some(&1));
    }

    #[test]
    fn metres_is_roughly_right() {
        // Connolly to Heuston is about 3.6 km.
        let d = metres((53.3531, -6.24591), (53.3464, -6.29461));
        assert!((3_200.0..3_600.0).contains(&d), "{d}");
    }
}
