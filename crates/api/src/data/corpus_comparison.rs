//! CORPUS (`corpus_locations`) compared with the timetable-derived
//! crosswalk (`tiploc_crs`, `stanox_crs`) and the Knowledgebase station
//! names (`stations`), before anything user-visible reads CORPUS.
//!
//! CORPUS is put through the same conservative inference
//! (`common::corpus_inference`) and narrowed to the same one-CRS-per-key
//! crosswalk the runtime fallback would use
//! ([`crate::data::corpus_crosswalk`]), so "CORPUS only" below is exactly
//! what turning the fallback on would add, and "conflict" what it would
//! NOT change (the timetable wins).
//!
//! Two ways to run it, both read-only:
//!
//! - after every CORPUS load, `POST /private/corpus-locations` logs the
//!   [`ReportDetail::Summary`] and sets the `distant_signal_api_corpus_comparison_*`
//!   gauges ([`record_metrics`]);
//! - on demand, `corpus_compare` (`crates/api/src/bin/corpus_compare.rs`,
//!   shipped in the api image) prints the full report against
//!   `DATABASE_URL`.
//!
//! With no CORPUS loaded neither does any work: the route never runs, and
//! the binary stops after one `COUNT(*)`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use anyhow::Result;
use chrono::NaiveDate;
use common::corpus_inference::Rule;
use sqlx::PgPool;

use crate::data::corpus::CorpusLocation;
use crate::data::corpus_crosswalk;
use crate::data::queries::normalize_code;

/// The timetable side of the comparison.
#[derive(Debug, Clone, Default)]
pub struct Timetable {
    /// TIPLOC -> (CRS, station name), `tiploc_crs` preferred over
    /// `stanox_crs` for a TIPLOC in both -- the same precedence as
    /// `queries::crs_for_tiploc`.
    pub tiplocs: BTreeMap<String, (String, String)>,
    /// `stanox_crs`: STANOX -> CRS.
    pub stanoxes: BTreeMap<String, String>,
    /// Every STANOX either timetable table mentions. One in `tiploc_crs`
    /// but not `stanox_crs` was left out by `schedule-reference` on
    /// purpose (shared by two stations).
    pub known_stanoxes: BTreeSet<String>,
    /// `stations` (Knowledgebase): CRS -> name.
    pub station_names: BTreeMap<String, String>,
    /// TIPLOCs called at on one service date (`schedule_calling_points_full`),
    /// when asked for.
    pub called_tiplocs: Option<(NaiveDate, BTreeSet<String>)>,
}

/// Reads the timetable side. `calling_points_date` adds the TIPLOCs called
/// at that day (one primary-key range scan of `schedule_calling_points_full`);
/// the post-load summary leaves it out.
pub async fn load_timetable(
    pool: &PgPool,
    calling_points_date: Option<NaiveDate>,
) -> Result<Timetable> {
    let mut timetable = Timetable::default();
    let stanox_rows: Vec<(String, String, String, String)> =
        sqlx::query_as("SELECT stanox, crs, tiploc, station_name FROM stanox_crs")
            .fetch_all(pool)
            .await?;
    let tiploc_rows: Vec<(String, String, String, String)> =
        sqlx::query_as("SELECT tiploc, crs, station_name, stanox FROM tiploc_crs")
            .fetch_all(pool)
            .await?;
    for (stanox, crs, tiploc, name) in stanox_rows {
        let stanox = stanox.trim().to_owned();
        timetable.known_stanoxes.insert(stanox.clone());
        timetable.stanoxes.insert(stanox, normalize_code(&crs));
        timetable
            .tiplocs
            .insert(normalize_code(&tiploc), (normalize_code(&crs), name));
    }
    // Second, so a TIPLOC in both takes its `tiploc_crs` row.
    for (tiploc, crs, name, stanox) in tiploc_rows {
        let stanox = stanox.trim();
        if !stanox.is_empty() {
            timetable.known_stanoxes.insert(stanox.to_owned());
        }
        timetable
            .tiplocs
            .insert(normalize_code(&tiploc), (normalize_code(&crs), name));
    }
    let names: Vec<(String, String)> = sqlx::query_as("SELECT TRIM(crs), name FROM stations")
        .fetch_all(pool)
        .await?;
    timetable.station_names = names
        .into_iter()
        .map(|(crs, name)| (normalize_code(&crs), name))
        .collect();
    if let Some(date) = calling_points_date {
        let called: Vec<(String,)> = sqlx::query_as(
            "SELECT DISTINCT tiploc FROM schedule_calling_points_full WHERE service_date = $1",
        )
        .bind(date)
        .fetch_all(pool)
        .await?;
        timetable.called_tiplocs = Some((
            date,
            called.into_iter().map(|(t,)| normalize_code(&t)).collect(),
        ));
    }
    Ok(timetable)
}

/// A key both sides give a CRS, with different CRS codes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conflict {
    pub key: String,
    pub timetable_crs: String,
    pub corpus_crs: String,
    /// CORPUS's rule for a TIPLOC; `None` for a STANOX.
    pub rule: Option<Rule>,
}

/// A key only CORPUS gives a CRS: what the fallback would add.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fill {
    pub key: String,
    pub crs: String,
    pub rule: Option<Rule>,
    /// The key's own CORPUS description (a TIPLOC's `NLCDESC`), or the
    /// station's name for a STANOX.
    pub desc: String,
}

/// Two names for the same thing that differ.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameDiff {
    pub key: String,
    pub app: String,
    pub corpus: String,
}

#[derive(Debug, Clone, Default)]
pub struct CorpusComparison {
    pub corpus_rows: usize,
    pub corpus_tiplocs_with_crs: usize,
    pub corpus_stanoxes_with_crs: usize,

    /// TIPLOCs both sides map to the same CRS, by CORPUS rule.
    pub tiploc_agree: BTreeMap<Rule, usize>,
    pub tiploc_conflicts: Vec<Conflict>,
    /// Timetable TIPLOCs CORPUS does not list at all.
    pub tiploc_timetable_only_absent: BTreeSet<String>,
    /// Timetable TIPLOCs CORPUS lists but gives no single CRS (no station at
    /// its STANOX, a signal/junction by name, ambiguous, or several
    /// `3ALPHA`s): TIPLOC -> CORPUS description.
    pub tiploc_timetable_only_no_corpus_crs: BTreeMap<String, String>,
    /// TIPLOCs only CORPUS maps: the fallback's TIPLOC fills.
    pub tiploc_corpus_only: Vec<Fill>,
    /// With calling points loaded: the service date, how many TIPLOCs
    /// called at that day have no timetable CRS, and which of them CORPUS
    /// fills.
    pub called: Option<(NaiveDate, usize, Vec<Fill>)>,

    pub stanox_agree: usize,
    pub stanox_conflicts: Vec<Conflict>,
    pub stanox_timetable_only: BTreeSet<String>,
    /// STANOXes the timetable does not know at all: the fallback's STANOX
    /// fills.
    pub stanox_corpus_only: Vec<Fill>,
    /// STANOXes CORPUS maps that the timetable deliberately left out of
    /// `stanox_crs` (in `tiploc_crs` only): never filled.
    pub stanox_corpus_blocked: Vec<Fill>,

    /// Agreeing TIPLOCs whose timetable name equals / differs from their own
    /// CORPUS `NLCDESC` (compared case- and punctuation-insensitively).
    pub tiploc_names_same: usize,
    pub tiploc_name_diffs: Vec<NameDiff>,
    /// CRS codes in both `stations` and CORPUS whose names match / differ.
    pub crs_names_same: usize,
    pub crs_name_diffs: Vec<NameDiff>,
    pub crs_in_stations_not_corpus: BTreeSet<String>,
    pub crs_in_corpus_not_stations: BTreeSet<String>,
}

/// Upper-case words of `name`, for a like-for-like name comparison.
fn name_key(name: &str) -> String {
    name.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(str::to_ascii_uppercase)
        .collect::<Vec<_>>()
        .join(" ")
}

/// Compares one CORPUS delivery with the timetable side.
pub fn compare(locations: &[CorpusLocation], timetable: &Timetable) -> CorpusComparison {
    let rows = corpus_crosswalk::corpus_rows(locations);
    let (inferred, crosswalk) = corpus_crosswalk::derive(&rows);

    // A TIPLOC's own description: its first row's NLCDESC.
    let mut corpus_desc: BTreeMap<String, String> = BTreeMap::new();
    for l in locations {
        if let Some(tiploc) = l.tiploc.as_deref().map(normalize_code) {
            corpus_desc
                .entry(tiploc)
                .or_insert_with(|| l.nlc_desc.clone().unwrap_or_default());
        }
    }
    let corpus_tiplocs: BTreeMap<&str, (&str, Rule)> = crosswalk
        .tiplocs
        .iter()
        .map(|t| (t.tiploc.as_str(), (t.crs.as_str(), t.rule)))
        .collect();
    let desc_of = |tiploc: &str| corpus_desc.get(tiploc).cloned().unwrap_or_default();

    let mut c = CorpusComparison {
        corpus_rows: locations.len(),
        corpus_tiplocs_with_crs: crosswalk.tiplocs.len(),
        corpus_stanoxes_with_crs: crosswalk.stanoxes.len(),
        ..CorpusComparison::default()
    };

    for (tiploc, (tt_crs, tt_name)) in &timetable.tiplocs {
        match corpus_tiplocs.get(tiploc.as_str()) {
            Some((crs, rule)) if crs == tt_crs => {
                *c.tiploc_agree.entry(*rule).or_default() += 1;
                let corpus_name = desc_of(tiploc);
                if name_key(tt_name) == name_key(&corpus_name) {
                    c.tiploc_names_same += 1;
                } else {
                    c.tiploc_name_diffs.push(NameDiff {
                        key: tiploc.clone(),
                        app: tt_name.clone(),
                        corpus: corpus_name,
                    });
                }
            }
            Some((crs, rule)) => c.tiploc_conflicts.push(Conflict {
                key: tiploc.clone(),
                timetable_crs: tt_crs.clone(),
                corpus_crs: (*crs).to_owned(),
                rule: Some(*rule),
            }),
            None => match corpus_desc.get(tiploc) {
                Some(desc) => {
                    c.tiploc_timetable_only_no_corpus_crs
                        .insert(tiploc.clone(), desc.clone());
                }
                None => {
                    c.tiploc_timetable_only_absent.insert(tiploc.clone());
                }
            },
        }
    }
    for t in &crosswalk.tiplocs {
        if !timetable.tiplocs.contains_key(&t.tiploc) {
            c.tiploc_corpus_only.push(Fill {
                key: t.tiploc.clone(),
                crs: t.crs.clone(),
                rule: Some(t.rule),
                desc: desc_of(&t.tiploc),
            });
        }
    }
    if let Some((date, called)) = &timetable.called_tiplocs {
        let without_crs: Vec<&String> = called
            .iter()
            .filter(|t| !timetable.tiplocs.contains_key(*t))
            .collect();
        let fills = c
            .tiploc_corpus_only
            .iter()
            .filter(|f| called.contains(&f.key))
            .cloned()
            .collect();
        c.called = Some((*date, without_crs.len(), fills));
    }

    let corpus_stanoxes: BTreeMap<&str, &common::corpus_inference::StanoxCrs> = crosswalk
        .stanoxes
        .iter()
        .map(|s| (s.stanox.as_str(), s))
        .collect();
    for (stanox, tt_crs) in &timetable.stanoxes {
        match corpus_stanoxes.get(stanox.as_str()) {
            Some(s) if &s.crs == tt_crs => c.stanox_agree += 1,
            Some(s) => c.stanox_conflicts.push(Conflict {
                key: stanox.clone(),
                timetable_crs: tt_crs.clone(),
                corpus_crs: s.crs.clone(),
                rule: None,
            }),
            None => {
                c.stanox_timetable_only.insert(stanox.clone());
            }
        }
    }
    for s in &crosswalk.stanoxes {
        if timetable.stanoxes.contains_key(&s.stanox) {
            continue;
        }
        let fill = Fill {
            key: s.stanox.clone(),
            crs: s.crs.clone(),
            rule: None,
            desc: s.station_name.clone(),
        };
        if timetable.known_stanoxes.contains(&s.stanox) {
            c.stanox_corpus_blocked.push(fill);
        } else {
            c.stanox_corpus_only.push(fill);
        }
    }

    let corpus_names: BTreeMap<&str, &str> = inferred
        .rows
        .iter()
        .map(|(crs, _, name)| (crs.as_str(), name.as_str()))
        .collect();
    for (crs, name) in &timetable.station_names {
        match corpus_names.get(crs.as_str()) {
            Some(corpus) if name_key(name) == name_key(corpus) => c.crs_names_same += 1,
            Some(corpus) => c.crs_name_diffs.push(NameDiff {
                key: crs.clone(),
                app: name.clone(),
                corpus: (*corpus).to_owned(),
            }),
            None => {
                c.crs_in_stations_not_corpus.insert(crs.clone());
            }
        }
    }
    c.crs_in_corpus_not_stations = corpus_names
        .keys()
        .filter(|crs| !timetable.station_names.contains_key(**crs))
        .map(|crs| (*crs).to_owned())
        .collect();
    c
}

/// How much [`render`] prints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReportDetail {
    /// Counts only (the post-load log).
    Summary,
    /// Counts plus every conflict and fill.
    Standard,
    /// Everything, including every name difference and one-sided key.
    Full,
}

fn rule_label(rule: Option<Rule>) -> &'static str {
    rule.map_or("-", Rule::as_str)
}

/// Renders the comparison as plain text.
pub fn render(c: &CorpusComparison, detail: ReportDetail) -> String {
    let mut out = String::new();
    let agree: usize = c.tiploc_agree.values().sum();
    let _ = writeln!(out, "=== CORPUS vs timetable crosswalk ===");
    let _ = writeln!(
        out,
        "CORPUS rows: {}; TIPLOCs with one CRS: {}; STANOXes with one CRS: {}",
        c.corpus_rows, c.corpus_tiplocs_with_crs, c.corpus_stanoxes_with_crs
    );
    let _ = writeln!(
        out,
        "\n-- TIPLOC -> CRS (timetable: tiploc_crs, then stanox_crs) --"
    );
    let _ = writeln!(out, "agree: {agree}");
    for rule in Rule::ALL {
        let _ = writeln!(
            out,
            "  by {}: {}",
            rule.label(),
            c.tiploc_agree.get(&rule).copied().unwrap_or(0)
        );
    }
    let _ = writeln!(
        out,
        "conflicting CRS (timetable wins): {}",
        c.tiploc_conflicts.len()
    );
    let _ = writeln!(
        out,
        "timetable only, TIPLOC absent from CORPUS: {}",
        c.tiploc_timetable_only_absent.len()
    );
    let _ = writeln!(
        out,
        "timetable only, CORPUS gives no single CRS: {}",
        c.tiploc_timetable_only_no_corpus_crs.len()
    );
    let _ = writeln!(
        out,
        "CORPUS only (the fallback would fill): {}",
        c.tiploc_corpus_only.len()
    );
    for rule in Rule::ALL {
        let n = c
            .tiploc_corpus_only
            .iter()
            .filter(|f| f.rule == Some(rule))
            .count();
        let _ = writeln!(out, "  by {}: {n}", rule.label());
    }
    if let Some((date, without_crs, fills)) = &c.called {
        let _ = writeln!(
            out,
            "TIPLOCs called at on {date} with no timetable CRS: {without_crs}; CORPUS fills {}",
            fills.len()
        );
    }
    let _ = writeln!(out, "\n-- STANOX -> CRS (timetable: stanox_crs) --");
    let _ = writeln!(out, "agree: {}", c.stanox_agree);
    let _ = writeln!(
        out,
        "conflicting CRS (timetable wins): {}",
        c.stanox_conflicts.len()
    );
    let _ = writeln!(out, "timetable only: {}", c.stanox_timetable_only.len());
    let _ = writeln!(
        out,
        "CORPUS only, unknown to the timetable (the fallback would fill): {}",
        c.stanox_corpus_only.len()
    );
    let _ = writeln!(
        out,
        "CORPUS only, but left out of stanox_crs on purpose (never filled): {}",
        c.stanox_corpus_blocked.len()
    );
    let _ = writeln!(out, "\n-- names --");
    let _ = writeln!(
        out,
        "agreeing TIPLOCs, timetable name vs own CORPUS NLCDESC: {} same, {} different",
        c.tiploc_names_same,
        c.tiploc_name_diffs.len()
    );
    let _ = writeln!(
        out,
        "stations (Knowledgebase) name vs CORPUS station name: {} same, {} different; \
         CRS only in stations: {}; only in CORPUS: {}",
        c.crs_names_same,
        c.crs_name_diffs.len(),
        c.crs_in_stations_not_corpus.len(),
        c.crs_in_corpus_not_stations.len()
    );
    if detail == ReportDetail::Summary {
        return out;
    }

    let conflicts = |out: &mut String, title: &str, list: &[Conflict]| {
        let _ = writeln!(out, "\n{title}: {}", list.len());
        for x in list {
            let _ = writeln!(
                out,
                "  {}: timetable {} / CORPUS {} [{}]",
                x.key,
                x.timetable_crs,
                x.corpus_crs,
                rule_label(x.rule)
            );
        }
    };
    let fills = |out: &mut String, title: &str, list: &[Fill]| {
        let _ = writeln!(out, "\n{title}: {}", list.len());
        for x in list {
            let _ = writeln!(
                out,
                "  {} -> {} ({}) [{}]",
                x.key,
                x.crs,
                x.desc,
                rule_label(x.rule)
            );
        }
    };
    conflicts(&mut out, "TIPLOC conflicts", &c.tiploc_conflicts);
    fills(&mut out, "TIPLOC fills", &c.tiploc_corpus_only);
    if let Some((date, _, called_fills)) = &c.called {
        fills(
            &mut out,
            &format!("TIPLOC fills called at on {date}"),
            called_fills,
        );
    }
    conflicts(&mut out, "STANOX conflicts", &c.stanox_conflicts);
    fills(&mut out, "STANOX fills", &c.stanox_corpus_only);
    fills(
        &mut out,
        "STANOXes CORPUS maps but the timetable left out on purpose",
        &c.stanox_corpus_blocked,
    );
    if detail < ReportDetail::Full {
        return out;
    }

    let names = |out: &mut String, title: &str, list: &[NameDiff]| {
        let _ = writeln!(out, "\n{title}: {}", list.len());
        for x in list {
            let _ = writeln!(out, "  {}: {:?} / CORPUS {:?}", x.key, x.app, x.corpus);
        }
    };
    names(
        &mut out,
        "TIPLOC name differences (timetable)",
        &c.tiploc_name_diffs,
    );
    names(
        &mut out,
        "CRS name differences (stations)",
        &c.crs_name_diffs,
    );
    let keys = |out: &mut String, title: &str, set: &BTreeSet<String>| {
        let _ = writeln!(out, "\n{title}: {}", set.len());
        for chunk in set.iter().collect::<Vec<_>>().chunks(12) {
            let line: Vec<&str> = chunk.iter().map(|s| s.as_str()).collect();
            let _ = writeln!(out, "  {}", line.join(" "));
        }
    };
    keys(
        &mut out,
        "timetable TIPLOCs absent from CORPUS",
        &c.tiploc_timetable_only_absent,
    );
    let _ = writeln!(
        out,
        "\ntimetable TIPLOCs CORPUS gives no single CRS: {}",
        c.tiploc_timetable_only_no_corpus_crs.len()
    );
    for (tiploc, desc) in &c.tiploc_timetable_only_no_corpus_crs {
        let _ = writeln!(out, "  {tiploc} ({desc})");
    }
    keys(
        &mut out,
        "timetable-only STANOXes",
        &c.stanox_timetable_only,
    );
    keys(
        &mut out,
        "CRS only in stations",
        &c.crs_in_stations_not_corpus,
    );
    keys(
        &mut out,
        "CRS only in CORPUS",
        &c.crs_in_corpus_not_stations,
    );
    out
}

/// Sets the `distant_signal_api_corpus_comparison_*` gauges from `c`.
pub fn record_metrics(c: &CorpusComparison) {
    let agree: usize = c.tiploc_agree.values().sum();
    for (outcome, n) in [
        ("agree", agree),
        ("conflict", c.tiploc_conflicts.len()),
        (
            "timetable_only",
            c.tiploc_timetable_only_absent.len() + c.tiploc_timetable_only_no_corpus_crs.len(),
        ),
        ("corpus_only", c.tiploc_corpus_only.len()),
    ] {
        metrics::gauge!(
            common::metrics::metric_name("api_corpus_comparison_tiplocs"),
            "outcome" => outcome
        )
        .set(n as f64);
    }
    for (outcome, n) in [
        ("agree", c.stanox_agree),
        ("conflict", c.stanox_conflicts.len()),
        ("timetable_only", c.stanox_timetable_only.len()),
        ("corpus_only", c.stanox_corpus_only.len()),
        ("corpus_blocked", c.stanox_corpus_blocked.len()),
    ] {
        metrics::gauge!(
            common::metrics::metric_name("api_corpus_comparison_stanoxes"),
            "outcome" => outcome
        )
        .set(n as f64);
    }
}

/// The post-load comparison: reads the timetable side, logs the summary
/// and sets the gauges. Never fails the load; an error is logged.
pub async fn log_after_load(pool: &PgPool, locations: &[CorpusLocation]) {
    match load_timetable(pool, None).await {
        Ok(timetable) => {
            let c = compare(locations, &timetable);
            record_metrics(&c);
            tracing::info!(
                tiploc_agree = c.tiploc_agree.values().sum::<usize>(),
                tiploc_conflicts = c.tiploc_conflicts.len(),
                tiploc_corpus_only = c.tiploc_corpus_only.len(),
                stanox_agree = c.stanox_agree,
                stanox_conflicts = c.stanox_conflicts.len(),
                stanox_corpus_only = c.stanox_corpus_only.len(),
                report = %render(&c, ReportDetail::Summary),
                "CORPUS vs timetable crosswalk comparison (run corpus_compare for the full report)"
            );
        }
        Err(err) => {
            tracing::error!(error = ?err, "CORPUS vs timetable comparison failed; the load itself succeeded")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loc(nlc: &str, stanox: &str, tiploc: &str, crs: &str, desc: &str) -> CorpusLocation {
        let opt = |s: &str| (!s.is_empty()).then(|| s.to_owned());
        CorpusLocation {
            nlc: nlc.to_owned(),
            stanox: opt(stanox),
            tiploc: opt(tiploc),
            crs: opt(crs),
            uic: None,
            nlc_desc: opt(desc),
            nlc_desc16: None,
        }
    }

    fn fixture() -> (Vec<CorpusLocation>, Timetable) {
        let corpus = vec![
            loc(
                "559500",
                "87219",
                "CLPHMJN",
                "CLJ",
                "CLAPHAM JUNCTION LONDON",
            ),
            loc("559572", "87219", "CLPHMJW", "", "CLAPHAM JN (WINDSOR)"),
            loc("559595", "87219", "CLPHJLP", "", "CLAPHAM JUNCTION LOOP"),
            loc("542600", "87201", "VICTRIA", "VIC", "VICTORIA LONDON"),
            loc("111100", "11111", "CONFLCT", "AAA", "CONFLICT A"),
            loc("222200", "22222", "NEWSTN", "NEW", "NEW STATION"),
            loc("333300", "33333", "SHARED1", "SHA", "SHARED ONE"),
            loc("444400", "44444", "TTSTNX", "TTS", "TIMETABLE STANOX"),
        ];
        let mut t = Timetable::default();
        let tip = |crs: &str, name: &str| (crs.to_owned(), name.to_owned());
        t.tiplocs
            .insert("CLPHMJN".into(), tip("CLJ", "CLAPHAM JUNCTION"));
        t.tiplocs
            .insert("VICTRIA".into(), tip("VIC", "VICTORIA LONDON"));
        t.tiplocs.insert("CONFLCT".into(), tip("BBB", "CONFLICT B"));
        t.tiplocs
            .insert("CLPHJLP".into(), tip("CLJ", "CLAPHAM LOOP"));
        t.tiplocs.insert("GONE".into(), tip("GON", "GONE"));
        t.stanoxes.insert("87219".into(), "CLJ".into());
        t.stanoxes.insert("87201".into(), "VIC".into());
        t.stanoxes.insert("11111".into(), "BBB".into());
        t.stanoxes.insert("44444".into(), "TTS".into());
        t.stanoxes.insert("55555".into(), "FIV".into());
        t.known_stanoxes = ["87219", "87201", "11111", "44444", "55555", "33333"]
            .into_iter()
            .map(String::from)
            .collect();
        t.station_names
            .insert("CLJ".into(), "Clapham Junction".into());
        t.station_names
            .insert("VIC".into(), "London Victoria".into());
        t.station_names.insert("KBX".into(), "Kirby Cross".into());
        t.called_tiplocs = Some((
            NaiveDate::from_ymd_opt(2026, 9, 28).unwrap(),
            ["CLPHMJN", "CLPHMJW", "NOCRS"]
                .into_iter()
                .map(String::from)
                .collect(),
        ));
        (corpus, t)
    }

    #[test]
    fn every_bucket_is_counted() {
        let (corpus, timetable) = fixture();
        let c = compare(&corpus, &timetable);
        assert_eq!(c.tiploc_agree.get(&Rule::Direct), Some(&2));
        assert_eq!(
            c.tiploc_conflicts,
            vec![Conflict {
                key: "CONFLCT".into(),
                timetable_crs: "BBB".into(),
                corpus_crs: "AAA".into(),
                rule: Some(Rule::Direct),
            }]
        );
        assert_eq!(
            c.tiploc_timetable_only_absent,
            BTreeSet::from(["GONE".to_owned()])
        );
        // A loop at the station's STANOX: CORPUS gives it no CRS.
        assert_eq!(
            c.tiploc_timetable_only_no_corpus_crs
                .keys()
                .collect::<Vec<_>>(),
            ["CLPHJLP"]
        );
        let fills: Vec<(&str, &str, Option<Rule>)> = c
            .tiploc_corpus_only
            .iter()
            .map(|f| (f.key.as_str(), f.crs.as_str(), f.rule))
            .collect();
        assert_eq!(
            fills,
            vec![
                ("CLPHMJW", "CLJ", Some(Rule::StationName)),
                ("NEWSTN", "NEW", Some(Rule::Direct)),
                ("SHARED1", "SHA", Some(Rule::Direct)),
                ("TTSTNX", "TTS", Some(Rule::Direct)),
            ]
        );
        let (_, without_crs, called_fills) = c.called.as_ref().unwrap();
        assert_eq!(*without_crs, 2);
        assert_eq!(called_fills.len(), 1);
        assert_eq!(called_fills[0].key, "CLPHMJW");

        assert_eq!(c.stanox_agree, 3);
        assert_eq!(c.stanox_conflicts.len(), 1);
        assert_eq!(c.stanox_conflicts[0].key, "11111");
        assert_eq!(
            c.stanox_timetable_only,
            BTreeSet::from(["55555".to_owned()])
        );
        assert_eq!(
            c.stanox_corpus_only
                .iter()
                .map(|f| f.key.as_str())
                .collect::<Vec<_>>(),
            ["22222"]
        );
        assert_eq!(
            c.stanox_corpus_blocked
                .iter()
                .map(|f| f.key.as_str())
                .collect::<Vec<_>>(),
            ["33333"]
        );

        // CLPHMJN's timetable name is shorter; VICTRIA's matches.
        assert_eq!(c.tiploc_names_same, 1);
        assert_eq!(c.tiploc_name_diffs.len(), 1);
        assert_eq!(c.crs_names_same, 0);
        assert_eq!(c.crs_name_diffs.len(), 2);
        assert_eq!(
            c.crs_in_stations_not_corpus,
            BTreeSet::from(["KBX".to_owned()])
        );
        assert!(c.crs_in_corpus_not_stations.contains("NEW"));

        let summary = render(&c, ReportDetail::Summary);
        assert!(
            summary.contains("conflicting CRS (timetable wins): 1"),
            "{summary}"
        );
        assert!(
            summary.contains("CORPUS only (the fallback would fill): 4"),
            "{summary}"
        );
        assert!(!summary.contains("CONFLCT"));
        let full = render(&c, ReportDetail::Full);
        assert!(
            full.contains("  CONFLCT: timetable BBB / CORPUS AAA [direct]"),
            "{full}"
        );
        assert!(
            full.contains("  CLPHMJW -> CLJ (CLAPHAM JN (WINDSOR)) [station_name]"),
            "{full}"
        );
        assert!(full.contains("  33333 -> SHA (SHARED ONE) [-]"), "{full}");
    }

    #[test]
    fn empty_corpus_compares_to_nothing() {
        let (_, timetable) = fixture();
        let c = compare(&[], &timetable);
        assert_eq!(c.tiploc_agree.values().sum::<usize>(), 0);
        assert!(c.tiploc_corpus_only.is_empty());
        assert_eq!(c.tiploc_timetable_only_absent.len(), 5);
    }
}
