//! Which trains are a line's own: per-line train membership ("scope").
//!
//! A line's population (`schedule-reference`'s
//! `publish_schedule_line_population`) is every schedule touching one of the
//! line's stations -- at a busy hub that is mostly other routes' trains
//! (about 9% precision measured on 2026-10-06). This module tags each
//! `(line, train)` pair with a [`LineScope`]:
//!
//! * [`LineScope::Line`] -- one of the line's own trains (rule "R75" of
//!   docs/superpowers/specs/2026-10-06-line-membership-design.md);
//! * [`LineScope::Shared`] -- runs a real stretch of the line but is not one
//!   of its own (another operator on the same track, or the operator's
//!   train for another of its lines);
//! * [`LineScope::Touch`] -- only touches the line (a hub call, a
//!   crossing).
//!
//! The rule, per `(line, train)`:
//!
//! 1. Not a bus or ship ([`crate::records::is_bus_or_ship`]).
//! 2. A *run*: consecutive line stations in the train's TIPLOC path, each
//!    pair joined by on-route TIPLOCs, covering at least two stations. A
//!    line's route TIPLOCs (junctions, loops, unlisted stations) are LEARNED
//!    from its own operator's trains between catalogue-adjacent stations, so
//!    CIF recording passes only at timing points does not matter; a stretch
//!    is accepted when at most [`UNKNOWN_SLACK`] of its intermediate
//!    TIPLOCs were never learned.
//! 3. Run by one of the line's operators -- or by anyone, when none of the
//!    line's operators runs a train along it that day (the Island Line
//!    fallback).
//! 4. One of: the whole journey is on the line's route; the run spans at
//!    least 75% of the catalogue stations; the line is the train's best fit
//!    among its operator's lines (most run stations, then the larger share
//!    of the line; ties keep all); or the train's best-fit line is in this
//!    line's `trunk_for`.
//!
//! A pair failing 1, 3 or 4 but with a run is `Shared`; with no run,
//! `Touch`. Pure: no I/O, deterministic for a given input.

use std::collections::{HashMap, HashSet};

use chrono::NaiveTime;
use serde::{Deserialize, Serialize};

use crate::records::{CallingPoint, is_bus_or_ship};
use crate::resolve::ResolvedSchedule;
use crate::tiploc::normalize_tiploc;

/// How many intermediate TIPLOCs of a stretch between two line stations may
/// be unknown (never seen on the line's own trains) for the stretch still
/// to count as on the route.
pub const UNKNOWN_SLACK: usize = 1;

/// When learning route TIPLOCs, how far apart (in catalogue positions) two
/// consecutive calls of a seed train may be -- further apart means it left
/// the line in between.
const LEARN_MAX_GAP: usize = 3;

/// When learning between catalogue-ADJACENT stations, how many stations the
/// catalogue does not list may lie between them (stations the catalogue
/// skips). Between stations further apart, none may.
const LEARN_ADJACENT_UNLISTED: usize = 6;

/// The span threshold of rule 4: a run covering at least this share of the
/// line's catalogue stations makes the train one of the line's own
/// (user decision, 2026-10-06).
const SPAN_NUMERATOR: usize = 3;
const SPAN_DENOMINATOR: usize = 4;

/// A train's membership of one line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LineScope {
    /// One of the line's own trains.
    Line,
    /// Runs a stretch of the line, but is not one of its own.
    Shared,
    /// Only touches the line.
    Touch,
}

impl LineScope {
    pub fn as_str(self) -> &'static str {
        match self {
            LineScope::Line => "line",
            LineScope::Shared => "shared",
            LineScope::Touch => "touch",
        }
    }
}

/// The direction of a train's run, by the line's catalogue station order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunDirection {
    /// From an earlier catalogue station to a later one.
    Down,
    /// From a later catalogue station to an earlier one.
    Up,
    /// Starts and ends at the same line station (a circle, e.g. Cathcart).
    Loop,
}

/// When a train is due on a line: its first booked public call at one of
/// the line's stations, in Europe/London local time, `day_offset` days
/// after the schedule's service date.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineDue {
    pub time: NaiveTime,
    #[serde(default)]
    pub day_offset: u8,
}

/// One line, as membership needs it. Station and alias CRS codes are
/// compared upper-case.
#[derive(Debug, Clone, Default)]
pub struct MembershipLine {
    pub id: String,
    pub operators: Vec<String>,
    /// Catalogue stations, in order (CRS).
    pub stations: Vec<String>,
    /// CIF CRS -> catalogue station CRS (the catalogue's `crs_aliases`).
    pub crs_aliases: HashMap<String, String>,
    /// Line ids this line is the trunk of (the catalogue's `trunk_for`).
    pub trunk_for: Vec<String>,
}

/// One train's membership of one line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Membership {
    pub scope: LineScope,
    /// First and last line station of the run (catalogue CRS); `None`
    /// without a run.
    pub run_first_crs: Option<String>,
    pub run_last_crs: Option<String>,
    pub direction: Option<RunDirection>,
}

/// Every non-cancelled schedule of one service date, reduced to its TIPLOC
/// path (interned), operator and status. Built with [`DayTrains::push`] in
/// the order the population should be published in.
#[derive(Debug, Default)]
pub struct DayTrains {
    tiploc_ids: HashMap<Box<str>, u32>,
    tiploc_names: Vec<Box<str>>,
    /// Per TIPLOC id: does any train of the day call (arrive or depart)
    /// there? A TIPLOC only ever passed is a junction or timing point, even
    /// when the crosswalk files it under a station's CRS (`ACTONW` Acton
    /// West is `EAL`, `NWTLEJ` Newton East Junction is `NTN`).
    called: Vec<bool>,
    trains: Vec<DayTrain>,
}

#[derive(Debug)]
struct DayTrain {
    uid: Box<str>,
    operator: Option<Box<str>>,
    bus: bool,
    path: Vec<u32>,
}

impl DayTrains {
    pub fn new() -> Self {
        Self::default()
    }

    fn intern(&mut self, tiploc: &str) -> u32 {
        if let Some(&id) = self.tiploc_ids.get(tiploc) {
            return id;
        }
        let id = u32::try_from(self.tiploc_names.len()).unwrap_or(u32::MAX);
        self.tiploc_ids.insert(tiploc.into(), id);
        self.tiploc_names.push(tiploc.into());
        self.called.push(false);
        id
    }

    /// Adds one resolved schedule. A cancelled one is ignored.
    pub fn push(&mut self, schedule: &ResolvedSchedule) {
        if schedule.cancelled {
            return;
        }
        let mut path = Vec::with_capacity(schedule.calling_points.len());
        for cp in &schedule.calling_points {
            let id = self.intern(normalize_tiploc(&cp.tiploc));
            if cp.booked_arrival.is_some() || cp.booked_departure.is_some() {
                self.called[id as usize] = true;
            }
            path.push(id);
        }
        self.trains.push(DayTrain {
            uid: schedule.uid.as_str().into(),
            operator: schedule.operator_atoc.as_deref().map(Into::into),
            bus: is_bus_or_ship(schedule.train_status),
            path,
        });
    }

    pub fn len(&self) -> usize {
        self.trains.len()
    }

    pub fn is_empty(&self) -> bool {
        self.trains.is_empty()
    }

    /// The UID of train `index` (in [`Self::push`] order).
    pub fn uid(&self, index: usize) -> &str {
        &self.trains[index].uid
    }

    /// Is `tiploc` called at by any train of the day?
    pub fn is_called(&self, tiploc: &str) -> bool {
        self.tiploc_ids
            .get(normalize_tiploc(tiploc))
            .is_some_and(|&id| self.called[id as usize])
    }
}

/// One line's classified population.
#[derive(Debug, Default)]
pub struct LineMembers {
    /// `(train index, membership)` for every train touching the line, in
    /// [`DayTrains::push`] order.
    pub members: Vec<(usize, Membership)>,
    /// The line's station TIPLOCs (called at by some train that day, aliases
    /// included) -- what "touching" and [`line_due`] look for.
    pub station_tiplocs: HashSet<Box<str>>,
}

/// Per-line view used while classifying.
struct LineCtx<'a> {
    line: &'a MembershipLine,
    /// CRS id -> catalogue positions (a station can repeat on a loop line).
    positions: HashMap<u32, Vec<usize>>,
    /// Catalogue stations as CRS ids.
    stations: Vec<u32>,
    /// CIF CRS id -> catalogue CRS id.
    aliases: HashMap<u32, u32>,
}

impl LineCtx<'_> {
    fn node_crs(&self, crs_of: &[Option<u32>], tiploc: u32) -> Option<u32> {
        let crs = crs_of[tiploc as usize]?;
        Some(self.aliases.get(&crs).copied().unwrap_or(crs))
    }

    fn line_crs(&self, crs_of: &[Option<u32>], tiploc: u32) -> Option<u32> {
        self.node_crs(crs_of, tiploc)
            .filter(|crs| self.positions.contains_key(crs))
    }

    fn first_position(&self, crs: u32) -> usize {
        self.positions
            .get(&crs)
            .and_then(|p| p.first())
            .copied()
            .unwrap_or(0)
    }

    /// `(path index, catalogue CRS id)` of the train's line-station
    /// TIPLOCs, with consecutive TIPLOCs of one station (several platforms
    /// or ends) collapsed onto the last of them.
    fn occurrences(&self, crs_of: &[Option<u32>], path: &[u32]) -> Vec<(usize, u32)> {
        let mut occ: Vec<(usize, u32)> = Vec::new();
        for (i, &tiploc) in path.iter().enumerate() {
            let Some(crs) = self.line_crs(crs_of, tiploc) else {
                continue;
            };
            if let Some(last) = occ.last_mut()
                && last.1 == crs
                && path[last.0..i]
                    .iter()
                    .all(|&m| self.node_crs(crs_of, m).is_none_or(|c| c == crs))
            {
                last.0 = i;
                continue;
            }
            occ.push((i, crs));
        }
        occ
    }
}

/// What one train does on one line.
#[derive(Debug, Clone, Default)]
struct Facts {
    /// Distinct stations of the longest run (`0`: never touches a station,
    /// `1`: touches but no run).
    longest: usize,
    /// The run chain's first and last station (not de-duplicated, so a
    /// circle starts and ends at the same one).
    ends: Option<(u32, u32)>,
    whole: bool,
    span: usize,
}

/// `(stations, stations / line length)`, compared exactly.
#[derive(Debug, Clone, Copy)]
struct Fit {
    longest: usize,
    line_len: usize,
}

impl Fit {
    fn cmp(self, other: Self) -> std::cmp::Ordering {
        self.longest
            .cmp(&other.longest)
            .then_with(|| (self.longest * other.line_len).cmp(&(other.longest * self.line_len)))
    }
}

/// Classifies every line's population for one day.
///
/// `station_crs`: normalized TIPLOC -> CRS (the CIF crosswalk). Only
/// TIPLOCs some train of the day calls at count as stations
/// ([`DayTrains::is_called`]). Returns one [`LineMembers`] per `lines`
/// entry, in the same order.
#[expect(
    clippy::too_many_lines,
    reason = "one pass per rule step; splitting would scatter the shared per-day tables"
)]
pub fn classify<S: std::hash::BuildHasher>(
    day: &DayTrains,
    lines: &[MembershipLine],
    station_crs: &HashMap<String, String, S>,
) -> Vec<LineMembers> {
    // CRS interning, and each TIPLOC's station CRS (called TIPLOCs only).
    let mut crs_ids: HashMap<String, u32> = HashMap::new();
    let mut crs_names: Vec<String> = Vec::new();
    let mut intern_crs = |crs: &str| -> u32 {
        let crs = crs.trim().to_ascii_uppercase();
        if let Some(&id) = crs_ids.get(&crs) {
            return id;
        }
        let id = u32::try_from(crs_names.len()).unwrap_or(u32::MAX);
        crs_ids.insert(crs.clone(), id);
        crs_names.push(crs);
        id
    };
    let crs_of: Vec<Option<u32>> = day
        .tiploc_names
        .iter()
        .enumerate()
        .map(|(id, name)| {
            if !day.called[id] {
                return None;
            }
            station_crs.get(&**name).map(|crs| intern_crs(crs))
        })
        .collect();

    let ctxs: Vec<LineCtx<'_>> = lines
        .iter()
        .map(|line| {
            let stations: Vec<u32> = line.stations.iter().map(|s| intern_crs(s)).collect();
            let mut positions: HashMap<u32, Vec<usize>> = HashMap::new();
            for (k, &crs) in stations.iter().enumerate() {
                positions.entry(crs).or_default().push(k);
            }
            let aliases = line
                .crs_aliases
                .iter()
                .map(|(from, to)| (intern_crs(from), intern_crs(to)))
                .filter(|(_, to)| positions.contains_key(to))
                .collect();
            LineCtx {
                line,
                positions,
                stations,
                aliases,
            }
        })
        .collect();
    let crs_names = crs_names;

    // Populations: every train with a TIPLOC on the line.
    let mut lines_by_crs: HashMap<u32, Vec<usize>> = HashMap::new();
    for (li, ctx) in ctxs.iter().enumerate() {
        for &crs in ctx.positions.keys().chain(ctx.aliases.keys()) {
            lines_by_crs.entry(crs).or_default().push(li);
        }
    }
    let mut populations: Vec<Vec<usize>> = vec![Vec::new(); lines.len()];
    let mut touched: Vec<usize> = Vec::new();
    for (ti, train) in day.trains.iter().enumerate() {
        touched.clear();
        for &tiploc in &train.path {
            if let Some(crs) = crs_of[tiploc as usize]
                && let Some(lis) = lines_by_crs.get(&crs)
            {
                touched.extend(lis);
            }
        }
        touched.sort_unstable();
        touched.dedup();
        for &li in &touched {
            populations[li].push(ti);
        }
    }

    let operator_ok = |line: &MembershipLine, train: &DayTrain| {
        line.operators.is_empty()
            || train
                .operator
                .as_deref()
                .is_some_and(|op| line.operators.iter().any(|o| o == op))
    };

    // Per line: occurrences, learned route TIPLOCs, then facts per train.
    let mut facts: Vec<Vec<Facts>> = Vec::with_capacity(lines.len());
    let mut own_runs: Vec<bool> = Vec::with_capacity(lines.len());
    for (li, ctx) in ctxs.iter().enumerate() {
        let pop = &populations[li];
        let occs: Vec<Vec<(usize, u32)>> = pop
            .iter()
            .map(|&ti| ctx.occurrences(&crs_of, &day.trains[ti].path))
            .collect();
        let learned = learn_route(ctx, day, &crs_of, &crs_names, pop, &occs, &operator_ok);
        let line_facts: Vec<Facts> = pop
            .iter()
            .zip(&occs)
            .map(|(&ti, occ)| train_facts(ctx, &crs_of, &day.trains[ti].path, occ, &learned))
            .collect();
        own_runs.push(pop.iter().zip(&line_facts).any(|(&ti, f)| {
            let train = &day.trains[ti];
            !train.bus && f.longest >= 2 && operator_ok(ctx.line, train)
        }));
        facts.push(line_facts);
    }

    // Per train: its runs on every line it touches, for best fit.
    let mut fits: Vec<Vec<(usize, Fit, bool)>> = vec![Vec::new(); day.trains.len()];
    for (li, pop) in populations.iter().enumerate() {
        for (pi, &ti) in pop.iter().enumerate() {
            let f = &facts[li][pi];
            if f.longest >= 2 {
                let train = &day.trains[ti];
                let own = operator_ok(&lines[li], train) || !own_runs[li];
                fits[ti].push((
                    li,
                    Fit {
                        longest: f.longest,
                        line_len: lines[li].stations.len().max(1),
                    },
                    own,
                ));
            }
        }
    }

    let mut out = Vec::with_capacity(lines.len());
    for (li, ctx) in ctxs.iter().enumerate() {
        let line = ctx.line;
        let n = line.stations.len().max(1);
        let mut members = Vec::with_capacity(populations[li].len());
        for (pi, &ti) in populations[li].iter().enumerate() {
            let train = &day.trains[ti];
            let f = &facts[li][pi];
            let run = f.longest >= 2;
            let op_ok = operator_ok(line, train) || !own_runs[li];
            let is_line = !train.bus
                && run
                && op_ok
                && (f.whole
                    || f.span * SPAN_DENOMINATOR >= n * SPAN_NUMERATOR
                    || best_fit(&fits[ti], li)
                    || best_lines(&fits[ti])
                        .any(|best| line.trunk_for.iter().any(|t| t == &lines[best].id)));
            let scope = if is_line {
                LineScope::Line
            } else if run {
                LineScope::Shared
            } else {
                LineScope::Touch
            };
            let (run_first_crs, run_last_crs, direction) = match (run, f.ends) {
                (true, Some((first, last))) => {
                    let (a, b) = (ctx.first_position(first), ctx.first_position(last));
                    let direction = match (a, b) {
                        _ if first == last => RunDirection::Loop,
                        (a, b) if b > a => RunDirection::Down,
                        (a, b) if b < a => RunDirection::Up,
                        _ => RunDirection::Loop,
                    };
                    (
                        Some(crs_names[first as usize].clone()),
                        Some(crs_names[last as usize].clone()),
                        Some(direction),
                    )
                }
                _ => (None, None, None),
            };
            members.push((
                ti,
                Membership {
                    scope,
                    run_first_crs,
                    run_last_crs,
                    direction,
                },
            ));
        }
        let station_tiplocs = day
            .tiploc_names
            .iter()
            .enumerate()
            .filter(|&(id, _)| {
                ctx.line_crs(&crs_of, u32::try_from(id).unwrap_or(u32::MAX))
                    .is_some()
            })
            .map(|(_, name)| name.clone())
            .collect();
        out.push(LineMembers {
            members,
            station_tiplocs,
        });
    }
    out
}

/// Is line `li` this train's best fit? Among the lines it has a run on,
/// its own operator's lines first: a line outside them never is, when
/// there are any. Ties keep all.
fn best_fit(fits: &[(usize, Fit, bool)], li: usize) -> bool {
    let Some(&(_, mine, mine_own)) = fits.iter().find(|(l, ..)| *l == li) else {
        return false;
    };
    let any_own = fits.iter().any(|(.., own)| *own);
    if any_own && !mine_own {
        return false;
    }
    fits.iter()
        .filter(|(.., own)| *own || !any_own)
        .all(|(_, other, _)| mine.cmp(*other) != std::cmp::Ordering::Less)
}

/// The train's best-fit lines among its own operator's (empty when it has
/// a run on none of them).
fn best_lines(fits: &[(usize, Fit, bool)]) -> impl Iterator<Item = usize> + '_ {
    let top = fits
        .iter()
        .filter(|(.., own)| *own)
        .map(|(_, fit, _)| *fit)
        .max_by(|a, b| a.cmp(*b));
    fits.iter()
        .filter(move |(_, fit, own)| {
            *own && top.is_some_and(|top| fit.cmp(top) == std::cmp::Ordering::Equal)
        })
        .map(|(li, ..)| *li)
}

/// The line's route TIPLOCs, learned from its own operator's trains (or,
/// when it has none that day, every non-bus train): every TIPLOC between two
/// consecutive line-station calls at most [`LEARN_MAX_GAP`] catalogue
/// positions apart, unless the stretch also passes stations the catalogue
/// does not list between them (beyond [`LEARN_ADJACENT_UNLISTED`] for
/// adjacent stations) -- that train left the line in between.
fn learn_route(
    ctx: &LineCtx<'_>,
    day: &DayTrains,
    crs_of: &[Option<u32>],
    crs_names: &[String],
    pop: &[usize],
    occs: &[Vec<(usize, u32)>],
    operator_ok: &impl Fn(&MembershipLine, &DayTrain) -> bool,
) -> HashSet<u32> {
    let own: Vec<usize> = (0..pop.len())
        .filter(|&pi| {
            let train = &day.trains[pop[pi]];
            !train.bus && operator_ok(ctx.line, train)
        })
        .collect();
    let seeds: Vec<usize> = if own.is_empty() {
        (0..pop.len())
            .filter(|&pi| !day.trains[pop[pi]].bus)
            .collect()
    } else {
        own
    };
    let mut learned = HashSet::new();
    for pi in seeds {
        let path = &day.trains[pop[pi]].path;
        for pair in occs[pi].windows(2) {
            let ((i, a), (j, b)) = (pair[0], pair[1]);
            if a == b {
                continue;
            }
            let (Some(pa), Some(pb)) = (ctx.positions.get(&a), ctx.positions.get(&b)) else {
                continue;
            };
            let mut best: Option<(usize, usize)> = None;
            for &ka in pa {
                for &kb in pb {
                    if best.is_none_or(|(x, y)| ka.abs_diff(kb) < x.abs_diff(y)) {
                        best = Some((ka, kb));
                    }
                }
            }
            let Some((ka, kb)) = best else { continue };
            if ka.abs_diff(kb) > LEARN_MAX_GAP {
                continue;
            }
            let (lo, hi) = (ka.min(kb), ka.max(kb));
            let allowed = &ctx.stations[lo..=hi];
            let unlisted = path[i + 1..j]
                .iter()
                .filter_map(|&m| ctx.node_crs(crs_of, m))
                .filter(|c| !crs_names[*c as usize].starts_with('X') && !allowed.contains(c))
                .count();
            let limit = if hi - lo == 1 {
                LEARN_ADJACENT_UNLISTED
            } else {
                0
            };
            if unlisted > limit {
                continue;
            }
            learned.extend(path[i + 1..j].iter().copied());
        }
    }
    learned
}

fn train_facts(
    ctx: &LineCtx<'_>,
    crs_of: &[Option<u32>],
    path: &[u32],
    occ: &[(usize, u32)],
    learned: &HashSet<u32>,
) -> Facts {
    let Some(&(_, first)) = occ.first() else {
        return Facts::default();
    };
    // Chains of accepted stretches; keep the one with the most distinct
    // stations (the first on a tie).
    let mut best: Vec<u32> = Vec::new();
    let mut best_distinct = 0;
    let mut cur: Vec<u32> = vec![first];
    let mut consider = |chain: &Vec<u32>| {
        let distinct = distinct_in_order(chain).len();
        if distinct > best_distinct {
            best_distinct = distinct;
            best.clone_from(chain);
        }
    };
    for pair in occ.windows(2) {
        let ((i, a), (j, b)) = (pair[0], pair[1]);
        let unknown = path[i + 1..j]
            .iter()
            .filter(|m| !learned.contains(m))
            .count();
        if a != b && unknown <= UNKNOWN_SLACK {
            cur.push(b);
        } else {
            consider(&cur);
            cur = vec![b];
        }
    }
    consider(&cur);
    let run = distinct_in_order(&best);
    if run.len() < 2 {
        return Facts {
            longest: 1,
            ..Facts::default()
        };
    }
    let positions: Vec<usize> = run.iter().map(|&c| ctx.first_position(c)).collect();
    let span = positions.iter().max().copied().unwrap_or(0)
        - positions.iter().min().copied().unwrap_or(0)
        + 1;
    let whole = path
        .iter()
        .all(|t| learned.contains(t) || ctx.line_crs(crs_of, *t).is_some());
    Facts {
        longest: run.len(),
        ends: Some((best[0], best[best.len() - 1])),
        whole,
        span,
    }
}

fn distinct_in_order(chain: &[u32]) -> Vec<u32> {
    let mut seen = HashSet::new();
    chain.iter().copied().filter(|c| seen.insert(*c)).collect()
}

/// The first booked public call of `calling_points` at a TIPLOC for which
/// `is_line_station` is true (normalized TIPLOC): the public departure,
/// else the public arrival (a terminating call). `None` when the train
/// makes no public call on the line.
pub fn line_due(
    calling_points: &[CallingPoint],
    is_line_station: impl Fn(&str) -> bool,
) -> Option<LineDue> {
    calling_points.iter().find_map(|cp| {
        if !is_line_station(normalize_tiploc(&cp.tiploc)) {
            return None;
        }
        if let Some(time) = cp.public_departure {
            return Some(LineDue {
                time,
                day_offset: cp.departure_day_offset(),
            });
        }
        cp.public_arrival.map(|time| LineDue {
            time,
            day_offset: cp.day_offset,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::{Activity, CallingPointKind, StpIndicator, Tiploc};

    /// `"XA*"` calls at TIPLOC `XA`, `"XA"` passes it.
    fn schedule(uid: &str, operator: &str, status: char, path: &[&str]) -> ResolvedSchedule {
        let last = path.len() - 1;
        let calling_points = path
            .iter()
            .enumerate()
            .map(|(i, token)| {
                let (tiploc, calls) = token
                    .strip_suffix('*')
                    .map_or((*token, false), |t| (t, true));
                let time = calls.then(|| NaiveTime::from_hms_opt(10, 0, 0).unwrap());
                CallingPoint {
                    tiploc: Tiploc::new(tiploc),
                    kind: if i == 0 {
                        CallingPointKind::Origin
                    } else if i == last {
                        CallingPointKind::Terminate
                    } else {
                        CallingPointKind::Intermediate
                    },
                    booked_arrival: None,
                    booked_departure: time,
                    is_half_minute_arrival: false,
                    is_half_minute_departure: false,
                    day_offset: 0,
                    activity: Activity::default(),
                    public_arrival: None,
                    public_departure: time,
                    platform: None,
                    booked_pass: None,
                }
            })
            .collect();
        ResolvedSchedule {
            uid: uid.to_string(),
            stp_indicator: StpIndicator::Permanent,
            cancelled: false,
            calling_points,
            operator_atoc: Some(operator.to_string()),
            headcode: None,
            rsid: None,
            train_status: Some(status),
        }
    }

    fn line(id: &str, operators: &[&str], stations: &[&str]) -> MembershipLine {
        MembershipLine {
            id: id.to_string(),
            operators: operators.iter().map(ToString::to_string).collect(),
            stations: stations.iter().map(ToString::to_string).collect(),
            ..MembershipLine::default()
        }
    }

    /// TIPLOC `XA` is station `A`, `XB` is `B`, ...; `JUNC` is a junction
    /// the crosswalk files under `A`; `J1`/`J2` have no CRS at all.
    fn crosswalk() -> HashMap<String, String> {
        ["A", "B", "C", "D", "E", "H"]
            .iter()
            .map(|c| (format!("X{c}"), (*c).to_string()))
            .chain([("JUNC".to_string(), "A".to_string())])
            .collect()
    }

    fn member<'a>(
        out: &'a [LineMembers],
        day: &DayTrains,
        line: usize,
        uid: &str,
    ) -> Option<&'a Membership> {
        out[line]
            .members
            .iter()
            .find(|(t, _)| day.uid(*t) == uid)
            .map(|(_, m)| m)
    }

    fn scope_of(out: &[LineMembers], day: &DayTrains, line: usize, uid: &str) -> Option<LineScope> {
        member(out, day, line, uid).map(|m| m.scope)
    }

    #[test]
    fn a_run_along_the_line_is_line_and_a_hub_call_is_touch() {
        let mut day = DayTrains::new();
        day.push(&schedule(
            "T1",
            "AA",
            'P',
            &["XA*", "J1", "XB*", "XC*", "XD*"],
        ));
        day.push(&schedule("T2", "AA", 'P', &["XH*", "XA*", "J2"]));
        let lines = [line("l", &["AA"], &["A", "B", "C", "D"])];
        let out = classify(&day, &lines, &crosswalk());
        assert_eq!(scope_of(&out, &day, 0, "T2"), Some(LineScope::Touch));
        let m = member(&out, &day, 0, "T1").unwrap();
        assert_eq!(m.scope, LineScope::Line);
        assert_eq!(m.run_first_crs.as_deref(), Some("A"));
        assert_eq!(m.run_last_crs.as_deref(), Some("D"));
        assert_eq!(m.direction, Some(RunDirection::Down));
    }

    #[test]
    fn another_operator_and_a_bus_along_the_line_are_shared() {
        let mut day = DayTrains::new();
        day.push(&schedule("OWN", "AA", 'P', &["XA*", "XB*", "XC*", "XD*"]));
        day.push(&schedule("OTHER", "BB", 'P', &["XD*", "XC*", "XB*", "XA*"]));
        day.push(&schedule("BUS", "AA", 'B', &["XA*", "XB*", "XC*", "XD*"]));
        let lines = [line("l", &["AA"], &["A", "B", "C", "D"])];
        let out = classify(&day, &lines, &crosswalk());
        assert_eq!(scope_of(&out, &day, 0, "OWN"), Some(LineScope::Line));
        assert_eq!(scope_of(&out, &day, 0, "OTHER"), Some(LineScope::Shared));
        assert_eq!(scope_of(&out, &day, 0, "BUS"), Some(LineScope::Shared));
        assert_eq!(
            member(&out, &day, 0, "OTHER").unwrap().direction,
            Some(RunDirection::Up)
        );
    }

    #[test]
    fn with_none_of_its_operators_running_any_operator_counts() {
        // The Island Line fallback: the catalogue says AA, the trains run as CC.
        let mut day = DayTrains::new();
        day.push(&schedule("T1", "CC", 'P', &["XA*", "XB*", "XC*"]));
        let lines = [line("l", &["AA"], &["A", "B", "C"])];
        let out = classify(&day, &lines, &crosswalk());
        assert_eq!(scope_of(&out, &day, 0, "T1"), Some(LineScope::Line));
    }

    #[test]
    fn best_fit_and_trunk_for() {
        // Two lines of AA's share A-B. T1 runs the main line end to end;
        // T2 is the branch train, which only spans two of main's five.
        let mut day = DayTrains::new();
        day.push(&schedule(
            "T1",
            "AA",
            'P',
            &["XA*", "XB*", "XC*", "XD*", "XE*"],
        ));
        day.push(&schedule("T2", "AA", 'P', &["XA*", "XB*", "XH*"]));
        let mut lines = [
            line("main", &["AA"], &["A", "B", "C", "D", "E"]),
            line("branch", &["AA"], &["A", "B", "H"]),
        ];
        let out = classify(&day, &lines, &crosswalk());
        assert_eq!(scope_of(&out, &day, 0, "T1"), Some(LineScope::Line));
        assert_eq!(scope_of(&out, &day, 1, "T1"), Some(LineScope::Shared));
        assert_eq!(scope_of(&out, &day, 1, "T2"), Some(LineScope::Line));
        assert_eq!(scope_of(&out, &day, 0, "T2"), Some(LineScope::Shared));

        // main is the trunk of branch: T2 is one of main's own trains too.
        lines[0].trunk_for = vec!["branch".to_string()];
        let out = classify(&day, &lines, &crosswalk());
        assert_eq!(scope_of(&out, &day, 0, "T2"), Some(LineScope::Line));
    }

    #[test]
    fn a_tiploc_nobody_calls_at_is_not_a_station() {
        // JUNC is filed under A by the crosswalk, but is only ever passed.
        let mut day = DayTrains::new();
        day.push(&schedule("T1", "AA", 'P', &["XH*", "JUNC", "XE*"]));
        let lines = [line("l", &["AA"], &["A", "B"])];
        let out = classify(&day, &lines, &crosswalk());
        assert!(out[0].members.is_empty());
        assert!(!day.is_called("JUNC"));
        assert!(day.is_called("XH"));
    }

    #[test]
    fn crs_aliases_count_as_the_catalogue_station() {
        let mut day = DayTrains::new();
        day.push(&schedule("T1", "AA", 'P', &["XH*", "XB*"]));
        let mut l = line("l", &["AA"], &["A", "B"]);
        l.crs_aliases.insert("H".to_string(), "A".to_string());
        let out = classify(&day, &[l], &crosswalk());
        let m = member(&out, &day, 0, "T1").unwrap();
        assert_eq!(m.scope, LineScope::Line);
        assert_eq!(m.run_first_crs.as_deref(), Some("A"));
        assert!(out[0].station_tiplocs.contains("XH"));
    }

    #[test]
    fn a_circle_is_a_loop() {
        let mut day = DayTrains::new();
        day.push(&schedule(
            "CIRCLE",
            "AA",
            'P',
            &["XA*", "XB*", "XC*", "XA*"],
        ));
        let lines = [line("l", &["AA"], &["A", "B", "C"])];
        let out = classify(&day, &lines, &crosswalk());
        let m = member(&out, &day, 0, "CIRCLE").unwrap();
        assert_eq!(m.direction, Some(RunDirection::Loop));
        assert_eq!(m.run_first_crs.as_deref(), Some("A"));
        assert_eq!(m.run_last_crs.as_deref(), Some("A"));
    }

    #[test]
    fn line_due_is_the_first_public_call_on_the_line() {
        let s = schedule("T1", "AA", 'P', &["XH*", "XB", "XC*", "XD*"]);
        // XB is passed, so the first public call on the line is XC.
        let due = line_due(&s.calling_points, |t| t == "XB" || t == "XC");
        assert_eq!(
            due,
            Some(LineDue {
                time: NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
                day_offset: 0
            })
        );
        assert_eq!(line_due(&s.calling_points, |t| t == "XB"), None);
    }

    #[test]
    fn scope_and_direction_serialize_lower_case() {
        assert_eq!(
            serde_json::to_string(&LineScope::Shared).unwrap(),
            "\"shared\""
        );
        assert_eq!(
            serde_json::to_string(&RunDirection::Loop).unwrap(),
            "\"loop\""
        );
        assert_eq!(LineScope::Touch.as_str(), "touch");
    }
}
