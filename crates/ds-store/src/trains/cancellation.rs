//! The train-level `cancellation` model (DS-181), served by the api on
//! `GET /Train/by-uid/{uid}/{date}` (`trains::PublicTrainState`),
//! `GET /Train/{trackingId}` and each journey leg's `trackedTrainState`
//! (`train_tracking::TrackedTrainState`), with each journey stop's
//! `cancelled` flag beside it. It lives here (moved from the api's
//! `data::cancellation`) so the notifier words its cancellation pushes by
//! the same rules ([`derive_from_timetable`]), and the list rows' compact
//! form ([`CancellationSummary`]) is cut from the same model.
//!
//! ```json
//! "cancellation": {
//!   "kind": "partial",
//!   "startsFrom": null,
//!   "terminatesAt": { "crs": "RAY", "name": "Raynes Park", "stopIndex": 5 },
//!   "from": { "crs": "RAY", "name": "Raynes Park", "stopIndex": 5 },
//!   "to": { "crs": "WAT", "name": "London Waterloo", "stopIndex": 11 },
//!   "sections": [{ "from": { ... }, "to": { ... } }],
//!   "reason": { "code": "JP", "text": "Failure to maintain vegetation ...", "source": "trust" },
//!   "at": "2026-10-25T20:50:00Z"
//! }
//! ```
//!
//! `null` when no part of the train's run is cancelled, including a train
//! that was cancelled and then reinstated from where it was cancelled.
//!
//! **Which calls are cancelled.** The stored TRUST messages
//! (`train_reasons`: the latest `0002` Cancellation, `0005` Reinstatement
//! and `0006` Change of Origin, each with its location) are applied in the
//! order they were made (`event_at`; unknown sorts first):
//!
//! - `0002`, `EN ROUTE` or `OUT OF PLAN` at a call of the train: every
//!   later call is cancelled. The train runs to that call and terminates
//!   there. Any other type (`AT ORIGIN`, `ON CALL`), or an en route one
//!   whose location is not a call: the whole train, or everything after
//!   the last call it reported.
//! - `0005` at a call: that call and every later one run again. Without a
//!   known location: the calls the cancellation took away run again.
//! - `0006` at a call: the train starts there. Earlier calls it has not
//!   already made are cancelled, and that call and every later one run.
//!
//! A call TRUST reported (an actual arrival or departure) is never
//! cancelled.
//!
//! A cancellation that no later `0005` or `0006` explains is dropped as
//! stale when the train reported a call the cancellation took away (it
//! evidently ran there), or when the status is no longer `cancelled` and
//! the train has reported nothing at all (a `0005` the ingest missed).
//!
//! When the train status is `cancelled` but nothing stored places it (no
//! `0002`: the message predates DS-181's ingest or could not be matched to
//! the train; or the stored one was dropped as stale), the calls after the
//! last reported one are cancelled.
//!
//! **Summary.** Only public calls count (a passing point or a
//! working-only call is never named): `kind` is `full` when every public
//! call is cancelled, else `partial`. `sections` are the runs of cancelled
//! calls, each named by the calls either side of it ("cancelled between
//! `from` and `to`"), or by its own first or last call at either end of the
//! route. `startsFrom` and `terminatesAt` name the first and last calls
//! still served when the train starts or ends short; `from` and `to` repeat
//! the only section's ends, and are `null` when there are several.
//! `stopIndex` indexes the response's `journeyStops`.
//!
//! **Reason source precedence** (`reason.source`), best first:
//!
//! 1. `darwin`: Darwin Push Port `cancelReason`, the operator's
//!    passenger text for this service. Not served yet: Darwin ingest is
//!    DS-152. It will slot in above `ldbws` here.
//! 2. `ldbws`: the same Darwin text read off a live LDBWS departure board
//!    (`journeyStops[].board.cancelReason`, a board polled in the last 10
//!    minutes that lists this train as cancelled). Only while the train is
//!    still on a board, so it gives way to `trust` once the train has gone.
//! 3. `trust`: the TRUST `0002` (else `0006`) delay attribution code, with
//!    its Network Rail glossary text (`train_reasons::reason_text`, `null`
//!    for an unknown or system code such as `PD`).
//!
//! `reason` is `null` when no source has one. `at` is when the cancellation
//! (else the change of origin) was made, by TRUST's own timestamp.

use crate::backlog::reasons::{ReasonEvent, StoredReasons, reason_text};
use crate::trains::types::{JourneyStop, tiploc_key};
use chrono::{DateTime, Utc};
use serde::Serialize;

/// Whether every public call is cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum CancellationKind {
    Full,
    Partial,
}

/// Where a cancellation's reason came from; see the module doc for the
/// precedence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ReasonSource {
    /// Darwin Push Port (DS-152). Not produced yet.
    Darwin,
    Ldbws,
    Trust,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancellationReason {
    /// The source's own code: a TRUST delay attribution code (`"JP"`), or
    /// `null` for LDBWS, which serves text only.
    pub code: Option<String>,
    /// Passenger-readable text: Darwin's own sentence for `ldbws`, the
    /// glossary description for `trust`. `null` for a TRUST code with no
    /// text (`PD`, `ZW`, or one missing from the 2021 glossary).
    pub text: Option<String>,
    pub source: ReasonSource,
}

/// A call named by a cancellation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancellationStop {
    pub crs: Option<String>,
    pub name: Option<String>,
    /// Index into the response's `journeyStops`.
    pub stop_index: usize,
}

/// A part of the route the train does not run: the calls strictly between
/// `from` and `to` are cancelled, and so is `from` at the origin end or `to`
/// at the destination end.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelledSection {
    pub from: CancellationStop,
    pub to: CancellationStop,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Cancellation {
    pub kind: CancellationKind,
    /// The first call still served, when the train starts short of its
    /// booked origin (a change of origin, or cancelled at the start).
    pub starts_from: Option<CancellationStop>,
    /// The last call still served, when the train terminates short of its
    /// booked destination.
    pub terminates_at: Option<CancellationStop>,
    /// The only section's `from`, or `null` with several.
    pub from: Option<CancellationStop>,
    /// The only section's `to`, or `null` with several.
    pub to: Option<CancellationStop>,
    /// In route order; empty when the train has no calling points.
    pub sections: Vec<CancelledSection>,
    pub reason: Option<CancellationReason>,
    /// When the cancellation (else the change of origin) was made.
    pub at: Option<DateTime<Utc>>,
}

/// Sets each stop's `cancelled` flag and returns the train's
/// [`Cancellation`]. `stops` is `None` when the train has no calling points
/// (a pending subscription); see the module doc for the rules.
pub fn derive(
    status: Option<&str>,
    stored: Option<&StoredReasons>,
    stops: Option<&mut [JourneyStop]>,
) -> Option<Cancellation> {
    let status_cancelled = status == Some("cancelled");
    let empty = StoredReasons::default();
    let stored = stored.unwrap_or(&empty);
    let Some(stops) = stops else {
        // No calls to place it on: only a whole-train answer.
        return status_cancelled.then(|| Cancellation {
            kind: CancellationKind::Full,
            starts_from: None,
            terminates_at: None,
            from: None,
            to: None,
            sections: Vec::new(),
            reason: trust_reason(stored, true),
            at: stored.cancellation.as_ref().and_then(|c| c.event_at),
        });
    };
    for stop in stops.iter_mut() {
        stop.cancelled = false;
    }
    let mask = cancelled_calls(status_cancelled, stored, stops);
    for (stop, cancelled) in stops.iter_mut().zip(&mask) {
        stop.cancelled = cancelled.is_some();
    }
    let used_cancellation = mask.contains(&Some(Cause::Cancellation));
    let used_origin = mask.contains(&Some(Cause::ChangeOfOrigin));
    summarise(stops, stored, used_cancellation, used_origin)
}

/// Why a call is cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cause {
    Cancellation,
    ChangeOfOrigin,
}

fn reached(stop: &JourneyStop) -> bool {
    stop.actual_arrival.is_some() || stop.actual_departure.is_some()
}

/// The indices of the calls at `event`'s location: by TIPLOC, else by CRS.
fn calls_at(stops: &[JourneyStop], event: &ReasonEvent) -> Vec<usize> {
    let by = |matches: &dyn Fn(&JourneyStop) -> bool| -> Vec<usize> {
        stops
            .iter()
            .enumerate()
            .filter(|(_, stop)| matches(stop))
            .map(|(index, _)| index)
            .collect()
    };
    if let Some(tiploc) = event.loc_tiploc.as_deref().map(tiploc_key) {
        let found = by(&|stop| stop.tiploc.as_deref() == Some(tiploc.as_str()));
        if !found.is_empty() {
            return found;
        }
    }
    if let Some(crs) = event.loc_crs.as_deref().map(str::trim) {
        return by(&|stop| {
            stop.crs
                .as_deref()
                .is_some_and(|c| c.eq_ignore_ascii_case(crs))
        });
    }
    Vec::new()
}

#[derive(Clone, Copy)]
enum Kind {
    Cancellation,
    Reinstatement,
    ChangeOfOrigin,
}

/// Each call's cancellation cause, `None` when it runs.
fn cancelled_calls(
    status_cancelled: bool,
    stored: &StoredReasons,
    stops: &[JourneyStop],
) -> Vec<Option<Cause>> {
    let n = stops.len();
    let last_reached = stops.iter().rposition(reached);
    let mut mask: Vec<Option<Cause>> = vec![None; n];

    let mut events: Vec<(Kind, &ReasonEvent)> = [
        (Kind::Cancellation, stored.cancellation.as_ref()),
        (Kind::Reinstatement, stored.reinstatement.as_ref()),
        (Kind::ChangeOfOrigin, stored.change_of_origin.as_ref()),
    ]
    .into_iter()
    .filter_map(|(kind, event)| event.map(|e| (kind, e)))
    .collect();
    // Oldest first; a message with no time sorts as oldest. Stable, so two
    // untimed messages keep 0002, 0005, 0006 order.
    events.sort_by_key(|(_, event)| event.event_at);

    // Where the latest cancellation started cutting, for a reinstatement
    // with no known location.
    let mut cut_from: Option<usize> = None;
    for (kind, event) in &events {
        match kind {
            Kind::Cancellation => {
                let en_route =
                    matches!(event.canx_type.as_deref(), Some("EN ROUTE" | "OUT OF PLAN"));
                let at = calls_at(stops, event);
                // A loop visits a place twice: the visit the train has got
                // to, else the first.
                let at = at
                    .iter()
                    .rev()
                    .find(|&&i| last_reached.is_some_and(|r| i <= r))
                    .or_else(|| at.first())
                    .copied();
                let start = match (en_route, at) {
                    (true, Some(i)) => i + 1,
                    (true, None) => last_reached.map_or(0, |r| r + 1),
                    (false, _) => 0,
                };
                for slot in mask.iter_mut().skip(start) {
                    *slot = Some(Cause::Cancellation);
                }
                cut_from = Some(start);
            }
            Kind::Reinstatement => {
                let from = calls_at(stops, event)
                    .into_iter()
                    .find(|&i| cut_from.is_none_or(|c| i + 1 >= c))
                    .or(cut_from)
                    .unwrap_or(0);
                for slot in mask.iter_mut().skip(from) {
                    if *slot == Some(Cause::Cancellation) {
                        *slot = None;
                    }
                }
            }
            Kind::ChangeOfOrigin => {
                let Some(origin) = calls_at(stops, event).into_iter().next() else {
                    continue;
                };
                for (index, slot) in mask.iter_mut().enumerate() {
                    let runs = index >= origin || (slot.is_none() && reached(&stops[index]));
                    *slot = if runs {
                        None
                    } else {
                        Some(slot.unwrap_or(Cause::ChangeOfOrigin))
                    };
                }
            }
        }
    }

    // A later 0005 or 0006 already says what happened after the
    // cancellation; only an unexplained one is checked for staleness.
    let superseded = stored.cancellation.as_ref().is_some_and(|c| {
        [&stored.reinstatement, &stored.change_of_origin]
            .into_iter()
            .flatten()
            .any(|later| later.event_at >= c.event_at)
    });
    let mut dropped = false;
    if stored.cancellation.is_some() && !superseded {
        let ran_through_it = stops
            .iter()
            .zip(&mask)
            .any(|(stop, m)| *m == Some(Cause::Cancellation) && reached(stop));
        let lifted_without_report = !status_cancelled && last_reached.is_none();
        if ran_through_it || lifted_without_report {
            for slot in &mut mask {
                if *slot == Some(Cause::Cancellation) {
                    *slot = None;
                }
            }
            dropped = true;
        }
    }
    // TRUST still calls the train cancelled but nothing stored places it:
    // everything after the last call it reported.
    if status_cancelled && (stored.cancellation.is_none() || dropped) {
        let start = last_reached.map_or(0, |r| r + 1);
        for slot in mask.iter_mut().skip(start) {
            slot.get_or_insert(Cause::Cancellation);
        }
    }
    for (stop, slot) in stops.iter().zip(&mut mask) {
        if reached(stop) {
            *slot = None;
        }
    }
    mask
}

fn has_public_time(stop: &JourneyStop) -> bool {
    stop.timetable.public_arrival.is_some() || stop.timetable.public_departure.is_some()
}

/// A call a passenger can use: not a passing point or a working-only call.
/// A train stored before public times were kept has none at all; then any
/// call at a station counts.
fn is_public(stop: &JourneyStop, any_public_time: bool) -> bool {
    !stop.timetable.passing_point
        && if any_public_time {
            has_public_time(stop)
        } else {
            stop.crs.is_some()
        }
}

fn named(stops: &[JourneyStop], index: usize) -> CancellationStop {
    CancellationStop {
        crs: stops[index].crs.clone(),
        name: stops[index].name.clone(),
        stop_index: index,
    }
}

fn summarise(
    stops: &[JourneyStop],
    stored: &StoredReasons,
    used_cancellation: bool,
    used_origin: bool,
) -> Option<Cancellation> {
    let any_public_time = stops.iter().any(has_public_time);
    let public: Vec<usize> = (0..stops.len())
        .filter(|&i| is_public(&stops[i], any_public_time))
        .collect();
    let cancelled = |p: usize| stops[public[p]].cancelled;
    if !(0..public.len()).any(cancelled) {
        return None;
    }
    let mut sections = Vec::new();
    let mut p = 0;
    while p < public.len() {
        if !cancelled(p) {
            p += 1;
            continue;
        }
        let first = p;
        while p + 1 < public.len() && cancelled(p + 1) {
            p += 1;
        }
        let last = p;
        let from = if first == 0 { first } else { first - 1 };
        let to = if last + 1 < public.len() {
            last + 1
        } else {
            last
        };
        sections.push(CancelledSection {
            from: named(stops, public[from]),
            to: named(stops, public[to]),
        });
        p += 1;
    }
    let full = (0..public.len()).all(cancelled);
    let (starts_from, terminates_at) = if full {
        (None, None)
    } else {
        let first_served = (0..public.len()).find(|&p| !cancelled(p));
        let last_served = (0..public.len()).rfind(|&p| !cancelled(p));
        (
            first_served
                .filter(|_| cancelled(0))
                .map(|p| named(stops, public[p])),
            last_served
                .filter(|_| cancelled(public.len() - 1))
                .map(|p| named(stops, public[p])),
        )
    };
    let (from, to) = match sections.as_slice() {
        [only] => (Some(only.from.clone()), Some(only.to.clone())),
        _ => (None, None),
    };
    let reason = board_reason(stops).or_else(|| trust_reason(stored, used_cancellation));
    let at = if used_cancellation {
        stored.cancellation.as_ref().and_then(|c| c.event_at)
    } else if used_origin {
        stored.change_of_origin.as_ref().and_then(|o| o.event_at)
    } else {
        None
    };
    Some(Cancellation {
        kind: if full {
            CancellationKind::Full
        } else {
            CancellationKind::Partial
        },
        starts_from,
        terminates_at,
        from,
        to,
        sections,
        reason,
        at,
    })
}

/// Darwin's own text from a live LDBWS board that lists this train as
/// cancelled: a cancelled call's board first, else any.
fn board_reason(stops: &[JourneyStop]) -> Option<CancellationReason> {
    let text_of = |stop: &JourneyStop| {
        let board = stop.board.as_ref().filter(|b| b.is_cancelled)?;
        let text = board.cancel_reason.as_deref()?.trim();
        (!text.is_empty()).then(|| text.to_string())
    };
    stops
        .iter()
        .filter(|s| s.cancelled)
        .find_map(text_of)
        .or_else(|| stops.iter().find_map(text_of))
        .map(|text| CancellationReason {
            code: None,
            text: Some(text),
            source: ReasonSource::Ldbws,
        })
}

/// The TRUST code behind the cancellation: the `0002`'s when it is in
/// force, else the `0006`'s.
fn trust_reason(stored: &StoredReasons, used_cancellation: bool) -> Option<CancellationReason> {
    let code = if used_cancellation {
        stored
            .cancel_code()
            .or_else(|| stored.change_of_origin_code())
    } else {
        stored.change_of_origin_code()
    }?;
    Some(CancellationReason {
        code: Some(code.to_string()),
        text: reason_text(code).map(str::to_string),
        source: ReasonSource::Trust,
    })
}

/// A place a [`CancellationSummary`] names. No `stopIndex`: a list row
/// carries no stops to index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancellationPlace {
    pub crs: Option<String>,
    pub name: Option<String>,
}

impl From<&CancellationStop> for CancellationPlace {
    fn from(stop: &CancellationStop) -> Self {
        Self {
            crs: stop.crs.clone(),
            name: stop.name.clone(),
        }
    }
}

impl CancellationPlace {
    /// The place's name, else its CRS.
    pub fn label(&self) -> Option<&str> {
        [&self.name, &self.crs]
            .into_iter()
            .flatten()
            .map(|text| text.trim())
            .find(|text| !text.is_empty())
    }
}

/// The compact form of a [`Cancellation`] for rows that carry no stops
/// (`GET /Journeys/mine`, `GET /Train/mine`) and for push copy: enough to
/// say "Terminates at York" or "Starts from York" rather than plain
/// "Cancelled", and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancellationSummary {
    pub kind: CancellationKind,
    pub starts_from: Option<CancellationPlace>,
    pub terminates_at: Option<CancellationPlace>,
    pub reason: Option<CancellationReason>,
}

impl From<&Cancellation> for CancellationSummary {
    fn from(c: &Cancellation) -> Self {
        Self {
            kind: c.kind,
            starts_from: c.starts_from.as_ref().map(CancellationPlace::from),
            terminates_at: c.terminates_at.as_ref().map(CancellationPlace::from),
            reason: c.reason.clone(),
        }
    }
}

impl CancellationSummary {
    /// The reason's passenger text, when there is one.
    pub fn reason_text(&self) -> Option<&str> {
        self.reason
            .as_ref()?
            .text
            .as_deref()
            .map(str::trim)
            .filter(|text| !text.is_empty())
    }
}

/// [`derive`] for train `trains_id` from what the database holds, without
/// the api's live overlay: its stored calling points
/// (`trains.calling_points`, named through `tiploc_crs`/`stanox_crs` and
/// `stations`), the calls TRUST reported at (a `0003` with an actual time,
/// by CRS: the first visit of each reported station) and its stored TRUST
/// messages (`train_reasons`). For the notifier's push copy. There is no
/// LDBWS board overlay here, so the reason is always the TRUST one. A
/// train with no stored calling points gets the whole-train answer
/// [`derive`] gives without stops. `None` when nothing is cancelled or the
/// train has no row.
pub async fn derive_from_timetable(
    pool: &sqlx::PgPool,
    trains_id: i64,
) -> anyhow::Result<Option<Cancellation>> {
    type TrainRow = (Option<serde_json::Value>, chrono::NaiveDate, Option<String>);
    let row: Option<TrainRow> = sqlx::query_as(
        "SELECT tr.calling_points, tr.service_date, cs.status \
         FROM trains tr LEFT JOIN train_current_state cs ON cs.trains_id = tr.id \
         WHERE tr.id = $1",
    )
    .bind(trains_id)
    .fetch_optional(pool)
    .await?;
    let Some((calling_points, service_date, status)) = row else {
        return Ok(None);
    };
    let stored = crate::backlog::reasons::reasons_for_trains(pool, &[trains_id])
        .await?
        .remove(&trains_id);
    let calling_points_raw: Vec<crate::trains::types::RawCallingPoint> = match calling_points {
        Some(json) => serde_json::from_value(json).unwrap_or_else(|err| {
            tracing::warn!(trains_id, error = %err, "stored calling points do not parse");
            Vec::new()
        }),
        None => Vec::new(),
    };
    if calling_points_raw.is_empty() {
        return Ok(derive(status.as_deref(), stored.as_ref(), None));
    }
    let mut tiplocs: Vec<String> = calling_points_raw
        .iter()
        .map(|cp| tiploc_key(&cp.tiploc))
        .collect();
    tiplocs.sort();
    tiplocs.dedup();
    let crs_by_tiploc = crate::reference::crs_for_tiplocs_batch(pool, &tiplocs).await?;
    let mut stops: Vec<JourneyStop> = calling_points_raw
        .iter()
        .map(|cp| {
            let crs = crs_by_tiploc
                .get(&tiploc_key(&cp.tiploc))
                .filter(|crs| crate::reference::is_bookable_crs(crs))
                .cloned();
            JourneyStop::from_calling_point(cp, crs, service_date)
        })
        .collect();
    let mut codes: Vec<String> = stops.iter().filter_map(|s| s.crs.clone()).collect();
    codes.sort();
    codes.dedup();
    let names: std::collections::HashMap<String, String> = sqlx::query_as::<_, (String, String)>(
        "SELECT TRIM(crs), name FROM stations WHERE crs = ANY($1::bpchar[])",
    )
    .bind(&codes)
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect();
    let reported: Vec<(String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT UPPER(TRIM(loc_crs)), MIN(actual_timestamp) FROM train_movement_events \
         WHERE trains_id = $1 AND msg_type = '0003' AND actual_timestamp IS NOT NULL \
           AND loc_crs IS NOT NULL \
         GROUP BY 1",
    )
    .bind(trains_id)
    .fetch_all(pool)
    .await?;
    apply_names_and_reports(&mut stops, &names, &reported);
    Ok(derive(status.as_deref(), stored.as_ref(), Some(&mut stops)))
}

/// Names each stop from `names` (CRS to station name) and marks the first
/// call at each `reported` station as reached at that time.
fn apply_names_and_reports(
    stops: &mut [JourneyStop],
    names: &std::collections::HashMap<String, String>,
    reported: &[(String, DateTime<Utc>)],
) {
    for stop in stops.iter_mut() {
        if let Some(crs) = &stop.crs {
            stop.name = names.get(&crs.to_uppercase()).cloned();
        }
    }
    for (crs, at) in reported {
        if let Some(stop) = stops.iter_mut().find(|s| {
            s.crs
                .as_deref()
                .is_some_and(|c| c.trim().eq_ignore_ascii_case(crs))
        }) {
            stop.actual_departure = Some(*at);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trains::types::{StopBoard, StopStatus, StopTimetable};

    /// A public call at `tiploc` (CRS the first three letters), booked at
    /// 08:`minute`.
    fn call(tiploc: &str, minute: u32) -> JourneyStop {
        let at: DateTime<Utc> = format!("2031-05-06T08:{minute:02}:00Z").parse().unwrap();
        JourneyStop {
            crs: Some(tiploc[..3].to_string()),
            name: Some(format!("{tiploc} station")),
            location_type: None,
            parent_crs: None,
            tiploc: Some(tiploc.to_string()),
            kind: None,
            actual_arrival: None,
            actual_departure: None,
            estimated_arrival: None,
            estimated_departure: None,
            estimate_source: None,
            last_event_type: None,
            variation_status: None,
            delay_minutes: None,
            delay_basis: None,
            stop_status: StopStatus::Scheduled,
            skip_source: None,
            platform: None,
            planned_platform: None,
            platform_changed: false,
            platform_status: None,
            platform_evidence: None,
            booked_platform: None,
            platform_basis: None,
            platform_suppressed: false,
            display_platform: None,
            live_status: None,
            late_minutes: None,
            cancelled: false,
            board: None,
            timetable: StopTimetable {
                working_minute_arrival: Some(at),
                working_minute_departure: Some(at),
                public_arrival: Some(at),
                public_departure: Some(at),
                working_arrival: Some(at),
                working_departure: Some(at),
                working_pass: None,
                can_board: true,
                can_alight: true,
                request_stop: false,
                passing_point: false,
            },
        }
    }

    /// Five calls AAA..EEE, the train reported at the first `reached`.
    fn route(reached: usize) -> Vec<JourneyStop> {
        let mut stops: Vec<JourneyStop> = ["AAAX", "BBBX", "CCCX", "DDDX", "EEEX"]
            .iter()
            .enumerate()
            .map(|(i, t)| call(t, u32::try_from(i).unwrap() * 10))
            .collect();
        for stop in stops.iter_mut().take(reached) {
            stop.actual_departure = stop.timetable.public_departure;
        }
        stops
    }

    fn event(code: &str, canx_type: Option<&str>, tiploc: Option<&str>, at: &str) -> ReasonEvent {
        ReasonEvent {
            code: (!code.is_empty()).then(|| code.to_string()),
            canx_type: canx_type.map(str::to_string),
            loc_stanox: None,
            loc_tiploc: tiploc.map(str::to_string),
            loc_crs: None,
            event_at: Some(format!("2031-05-06T{at}:00Z").parse().unwrap()),
        }
    }

    fn crs(stop: Option<&CancellationStop>) -> Option<&str> {
        stop.and_then(|s| s.crs.as_deref())
    }

    fn mask(stops: &[JourneyStop]) -> Vec<bool> {
        stops.iter().map(|s| s.cancelled).collect()
    }

    #[test]
    fn a_full_cancellation_with_a_reason() {
        let stored = StoredReasons {
            cancellation: Some(event("IB", Some("AT ORIGIN"), Some("AAAX"), "07:30")),
            ..StoredReasons::default()
        };
        let mut stops = route(0);
        let c = derive(Some("cancelled"), Some(&stored), Some(&mut stops)).unwrap();
        assert_eq!(c.kind, CancellationKind::Full);
        assert_eq!(mask(&stops), vec![true; 5]);
        assert_eq!(c.starts_from, None);
        assert_eq!(c.terminates_at, None);
        assert_eq!(
            (crs(c.from.as_ref()), crs(c.to.as_ref())),
            (Some("AAA"), Some("EEE"))
        );
        let reason = c.reason.unwrap();
        assert_eq!(reason.code.as_deref(), Some("IB"));
        assert_eq!(
            reason.text.as_deref(),
            Some("Points failure (including no fault found)")
        );
        assert_eq!(reason.source, ReasonSource::Trust);
        assert_eq!(c.at, Some("2031-05-06T07:30:00Z".parse().unwrap()));
    }

    #[test]
    fn terminated_short_en_route() {
        // L83326's shape: reported to the third call, cancelled en route there.
        let stored = StoredReasons {
            cancellation: Some(event("JP", Some("EN ROUTE"), Some("CCCX"), "20:50")),
            ..StoredReasons::default()
        };
        let mut stops = route(3);
        stops[2].actual_departure = None;
        stops[2].actual_arrival = stops[2].timetable.public_arrival;
        let c = derive(Some("cancelled"), Some(&stored), Some(&mut stops)).unwrap();
        assert_eq!(c.kind, CancellationKind::Partial);
        assert_eq!(mask(&stops), vec![false, false, false, true, true]);
        assert_eq!(crs(c.terminates_at.as_ref()), Some("CCC"));
        assert_eq!(c.starts_from, None);
        assert_eq!(
            (crs(c.from.as_ref()), crs(c.to.as_ref())),
            (Some("CCC"), Some("EEE"))
        );
        assert_eq!(c.reason.unwrap().code.as_deref(), Some("JP"));
    }

    #[test]
    fn change_of_origin_starts_short() {
        let stored = StoredReasons {
            change_of_origin: Some(event("YI", None, Some("CCCX"), "07:50")),
            ..StoredReasons::default()
        };
        let mut stops = route(0);
        let c = derive(Some("en_route"), Some(&stored), Some(&mut stops)).unwrap();
        assert_eq!(c.kind, CancellationKind::Partial);
        assert_eq!(mask(&stops), vec![true, true, false, false, false]);
        assert_eq!(crs(c.starts_from.as_ref()), Some("CCC"));
        assert_eq!(c.terminates_at, None);
        assert_eq!(
            (crs(c.from.as_ref()), crs(c.to.as_ref())),
            (Some("AAA"), Some("CCC"))
        );
        assert_eq!(c.reason.unwrap().code.as_deref(), Some("YI"));
        assert_eq!(c.at, Some("2031-05-06T07:50:00Z".parse().unwrap()));
    }

    #[test]
    fn cancelled_at_origin_then_a_change_of_origin_starts_short_with_the_cancel_reason() {
        let stored = StoredReasons {
            cancellation: Some(event("M8", Some("AT ORIGIN"), Some("AAAX"), "07:40")),
            change_of_origin: Some(event("", None, Some("BBBX"), "07:45")),
            ..StoredReasons::default()
        };
        let mut stops = route(0);
        let c = derive(Some("en_route"), Some(&stored), Some(&mut stops)).unwrap();
        assert_eq!(mask(&stops), vec![true, false, false, false, false]);
        assert_eq!(crs(c.starts_from.as_ref()), Some("BBB"));
        // The 0002 cut everything, so its code is the cause.
        assert_eq!(c.reason.unwrap().code.as_deref(), Some("M8"));
    }

    #[test]
    fn cancelled_en_route_then_restarting_further_on_is_cancelled_between() {
        for later in ["reinstatement", "change of origin"] {
            let restart = event("", None, Some("DDDX"), "08:30");
            let stored = StoredReasons {
                cancellation: Some(event("TG", Some("EN ROUTE"), Some("BBBX"), "08:12")),
                reinstatement: (later == "reinstatement").then(|| restart.clone()),
                change_of_origin: (later == "change of origin").then_some(restart),
            };
            let mut stops = route(2);
            let c = derive(Some("en_route"), Some(&stored), Some(&mut stops)).unwrap();
            assert_eq!(
                mask(&stops),
                vec![false, false, true, false, false],
                "{later}"
            );
            assert_eq!(c.kind, CancellationKind::Partial);
            assert_eq!(c.starts_from, None, "{later}");
            assert_eq!(c.terminates_at, None, "{later}");
            assert_eq!(
                (crs(c.from.as_ref()), crs(c.to.as_ref())),
                (Some("BBB"), Some("DDD")),
                "{later}"
            );
            assert_eq!(c.reason.unwrap().code.as_deref(), Some("TG"), "{later}");
        }
    }

    #[test]
    fn reinstated_where_it_was_cancelled_is_not_cancelled() {
        for location in [Some("AAAX"), None] {
            let stored = StoredReasons {
                cancellation: Some(event("TG", Some("AT ORIGIN"), Some("AAAX"), "07:30")),
                reinstatement: Some(event("", None, location, "07:40")),
                ..StoredReasons::default()
            };
            let mut stops = route(0);
            assert_eq!(
                derive(Some("en_route"), Some(&stored), Some(&mut stops)),
                None
            );
            assert_eq!(mask(&stops), vec![false; 5]);
        }
    }

    #[test]
    fn a_cancellation_after_a_reinstatement_is_in_force() {
        let stored = StoredReasons {
            cancellation: Some(event("TG", Some("AT ORIGIN"), Some("AAAX"), "07:50")),
            reinstatement: Some(event("", None, Some("AAAX"), "07:40")),
            ..StoredReasons::default()
        };
        let mut stops = route(0);
        let c = derive(Some("cancelled"), Some(&stored), Some(&mut stops)).unwrap();
        assert_eq!(c.kind, CancellationKind::Full);
    }

    #[test]
    fn no_reason_given() {
        let stored = StoredReasons {
            cancellation: Some(event("", Some("AT ORIGIN"), Some("AAAX"), "07:30")),
            ..StoredReasons::default()
        };
        let mut stops = route(0);
        let c = derive(Some("cancelled"), Some(&stored), Some(&mut stops)).unwrap();
        assert_eq!(c.kind, CancellationKind::Full);
        assert_eq!(c.reason, None);

        // A system code is served without text.
        let stored = StoredReasons {
            cancellation: Some(event("PD", Some("ON CALL"), None, "07:30")),
            ..StoredReasons::default()
        };
        let c = derive(Some("cancelled"), Some(&stored), Some(&mut route(0))).unwrap();
        let reason = c.reason.unwrap();
        assert_eq!(reason.code.as_deref(), Some("PD"));
        assert_eq!(reason.text, None);
    }

    #[test]
    fn a_cancelled_status_without_a_stored_message_cuts_after_the_last_report() {
        let mut stops = route(2);
        let c = derive(Some("cancelled"), None, Some(&mut stops)).unwrap();
        assert_eq!(mask(&stops), vec![false, false, true, true, true]);
        assert_eq!(crs(c.terminates_at.as_ref()), Some("BBB"));
        assert_eq!(c.reason, None);
        assert_eq!(c.at, None);

        let mut stops = route(0);
        let c = derive(Some("cancelled"), None, Some(&mut stops)).unwrap();
        assert_eq!(c.kind, CancellationKind::Full);
    }

    #[test]
    fn a_stale_cancellation_is_dropped() {
        // The train ran through a call the cancellation took away, and no
        // 0005 was stored.
        let stored = StoredReasons {
            cancellation: Some(event("TG", Some("AT ORIGIN"), Some("AAAX"), "07:30")),
            ..StoredReasons::default()
        };
        let mut stops = route(3);
        assert_eq!(
            derive(Some("en_route"), Some(&stored), Some(&mut stops)),
            None
        );
        // Still cancelled by status: the cut falls after the last report.
        let mut stops = route(3);
        let c = derive(Some("cancelled"), Some(&stored), Some(&mut stops)).unwrap();
        assert_eq!(mask(&stops), vec![false, false, false, true, true]);
        assert_eq!(crs(c.terminates_at.as_ref()), Some("CCC"));
        // Not cancelled any more and nothing reported: a missed 0005.
        let mut stops = route(0);
        assert_eq!(
            derive(Some("awaiting_activation"), Some(&stored), Some(&mut stops)),
            None
        );
    }

    #[test]
    fn a_terminated_short_train_stays_cancelled_after_a_late_movement() {
        // A movement before the cut, processed after the 0002, flips the
        // status back to en_route; the cut still stands.
        let stored = StoredReasons {
            cancellation: Some(event("JP", Some("EN ROUTE"), Some("CCCX"), "08:25")),
            ..StoredReasons::default()
        };
        let mut stops = route(3);
        let c = derive(Some("en_route"), Some(&stored), Some(&mut stops)).unwrap();
        assert_eq!(crs(c.terminates_at.as_ref()), Some("CCC"));
    }

    #[test]
    fn an_ldbws_reason_outranks_trust() {
        let stored = StoredReasons {
            cancellation: Some(event("IB", Some("AT ORIGIN"), Some("AAAX"), "07:30")),
            ..StoredReasons::default()
        };
        let mut stops = route(0);
        stops[1].board = Some(StopBoard {
            delay_reason: None,
            cancel_reason: Some(
                "This train has been cancelled because of a fault with the signalling system"
                    .to_string(),
            ),
            is_cancelled: true,
            delay_minutes: None,
            estimated: "Cancelled".to_string(),
            observed_at: "2031-05-06T07:35:00Z".parse().unwrap(),
        });
        let reason = derive(Some("cancelled"), Some(&stored), Some(&mut stops))
            .unwrap()
            .reason
            .unwrap();
        assert_eq!(reason.source, ReasonSource::Ldbws);
        assert_eq!(reason.code, None);
        assert!(reason.text.unwrap().ends_with("signalling system"));
    }

    #[test]
    fn passing_points_are_never_named_or_counted() {
        let stored = StoredReasons {
            cancellation: Some(event("TG", Some("EN ROUTE"), Some("CCCX"), "08:25")),
            ..StoredReasons::default()
        };
        let mut stops = route(3);
        // The last call is a passing point: only DDD is a public cancelled call.
        stops[4].timetable.passing_point = true;
        let c = derive(Some("cancelled"), Some(&stored), Some(&mut stops)).unwrap();
        assert_eq!(c.sections.len(), 1);
        assert_eq!(crs(c.to.as_ref()), Some("DDD"));
        assert_eq!(crs(c.terminates_at.as_ref()), Some("CCC"));
    }

    #[test]
    fn a_train_without_public_times_counts_its_station_calls() {
        let stored = StoredReasons {
            cancellation: Some(event("TG", Some("EN ROUTE"), Some("CCCX"), "08:25")),
            ..StoredReasons::default()
        };
        let mut stops = route(3);
        for stop in &mut stops {
            stop.timetable.public_arrival = None;
            stop.timetable.public_departure = None;
        }
        let c = derive(Some("cancelled"), Some(&stored), Some(&mut stops)).unwrap();
        assert_eq!(crs(c.terminates_at.as_ref()), Some("CCC"));
    }

    #[test]
    fn a_pending_train_without_stops_is_only_ever_wholly_cancelled() {
        let stored = StoredReasons {
            cancellation: Some(event("TG", Some("AT ORIGIN"), None, "07:30")),
            ..StoredReasons::default()
        };
        let c = derive(Some("cancelled"), Some(&stored), None).unwrap();
        assert_eq!(c.kind, CancellationKind::Full);
        assert!(c.sections.is_empty());
        assert_eq!(derive(Some("en_route"), Some(&stored), None), None);
    }

    #[test]
    fn serializes_in_camel_case() {
        let stored = StoredReasons {
            cancellation: Some(event("JP", Some("EN ROUTE"), Some("CCCX"), "20:50")),
            ..StoredReasons::default()
        };
        let mut stops = route(3);
        let c = derive(Some("cancelled"), Some(&stored), Some(&mut stops)).unwrap();
        let json = serde_json::to_value(&c).unwrap();
        assert_eq!(json["kind"], "partial");
        assert_eq!(json["terminatesAt"]["crs"], "CCC");
        assert_eq!(json["terminatesAt"]["stopIndex"], 2);
        assert_eq!(json["startsFrom"], serde_json::Value::Null);
        assert_eq!(json["reason"]["source"], "trust");
        assert_eq!(json["sections"][0]["to"]["name"], "EEEX station");
        assert_eq!(json["at"], "2031-05-06T20:50:00Z");
    }
}
