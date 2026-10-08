//! `line_train_summaries`: the readers. The derivation and the writes
//! (the population upsert with its summaries) moved to
//! `ds_store::schedule::summaries` (ingest architecture plan 2a.2), which
//! is re-exported here so no call site changes; see its module doc for the
//! table and the derivation.

use std::collections::BTreeMap;

use anyhow::Result;
use sqlx::PgPool;

pub use ds_store::schedule::summaries::*;

/// Which rows of a summary read carry their stops and ends: those the
/// response can render. Every row when there is no window; otherwise the
/// rows due in `window`, plus with `at` the running candidates (due at
/// or before `at`, last on-line arrival no more than
/// [`RUNNING_DELAY_GRACE_MINUTES`] before it).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShipRule {
    pub window: Option<(i32, i32)>,
    pub at: Option<i32>,
}

/// One decoded `line_train_summaries` summary read, column for column.
type SummaryRowTuple = (
    Option<String>,
    bool,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i32>,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn summary_row_from_tuple(
    (
        uid,
        _,
        operator_atoc,
        train_status,
        scope,
        direction,
        due_minute,
        origin_crs,
        destination_crs,
        stops,
    ): SummaryRowTuple,
) -> Result<SummaryRow> {
    Ok(SummaryRow {
        uid: uid.unwrap_or_default(),
        operator_atoc,
        train_status,
        scope,
        direction,
        due_minute,
        origin_crs,
        destination_crs,
        stops: stops
            .map(|text| serde_json::from_str(&text))
            .transpose()?
            .unwrap_or_default(),
    })
}

/// The `(line_id, service_date)` rows derived with `fingerprint`,
/// narrowed to `scopes` (unless the population predates membership),
/// each with its stops and ends only when `ship` says so. `None` when
/// there are no such rows (the caller falls back to the population).
/// Table order; the caller sorts.
pub async fn list_summary_rows(
    pool: &PgPool,
    line_id: &str,
    service_date: chrono::NaiveDate,
    fingerprint: &str,
    scopes: Option<&[String]>,
    ship: ShipRule,
) -> Result<Option<(Vec<SummaryRow>, bool)>> {
    let rows: Vec<SummaryRowTuple> = sqlx::query_as(
        r"
        WITH hs AS (
            SELECT has_scope FROM line_train_summaries
            WHERE line_id = $1 AND service_date = $2 AND derivation = $3
            LIMIT 1
        ),
        r AS (
            SELECT t.*, $5::int IS NULL
                     OR (t.due_minute >= $5::int AND t.due_minute < $6::int)
                     OR ($7::int IS NOT NULL AND t.due_minute <= $7::int
                         AND t.end_minute + $8::int >= $7::int) AS ship
            FROM hs
            JOIN line_train_summaries t
              ON t.line_id = $1 AND t.service_date = $2 AND t.derivation = $3
             AND ($4::text[] IS NULL OR NOT hs.has_scope OR t.scope = ANY($4::text[]))
        )
        SELECT r.uid, hs.has_scope, r.operator_atoc, r.train_status, r.scope, r.direction,
               r.due_minute,
               CASE WHEN r.ship THEN r.origin_crs END,
               CASE WHEN r.ship THEN r.destination_crs END,
               CASE WHEN r.ship THEN r.on_line_stops::text END
        FROM hs LEFT JOIN r ON true
        ",
    )
    .bind(line_id)
    .bind(service_date)
    .bind(fingerprint)
    .bind(scopes)
    .bind(ship.window.map(|(from, _)| from))
    .bind(ship.window.map(|(_, to)| to))
    .bind(ship.at)
    .bind(RUNNING_DELAY_GRACE_MINUTES)
    .fetch_all(pool)
    .await?;
    let Some(has_scope) = rows.first().map(|row| row.1) else {
        return Ok(None);
    };
    let rows = rows
        .into_iter()
        .filter(|row| row.0.is_some())
        .map(summary_row_from_tuple)
        .collect::<Result<Vec<_>>>()?;
    Ok(Some((rows, has_scope)))
}

/// `GET /public/lines/{id}/timetable`'s filters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TimetableFilter {
    /// `None`: every scope.
    pub scopes: Option<Vec<String>>,
    /// `None`: every direction.
    pub directions: Option<Vec<String>>,
    /// Catalogue CRS: only trains with a public call there, timed by it.
    pub from: Option<String>,
    /// Catalogue CRS: only trains with a public call there (after `from`'s).
    pub to: Option<String>,
    /// Only trains whose time is at or after this minute.
    pub at: Option<i32>,
    /// Keyset cursor: only trains after this `(time, uid)`.
    pub after: Option<(i32, String)>,
    pub limit: usize,
}

/// One timetable row: the train, its time (at `from`, else on the line)
/// and its arrival at `to`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimetableEntry {
    pub row: SummaryRow,
    pub minute: i32,
    pub to_arrival: Option<i32>,
}

/// One page of the timetable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimetablePage {
    pub has_scope: bool,
    pub entries: Vec<TimetableEntry>,
    /// The cursor for the next page, `None` on the last.
    pub next: Option<(i32, String)>,
    /// Trains per scope and direction (`None` for a train without one)
    /// for the whole day under the scope and station filters -- before
    /// `directions`, `at` and the cursor. Ordered by scope, direction.
    pub counts: Vec<(Option<String>, Option<String>, i64)>,
}

/// The time a train is listed at under `filter`'s stations, and its
/// arrival at `to`: `None` when it does not call at them in order.
fn station_times(row: &SummaryRow, filter: &TimetableFilter) -> Option<(i32, Option<i32>)> {
    let from_index = match &filter.from {
        None => None,
        Some(from) => Some(row.stops.iter().position(|s| &s.crs == from)?),
    };
    let minute = match from_index {
        None => row.due_minute?,
        Some(i) => row.stops.get(i)?.minute,
    };
    let to_arrival = match &filter.to {
        None => None,
        Some(to) => {
            let start = from_index.map_or(0, |i| i + 1);
            Some(
                row.stops
                    .get(start..)?
                    .iter()
                    .find(|s| &s.crs == to)?
                    .arrival_minute,
            )
        }
    };
    Some((minute, to_arrival))
}

/// The timetable page over rows already in memory: the population
/// fallback, and the reference [`timetable_page`]'s SQL is tested
/// against.
pub fn timetable_page_in_memory(
    rows: Vec<SummaryRow>,
    has_scope: bool,
    filter: &TimetableFilter,
) -> TimetablePage {
    let timed: Vec<TimetableEntry> = rows
        .into_iter()
        .filter(|r| {
            !has_scope
                || filter
                    .scopes
                    .as_ref()
                    .is_none_or(|s| r.scope.as_ref().is_some_and(|own| s.contains(own)))
        })
        .filter_map(|row| {
            let (minute, to_arrival) = station_times(&row, filter)?;
            Some(TimetableEntry {
                row,
                minute,
                to_arrival,
            })
        })
        .collect();
    let mut counts: BTreeMap<(Option<String>, Option<String>), i64> = BTreeMap::new();
    for e in &timed {
        *counts
            .entry((e.row.scope.clone(), e.row.direction.clone()))
            .or_default() += 1;
    }
    let mut listed: Vec<TimetableEntry> = timed
        .into_iter()
        .filter(|e| {
            filter
                .directions
                .as_ref()
                .is_none_or(|d| e.row.direction.as_ref().is_some_and(|own| d.contains(own)))
        })
        .filter(|e| filter.at.is_none_or(|at| e.minute >= at))
        .filter(|e| {
            filter
                .after
                .as_ref()
                .is_none_or(|(m, uid)| (e.minute, e.row.uid.as_str()) > (*m, uid.as_str()))
        })
        .collect();
    listed.sort_by(|a, b| (a.minute, &a.row.uid).cmp(&(b.minute, &b.row.uid)));
    let next = next_cursor(&listed, filter.limit);
    listed.truncate(filter.limit);
    TimetablePage {
        has_scope,
        entries: listed,
        next,
        counts: counts
            .into_iter()
            .map(|((scope, direction), n)| (scope, direction, n))
            .collect(),
    }
}

/// The cursor after a page of `limit` out of `listed` (which holds at
/// least one more when there is a next page).
fn next_cursor(listed: &[TimetableEntry], limit: usize) -> Option<(i32, String)> {
    if listed.len() <= limit {
        return None;
    }
    listed
        .get(limit.checked_sub(1)?)
        .map(|e| (e.minute, e.row.uid.clone()))
}

/// The timetable's rows with their station times, as SQL (`$1` line,
/// `$2` date, `$3` fingerprint, `$4` scopes, `$5` from, `$6` to):
/// [`station_times`] for every row of the date, filtered by scope and
/// stations. `uid` is in the "C" collation so the keyset order is Rust's
/// byte order.
const TIMETABLE_BASE_SQL: &str = r#"
    hs AS (
        SELECT has_scope FROM line_train_summaries
        WHERE line_id = $1 AND service_date = $2 AND derivation = $3
        LIMIT 1
    ),
    base AS (
        SELECT t.uid COLLATE "C" AS uid, t.operator_atoc, t.train_status, t.scope,
               t.direction, t.due_minute, t.origin_crs, t.destination_crs, t.on_line_stops,
               CASE WHEN $5::text IS NULL THEN t.due_minute ELSE f.minute END AS minute,
               a.arrival AS to_arrival
        FROM hs
        JOIN line_train_summaries t
          ON t.line_id = $1 AND t.service_date = $2 AND t.derivation = $3
         AND ($4::text[] IS NULL OR NOT hs.has_scope OR t.scope = ANY($4::text[]))
        LEFT JOIN LATERAL (
            SELECT (x.e ->> 'minute')::int AS minute, x.i
            FROM jsonb_array_elements(t.on_line_stops) WITH ORDINALITY AS x(e, i)
            WHERE x.e ->> 'crs' = $5::text
            ORDER BY x.i LIMIT 1
        ) f ON true
        LEFT JOIN LATERAL (
            SELECT (x.e ->> 'arrival')::int AS arrival
            FROM jsonb_array_elements(t.on_line_stops) WITH ORDINALITY AS x(e, i)
            WHERE x.e ->> 'crs' = $6::text AND x.i > COALESCE(f.i, 0)
            ORDER BY x.i LIMIT 1
        ) a ON true
        WHERE ($5::text IS NULL OR f.minute IS NOT NULL)
          AND ($6::text IS NULL OR a.arrival IS NOT NULL)
    )
"#;

/// One decoded timetable row, column for column.
type TimetableTuple = (
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i32>,
    Option<String>,
    Option<String>,
    String,
    i32,
    Option<i32>,
);

fn timetable_entry_from_tuple(
    (
        uid,
        operator_atoc,
        train_status,
        scope,
        direction,
        due_minute,
        origin_crs,
        destination_crs,
        stops,
        minute,
        to_arrival,
    ): TimetableTuple,
) -> Result<TimetableEntry> {
    Ok(TimetableEntry {
        row: SummaryRow {
            uid,
            operator_atoc,
            train_status,
            scope,
            direction,
            due_minute,
            origin_crs,
            destination_crs,
            stops: serde_json::from_str(&stops)?,
        },
        minute,
        to_arrival,
    })
}

/// One timetable page from the table: the same answer as
/// [`timetable_page_in_memory`] over the rows of `(line_id, service_date)`
/// derived with `fingerprint`, filtered, ordered and cut in SQL. `None`
/// when there are no such rows (the caller falls back to the population).
pub async fn timetable_page(
    pool: &PgPool,
    line_id: &str,
    service_date: chrono::NaiveDate,
    fingerprint: &str,
    filter: &TimetableFilter,
) -> Result<Option<TimetablePage>> {
    let has_scope: Option<(bool,)> = sqlx::query_as(
        "SELECT has_scope FROM line_train_summaries \
         WHERE line_id = $1 AND service_date = $2 AND derivation = $3 LIMIT 1",
    )
    .bind(line_id)
    .bind(service_date)
    .bind(fingerprint)
    .fetch_optional(pool)
    .await?;
    let Some((has_scope,)) = has_scope else {
        return Ok(None);
    };
    let limit = i64::try_from(filter.limit).unwrap_or(i64::MAX);
    let rows: Vec<TimetableTuple> = sqlx::query_as(&format!(
        r#"
        WITH {TIMETABLE_BASE_SQL}
        SELECT uid, operator_atoc, train_status, scope, direction, due_minute, origin_crs,
               destination_crs, on_line_stops::text, minute, to_arrival
        FROM base
        WHERE minute IS NOT NULL
          AND ($7::text[] IS NULL OR direction = ANY($7::text[]))
          AND ($8::int IS NULL OR minute >= $8::int)
          AND ($9::int IS NULL OR (minute, uid) > ($9::int, $10::text COLLATE "C"))
        ORDER BY minute, uid
        LIMIT $11
        "#
    ))
    .bind(line_id)
    .bind(service_date)
    .bind(fingerprint)
    .bind(filter.scopes.as_deref())
    .bind(filter.from.as_deref())
    .bind(filter.to.as_deref())
    .bind(filter.directions.as_deref())
    .bind(filter.at)
    .bind(filter.after.as_ref().map(|(m, _)| *m))
    .bind(filter.after.as_ref().map(|(_, uid)| uid.as_str()))
    .bind(limit.saturating_add(1))
    .fetch_all(pool)
    .await?;
    let counts: Vec<(Option<String>, Option<String>, i64)> = sqlx::query_as(&format!(
        r#"
        WITH {TIMETABLE_BASE_SQL}
        SELECT scope, direction, count(*) FROM base
        WHERE minute IS NOT NULL
        GROUP BY scope, direction
        ORDER BY scope COLLATE "C" NULLS FIRST, direction COLLATE "C" NULLS FIRST
        "#
    ))
    .bind(line_id)
    .bind(service_date)
    .bind(fingerprint)
    .bind(filter.scopes.as_deref())
    .bind(filter.from.as_deref())
    .bind(filter.to.as_deref())
    .fetch_all(pool)
    .await?;
    let mut entries = rows
        .into_iter()
        .map(timetable_entry_from_tuple)
        .collect::<Result<Vec<_>>>()?;
    let next = next_cursor(&entries, filter.limit);
    entries.truncate(filter.limit);
    Ok(Some(TimetablePage {
        has_scope,
        entries,
        next,
        counts,
    }))
}
