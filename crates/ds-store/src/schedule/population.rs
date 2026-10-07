//! The per-line schedule population (`schedule_line_population`):
//! `schedule-reference` publishes one line's day of trains as one JSONB
//! row, and the readers fetch it whole or conditionally.
//!
//! The api's publish route writes through
//! `data::line_train_summaries::upsert_population_with_summaries`, which
//! runs the same statement as [`upsert_schedule_line_population`] and also
//! derives the line's train summaries; that writer has not moved yet.
//!
//! Moved unchanged from the api's `data::queries` (ingest architecture
//! plan 1A.7; the schedule-match sweep's three readers,
//! `get_schedule_line_population_entries`,
//! `get_overnight_schedule_line_population_entries` and
//! `list_line_ids_with_uid_in_population`, in 1A.10); the api re-exports
//! them from there.

use anyhow::Result;
use sqlx::PgPool;

/// Upserts one line's population for one service date -- wholesale
/// replaces any existing row for that `(line_id, service_date)` (a fresh
/// CIF read supersedes the prior one entirely, never merged). `population`
/// is stored opaquely; `api` never deserializes it into
/// `schedule_query::LinePopulationEntry` -- only `schedule-reference`
/// (writer) and `full-coverage-consumer` (reader) need that shape.
///
/// `population_json` is the population as JSON TEXT, bound as `text` and
/// cast with `$3::jsonb` so Postgres does the parse. It used to be a
/// `&serde_json::Value`, which meant the POST handler built a full `Value`
/// tree of a body that can reach 31 MB of JSON text (one line's population,
/// measured in production 2026-09-26) -- several times that size in heap --
/// and then re-encoded it for the bind. That, under `schedule-reference`'s
/// restart-time republish storms, is what OOM-killed `api` against its
/// 1536Mi limit. The caller must pass syntactically valid JSON (the route
/// gets that for free from `serde_json::value::RawValue`); invalid text is
/// rejected by Postgres's own jsonb input function as an error.
///
/// **An identical re-publish is a no-op.** `schedule-reference` republishes
/// every line for every date in its window each cycle, and almost all of
/// those populations are unchanged. Each blob is ~0.5 MB of compressed
/// JSONB, so an unconditional `DO UPDATE` wrote a whole new TOAST copy per
/// row per publish (in production: ~920 MB live data behind a 1.5 GB TOAST
/// file, rewritten daily). The `WHERE ... IS DISTINCT FROM` skips the row
/// entirely when the content is equal (jsonb equality, so key order and
/// whitespace don't matter). The consequence is that `updated_at` means
/// "when this population last CHANGED", not "when it was last published" --
/// which is exactly what makes it usable as the `ETag` of
/// `GET /private/schedule-line-population` (see
/// [`get_schedule_line_population_conditional`]). Publish freshness is
/// tracked by `schedule_reference_publishes`, not here.
pub async fn upsert_schedule_line_population(
    pool: &PgPool,
    line_id: &str,
    service_date: chrono::NaiveDate,
    population_json: &str,
) -> Result<()> {
    sqlx::query(
        r"
        INSERT INTO schedule_line_population (line_id, service_date, population, updated_at)
        VALUES ($1, $2, $3::jsonb, now())
        ON CONFLICT (line_id, service_date) DO UPDATE SET
            population = EXCLUDED.population,
            updated_at = EXCLUDED.updated_at
        WHERE schedule_line_population.population IS DISTINCT FROM EXCLUDED.population
        ",
    )
    .bind(line_id)
    .bind(service_date)
    .bind(population_json)
    .execute(pool)
    .await?;
    Ok(())
}

/// Reads one line's population for one service date, if published, as
/// Postgres's own JSON text rendering of the stored jsonb
/// (`population::text`).
///
/// Text, not `serde_json::Value`: every caller either relays it verbatim
/// (`GET /public/lines/{id}/schedule`) or deserializes it straight into its
/// own type. Decoding into a `Value` first cost several times the text size
/// in heap for a blob that reaches 31 MB of text -- see
/// [`upsert_schedule_line_population`]'s doc comment.
///
/// `None` when `full-coverage-consumer` reloads before `schedule-reference`
/// has ever published that day's population yet (a real, expected startup
/// race, not an error -- the caller treats it the same as "empty
/// population," per Decision 2e's own Pending semantics).
pub async fn get_schedule_line_population(
    pool: &PgPool,
    line_id: &str,
    service_date: chrono::NaiveDate,
) -> Result<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT population::text FROM schedule_line_population \
         WHERE line_id = $1 AND service_date = $2",
    )
    .bind(line_id)
    .bind(service_date)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(population,)| population))
}

/// Result of [`get_schedule_line_population_conditional`] for a row that
/// exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConditionalPopulation {
    /// The caller's validator matched the row's `updated_at`: the body was
    /// not read (not even detoasted -- see the query's `CASE`).
    NotModified {
        updated_at: chrono::DateTime<chrono::Utc>,
    },
    /// The row changed since the caller's validator (or it sent none):
    /// the population as JSON text, same as [`get_schedule_line_population`].
    Modified {
        updated_at: chrono::DateTime<chrono::Utc>,
        population: String,
    },
}

/// [`get_schedule_line_population`] plus a conditional-GET short-circuit
/// for `GET /private/schedule-line-population`'s `If-None-Match`.
///
/// `updated_at` is the row's version: [`upsert_schedule_line_population`]
/// only touches it when the stored jsonb actually changes, so an unchanged
/// `updated_at` means an unchanged population. When it equals any of
/// `known_versions` (or `match_any` is set, `If-None-Match: *`), the `CASE`
/// never evaluates `population::text`, so Postgres does not even
/// decompress the `TOASTed` blob, and `api` allocates nothing for it.
///
/// `None` exactly when [`get_schedule_line_population`] would return
/// `None`: no row at all.
pub async fn get_schedule_line_population_conditional(
    pool: &PgPool,
    line_id: &str,
    service_date: chrono::NaiveDate,
    match_any: bool,
    known_versions: &[chrono::DateTime<chrono::Utc>],
) -> Result<Option<ConditionalPopulation>> {
    let row: Option<(chrono::DateTime<chrono::Utc>, Option<String>)> = sqlx::query_as(
        "SELECT updated_at, \
                CASE WHEN $3 OR updated_at = ANY($4) THEN NULL ELSE population::text END \
         FROM schedule_line_population \
         WHERE line_id = $1 AND service_date = $2",
    )
    .bind(line_id)
    .bind(service_date)
    .bind(match_any)
    .bind(known_versions)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(updated_at, population)| match population {
        // `population` is `NOT NULL`, so a NULL here can only be the `CASE`'s
        // not-modified branch.
        None => ConditionalPopulation::NotModified { updated_at },
        Some(population) => ConditionalPopulation::Modified {
            updated_at,
            population,
        },
    }))
}

/// One line's population for one service date, deserialized into
/// `schedule_query::LinePopulationEntry` for
/// `schedule_matching::find_schedule_match` -- optionally narrowed IN SQL
/// to the entries whose `uid` is `only_uid`.
///
/// `find_schedule_match` runs this once per candidate line (a busy
/// terminus sits on a dozen catalogued lines), and with a known uid it
/// only ever keeps that one uid's entries. Filtering in Postgres
/// (`jsonb_array_elements` + `jsonb_agg ... ORDER BY ord`, preserving the
/// published array order that `match_pin`'s tie-break depends on) means
/// `api` receives a few kB instead of a whole line's day -- up to 31 MB of
/// text -- per candidate line. The untargeted path (`only_uid = None`)
/// still needs every entry, but now deserializes the text straight into
/// the typed entries instead of going through a `serde_json::Value` first.
///
/// `None` when no row exists; `Some(vec![])` when a row exists but carries
/// no entry for `only_uid`.
pub async fn get_schedule_line_population_entries(
    pool: &PgPool,
    line_id: &str,
    service_date: chrono::NaiveDate,
    only_uid: Option<&str>,
) -> Result<Option<Vec<schedule_query::LinePopulationEntry>>> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT CASE WHEN $3::text IS NULL THEN population::text \
                ELSE COALESCE( \
                    (SELECT jsonb_agg(x.e ORDER BY x.ord) \
                     FROM jsonb_array_elements(population) WITH ORDINALITY AS x(e, ord) \
                     WHERE x.e ->> 'uid' = $3::text), \
                    '[]'::jsonb)::text \
                END \
         FROM schedule_line_population \
         WHERE line_id = $1 AND service_date = $2",
    )
    .bind(line_id)
    .bind(service_date)
    .bind(only_uid)
    .fetch_optional(pool)
    .await?;
    row.map(|(text,)| serde_json::from_str(&text).map_err(Into::into))
        .transpose()
}

/// The entries of one line's population for `service_date` that run past
/// midnight: those with at least one calling point on a later calendar day
/// (`day_offset >= 1`), in published order. `None` when no row exists.
///
/// For `schedule_matching::find_schedule_match`'s untargeted path, which
/// must also consider the PREVIOUS day's schedules for a pin after midnight
/// (a pin is dated by its own departure, a schedule by its origin). Only
/// overnight entries can have a calling point on the pin's day, so the
/// filter runs in SQL and the rest of the day's population never leaves
/// Postgres.
pub async fn get_overnight_schedule_line_population_entries(
    pool: &PgPool,
    line_id: &str,
    service_date: chrono::NaiveDate,
) -> Result<Option<Vec<schedule_query::LinePopulationEntry>>> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT COALESCE( \
                    (SELECT jsonb_agg(x.e ORDER BY x.ord) \
                     FROM jsonb_array_elements(population) WITH ORDINALITY AS x(e, ord) \
                     WHERE jsonb_path_exists(x.e, '$.calling_points[*] ? (@.day_offset >= 1)')), \
                    '[]'::jsonb)::text \
         FROM schedule_line_population \
         WHERE line_id = $1 AND service_date = $2",
    )
    .bind(line_id)
    .bind(service_date)
    .fetch_optional(pool)
    .await?;
    row.map(|(text,)| serde_json::from_str(&text).map_err(Into::into))
        .transpose()
}

/// Every published line whose `service_date` population contains a schedule
/// with this `train_uid` -- the identity-first inverse of
/// [`get_schedule_line_population`], which can only answer "what is on THIS
/// line."
///
/// Exists for `schedule_matching::find_schedule_match`'s
/// known-identity fallback: a train whose ORIGIN station appears on no
/// `lines/*.toml` at all (a real shape -- an uncatalogued branch terminus)
/// has no candidate line to look up by CRS, even though
/// `schedule_query::schedules_touching` has already put its whole schedule,
/// origin calling point included, into the population of every line that
/// lists ANY station it calls at further down the route. Without this, such
/// a train's schedule was unreachable from a known `train_uid`.
///
/// One JSONB containment query, not 100-plus per-line reads: `population @>
/// '[{"uid": ...}]'` is true exactly when some array element of
/// `population` is an object carrying that `uid` (jsonb containment matches
/// objects partially), so Postgres does the "which line carries this uid"
/// scan itself. `ORDER BY line_id` so a caller iterating the result is
/// deterministic across calls, matching `crs_to_line_ids`' own
/// alphabetical-by-file candidate ordering.
///
/// Deliberately NOT indexed: this runs only on the fallback path above (a
/// known uid whose origin CRS is uncatalogued), the table holds one row per
/// line per date (a few hundred rows), and a GIN index on a
/// whole-day-of-schedules JSONB column would cost every
/// `schedule-reference` publish far more than it saves here.
pub async fn list_line_ids_with_uid_in_population(
    pool: &PgPool,
    service_date: chrono::NaiveDate,
    train_uid: &str,
) -> Result<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT line_id FROM schedule_line_population \
         WHERE service_date = $1 \
           AND population @> jsonb_build_array(jsonb_build_object('uid', $2::text)) \
         ORDER BY line_id",
    )
    .bind(service_date)
    .bind(train_uid)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(line_id,)| line_id).collect())
}
