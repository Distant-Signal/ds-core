//! The schedule publish products and their diff-publish protocol:
//! `schedule_network_departures` (one JSONB board per station and date),
//! and `schedule_destination_departures` and
//! `schedule_calling_points_full`, which `schedule-reference` publishes in
//! chunks through [`SchedulePublishPart`] and the `*_publish_keys` staging
//! tables (stage, analyze, diff-delete on the final chunk).
//!
//! Moved unchanged from the api's `data::queries` (ingest architecture
//! plan 1A.7); the api re-exports them from there. The DB tests include
//! the 2026-09-27 regression tests, PL-14 (an empty final publish) and the
//! `analyze_publish_keys` `reltuples` test.

use anyhow::Result;
use serde::Deserialize;
use sqlx::PgPool;

use crate::freshness::{last_per_key, normalize_code};

/// One `POST /private/schedule-network-departures` batch element --
/// query-scoped, deserialized straight off the request body by
/// `routes::ingest::post_schedule_network_departures`. Defined here
/// (the data layer), not in `routes/ingest.rs`, so the data layer never
/// depends on a route-layer type -- same direction as every other
/// dependency between these two files. `departures` stays an opaque
/// `serde_json::Value` -- see this table's own migration comment for why.
#[derive(Debug, Clone, Deserialize)]
pub struct ScheduleNetworkDeparturesRow {
    pub crs: String,
    pub service_date: chrono::NaiveDate,
    pub departures: serde_json::Value,
}

/// Upserts one cycle's batch of per-station CIF-derived departures --
/// wholesale replaces any existing row for each `(crs, service_date)` (a
/// fresh cycle's grouping pass supersedes the prior one entirely, never
/// merged): one `INSERT ... SELECT FROM UNNEST ... ON CONFLICT` for the
/// batch, skipping boards identical to the stored one.
pub async fn upsert_schedule_network_departures(
    pool: &PgPool,
    rows: &[ScheduleNetworkDeparturesRow],
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let batch = last_per_key(rows, |row| (normalize_code(&row.crs), row.service_date));
    let crs: Vec<String> = batch.iter().map(|r| normalize_code(&r.crs)).collect();
    let service_date: Vec<chrono::NaiveDate> = batch.iter().map(|r| r.service_date).collect();
    let departures: Vec<&serde_json::Value> = batch.iter().map(|r| &r.departures).collect();

    // An unchanged `(crs, service_date)` board is left alone -- each cycle
    // republishes every station's JSONB board, most of them identical to the
    // last cycle's. `updated_at` therefore means "last changed" (nothing
    // reads it).
    sqlx::query(
        r"
        INSERT INTO schedule_network_departures (crs, service_date, departures, updated_at)
        SELECT crs, service_date, departures, now()
        FROM UNNEST($1::text[], $2::date[], $3::jsonb[]) AS i(crs, service_date, departures)
        ON CONFLICT (crs, service_date) DO UPDATE SET
            departures = EXCLUDED.departures,
            updated_at = EXCLUDED.updated_at
        WHERE schedule_network_departures.departures IS DISTINCT FROM EXCLUDED.departures
        ",
    )
    .bind(&crs)
    .bind(&service_date)
    .bind(&departures)
    .execute(pool)
    .await?;
    Ok(rows.len() as u64)
}

/// One `POST /private/schedule-destination-departures` batch element -- one
/// DEPARTURE, not one destination bucket. Query-scoped, deserialized
/// straight off the request body by
/// `routes::ingest::post_schedule_destination_departures`. Defined here
/// (the data layer), not in `routes/ingest.rs`, so the data layer never
/// depends on a route-layer type -- same direction as every other
/// dependency between these two files.
///
/// Deliberately NOT shaped like `ScheduleNetworkDeparturesRow` above, which
/// carries an opaque `serde_json::Value` bucket. Every field here is a flat
/// scalar mapping one-to-one onto a column of
/// `schedule_destination_departures`, because the destination product needs
/// to be FILTERED and PAGINATED in SQL rather than stored and relayed
/// whole. See
/// docs/superpowers/specs/2026-09-07-train-listing-destination-search-sizing-design.md
/// §3 for why the bucket shape could not work here.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ScheduleDestinationDeparturesRow {
    pub service_date: chrono::NaiveDate,
    pub destination_crs: String,
    pub scheduled: chrono::NaiveTime,
    /// How many calendar days past `service_date` `scheduled` actually
    /// falls on -- mirrors `schedule_query::DestinationDeparture::day_offset`
    /// verbatim (this row is built directly from one, see
    /// `schedule-reference::schedule_destination_departures_rows`). See
    /// `schedule_query::CallingPoint::day_offset`'s own doc comment for the
    /// live-confirmed real overnight-service example this exists for.
    /// `#[serde(default)]` for the same rolling-deploy-safety reason
    /// `true_origin_crs`/`destination_arrival` tolerate a missing key.
    #[serde(default)]
    pub day_offset: i16,
    pub train_uid: String,
    pub origin_crs: String,
    pub true_origin_crs: Option<String>,
    /// THIS row's own calling point's `booked_arrival` -- NOT the
    /// schedule's true destination's arrival (`destination_arrival`
    /// below). Mirrors `schedule_query::DestinationDeparture::calling_point_arrival`'s
    /// own doc comment exactly: `None` for the schedule's true origin (an
    /// `Origin` calling point never has a `booked_arrival`), `Some` for a
    /// genuine `Intermediate` calling point. Backs `GET
    /// /public/trains/search?arrival_from=&arrival_to=`, which only
    /// applies when `stops_at` is set -- see
    /// docs/superpowers/specs/2026-09-09-stops-at-search-filter-design.md.
    pub calling_point_arrival: Option<chrono::NaiveTime>,
    /// The schedule's terminating calling point's own `booked_arrival`,
    /// mirroring `true_origin_crs`'s plumbing exactly -- see
    /// `schedule_query::DestinationDeparture::destination_arrival`'s own
    /// doc comment and
    /// docs/superpowers/specs/2026-09-08-destination-arrival-time-filter-design.md.
    /// Missing from the wire JSON deserializes as `None` (Option<T> fields
    /// are optional-by-default for self-describing formats like JSON),
    /// same as `true_origin_crs`.
    pub destination_arrival: Option<chrono::NaiveTime>,
    /// How many calendar days past `service_date` `destination_arrival`
    /// actually falls on -- mirrors
    /// `schedule_query::DestinationDeparture::destination_arrival_day_offset`
    /// verbatim (this row is built directly from one, see
    /// `schedule-reference::schedule_destination_departures_rows`). NOT the
    /// same value as this row's own `day_offset` above in general: on a
    /// genuine overnight schedule, the DEPARTING calling point this row
    /// represents and the schedule's TERMINATING calling point can fall on
    /// two different calendar days (see that struct's own doc comment for
    /// the live-confirmed c2c UID `F49687` example). `#[serde(default)]`
    /// for the same rolling-deploy-safety reason `day_offset` above tolerates
    /// a missing key -- a row published before this field existed still
    /// deserializes, as `0` ("assume same day as the departure").
    #[serde(default)]
    pub destination_arrival_day_offset: i16,
    /// The schedule's `BX` ATOC Code, mirroring
    /// `schedule_query::records::DestinationDeparture::operator_atoc`'s own
    /// doc comment exactly: computed once per schedule and copied
    /// unchanged onto every departure row it contributes, `None` when no
    /// `BX` line follows the schedule's `BS` line (or its ATOC Code field
    /// was blank/undecodable). `#[serde(default)]` for the same
    /// rolling-deploy-safety reason `day_offset`/`destination_arrival_day_offset`
    /// above tolerate a missing key -- a row published before this field
    /// existed still deserializes, as `None`.
    #[serde(default)]
    pub operator_atoc: Option<String>,
    /// The schedule's CIF `BS` Train Identity (4-character signalling
    /// headcode, e.g. `"1S00"`), mirroring
    /// `schedule_query::records::DestinationDeparture::headcode`: once per
    /// schedule, copied onto every row. NOT the TRUST 10-char
    /// `trains.train_id`. `#[serde(default)]` so a publisher that predates
    /// the field still deserializes (as `None`).
    #[serde(default)]
    pub headcode: Option<String>,
    /// The schedule's CIF `BX` Retail Service ID (8 characters, e.g.
    /// `"SR408800"`), mirroring
    /// `schedule_query::records::DestinationDeparture::rsid`: once per
    /// schedule (the STP-resolved winner's value), copied onto every row.
    /// Backs `GET /public/trains/resolve`. `#[serde(default)]` so a
    /// publisher that predates the field still deserializes (as `None`).
    #[serde(default)]
    pub rsid: Option<String>,
    /// The public (GBTT) counterparts of `scheduled`,
    /// `calling_point_arrival` and `destination_arrival` -- see
    /// `schedule_query::DestinationDeparture`. `#[serde(default)]` for the
    /// same rolling-deploy reason as `rsid`: NULL until the next publish.
    #[serde(default)]
    pub public_departure: Option<chrono::NaiveTime>,
    #[serde(default)]
    pub public_calling_point_arrival: Option<chrono::NaiveTime>,
    #[serde(default)]
    pub public_destination_arrival: Option<chrono::NaiveTime>,
    /// Passenger direction at this row's call (migration
    /// `20261001170000`): `can_board` is `false` on a set-down-only row,
    /// `can_alight` on a pick-up-only one. `None` from a publisher that
    /// predates them (stored NULL, read as `true`).
    #[serde(default)]
    pub can_board: Option<bool>,
    #[serde(default)]
    pub can_alight: Option<bool>,
}

/// Replaces one CIF delivery's worth of per-destination departures -- `rows`
/// is the COMPLETE new set for every service date it touches.
///
/// **A diff, not a delete-then-insert (2026-09-26).** This used to clear the
/// touched dates and re-insert every row, which in production left the
/// table's B-tree indexes 52-66% bloated (a delivery covers each date of the
/// 8-day window several times over, and most re-published rows were
/// byte-identical to what was already there). It is now, in ONE transaction,
/// exactly [`upsert_schedule_destination_departures_publish_part`] run as a
/// single first-and-final chunk:
///
/// 1. one multi-row `INSERT ... SELECT FROM UNNEST(...) ON CONFLICT (pk) DO
///    UPDATE ... WHERE (<non-key columns>) IS DISTINCT FROM (EXCLUDED...)`
///    -- an unchanged row produces no new tuple and no index entry, a changed
///    row is updated in place (no indexed column is a non-key column, so
///    that is usually a HOT update), a new row is inserted; then
/// 2. `DELETE` every row of the touched dates whose key is not in `rows`.
///
/// **Unchanged rows are skipped before the upsert, not just by its guard
/// (WAL cut, 2026-10-08).** `ON CONFLICT DO UPDATE ... WHERE false` still
/// LOCKS the conflicting tuple: it sets `xmax`, writes a heap-lock WAL
/// record and dirties the page (a full-page image after each checkpoint).
/// Measured locally (PG 18, 4 dates of 266k departures + 490k calling
/// points, 50k-row chunks), republishing identical data wrote ~572 MB of
/// WAL -- ~190 bytes per row, every daily cycle, for every date of the
/// window. The upsert's `SELECT` now LEFT JOINs each incoming row to its
/// stored copy (a `LATERAL ... OFFSET 0` per-row primary-key probe, an
/// MVCC read that takes no row lock) and keeps only rows that are new or
/// differ, so the same republish writes ~50 kB. No schema change. Rows
/// written vs skipped are counted in [`SCHEDULE_PUBLISH_ROWS_METRIC`].
///
/// The observable result is identical to the old wholesale replace: after
/// the call, each touched date holds exactly `rows`. `UNNEST` follows this
/// crate's own established batch pattern -- see
/// `crate::data::trains::find_or_create_trains_batch` -- so bind-parameter
/// count is independent of row count, and there are no per-row round trips.
///
/// **An empty `rows` is a no-op, and that is load-bearing.** A publish that
/// produced nothing (an upstream parse failure, a delivery with no
/// schedules) must not be allowed to delete a service date's real
/// timetable -- step 2 above would otherwise delete the whole date. Guarded
/// and tested (`upsert_with_an_empty_batch_does_not_wipe_the_day`).
///
/// Rows sharing a primary key within one batch (the publisher can emit a
/// byte-identical duplicate for a pathological schedule) are collapsed to
/// the FIRST such row -- the same row the previous `ON CONFLICT DO NOTHING`
/// insert kept -- rather than failing a ~377,000-row batch (`ON CONFLICT DO
/// UPDATE` cannot touch one row twice in one statement). The return value
/// is rows actually inserted or changed; an unchanged row is not counted.
///
/// This is the "one call is the whole day" entry point every in-process
/// caller (this file's own tests, `crates/api/src/routes/journeys.rs`'s
/// fixtures) uses. The ingest route
/// (`crates/api/src/routes/ingest.rs::post_schedule_destination_departures`)
/// receives a day in several chunks and so calls
/// [`upsert_schedule_destination_departures_publish_part`] directly. (The
/// legacy delete-then-insert chunk path for a publisher without
/// `publish_id` was removed with F-LEGACY on 2026-09-27; such a request is
/// now a 400.)
pub async fn upsert_schedule_destination_departures(
    pool: &PgPool,
    rows: &[ScheduleDestinationDeparturesRow],
) -> Result<u64> {
    let publish_id = in_process_publish_id();
    upsert_schedule_destination_departures_publish_part(
        pool,
        rows,
        SchedulePublishPart {
            publish_id: &publish_id,
            first_chunk: true,
            final_total_rows: Some(rows.len() as u64),
        },
    )
    .await
}

/// One HTTP chunk of a diff-based, possibly multi-chunk schedule publish --
/// the unit [`upsert_schedule_destination_departures_publish_part`] and
/// [`upsert_schedule_calling_points_full_publish_part`] work in.
///
/// # How a multi-chunk diff publish stays correct
///
/// Each chunk upserts its own rows immediately (unchanged rows untouched,
/// changed rows updated, new rows inserted) and records their primary keys
/// in the table's `*_publish_keys` staging table under `publish_id`
/// (migration `20260926183000_schedule_publish_keys.sql`). Only the FINAL
/// chunk (`final_total_rows: Some(n)`) deletes anything: every row of the
/// publish's staged service dates whose key was not staged by any chunk of
/// this publish. That is the one point at which "absent from the new
/// publish" is actually known -- a per-chunk delete of "rows not in THIS
/// chunk" would delete every earlier chunk's rows.
///
/// The final chunk first checks that exactly `n` keys are staged for
/// `publish_id` (one per row the publisher sent, across all chunks). Any
/// mismatch -- a chunk handled by an older `api` that doesn't stage keys, a
/// replayed chunk, staging truncated by a crash (the tables are UNLOGGED), a
/// newer publish for the same date having superseded this one -- skips the
/// delete and only logs a warning. Failing closed that way can only leave
/// stale rows in place until the next publish; it can never delete a live
/// row the publish meant to keep.
///
/// `first_chunk` discards staged keys left by any OTHER publish of the same
/// dates (an abandoned, failed-part-way publish, or a concurrent publisher
/// that has now been superseded) and anything staged more than an hour ago,
/// so the staging tables hold at most about one in-flight publish per date.
/// (An hour, not the original day, since 2026-09-27: a publish takes minutes,
/// and a cycle whose final chunks all fail -- as every date's did in that
/// day's incident -- otherwise leaves every date's keys, ~1.8M rows, staged
/// for a day. A publish still in flight after an hour losing its keys only
/// fails its count check: it deletes nothing.)
///
/// # Visibility
///
/// Each chunk is its own transaction. While a publish is in flight a reader
/// sees the previous publish's rows plus whatever the new publish has
/// upserted so far -- a date is never emptied or half-populated, which the
/// old per-date DELETE-then-reinsert could do between chunks. The one
/// transient oddity is that a row whose KEY changed (e.g. a retimed
/// departure) is briefly present under both its old and new key until the
/// final chunk's delete commits.
#[derive(Debug, Clone, Copy)]
pub struct SchedulePublishPart<'a> {
    /// Chosen by the publisher, constant across one publish's chunks, unique
    /// per publish.
    pub publish_id: &'a str,
    /// This is the publish's first chunk.
    pub first_chunk: bool,
    /// `Some(total rows across every chunk of the publish)` on the final
    /// chunk only.
    pub final_total_rows: Option<u64>,
}

/// Longest `publish_id` accepted -- `schedule-reference`'s own ids are well
/// under this; the bound only stops a malformed caller staging arbitrarily
/// large keys.
pub const MAX_PUBLISH_ID_LEN: usize = 128;

impl<'a> SchedulePublishPart<'a> {
    /// A chunk's place in its publish, from the chunk's protocol parameters
    /// (the api's `ScheduleChunkParams` query string), or why the diff
    /// protocol cannot apply the chunk (the api answers 400 with it).
    ///
    /// `publish_id` is required (F-LEGACY, 2026-09-27: a request without
    /// one used to select the removed delete-then-insert chunk path) and
    /// 1-[`MAX_PUBLISH_ID_LEN`] characters long. `last_chunk` makes this the
    /// final chunk and then requires `total_rows`; `total_rows` is ignored
    /// otherwise.
    pub fn new(
        publish_id: Option<&'a str>,
        first_chunk: bool,
        last_chunk: bool,
        total_rows: Option<u64>,
    ) -> Result<Self, String> {
        let Some(publish_id) = publish_id else {
            return Err(
                "publish_id is required (the legacy delete-then-insert chunk protocol was \
                 removed)"
                    .to_string(),
            );
        };
        if publish_id.is_empty() || publish_id.len() > MAX_PUBLISH_ID_LEN {
            return Err(format!(
                "publish_id must be 1-{MAX_PUBLISH_ID_LEN} characters"
            ));
        }
        let final_total_rows = match (last_chunk, total_rows) {
            (true, Some(total)) => Some(total),
            (true, None) => {
                return Err("last_chunk=true requires total_rows".to_string());
            }
            (false, _) => None,
        };
        Ok(Self {
            publish_id,
            first_chunk,
            final_total_rows,
        })
    }

    /// For a chunk with no rows: the date an empty final publish clears
    /// (PL-14), or why the chunk is refused (the api answers 400): a
    /// `total_rows=0` final chunk must say which date it covers, since there
    /// are no staged keys to learn it from.
    pub fn empty_publish_date(
        last_chunk: bool,
        total_rows: Option<u64>,
        service_date: Option<chrono::NaiveDate>,
    ) -> Result<Option<chrono::NaiveDate>, String> {
        if last_chunk && total_rows == Some(0) && service_date.is_none() {
            return Err("a final chunk with total_rows=0 requires service_date".to_string());
        }
        Ok(service_date)
    }
}

/// A publish id for an in-process "one call is the whole set" publish --
/// never shared with another call, so it can never collide with a real
/// publisher's id or with a concurrent in-process call.
fn in_process_publish_id() -> String {
    format!("in-process-{:016x}", rand::random::<u64>())
}

/// Per-table SQL for the staging half of a diff publish -- see
/// [`SchedulePublishPart`]. Each statement takes `$1` = publish id.
struct PublishKeysSql {
    /// Human-readable product name, for logs.
    product: &'static str,
    /// `$2` = the chunk's distinct service dates.
    discard_superseded: &'static str,
    /// Returns `(staged row count, distinct staged service dates)`.
    summarize: &'static str,
    /// Refreshes the staging table's planner statistics. Run (inside the
    /// final chunk's transaction) right before `delete_missing`; takes no
    /// parameters. See [`finish_publish_part`] for why.
    ///
    /// Calls the `analyze_publish_keys` SECURITY DEFINER function
    /// (migration 20261001140000) rather than a bare `ANALYZE`: on
    /// Postgres 16 only the owner may ANALYZE a table, and for the
    /// non-superuser app role a bare `ANALYZE` silently skips the table
    /// with a WARNING (docs/postgres-app-role.md).
    analyze: &'static str,
    /// `$2` = the publish's staged service dates.
    delete_missing: &'static str,
    drop_publish: &'static str,
    /// This product's `pg_try_advisory_xact_lock` key, taken by every final
    /// chunk so at most one `delete_missing` per product runs at a time. See
    /// [`finish_publish_part`]. Must be unique across the codebase (no other
    /// advisory locks exist as of 2026-09-27).
    final_lock_key: i64,
}

/// `statement_timeout` for every statement of a schedule ingest chunk's
/// transaction (both products), raised above the
/// pool's 60s default (`common::pg`) with `SET LOCAL`. The bulk
/// `INSERT ... SELECT FROM UNNEST` of up to ~250k rows takes 3-8s in
/// production; 120s
/// keeps a healthy chunk far inside the budget while still bounding a
/// runaway. A final chunk's delete phase then sets its own
/// [`PUBLISH_DELETE_STATEMENT_TIMEOUT`].
const SCHEDULE_CHUNK_STATEMENT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// `statement_timeout` for every statement of a final chunk's delete phase
/// (the advisory lock, `summarize`, `analyze`, `delete_missing`,
/// `drop_publish`), set with `SET LOCAL` so it ends with the transaction.
///
/// With the `*_publish_keys_probe` indexes and the ANALYZE, `delete_missing`
/// takes seconds; on 2026-09-27 without them it ran 10-15+ minutes, kept
/// going after the publisher gave up, and was piled up behind retries. 120s
/// is far above healthy and far below that: a runaway is cancelled
/// (SQLSTATE 57014), which aborts and rolls back this one chunk only --
/// nothing is deleted, the date keeps its previous rows plus the upserts,
/// exactly like any other failed final chunk -- and `api` answers 503.
///
/// `schedule-reference`'s final-chunk HTTP timeout
/// (`FINAL_CHUNK_REQUEST_TIMEOUT`) is deliberately longer than this, so the
/// publisher normally hears the 503 instead of timing out while the server
/// is still working.
const PUBLISH_DELETE_STATEMENT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// A final publish chunk was refused because another final chunk of the same
/// product is still running its delete phase (it holds that product's
/// advisory lock). Nothing was written: the chunk's transaction rolls back.
/// `api` maps this to 409 Conflict; see [`finish_publish_part`].
#[derive(Debug)]
pub struct SchedulePublishBusy {
    pub product: &'static str,
}

impl std::fmt::Display for SchedulePublishBusy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "another final chunk of a {} publish is still deleting; refusing to run a second \
             delete concurrently",
            self.product
        )
    }
}

impl std::error::Error for SchedulePublishBusy {}

/// Whether `err` is Postgres cancelling a statement (SQLSTATE 57014,
/// `query_canceled`) -- in the publish path, [`PUBLISH_DELETE_STATEMENT_TIMEOUT`]
/// expiring.
pub fn is_statement_timeout(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<sqlx::Error>()
            .and_then(sqlx::Error::as_database_error)
            .and_then(sqlx::error::DatabaseError::code)
            .is_some_and(|code| code == "57014")
    })
}

const DESTINATION_DEPARTURES_PUBLISH_KEYS_SQL: PublishKeysSql = PublishKeysSql {
    product: "schedule_destination_departures",
    discard_superseded: "DELETE FROM schedule_destination_departures_publish_keys \
         WHERE (service_date = ANY($2::date[]) AND publish_id <> $1) \
            OR staged_at < now() - interval '1 hour'",
    summarize: "SELECT COUNT(*), COALESCE(array_agg(DISTINCT service_date), '{}') \
         FROM schedule_destination_departures_publish_keys WHERE publish_id = $1",
    analyze: "SELECT analyze_publish_keys('schedule_destination_departures_publish_keys')",
    delete_missing: "DELETE FROM schedule_destination_departures d \
         WHERE d.service_date = ANY($2::date[]) \
           AND NOT EXISTS ( \
               SELECT 1 FROM schedule_destination_departures_publish_keys k \
               WHERE k.publish_id = $1 \
                 AND k.service_date = d.service_date \
                 AND k.destination_crs = d.destination_crs \
                 AND k.scheduled = d.scheduled \
                 AND k.train_uid = d.train_uid \
                 AND k.origin_crs = d.origin_crs)",
    drop_publish: "DELETE FROM schedule_destination_departures_publish_keys WHERE publish_id = $1",
    // ASCII "sddpubfn" -- arbitrary, just distinct.
    final_lock_key: 0x7364_6470_7562_666e,
};

const CALLING_POINTS_FULL_PUBLISH_KEYS_SQL: PublishKeysSql = PublishKeysSql {
    product: "schedule_calling_points_full",
    discard_superseded: "DELETE FROM schedule_calling_points_full_publish_keys \
         WHERE (service_date = ANY($2::date[]) AND publish_id <> $1) \
            OR staged_at < now() - interval '1 hour'",
    summarize: "SELECT COUNT(*), COALESCE(array_agg(DISTINCT service_date), '{}') \
         FROM schedule_calling_points_full_publish_keys WHERE publish_id = $1",
    analyze: "SELECT analyze_publish_keys('schedule_calling_points_full_publish_keys')",
    delete_missing: "DELETE FROM schedule_calling_points_full c \
         WHERE c.service_date = ANY($2::date[]) \
           AND NOT EXISTS ( \
               SELECT 1 FROM schedule_calling_points_full_publish_keys k \
               WHERE k.publish_id = $1 \
                 AND k.service_date = c.service_date \
                 AND k.uid = c.uid \
                 AND k.seq = c.seq)",
    drop_publish: "DELETE FROM schedule_calling_points_full_publish_keys WHERE publish_id = $1",
    // ASCII "scppubfn" -- arbitrary, just distinct.
    final_lock_key: 0x7363_7070_7562_666e,
};

/// The start-of-chunk half of [`SchedulePublishPart`]'s protocol: on the
/// first chunk, discard staged keys from any other publish of these dates.
async fn discard_superseded_publish_keys(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sql: &PublishKeysSql,
    part: SchedulePublishPart<'_>,
    distinct_dates: &[chrono::NaiveDate],
) -> Result<()> {
    if part.first_chunk {
        sqlx::query(sql.discard_superseded)
            .bind(part.publish_id)
            .bind(distinct_dates)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

/// The end-of-chunk half of [`SchedulePublishPart`]'s protocol: on the final
/// chunk, verify the staged key count, delete the rows the publish did not
/// carry, and drop the publish's staged keys. Returns rows deleted.
///
/// **One delete per product at a time (2026-09-27 incident).** The final
/// chunk first takes the product's transaction-scoped advisory lock with
/// `pg_try_advisory_xact_lock`; if another final chunk of the same product
/// holds it, this chunk fails at once with [`SchedulePublishBusy`] (409) and
/// rolls back, instead of queueing. Queueing (`pg_advisory_xact_lock`) was
/// rejected: the client that sent a queued chunk has usually given up by the
/// time the lock frees, so the queued chunk would then run a full, now
/// pointless, delete of its own -- exactly the pile-up of the incident (8+
/// concurrent deletes ~45s apart), merely serialised. Failing fast costs a
/// retry nothing on the server, and the publisher does not retry a final
/// chunk refused this way within the same cycle.
///
/// Every statement from here on runs under [`PUBLISH_DELETE_STATEMENT_TIMEOUT`]
/// (passed in as `statement_timeout` so tests can shorten it).
async fn finish_publish_part(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sql: &PublishKeysSql,
    part: SchedulePublishPart<'_>,
    statement_timeout: std::time::Duration,
) -> Result<u64> {
    finish_publish_part_declaring(tx, sql, part, statement_timeout, &[]).await
}

/// [`finish_publish_part`], for a publish that also declares its service
/// dates (PL-14). The dates a publish covers are normally read back from its
/// staged keys, so a publish with NO rows (`total_rows=0`: a date that
/// legitimately has no trains, e.g. Christmas Day) used to have no dates to
/// delete from, and the previous publish's rows for that date survived as
/// stale data. When nothing is staged and the publisher's total is 0,
/// `declared_dates` stands in: every row of those dates is deleted (none of
/// them was carried by this publish). Ignored otherwise.
async fn finish_publish_part_declaring(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sql: &PublishKeysSql,
    part: SchedulePublishPart<'_>,
    statement_timeout: std::time::Duration,
    declared_dates: &[chrono::NaiveDate],
) -> Result<u64> {
    let Some(expected) = part.final_total_rows else {
        return Ok(0);
    };

    // `SET` cannot take a bind parameter; this is our own integer.
    sqlx::query(&format!(
        "SET LOCAL statement_timeout = {}",
        statement_timeout.as_millis()
    ))
    .execute(&mut **tx)
    .await?;

    let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
        .bind(sql.final_lock_key)
        .fetch_one(&mut **tx)
        .await?;
    if !locked {
        return Err(SchedulePublishBusy {
            product: sql.product,
        }
        .into());
    }

    let (staged, mut dates): (i64, Vec<chrono::NaiveDate>) = sqlx::query_as(sql.summarize)
        .bind(part.publish_id)
        .fetch_one(&mut **tx)
        .await?;
    if staged == 0 && expected == 0 {
        dates = declared_dates.to_vec();
    }

    let deleted = if u64::try_from(staged).ok() == Some(expected) && !dates.is_empty() {
        // Fresh statistics before the anti-join. Between publishes the
        // staging table is emptied, so whatever statistics autoanalyze last
        // left describe an OLDER publish: `publish_id = $1` is then a value
        // missing from the MCV list and is estimated at ~0 rows, and the
        // planner picks a nested-loop anti-join. Without an index that
        // re-scanned every staged key once per target row -- in production
        // (2026-09-27) 150k keys x ~1.66M rows, 6+ minutes at a full core.
        // The `*_publish_keys_probe` indexes (migrations 20260926220000 /
        // 20260926220100) bound the damage if a nested loop is still chosen;
        // this makes the planner see the real row count and pick a
        // hash/merge anti-join in the first place. ANALYZE also invalidates
        // this connection's cached plan for `delete_missing`.
        //
        // Cost and locking: the staging table holds about one in-flight
        // publish (~150k narrow rows), and ANALYZE samples at most 30k of
        // them -- tens of milliseconds. It takes SHARE UPDATE EXCLUSIVE,
        // held to COMMIT, which does NOT conflict with the ROW EXCLUSIVE
        // lock that other publishes' chunk INSERTs / DELETEs take, so they
        // proceed concurrently. It does conflict with itself, so two final
        // chunks of the SAME product serialise (the second waits for the
        // first's delete to commit); publishes are per-date and sequential
        // from one publisher, so that is at worst a short wait. (Two
        // concurrent finals for OVERLAPPING dates -- already the unsupported
        // "one publish supersedes another" case -- could now deadlock on
        // this lock plus target-row locks; Postgres detects that and aborts
        // one chunk, which the publisher retries, rather than hanging.)
        //
        // ANALYZE samples this transaction's own uncommitted inserts as
        // live rows, so the final chunk's keys are counted too.
        sqlx::query(sql.analyze).execute(&mut **tx).await?;

        sqlx::query(sql.delete_missing)
            .bind(part.publish_id)
            .bind(&dates)
            .execute(&mut **tx)
            .await?
            .rows_affected()
    } else {
        tracing::warn!(
            product = sql.product,
            publish_id = part.publish_id,
            staged,
            expected,
            "schedule publish finished with a staged key count that does not match the \
             publisher's total; NOT deleting rows missing from this publish (stale rows stay \
             until the next complete publish)"
        );
        metrics::counter!(
            common::metrics::metric_name(staged_mismatch_metric()),
            "product" => sql.product
        )
        .increment(1);
        0
    };

    sqlx::query(sql.drop_publish)
        .bind(part.publish_id)
        .execute(&mut **tx)
        .await?;

    Ok(deleted)
}

/// A final chunk that carries no rows -- only the end of a publish: its
/// `total_rows` (normally 0) and the `service_date` the publisher covered
/// (PL-14). With `total_rows = 0` every existing row of `service_date` is
/// deleted, since the publish carried none of them; see
/// [`finish_publish_part_declaring`]. `part` must be a final chunk
/// (`final_total_rows` set); a non-final empty chunk is a no-op.
async fn finish_publish_without_rows(
    pool: &PgPool,
    sql: &PublishKeysSql,
    part: SchedulePublishPart<'_>,
    service_date: Option<chrono::NaiveDate>,
) -> Result<u64> {
    if part.final_total_rows.is_none() {
        return Ok(0);
    }
    let declared: Vec<chrono::NaiveDate> = service_date.into_iter().collect();
    let mut tx = pool.begin().await?;
    common::pg::set_local_statement_timeout(&mut tx, SCHEDULE_CHUNK_STATEMENT_TIMEOUT).await?;
    discard_superseded_publish_keys(&mut tx, sql, part, &declared).await?;
    let deleted = finish_publish_part_declaring(
        &mut tx,
        sql,
        part,
        PUBLISH_DELETE_STATEMENT_TIMEOUT,
        &declared,
    )
    .await?;
    tx.commit().await?;
    Ok(deleted)
}

/// [`finish_publish_without_rows`] for `schedule_destination_departures`.
/// Returns rows deleted.
pub async fn finish_schedule_destination_departures_publish_without_rows(
    pool: &PgPool,
    part: SchedulePublishPart<'_>,
    service_date: Option<chrono::NaiveDate>,
) -> Result<u64> {
    finish_publish_without_rows(
        pool,
        &DESTINATION_DEPARTURES_PUBLISH_KEYS_SQL,
        part,
        service_date,
    )
    .await
}

/// [`finish_publish_without_rows`] for `schedule_calling_points_full`.
/// Returns rows deleted.
pub async fn finish_schedule_calling_points_full_publish_without_rows(
    pool: &PgPool,
    part: SchedulePublishPart<'_>,
    service_date: Option<chrono::NaiveDate>,
) -> Result<u64> {
    finish_publish_without_rows(
        pool,
        &CALLING_POINTS_FULL_PUBLISH_KEYS_SQL,
        part,
        service_date,
    )
    .await
}

/// `api_schedule_publish_staged_mismatch_total{product}`: a final publish
/// chunk whose staged key count did not match the publisher's total, so the
/// rows missing from that publish were NOT deleted (SCHED-2). The chart's
/// `DistantSignalSchedulePublishStagedMismatch` alert reads it. The api's
/// name; a direct writer counts [`STORE_SCHEDULE_PUBLISH_STAGED_MISMATCH_METRIC`]
/// instead (see [`use_store_metric_names`]).
pub const SCHEDULE_PUBLISH_STAGED_MISMATCH_METRIC: &str =
    "api_schedule_publish_staged_mismatch_total";

/// [`SCHEDULE_PUBLISH_STAGED_MISMATCH_METRIC`] under its `store_` name (spec
/// §14.1), counted by a process other than the api that publishes directly
/// (schedule-reference with `INGEST_SINK=db`, plan 2a.5). The alert takes
/// `or` of both names until phase 5 drops the api's.
pub const STORE_SCHEDULE_PUBLISH_STAGED_MISMATCH_METRIC: &str =
    "store_schedule_publish_staged_mismatch_total";

/// Whether this process counts the `store_` names; see
/// [`use_store_metric_names`].
static STORE_METRIC_NAMES: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Makes this process count the publish metrics under their `store_` names
/// (spec §14.1): call once at startup, before
/// [`register_schedule_publish_metrics`], in a direct writer. The api never
/// calls it, so its metric names are unchanged.
pub fn use_store_metric_names() {
    STORE_METRIC_NAMES.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// The staged-mismatch counter's name in this process.
fn staged_mismatch_metric() -> &'static str {
    staged_mismatch_metric_for(STORE_METRIC_NAMES.load(std::sync::atomic::Ordering::Relaxed))
}

const fn staged_mismatch_metric_for(store_names: bool) -> &'static str {
    if store_names {
        STORE_SCHEDULE_PUBLISH_STAGED_MISMATCH_METRIC
    } else {
        SCHEDULE_PUBLISH_STAGED_MISMATCH_METRIC
    }
}

/// `api_schedule_publish_rows_total{product, outcome}`: incoming rows of a
/// per-date schedule publish, by what the upsert did with them --
/// `outcome="written"` (inserted, or updated because a value changed) or
/// `outcome="unchanged"` (identical to the stored row, so skipped without
/// being written or locked; a same-key duplicate within one chunk also
/// counts here). `product` is `schedule_destination_departures`,
/// `schedule_calling_points_full` or `schedule_services`. A direct writer
/// counts [`STORE_SCHEDULE_PUBLISH_ROWS_METRIC`] instead (see
/// [`use_store_metric_names`]).
pub const SCHEDULE_PUBLISH_ROWS_METRIC: &str = "api_schedule_publish_rows_total";

/// [`SCHEDULE_PUBLISH_ROWS_METRIC`] under its `store_` name (spec §14.1).
pub const STORE_SCHEDULE_PUBLISH_ROWS_METRIC: &str = "store_schedule_publish_rows_total";

/// Every `product` label [`SCHEDULE_PUBLISH_ROWS_METRIC`] carries.
const PUBLISH_ROWS_PRODUCTS: [&str; 3] = [
    "schedule_destination_departures",
    "schedule_calling_points_full",
    crate::schedule::services::PRODUCT,
];

/// The publish-rows counter's name in this process.
fn publish_rows_metric() -> &'static str {
    if STORE_METRIC_NAMES.load(std::sync::atomic::Ordering::Relaxed) {
        STORE_SCHEDULE_PUBLISH_ROWS_METRIC
    } else {
        SCHEDULE_PUBLISH_ROWS_METRIC
    }
}

/// Counts one committed publish statement's rows in
/// [`SCHEDULE_PUBLISH_ROWS_METRIC`]: `written` of the `incoming` rows were
/// inserted or updated, the rest were unchanged.
pub(crate) fn count_publish_rows(product: &'static str, incoming: usize, written: u64) {
    let incoming = incoming as u64;
    let name = common::metrics::metric_name(publish_rows_metric());
    metrics::counter!(name.clone(), "product" => product, "outcome" => "written")
        .increment(written);
    metrics::counter!(name, "product" => product, "outcome" => "unchanged")
        .increment(incoming.saturating_sub(written));
}

/// Registers [`SCHEDULE_PUBLISH_STAGED_MISMATCH_METRIC`] at 0 for both
/// products at startup, so the alert's `increase()` sees the first mismatch,
/// and every [`SCHEDULE_PUBLISH_ROWS_METRIC`] series at 0.
pub fn register_schedule_publish_metrics() {
    for sql in [
        &DESTINATION_DEPARTURES_PUBLISH_KEYS_SQL,
        &CALLING_POINTS_FULL_PUBLISH_KEYS_SQL,
    ] {
        metrics::counter!(
            common::metrics::metric_name(staged_mismatch_metric()),
            "product" => sql.product
        )
        .increment(0);
    }
    for product in PUBLISH_ROWS_PRODUCTS {
        count_publish_rows(product, 0, 0);
    }
}

/// One chunk of a diff-based `schedule_destination_departures` publish --
/// see [`SchedulePublishPart`] for the multi-chunk protocol and
/// [`upsert_schedule_destination_departures`] for the per-row upsert
/// semantics (that function is exactly this one, called once as a first and
/// final chunk). One transaction per call. An empty `rows` is a no-op (it
/// neither stages nor finalizes). Returns rows inserted or changed.
#[expect(
    clippy::too_many_lines,
    reason = "long but linear; splitting it would scatter its shared state across helpers"
)]
pub async fn upsert_schedule_destination_departures_publish_part(
    pool: &PgPool,
    rows: &[ScheduleDestinationDeparturesRow],
    part: SchedulePublishPart<'_>,
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }

    let service_dates: Vec<chrono::NaiveDate> = rows.iter().map(|r| r.service_date).collect();
    let destination_crs: Vec<&str> = rows.iter().map(|r| r.destination_crs.as_str()).collect();
    let scheduled: Vec<chrono::NaiveTime> = rows.iter().map(|r| r.scheduled).collect();
    let day_offsets: Vec<i16> = rows.iter().map(|r| r.day_offset).collect();
    let train_uids: Vec<&str> = rows.iter().map(|r| r.train_uid.as_str()).collect();
    let origin_crs: Vec<&str> = rows.iter().map(|r| r.origin_crs.as_str()).collect();
    let true_origin_crs: Vec<Option<&str>> =
        rows.iter().map(|r| r.true_origin_crs.as_deref()).collect();
    let calling_point_arrival: Vec<Option<chrono::NaiveTime>> =
        rows.iter().map(|r| r.calling_point_arrival).collect();
    let destination_arrival: Vec<Option<chrono::NaiveTime>> =
        rows.iter().map(|r| r.destination_arrival).collect();
    let destination_arrival_day_offsets: Vec<i16> = rows
        .iter()
        .map(|r| r.destination_arrival_day_offset)
        .collect();
    let operator_atoc: Vec<Option<&str>> =
        rows.iter().map(|r| r.operator_atoc.as_deref()).collect();
    let headcode: Vec<Option<&str>> = rows.iter().map(|r| r.headcode.as_deref()).collect();
    let rsid: Vec<Option<&str>> = rows.iter().map(|r| r.rsid.as_deref()).collect();
    let public_departure: Vec<Option<chrono::NaiveTime>> =
        rows.iter().map(|r| r.public_departure).collect();
    let public_calling_point_arrival: Vec<Option<chrono::NaiveTime>> = rows
        .iter()
        .map(|r| r.public_calling_point_arrival)
        .collect();
    let public_destination_arrival: Vec<Option<chrono::NaiveTime>> =
        rows.iter().map(|r| r.public_destination_arrival).collect();
    let can_board: Vec<Option<bool>> = rows.iter().map(|r| r.can_board).collect();
    let can_alight: Vec<Option<bool>> = rows.iter().map(|r| r.can_alight).collect();

    let mut distinct_dates = service_dates.clone();
    distinct_dates.sort_unstable();
    distinct_dates.dedup();

    let sql = &DESTINATION_DEPARTURES_PUBLISH_KEYS_SQL;
    let mut tx = pool.begin().await?;
    common::pg::set_local_statement_timeout(&mut tx, SCHEDULE_CHUNK_STATEMENT_TIMEOUT).await?;

    discard_superseded_publish_keys(&mut tx, sql, part, &distinct_dates).await?;

    // One staged key per incoming row, duplicates included -- the final
    // chunk's count check compares against the publisher's raw row total.
    sqlx::query(
        "INSERT INTO schedule_destination_departures_publish_keys \
            (publish_id, service_date, destination_crs, scheduled, train_uid, origin_crs) \
         SELECT $1, * FROM UNNEST($2::date[], $3::text[], $4::time[], $5::text[], $6::text[])",
    )
    .bind(part.publish_id)
    .bind(&service_dates)
    .bind(&destination_crs)
    .bind(&scheduled)
    .bind(&train_uids)
    .bind(&origin_crs)
    .execute(&mut *tx)
    .await?;

    // DISTINCT ON ... ORDER BY key, ordinality keeps the FIRST of any
    // same-key rows in the batch (`ON CONFLICT DO UPDATE` cannot affect one
    // row twice). The LATERAL probe then drops every row whose stored copy
    // is identical, so an unchanged row never reaches `ON CONFLICT` and is
    // never even locked -- see "Unchanged rows are skipped" on
    // [`upsert_schedule_destination_departures`]. It filters AFTER the
    // dedup: filtering first could let a later same-key duplicate win.
    // `OFFSET 0` keeps the probe a per-row index lookup (a plain anti-join
    // is planned as a hash join over a seq scan of the WHOLE table, every
    // chunk). The `ON CONFLICT ... WHERE ... IS DISTINCT FROM` guard stays
    // for a row a concurrent writer changed after this statement's snapshot.
    let result = sqlx::query(
        "INSERT INTO schedule_destination_departures AS d \
            (service_date, destination_crs, scheduled, day_offset, train_uid, origin_crs, true_origin_crs, calling_point_arrival, destination_arrival, destination_arrival_day_offset, operator_atoc, headcode, rsid, \
             public_departure, public_calling_point_arrival, public_destination_arrival, can_board, can_alight) \
         SELECT n.service_date, n.destination_crs, n.scheduled, n.day_offset, n.train_uid, n.origin_crs, n.true_origin_crs, n.calling_point_arrival, n.destination_arrival, n.destination_arrival_day_offset, n.operator_atoc, n.headcode, n.rsid, \
                n.public_departure, n.public_calling_point_arrival, n.public_destination_arrival, n.can_board, n.can_alight \
         FROM ( \
             SELECT DISTINCT ON (service_date, destination_crs, scheduled, train_uid, origin_crs) \
                    service_date, destination_crs, scheduled, day_offset, train_uid, origin_crs, true_origin_crs, calling_point_arrival, destination_arrival, destination_arrival_day_offset, operator_atoc, headcode, rsid, \
                    public_departure, public_calling_point_arrival, public_destination_arrival, can_board, can_alight \
             FROM UNNEST($1::date[], $2::text[], $3::time[], $4::smallint[], $5::text[], $6::text[], $7::text[], $8::time[], $9::time[], $10::smallint[], $11::text[], $12::text[], $13::text[], \
                         $14::time[], $15::time[], $16::time[], $17::boolean[], $18::boolean[]) \
                  WITH ORDINALITY AS t(service_date, destination_crs, scheduled, day_offset, train_uid, origin_crs, true_origin_crs, calling_point_arrival, destination_arrival, destination_arrival_day_offset, operator_atoc, headcode, rsid, \
                                       public_departure, public_calling_point_arrival, public_destination_arrival, can_board, can_alight, ord) \
             ORDER BY service_date, destination_crs, scheduled, train_uid, origin_crs, ord \
         ) n \
         LEFT JOIN LATERAL ( \
             SELECT true AS found, e.day_offset, e.true_origin_crs, e.calling_point_arrival, e.destination_arrival, e.destination_arrival_day_offset, e.operator_atoc, e.headcode, e.rsid, \
                    e.public_departure, e.public_calling_point_arrival, e.public_destination_arrival, e.can_board, e.can_alight \
             FROM schedule_destination_departures e \
             WHERE (e.service_date, e.destination_crs, e.scheduled, e.train_uid, e.origin_crs) = (n.service_date, n.destination_crs, n.scheduled, n.train_uid, n.origin_crs) \
             OFFSET 0 \
         ) e ON true \
         WHERE e.found IS NULL \
            OR (e.day_offset, e.true_origin_crs, e.calling_point_arrival, e.destination_arrival, e.destination_arrival_day_offset, e.operator_atoc, e.headcode, e.rsid, \
                e.public_departure, e.public_calling_point_arrival, e.public_destination_arrival, e.can_board, e.can_alight) \
               IS DISTINCT FROM \
               (n.day_offset, n.true_origin_crs, n.calling_point_arrival, n.destination_arrival, n.destination_arrival_day_offset, n.operator_atoc, n.headcode, n.rsid, \
                n.public_departure, n.public_calling_point_arrival, n.public_destination_arrival, n.can_board, n.can_alight) \
         ON CONFLICT (service_date, destination_crs, scheduled, train_uid, origin_crs) DO UPDATE SET \
            day_offset = EXCLUDED.day_offset, \
            true_origin_crs = EXCLUDED.true_origin_crs, \
            calling_point_arrival = EXCLUDED.calling_point_arrival, \
            destination_arrival = EXCLUDED.destination_arrival, \
            destination_arrival_day_offset = EXCLUDED.destination_arrival_day_offset, \
            operator_atoc = EXCLUDED.operator_atoc, \
            headcode = EXCLUDED.headcode, \
            rsid = EXCLUDED.rsid, \
            public_departure = EXCLUDED.public_departure, \
            public_calling_point_arrival = EXCLUDED.public_calling_point_arrival, \
            public_destination_arrival = EXCLUDED.public_destination_arrival, \
            can_board = EXCLUDED.can_board, \
            can_alight = EXCLUDED.can_alight \
         WHERE (d.day_offset, d.true_origin_crs, d.calling_point_arrival, d.destination_arrival, d.destination_arrival_day_offset, d.operator_atoc, d.headcode, d.rsid, \
                d.public_departure, d.public_calling_point_arrival, d.public_destination_arrival, d.can_board, d.can_alight) \
               IS DISTINCT FROM \
               (EXCLUDED.day_offset, EXCLUDED.true_origin_crs, EXCLUDED.calling_point_arrival, EXCLUDED.destination_arrival, EXCLUDED.destination_arrival_day_offset, EXCLUDED.operator_atoc, EXCLUDED.headcode, EXCLUDED.rsid, \
                EXCLUDED.public_departure, EXCLUDED.public_calling_point_arrival, EXCLUDED.public_destination_arrival, EXCLUDED.can_board, EXCLUDED.can_alight)",
    )
    .bind(&service_dates)
    .bind(&destination_crs)
    .bind(&scheduled)
    .bind(&day_offsets)
    .bind(&train_uids)
    .bind(&origin_crs)
    .bind(&true_origin_crs)
    .bind(&calling_point_arrival)
    .bind(&destination_arrival)
    .bind(&destination_arrival_day_offsets)
    .bind(&operator_atoc)
    .bind(&headcode)
    .bind(&rsid)
    .bind(&public_departure)
    .bind(&public_calling_point_arrival)
    .bind(&public_destination_arrival)
    .bind(&can_board)
    .bind(&can_alight)
    .execute(&mut *tx)
    .await?;

    let deleted = finish_publish_part(&mut tx, sql, part, PUBLISH_DELETE_STATEMENT_TIMEOUT).await?;

    tx.commit().await?;
    count_publish_rows(sql.product, rows.len(), result.rows_affected());
    if part.final_total_rows.is_some() {
        tracing::debug!(
            publish_id = part.publish_id,
            upserted = result.rows_affected(),
            deleted,
            "finished schedule_destination_departures publish"
        );
    }
    Ok(result.rows_affected())
}

/// One `schedule_calling_points_full` row -- the literal, un-bucketed
/// "ordered `stop_times` per trip" shape, one row per calling point of one
/// resolved (non-cancelled) schedule on one service date. Mirrors
/// `schedule_query::CallingPoint` plus the schedule-level `uid` and the
/// publish-time-assigned `seq` ordering key, exactly as
/// `schedule-reference::publish_schedule_calling_points_full` emits them.
/// See docs/superpowers/plans/2026-09-22-dynamic-trip-planning-phase2-connections-array-plan.md
/// Task 1 and the migration's own doc comment
/// (`20260923100000_schedule_calling_points_full.sql`).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ScheduleCallingPointsFullRow {
    pub service_date: chrono::NaiveDate,
    pub uid: String,
    /// 0-based position within this schedule's own calling-point sequence
    /// (from the publisher's own `.enumerate()`, `schedule-reference`'s
    /// `publish_schedule_calling_points_full`) -- the ORDER BY key that
    /// reconstructs stopping order; NOT a real CIF field, assigned at
    /// publish time.
    pub seq: i16,
    pub tiploc: String,
    /// One of `"origin"`, `"intermediate"`, `"terminate"` -- mirrors
    /// `schedule_query::CallingPointKind`'s three variants verbatim, kept
    /// as a plain string here (not a Rust enum) since this row's only job
    /// is to pass straight through to the `CHECK (kind IN (...))` column
    /// the migration defines.
    pub kind: String,
    pub booked_arrival: Option<chrono::NaiveTime>,
    pub booked_departure: Option<chrono::NaiveTime>,
    pub day_offset: i16,
    /// CIF booked platform -- see
    /// `schedule_query::records::CallingPoint::platform`. `#[serde(default)]`
    /// so a `schedule-reference` build that predates this field still
    /// ingests (as NULL, "not known") during a rolling deploy.
    #[serde(default)]
    pub platform: Option<String>,
    /// CIF public (GBTT) arrival -- see
    /// `schedule_query::records::CallingPoint::public_arrival`. This and
    /// every field below are `#[serde(default)]` so a `schedule-reference`
    /// build that predates them still ingests (as NULL, "not known").
    #[serde(default)]
    pub public_arrival: Option<chrono::NaiveTime>,
    #[serde(default)]
    pub public_departure: Option<chrono::NaiveTime>,
    /// The exact working (WTT) times, `:30` seconds for a half-minute --
    /// `CallingPoint::working_arrival`/`working_departure`/`working_pass`.
    #[serde(default)]
    pub working_arrival: Option<chrono::NaiveTime>,
    #[serde(default)]
    pub working_departure: Option<chrono::NaiveTime>,
    #[serde(default)]
    pub working_pass: Option<chrono::NaiveTime>,
    /// `CallingPoint::can_board`/`can_alight`/`is_request_stop`.
    #[serde(default)]
    pub can_board: Option<bool>,
    #[serde(default)]
    pub can_alight: Option<bool>,
    #[serde(default)]
    pub request_stop: Option<bool>,
}

/// Replaces one service date's worth of whole-network resolved calling
/// points -- `rows` is the COMPLETE new set for every service date it
/// touches. Same diff shape as [`upsert_schedule_destination_departures`]
/// (see its doc comment for the 2026-09-26 index-bloat measurement that
/// motivated it): one transaction, one `INSERT ... SELECT FROM UNNEST(...)
/// ON CONFLICT (service_date, uid, seq) DO UPDATE ... WHERE (<non-key
/// columns>) IS DISTINCT FROM (...)` so unchanged calling points are never
/// rewritten, then a `DELETE` of every row of the touched dates whose key is
/// not in `rows`. Observably identical to the wholesale replace it
/// supersedes.
///
/// **An empty `rows` is a no-op, and that is load-bearing**, same posture
/// and same reason as `upsert_schedule_destination_departures`: a publish
/// that produced nothing must not be allowed to delete a service date's
/// real data.
///
/// Same-key rows within one batch collapse to the first of them. Returns
/// rows inserted or changed.
///
/// The "one call is the whole day" entry point every in-process caller
/// (this file's own tests, `crates/api/src/routes/train.rs` and
/// `crates/api/src/data/journey.rs`'s fixtures) uses. The ingest route
/// (`crates/api/src/routes/ingest.rs::post_schedule_calling_points_full`)
/// instead calls [`upsert_schedule_calling_points_full_publish_part`].
pub async fn upsert_schedule_calling_points_full(
    pool: &PgPool,
    rows: &[ScheduleCallingPointsFullRow],
) -> Result<u64> {
    let publish_id = in_process_publish_id();
    upsert_schedule_calling_points_full_publish_part(
        pool,
        rows,
        SchedulePublishPart {
            publish_id: &publish_id,
            first_chunk: true,
            final_total_rows: Some(rows.len() as u64),
        },
    )
    .await
}

/// One chunk of a diff-based `schedule_calling_points_full` publish -- see
/// [`SchedulePublishPart`] for the multi-chunk protocol and
/// [`upsert_schedule_calling_points_full`] for the per-row upsert semantics
/// (that function is exactly this one, called once as a first and final
/// chunk). One transaction per call. An empty `rows` is a no-op. Returns
/// rows inserted or changed.
#[expect(
    clippy::too_many_lines,
    reason = "long but linear; splitting it would scatter its shared state across helpers"
)]
pub async fn upsert_schedule_calling_points_full_publish_part(
    pool: &PgPool,
    rows: &[ScheduleCallingPointsFullRow],
    part: SchedulePublishPart<'_>,
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }

    let service_dates: Vec<chrono::NaiveDate> = rows.iter().map(|r| r.service_date).collect();
    let uids: Vec<&str> = rows.iter().map(|r| r.uid.as_str()).collect();
    let seqs: Vec<i16> = rows.iter().map(|r| r.seq).collect();
    let tiplocs: Vec<&str> = rows.iter().map(|r| r.tiploc.as_str()).collect();
    let kinds: Vec<&str> = rows.iter().map(|r| r.kind.as_str()).collect();
    let booked_arrivals: Vec<Option<chrono::NaiveTime>> =
        rows.iter().map(|r| r.booked_arrival).collect();
    let booked_departures: Vec<Option<chrono::NaiveTime>> =
        rows.iter().map(|r| r.booked_departure).collect();
    let day_offsets: Vec<i16> = rows.iter().map(|r| r.day_offset).collect();
    let platforms: Vec<Option<&str>> = rows.iter().map(|r| r.platform.as_deref()).collect();
    let public_arrivals: Vec<Option<chrono::NaiveTime>> =
        rows.iter().map(|r| r.public_arrival).collect();
    let public_departures: Vec<Option<chrono::NaiveTime>> =
        rows.iter().map(|r| r.public_departure).collect();
    let working_arrivals: Vec<Option<chrono::NaiveTime>> =
        rows.iter().map(|r| r.working_arrival).collect();
    let working_departures: Vec<Option<chrono::NaiveTime>> =
        rows.iter().map(|r| r.working_departure).collect();
    let working_passes: Vec<Option<chrono::NaiveTime>> =
        rows.iter().map(|r| r.working_pass).collect();
    let can_board: Vec<Option<bool>> = rows.iter().map(|r| r.can_board).collect();
    let can_alight: Vec<Option<bool>> = rows.iter().map(|r| r.can_alight).collect();
    let request_stop: Vec<Option<bool>> = rows.iter().map(|r| r.request_stop).collect();

    let mut distinct_dates = service_dates.clone();
    distinct_dates.sort_unstable();
    distinct_dates.dedup();

    let sql = &CALLING_POINTS_FULL_PUBLISH_KEYS_SQL;
    let mut tx = pool.begin().await?;
    common::pg::set_local_statement_timeout(&mut tx, SCHEDULE_CHUNK_STATEMENT_TIMEOUT).await?;

    discard_superseded_publish_keys(&mut tx, sql, part, &distinct_dates).await?;

    sqlx::query(
        "INSERT INTO schedule_calling_points_full_publish_keys (publish_id, service_date, uid, seq) \
         SELECT $1, * FROM UNNEST($2::date[], $3::text[], $4::smallint[])",
    )
    .bind(part.publish_id)
    .bind(&service_dates)
    .bind(&uids)
    .bind(&seqs)
    .execute(&mut *tx)
    .await?;

    // See `upsert_schedule_destination_departures_publish_part` for the
    // DISTINCT ON / LATERAL unchanged-row skip / IS DISTINCT FROM
    // reasoning -- identical here.
    let result = sqlx::query(
        "INSERT INTO schedule_calling_points_full AS c \
            (service_date, uid, seq, tiploc, kind, booked_arrival, booked_departure, day_offset, platform, \
             public_arrival, public_departure, working_arrival, working_departure, working_pass, \
             can_board, can_alight, request_stop) \
         SELECT n.service_date, n.uid, n.seq, n.tiploc, n.kind, n.booked_arrival, n.booked_departure, n.day_offset, n.platform, \
                n.public_arrival, n.public_departure, n.working_arrival, n.working_departure, n.working_pass, \
                n.can_board, n.can_alight, n.request_stop \
         FROM ( \
             SELECT DISTINCT ON (service_date, uid, seq) \
                    service_date, uid, seq, tiploc, kind, booked_arrival, booked_departure, day_offset, platform, \
                    public_arrival, public_departure, working_arrival, working_departure, working_pass, \
                    can_board, can_alight, request_stop \
             FROM UNNEST($1::date[], $2::text[], $3::smallint[], $4::text[], $5::text[], $6::time[], $7::time[], $8::smallint[], $9::text[], \
                         $10::time[], $11::time[], $12::time[], $13::time[], $14::time[], $15::bool[], $16::bool[], $17::bool[]) \
                  WITH ORDINALITY AS t(service_date, uid, seq, tiploc, kind, booked_arrival, booked_departure, day_offset, platform, \
                                       public_arrival, public_departure, working_arrival, working_departure, working_pass, \
                                       can_board, can_alight, request_stop, ord) \
             ORDER BY service_date, uid, seq, ord \
         ) n \
         LEFT JOIN LATERAL ( \
             SELECT true AS found, e.tiploc, e.kind, e.booked_arrival, e.booked_departure, e.day_offset, e.platform, \
                    e.public_arrival, e.public_departure, e.working_arrival, e.working_departure, e.working_pass, \
                    e.can_board, e.can_alight, e.request_stop \
             FROM schedule_calling_points_full e \
             WHERE (e.service_date, e.uid, e.seq) = (n.service_date, n.uid, n.seq) \
             OFFSET 0 \
         ) e ON true \
         WHERE e.found IS NULL \
            OR (e.tiploc, e.kind, e.booked_arrival, e.booked_departure, e.day_offset, e.platform, \
                e.public_arrival, e.public_departure, e.working_arrival, e.working_departure, e.working_pass, \
                e.can_board, e.can_alight, e.request_stop) \
               IS DISTINCT FROM \
               (n.tiploc, n.kind, n.booked_arrival, n.booked_departure, n.day_offset, n.platform, \
                n.public_arrival, n.public_departure, n.working_arrival, n.working_departure, n.working_pass, \
                n.can_board, n.can_alight, n.request_stop) \
         ON CONFLICT (service_date, uid, seq) DO UPDATE SET \
            tiploc = EXCLUDED.tiploc, \
            kind = EXCLUDED.kind, \
            booked_arrival = EXCLUDED.booked_arrival, \
            booked_departure = EXCLUDED.booked_departure, \
            day_offset = EXCLUDED.day_offset, \
            platform = EXCLUDED.platform, \
            public_arrival = EXCLUDED.public_arrival, \
            public_departure = EXCLUDED.public_departure, \
            working_arrival = EXCLUDED.working_arrival, \
            working_departure = EXCLUDED.working_departure, \
            working_pass = EXCLUDED.working_pass, \
            can_board = EXCLUDED.can_board, \
            can_alight = EXCLUDED.can_alight, \
            request_stop = EXCLUDED.request_stop \
         WHERE (c.tiploc, c.kind, c.booked_arrival, c.booked_departure, c.day_offset, c.platform, \
                c.public_arrival, c.public_departure, c.working_arrival, c.working_departure, c.working_pass, \
                c.can_board, c.can_alight, c.request_stop) \
               IS DISTINCT FROM \
               (EXCLUDED.tiploc, EXCLUDED.kind, EXCLUDED.booked_arrival, EXCLUDED.booked_departure, EXCLUDED.day_offset, EXCLUDED.platform, \
                EXCLUDED.public_arrival, EXCLUDED.public_departure, EXCLUDED.working_arrival, EXCLUDED.working_departure, EXCLUDED.working_pass, \
                EXCLUDED.can_board, EXCLUDED.can_alight, EXCLUDED.request_stop)",
    )
    .bind(&service_dates)
    .bind(&uids)
    .bind(&seqs)
    .bind(&tiplocs)
    .bind(&kinds)
    .bind(&booked_arrivals)
    .bind(&booked_departures)
    .bind(&day_offsets)
    .bind(&platforms)
    .bind(&public_arrivals)
    .bind(&public_departures)
    .bind(&working_arrivals)
    .bind(&working_departures)
    .bind(&working_passes)
    .bind(&can_board)
    .bind(&can_alight)
    .bind(&request_stop)
    .execute(&mut *tx)
    .await?;

    let deleted = finish_publish_part(&mut tx, sql, part, PUBLISH_DELETE_STATEMENT_TIMEOUT).await?;

    tx.commit().await?;
    count_publish_rows(sql.product, rows.len(), result.rows_affected());
    if part.final_total_rows.is_some() {
        tracing::debug!(
            publish_id = part.publish_id,
            upserted = result.rows_affected(),
            deleted,
            "finished schedule_calling_points_full publish"
        );
    }
    Ok(result.rows_affected())
}

/// The diff-based schedule publish (`SchedulePublishPart`,
/// `upsert_schedule_destination_departures_publish_part`,
/// `upsert_schedule_calling_points_full_publish_part`): an unchanged row is
/// never physically rewritten, a changed row is updated, a row missing from
/// the new publish is deleted -- but only once the whole publish has
/// arrived -- and nothing outside the publish's own dates is touched.
///
/// "Physically untouched" is asserted through each row's `xmin` (the
/// inserting/updating transaction id) and `ctid` (its heap location): an
/// `UPDATE` -- even a no-op one -- writes a new tuple version with a new
/// `xmin` and, since the new version lives somewhere else, a new `ctid`.
/// Both staying equal means no tuple (and so no index entry) was written.
///
/// Fixture dates are July 2099, used by no other test module.
#[cfg(test)]
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::too_many_lines,
    reason = "test code: casts of small known test values; scenario tests read top to bottom"
)]
mod schedule_publish_diff_tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    async fn test_pool() -> PgPool {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres")
    }

    fn fixture_date(day: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(2099, 7, day).expect("valid fixture date")
    }

    fn time(h: u32, m: u32) -> chrono::NaiveTime {
        chrono::NaiveTime::from_hms_opt(h, m, 0).expect("valid fixture time")
    }

    fn departure(
        service_date: chrono::NaiveDate,
        uid: &str,
        scheduled: chrono::NaiveTime,
        operator_atoc: Option<&str>,
    ) -> ScheduleDestinationDeparturesRow {
        ScheduleDestinationDeparturesRow {
            service_date,
            destination_crs: "ZRD".to_string(),
            scheduled,
            day_offset: 0,
            train_uid: uid.to_string(),
            origin_crs: "EUS".to_string(),
            true_origin_crs: None,
            calling_point_arrival: None,
            destination_arrival: Some(time(12, 0)),
            destination_arrival_day_offset: 0,
            operator_atoc: operator_atoc.map(str::to_string),
            headcode: None,
            rsid: None,
            ..Default::default()
        }
    }

    fn calling_point(
        service_date: chrono::NaiveDate,
        uid: &str,
        seq: i16,
        platform: Option<&str>,
    ) -> ScheduleCallingPointsFullRow {
        ScheduleCallingPointsFullRow {
            service_date,
            uid: uid.to_string(),
            seq,
            tiploc: "EUSTON".to_string(),
            kind: "intermediate".to_string(),
            booked_arrival: Some(time(8, 0)),
            booked_departure: Some(time(8, 2)),
            day_offset: 0,
            platform: platform.map(str::to_string),
            ..Default::default()
        }
    }

    async fn clear_dates(pool: &PgPool, dates: &[chrono::NaiveDate]) {
        for table in [
            "schedule_destination_departures",
            "schedule_calling_points_full",
            "schedule_destination_departures_publish_keys",
            "schedule_calling_points_full_publish_keys",
        ] {
            sqlx::query(&format!(
                "DELETE FROM {table} WHERE service_date = ANY($1::date[])"
            ))
            .bind(dates)
            .execute(pool)
            .await
            .expect("cleanup fixture rows");
        }
    }

    /// `(train_uid, scheduled, operator_atoc, xmin, ctid)` for every
    /// departure row of `date`, in key order.
    type DepartureTuple = (String, chrono::NaiveTime, Option<String>, String, String);

    async fn departure_tuples(pool: &PgPool, date: chrono::NaiveDate) -> Vec<DepartureTuple> {
        sqlx::query_as(
            "SELECT train_uid, scheduled, operator_atoc, xmin::text, ctid::text \
             FROM schedule_destination_departures WHERE service_date = $1 \
             ORDER BY train_uid, scheduled",
        )
        .bind(date)
        .fetch_all(pool)
        .await
        .expect("read back departures")
    }

    /// `(uid, seq, platform, xmin, ctid)` for every calling-point row of
    /// `date`, in key order.
    type CallingPointTuple = (String, i16, Option<String>, String, String);

    async fn calling_point_tuples(
        pool: &PgPool,
        date: chrono::NaiveDate,
    ) -> Vec<CallingPointTuple> {
        sqlx::query_as(
            "SELECT uid, seq, platform, xmin::text, ctid::text \
             FROM schedule_calling_points_full WHERE service_date = $1 ORDER BY uid, seq",
        )
        .bind(date)
        .fetch_all(pool)
        .await
        .expect("read back calling points")
    }

    fn uids<T>(tuples: &[(String, T, Option<String>, String, String)]) -> Vec<&str> {
        tuples.iter().map(|t| t.0.as_str()).collect()
    }

    async fn staged_key_count(pool: &PgPool, table: &str, publish_id: &str) -> i64 {
        sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM {table} WHERE publish_id = $1"
        ))
        .bind(publish_id)
        .fetch_one(pool)
        .await
        .expect("count staged keys")
    }

    /// **The point of the change.** Republishing a byte-identical set writes
    /// nothing: every row keeps its `xmin` and `ctid`, and the call reports
    /// zero rows upserted.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_publish_diff -- --ignored --test-threads=1`"]
    async fn republishing_identical_departures_leaves_every_row_physically_untouched() {
        let pool = test_pool().await;
        let date = fixture_date(1);
        clear_dates(&pool, &[date]).await;

        let rows = vec![
            departure(date, "DIFF-A", time(8, 0), Some("VT")),
            departure(date, "DIFF-B", time(9, 0), None),
            departure(date, "DIFF-C", time(10, 0), Some("LM")),
        ];
        assert_eq!(
            upsert_schedule_destination_departures(&pool, &rows)
                .await
                .expect("first publish"),
            3
        );
        let before = departure_tuples(&pool, date).await;

        let upserted = upsert_schedule_destination_departures(&pool, &rows)
            .await
            .expect("identical republish");
        let after = departure_tuples(&pool, date).await;

        assert_eq!(upserted, 0, "an identical republish changes no row");
        assert_eq!(
            after, before,
            "an identical republish must not write a single tuple (xmin/ctid unchanged)"
        );
        assert_eq!(
            locked_rows(&pool, "schedule_destination_departures", date).await,
            0,
            "an identical republish must not lock a single row (no heap-lock WAL)"
        );

        clear_dates(&pool, &[date]).await;
    }

    /// The same property for `schedule_calling_points_full`.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_publish_diff -- --ignored --test-threads=1`"]
    async fn republishing_identical_calling_points_leaves_every_row_physically_untouched() {
        let pool = test_pool().await;
        let date = fixture_date(2);
        clear_dates(&pool, &[date]).await;

        let rows = vec![
            calling_point(date, "DIFF-A", 0, Some("1")),
            calling_point(date, "DIFF-A", 1, None),
            calling_point(date, "DIFF-B", 0, Some("4A")),
        ];
        upsert_schedule_calling_points_full(&pool, &rows)
            .await
            .expect("first publish");
        let before = calling_point_tuples(&pool, date).await;

        let upserted = upsert_schedule_calling_points_full(&pool, &rows)
            .await
            .expect("identical republish");

        assert_eq!(upserted, 0);
        assert_eq!(calling_point_tuples(&pool, date).await, before);
        assert_eq!(
            locked_rows(&pool, "schedule_calling_points_full", date).await,
            0,
            "an identical republish must not lock a single row"
        );

        clear_dates(&pool, &[date]).await;
    }

    /// Rows of `table` on `date` with a non-zero `xmax`: rows some
    /// transaction updated, deleted or LOCKED since they were written. `ON
    /// CONFLICT DO UPDATE ... WHERE false` locks the conflicting row (and
    /// so writes WAL) without changing `xmin`/`ctid`; this is how the
    /// unchanged-row skip is observed.
    async fn locked_rows(pool: &PgPool, table: &str, date: chrono::NaiveDate) -> i64 {
        sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM {table} WHERE service_date = $1 AND xmax::text <> '0'"
        ))
        .bind(date)
        .fetch_one(pool)
        .await
        .expect("count locked rows")
    }

    /// The WAL cut (2026-10-08) end to end over the multi-chunk protocol: a
    /// two-chunk republish carrying one unchanged, one changed and one new
    /// row writes exactly the changed and new rows, never touches (not
    /// even locks) the unchanged one, still deletes the row it dropped, and
    /// counts written vs unchanged rows. A second, all-unchanged republish
    /// whose final count does not match still takes the staged-mismatch
    /// path: the skip does not affect key staging.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_publish_diff -- --ignored --test-threads=1`"]
    async fn a_republish_skips_unchanged_rows_without_locking_them() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let pool = test_pool().await;
        let date = fixture_date(14);
        clear_dates(&pool, &[date]).await;

        upsert_schedule_destination_departures(
            &pool,
            &[
                departure(date, "KEEP", time(8, 0), Some("VT")),
                departure(date, "CHANGE", time(9, 0), Some("VT")),
                departure(date, "DROP", time(10, 0), Some("VT")),
            ],
        )
        .await
        .expect("seed");
        let before = departure_tuples(&pool, date).await;
        let keep_before = before.iter().find(|t| t.0 == "KEEP").unwrap().clone();
        let change_before = before.iter().find(|t| t.0 == "CHANGE").unwrap().clone();

        let publish_id = "test-wal-cut";
        let first = upsert_schedule_destination_departures_publish_part(
            &pool,
            &[
                departure(date, "KEEP", time(8, 0), Some("VT")),
                departure(date, "CHANGE", time(9, 0), Some("LM")),
            ],
            SchedulePublishPart {
                publish_id,
                first_chunk: true,
                final_total_rows: None,
            },
        )
        .await
        .expect("first chunk");
        assert_eq!(first, 1, "only the changed row is written");
        let last = upsert_schedule_destination_departures_publish_part(
            &pool,
            &[departure(date, "NEW", time(11, 0), None)],
            SchedulePublishPart {
                publish_id,
                first_chunk: false,
                final_total_rows: Some(3),
            },
        )
        .await
        .expect("final chunk");
        assert_eq!(last, 1, "the new row is inserted");

        let after = departure_tuples(&pool, date).await;
        assert_eq!(uids(&after), vec!["CHANGE", "KEEP", "NEW"], "DROP deleted");
        let change = after.iter().find(|t| t.0 == "CHANGE").unwrap();
        assert_eq!(
            change.2.as_deref(),
            Some("LM"),
            "the changed row is written"
        );
        assert_ne!(change.3, change_before.3);
        let keep = after.iter().find(|t| t.0 == "KEEP").unwrap();
        assert_eq!(keep, &keep_before, "the unchanged row is not rewritten");
        let keep_xmax = || async {
            sqlx::query_scalar::<_, String>(
                "SELECT xmax::text FROM schedule_destination_departures \
                 WHERE service_date = $1 AND train_uid = 'KEEP'",
            )
            .bind(date)
            .fetch_one(&pool)
            .await
            .expect("read KEEP xmax")
        };
        assert_eq!(
            keep_xmax().await,
            "0",
            "the unchanged row is not even locked"
        );

        // Written: the seed's 3 rows, then CHANGE and NEW. Unchanged: KEEP.
        let rendered = handle.render();
        for line in [
            r#"distant_signal_api_schedule_publish_rows_total{product="schedule_destination_departures",outcome="written"} 5"#,
            r#"distant_signal_api_schedule_publish_rows_total{product="schedule_destination_departures",outcome="unchanged"} 1"#,
        ] {
            assert!(rendered.contains(line), "{line}\n{rendered}");
        }

        // All-unchanged final chunk claiming more rows than were staged.
        let settled = departure_tuples(&pool, date).await;
        upsert_schedule_destination_departures_publish_part(
            &pool,
            &[departure(date, "KEEP", time(8, 0), Some("VT"))],
            SchedulePublishPart {
                publish_id: "test-wal-cut-mismatch",
                first_chunk: true,
                final_total_rows: Some(2),
            },
        )
        .await
        .expect("mismatched final chunk");
        assert_eq!(
            departure_tuples(&pool, date).await,
            settled,
            "a mismatched publish deletes nothing and rewrites nothing"
        );
        // (Only KEEP: the row CHANGE was updated through `ON CONFLICT`, whose
        // new version carries the updater's lock-only xmax.)
        assert_eq!(keep_xmax().await, "0", "KEEP is still never locked");
        assert!(
            handle.render().contains(
                r#"distant_signal_api_schedule_publish_staged_mismatch_total{product="schedule_destination_departures"} 1"#
            ),
            "the staged-mismatch path still triggers"
        );

        clear_dates(&pool, &[date]).await;
    }

    /// A republish that changes one row, drops one and adds one: the changed
    /// row is updated in place, the dropped one is deleted, the new one is
    /// inserted, the unchanged one is not rewritten -- and a second date the
    /// publish does not touch is left physically alone.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_publish_diff -- --ignored --test-threads=1`"]
    async fn a_departures_republish_updates_changed_deletes_missing_and_spares_other_dates() {
        let pool = test_pool().await;
        let date = fixture_date(3);
        let other_date = fixture_date(4);
        clear_dates(&pool, &[date, other_date]).await;

        upsert_schedule_destination_departures(
            &pool,
            &[
                departure(date, "KEEP", time(8, 0), Some("VT")),
                departure(date, "CHANGE", time(9, 0), Some("VT")),
                departure(date, "DROP", time(10, 0), Some("VT")),
            ],
        )
        .await
        .expect("seed date");
        upsert_schedule_destination_departures(
            &pool,
            &[departure(other_date, "OTHER", time(8, 0), Some("VT"))],
        )
        .await
        .expect("seed other date");
        let before = departure_tuples(&pool, date).await;
        let other_before = departure_tuples(&pool, other_date).await;

        let upserted = upsert_schedule_destination_departures(
            &pool,
            &[
                departure(date, "KEEP", time(8, 0), Some("VT")),
                departure(date, "CHANGE", time(9, 0), Some("LM")),
                departure(date, "NEW", time(11, 0), None),
            ],
        )
        .await
        .expect("republish");
        let after = departure_tuples(&pool, date).await;

        assert_eq!(upserted, 2, "one update plus one insert");
        assert_eq!(uids(&after), vec!["CHANGE", "KEEP", "NEW"]);
        let change = &after[0];
        assert_eq!(
            change.2.as_deref(),
            Some("LM"),
            "the changed row is updated"
        );
        assert_ne!(
            change.3, before[0].3,
            "the changed row got a new tuple version"
        );
        let keep_before = before.iter().find(|t| t.0 == "KEEP").unwrap();
        assert_eq!(
            (&after[1].3, &after[1].4),
            (&keep_before.3, &keep_before.4),
            "the unchanged row is not rewritten"
        );
        assert_eq!(
            departure_tuples(&pool, other_date).await,
            other_before,
            "a date outside the publish is untouched"
        );

        clear_dates(&pool, &[date, other_date]).await;
    }

    async fn stored_rsids(pool: &PgPool, date: chrono::NaiveDate) -> Vec<(String, Option<String>)> {
        sqlx::query_as(
            "SELECT train_uid, rsid FROM schedule_destination_departures \
             WHERE service_date = $1 ORDER BY train_uid",
        )
        .bind(date)
        .fetch_all(pool)
        .await
        .expect("read back rsids")
    }

    /// `rsid` is part of the diff: a republish that changes ONLY a row's
    /// Retail Service ID updates it (it is in the `IS DISTINCT FROM` guard),
    /// and an identical republish after that still writes nothing.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_publish_diff -- --ignored --test-threads=1`"]
    async fn a_departures_republish_that_only_changes_rsid_updates_the_row() {
        let pool = test_pool().await;
        let date = fixture_date(11);
        clear_dates(&pool, &[date]).await;

        let with_rsid = |rsid: Option<&str>| ScheduleDestinationDeparturesRow {
            rsid: rsid.map(str::to_string),
            ..departure(date, "RSID-A", time(8, 0), Some("VT"))
        };
        let untouched = departure(date, "RSID-B", time(9, 0), Some("VT"));

        upsert_schedule_destination_departures(&pool, &[with_rsid(None), untouched.clone()])
            .await
            .expect("first publish");
        assert_eq!(
            stored_rsids(&pool, date).await,
            vec![("RSID-A".to_string(), None), ("RSID-B".to_string(), None)]
        );

        let changed = [with_rsid(Some("VT123401")), untouched];
        assert_eq!(
            upsert_schedule_destination_departures(&pool, &changed)
                .await
                .expect("rsid-only republish"),
            1,
            "a changed rsid alone must count as a changed row"
        );
        assert_eq!(
            stored_rsids(&pool, date).await,
            vec![
                ("RSID-A".to_string(), Some("VT123401".to_string())),
                ("RSID-B".to_string(), None),
            ]
        );
        let before = departure_tuples(&pool, date).await;
        assert_eq!(
            upsert_schedule_destination_departures(&pool, &changed)
                .await
                .expect("identical republish"),
            0
        );
        assert_eq!(departure_tuples(&pool, date).await, before);

        clear_dates(&pool, &[date]).await;
    }

    /// **The multi-chunk contract.** A publish split over three chunks ends
    /// with exactly the union of the chunks: rows from an earlier chunk are
    /// not lost to a later one, and the previous publish's rows that no chunk
    /// carried are deleted -- but only by the FINAL chunk, so a reader never
    /// sees the date shrink mid-publish. A row carried unchanged by a later
    /// chunk is never rewritten.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_publish_diff -- --ignored --test-threads=1`"]
    async fn a_multi_chunk_departures_publish_ends_with_exactly_the_union_of_its_chunks() {
        let pool = test_pool().await;
        let date = fixture_date(5);
        let other_date = fixture_date(6);
        clear_dates(&pool, &[date, other_date]).await;

        upsert_schedule_destination_departures(
            &pool,
            &[
                departure(date, "SAME", time(8, 0), Some("VT")),
                departure(date, "STALE", time(9, 0), Some("VT")),
            ],
        )
        .await
        .expect("previous publish");
        upsert_schedule_destination_departures(
            &pool,
            &[departure(other_date, "OTHER", time(8, 0), None)],
        )
        .await
        .expect("seed other date");
        let same_before = departure_tuples(&pool, date).await[0].clone();
        let other_before = departure_tuples(&pool, other_date).await;

        let publish_id = "test-multi-chunk-departures";
        let part = |first_chunk, final_total_rows| SchedulePublishPart {
            publish_id,
            first_chunk,
            final_total_rows,
        };
        upsert_schedule_destination_departures_publish_part(
            &pool,
            &[
                departure(date, "C1-A", time(6, 0), None),
                departure(date, "C1-B", time(6, 30), None),
            ],
            part(true, None),
        )
        .await
        .expect("chunk 1");
        upsert_schedule_destination_departures_publish_part(
            &pool,
            &[departure(date, "C2-A", time(7, 0), None)],
            part(false, None),
        )
        .await
        .expect("chunk 2");

        assert_eq!(
            uids(&departure_tuples(&pool, date).await),
            vec!["C1-A", "C1-B", "C2-A", "SAME", "STALE"],
            "mid-publish, nothing is deleted yet: old rows plus the chunks so far"
        );

        upsert_schedule_destination_departures_publish_part(
            &pool,
            &[
                departure(date, "SAME", time(8, 0), Some("VT")),
                departure(date, "C3-A", time(12, 0), None),
            ],
            part(false, Some(5)),
        )
        .await
        .expect("final chunk");

        let after = departure_tuples(&pool, date).await;
        assert_eq!(
            uids(&after),
            vec!["C1-A", "C1-B", "C2-A", "C3-A", "SAME"],
            "exactly the union of the chunks: no chunk lost, the stale row gone"
        );
        let same_after = after.iter().find(|t| t.0 == "SAME").unwrap();
        assert_eq!(
            same_after, &same_before,
            "an unchanged row is never rewritten"
        );
        assert_eq!(departure_tuples(&pool, other_date).await, other_before);
        assert_eq!(
            staged_key_count(
                &pool,
                "schedule_destination_departures_publish_keys",
                publish_id
            )
            .await,
            0,
            "the final chunk drops its publish's staged keys"
        );

        clear_dates(&pool, &[date, other_date]).await;
    }

    /// The calling-points sibling of the multi-chunk contract, including an
    /// in-place update carried by a later chunk.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_publish_diff -- --ignored --test-threads=1`"]
    async fn a_multi_chunk_calling_points_publish_ends_with_exactly_the_union_of_its_chunks() {
        let pool = test_pool().await;
        let date = fixture_date(7);
        clear_dates(&pool, &[date]).await;

        upsert_schedule_calling_points_full(
            &pool,
            &[
                calling_point(date, "SAME", 0, Some("1")),
                calling_point(date, "SAME", 1, Some("2")),
                calling_point(date, "STALE", 0, None),
            ],
        )
        .await
        .expect("previous publish");
        let before = calling_point_tuples(&pool, date).await;

        let publish_id = "test-multi-chunk-calling-points";
        let part = |first_chunk, final_total_rows| SchedulePublishPart {
            publish_id,
            first_chunk,
            final_total_rows,
        };
        upsert_schedule_calling_points_full_publish_part(
            &pool,
            &[
                calling_point(date, "C1", 0, None),
                calling_point(date, "SAME", 0, Some("1")),
            ],
            part(true, None),
        )
        .await
        .expect("chunk 1");
        let upserted = upsert_schedule_calling_points_full_publish_part(
            &pool,
            &[
                calling_point(date, "SAME", 1, Some("3")),
                calling_point(date, "C2", 0, None),
            ],
            part(false, Some(4)),
        )
        .await
        .expect("final chunk");
        assert_eq!(upserted, 2, "one platform change plus one new row");

        let after = calling_point_tuples(&pool, date).await;
        let keys: Vec<(&str, i16)> = after.iter().map(|t| (t.0.as_str(), t.1)).collect();
        assert_eq!(keys, vec![("C1", 0), ("C2", 0), ("SAME", 0), ("SAME", 1)]);
        assert_eq!(after[2], before[0], "SAME/0 unchanged, so never rewritten");
        assert_eq!(
            after[3].2.as_deref(),
            Some("3"),
            "SAME/1's platform updated"
        );

        clear_dates(&pool, &[date]).await;
    }

    /// PL-14: a publish with no rows for its date (`total_rows=0`, e.g. no
    /// trains on Christmas Day) deletes that date's previous rows -- and
    /// only that date's.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_publish_diff -- --ignored --test-threads=1`"]
    async fn an_empty_final_publish_clears_only_its_declared_date() {
        let pool = test_pool().await;
        let date = fixture_date(15);
        let other = fixture_date(16);
        clear_dates(&pool, &[date, other]).await;
        for day in [date, other] {
            upsert_schedule_destination_departures(
                &pool,
                &[departure(day, "PREVIOUS", time(8, 0), None)],
            )
            .await
            .expect("previous publish");
        }

        // A non-final empty chunk is a no-op.
        let deleted = finish_schedule_destination_departures_publish_without_rows(
            &pool,
            SchedulePublishPart {
                publish_id: "test-empty-middle",
                first_chunk: true,
                final_total_rows: None,
            },
            Some(date),
        )
        .await
        .expect("non-final empty chunk");
        assert_eq!(deleted, 0);
        assert_eq!(uids(&departure_tuples(&pool, date).await), vec!["PREVIOUS"]);

        let deleted = finish_schedule_destination_departures_publish_without_rows(
            &pool,
            SchedulePublishPart {
                publish_id: "test-empty-final",
                first_chunk: true,
                final_total_rows: Some(0),
            },
            Some(date),
        )
        .await
        .expect("empty final publish");
        assert_eq!(deleted, 1);
        assert!(departure_tuples(&pool, date).await.is_empty());
        assert_eq!(
            uids(&departure_tuples(&pool, other).await),
            vec!["PREVIOUS"],
            "another date is untouched"
        );

        // Without a declared date there is nothing it may delete.
        upsert_schedule_calling_points_full(&pool, &[calling_point(date, "CP", 0, None)])
            .await
            .expect("calling point");
        let deleted = finish_schedule_calling_points_full_publish_without_rows(
            &pool,
            SchedulePublishPart {
                publish_id: "test-empty-undated",
                first_chunk: true,
                final_total_rows: Some(0),
            },
            None,
        )
        .await
        .expect("undated empty publish");
        assert_eq!(deleted, 0);
        let deleted = finish_schedule_calling_points_full_publish_without_rows(
            &pool,
            SchedulePublishPart {
                publish_id: "test-empty-dated",
                first_chunk: true,
                final_total_rows: Some(0),
            },
            Some(date),
        )
        .await
        .expect("dated empty publish");
        assert_eq!(deleted, 1);

        clear_dates(&pool, &[date, other]).await;
    }

    /// **Fail closed.** If the final chunk finds fewer (or more) staged keys
    /// than the publisher's `total_rows` -- a chunk went to an `api` that
    /// doesn't stage keys, a chunk was replayed, staging was lost -- it must
    /// NOT delete anything: "rows not in this publish" is not actually known.
    /// Stale rows survive to the next complete publish; no live row is lost.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_publish_diff -- --ignored --test-threads=1`"]
    async fn a_final_chunk_whose_staged_count_does_not_match_deletes_nothing() {
        // SCHED-2: the skipped delete is counted, not only logged.
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let pool = test_pool().await;
        let date = fixture_date(8);
        clear_dates(&pool, &[date]).await;

        upsert_schedule_destination_departures(
            &pool,
            &[
                departure(date, "UNSTAGED", time(8, 0), None),
                departure(date, "STALE", time(9, 0), None),
            ],
        )
        .await
        .expect("previous publish");

        // Two-chunk publish whose first chunk never staged (as if handled by
        // an older api): only the final chunk's one row is staged, but the
        // publisher says the publish had two.
        let publish_id = "test-count-mismatch";
        upsert_schedule_destination_departures_publish_part(
            &pool,
            &[departure(date, "FINAL", time(10, 0), None)],
            SchedulePublishPart {
                publish_id,
                first_chunk: false,
                final_total_rows: Some(2),
            },
        )
        .await
        .expect("final chunk");

        assert_eq!(
            uids(&departure_tuples(&pool, date).await),
            vec!["FINAL", "STALE", "UNSTAGED"],
            "a mismatched count must leave every existing row in place"
        );
        assert_eq!(
            staged_key_count(
                &pool,
                "schedule_destination_departures_publish_keys",
                publish_id
            )
            .await,
            0,
            "staged keys are dropped even when the delete is skipped"
        );
        let rendered = handle.render();
        assert!(
            rendered.contains(
                r#"distant_signal_api_schedule_publish_staged_mismatch_total{product="schedule_destination_departures"} 1"#
            ),
            "{rendered}"
        );

        clear_dates(&pool, &[date]).await;
    }

    /// A newer publish of the same date supersedes an older in-flight one:
    /// the newer publish's first chunk discards the older publish's staged
    /// keys, so the older publish's final chunk fails closed rather than
    /// deleting the newer publish's rows as "missing".
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_publish_diff -- --ignored --test-threads=1`"]
    async fn a_newer_publish_of_the_same_date_supersedes_an_older_in_flight_one() {
        let pool = test_pool().await;
        let date = fixture_date(9);
        clear_dates(&pool, &[date]).await;

        let part = |publish_id, first_chunk, final_total_rows| SchedulePublishPart {
            publish_id,
            first_chunk,
            final_total_rows,
        };
        upsert_schedule_calling_points_full_publish_part(
            &pool,
            &[calling_point(date, "OLD-PUB", 0, None)],
            part("test-older", true, None),
        )
        .await
        .expect("older publish, chunk 1");
        upsert_schedule_calling_points_full_publish_part(
            &pool,
            &[calling_point(date, "NEW-PUB", 0, None)],
            part("test-newer", true, None),
        )
        .await
        .expect("newer publish, chunk 1");
        upsert_schedule_calling_points_full_publish_part(
            &pool,
            &[calling_point(date, "OLD-PUB", 1, None)],
            part("test-older", false, Some(2)),
        )
        .await
        .expect("older publish, final chunk");

        let keys: Vec<(String, i16)> = calling_point_tuples(&pool, date)
            .await
            .into_iter()
            .map(|t| (t.0, t.1))
            .collect();
        assert_eq!(
            keys,
            vec![
                ("NEW-PUB".to_string(), 0),
                ("OLD-PUB".to_string(), 0),
                ("OLD-PUB".to_string(), 1),
            ],
            "the superseded publish must not delete the newer publish's rows"
        );

        upsert_schedule_calling_points_full_publish_part(
            &pool,
            &[calling_point(date, "NEW-PUB", 1, None)],
            part("test-newer", false, Some(2)),
        )
        .await
        .expect("newer publish, final chunk");
        let keys: Vec<(String, i16)> = calling_point_tuples(&pool, date)
            .await
            .into_iter()
            .map(|t| (t.0, t.1))
            .collect();
        assert_eq!(
            keys,
            vec![("NEW-PUB".to_string(), 0), ("NEW-PUB".to_string(), 1)],
            "the newer publish completes normally"
        );

        clear_dates(&pool, &[date]).await;
    }

    /// Same-key rows within one batch collapse to the first of them instead
    /// of failing the whole batch (`ON CONFLICT DO UPDATE` cannot touch one
    /// row twice in one statement) -- the same row the previous
    /// `ON CONFLICT DO NOTHING` insert kept.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_publish_diff -- --ignored --test-threads=1`"]
    async fn duplicate_keys_within_one_batch_keep_the_first_row() {
        let pool = test_pool().await;
        let date = fixture_date(10);
        clear_dates(&pool, &[date]).await;

        upsert_schedule_destination_departures(
            &pool,
            &[
                departure(date, "DUP", time(8, 0), Some("FIRST")),
                departure(date, "DUP", time(8, 0), Some("SECOND")),
            ],
        )
        .await
        .expect("a duplicate key must not fail the batch");
        upsert_schedule_calling_points_full(
            &pool,
            &[
                calling_point(date, "DUP", 0, Some("FIRST")),
                calling_point(date, "DUP", 0, Some("SECOND")),
            ],
        )
        .await
        .expect("a duplicate key must not fail the batch");

        let departures = departure_tuples(&pool, date).await;
        assert_eq!(departures.len(), 1);
        assert_eq!(departures[0].2.as_deref(), Some("FIRST"));
        let calling_points = calling_point_tuples(&pool, date).await;
        assert_eq!(calling_points.len(), 1);
        assert_eq!(calling_points[0].2.as_deref(), Some("FIRST"));

        clear_dates(&pool, &[date]).await;
    }

    /// `(indexdef, indisvalid)` for `index` on `table`, if it exists.
    async fn index_definition(pool: &PgPool, table: &str, index: &str) -> Option<(String, bool)> {
        sqlx::query_as(
            "SELECT pg_get_indexdef(i.indexrelid), i.indisvalid \
             FROM pg_index i \
             JOIN pg_class ic ON ic.oid = i.indexrelid \
             JOIN pg_class tc ON tc.oid = i.indrelid \
             WHERE tc.relname = $1 AND ic.relname = $2",
        )
        .bind(table)
        .bind(index)
        .fetch_optional(pool)
        .await
        .expect("read index definition")
    }

    /// Migrations 20260926220000 / 20260926220100 build the anti-join probe
    /// indexes, valid, with `publish_id` leading and the `delete_missing`
    /// equality columns after it.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_publish_diff -- --ignored --test-threads=1`"]
    async fn the_publish_key_staging_tables_have_valid_probe_indexes() {
        let pool = test_pool().await;
        for (table, index, columns) in [
            (
                "schedule_destination_departures_publish_keys",
                "schedule_destination_departures_publish_keys_probe",
                "(publish_id, service_date, destination_crs, scheduled, train_uid, origin_crs)",
            ),
            (
                "schedule_calling_points_full_publish_keys",
                "schedule_calling_points_full_publish_keys_probe",
                "(publish_id, service_date, uid, seq)",
            ),
        ] {
            let (def, valid) = index_definition(&pool, table, index)
                .await
                .unwrap_or_else(|| panic!("{index} must exist on {table}"));
            assert!(
                valid,
                "{index} must be valid (a failed CONCURRENTLY build is not)"
            );
            assert!(
                def.ends_with(&format!("USING btree {columns}")),
                "{index} has an unexpected definition: {def}"
            );
        }
    }

    /// `EXPLAIN` of `sql.delete_missing` for `publish_id` / `dates`, run in
    /// `tx` so it sees that transaction's `SET LOCAL`s and ANALYZE.
    async fn explain_delete_missing(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        sql: &PublishKeysSql,
        publish_id: &str,
        dates: &[chrono::NaiveDate],
    ) -> String {
        let lines: Vec<String> = sqlx::query_scalar(&format!("EXPLAIN {}", sql.delete_missing))
            .bind(publish_id)
            .bind(dates)
            .fetch_all(&mut **tx)
            .await
            .expect("EXPLAIN delete_missing");
        lines.join("\n")
    }

    /// Checks the production failure mode (2026-09-27: a nested-loop
    /// anti-join that seq-scanned the staging table once per target row)
    /// cannot recur for `sql`, given `publish_id`'s keys staged on `dates`:
    ///
    /// 1. After the `analyze` statement `finish_publish_part` now runs, the
    ///    planner's chosen plan is not a nested loop over a seq-scanned
    ///    staging table.
    /// 2. Even if a nested loop IS chosen (forced here by disabling hash and
    ///    merge joins), its inner side probes the new index rather than
    ///    seq-scanning the staging table.
    async fn assert_delete_missing_plan_is_safe(
        pool: &PgPool,
        sql: &PublishKeysSql,
        keys_table: &str,
        probe_index: &str,
        publish_id: &str,
        dates: &[chrono::NaiveDate],
    ) {
        let seq_scan_of_keys = format!("Seq Scan on {keys_table}");

        let mut tx = pool.begin().await.expect("begin");
        sqlx::query(sql.analyze)
            .execute(&mut *tx)
            .await
            .expect("ANALYZE the staging table inside a transaction");
        let plan = explain_delete_missing(&mut tx, sql, publish_id, dates).await;
        assert!(
            !(plan.contains("Nested Loop") && plan.contains(&seq_scan_of_keys)),
            "after ANALYZE, {} must not be a nested loop over a seq-scanned staging table:\n{plan}",
            sql.product
        );

        sqlx::query("SET LOCAL enable_hashjoin = off")
            .execute(&mut *tx)
            .await
            .expect("disable hash joins");
        sqlx::query("SET LOCAL enable_mergejoin = off")
            .execute(&mut *tx)
            .await
            .expect("disable merge joins");
        let forced = explain_delete_missing(&mut tx, sql, publish_id, dates).await;
        assert!(
            forced.contains("Nested Loop"),
            "sanity: with hash and merge joins disabled the plan is a nested loop:\n{forced}"
        );
        assert!(
            forced.contains(probe_index) && !forced.contains(&seq_scan_of_keys),
            "a nested-loop {} anti-join must probe {probe_index}, not seq-scan the staging \
             table:\n{forced}",
            sql.product
        );
        tx.rollback().await.expect("rollback");
    }

    /// The final chunk's `analyze` really refreshes the staging table's
    /// statistics for whatever role `DATABASE_URL` connects as -- in
    /// particular the non-superuser app role of the role split
    /// (docs/postgres-app-role.md), for which a bare `ANALYZE` only warns
    /// and skips the table. `pg_class.reltuples` (readable by anyone) only
    /// moves to the staged row count if the ANALYZE actually ran.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_publish_diff -- --ignored --test-threads=1`"]
    async fn the_publish_analyze_refreshes_statistics_as_the_connecting_role() {
        const ROWS: i64 = 3_000;
        let pool = test_pool().await;
        for (sql, keys_table) in [
            (
                &DESTINATION_DEPARTURES_PUBLISH_KEYS_SQL,
                "schedule_destination_departures_publish_keys",
            ),
            (
                &CALLING_POINTS_FULL_PUBLISH_KEYS_SQL,
                "schedule_calling_points_full_publish_keys",
            ),
        ] {
            let mut tx = pool.begin().await.expect("begin");
            // Everything below rolls back: these keys never commit.
            sqlx::query(&format!("DELETE FROM {keys_table}"))
                .execute(&mut *tx)
                .await
                .expect("empty the staging table inside the transaction");
            let insert = if keys_table.starts_with("schedule_destination") {
                format!(
                    "INSERT INTO {keys_table} \
                     (publish_id, service_date, destination_crs, scheduled, train_uid, origin_crs) \
                     SELECT 'test-analyze-role', DATE '2050-01-01', 'Z' || (g % 90 + 10)::text, \
                            TIME '08:00', 'T' || g::text, 'Y99' \
                     FROM generate_series(1, $1) g"
                )
            } else {
                format!(
                    "INSERT INTO {keys_table} (publish_id, service_date, uid, seq) \
                     SELECT 'test-analyze-role', DATE '2050-01-01', 'T' || g::text, 1 \
                     FROM generate_series(1, $1) g"
                )
            };
            sqlx::query(&insert)
                .bind(ROWS)
                .execute(&mut *tx)
                .await
                .unwrap_or_else(|err| panic!("stage keys in {keys_table}: {err}"));
            sqlx::query(sql.analyze)
                .execute(&mut *tx)
                .await
                .unwrap_or_else(|err| panic!("{}: {err}", sql.analyze));
            let reltuples: f32 =
                sqlx::query_scalar("SELECT reltuples FROM pg_class WHERE oid = $1::regclass")
                    .bind(keys_table)
                    .fetch_one(&mut *tx)
                    .await
                    .expect("reltuples");
            assert_eq!(
                reltuples as i64, ROWS,
                "{}: the staging table's statistics were not refreshed (the ANALYZE was \
                 skipped for lack of ownership?)",
                sql.analyze
            );
            tx.rollback().await.expect("rollback");
        }

        // Only the two staging tables, never anything a caller names.
        let err = sqlx::query("SELECT analyze_publish_keys('users')")
            .execute(&pool)
            .await
            .expect_err("analyze_publish_keys must refuse any other table");
        assert_eq!(
            err.as_database_error()
                .and_then(sqlx::error::DatabaseError::code)
                .as_deref(),
            Some("22023"),
            "{err}"
        );
    }

    /// Regression test for the 2026-09-27 production CPU burn: stage a
    /// realistic-shaped publish (with the staging table's statistics left
    /// describing an OLDER publish id, as in production), then check the
    /// final chunk's `delete_missing` plan for both products, and that the
    /// final chunk itself -- which now ANALYZEs before deleting -- still
    /// deletes exactly the rows the publish did not carry.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_publish_diff -- --ignored --test-threads=1`"]
    async fn the_final_chunk_delete_never_nested_loops_over_a_seq_scanned_staging_table() {
        const ROWS: usize = 2_000;
        let pool = test_pool().await;
        let date = fixture_date(13);
        let dates = [date];
        clear_dates(&pool, &dates).await;

        let departures: Vec<ScheduleDestinationDeparturesRow> = (0..ROWS)
            .map(|i| departure(date, &format!("PLAN-{i:05}"), time(8, 0), None))
            .collect();
        let calling_points: Vec<ScheduleCallingPointsFullRow> = (0..ROWS)
            .map(|i| calling_point(date, &format!("PLAN-{:05}", i / 10), (i % 10) as i16, None))
            .collect();

        // An older, complete publish: its rows are in the target tables, and
        // the staging tables' statistics are left describing ITS publish id
        // (then its keys are dropped), exactly the stale state production
        // was in when the new publish's final chunk planned its delete.
        upsert_schedule_destination_departures_publish_part(
            &pool,
            &departures,
            SchedulePublishPart {
                publish_id: "test-plan-old",
                first_chunk: true,
                final_total_rows: None,
            },
        )
        .await
        .expect("old publish, departures");
        upsert_schedule_calling_points_full_publish_part(
            &pool,
            &calling_points,
            SchedulePublishPart {
                publish_id: "test-plan-old",
                first_chunk: true,
                final_total_rows: None,
            },
        )
        .await
        .expect("old publish, calling points");
        for sql in [
            &DESTINATION_DEPARTURES_PUBLISH_KEYS_SQL,
            &CALLING_POINTS_FULL_PUBLISH_KEYS_SQL,
        ] {
            sqlx::query(sql.analyze)
                .execute(&pool)
                .await
                .expect("analyze old staging");
            sqlx::query(sql.drop_publish)
                .bind("test-plan-old")
                .execute(&pool)
                .await
                .expect("drop old staging");
        }

        // The new publish carries every row but the last; stage it without
        // finalizing, then check the plan the final chunk would get.
        let publish_id = "test-plan-new";
        let new_part = SchedulePublishPart {
            publish_id,
            first_chunk: true,
            final_total_rows: None,
        };
        upsert_schedule_destination_departures_publish_part(
            &pool,
            &departures[..ROWS - 1],
            new_part,
        )
        .await
        .expect("new publish, departures");
        upsert_schedule_calling_points_full_publish_part(
            &pool,
            &calling_points[..ROWS - 1],
            new_part,
        )
        .await
        .expect("new publish, calling points");

        assert_delete_missing_plan_is_safe(
            &pool,
            &DESTINATION_DEPARTURES_PUBLISH_KEYS_SQL,
            "schedule_destination_departures_publish_keys",
            "schedule_destination_departures_publish_keys_probe",
            publish_id,
            &dates,
        )
        .await;
        assert_delete_missing_plan_is_safe(
            &pool,
            &CALLING_POINTS_FULL_PUBLISH_KEYS_SQL,
            "schedule_calling_points_full_publish_keys",
            "schedule_calling_points_full_publish_keys_probe",
            publish_id,
            &dates,
        )
        .await;

        // Finalize through the real path (which ANALYZEs in its own
        // transaction): the last row is re-sent by the final chunk, so every
        // row survives and nothing is deleted, then a publish without it
        // deletes exactly it.
        let final_part = SchedulePublishPart {
            publish_id,
            first_chunk: false,
            final_total_rows: Some(ROWS as u64),
        };
        upsert_schedule_destination_departures_publish_part(
            &pool,
            &departures[ROWS - 1..],
            final_part,
        )
        .await
        .expect("new publish, departures final chunk");
        upsert_schedule_calling_points_full_publish_part(
            &pool,
            &calling_points[ROWS - 1..],
            final_part,
        )
        .await
        .expect("new publish, calling points final chunk");
        assert_eq!(departure_tuples(&pool, date).await.len(), ROWS);
        assert_eq!(calling_point_tuples(&pool, date).await.len(), ROWS);

        upsert_schedule_destination_departures(&pool, &departures[..ROWS - 1])
            .await
            .expect("publish without the last departure");
        upsert_schedule_calling_points_full(&pool, &calling_points[..ROWS - 1])
            .await
            .expect("publish without the last calling point");
        assert_eq!(departure_tuples(&pool, date).await.len(), ROWS - 1);
        assert_eq!(calling_point_tuples(&pool, date).await.len(), ROWS - 1);
        for table in [
            "schedule_destination_departures_publish_keys",
            "schedule_calling_points_full_publish_keys",
        ] {
            assert_eq!(staged_key_count(&pool, table, publish_id).await, 0);
        }

        clear_dates(&pool, &dates).await;
    }

    /// Stages `rows`' keys under `publish_id` directly, as the earlier chunks
    /// of a publish would have, inside `tx`.
    async fn stage_departure_keys(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        publish_id: &str,
        rows: &[ScheduleDestinationDeparturesRow],
    ) {
        for row in rows {
            sqlx::query(
                "INSERT INTO schedule_destination_departures_publish_keys \
                    (publish_id, service_date, destination_crs, scheduled, train_uid, origin_crs) \
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(publish_id)
            .bind(row.service_date)
            .bind(&row.destination_crs)
            .bind(row.scheduled)
            .bind(&row.train_uid)
            .bind(&row.origin_crs)
            .execute(&mut **tx)
            .await
            .expect("stage key");
        }
    }

    /// **2026-09-27 incident regression.** While one final chunk of a
    /// product is in its delete phase (holding the product's advisory lock,
    /// transaction not yet committed), a second final chunk of the same
    /// product is refused at once with `SchedulePublishBusy` and rolls back
    /// entirely -- it neither runs its own delete concurrently nor queues to
    /// run it later. Once the first commits, the second goes through.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_publish_diff -- --ignored --test-threads=1`"]
    async fn a_second_final_chunk_is_refused_while_the_first_is_deleting() {
        let pool = test_pool().await;
        let (date_a, date_b) = (fixture_date(20), fixture_date(21));
        let dates = [date_a, date_b];
        clear_dates(&pool, &dates).await;
        let sql = &DESTINATION_DEPARTURES_PUBLISH_KEYS_SQL;

        let rows_a = vec![departure(date_a, "A00001", time(8, 0), None)];
        let rows_b = vec![departure(date_b, "B00001", time(9, 0), None)];

        // Publish A's final chunk, stopped between its delete and COMMIT.
        let mut tx_a = pool.begin().await.expect("begin A");
        stage_departure_keys(&mut tx_a, "lock-test-a", &rows_a).await;
        finish_publish_part(
            &mut tx_a,
            sql,
            SchedulePublishPart {
                publish_id: "lock-test-a",
                first_chunk: true,
                final_total_rows: Some(1),
            },
            PUBLISH_DELETE_STATEMENT_TIMEOUT,
        )
        .await
        .expect("A's final chunk takes the lock and deletes");

        let part_b = SchedulePublishPart {
            publish_id: "lock-test-b",
            first_chunk: true,
            final_total_rows: Some(1),
        };
        let started = std::time::Instant::now();
        let err = upsert_schedule_destination_departures_publish_part(&pool, &rows_b, part_b)
            .await
            .expect_err("B's final chunk must be refused while A holds the lock");
        assert!(
            err.downcast_ref::<SchedulePublishBusy>().is_some(),
            "expected SchedulePublishBusy, got {err:?}"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "B must fail fast, not queue behind A"
        );
        // B rolled back whole: no upserted rows, no staged keys left behind.
        assert!(departure_tuples(&pool, date_b).await.is_empty());
        assert_eq!(
            staged_key_count(
                &pool,
                "schedule_destination_departures_publish_keys",
                "lock-test-b"
            )
            .await,
            0
        );

        tx_a.commit().await.expect("commit A");

        upsert_schedule_destination_departures_publish_part(&pool, &rows_b, part_b)
            .await
            .expect("B goes through once A has committed");
        assert_eq!(uids(&departure_tuples(&pool, date_b).await), ["B00001"]);

        // A different product's lock is independent.
        let mut tx_c = pool.begin().await.expect("begin C");
        let other: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
            .bind(DESTINATION_DEPARTURES_PUBLISH_KEYS_SQL.final_lock_key)
            .fetch_one(&mut *tx_c)
            .await
            .expect("take departures lock");
        assert!(other);
        upsert_schedule_calling_points_full_publish_part(
            &pool,
            &[calling_point(date_b, "B00001", 0, None)],
            SchedulePublishPart {
                publish_id: "lock-test-c",
                first_chunk: true,
                final_total_rows: Some(1),
            },
        )
        .await
        .expect("calling points are not blocked by the departures lock");
        tx_c.rollback().await.expect("rollback C");

        clear_dates(&pool, &dates).await;
    }

    /// A final chunk whose delete phase exceeds its statement timeout is
    /// cancelled with SQLSTATE 57014 (`is_statement_timeout`, which `api`
    /// maps to 503) and, once its transaction rolls back, leaves the target
    /// rows exactly as they were and the advisory lock free.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_publish_diff -- --ignored --test-threads=1`"]
    async fn a_timed_out_final_chunk_rolls_back_and_deletes_nothing() {
        let pool = test_pool().await;
        let date = fixture_date(22);
        clear_dates(&pool, &[date]).await;
        let sql = &DESTINATION_DEPARTURES_PUBLISH_KEYS_SQL;

        let rows = vec![
            departure(date, "K00001", time(8, 0), None),
            departure(date, "K00002", time(9, 0), None),
        ];
        upsert_schedule_destination_departures(&pool, &rows)
            .await
            .expect("seed the date");
        let before = departure_tuples(&pool, date).await;
        assert_eq!(before.len(), 2);

        // Another session holds K00002's row lock, so the delete that would
        // remove it (the publish below omits it) blocks until cancelled.
        let mut blocker = pool.begin().await.expect("begin blocker");
        sqlx::query(
            "SELECT 1 FROM schedule_destination_departures \
             WHERE service_date = $1 AND train_uid = 'K00002' FOR UPDATE",
        )
        .bind(date)
        .execute(&mut *blocker)
        .await
        .expect("lock K00002");

        let mut tx = pool.begin().await.expect("begin publish");
        stage_departure_keys(&mut tx, "timeout-test", &rows[..1]).await;
        let started = std::time::Instant::now();
        let err = finish_publish_part(
            &mut tx,
            sql,
            SchedulePublishPart {
                publish_id: "timeout-test",
                first_chunk: true,
                final_total_rows: Some(1),
            },
            std::time::Duration::from_millis(500),
        )
        .await
        .expect_err("the blocked delete must be cancelled by the statement timeout");
        assert!(is_statement_timeout(&err), "expected 57014, got {err:?}");
        assert!(started.elapsed() < std::time::Duration::from_secs(30));
        tx.rollback().await.expect("rollback publish");
        blocker.rollback().await.expect("rollback blocker");

        // Untouched: same rows, same row versions, nothing staged.
        assert_eq!(departure_tuples(&pool, date).await, before);
        assert_eq!(
            staged_key_count(
                &pool,
                "schedule_destination_departures_publish_keys",
                "timeout-test"
            )
            .await,
            0
        );

        // The lock went with the rolled-back transaction, and SET LOCAL did
        // not leak onto the pooled connection: a normal publish succeeds.
        upsert_schedule_destination_departures(&pool, &rows[..1])
            .await
            .expect("a later publish goes through");
        assert_eq!(uids(&departure_tuples(&pool, date).await), ["K00001"]);
        let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
            .fetch_one(&pool)
            .await
            .expect("show statement_timeout");
        assert_eq!(timeout, "0");

        clear_dates(&pool, &[date]).await;
    }
}

#[cfg(test)]
mod schedule_destination_departures_row_serde_tests {
    use super::ScheduleDestinationDeparturesRow;

    fn base() -> serde_json::Value {
        serde_json::json!({
            "service_date": "2026-09-26",
            "destination_crs": "EDB",
            "scheduled": "09:00:00",
            "train_uid": "C00573",
            "origin_crs": "KGX",
        })
    }

    #[test]
    fn a_payload_from_a_publisher_predating_headcode_deserializes_as_none() {
        let row: ScheduleDestinationDeparturesRow = serde_json::from_value(base()).unwrap();
        assert_eq!(row.headcode, None);
    }

    #[test]
    fn a_published_headcode_and_an_explicit_null_both_deserialize() {
        let mut with = base();
        with["headcode"] = serde_json::json!("1S00");
        let row: ScheduleDestinationDeparturesRow = serde_json::from_value(with).unwrap();
        assert_eq!(row.headcode.as_deref(), Some("1S00"));

        let mut null = base();
        null["headcode"] = serde_json::Value::Null;
        let row: ScheduleDestinationDeparturesRow = serde_json::from_value(null).unwrap();
        assert_eq!(row.headcode, None);
    }

    #[test]
    fn rsid_is_optional_on_the_wire_and_accepts_a_value_or_null() {
        let row: ScheduleDestinationDeparturesRow = serde_json::from_value(base()).unwrap();
        assert_eq!(row.rsid, None);

        let mut with = base();
        with["rsid"] = serde_json::json!("SR408800");
        let row: ScheduleDestinationDeparturesRow = serde_json::from_value(with).unwrap();
        assert_eq!(row.rsid.as_deref(), Some("SR408800"));

        let mut null = base();
        null["rsid"] = serde_json::Value::Null;
        let row: ScheduleDestinationDeparturesRow = serde_json::from_value(null).unwrap();
        assert_eq!(row.rsid, None);
    }
}

/// `SchedulePublishPart::new` and `empty_publish_date`: the chunk-parameter
/// validation the api's `ScheduleChunkParams` delegates to.
#[cfg(test)]
mod schedule_publish_part_tests {
    use super::{MAX_PUBLISH_ID_LEN, SchedulePublishPart};

    /// F-LEGACY: a chunk without `publish_id` is refused.
    #[test]
    fn no_publish_id_is_refused() {
        for first_chunk in [false, true] {
            assert!(SchedulePublishPart::new(None, first_chunk, false, None).is_err());
        }
    }

    #[test]
    fn the_publish_id_length_is_bounded() {
        let long = "x".repeat(MAX_PUBLISH_ID_LEN + 1);
        assert!(SchedulePublishPart::new(Some(""), true, false, None).is_err());
        assert!(SchedulePublishPart::new(Some(&long), true, false, None).is_err());
        let max = "x".repeat(MAX_PUBLISH_ID_LEN);
        assert!(SchedulePublishPart::new(Some(&max), true, false, None).is_ok());
    }

    #[test]
    fn only_the_last_chunk_finalizes_and_it_needs_its_total() {
        let Ok(middle) = SchedulePublishPart::new(Some("p1"), false, false, Some(7)) else {
            panic!("a middle chunk is valid");
        };
        assert_eq!(
            (
                middle.publish_id,
                middle.first_chunk,
                middle.final_total_rows
            ),
            ("p1", false, None)
        );
        let Ok(last) = SchedulePublishPart::new(Some("p1"), false, true, Some(7)) else {
            panic!("a final chunk with its total is valid");
        };
        assert_eq!(last.final_total_rows, Some(7));
        assert!(SchedulePublishPart::new(Some("p1"), false, true, None).is_err());
    }

    /// PL-14: an empty final chunk with `total_rows=0` must name its date.
    #[test]
    fn an_empty_final_publish_needs_its_service_date() {
        let date = chrono::NaiveDate::from_ymd_opt(2026, 12, 25);
        assert!(SchedulePublishPart::empty_publish_date(true, Some(0), None).is_err());
        assert_eq!(
            SchedulePublishPart::empty_publish_date(true, Some(0), date),
            Ok(date)
        );
        assert_eq!(
            SchedulePublishPart::empty_publish_date(false, None, None),
            Ok(None),
            "a non-final empty chunk is a no-op and needs no date"
        );
    }
}

/// `upsert_schedule_destination_departures`'s whole-day replace, moved
/// from the api's `schedule_destination_departures_query_tests` (whose
/// search tests stay there, on the same shared fixtures). Each test owns
/// a distinct `service_date` in January 2099.
#[cfg(test)]
mod schedule_destination_departures_upsert_tests {
    use super::*;
    use crate::test_support::connect as test_pool;
    use crate::test_support::destination_departures::{delete_day, fixture_date, row, seed, time};

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_destination_departures -- --ignored --test-threads=1`"]
    async fn upsert_wholesale_replaces_the_whole_service_date() {
        // The flat-shape successor to the bucket table's
        // "wholesale-replaces an existing row for the same key" test. The
        // unit of replacement is now the DAY, not one (destination_crs,
        // service_date) key -- a fresh delivery's grouping supersedes the
        // prior one entirely, including destinations that vanished from it.
        let pool = test_pool().await;
        let date = fixture_date(1);
        delete_day(&pool, date).await;

        let first = vec![
            row(date, "ZRB", time(8, 0), "OLD1", "EUS", None, None),
            row(date, "ZRC", time(9, 0), "OLD2", "CRE", None, None),
        ];
        let inserted = upsert_schedule_destination_departures(&pool, &first)
            .await
            .expect("first upsert");
        assert_eq!(inserted, 2);

        // The second publish drops ZRC entirely and changes ZRB's row.
        let second = vec![row(date, "ZRB", time(9, 30), "NEW1", "CRE", None, None)];
        upsert_schedule_destination_departures(&pool, &second)
            .await
            .expect("second upsert");

        let stored: Vec<(String, chrono::NaiveTime, String)> = sqlx::query_as(
            "SELECT destination_crs, scheduled, train_uid \
             FROM schedule_destination_departures WHERE service_date = $1 \
             ORDER BY destination_crs",
        )
        .bind(date)
        .fetch_all(&pool)
        .await
        .expect("read back");

        assert_eq!(
            stored.len(),
            1,
            "a fresh publish wholesale-replaces the whole service_date, never merges into it"
        );
        assert_eq!(stored[0].0, "ZRB");
        assert_eq!(stored[0].1, time(9, 30));
        assert_eq!(stored[0].2, "NEW1");

        delete_day(&pool, date).await;
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_destination_departures -- --ignored --test-threads=1`"]
    async fn upsert_with_an_empty_batch_does_not_wipe_the_day() {
        // Guards the one way a DELETE-then-INSERT upsert can destroy real
        // data that a per-row ON CONFLICT loop never could: a publish that
        // produced no rows (a parse failure upstream, an empty grouping)
        // must be a no-op, NOT "delete today's timetable".
        let pool = test_pool().await;
        let date = fixture_date(2);
        seed(&pool, date).await;

        let affected = upsert_schedule_destination_departures(&pool, &[])
            .await
            .expect("empty upsert");
        assert_eq!(affected, 0);

        let (remaining,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM schedule_destination_departures WHERE service_date = $1",
        )
        .bind(date)
        .fetch_one(&pool)
        .await
        .expect("count");
        assert_eq!(
            remaining, 3,
            "an empty batch must leave the day untouched, never clear it"
        );

        delete_day(&pool, date).await;
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_destination_departures -- --ignored --test-threads=1`"]
    async fn upsert_round_trips_true_origin_crs_including_a_null_value() {
        let pool = test_pool().await;
        let date = fixture_date(20);
        delete_day(&pool, date).await;

        upsert_schedule_destination_departures(
            &pool,
            &[
                row(date, "ZRD", time(8, 0), "C30001", "EUS", Some("PAD"), None),
                row(date, "ZRD", time(9, 0), "C30002", "CRE", None, None),
            ],
        )
        .await
        .expect("seed rows");

        let stored: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT train_uid, true_origin_crs FROM schedule_destination_departures \
             WHERE service_date = $1 ORDER BY train_uid",
        )
        .bind(date)
        .fetch_all(&pool)
        .await
        .expect("read back");

        assert_eq!(stored.len(), 2);
        assert_eq!(stored[0], ("C30001".to_string(), Some("PAD".to_string())));
        assert_eq!(
            stored[1],
            ("C30002".to_string(), None),
            "an absent true_origin_crs must round-trip as SQL NULL, not an empty string"
        );

        delete_day(&pool, date).await;
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_destination_departures -- --ignored --test-threads=1`"]
    async fn upsert_round_trips_destination_arrival_including_a_null_value() {
        let pool = test_pool().await;
        let date = fixture_date(31);
        delete_day(&pool, date).await;

        upsert_schedule_destination_departures(
            &pool,
            &[
                row(
                    date,
                    "ZRD",
                    time(8, 0),
                    "C70001",
                    "EUS",
                    Some("EUS"),
                    Some(time(11, 30)),
                ),
                row(date, "ZRD", time(9, 0), "C70002", "CRE", None, None),
            ],
        )
        .await
        .expect("seed rows");

        let stored: Vec<(String, Option<chrono::NaiveTime>)> = sqlx::query_as(
            "SELECT train_uid, destination_arrival FROM schedule_destination_departures \
             WHERE service_date = $1 ORDER BY train_uid",
        )
        .bind(date)
        .fetch_all(&pool)
        .await
        .expect("read back");

        assert_eq!(stored.len(), 2);
        assert_eq!(stored[0], ("C70001".to_string(), Some(time(11, 30))));
        assert_eq!(
            stored[1],
            ("C70002".to_string(), None),
            "an absent destination_arrival must round-trip as SQL NULL, not a fabricated time"
        );

        delete_day(&pool, date).await;
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_destination_departures -- --ignored --test-threads=1`"]
    async fn upsert_round_trips_operator_atoc_including_a_null_value() {
        let pool = test_pool().await;
        let date = fixture_date(25);
        delete_day(&pool, date).await;

        upsert_schedule_destination_departures(
            &pool,
            &[
                ScheduleDestinationDeparturesRow {
                    service_date: date,
                    destination_crs: "ZRD".to_string(),
                    scheduled: time(8, 0),
                    day_offset: 0,
                    train_uid: "C80001".to_string(),
                    origin_crs: "EUS".to_string(),
                    true_origin_crs: Some("EUS".to_string()),
                    calling_point_arrival: None,
                    destination_arrival: None,
                    destination_arrival_day_offset: 0,
                    operator_atoc: Some("SR".to_string()),
                    headcode: Some("1S00".to_string()),
                    rsid: Some("SR408800".to_string()),
                    ..Default::default()
                },
                ScheduleDestinationDeparturesRow {
                    service_date: date,
                    destination_crs: "ZRD".to_string(),
                    scheduled: time(9, 0),
                    day_offset: 0,
                    train_uid: "C80002".to_string(),
                    origin_crs: "CRE".to_string(),
                    true_origin_crs: None,
                    calling_point_arrival: None,
                    destination_arrival: None,
                    destination_arrival_day_offset: 0,
                    operator_atoc: None,
                    headcode: None,
                    rsid: None,
                    ..Default::default()
                },
            ],
        )
        .await
        .expect("seed rows");

        let stored: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT train_uid, operator_atoc FROM schedule_destination_departures \
             WHERE service_date = $1 ORDER BY train_uid",
        )
        .bind(date)
        .fetch_all(&pool)
        .await
        .expect("read back");

        assert_eq!(stored.len(), 2);
        assert_eq!(stored[0], ("C80001".to_string(), Some("SR".to_string())));
        assert_eq!(
            stored[1],
            ("C80002".to_string(), None),
            "an absent operator_atoc must round-trip as SQL NULL, not an empty string"
        );

        let headcodes: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT train_uid, headcode FROM schedule_destination_departures \
             WHERE service_date = $1 ORDER BY train_uid",
        )
        .bind(date)
        .fetch_all(&pool)
        .await
        .expect("read back headcodes");
        assert_eq!(
            headcodes,
            vec![
                ("C80001".to_string(), Some("1S00".to_string())),
                ("C80002".to_string(), None),
            ],
            "a blank CIF Train Identity must round-trip as SQL NULL"
        );

        let rsids: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT train_uid, rsid FROM schedule_destination_departures \
             WHERE service_date = $1 ORDER BY train_uid",
        )
        .bind(date)
        .fetch_all(&pool)
        .await
        .expect("read back rsids");
        assert_eq!(
            rsids,
            vec![
                ("C80001".to_string(), Some("SR408800".to_string())),
                ("C80002".to_string(), None),
            ],
            "a blank CIF Retail Service ID must round-trip as SQL NULL"
        );

        delete_day(&pool, date).await;
    }
}

/// The staged-mismatch counter's two names (plan 2a.5): the api keeps its
/// `api_` name; a direct writer counts the `store_` one.
#[cfg(test)]
mod staged_mismatch_metric_name_tests {
    use super::{
        SCHEDULE_PUBLISH_STAGED_MISMATCH_METRIC, STORE_SCHEDULE_PUBLISH_STAGED_MISMATCH_METRIC,
        staged_mismatch_metric_for,
    };

    #[test]
    fn the_api_name_is_the_default_and_the_store_name_is_opt_in() {
        assert_eq!(
            staged_mismatch_metric_for(false),
            SCHEDULE_PUBLISH_STAGED_MISMATCH_METRIC
        );
        assert_eq!(
            staged_mismatch_metric_for(true),
            STORE_SCHEDULE_PUBLISH_STAGED_MISMATCH_METRIC
        );
        assert_eq!(
            STORE_SCHEDULE_PUBLISH_STAGED_MISMATCH_METRIC.strip_prefix("store_"),
            SCHEDULE_PUBLISH_STAGED_MISMATCH_METRIC.strip_prefix("api_"),
        );
    }
}

#[cfg(test)]
mod publish_rows_metric_tests {
    use super::{
        SCHEDULE_PUBLISH_ROWS_METRIC, STORE_SCHEDULE_PUBLISH_ROWS_METRIC, count_publish_rows,
        register_schedule_publish_metrics,
    };
    use metrics_exporter_prometheus::PrometheusBuilder;

    #[test]
    fn rows_are_counted_as_written_or_unchanged_and_registered_at_zero() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            register_schedule_publish_metrics();
            count_publish_rows("schedule_calling_points_full", 50_000, 12);
        });
        let rendered = handle.render();
        for line in [
            r#"distant_signal_api_schedule_publish_rows_total{product="schedule_calling_points_full",outcome="written"} 12"#,
            r#"distant_signal_api_schedule_publish_rows_total{product="schedule_calling_points_full",outcome="unchanged"} 49988"#,
            r#"distant_signal_api_schedule_publish_rows_total{product="schedule_destination_departures",outcome="unchanged"} 0"#,
            r#"distant_signal_api_schedule_publish_rows_total{product="schedule_services",outcome="written"} 0"#,
        ] {
            assert!(rendered.contains(line), "{line}\n{rendered}");
        }
        assert_eq!(
            STORE_SCHEDULE_PUBLISH_ROWS_METRIC.strip_prefix("store_"),
            SCHEDULE_PUBLISH_ROWS_METRIC.strip_prefix("api_"),
        );
    }
}
