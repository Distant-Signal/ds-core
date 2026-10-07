//! The schedule pipeline's delivery markers: `schedule-ingest`'s record of
//! each extracted feed delivery (`schedule_feed_ingests`) and
//! `schedule-reference`'s record of each fully published delivery
//! (`schedule_reference_publishes`), plus the feed-ingest record's body
//! and its validation, [`schedule_feed_ingest_problem`].
//!
//! Moved unchanged (ingest architecture plan 1A.7) from the api's
//! `data::queries` and, for the request body and its validation,
//! `routes::ingest`; the api re-exports them from there.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::corpus::is_sha256_hex;

/// Timestamp of the most recently *delivered* schedule feed (i.e.
/// `MAX(delivered_at)`, the delivery zip's own mtime -- not
/// `MAX(ingested_at)`, when this table happened to be written to), or
/// `None` if `schedule_feed_ingests` has never been populated. Backs both
/// `GET /private/schedule-feed-ingests` (the `schedule-ingest` crate's
/// startup check) and the public `/public/freshness` endpoint's
/// `schedule_feed` field -- using `delivered_at` here is what makes that
/// freshness signal mean "when did a real feed delivery last land", not
/// "when did `schedule-ingest` last happen to run a cycle that processed
/// one" (see
/// docs/superpowers/specs/2026-09-03-schedule-feed-zip-delivery-correction.md).
pub async fn last_schedule_feed_fetch(
    pool: &PgPool,
) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    let (delivered_at,): (Option<chrono::DateTime<chrono::Utc>>,) =
        sqlx::query_as("SELECT MAX(delivered_at) FROM schedule_feed_ingests")
            .fetch_one(pool)
            .await?;
    Ok(delivered_at)
}

/// Records one verified schedule-feed delivery, keyed on `delivered_at` --
/// the delivery zip's own mtime, the one stable identifier a plain-overwrite
/// delivery has (there is no sequence number -- see this table's own
/// migration). `ON CONFLICT (delivered_at) DO NOTHING`, not an upsert -- a
/// re-POST of an already-recorded delivery (e.g. after `schedule-ingest`
/// restarts and re-observes a delivery it already recorded, since it keeps
/// no persistent state of its own) is a harmless no-op, not an error,
/// matching this route's own idempotency needs -- `schedule-ingest` itself
/// doesn't track "have I already `POSTed` this" locally (state lives here).
pub async fn insert_schedule_feed_ingest(
    pool: &PgPool,
    delivered_at: chrono::DateTime<chrono::Utc>,
    ingested_at: chrono::DateTime<chrono::Utc>,
    files: &serde_json::Value,
    source: &ScheduleFeedSource<'_>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO schedule_feed_ingests \
             (delivered_at, ingested_at, files, source_file, source_bytes, source_sha256) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (delivered_at) DO NOTHING",
    )
    .bind(delivered_at)
    .bind(ingested_at)
    .bind(files)
    .bind(source.file)
    .bind(source.bytes)
    .bind(source.sha256)
    .execute(pool)
    .await?;
    Ok(())
}

/// The delivered zip behind a `schedule_feed_ingests` row: its name, size
/// and SHA-256 as `schedule-ingest` read it (migration
/// `20261001150000_schedule_feed_delivery_sha256.sql`). Every field is
/// `None` for a record from a `schedule-ingest` that predates it.
#[derive(Debug, Clone, Copy, Default)]
pub struct ScheduleFeedSource<'a> {
    pub file: Option<&'a str>,
    pub bytes: Option<i64>,
    pub sha256: Option<&'a str>,
}

/// The delivery directory name (`YYYYMMDDTHHMMSSZ`) whose `schedule-reference`
/// publish cycle most recently COMPLETED -- every product for that delivery
/// published successfully -- or `None` if that producer has never completed
/// a full cycle (a fresh deployment).
///
/// Deliberately NOT [`last_schedule_feed_fetch`] above, and the difference is
/// the whole point of this table existing: that one reports when a delivery
/// was EXTRACTED by `schedule-ingest`, which says nothing about whether
/// `schedule-reference` ever processed it. Backs `GET
/// /private/schedule-reference-publishes`, which
/// `schedule-reference::main::seed_last_processed_delivery` reads once at
/// startup. See `20260925130000_schedule_reference_publishes.sql` for the
/// production failure mode this replaced.
///
/// `ORDER BY completed_at DESC`, not `MAX(delivery)`: the delivery name
/// happens to sort chronologically today, but ordering on the column that
/// actually means "when did this finish" cannot be broken by a future change
/// to the directory-name format.
pub async fn last_completed_schedule_reference_publish(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT delivery FROM schedule_reference_publishes ORDER BY completed_at DESC LIMIT 1",
    )
    .fetch_optional(executor)
    .await?;
    Ok(row.map(|(delivery,)| delivery))
}

/// Records that `schedule-reference` has completed a FULL successful publish
/// cycle for `delivery` -- every product derived from that delivery landed.
///
/// `ON CONFLICT (delivery) DO UPDATE SET completed_at = now()`, not `DO
/// NOTHING`: a re-POST for the same delivery means that delivery's whole
/// cycle really did run to completion again (the marker was lost, or a
/// previous cycle left it unset because a product had failed and the retry
/// has now succeeded), and `completed_at` should reflect the latest such
/// completion so [`last_completed_schedule_reference_publish`]'s ordering
/// stays honest.
pub async fn insert_schedule_reference_publish(pool: &PgPool, delivery: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO schedule_reference_publishes (delivery, completed_at) VALUES ($1, NOW()) \
         ON CONFLICT (delivery) DO UPDATE SET completed_at = NOW()",
    )
    .bind(delivery)
    .execute(pool)
    .await?;
    Ok(())
}

/// `schedule-ingest`'s per-delivery record of one successfully-verified CIF
/// SCHEDULE feed delivery. Unlike the other ingest routes this isn't a
/// per-poll-cycle batch of reference data -- it's one row per delivery,
/// recorded once a stable `.zip` delivery has been extracted (see
/// `crates/schedule-ingest`).
///
/// `delivered_at` is the delivery zip's own mtime -- the real identity of
/// "which delivery is this" now that there is no sequence number (see
/// `docs/superpowers/specs/2026-09-03-schedule-feed-zip-delivery-correction.md`).
/// `ingested_at` is when this process actually happened to be processed,
/// kept only as separate observability data.
///
/// `source_*` describe the delivered zip itself (its name, size and
/// SHA-256 as `schedule-ingest` read it); absent from an older
/// `schedule-ingest`, so all optional.
#[derive(Debug, Deserialize)]
pub struct ScheduleFeedIngestRequest {
    pub delivered_at: chrono::DateTime<chrono::Utc>,
    pub ingested_at: chrono::DateTime<chrono::Utc>,
    pub files: Vec<ScheduleFeedFile>,
    #[serde(default)]
    pub source_file: Option<String>,
    #[serde(default)]
    pub source_bytes: Option<u64>,
    #[serde(default)]
    pub source_sha256: Option<String>,
}

/// One file observed as part of a schedule-feed delivery. `bytes` is the
/// size `schedule-ingest` itself observed on disk once stable, not a
/// manifest-declared size -- the real manifest format has no such field.
/// `sha256` is the extracted file's hash, when `schedule-ingest` extracted
/// it itself (a delivery re-posted from its completion marker carries
/// none).
#[derive(Debug, Deserialize, Serialize)]
pub struct ScheduleFeedFile {
    pub name: String,
    pub bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

/// A 422 for any malformed provenance field in a schedule-feed record.
pub fn schedule_feed_ingest_problem(req: &ScheduleFeedIngestRequest) -> Option<String> {
    if let Some(sha) = &req.source_sha256
        && !is_sha256_hex(sha)
    {
        return Some("source_sha256 must be 64 lowercase hex digits".to_string());
    }
    if req
        .source_bytes
        .is_some_and(|bytes| i64::try_from(bytes).is_err())
    {
        return Some("source_bytes is out of range".to_string());
    }
    if req
        .source_file
        .as_deref()
        .is_some_and(|f| f.trim().is_empty())
    {
        return Some("source_file must not be blank".to_string());
    }
    req.files
        .iter()
        .find(|f| f.sha256.as_deref().is_some_and(|s| !is_sha256_hex(s)))
        .map(|f| {
            format!(
                "files[].sha256 of {:?} must be 64 lowercase hex digits",
                f.name
            )
        })
}

// Tested at the query level rather than through a full route/router
// harness: `routes/ingest.rs` has no existing route-level `db_tests`
// precedent to mirror (unlike, say, a hypothetical prior ingest route with
// its own axum test setup), and exercising `insert_schedule_feed_ingest`/
// `last_schedule_feed_fetch` directly against a live database already
// covers the real SQL and `ON CONFLICT DO NOTHING` idempotency behavior
// that matters here -- the route handlers themselves are thin
// serialize/deserialize wrappers around these two functions.
#[cfg(test)]
mod schedule_feed_ingest_query_tests {
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

    /// The delivered zip's name, size and SHA-256 land in their columns,
    /// and the per-file hashes stay inside `files`; a malformed hash is
    /// refused by the column's CHECK.
    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p ds-store \
                schedule_feed_insert_records_the_delivered_zip_provenance \
                -- --ignored --test-threads=1`"]
    async fn schedule_feed_insert_records_the_delivered_zip_provenance() {
        use chrono::SubsecRound;

        let pool = test_pool().await;
        let delivered_at = (chrono::Utc::now() - chrono::Duration::days(400)).trunc_subsecs(0);
        let sha = "ab".repeat(32);
        let files =
            serde_json::json!([{"name": "RJTTF975MCA.txt", "bytes": 3, "sha256": "cd".repeat(32)}]);
        insert_schedule_feed_ingest(
            &pool,
            delivered_at,
            delivered_at,
            &files,
            &ScheduleFeedSource {
                file: Some("timetable_full.zip"),
                bytes: Some(77_222_226),
                sha256: Some(&sha),
            },
        )
        .await
        .expect("insert with provenance");
        let row: (
            Option<String>,
            Option<i64>,
            Option<String>,
            serde_json::Value,
        ) = sqlx::query_as(
            "SELECT source_file, source_bytes, source_sha256, files \
             FROM schedule_feed_ingests WHERE delivered_at = $1",
        )
        .bind(delivered_at)
        .fetch_one(&pool)
        .await
        .expect("read back");
        assert_eq!(row.0.as_deref(), Some("timetable_full.zip"));
        assert_eq!(row.1, Some(77_222_226));
        assert_eq!(row.2.as_deref(), Some(sha.as_str()));
        assert_eq!(row.3, files);

        let bad_at = delivered_at + chrono::Duration::seconds(1);
        let err = insert_schedule_feed_ingest(
            &pool,
            bad_at,
            bad_at,
            &files,
            &ScheduleFeedSource {
                sha256: Some("NOT-A-SHA"),
                ..ScheduleFeedSource::default()
            },
        )
        .await;
        assert!(
            err.is_err(),
            "the CHECK constraint refuses a malformed hash"
        );

        sqlx::query("DELETE FROM schedule_feed_ingests WHERE delivered_at IN ($1, $2)")
            .bind(delivered_at)
            .bind(bad_at)
            .execute(&pool)
            .await
            .expect("cleanup fixture rows");
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p ds-store \
                schedule_feed_insert_then_last_fetch_returns_the_delivered_at \
                -- --ignored --test-threads=1`"]
    async fn schedule_feed_insert_then_last_fetch_returns_the_delivered_at() {
        use chrono::SubsecRound;

        let pool = test_pool().await;
        // `schedule_feed_ingests.delivered_at`/`ingested_at` are both
        // `TIMESTAMPTZ`, which Postgres only ever stores at microsecond
        // precision (it silently truncates, not rounds, anything finer) --
        // whereas `chrono::Utc::now()` captures nanosecond precision from
        // the system clock. Truncate the in-memory expectations to the
        // same microsecond precision the round trip through Postgres
        // actually guarantees, rather than asserting bit-for-bit equality
        // against a precision level the database can't preserve.
        let delivered_at = chrono::Utc::now().trunc_subsecs(6);
        let ingested_at = (delivered_at + chrono::Duration::minutes(5)).trunc_subsecs(6);
        let files = serde_json::json!([{"name": "TEST.DAT", "bytes": 123}]);

        insert_schedule_feed_ingest(
            &pool,
            delivered_at,
            ingested_at,
            &files,
            &ScheduleFeedSource::default(),
        )
        .await
        .expect("insert schedule feed ingest");

        let last = last_schedule_feed_fetch(&pool)
            .await
            .expect("last_schedule_feed_fetch");
        assert_eq!(
            last,
            Some(delivered_at),
            "freshness must reflect delivered_at, not ingested_at"
        );

        sqlx::query("DELETE FROM schedule_feed_ingests WHERE delivered_at = $1")
            .bind(delivered_at)
            .execute(&pool)
            .await
            .expect("cleanup fixture row");
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p ds-store \
                schedule_feed_reinserting_the_same_delivered_at_does_not_change_the_row \
                -- --ignored --test-threads=1`"]
    async fn schedule_feed_reinserting_the_same_delivered_at_does_not_change_the_row() {
        use chrono::SubsecRound;

        let pool = test_pool().await;
        // See the trunc_subsecs(6) comment in
        // `schedule_feed_insert_then_last_fetch_returns_the_delivered_at`
        // above.
        let delivered_at = chrono::Utc::now().trunc_subsecs(6);
        let first_ingested_at = delivered_at.trunc_subsecs(6);
        let first_files = serde_json::json!([{"name": "TEST-A.DAT", "bytes": 111}]);

        insert_schedule_feed_ingest(
            &pool,
            delivered_at,
            first_ingested_at,
            &first_files,
            &ScheduleFeedSource::default(),
        )
        .await
        .expect("insert schedule feed ingest");

        // Same delivered_at (this is the whole point -- a re-POST of an
        // already-recorded delivery, e.g. after schedule-ingest restarts),
        // but a different ingested_at and files -- ON CONFLICT DO NOTHING
        // means this second insert must be a harmless no-op, not an
        // upsert.
        let second_ingested_at = (first_ingested_at + chrono::Duration::hours(1)).trunc_subsecs(6);
        let second_files = serde_json::json!([{"name": "TEST-B.DAT", "bytes": 222}]);
        insert_schedule_feed_ingest(
            &pool,
            delivered_at,
            second_ingested_at,
            &second_files,
            &ScheduleFeedSource::default(),
        )
        .await
        .expect("re-insert schedule feed ingest with the same delivered_at");

        let last = last_schedule_feed_fetch(&pool)
            .await
            .expect("last_schedule_feed_fetch");
        assert_eq!(
            last,
            Some(delivered_at),
            "the original row must survive unchanged"
        );

        let (stored_files,): (serde_json::Value,) =
            sqlx::query_as("SELECT files FROM schedule_feed_ingests WHERE delivered_at = $1")
                .bind(delivered_at)
                .fetch_one(&pool)
                .await
                .expect("fetch stored row");
        assert_eq!(
            stored_files, first_files,
            "the original files payload must survive unchanged"
        );

        sqlx::query("DELETE FROM schedule_feed_ingests WHERE delivered_at = $1")
            .bind(delivered_at)
            .execute(&pool)
            .await
            .expect("cleanup fixture row");
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p ds-store \
                schedule_feed_last_fetch_against_an_empty_table_returns_none \
                -- --ignored --test-threads=1`"]
    async fn schedule_feed_last_fetch_against_an_empty_table_returns_none() {
        let pool = test_pool().await;

        // No fixture row inserted/deleted here for this timestamp -- this
        // asserts the zero-rows-for-this-value case, matching
        // `last_stations_fetch`'s own doc comment about `MAX(...)` over zero
        // rows returning one row with a NULL column.
        let sentinel_delivered_at = chrono::DateTime::parse_from_rfc3339("2000-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        sqlx::query("DELETE FROM schedule_feed_ingests WHERE delivered_at = $1")
            .bind(sentinel_delivered_at)
            .execute(&pool)
            .await
            .expect("ensure fixture delivered_at is absent");

        let last = last_schedule_feed_fetch(&pool)
            .await
            .expect("last_schedule_feed_fetch");
        // Note: this only proves `None` when the whole table is empty (the
        // realistic case for a fresh environment); if other rows already
        // exist this assertion is skipped rather than false-failing.
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM schedule_feed_ingests")
            .fetch_one(&pool)
            .await
            .expect("count rows");
        if count == 0 {
            assert_eq!(last, None);
        }
    }
}

#[cfg(test)]
mod schedule_feed_provenance_tests {
    use super::*;

    fn request(body: serde_json::Value) -> ScheduleFeedIngestRequest {
        serde_json::from_value(body).expect("valid ScheduleFeedIngestRequest JSON")
    }

    #[test]
    fn an_older_schedule_ingest_record_without_provenance_is_accepted() {
        let req = request(serde_json::json!({
            "delivered_at": "2026-09-30T19:59:59Z",
            "ingested_at": "2026-09-30T20:00:35Z",
            "files": [{"name": "RJTTF975MCA.txt", "bytes": 724_116_170_u64}]
        }));
        assert_eq!(schedule_feed_ingest_problem(&req), None);
        assert_eq!(req.source_sha256, None);
        assert_eq!(req.files[0].sha256, None);
        // No `sha256` key is written back into `files` for such a record.
        assert_eq!(
            serde_json::to_value(&req.files).unwrap(),
            serde_json::json!([{"name": "RJTTF975MCA.txt", "bytes": 724_116_170_u64}])
        );
    }

    #[test]
    fn a_record_with_well_formed_provenance_is_accepted() {
        let req = request(serde_json::json!({
            "delivered_at": "2026-09-30T19:59:59Z",
            "ingested_at": "2026-09-30T20:00:35Z",
            "files": [{"name": "RJTTF975MCA.txt", "bytes": 3, "sha256": "ab".repeat(32)}],
            "source_file": "timetable_full.zip",
            "source_bytes": 77_222_226,
            "source_sha256": "0123456789abcdef".repeat(4)
        }));
        assert_eq!(schedule_feed_ingest_problem(&req), None);
    }

    #[test]
    fn malformed_provenance_is_refused() {
        for (field, value) in [
            (
                "source_sha256",
                serde_json::json!("ABCDEF".repeat(10) + "abcd"),
            ),
            ("source_sha256", serde_json::json!("abc")),
            ("source_bytes", serde_json::json!(u64::MAX)),
            ("source_file", serde_json::json!(" ")),
        ] {
            let mut body = serde_json::json!({
                "delivered_at": "2026-09-30T19:59:59Z",
                "ingested_at": "2026-09-30T20:00:35Z",
                "files": []
            });
            body[field] = value;
            assert!(
                schedule_feed_ingest_problem(&request(body)).is_some(),
                "{field} must be validated"
            );
        }
        let bad_file = request(serde_json::json!({
            "delivered_at": "2026-09-30T19:59:59Z",
            "ingested_at": "2026-09-30T20:00:35Z",
            "files": [{"name": "RJTTF975MCA.txt", "bytes": 3, "sha256": "xyz"}]
        }));
        assert!(schedule_feed_ingest_problem(&bad_file).is_some());
    }
}
