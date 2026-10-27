//! TRUST reason codes for a train, and the train-level `cancelled` flag,
//! for the train-detail read models (`trains::PublicTrainState`,
//! `train_tracking::TrackedTrainState`) and the line trains list.
//!
//! **Write side.** `trust-backlog-consumer` posts a
//! `common::TrainReasonMessage` to `POST /private/train-reasons` for every
//! `0002` Cancellation and `0006` Change of Origin that carries a code
//! ([`upsert_reasons`]). Each is filed against its shared `trains` row in
//! `train_reasons`, one row per (train, message type), the latest winning.
//!
//! **Read side.** Every field is nullable and read at request time:
//! - `cancelled`: `status == "cancelled"`, the same TRUST-derived status
//!   the response already carries. A reinstated train is no longer
//!   cancelled.
//! - `cancelReasonCode`/`cancelReason`: the stored `0002` code and its
//!   text. Served only while `cancelled` is true, so a reinstated train
//!   does not show a stale reason.
//! - `changeOfOriginReasonCode`/`changeOfOriginReason`: the stored `0006`
//!   code and its text, whenever there is one.
//!
//! **Text.** A code is looked up in the Network Rail Historic Delay
//! Attribution Glossary (reference-data/delay-attribution-reasons.tsv,
//! August 2021, bundled at compile time). The text is the glossary's own
//! description, verbatim: industry attribution wording, not Darwin's
//! passenger prose. The text is `None`, and the code is still served, when:
//! - the glossary lacks the code (added or renamed since 2021);
//! - the code is in [`SUPPRESSED_TEXT_CODES`]: system codes whose text
//!   says nothing about why the train is not running.
//!
//! **No delay reason.** TRUST's open movement feed carries no reason on a
//! `0003` Movement, so DS has no equivalent of Darwin's `delayReason`, and
//! no such field is served.

use std::collections::HashMap;
use std::sync::LazyLock;

use chrono::{DateTime, Utc};
use sqlx::PgPool;

/// The bundled glossary, `code -> description`.
static REASON_TEXT: LazyLock<HashMap<&'static str, &'static str>> = LazyLock::new(|| {
    parse_reason_table(include_str!(
        "../../../../reference-data/delay-attribution-reasons.tsv"
    ))
});

/// Codes served without text. Both are system codes, not causes:
/// - `PD`: "System generated cancellation (NOT to be attributed to
///   manually)". This is 70% of all cancellations in a 2026-09-27 feed
///   sample, nearly all `ON CALL`, meaning a planned cancellation of a
///   schedule that was never going to run.
/// - `ZW`: "Unattributed Cancellations System Roll-ups Only".
///
/// Showing either text to a passenger would read as a reason when it is
/// not one. The code is still served, so a client can word it itself (for
/// example "planned cancellation" for `PD`).
pub const SUPPRESSED_TEXT_CODES: [&str; 2] = ["PD", "ZW"];

fn parse_reason_table(raw: &'static str) -> HashMap<&'static str, &'static str> {
    raw.lines()
        .filter(|line| !line.starts_with('#'))
        .skip(1) // column header
        .filter_map(|line| {
            let mut columns = line.split('\t');
            let code = columns.next()?.trim();
            let _name = columns.next()?;
            let description = columns.next()?.trim();
            (!code.is_empty() && !description.is_empty()).then_some((code, description))
        })
        .collect()
}

/// The passenger-facing text for `code`, or `None` (see the module doc).
pub fn reason_text(code: &str) -> Option<&'static str> {
    let code = code.trim();
    if SUPPRESSED_TEXT_CODES.contains(&code) {
        return None;
    }
    REASON_TEXT.get(code).copied()
}

/// Stores each reason against its shared `trains` row. Returns how many
/// rows were written.
///
/// The row is found by `(train_uid, service_date)` when the consumer knew
/// the uid, creating it if needed exactly as the backlog's shared movement
/// write does. Without a uid it is found by TRUST `(train_id,
/// service_date)`, and a reason whose train has no row yet is dropped, the
/// same as that train's movements.
///
/// A row that fails for a data error (SQLSTATE class 22/23) is logged and
/// skipped: it can never succeed on retry and must not hold the batch. Any
/// other error is returned, so the route answers 500. Every write is an
/// idempotent upsert, so a retried batch is harmless. An older message
/// (by `event_at`) never replaces a newer one.
pub async fn upsert_reasons(
    pool: &PgPool,
    reasons: &[common::TrainReasonMessage],
) -> anyhow::Result<u64> {
    let mut written = 0;
    for (index, reason) in reasons.iter().enumerate() {
        match upsert_one(pool, reason).await {
            Ok(true) => written += 1,
            Ok(false) => {}
            Err(err) => {
                if crate::backlog::classify_anyhow_data_error(&err).is_some() {
                    tracing::warn!(index, error = ?err, reason = ?reason, "skipping a train reason rejected for a data error");
                    metrics::counter!(common::metrics::metric_name(
                        "store_train_reasons_rejected_rows_total"
                    ))
                    .increment(1);
                } else {
                    return Err(err);
                }
            }
        }
    }
    Ok(written)
}

async fn upsert_one(pool: &PgPool, reason: &common::TrainReasonMessage) -> anyhow::Result<bool> {
    let trains_id = if let Some(uid) = reason.train_uid.as_deref() {
        Some(crate::trains::find_or_create_train(pool, uid, reason.service_date).await?)
    } else {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT id FROM trains WHERE train_id = $1 AND service_date = $2")
                .bind(&reason.train_id)
                .bind(reason.service_date)
                .fetch_optional(pool)
                .await?;
        row.map(|(id,)| id)
    };
    let Some(trains_id) = trains_id else {
        return Ok(false);
    };
    let result = sqlx::query(
        "INSERT INTO train_reasons (trains_id, msg_type, reason_code, canx_type, loc_stanox, event_at) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (trains_id, msg_type) DO UPDATE SET \
             reason_code = EXCLUDED.reason_code, canx_type = EXCLUDED.canx_type, \
             loc_stanox = EXCLUDED.loc_stanox, event_at = EXCLUDED.event_at, received_at = NOW() \
         WHERE train_reasons.event_at IS NULL OR EXCLUDED.event_at >= train_reasons.event_at",
    )
    .bind(trains_id)
    .bind(&reason.msg_type)
    .bind(reason.reason_code.trim())
    .bind(&reason.canx_type)
    .bind(&reason.loc_stanox)
    .bind(reason.event_at)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// One stored TRUST message for a train (a `train_reasons` row), with its
/// location resolved through `stanox_crs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReasonEvent {
    /// The delay attribution code; `None` when the message carried none
    /// (stored as `''`, and always so for a `0005`).
    pub code: Option<String>,
    /// `0002` only: `AT ORIGIN`, `EN ROUTE`, `ON CALL` or `OUT OF PLAN`.
    pub canx_type: Option<String>,
    pub loc_stanox: Option<String>,
    /// `stanox_crs`'s TIPLOC and CRS for `loc_stanox`, when it has a row.
    pub loc_tiploc: Option<String>,
    pub loc_crs: Option<String>,
    /// The message's own corrected timestamp: when the cancellation,
    /// reinstatement or change of origin was made.
    pub event_at: Option<DateTime<Utc>>,
}

/// The stored messages for one train: the latest of each type.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoredReasons {
    /// `0002` Cancellation.
    pub cancellation: Option<ReasonEvent>,
    /// `0005` Reinstatement (DS-181).
    pub reinstatement: Option<ReasonEvent>,
    /// `0006` Change of Origin.
    pub change_of_origin: Option<ReasonEvent>,
}

impl StoredReasons {
    /// The `0002` code, if the stored cancellation carried one.
    pub fn cancel_code(&self) -> Option<&str> {
        self.cancellation.as_ref()?.code.as_deref()
    }

    /// The `0006` code, if the stored change of origin carried one.
    pub fn change_of_origin_code(&self) -> Option<&str> {
        self.change_of_origin.as_ref()?.code.as_deref()
    }
}

/// The stored messages of every train in `trains_ids`, keyed by
/// `trains.id`, each location resolved through `stanox_crs`. A train with
/// none is absent. One query for the whole batch.
pub async fn reasons_for_trains(
    pool: &PgPool,
    trains_ids: &[i64],
) -> anyhow::Result<HashMap<i64, StoredReasons>> {
    if trains_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<ReasonEventRow> = sqlx::query_as(
        "SELECT r.trains_id, r.msg_type, NULLIF(BTRIM(r.reason_code), '') AS code, \
                r.canx_type, r.loc_stanox, s.tiploc AS loc_tiploc, s.crs AS loc_crs, \
                r.event_at \
         FROM train_reasons r \
         LEFT JOIN stanox_crs s ON s.stanox = r.loc_stanox \
         WHERE r.trains_id = ANY($1)",
    )
    .bind(trains_ids)
    .fetch_all(pool)
    .await?;
    let mut out: HashMap<i64, StoredReasons> = HashMap::new();
    for row in rows {
        let event = ReasonEvent {
            code: row.code,
            canx_type: row.canx_type.map(|t| t.trim().to_string()),
            loc_stanox: row.loc_stanox,
            loc_tiploc: row.loc_tiploc,
            loc_crs: row.loc_crs,
            event_at: row.event_at,
        };
        let entry = out.entry(row.trains_id).or_default();
        match row.msg_type.as_str() {
            "0002" => entry.cancellation = Some(event),
            "0005" => entry.reinstatement = Some(event),
            "0006" => entry.change_of_origin = Some(event),
            _ => {}
        }
    }
    Ok(out)
}

#[derive(sqlx::FromRow)]
struct ReasonEventRow {
    trains_id: i64,
    msg_type: String,
    code: Option<String>,
    canx_type: Option<String>,
    loc_stanox: Option<String>,
    loc_tiploc: Option<String>,
    loc_crs: Option<String>,
    event_at: Option<DateTime<Utc>>,
}

/// The five wire fields, computed from a train's status and stored codes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReasonFields {
    pub cancelled: bool,
    pub cancel_reason_code: Option<String>,
    pub cancel_reason: Option<String>,
    pub change_of_origin_reason_code: Option<String>,
    pub change_of_origin_reason: Option<String>,
}

pub fn reason_fields(status: Option<&str>, stored: Option<&StoredReasons>) -> ReasonFields {
    let cancelled = status == Some("cancelled");
    let cancel_reason_code = stored
        .and_then(StoredReasons::cancel_code)
        .filter(|_| cancelled)
        .map(str::to_string);
    let change_of_origin_reason_code = stored
        .and_then(StoredReasons::change_of_origin_code)
        .map(str::to_string);
    ReasonFields {
        cancelled,
        cancel_reason: cancel_reason_code
            .as_deref()
            .and_then(reason_text)
            .map(str::to_string),
        cancel_reason_code,
        change_of_origin_reason: change_of_origin_reason_code
            .as_deref()
            .and_then(reason_text)
            .map(str::to_string),
        change_of_origin_reason_code,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_bundled_glossary_parses_and_maps_known_codes() {
        assert!(REASON_TEXT.len() > 300, "{} codes", REASON_TEXT.len());
        assert_eq!(reason_text("TG"), Some("Driver"));
        assert_eq!(
            reason_text(" IB "),
            Some("Points failure (including no fault found)")
        );
    }

    #[test]
    fn unknown_and_system_codes_have_no_text() {
        assert_eq!(reason_text("Q?"), None);
        assert!(REASON_TEXT.contains_key("PD"), "PD is in the glossary");
        assert_eq!(reason_text("PD"), None);
        assert_eq!(reason_text("ZW"), None);
    }

    #[test]
    fn a_cancel_reason_is_served_only_while_cancelled() {
        let coded = |code: &str| ReasonEvent {
            code: Some(code.to_string()),
            ..ReasonEvent::default()
        };
        let stored = StoredReasons {
            cancellation: Some(coded("TG")),
            reinstatement: None,
            change_of_origin: Some(coded("YI")),
        };
        let stored = || Some(&stored);
        let cancelled = reason_fields(Some("cancelled"), stored());
        assert!(cancelled.cancelled);
        assert_eq!(cancelled.cancel_reason_code.as_deref(), Some("TG"));
        assert_eq!(cancelled.cancel_reason.as_deref(), Some("Driver"));
        assert_eq!(
            cancelled.change_of_origin_reason_code.as_deref(),
            Some("YI")
        );
        assert!(cancelled.change_of_origin_reason.is_some());

        // Reinstated: running again, so the old cancellation reason is hidden.
        let running = reason_fields(Some("en_route"), stored());
        assert!(!running.cancelled);
        assert_eq!(running.cancel_reason_code, None);
        assert_eq!(running.cancel_reason, None);
        assert_eq!(running.change_of_origin_reason_code.as_deref(), Some("YI"));

        assert_eq!(reason_fields(None, None), ReasonFields::default());

        // An uncoded cancellation (stored as '') serves no code.
        let uncoded = StoredReasons {
            cancellation: Some(ReasonEvent::default()),
            ..StoredReasons::default()
        };
        let fields = reason_fields(Some("cancelled"), Some(&uncoded));
        assert!(fields.cancelled);
        assert_eq!(fields.cancel_reason_code, None);
    }
}

#[cfg(test)]
mod db_tests {
    use super::*;
    use crate::test_support::{connect, train_reason_message as message};

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                reasons_upsert_newest_wins_and_resolve_by_uid_or_train_id -- --ignored --test-threads=1`"]
    async fn reasons_upsert_newest_wins_and_resolve_by_uid_or_train_id() {
        let pool = connect().await;
        let date: chrono::NaiveDate = "2031-05-06".parse().unwrap();
        sqlx::query("DELETE FROM trains WHERE train_uid IN ('TRSN01', 'TRSN02')")
            .execute(&pool)
            .await
            .unwrap();

        // By uid: the trains row is created on demand.
        let written = upsert_reasons(
            &pool,
            &[
                message(Some("TRSN01"), "0002", "TG", "2031-05-06T08:00:00Z"),
                // Older message for the same train and type: ignored.
                message(Some("TRSN01"), "0002", "IB", "2031-05-06T07:00:00Z"),
                message(Some("TRSN01"), "0006", "YI", "2031-05-06T07:30:00Z"),
            ],
        )
        .await
        .unwrap();
        assert_eq!(written, 2);
        let trains_id = crate::trains::find_or_create_train(&pool, "TRSN01", date)
            .await
            .unwrap();
        let stored = reasons_for_trains(&pool, &[trains_id]).await.unwrap();
        let train = &stored[&trains_id];
        assert_eq!(train.cancel_code(), Some("TG"));
        assert_eq!(train.change_of_origin_code(), Some("YI"));
        let cancellation = train.cancellation.as_ref().unwrap();
        assert_eq!(cancellation.canx_type.as_deref(), Some("AT ORIGIN"));
        assert_eq!(cancellation.loc_stanox.as_deref(), Some("87701"));
        assert_eq!(
            cancellation.event_at,
            Some("2031-05-06T08:00:00Z".parse().unwrap())
        );
        assert_eq!(train.reinstatement, None);

        // A newer message replaces it (a re-cancellation after a reinstatement).
        upsert_reasons(
            &pool,
            &[message(
                Some("TRSN01"),
                "0002",
                "M8",
                "2031-05-06T09:00:00Z",
            )],
        )
        .await
        .unwrap();
        let stored = reasons_for_trains(&pool, &[trains_id]).await.unwrap();
        assert_eq!(stored[&trains_id].cancel_code(), Some("M8"));

        // A reinstatement (DS-181) and an uncoded cancellation are kept:
        // no code, but their location and time.
        upsert_reasons(
            &pool,
            &[
                message(Some("TRSN01"), "0005", "", "2031-05-06T09:30:00Z"),
                message(Some("TRSN01"), "0002", "  ", "2031-05-06T09:45:00Z"),
            ],
        )
        .await
        .unwrap();
        let stored = reasons_for_trains(&pool, &[trains_id]).await.unwrap();
        let train = &stored[&trains_id];
        let reinstatement = train.reinstatement.as_ref().unwrap();
        assert_eq!(reinstatement.code, None);
        assert_eq!(reinstatement.loc_stanox.as_deref(), Some("87701"));
        assert_eq!(train.cancel_code(), None);
        assert_eq!(
            train.cancellation.as_ref().unwrap().event_at,
            Some("2031-05-06T09:45:00Z".parse().unwrap())
        );

        // Without a uid: found by TRUST train_id once the row carries it;
        // dropped (not an error) before then.
        let second = crate::trains::find_or_create_train(&pool, "TRSN02", date)
            .await
            .unwrap();
        let mut no_uid = message(None, "0002", "XA", "2031-05-06T10:00:00Z");
        no_uid.train_id = "9TR0002Q26".to_string();
        assert_eq!(
            upsert_reasons(&pool, std::slice::from_ref(&no_uid))
                .await
                .unwrap(),
            0
        );
        crate::trains::mark_train_resolved(&pool, second, "9TR0002Q26")
            .await
            .unwrap();
        assert_eq!(upsert_reasons(&pool, &[no_uid]).await.unwrap(), 1);
        let stored = reasons_for_trains(&pool, &[second]).await.unwrap();
        assert_eq!(stored[&second].cancel_code(), Some("XA"));

        // Retention follows `trains`.
        sqlx::query("DELETE FROM trains WHERE train_uid IN ('TRSN01', 'TRSN02')")
            .execute(&pool)
            .await
            .unwrap();
        let (left,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM train_reasons WHERE trains_id = ANY($1)")
                .bind(vec![trains_id, second])
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(left, 0);
    }
}
