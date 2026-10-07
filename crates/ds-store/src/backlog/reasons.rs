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
                        "api_train_reasons_rejected_rows_total"
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

/// The stored codes for one train.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoredReasons {
    pub cancel_code: Option<String>,
    pub change_of_origin_code: Option<String>,
}

/// The stored codes of every train in `trains_ids`, keyed by `trains.id`.
/// A train with none is absent. One query for the whole batch.
pub async fn reasons_for_trains(
    pool: &PgPool,
    trains_ids: &[i64],
) -> anyhow::Result<HashMap<i64, StoredReasons>> {
    if trains_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT trains_id, msg_type, reason_code FROM train_reasons WHERE trains_id = ANY($1)",
    )
    .bind(trains_ids)
    .fetch_all(pool)
    .await?;
    let mut out: HashMap<i64, StoredReasons> = HashMap::new();
    for (trains_id, msg_type, code) in rows {
        let entry = out.entry(trains_id).or_default();
        match msg_type.as_str() {
            "0002" => entry.cancel_code = Some(code),
            "0006" => entry.change_of_origin_code = Some(code),
            _ => {}
        }
    }
    Ok(out)
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

pub fn reason_fields(status: Option<&str>, stored: Option<StoredReasons>) -> ReasonFields {
    let cancelled = status == Some("cancelled");
    let stored = stored.unwrap_or_default();
    let cancel_reason_code = stored.cancel_code.filter(|_| cancelled);
    ReasonFields {
        cancelled,
        cancel_reason: cancel_reason_code
            .as_deref()
            .and_then(reason_text)
            .map(str::to_string),
        cancel_reason_code,
        change_of_origin_reason: stored
            .change_of_origin_code
            .as_deref()
            .and_then(reason_text)
            .map(str::to_string),
        change_of_origin_reason_code: stored.change_of_origin_code,
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
        let stored = || {
            Some(StoredReasons {
                cancel_code: Some("TG".to_string()),
                change_of_origin_code: Some("YI".to_string()),
            })
        };
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
    }
}
