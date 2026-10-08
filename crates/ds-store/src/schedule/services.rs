//! `schedule_services`' write side: the per-date publish of every
//! schedule's service mode (train, replacement bus, bus or ferry). Moved
//! from the api's `data::schedule_services` (ingest architecture plan
//! 2a.2), whose readers stay in the api and re-export these. See migration
//! `20261006160000_schedule_services.sql`.

use anyhow::Result;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// One published `schedule_services` row -- the wire shape of
/// `POST /private/schedule-services` (`snake_case`, like every other
/// `schedule-reference` product).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ScheduleServiceRow {
    pub service_date: NaiveDate,
    pub uid: String,
    /// `train`/`replacement_bus`/`bus`/`ferry`.
    pub mode: String,
    pub train_status: Option<String>,
    pub train_category: Option<String>,
    pub headcode: Option<String>,
    pub rsid: Option<String>,
    pub operator_atoc: Option<String>,
    /// `P`/`O`/`N`.
    pub stp: String,
}

/// Why a publish was refused before touching the table: the caller's data
/// is wrong (a 400), not the database.
#[derive(Debug)]
pub struct InvalidPublish(pub String);

impl std::fmt::Display for InvalidPublish {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for InvalidPublish {}

/// Checks every row belongs to `service_date` and carries values the
/// table's CHECK constraints accept, so a bad publish is a clear 400 rather
/// than a constraint-violation 500.
fn validate(service_date: NaiveDate, rows: &[ScheduleServiceRow]) -> Result<(), InvalidPublish> {
    for row in rows {
        if row.service_date != service_date {
            return Err(InvalidPublish(format!(
                "row for {} {} is outside the published service_date {service_date}",
                row.uid, row.service_date
            )));
        }
        if !matches!(
            row.mode.as_str(),
            "train" | "replacement_bus" | "bus" | "ferry"
        ) {
            return Err(InvalidPublish(format!(
                "row {} has unknown mode {:?}",
                row.uid, row.mode
            )));
        }
        if !matches!(row.stp.as_str(), "P" | "O" | "N") {
            return Err(InvalidPublish(format!(
                "row {} has unknown stp {:?}",
                row.uid, row.stp
            )));
        }
        if row.uid.is_empty() || row.uid.len() > 16 {
            return Err(InvalidPublish(format!("row has a bad uid {:?}", row.uid)));
        }
        if row
            .train_status
            .as_deref()
            .is_some_and(|s| s.chars().count() != 1)
            || row
                .train_category
                .as_deref()
                .is_some_and(|c| c.chars().count() != 2)
        {
            return Err(InvalidPublish(format!(
                "row {} has a malformed train status or category",
                row.uid
            )));
        }
    }
    Ok(())
}

/// `statement_timeout` for the publish transaction. A day is ~25-30k rows;
/// the upsert and anti-join delete take well under a second.
const PUBLISH_STATEMENT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// Replaces `service_date`'s rows with `rows`, in one transaction: upserts
/// every row (an unchanged row is not rewritten), then deletes the date's
/// rows whose uid the publish did not carry. A reader sees the old day or
/// the new one, never a mix. An empty `rows` clears the date -- the
/// publisher only sends one when the rest of its window has schedules (the
/// same PL-14 guard as the other per-date products). Returns rows inserted
/// or changed.
///
/// Small enough (one row per schedule, not per calling point) that one
/// request carries a whole day, so this needs none of the multi-chunk diff
/// protocol `schedule_calling_points_full` uses.
pub async fn replace_for_date(
    pool: &PgPool,
    service_date: NaiveDate,
    rows: &[ScheduleServiceRow],
) -> Result<u64> {
    validate(service_date, rows)?;

    let uids: Vec<&str> = rows.iter().map(|r| r.uid.as_str()).collect();
    let modes: Vec<&str> = rows.iter().map(|r| r.mode.as_str()).collect();
    let statuses: Vec<Option<&str>> = rows.iter().map(|r| r.train_status.as_deref()).collect();
    let categories: Vec<Option<&str>> = rows.iter().map(|r| r.train_category.as_deref()).collect();
    let headcodes: Vec<Option<&str>> = rows.iter().map(|r| r.headcode.as_deref()).collect();
    let rsids: Vec<Option<&str>> = rows.iter().map(|r| r.rsid.as_deref()).collect();
    let operators: Vec<Option<&str>> = rows.iter().map(|r| r.operator_atoc.as_deref()).collect();
    let stps: Vec<&str> = rows.iter().map(|r| r.stp.as_str()).collect();

    let mut tx = pool.begin().await?;
    common::pg::set_local_statement_timeout(&mut tx, PUBLISH_STATEMENT_TIMEOUT).await?;

    // DISTINCT ON keeps the LAST row per uid (`ORDER BY ... ord DESC`) so a
    // duplicated uid cannot make ON CONFLICT touch one row twice.
    let upserted = sqlx::query(
        "INSERT INTO schedule_services AS s \
            (service_date, uid, mode, train_status, train_category, headcode, rsid, \
             operator_atoc, stp) \
         SELECT DISTINCT ON (uid) $1, uid, mode, train_status, train_category, headcode, rsid, \
                operator_atoc, stp \
         FROM UNNEST($2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], \
                     $8::text[], $9::text[]) \
              WITH ORDINALITY AS t(uid, mode, train_status, train_category, headcode, rsid, \
                                   operator_atoc, stp, ord) \
         ORDER BY uid, ord DESC \
         ON CONFLICT (service_date, uid) DO UPDATE SET \
            mode = EXCLUDED.mode, \
            train_status = EXCLUDED.train_status, \
            train_category = EXCLUDED.train_category, \
            headcode = EXCLUDED.headcode, \
            rsid = EXCLUDED.rsid, \
            operator_atoc = EXCLUDED.operator_atoc, \
            stp = EXCLUDED.stp \
         WHERE (s.mode, s.train_status, s.train_category, s.headcode, s.rsid, \
                s.operator_atoc, s.stp) \
               IS DISTINCT FROM \
               (EXCLUDED.mode, EXCLUDED.train_status, EXCLUDED.train_category, \
                EXCLUDED.headcode, EXCLUDED.rsid, EXCLUDED.operator_atoc, EXCLUDED.stp)",
    )
    .bind(service_date)
    .bind(&uids)
    .bind(&modes)
    .bind(&statuses)
    .bind(&categories)
    .bind(&headcodes)
    .bind(&rsids)
    .bind(&operators)
    .bind(&stps)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    let deleted = sqlx::query(
        "DELETE FROM schedule_services \
         WHERE service_date = $1 AND NOT (uid = ANY($2::text[]))",
    )
    .bind(service_date)
    .bind(&uids)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    tx.commit().await?;
    tracing::debug!(%service_date, upserted, deleted, "published schedule_services");
    Ok(upserted)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(uid: &str, date: NaiveDate) -> ScheduleServiceRow {
        ScheduleServiceRow {
            service_date: date,
            uid: uid.to_string(),
            mode: "bus".to_string(),
            train_status: Some("B".to_string()),
            train_category: Some("BS".to_string()),
            headcode: Some("0B00".to_string()),
            rsid: None,
            operator_atoc: Some("NT".to_string()),
            stp: "P".to_string(),
        }
    }

    #[test]
    fn validate_rejects_a_row_for_another_date_and_bad_values() {
        let date = NaiveDate::from_ymd_opt(2026, 10, 6).unwrap();
        assert!(validate(date, &[row("C30818", date)]).is_ok());
        let other = date.succ_opt().unwrap();
        assert!(validate(date, &[row("C30818", other)]).is_err());
        let mut bad_mode = row("C30818", date);
        bad_mode.mode = "tram".to_string();
        assert!(validate(date, &[bad_mode]).is_err());
        let mut bad_stp = row("C30818", date);
        bad_stp.stp = "C".to_string();
        assert!(validate(date, &[bad_stp]).is_err());
        let mut bad_category = row("C30818", date);
        bad_category.train_category = Some("B".to_string());
        assert!(validate(date, &[bad_category]).is_err());
    }
}
