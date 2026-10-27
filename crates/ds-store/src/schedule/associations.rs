//! `schedule_associations`: the CIF `AA` associations in force per service
//! date (pre-departure prediction phase 2, DS-152;
//! docs/superpowers/specs/2026-10-21-pre-departure-prediction-design.md
//! section 3a). schedule-reference resolves the STP overlay
//! (`schedule_query::AssociationIndex::on_date`) and replaces one date's
//! rows with [`replace_for_date`]; the late-start prediction
//! (`crate::trains::predicted_start`) caps an inferred previous working at
//! medium confidence when either train joins or divides that day.

use anyhow::Result;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// One association in force on `service_date` (the main train's date).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleAssociationRow {
    pub service_date: NaiveDate,
    pub main_uid: String,
    pub assoc_uid: String,
    pub assoc_service_date: NaiveDate,
    /// `JJ`, `VV` or `NP`.
    pub category: String,
    pub tiploc: String,
}

/// Replaces `service_date`'s associations with `rows` in one transaction,
/// so a reader sees the old set or the new one. A row for another date is
/// an error (nothing is written). A key given twice keeps its last row.
/// Returns the number of rows written.
pub async fn replace_for_date(
    pool: &PgPool,
    service_date: NaiveDate,
    rows: &[ScheduleAssociationRow],
) -> Result<u64> {
    if let Some(row) = rows.iter().find(|row| row.service_date != service_date) {
        anyhow::bail!(
            "schedule_associations publish for {service_date} has a row for {}",
            row.service_date
        );
    }
    let mains: Vec<&str> = rows.iter().map(|r| r.main_uid.as_str()).collect();
    let assocs: Vec<&str> = rows.iter().map(|r| r.assoc_uid.as_str()).collect();
    let assoc_dates: Vec<NaiveDate> = rows.iter().map(|r| r.assoc_service_date).collect();
    let categories: Vec<&str> = rows.iter().map(|r| r.category.as_str()).collect();
    let tiplocs: Vec<&str> = rows.iter().map(|r| r.tiploc.as_str()).collect();

    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM schedule_associations WHERE service_date = $1")
        .bind(service_date)
        .execute(&mut *tx)
        .await?;
    let written = sqlx::query(
        "INSERT INTO schedule_associations \
            (service_date, main_uid, assoc_uid, assoc_service_date, category, tiploc) \
         SELECT DISTINCT ON (main_uid, assoc_uid, tiploc) \
                $1::date, main_uid, assoc_uid, assoc_service_date, category, tiploc \
         FROM UNNEST($2::text[], $3::text[], $4::date[], $5::text[], $6::text[]) \
              WITH ORDINALITY AS t(main_uid, assoc_uid, assoc_service_date, category, tiploc, ord) \
         ORDER BY main_uid, assoc_uid, tiploc, ord DESC",
    )
    .bind(service_date)
    .bind(&mains)
    .bind(&assocs)
    .bind(&assoc_dates)
    .bind(&categories)
    .bind(&tiplocs)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    tx.commit().await?;
    Ok(written)
}

#[cfg(test)]
mod db_tests {
    use super::*;

    /// The owner's pool when set (the publish is schedule-reference's write).
    async fn pool() -> PgPool {
        crate::test_support::schedule_fixture_pool(&crate::test_support::connect().await).await
    }

    fn day(d: u32) -> NaiveDate {
        let date = NaiveDate::from_ymd_opt(2099, 7, d).expect("valid date");
        crate::test_support::assert_synthetic_date(date);
        date
    }

    fn row(date: NaiveDate, main: &str, assoc: &str, category: &str) -> ScheduleAssociationRow {
        ScheduleAssociationRow {
            service_date: date,
            main_uid: main.to_owned(),
            assoc_uid: assoc.to_owned(),
            assoc_service_date: date,
            category: category.to_owned(),
            tiploc: "YORK".to_owned(),
        }
    }

    async fn stored(pool: &PgPool, date: NaiveDate) -> Vec<(String, String, String)> {
        sqlx::query_as(
            "SELECT main_uid, assoc_uid, category FROM schedule_associations \
             WHERE service_date = $1 ORDER BY main_uid, assoc_uid",
        )
        .bind(date)
        .fetch_all(pool)
        .await
        .expect("read")
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL=... cargo test -p ds-store \
                associations -- --ignored --test-threads=1"]
    async fn a_publish_replaces_its_date_only() {
        let pool = pool().await;
        let (d1, d2) = (day(20), day(21));
        for date in [d1, d2] {
            replace_for_date(&pool, date, &[]).await.expect("clear");
        }
        replace_for_date(&pool, d2, &[row(d2, "M00009", "A00009", "VV")])
            .await
            .expect("other date");
        let written = replace_for_date(
            &pool,
            d1,
            &[
                row(d1, "M00001", "A00001", "JJ"),
                row(d1, "M00002", "A00002", "VV"),
                // A duplicate key keeps its last row.
                row(d1, "M00002", "A00002", "JJ"),
            ],
        )
        .await
        .expect("publish");
        assert_eq!(written, 2);
        assert_eq!(
            stored(&pool, d1).await,
            vec![
                ("M00001".into(), "A00001".into(), "JJ".into()),
                ("M00002".into(), "A00002".into(), "JJ".into()),
            ]
        );
        // A second publish replaces the first.
        replace_for_date(&pool, d1, &[row(d1, "M00003", "A00003", "VV")])
            .await
            .expect("republish");
        assert_eq!(stored(&pool, d1).await.len(), 1);
        assert_eq!(
            stored(&pool, d2).await.len(),
            1,
            "the other date is untouched"
        );
        // A row for another date refuses the whole publish.
        assert!(
            replace_for_date(&pool, d1, &[row(d2, "M00004", "A00004", "JJ")])
                .await
                .is_err()
        );
        assert_eq!(stored(&pool, d1).await.len(), 1);
        for date in [d1, d2] {
            replace_for_date(&pool, date, &[]).await.expect("clean up");
        }
    }
}
