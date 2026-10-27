//! The train-level `cancellation` model (DS-181). The rules and types live
//! in `ds_store::trains::cancellation`, shared with the notifier's push
//! copy; this module re-exports them and adds the list rows' compact form.
//!
//! **List rows** (`GET /Journeys/mine`, `GET /Train/mine`) carry no stops,
//! so they used to say plain "Cancelled" for a train that only terminates
//! short. Each now carries `cancellation`, a [`CancellationSummary`]:
//!
//! ```json
//! "cancellation": {
//!   "kind": "partial",
//!   "startsFrom": null,
//!   "terminatesAt": { "crs": "YRK", "name": "York" },
//!   "reason": { "code": "IA", "text": "Signal failure", "source": "trust" }
//! }
//! ```
//!
//! worked out exactly as the detail's `cancellation` is (the same stop
//! list, board overlay included, and the same stored TRUST messages), and
//! `null` when no part of the train's run is cancelled. Only a row whose
//! train is cancelled or has a stored cancellation or change of origin
//! builds a stop list, so a list of trains running normally costs one
//! extra query.

use std::collections::HashMap;

use sqlx::PgPool;

pub use ds_store::trains::cancellation::{
    Cancellation, CancellationKind, CancellationPlace, CancellationReason, CancellationStop,
    CancellationSummary, CancelledSection, ReasonSource, derive,
};

/// The [`CancellationSummary`] of each train in `trains` (`trains.id` and
/// its current status), keyed by `trains.id`; a train with nothing
/// cancelled is absent. Batched: one reasons read, then, only for the
/// trains that may have a cancellation, one stored-calling-points read and
/// one [`crate::data::journey::build_journey_stops_batch`].
pub async fn summaries_for_trains(
    pool: &PgPool,
    trains: &[(i64, Option<&str>)],
) -> anyhow::Result<HashMap<i64, CancellationSummary>> {
    let mut ids: Vec<i64> = trains.iter().map(|(id, _)| *id).collect();
    ids.sort_unstable();
    ids.dedup();
    let stored = crate::data::train_reasons::reasons_for_trains(pool, &ids).await?;
    let status_of: HashMap<i64, Option<&str>> = trains.iter().copied().collect();
    let candidates: Vec<i64> = ids
        .into_iter()
        .filter(|id| {
            status_of.get(id).copied().flatten() == Some("cancelled")
                || stored
                    .get(id)
                    .is_some_and(|s| s.cancellation.is_some() || s.change_of_origin.is_some())
        })
        .collect();
    if candidates.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<(i64, String, chrono::NaiveDate, Option<serde_json::Value>)> = sqlx::query_as(
        "SELECT id, train_uid, service_date, calling_points FROM trains WHERE id = ANY($1)",
    )
    .bind(&candidates)
    .fetch_all(pool)
    .await?;
    let requests: Vec<crate::data::journey::JourneyStopsRequest<'_>> = rows
        .iter()
        .map(
            |(id, uid, date, calling_points)| crate::data::journey::JourneyStopsRequest {
                trains_id: *id,
                train_uid: uid,
                service_date: *date,
                calling_points_json: calling_points.as_ref(),
                current_delay_minutes: None,
                skipped_stations: &[],
                platform: None,
                planned_platform: None,
                predicted_late_minutes: None,
            },
        )
        .collect();
    let built = crate::data::journey::build_journey_stops_batch(pool, &requests).await?;
    let mut out = HashMap::new();
    for ((id, ..), stops) in rows.iter().zip(built) {
        let mut stops = stops.unwrap_or_else(|err| {
            tracing::warn!(trains_id = id, error = ?err, "could not build stops for a cancellation summary");
            None
        });
        let status = status_of.get(id).copied().flatten();
        if let Some(cancellation) = derive(status, stored.get(id), stops.as_deref_mut()) {
            out.insert(*id, CancellationSummary::from(&cancellation));
        }
    }
    Ok(out)
}

/// Fills `cancellation` on each list row from [`summaries_for_trains`].
/// Best-effort, like the detail's reasons: a DB error leaves every row's
/// `cancellation` `null` and is logged.
pub async fn attach_summaries<T>(
    pool: &PgPool,
    rows: &mut [T],
    key: impl Fn(&T) -> Option<(i64, Option<&str>)>,
    set: impl Fn(&mut T, Option<CancellationSummary>),
) {
    let trains: Vec<(i64, Option<&str>)> = rows.iter().filter_map(&key).collect();
    if trains.is_empty() {
        return;
    }
    let summaries = match summaries_for_trains(pool, &trains).await {
        Ok(summaries) => summaries,
        Err(err) => {
            tracing::warn!(error = ?err, "could not work out cancellation summaries for a list");
            return;
        }
    };
    let ids: Vec<Option<i64>> = rows.iter().map(|row| key(row).map(|(id, _)| id)).collect();
    for (row, id) in rows.iter_mut().zip(ids) {
        set(row, id.and_then(|id| summaries.get(&id).cloned()));
    }
}

#[cfg(test)]
mod db_tests {
    use super::*;

    /// DS-184: a list row's summary is the detail's `cancellation`, cut
    /// down: an en-route `0002` at the second of four calls reads as
    /// "terminates at" that call with its reason; a train with nothing
    /// stored and running has none, and attach fills only the cancelled
    /// row.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                list_rows_carry_a_partial_cancellations_summary -- --ignored --test-threads=1`"]
    async fn list_rows_carry_a_partial_cancellations_summary() {
        let url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for DB-gated tests");
        let pool = PgPool::connect(&url).await.expect("connect");
        let date: chrono::NaiveDate = "2031-05-06".parse().unwrap();
        let _cleanup = ds_store::test_support::FixtureCleanup::new(
            &pool,
            [
                "DELETE FROM trains WHERE train_uid IN ('TLCS01', 'TLCS02')".to_string(),
                "DELETE FROM stanox_crs WHERE stanox IN ('99861', '99862', '99863', '99864')"
                    .to_string(),
                "DELETE FROM stations WHERE crs IN ('ZQA', 'ZQB', 'ZQC', 'ZQD')".to_string(),
            ],
        )
        .await;
        let ids = seed_trains(&pool, date).await;
        let mut cancel = ds_store::test_support::train_reason_message(
            Some("TLCS01"),
            "0002",
            "IA",
            "2031-05-06T08:12:00Z",
        );
        cancel.canx_type = Some("EN ROUTE".to_string());
        cancel.loc_stanox = Some("99862".to_string());
        crate::data::train_reasons::upsert_reasons(&pool, &[cancel])
            .await
            .unwrap();
        assert_summaries(&pool, &ids).await;
    }

    /// Two trains, TLCS01 and TLCS02, on four public calls ZQA..ZQD
    /// (TIPLOCs ZQTSTA..D, STANOX 99861..4), named "Test A".."Test D".
    async fn seed_trains(pool: &PgPool, date: chrono::NaiveDate) -> Vec<i64> {
        for (n, letter) in ["A", "B", "C", "D"].iter().enumerate() {
            sqlx::query(
                "INSERT INTO stanox_crs (stanox, crs, tiploc, station_name, source_sequence) \
                 VALUES ($1, $2, $3, $4, 0)",
            )
            .bind(format!("9986{}", n + 1))
            .bind(format!("ZQ{letter}"))
            .bind(format!("ZQTST{letter}"))
            .bind(format!("Test {letter}"))
            .execute(pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO stations (crs, name) VALUES ($1, $2)")
                .bind(format!("ZQ{letter}"))
                .bind(format!("Test {letter}"))
                .execute(pool)
                .await
                .unwrap();
        }
        let calling_points = serde_json::json!([
            {"tiploc": "ZQTSTA", "kind": "Origin", "bookedDeparture": "08:00:00",
             "publicDeparture": "08:00:00"},
            {"tiploc": "ZQTSTB", "kind": "Intermediate", "bookedArrival": "08:10:00",
             "bookedDeparture": "08:11:00", "publicArrival": "08:10:00",
             "publicDeparture": "08:11:00"},
            {"tiploc": "ZQTSTC", "kind": "Intermediate", "bookedArrival": "08:20:00",
             "bookedDeparture": "08:21:00", "publicArrival": "08:20:00",
             "publicDeparture": "08:21:00"},
            {"tiploc": "ZQTSTD", "kind": "Terminate", "bookedArrival": "08:30:00",
             "publicArrival": "08:30:00"},
        ]);
        let mut ids = Vec::new();
        for uid in ["TLCS01", "TLCS02"] {
            let id = crate::data::trains::find_or_create_train(pool, uid, date)
                .await
                .unwrap();
            sqlx::query("UPDATE trains SET calling_points = $1 WHERE id = $2")
                .bind(&calling_points)
                .bind(id)
                .execute(pool)
                .await
                .unwrap();
            ids.push(id);
        }
        ids
    }

    async fn assert_summaries(pool: &PgPool, ids: &[i64]) {
        let summaries = summaries_for_trains(
            pool,
            &[(ids[0], Some("cancelled")), (ids[1], Some("en_route"))],
        )
        .await
        .unwrap();
        assert_eq!(summaries.len(), 1, "{summaries:?}");
        let summary = &summaries[&ids[0]];
        assert_eq!(summary.kind, CancellationKind::Partial);
        assert_eq!(summary.starts_from, None);
        assert_eq!(
            summary.terminates_at,
            Some(CancellationPlace {
                crs: Some("ZQB".to_string()),
                name: Some("Test B".to_string()),
            })
        );
        assert_eq!(
            summary.reason_text(),
            Some("Signal failure (including no fault found)")
        );
        let json = serde_json::to_value(summary).unwrap();
        assert_eq!(json["terminatesAt"]["name"], "Test B");
        assert_eq!(json["reason"]["source"], "trust");

        // Attach: only the cancelled row gets one; a row with no train none.
        let mut rows: Vec<(Option<i64>, &str, Option<CancellationSummary>)> = vec![
            (Some(ids[0]), "cancelled", None),
            (Some(ids[1]), "en_route", None),
            (None, "pending", None),
        ];
        attach_summaries(
            pool,
            &mut rows,
            |row| row.0.map(|id| (id, Some(row.1))),
            |row, summary| row.2 = summary,
        )
        .await;
        assert_eq!(rows[0].2.as_ref(), Some(summary));
        assert_eq!(rows[1].2, None);
        assert_eq!(rows[2].2, None);
    }
}
