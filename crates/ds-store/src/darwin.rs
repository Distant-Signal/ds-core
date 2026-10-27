//! The Darwin Push Port tables (DS-152, migration
//! `20261027130000_darwin_pushport.sql`): [`apply`] writes one
//! `darwin-pushport/1` batch ([`common::darwin::Batch`]) for ingest-writer,
//! and [`calls_for_trains`] reads a set of trains' per-location platforms
//! for the api.
//!
//! **Ordering.** Every upsert is guarded by the Darwin message timestamp
//! (`ts`) of the row it would replace: a schedule call by `schedule_ts`, a
//! TS call by `ts_ts`, an association by `message_ts`. A replayed batch,
//! or one older than what is stored, changes nothing. Within a TS call a
//! group the message did not carry (platform, arrival, departure, pass)
//! keeps its stored value; the relay already merged its own batch that way
//! ([`common::darwin::Batch::merge`]), so each key appears once here.

use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use common::darwin::{Batch, Event, Platform};
use sqlx::{PgConnection, PgPool};

/// What [`apply`] wrote: rows inserted or updated per table (a row the
/// ordering guard refused is not counted).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Applied {
    pub services: u64,
    pub calls: u64,
    pub associations: u64,
    pub deactivated: u64,
}

impl Applied {
    pub fn total(&self) -> u64 {
        self.services + self.calls + self.associations + self.deactivated
    }
}

/// Writes `batch` in the caller's transaction, services first (a call or
/// association needs none, but a deactivation updates a known service).
pub async fn apply(conn: &mut PgConnection, batch: &Batch) -> Result<Applied> {
    let services = upsert_services(conn, batch).await?;
    let calls =
        upsert_schedule_calls(conn, batch).await? + upsert_status_calls(conn, batch).await?;
    let associations = upsert_associations(conn, batch).await?;
    let deactivated = deactivate(conn, batch).await?;
    Ok(Applied {
        services,
        calls,
        associations,
        deactivated,
    })
}

// `allow`, not `expect`: the lint fires on some toolchains only (workspace
// Cargo.toml).
#[allow(clippy::similar_names, reason = "the columns' own names (rid, rsid)")]
async fn upsert_services(conn: &mut PgConnection, batch: &Batch) -> Result<u64> {
    if batch.services.is_empty() {
        return Ok(0);
    }
    let mut rid = Vec::new();
    let mut uid = Vec::new();
    let mut ssd = Vec::new();
    let mut toc: Vec<Option<String>> = Vec::new();
    let mut train_id: Vec<Option<String>> = Vec::new();
    let mut rsid: Vec<Option<String>> = Vec::new();
    let mut is_passenger: Vec<Option<bool>> = Vec::new();
    let mut deleted: Vec<bool> = Vec::new();
    let mut cancel_reason: Vec<Option<String>> = Vec::new();
    let mut schedule_ts: Vec<Option<DateTime<Utc>>> = Vec::new();
    let mut message_ts = Vec::new();
    for service in &batch.services {
        rid.push(service.rid.as_str());
        uid.push(service.uid.as_str());
        ssd.push(service.ssd);
        let schedule = service.schedule.as_ref();
        toc.push(schedule.and_then(|s| s.toc.clone()));
        train_id.push(schedule.and_then(|s| s.train_id.clone()));
        rsid.push(schedule.and_then(|s| s.rsid.clone()));
        is_passenger.push(schedule.and_then(|s| s.is_passenger));
        deleted.push(schedule.is_some_and(|s| s.deleted));
        cancel_reason.push(schedule.and_then(|s| s.cancel_reason.clone()));
        schedule_ts.push(schedule.map(|_| service.ts));
        message_ts.push(service.ts);
    }
    // A schedule column changes only with a schedule message at least as
    // new as the stored one (`fresh`); a TS-only row leaves them alone.
    let sql = "INSERT INTO darwin_services AS d \
             (rid, uid, ssd, toc, train_id, rsid, is_passenger, is_deleted, cancel_reason, \
              schedule_ts, message_ts) \
         SELECT * FROM UNNEST($1::text[], $2::text[], $3::date[], $4::text[], $5::text[], \
                              $6::text[], $7::bool[], $8::bool[], $9::text[], \
                              $10::timestamptz[], $11::timestamptz[]) \
         ON CONFLICT (rid) DO UPDATE SET \
             uid = CASE WHEN EXCLUDED.message_ts >= d.message_ts THEN EXCLUDED.uid ELSE d.uid END, \
             ssd = CASE WHEN EXCLUDED.message_ts >= d.message_ts THEN EXCLUDED.ssd ELSE d.ssd END, \
             toc = CASE WHEN fresh THEN EXCLUDED.toc ELSE d.toc END, \
             train_id = CASE WHEN fresh THEN EXCLUDED.train_id ELSE d.train_id END, \
             rsid = CASE WHEN fresh THEN EXCLUDED.rsid ELSE d.rsid END, \
             is_passenger = CASE WHEN fresh THEN EXCLUDED.is_passenger ELSE d.is_passenger END, \
             is_deleted = CASE WHEN fresh THEN EXCLUDED.is_deleted ELSE d.is_deleted END, \
             cancel_reason = CASE WHEN fresh THEN EXCLUDED.cancel_reason ELSE d.cancel_reason END, \
             schedule_ts = CASE WHEN fresh THEN EXCLUDED.schedule_ts ELSE d.schedule_ts END, \
             message_ts = GREATEST(d.message_ts, EXCLUDED.message_ts), \
             updated_at = now() \
         WHERE EXCLUDED.message_ts >= d.message_ts \
            OR (EXCLUDED.schedule_ts IS NOT NULL \
                AND (d.schedule_ts IS NULL OR EXCLUDED.schedule_ts >= d.schedule_ts))"
        .replace(
            "fresh",
            "(EXCLUDED.schedule_ts IS NOT NULL \
                  AND (d.schedule_ts IS NULL OR EXCLUDED.schedule_ts >= d.schedule_ts))",
        );
    let result = sqlx::query(&sql)
        .bind(&rid)
        .bind(&uid)
        .bind(&ssd)
        .bind(&toc)
        .bind(&train_id)
        .bind(&rsid)
        .bind(&is_passenger)
        .bind(&deleted)
        .bind(&cancel_reason)
        .bind(&schedule_ts)
        .bind(&message_ts)
        .execute(&mut *conn)
        .await?;
    Ok(result.rows_affected())
}

#[allow(
    clippy::similar_names,
    reason = "the columns' own names (wta, wtd, wtp, pta, ptd); fires on some toolchains only"
)]
async fn upsert_schedule_calls(conn: &mut PgConnection, batch: &Batch) -> Result<u64> {
    if batch.schedule_calls.is_empty() {
        return Ok(0);
    }
    let calls = &batch.schedule_calls;
    let rid: Vec<&str> = calls.iter().map(|c| c.key.rid.as_str()).collect();
    let tiploc: Vec<&str> = calls.iter().map(|c| c.key.tiploc.as_str()).collect();
    let wta: Vec<&str> = calls.iter().map(|c| c.key.wta.as_str()).collect();
    let wtd: Vec<&str> = calls.iter().map(|c| c.key.wtd.as_str()).collect();
    let wtp: Vec<&str> = calls.iter().map(|c| c.key.wtp.as_str()).collect();
    let ssd: Vec<NaiveDate> = calls.iter().map(|c| c.ssd).collect();
    let pta: Vec<Option<&str>> = calls.iter().map(|c| c.pta.as_deref()).collect();
    let ptd: Vec<Option<&str>> = calls.iter().map(|c| c.ptd.as_deref()).collect();
    let planned: Vec<Option<&str>> = calls
        .iter()
        .map(|c| c.planned_platform.as_deref())
        .collect();
    let cancelled: Vec<bool> = calls.iter().map(|c| c.cancelled).collect();
    let ts: Vec<DateTime<Utc>> = calls.iter().map(|c| c.ts).collect();
    let result = sqlx::query(
        "INSERT INTO darwin_calls AS d \
             (rid, tiploc, wta, wtd, wtp, ssd, pta, ptd, planned_platform, is_cancelled, \
              schedule_ts) \
         SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], \
                              $6::date[], $7::text[], $8::text[], $9::text[], $10::bool[], \
                              $11::timestamptz[]) \
         ON CONFLICT (rid, tiploc, wta, wtd, wtp) DO UPDATE SET \
             ssd = EXCLUDED.ssd, \
             pta = EXCLUDED.pta, \
             ptd = EXCLUDED.ptd, \
             planned_platform = EXCLUDED.planned_platform, \
             is_cancelled = EXCLUDED.is_cancelled, \
             schedule_ts = EXCLUDED.schedule_ts, \
             updated_at = now() \
         WHERE d.schedule_ts IS NULL OR d.schedule_ts <= EXCLUDED.schedule_ts",
    )
    .bind(&rid)
    .bind(&tiploc)
    .bind(&wta)
    .bind(&wtd)
    .bind(&wtp)
    .bind(&ssd)
    .bind(&pta)
    .bind(&ptd)
    .bind(&planned)
    .bind(&cancelled)
    .bind(&ts)
    .execute(&mut *conn)
    .await?;
    Ok(result.rows_affected())
}

/// The columns of one event group, `None`s when the message lacked it.
fn event_columns(event: Option<&Event>) -> (Option<String>, Option<String>, Option<bool>) {
    match event {
        Some(event) => (event.et.clone(), event.at.clone(), Some(event.delayed)),
        None => (None, None, None),
    }
}

#[allow(
    clippy::too_many_lines,
    clippy::similar_names,
    reason = "one bulk upsert: a column array per field (named after its column), then the statement"
)]
async fn upsert_status_calls(conn: &mut PgConnection, batch: &Batch) -> Result<u64> {
    if batch.status_calls.is_empty() {
        return Ok(0);
    }
    let calls = &batch.status_calls;
    let rid: Vec<&str> = calls.iter().map(|c| c.key.rid.as_str()).collect();
    let tiploc: Vec<&str> = calls.iter().map(|c| c.key.tiploc.as_str()).collect();
    let wta: Vec<&str> = calls.iter().map(|c| c.key.wta.as_str()).collect();
    let wtd: Vec<&str> = calls.iter().map(|c| c.key.wtd.as_str()).collect();
    let wtp: Vec<&str> = calls.iter().map(|c| c.key.wtp.as_str()).collect();
    let ssd: Vec<NaiveDate> = calls.iter().map(|c| c.ssd).collect();
    let pta: Vec<Option<&str>> = calls.iter().map(|c| c.pta.as_deref()).collect();
    let ptd: Vec<Option<&str>> = calls.iter().map(|c| c.ptd.as_deref()).collect();
    let platform: Vec<Option<&Platform>> = calls.iter().map(|c| c.platform.as_ref()).collect();
    let number: Vec<Option<&str>> = platform
        .iter()
        .map(|p| p.map(|p| p.number.as_str()))
        .collect();
    let confirmed: Vec<Option<bool>> = platform.iter().map(|p| p.map(|p| p.confirmed)).collect();
    let suppressed: Vec<Option<bool>> = platform.iter().map(|p| p.map(|p| p.suppressed)).collect();
    let cis_suppressed: Vec<Option<bool>> = platform
        .iter()
        .map(|p| p.map(|p| p.cis_suppressed))
        .collect();
    let source: Vec<Option<String>> = platform
        .iter()
        .map(|p| p.and_then(|p| p.source).map(String::from))
        .collect();
    let (arr_et, arr_at, arr_delayed): (Vec<_>, Vec<_>, Vec<_>) = multiunzip(
        calls
            .iter()
            .map(|c| event_columns(c.arrival.as_ref()))
            .collect(),
    );
    let (dep_et, dep_at, dep_delayed): (Vec<_>, Vec<_>, Vec<_>) = multiunzip(
        calls
            .iter()
            .map(|c| event_columns(c.departure.as_ref()))
            .collect(),
    );
    let (pass_et, pass_at, _): (Vec<_>, Vec<_>, Vec<_>) = multiunzip(
        calls
            .iter()
            .map(|c| event_columns(c.pass.as_ref()))
            .collect(),
    );
    let call_suppressed: Vec<bool> = calls.iter().map(|c| c.suppressed).collect();
    let ts: Vec<DateTime<Utc>> = calls.iter().map(|c| c.ts).collect();
    // A NULL platform number or event time means the message lacked that
    // group (or field): the stored value stays.
    let result = sqlx::query(
        "INSERT INTO darwin_calls AS d \
             (rid, tiploc, wta, wtd, wtp, ssd, pta, ptd, platform, platform_confirmed, \
              platform_suppressed, platform_cis_suppressed, platform_source, \
              arrival_et, arrival_at, arrival_delayed, departure_et, departure_at, \
              departure_delayed, pass_et, pass_at, is_suppressed, ts_ts) \
         SELECT u.rid, u.tiploc, u.wta, u.wtd, u.wtp, u.ssd, u.pta, u.ptd, u.platform, \
                COALESCE(u.confirmed, FALSE), COALESCE(u.suppressed, FALSE), \
                COALESCE(u.cis_suppressed, FALSE), u.source, \
                u.arr_et, u.arr_at, COALESCE(u.arr_delayed, FALSE), \
                u.dep_et, u.dep_at, COALESCE(u.dep_delayed, FALSE), \
                u.pass_et, u.pass_at, u.call_suppressed, u.ts \
         FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::date[], \
                     $7::text[], $8::text[], $9::text[], $10::bool[], $11::bool[], \
                     $12::bool[], $13::text[], $14::text[], $15::text[], $16::bool[], \
                     $17::text[], $18::text[], $19::bool[], $20::text[], $21::text[], \
                     $22::bool[], $23::timestamptz[]) \
              AS u(rid, tiploc, wta, wtd, wtp, ssd, pta, ptd, platform, confirmed, \
                   suppressed, cis_suppressed, source, arr_et, arr_at, arr_delayed, \
                   dep_et, dep_at, dep_delayed, pass_et, pass_at, call_suppressed, ts) \
         ON CONFLICT (rid, tiploc, wta, wtd, wtp) DO UPDATE SET \
             ssd = EXCLUDED.ssd, \
             pta = COALESCE(EXCLUDED.pta, d.pta), \
             ptd = COALESCE(EXCLUDED.ptd, d.ptd), \
             platform = COALESCE(EXCLUDED.platform, d.platform), \
             platform_confirmed = CASE WHEN EXCLUDED.platform IS NOT NULL \
                 THEN EXCLUDED.platform_confirmed ELSE d.platform_confirmed END, \
             platform_suppressed = CASE WHEN EXCLUDED.platform IS NOT NULL \
                 THEN EXCLUDED.platform_suppressed ELSE d.platform_suppressed END, \
             platform_cis_suppressed = CASE WHEN EXCLUDED.platform IS NOT NULL \
                 THEN EXCLUDED.platform_cis_suppressed ELSE d.platform_cis_suppressed END, \
             platform_source = CASE WHEN EXCLUDED.platform IS NOT NULL \
                 THEN EXCLUDED.platform_source ELSE d.platform_source END, \
             arrival_et = COALESCE(EXCLUDED.arrival_et, d.arrival_et), \
             arrival_at = COALESCE(EXCLUDED.arrival_at, d.arrival_at), \
             arrival_delayed = CASE WHEN EXCLUDED.arrival_et IS NOT NULL \
                 OR EXCLUDED.arrival_at IS NOT NULL OR EXCLUDED.arrival_delayed \
                 THEN EXCLUDED.arrival_delayed ELSE d.arrival_delayed END, \
             departure_et = COALESCE(EXCLUDED.departure_et, d.departure_et), \
             departure_at = COALESCE(EXCLUDED.departure_at, d.departure_at), \
             departure_delayed = CASE WHEN EXCLUDED.departure_et IS NOT NULL \
                 OR EXCLUDED.departure_at IS NOT NULL OR EXCLUDED.departure_delayed \
                 THEN EXCLUDED.departure_delayed ELSE d.departure_delayed END, \
             pass_et = COALESCE(EXCLUDED.pass_et, d.pass_et), \
             pass_at = COALESCE(EXCLUDED.pass_at, d.pass_at), \
             is_suppressed = EXCLUDED.is_suppressed, \
             ts_ts = EXCLUDED.ts_ts, \
             updated_at = now() \
         WHERE d.ts_ts IS NULL OR d.ts_ts <= EXCLUDED.ts_ts",
    )
    .bind(&rid)
    .bind(&tiploc)
    .bind(&wta)
    .bind(&wtd)
    .bind(&wtp)
    .bind(&ssd)
    .bind(&pta)
    .bind(&ptd)
    .bind(&number)
    .bind(&confirmed)
    .bind(&suppressed)
    .bind(&cis_suppressed)
    .bind(&source)
    .bind(&arr_et)
    .bind(&arr_at)
    .bind(&arr_delayed)
    .bind(&dep_et)
    .bind(&dep_at)
    .bind(&dep_delayed)
    .bind(&pass_et)
    .bind(&pass_at)
    .bind(&call_suppressed)
    .bind(&ts)
    .execute(&mut *conn)
    .await?;
    Ok(result.rows_affected())
}

/// `Vec<(a, b, c)>` into three `Vec`s.
fn multiunzip<A, B, C>(rows: Vec<(A, B, C)>) -> (Vec<A>, Vec<B>, Vec<C>) {
    let mut a = Vec::with_capacity(rows.len());
    let mut b = Vec::with_capacity(rows.len());
    let mut c = Vec::with_capacity(rows.len());
    for (x, y, z) in rows {
        a.push(x);
        b.push(y);
        c.push(z);
    }
    (a, b, c)
}

async fn upsert_associations(conn: &mut PgConnection, batch: &Batch) -> Result<u64> {
    if batch.associations.is_empty() {
        return Ok(0);
    }
    let rows = &batch.associations;
    let main: Vec<&str> = rows.iter().map(|a| a.main_rid.as_str()).collect();
    let assoc: Vec<&str> = rows.iter().map(|a| a.assoc_rid.as_str()).collect();
    let tiploc: Vec<&str> = rows.iter().map(|a| a.tiploc.as_str()).collect();
    let category: Vec<&str> = rows.iter().map(|a| a.category.as_str()).collect();
    let cancelled: Vec<bool> = rows.iter().map(|a| a.cancelled).collect();
    let deleted: Vec<bool> = rows.iter().map(|a| a.deleted).collect();
    let time =
        |f: fn(&common::darwin::Association) -> Option<NaiveTime>| -> Vec<Option<NaiveTime>> {
            rows.iter().map(f).collect()
        };
    let main_ssd: Vec<Option<NaiveDate>> = rows.iter().map(|a| a.main_ssd).collect();
    let ts: Vec<DateTime<Utc>> = rows.iter().map(|a| a.ts).collect();
    let result = sqlx::query(
        "INSERT INTO darwin_associations AS d \
             (main_rid, assoc_rid, tiploc, category, is_cancelled, is_deleted, main_wta, \
              main_wtd, main_wtp, assoc_wta, assoc_wtd, assoc_wtp, main_ssd, message_ts) \
         SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::bool[], \
                              $6::bool[], $7::time[], $8::time[], $9::time[], $10::time[], \
                              $11::time[], $12::time[], $13::date[], $14::timestamptz[]) \
         ON CONFLICT (main_rid, assoc_rid, tiploc, category) DO UPDATE SET \
             is_cancelled = EXCLUDED.is_cancelled, \
             is_deleted = EXCLUDED.is_deleted, \
             main_wta = EXCLUDED.main_wta, main_wtd = EXCLUDED.main_wtd, \
             main_wtp = EXCLUDED.main_wtp, assoc_wta = EXCLUDED.assoc_wta, \
             assoc_wtd = EXCLUDED.assoc_wtd, assoc_wtp = EXCLUDED.assoc_wtp, \
             main_ssd = COALESCE(EXCLUDED.main_ssd, d.main_ssd), \
             message_ts = EXCLUDED.message_ts, \
             updated_at = now() \
         WHERE d.message_ts <= EXCLUDED.message_ts",
    )
    .bind(&main)
    .bind(&assoc)
    .bind(&tiploc)
    .bind(&category)
    .bind(&cancelled)
    .bind(&deleted)
    .bind(time(|a| a.main_wta))
    .bind(time(|a| a.main_wtd))
    .bind(time(|a| a.main_wtp))
    .bind(time(|a| a.assoc_wta))
    .bind(time(|a| a.assoc_wtd))
    .bind(time(|a| a.assoc_wtp))
    .bind(&main_ssd)
    .bind(&ts)
    .execute(&mut *conn)
    .await?;
    Ok(result.rows_affected())
}

/// Marks known services deactivated. A RID never seen has no row (Darwin
/// gives no UID with a deactivation), so there is nothing to mark.
async fn deactivate(conn: &mut PgConnection, batch: &Batch) -> Result<u64> {
    if batch.deactivated.is_empty() {
        return Ok(0);
    }
    let rid: Vec<&str> = batch.deactivated.iter().map(|d| d.rid.as_str()).collect();
    let ts: Vec<DateTime<Utc>> = batch.deactivated.iter().map(|d| d.ts).collect();
    let result = sqlx::query(
        "UPDATE darwin_services d SET deactivated = TRUE, \
             message_ts = GREATEST(d.message_ts, u.ts), updated_at = now() \
         FROM UNNEST($1::text[], $2::timestamptz[]) AS u(rid, ts) \
         WHERE d.rid = u.rid AND NOT d.deactivated",
    )
    .bind(&rid)
    .bind(&ts)
    .execute(&mut *conn)
    .await?;
    Ok(result.rows_affected())
}

/// One Push Port location of a train, as the api reads it.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "the table's own independent flags, as stored"
)]
pub struct DarwinCall {
    pub tiploc: String,
    /// Working times as Darwin gave them (`""` when absent).
    pub wta: String,
    pub wtd: String,
    pub wtp: String,
    pub planned_platform: Option<String>,
    pub is_cancelled: bool,
    pub platform: Option<String>,
    pub platform_confirmed: bool,
    pub platform_suppressed: bool,
    pub platform_cis_suppressed: bool,
    /// `suppressed`, `confirmed`, `altered` or `planned`; `None` without a
    /// platform.
    pub platform_basis: Option<String>,
    pub arrival_et: Option<String>,
    pub departure_et: Option<String>,
    pub updated_at: DateTime<Utc>,
}

impl DarwinCall {
    /// Whether the platform may not be shown to the public (`platsup` or
    /// `cisPlatsup`).
    pub fn platform_hidden(&self) -> bool {
        self.platform_suppressed || self.platform_cis_suppressed
    }
}

#[derive(sqlx::FromRow)]
struct CallRow {
    uid: String,
    ssd: NaiveDate,
    #[sqlx(flatten)]
    call: DarwinCall,
}

/// The Push Port locations of each `(train_uid, service_date)` in `trains`
/// that Darwin knows, in no particular order. When Darwin holds several
/// live RIDs for one train (rare), the most recently updated one's.
pub async fn calls_for_trains(
    pool: &PgPool,
    trains: &[(String, NaiveDate)],
) -> Result<HashMap<(String, NaiveDate), Vec<DarwinCall>>> {
    if trains.is_empty() {
        return Ok(HashMap::new());
    }
    let uids: Vec<&str> = trains.iter().map(|(uid, _)| uid.as_str()).collect();
    let dates: Vec<NaiveDate> = trains.iter().map(|(_, date)| *date).collect();
    let rows: Vec<CallRow> = sqlx::query_as(
        "WITH k AS (SELECT DISTINCT * FROM UNNEST($1::text[], $2::date[]) AS k(uid, ssd)), \
         s AS ( \
             SELECT DISTINCT ON (d.uid, d.ssd) d.rid, d.uid, d.ssd \
             FROM k JOIN darwin_services d ON d.uid = k.uid AND d.ssd = k.ssd \
             WHERE NOT d.is_deleted \
             ORDER BY d.uid, d.ssd, d.message_ts DESC \
         ) \
         SELECT s.uid, s.ssd, c.tiploc, c.wta, c.wtd, c.wtp, c.planned_platform, \
                c.is_cancelled, c.platform, c.platform_confirmed, c.platform_suppressed, \
                c.platform_cis_suppressed, c.platform_basis, c.arrival_et, c.departure_et, \
                c.updated_at \
         FROM s JOIN darwin_calls c ON c.rid = s.rid",
    )
    .bind(&uids)
    .bind(&dates)
    .fetch_all(pool)
    .await?;
    let mut out: HashMap<(String, NaiveDate), Vec<DarwinCall>> = HashMap::new();
    for row in rows {
        out.entry((row.uid, row.ssd)).or_default().push(row.call);
    }
    Ok(out)
}

#[cfg(test)]
mod db_tests {
    use chrono::Duration;
    use common::darwin::{
        Association, CallKey, Deactivated, ScheduleCall, ScheduleFields, Service, StatusCall,
    };

    use super::*;

    // Fixture RIDs and UIDs carry a `Z9` prefix and dates in 2099, so the
    // cleanup can never touch real Darwin rows.
    const RID: &str = "209907108Z90001";
    const PREV_RID: &str = "209907108Z90002";
    const UID: &str = "Z90001";
    const PREV_UID: &str = "Z90002";

    fn ssd() -> NaiveDate {
        NaiveDate::from_ymd_opt(2099, 7, 10).expect("valid date")
    }

    fn ts(seconds: i64) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2099-07-10T08:00:00Z")
            .expect("valid instant")
            .with_timezone(&Utc)
            + Duration::seconds(seconds)
    }

    async fn cleanup(pool: &PgPool) {
        for sql in [
            "DELETE FROM darwin_calls WHERE rid LIKE '%8Z9%' AND ssd >= '2099-01-01'",
            "DELETE FROM darwin_associations WHERE main_rid LIKE '%8Z9%'",
            "DELETE FROM darwin_services WHERE rid LIKE '%8Z9%' AND ssd >= '2099-01-01'",
        ] {
            sqlx::query(sql).execute(pool).await.expect("cleanup");
        }
    }

    fn key(rid: &str, tiploc: &str, wtd: &str) -> CallKey {
        CallKey {
            rid: rid.to_string(),
            tiploc: tiploc.to_string(),
            wta: String::new(),
            wtd: wtd.to_string(),
            wtp: String::new(),
        }
    }

    fn service(rid: &str, uid: &str, at: i64, schedule: bool) -> Service {
        Service {
            rid: rid.to_string(),
            uid: uid.to_string(),
            ssd: ssd(),
            schedule: schedule.then(|| ScheduleFields {
                toc: Some("NT".to_string()),
                train_id: Some("2A01".to_string()),
                rsid: Some("NT123400".to_string()),
                is_passenger: Some(true),
                deleted: false,
                cancel_reason: None,
            }),
            ts: ts(at),
        }
    }

    fn status(at: i64, platform: Option<(&str, bool)>, dep_et: Option<&str>) -> StatusCall {
        StatusCall {
            key: key(RID, "YORK", "10:12"),
            ssd: ssd(),
            pta: None,
            ptd: Some("10:12".to_string()),
            platform: platform.map(|(number, confirmed)| Platform {
                number: number.to_string(),
                confirmed,
                suppressed: false,
                cis_suppressed: false,
                source: Some('A'),
            }),
            arrival: None,
            departure: dep_et.map(|et| Event {
                et: Some(et.to_string()),
                at: None,
                delayed: false,
            }),
            pass: None,
            suppressed: false,
            ts: ts(at),
        }
    }

    async fn apply_batch(pool: &PgPool, batch: &Batch) -> Applied {
        let mut tx = pool.begin().await.expect("begin");
        let applied = apply(&mut tx, batch).await.expect("apply");
        tx.commit().await.expect("commit");
        applied
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL=... cargo test -p ds-store \
                darwin -- --ignored --test-threads=1"]
    async fn stores_platforms_with_their_flags_and_joins_them_to_the_train() {
        let pool = crate::test_support::connect().await;
        cleanup(&pool).await;

        let schedule = Batch {
            services: vec![service(RID, UID, 0, true)],
            schedule_calls: vec![ScheduleCall {
                key: key(RID, "YORK", "10:12"),
                ssd: ssd(),
                pta: None,
                ptd: Some("10:12".to_string()),
                planned_platform: Some("3".to_string()),
                cancelled: false,
                ts: ts(0),
            }],
            ..Batch::default()
        };
        assert_eq!(apply_batch(&pool, &schedule).await.total(), 2);

        // A TS message confirms platform 5 (an alteration from 3).
        let ts_batch = Batch {
            services: vec![service(RID, UID, 10, false)],
            status_calls: vec![status(10, Some(("5", true)), Some("10:15"))],
            ..Batch::default()
        };
        apply_batch(&pool, &ts_batch).await;
        // A later TS message without a platform keeps it; its forecast wins.
        let later = Batch {
            status_calls: vec![status(20, None, Some("10:17"))],
            ..Batch::default()
        };
        apply_batch(&pool, &later).await;
        // An older replay changes nothing.
        let replay = Batch {
            status_calls: vec![status(5, Some(("9", false)), Some("10:12"))],
            ..Batch::default()
        };
        assert_eq!(apply_batch(&pool, &replay).await.calls, 0);

        let calls = calls_for_trains(&pool, &[(UID.to_string(), ssd())])
            .await
            .expect("read");
        let call = &calls[&(UID.to_string(), ssd())][0];
        assert_eq!(call.platform.as_deref(), Some("5"));
        assert!(call.platform_confirmed);
        assert_eq!(call.planned_platform.as_deref(), Some("3"));
        assert_eq!(call.platform_basis.as_deref(), Some("confirmed"));
        assert_eq!(call.departure_et.as_deref(), Some("10:17"));
        assert_eq!(call.wtd, "10:12");

        // The service kept its schedule fields through the TS-only update.
        let (toc, deactivated): (Option<String>, bool) =
            sqlx::query_as("SELECT toc, deactivated FROM darwin_services WHERE rid = $1")
                .bind(RID)
                .fetch_one(&pool)
                .await
                .expect("service");
        assert_eq!(toc.as_deref(), Some("NT"));
        assert!(!deactivated);

        apply_batch(
            &pool,
            &Batch {
                deactivated: vec![Deactivated {
                    rid: RID.to_string(),
                    ts: ts(30),
                }],
                ..Batch::default()
            },
        )
        .await;
        let deactivated: bool =
            sqlx::query_scalar("SELECT deactivated FROM darwin_services WHERE rid = $1")
                .bind(RID)
                .fetch_one(&pool)
                .await
                .expect("service");
        assert!(deactivated);

        cleanup(&pool).await;
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL=... cargo test -p ds-store \
                darwin -- --ignored --test-threads=1"]
    async fn a_suppressed_platform_reads_as_suppressed() {
        let pool = crate::test_support::connect().await;
        cleanup(&pool).await;
        let mut call = status(10, Some(("2", false)), None);
        if let Some(platform) = call.platform.as_mut() {
            platform.cis_suppressed = true;
        }
        apply_batch(
            &pool,
            &Batch {
                services: vec![service(RID, UID, 10, false)],
                status_calls: vec![call],
                ..Batch::default()
            },
        )
        .await;
        let calls = calls_for_trains(&pool, &[(UID.to_string(), ssd())])
            .await
            .expect("read");
        let call = &calls[&(UID.to_string(), ssd())][0];
        assert!(call.platform_hidden());
        assert_eq!(call.platform_basis.as_deref(), Some("suppressed"));
        cleanup(&pool).await;
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL=... cargo test -p ds-store \
                darwin -- --ignored --test-threads=1"]
    async fn a_next_working_association_is_a_previous_working_link() {
        let pool = crate::test_support::connect().await;
        cleanup(&pool).await;
        let association = |at: i64, cancelled: bool| Association {
            main_rid: PREV_RID.to_string(),
            assoc_rid: RID.to_string(),
            tiploc: "YORK".to_string(),
            category: "NP".to_string(),
            cancelled,
            deleted: false,
            main_wta: NaiveTime::from_hms_opt(10, 0, 0),
            main_wtd: None,
            main_wtp: None,
            assoc_wta: None,
            assoc_wtd: NaiveTime::from_hms_opt(10, 12, 0),
            assoc_wtp: None,
            main_ssd: Some(ssd()),
            ts: ts(at),
        };
        apply_batch(
            &pool,
            &Batch {
                services: vec![
                    service(RID, UID, 0, true),
                    service(PREV_RID, PREV_UID, 0, true),
                ],
                associations: vec![association(0, false)],
                ..Batch::default()
            },
        )
        .await;
        let link: (String, i16) = sqlx::query_as(
            "SELECT prev_uid, booked_turnaround_minutes FROM darwin_previous_workings \
             WHERE uid = $1 AND service_date = $2",
        )
        .bind(UID)
        .bind(ssd())
        .fetch_one(&pool)
        .await
        .expect("a link");
        assert_eq!(link, (PREV_UID.to_string(), 12));

        // A cancellation removes it; an older replay does not bring it back.
        apply_batch(
            &pool,
            &Batch {
                associations: vec![association(20, true)],
                ..Batch::default()
            },
        )
        .await;
        apply_batch(
            &pool,
            &Batch {
                associations: vec![association(10, false)],
                ..Batch::default()
            },
        )
        .await;
        let links: i64 =
            sqlx::query_scalar("SELECT count(*) FROM darwin_previous_workings WHERE uid = $1")
                .bind(UID)
                .fetch_one(&pool)
                .await
                .expect("count");
        assert_eq!(links, 0);
        cleanup(&pool).await;
    }
}
