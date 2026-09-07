//! The shared-train-identity expand/contract migration's **backfill**, and
//! the **startup precondition** that stops the contract migration from
//! running before it.
//!
//! Background
//! ----------
//! `docs/superpowers/specs/2026-09-06-shared-train-identity-design.md` §2
//! is a textbook expand/contract sequence:
//!
//! * **Step A** (`migrations/20260906100000_trains.sql`) -- additive: create
//!   `trains`, add a nullable `tracked_trains.trains_id`.
//! * **Step D part 1** (`migrations/20260906110000_train_movement_trains_id.sql`)
//!   -- additive: add a nullable `trains_id` to `train_movement_events` and
//!   `train_current_state`.
//! * **backfill** -- populate every one of those new `trains_id` columns for
//!   rows that pre-date the expand phase. **This module.**
//! * **Step D final** (`migrations/20260906140000_drop_legacy_columns.sql`)
//!   -- IRREVERSIBLE: drops `train_movement_events.tracked_train_id`,
//!   `train_current_state.tracked_train_id`, and seven legacy columns from
//!   `tracked_trains` (`train_uid` among them).
//!
//! The middle step is not something a migration can do for itself: it wants
//! `find_or_create_train`'s upsert semantics per row, it can take a long
//! time on a large table, and it must be re-runnable. It was originally
//! written as two `#[ignore]`d tests, run by hand against the development
//! database, and then deleted once they had served their purpose -- which
//! left a real deployment with pre-existing `tracked_trains` data no
//! runnable backfill at all, and the drop migration with no precondition
//! stopping it from silently discarding the only column that could still
//! recover the link. This module is that backfill's permanent home.
//!
//! ## REQUIRED DEPLOY SEQUENCE (database with pre-existing data)
//!
//! ```text
//!   1. Deploy a build whose migrations stop BEFORE 20260906140000, or
//!      let `api` start once against the expand-phase migrations only.
//!   2. DATABASE_URL=... cargo run -p api --bin backfill_trains
//!         (or `/usr/local/bin/backfill_trains` in the container image)
//!      -- idempotent, safe to run repeatedly, safe to re-run after a
//!      partial failure.
//!   3. Confirm it reports `remaining gaps: 0` for every table.
//!   4. Only then deploy the build that includes 20260906140000.
//! ```
//!
//! Step 4 is enforced, not merely documented:
//! [`ensure_ready_for_contract_migration`] runs in `main.rs` immediately
//! before `sqlx::migrate!().run(...)` and returns an error -- refusing to
//! start the server, and therefore refusing to apply the drop -- if
//! 20260906140000 has not yet been applied and unbackfilled rows still
//! exist. A database that has already applied it (every environment that
//! ran this plan's own manual backfill, development included) short-circuits
//! on its very first query and is completely unaffected.
//!
//! Everything here is schema-aware rather than schema-assuming: each phase
//! first asks the catalog whether the columns it needs still exist, and
//! no-ops with an explanatory log line when they don't. That is what lets
//! one binary be safe to run against a database at ANY point in the
//! sequence -- including a fully-contracted one, where there is by
//! definition nothing left to do.

use sqlx::PgPool;

/// The contract migration this module's whole precondition exists to gate.
/// Matches `_sqlx_migrations.version`, which sqlx derives from the
/// migration filename's own numeric prefix.
const CONTRACT_MIGRATION_VERSION: i64 = 20260906140000;

/// What one [`run_backfill`] pass did, plus what it could not do. Returned
/// (rather than only logged) so both the binary and this module's own tests
/// can assert on real numbers instead of scraping stdout.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BackfillReport {
    /// Subscription rows linked to a `trains` row by phase B.
    pub subscriptions_linked: u64,
    /// `train_movement_events` rows given a `trains_id` by phase D.
    pub movement_events_linked: u64,
    /// `train_current_state` rows given a `trains_id` by phase D.
    pub current_state_linked: u64,
    /// Rows this backfill could NOT link, because the legacy identity they
    /// would have been linked *from* is itself absent -- a subscription
    /// with no `train_uid` (never resolved), or a movement/state row whose
    /// owning subscription has no `trains_id` for the same reason. This is
    /// the design spec's own accepted §1/§2-Step-B gap, not a failure:
    /// there is no identity to recover, so nothing is lost by the drop.
    pub accepted_gaps: u64,
}

impl BackfillReport {
    /// Total rows this pass actually changed. `0` on a re-run against
    /// already-backfilled data is the idempotency property callers assert.
    pub fn total_linked(&self) -> u64 {
        self.subscriptions_linked + self.movement_events_linked + self.current_state_linked
    }
}

/// Does `column` exist on `table` right now? Every phase below gates on
/// this rather than on a migration version, so the backfill is correct
/// against a database at any point in the expand/contract sequence --
/// including one that is already fully contracted, where the honest answer
/// is "nothing to do".
///
/// Resolved through `to_regclass` + `pg_attribute` rather than
/// `information_schema` with a hard-coded `table_schema = 'public'`: this
/// must answer for exactly the table the queries below will actually hit,
/// which is whatever the connection's own `search_path` resolves the bare
/// name to. `attisdropped` is checked because Postgres keeps a tombstone
/// `pg_attribute` row for a dropped column -- without it, `train_uid` would
/// still "exist" on an already-contracted database.
async fn column_exists(pool: &PgPool, table: &str, column: &str) -> anyhow::Result<bool> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_attribute \
         WHERE attrelid = to_regclass($1) AND attname = $2 \
           AND attnum > 0 AND NOT attisdropped)",
    )
    .bind(table)
    .bind(column)
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

/// See [`column_exists`] on why this is `to_regclass` and not
/// `information_schema`. `to_regclass` returns `NULL` (rather than
/// erroring) for a name nothing in the `search_path` resolves.
async fn table_exists(pool: &PgPool, table: &str) -> anyhow::Result<bool> {
    let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(table)
        .fetch_one(pool)
        .await?;
    Ok(exists)
}

/// The per-subscription table's CURRENT name. `20260907100000_rename_tracked_trains.sql`
/// renames `tracked_trains` to `train_subscriptions`, and that rename lands
/// AFTER the contract migration this module gates -- so a database that
/// still needs a backfill is, by construction, still on the old name. Both
/// are resolved here so one binary works either way.
///
/// Returns `None` only if neither table exists, i.e. an entirely
/// unmigrated database -- nothing to back up, nothing to back fill.
async fn subscriptions_table(pool: &PgPool) -> anyhow::Result<Option<&'static str>> {
    if table_exists(pool, "train_subscriptions").await? {
        return Ok(Some("train_subscriptions"));
    }
    if table_exists(pool, "tracked_trains").await? {
        return Ok(Some("tracked_trains"));
    }
    Ok(None)
}

/// How many rows one batched phase processes per statement. Bounded so a
/// large production table is walked in chunks rather than held in one
/// enormous transaction -- the same 500 the original one-off jobs used.
const BATCH: i64 = 500;

/// Phase B: give every subscription row that already carries a resolved
/// `train_uid` a `trains` row and a `trains_id` pointing at it.
///
/// Recovered verbatim (`SELECT ... WHERE train_uid IS NOT NULL AND
/// trains_id IS NULL LIMIT 500`, then `find_or_create_train` +
/// `UPDATE ... SET trains_id`) from the deleted
/// `backfill_trains_id_for_resolved_rows`, commit `e0fec2b`. The upsert is
/// inlined here rather than calling `crate::data::trains::find_or_create_train`
/// only so this module needs no other module at all; the SQL is identical.
async fn backfill_subscriptions(pool: &PgPool, table: &str) -> anyhow::Result<u64> {
    if !column_exists(pool, table, "train_uid").await? {
        tracing::info!(
            table,
            "phase B skipped: the legacy train_uid column no longer exists, so every \
             subscription that could be linked already has been"
        );
        return Ok(0);
    }

    let mut linked = 0;
    loop {
        let rows: Vec<(i64, String, chrono::NaiveDate)> = sqlx::query_as(&format!(
            "SELECT id, train_uid, service_date FROM {table} \
             WHERE train_uid IS NOT NULL AND trains_id IS NULL \
             LIMIT {BATCH}"
        ))
        .fetch_all(pool)
        .await?;
        if rows.is_empty() {
            break;
        }
        for (id, train_uid, service_date) in &rows {
            let (trains_id,): (i64,) = sqlx::query_as(
                "INSERT INTO trains (train_uid, service_date) VALUES ($1, $2) \
                 ON CONFLICT (train_uid, service_date) DO UPDATE SET train_uid = EXCLUDED.train_uid \
                 RETURNING id",
            )
            .bind(train_uid)
            .bind(service_date)
            .fetch_one(pool)
            .await?;
            sqlx::query(&format!("UPDATE {table} SET trains_id = $2 WHERE id = $1"))
                .bind(id)
                .bind(trains_id)
                .execute(pool)
                .await?;
            linked += 1;
        }
    }
    Ok(linked)
}

/// Phase D: copy each subscription's (now-populated) `trains_id` down onto
/// the movement rows that still key off `tracked_train_id`.
///
/// Recovered verbatim from the deleted Step D job, commit `438ff2c`'s
/// `run_backfill_pass`. `tt.trains_id IS NOT NULL` in the join is
/// load-bearing, not defensive: a movement row under a subscription that
/// itself never resolved has no identity to inherit and must stay `NULL`
/// (the accepted gap [`BackfillReport::accepted_gaps`] counts).
async fn backfill_movement_table(
    pool: &PgPool,
    table: &str,
    subscriptions: &str,
) -> anyhow::Result<u64> {
    if !column_exists(pool, table, "tracked_train_id").await? {
        tracing::info!(
            table,
            "phase D skipped: the legacy tracked_train_id column no longer exists, so every \
             row that could be linked already has been"
        );
        return Ok(0);
    }

    let mut linked = 0;
    loop {
        let result = sqlx::query(&format!(
            "WITH batch AS ( \
                SELECT m.id, s.trains_id AS new_trains_id \
                FROM {table} m \
                JOIN {subscriptions} s ON s.id = m.tracked_train_id \
                WHERE m.trains_id IS NULL AND s.trains_id IS NOT NULL \
                LIMIT {BATCH} \
             ) \
             UPDATE {table} m SET trains_id = batch.new_trains_id \
             FROM batch WHERE m.id = batch.id"
        ))
        .execute(pool)
        .await?;
        linked += result.rows_affected();
        if result.rows_affected() == 0 {
            break;
        }
    }
    Ok(linked)
}

/// Counts rows that STILL have no `trains_id` after a backfill pass and
/// never can -- the accepted gap. Only meaningful while the legacy columns
/// still exist; `0` once they don't.
async fn count_accepted_gaps(pool: &PgPool, subscriptions: &str) -> anyhow::Result<u64> {
    let mut gaps: i64 = 0;
    if column_exists(pool, subscriptions, "train_uid").await? {
        gaps += sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {subscriptions} WHERE trains_id IS NULL AND train_uid IS NULL"
        ))
        .fetch_one(pool)
        .await?;
    }
    for table in ["train_movement_events", "train_current_state"] {
        if column_exists(pool, table, "tracked_train_id").await? {
            gaps += sqlx::query_scalar::<_, i64>(&format!(
                "SELECT count(*) FROM {table} WHERE trains_id IS NULL AND tracked_train_id IS NOT NULL"
            ))
            .fetch_one(pool)
            .await?;
        }
    }
    Ok(gaps as u64)
}

/// The whole backfill, in the only order that works: phase B first (it is
/// what gives phase D a `trains_id` to copy down), then phase D for both
/// movement tables.
///
/// Idempotent by construction -- every phase selects only rows whose
/// `trains_id IS NULL`, so a second call against already-backfilled data
/// selects nothing on its very first batch and returns
/// `total_linked() == 0`. Safe to re-run after a partial failure for the
/// same reason: whatever the failed run had already committed is simply not
/// selected again.
pub async fn run_backfill(pool: &PgPool) -> anyhow::Result<BackfillReport> {
    let Some(subscriptions) = subscriptions_table(pool).await? else {
        tracing::info!("no tracked_trains/train_subscriptions table exists; nothing to backfill");
        return Ok(BackfillReport::default());
    };

    let subscriptions_linked = backfill_subscriptions(pool, subscriptions).await?;
    let movement_events_linked =
        backfill_movement_table(pool, "train_movement_events", subscriptions).await?;
    let current_state_linked =
        backfill_movement_table(pool, "train_current_state", subscriptions).await?;
    let accepted_gaps = count_accepted_gaps(pool, subscriptions).await?;

    Ok(BackfillReport {
        subscriptions_linked,
        movement_events_linked,
        current_state_linked,
        accepted_gaps,
    })
}

/// Has migration [`CONTRACT_MIGRATION_VERSION`] already been applied? A
/// database with no `_sqlx_migrations` table at all is a brand-new one --
/// answered `false` here, which is correct and harmless: the phase checks
/// below then find no legacy columns either and the whole guard no-ops.
async fn contract_migration_applied(pool: &PgPool) -> anyhow::Result<bool> {
    if !table_exists(pool, "_sqlx_migrations").await? {
        return Ok(false);
    }
    let applied: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM _sqlx_migrations WHERE version = $1 AND success)",
    )
    .bind(CONTRACT_MIGRATION_VERSION)
    .fetch_one(pool)
    .await?;
    Ok(applied)
}

/// The safety precondition the drop migration itself could not carry.
///
/// `migrations/20260906140000_drop_legacy_columns.sql` has already been
/// applied to every environment this plan touched, and sqlx validates the
/// checksum of every applied migration on startup -- so editing that file to
/// add a `DO $$ ... RAISE EXCEPTION ...` block would break `sqlx migrate
/// run` on exactly the databases it was meant to protect, and would never
/// re-run on them anyway. A migration ADDED after it cannot gate it either:
/// migrations apply in version order, so a newer file always runs later.
/// The only place left that runs *before* `sqlx::migrate!()` is process
/// startup, which is where this lives.
///
/// Mirrors the plan's own Task 22 Step 1 dry-run query shape
/// (`SELECT count(*) FROM train_movement_events WHERE tracked_train_id IS
/// NOT NULL AND trains_id IS NULL`), extended to `train_current_state` and
/// to the subscription table's own `train_uid`/`trains_id` pair, and
/// deliberately scoped to `tracked_train_id IS NOT NULL` / `train_uid IS
/// NOT NULL` so the design's own accepted gaps (a row with no legacy
/// identity to recover) never block a deploy.
///
/// Returns `Ok(())` and does nothing at all -- one `information_schema`
/// query -- once the contract migration is applied.
pub async fn ensure_ready_for_contract_migration(pool: &PgPool) -> anyhow::Result<()> {
    if contract_migration_applied(pool).await? {
        return Ok(());
    }

    let Some(subscriptions) = subscriptions_table(pool).await? else {
        return Ok(());
    };

    let mut blockers: Vec<String> = Vec::new();

    if column_exists(pool, subscriptions, "train_uid").await? {
        let count: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM {subscriptions} \
             WHERE train_uid IS NOT NULL AND trains_id IS NULL"
        ))
        .fetch_one(pool)
        .await?;
        if count > 0 {
            blockers.push(format!("{subscriptions}: {count} row(s)"));
        }
    }

    for table in ["train_movement_events", "train_current_state"] {
        if column_exists(pool, table, "tracked_train_id").await?
            && column_exists(pool, table, "trains_id").await?
        {
            // `JOIN ... WHERE s.trains_id IS NOT NULL` -- NOT the plan's own
            // bare dry-run shape (`tracked_train_id IS NOT NULL AND
            // trains_id IS NULL`). A movement row whose owning subscription
            // has no `trains_id` EITHER is the design's own accepted gap
            // (§2 Step B's "named edge case": nothing ever learned that
            // subscription's `train_uid`, so there is no shared identity to
            // recover). The bare count includes those, which would block
            // every future deploy forever on rows the backfill can never
            // link -- so this counts only what a backfill run would
            // actually have fixed, which is precisely "not already an
            // accepted, understood gap".
            let count: i64 = sqlx::query_scalar(&format!(
                "SELECT count(*) FROM {table} m \
                 JOIN {subscriptions} s ON s.id = m.tracked_train_id \
                 WHERE m.trains_id IS NULL AND s.trains_id IS NOT NULL"
            ))
            .fetch_one(pool)
            .await?;
            if count > 0 {
                blockers.push(format!("{table}: {count} row(s)"));
            }
        }
    }

    if blockers.is_empty() {
        return Ok(());
    }

    anyhow::bail!(
        "refusing to start: migration {CONTRACT_MIGRATION_VERSION} \
         (drop_legacy_columns) has not been applied yet, and it would IRREVERSIBLY drop the \
         only columns that can still recover the shared-train identity of these rows -- {}. \
         Run the backfill first:  DATABASE_URL=... cargo run -p api --bin backfill_trains  \
         (idempotent, safe to re-run), confirm it reports no remaining gaps, then start this \
         binary again. See crates/api/src/data/legacy_backfill.rs's module doc for the full \
         deploy sequence.",
        blockers.join("; ")
    );
}

#[cfg(test)]
mod db_tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    async fn connect() -> PgPool {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres")
    }

    /// The development/CI database this suite runs against is already fully
    /// contracted (`20260906140000` applied, `tracked_train_id`/`train_uid`
    /// gone), so the ONLY honest way to test the backfill's real SQL is to
    /// rebuild the pre-contract shape in a throwaway schema and run the
    /// exact same functions against it. `search_path` is set on this pool's
    /// own connection to the fixture schema ALONE -- deliberately not
    /// `fixture, public` -- so every unqualified name in the functions
    /// under test resolves to a fixture table, nothing here can touch a
    /// real row, and the real `public._sqlx_migrations` is invisible, which
    /// is exactly what lets this fixture present as the pre-contract
    /// database it is simulating.
    ///
    /// The DDL below is copied from the migrations themselves
    /// (`20260828120000_train_tracking.sql` as of the expand phase, plus
    /// `20260906100000_trains.sql` and `20260906110000_train_movement_trains_id.sql`),
    /// reduced to the columns these functions actually read or write.
    async fn expand_phase_fixture(schema: &str) -> PgPool {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let admin = connect().await;
        sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .execute(&admin)
            .await
            .expect("drop fixture schema");
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .expect("create fixture schema");
        admin.close().await;

        let pool = PgPoolOptions::new()
            .after_connect({
                let schema = schema.to_string();
                move |conn, _| {
                    let schema = schema.clone();
                    Box::pin(async move {
                        sqlx::query(&format!("SET search_path TO {schema}"))
                            .execute(&mut *conn)
                            .await?;
                        Ok(())
                    })
                }
            })
            .connect(&database_url)
            .await
            .expect("connect to the fixture schema");

        for ddl in [
            "CREATE TABLE trains ( \
                 id BIGSERIAL PRIMARY KEY, \
                 train_uid TEXT NOT NULL, \
                 service_date DATE NOT NULL, \
                 UNIQUE (train_uid, service_date))",
            "CREATE TABLE tracked_trains ( \
                 id BIGSERIAL PRIMARY KEY, \
                 service_date DATE NOT NULL, \
                 train_uid TEXT, \
                 trains_id BIGINT REFERENCES trains(id))",
            "CREATE TABLE train_movement_events ( \
                 id BIGSERIAL PRIMARY KEY, \
                 tracked_train_id BIGINT REFERENCES tracked_trains(id), \
                 dedup_key TEXT NOT NULL, \
                 trains_id BIGINT REFERENCES trains(id))",
            "CREATE TABLE train_current_state ( \
                 id BIGSERIAL PRIMARY KEY, \
                 tracked_train_id BIGINT REFERENCES tracked_trains(id), \
                 status TEXT NOT NULL DEFAULT 'en_route', \
                 trains_id BIGINT REFERENCES trains(id))",
        ] {
            sqlx::query(ddl)
                .execute(&pool)
                .await
                .expect("create fixture table");
        }
        pool
    }

    async fn drop_fixture(schema: &str) {
        let admin = connect().await;
        sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .execute(&admin)
            .await
            .expect("drop fixture schema");
        admin.close().await;
    }

    /// The whole of Fix 1's claim, end to end: seed the exact pre-existing
    /// legacy shape the deleted one-off jobs were written for, run the
    /// backfill binary's own entry point, and prove (a) every linkable row
    /// is linked, (b) the accepted-gap rows are left alone, (c) a second
    /// run changes nothing, and (d) the startup guard flips from "refuse"
    /// to "allow" as a direct result.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                run_backfill_links_pre_existing_legacy_rows -- --ignored --test-threads=1`"]
    async fn run_backfill_links_pre_existing_legacy_rows() {
        let schema = "backfill_fixture_a";
        let pool = expand_phase_fixture(schema).await;
        let service_date: chrono::NaiveDate = "2026-09-06".parse().unwrap();

        // A resolved legacy subscription: has a train_uid, no trains_id.
        let (resolved_sub,): (i64,) = sqlx::query_as(
            "INSERT INTO tracked_trains (service_date, train_uid) VALUES ($1, 'BACKFILL-UID-1') \
             RETURNING id",
        )
        .bind(service_date)
        .fetch_one(&pool)
        .await
        .expect("seed resolved subscription");

        // A SECOND subscriber sharing the exact same physical train -- the
        // scenario the shared `trains` table exists for. Both must end up
        // pointing at ONE trains row.
        let (shared_sub,): (i64,) = sqlx::query_as(
            "INSERT INTO tracked_trains (service_date, train_uid) VALUES ($1, 'BACKFILL-UID-1') \
             RETURNING id",
        )
        .bind(service_date)
        .fetch_one(&pool)
        .await
        .expect("seed a second subscriber for the same train");

        // The accepted gap: never resolved, so no train_uid to link from.
        let (unresolved_sub,): (i64,) = sqlx::query_as(
            "INSERT INTO tracked_trains (service_date, train_uid) VALUES ($1, NULL) RETURNING id",
        )
        .bind(service_date)
        .fetch_one(&pool)
        .await
        .expect("seed unresolved subscription");

        for (sub, dedup) in [
            (resolved_sub, "backfill-dedup-1"),
            (unresolved_sub, "backfill-dedup-gap"),
        ] {
            sqlx::query(
                "INSERT INTO train_movement_events (tracked_train_id, dedup_key) VALUES ($1, $2)",
            )
            .bind(sub)
            .bind(dedup)
            .execute(&pool)
            .await
            .expect("seed movement event");
            sqlx::query("INSERT INTO train_current_state (tracked_train_id) VALUES ($1)")
                .bind(sub)
                .execute(&pool)
                .await
                .expect("seed current state");
        }

        // RED, proven not assumed: the startup guard must refuse right now.
        let refusal = ensure_ready_for_contract_migration(&pool)
            .await
            .expect_err("the guard must refuse while unbackfilled rows exist");
        let refusal = format!("{refusal}");
        assert!(
            refusal.contains("refusing to start") && refusal.contains("backfill_trains"),
            "the refusal must name the fix; got: {refusal}"
        );

        // --- the backfill itself ---
        let report = run_backfill(&pool).await.expect("first backfill run");
        assert_eq!(
            report.subscriptions_linked, 2,
            "both subscribers sharing the train must be linked"
        );
        assert_eq!(report.movement_events_linked, 1);
        assert_eq!(report.current_state_linked, 1);
        assert_eq!(
            report.accepted_gaps, 3,
            "the unresolved subscription plus its own two movement rows stay unlinked"
        );

        let (a, b): (Option<i64>, Option<i64>) = {
            let a: (Option<i64>,) =
                sqlx::query_as("SELECT trains_id FROM tracked_trains WHERE id = $1")
                    .bind(resolved_sub)
                    .fetch_one(&pool)
                    .await
                    .expect("read back linked subscription");
            let b: (Option<i64>,) =
                sqlx::query_as("SELECT trains_id FROM tracked_trains WHERE id = $1")
                    .bind(shared_sub)
                    .fetch_one(&pool)
                    .await
                    .expect("read back the second subscriber");
            (a.0, b.0)
        };
        assert!(a.is_some());
        assert_eq!(
            a, b,
            "two subscribers sharing one train must share one trains row"
        );

        let (trains_rows,): (i64,) =
            sqlx::query_as("SELECT count(*) FROM trains WHERE train_uid = 'BACKFILL-UID-1'")
                .fetch_one(&pool)
                .await
                .expect("count trains rows");
        assert_eq!(
            trains_rows, 1,
            "exactly one shared trains row, not one per subscriber"
        );

        let (event_trains_id,): (Option<i64>,) = sqlx::query_as(
            "SELECT trains_id FROM train_movement_events WHERE dedup_key = 'backfill-dedup-1'",
        )
        .fetch_one(&pool)
        .await
        .expect("read back movement event");
        assert_eq!(event_trains_id, a);

        let (gap_event_trains_id,): (Option<i64>,) = sqlx::query_as(
            "SELECT trains_id FROM train_movement_events WHERE dedup_key = 'backfill-dedup-gap'",
        )
        .fetch_one(&pool)
        .await
        .expect("read back the accepted-gap movement event");
        assert_eq!(
            gap_event_trains_id, None,
            "a row under an unresolved subscription has no identity to inherit"
        );

        // --- GREEN on idempotency: the SAME function, again, no reset ---
        let second = run_backfill(&pool).await.expect("second backfill run");
        assert_eq!(
            second.total_linked(),
            0,
            "re-running against already-backfilled data must be a true no-op"
        );
        assert_eq!(
            second.accepted_gaps, 3,
            "the accepted gaps are stable, not growing"
        );

        let (trains_rows_after,): (i64,) =
            sqlx::query_as("SELECT count(*) FROM trains WHERE train_uid = 'BACKFILL-UID-1'")
                .fetch_one(&pool)
                .await
                .expect("count trains rows after the second run");
        assert_eq!(trains_rows_after, 1, "no duplicate trains row on a re-run");

        // GREEN: the guard now lets the contract migration through.
        ensure_ready_for_contract_migration(&pool)
            .await
            .expect("the guard must allow the drop once the backfill has run");

        pool.close().await;
        drop_fixture(schema).await;
    }

    /// The real development/CI database: already fully contracted, so the
    /// guard's very first query short-circuits and the backfill has nothing
    /// left to find. This is the case every existing environment is in, and
    /// it must cost nothing and change nothing.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                a_fully_contracted_database_is_ready_and_has_nothing_to_backfill -- --ignored \
                --test-threads=1`"]
    async fn a_fully_contracted_database_is_ready_and_has_nothing_to_backfill() {
        let pool = connect().await;
        assert!(
            contract_migration_applied(&pool)
                .await
                .expect("check migration"),
            "precondition: this suite's database has already applied 20260906140000"
        );
        ensure_ready_for_contract_migration(&pool)
            .await
            .expect("an already-contracted database must never be blocked");

        let report = run_backfill(&pool).await.expect("run_backfill");
        assert_eq!(
            report.total_linked(),
            0,
            "there is nothing left to link once the legacy columns are gone"
        );
        assert_eq!(report.accepted_gaps, 0);
    }
}
