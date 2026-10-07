//! The startup precondition for the shared-train-identity contract
//! migration (`20260906140000_drop_legacy_columns.sql`): moved from
//! `api::data::legacy_backfill` (plan task 1B.1), which keeps the backfill
//! itself (`run_backfill`, the `backfill_trains` binary) and re-exports
//! [`ensure_ready_for_contract_migration`]. See that module's doc for the
//! required deploy sequence.
//!
//! [`table_exists`], [`column_exists`] and [`subscriptions_table`] are
//! public because the backfill asks the same catalog questions.

use sqlx::PgPool;

/// The contract migration this module's whole precondition exists to gate.
/// Matches `_sqlx_migrations.version`, which sqlx derives from the
/// migration filename's own numeric prefix.
pub const CONTRACT_MIGRATION_VERSION: i64 = 20_260_906_140_000;

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
pub async fn column_exists(pool: &PgPool, table: &str, column: &str) -> anyhow::Result<bool> {
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
pub async fn table_exists(pool: &PgPool, table: &str) -> anyhow::Result<bool> {
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
pub async fn subscriptions_table(pool: &PgPool) -> anyhow::Result<Option<&'static str>> {
    if table_exists(pool, "train_subscriptions").await? {
        return Ok(Some("train_subscriptions"));
    }
    if table_exists(pool, "tracked_trains").await? {
        return Ok(Some("tracked_trains"));
    }
    Ok(None)
}

/// Has migration [`CONTRACT_MIGRATION_VERSION`] already been applied? A
/// database with no `_sqlx_migrations` table at all is a brand-new one --
/// answered `false` here, which is correct and harmless: the phase checks
/// below then find no legacy columns either and the whole guard no-ops.
pub async fn contract_migration_applied(pool: &PgPool) -> anyhow::Result<bool> {
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
