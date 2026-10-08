//! `ds-migrate`: the schema migrator as its own binary (ingest architecture
//! spec §12.1, plan 1B.1). Built into the api image as
//! `/usr/local/bin/ds-migrate`; the chart's migrate hook Job
//! (`templates/migrate-job.yaml`, `migrate.job`) runs `ds-migrate run`.
//!
//! ```text
//!   MIGRATION_DATABASE_URL=postgres://owner@... ds-migrate run
//!   DATABASE_URL=postgres://...                 ds-migrate wait [--role writer]
//!   MIGRATION_DATABASE_URL=postgres://owner@... ds-migrate backfill-trains
//!   MIGRATION_DATABASE_URL=postgres://owner@... \
//!     ds-migrate backfill-line-train-summaries [--force] [--lines-dir DIR]
//! ```
//!
//! - `run` does exactly what the api does at startup while
//!   `api.migrateOnStartup` is on: `ds_store::migrate`'s
//!   `ensure_ready_for_contract_migration`, then `run` (the advisory lock,
//!   `lock_timeout` and `statement_timeout` from
//!   `MIGRATION_LOCK_TIMEOUT_SECS`/`MIGRATION_STATEMENT_TIMEOUT_SECS`, the
//!   INVALID-index heal, every pending migration). It connects with
//!   `MIGRATION_DATABASE_URL` when set and not blank, else `DATABASE_URL`,
//!   as the api does. Two connections at most: the contract check's pool
//!   (one) and the migration connection (spec §5.3). The first connection
//!   is retried (`common::startup`) for up to
//!   `MIGRATION_CONNECT_DEADLINE_SECS` (default 120), then exits 1.
//! - `wait` blocks until the database has this build's schema: the schema
//!   gate the DB services run in-process (`ds_store::schema`, spec §12.2,
//!   plan 1B.2), from outside a service, for a script, an init container
//!   or an operator. With `--role` (or `DS_MIGRATE_ROLE`) it also waits
//!   for the grants `db-grants.yaml` gives that service's role, checked as
//!   the `DATABASE_URL` user, exactly as that service's own gate does;
//!   without it, only for the migration. It polls every 5 s and fails
//!   after 15 minutes (`ds_store::schema::DEADLINE`); a connection that
//!   fails is one more failed poll, so it needs no connect deadline of its
//!   own. One connection. The
//!   chart does not run it: the migrate Job runs `run`, and each service
//!   gates itself.
//! - `backfill-trains` and `backfill-line-train-summaries` are the one-off
//!   data backfills that used to be the api binaries `backfill_trains` and
//!   `backfill_line_train_summaries` (ingest phase 5 prep, Q2 of
//!   docs/ingest-phase5-runbook.md: they must never run with the api's
//!   credentials). They connect like `run` (`MIGRATION_DATABASE_URL`, the
//!   schema owner, else `DATABASE_URL`, the first connection retried for
//!   up to `MIGRATION_CONNECT_DEADLINE_SECS`) and refuse the api's role. The
//!   logic is `ds_store::migrate::legacy_backfill::run_backfill` and
//!   `ds_store::schedule::summaries::backfill_all`; both are idempotent.
//!   The old api binaries still work, deprecated, until step 5.4b.
//!
//! Exit status 0 on success, 1 on any error (logged as one line).

use std::time::Duration;

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use common::secret::Secret;
use ds_store::migrate::{self, MigrationSettings};
use ds_store::schema::{self, DbRole, SchemaGate};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

#[derive(Debug, Parser)]
#[command(name = "ds-migrate", about = "Distant Signal's schema migrator")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Check the contract-migration precondition, then apply every pending
    /// migration embedded in this build.
    Run(RunArgs),
    /// Wait until the database has this build's schema and, with `--role`,
    /// that role's grants (the schema gate, plan 1B.2).
    Wait(WaitArgs),
    /// The shared-train-identity backfill (formerly the api's
    /// `backfill_trains`), as the schema owner. Idempotent; a no-op on a
    /// contracted database.
    BackfillTrains(RunArgs),
    /// Derive `line_train_summaries` for every stored population whose rows
    /// are missing or stale (formerly the api's
    /// `backfill_line_train_summaries`), as the schema owner. Idempotent.
    BackfillLineTrainSummaries(SummariesArgs),
}

#[derive(Debug, Args)]
struct SummariesArgs {
    #[command(flatten)]
    connection: RunArgs,
    /// Rewrite every population's rows, current or not.
    #[arg(long)]
    force: bool,
    /// The line catalogue that decides what "current" means: the deployed
    /// image's, as the api uses.
    #[arg(long, env = "LINES_DIR", default_value = "/app/lines")]
    lines_dir: String,
}

/// The connection `run` migrates with. See [`RunArgs::url`].
#[derive(Debug, Args)]
struct RunArgs {
    /// Used when `MIGRATION_DATABASE_URL` is unset or blank.
    #[arg(long, env, hide_env_values = true)]
    database_url: Option<Secret>,
    /// The schema owner (the chart's migrate Job sets only this).
    #[arg(long, env, hide_env_values = true)]
    migration_database_url: Option<Secret>,
    /// How long to keep retrying the first connection (a saturated node, a
    /// late `NetworkPolicy`) before exiting non-zero. Seconds, at least 1.
    #[arg(long, env, default_value_t = 120, value_parser = clap::value_parser!(u64).range(1..))]
    migration_connect_deadline_secs: u64,
}

impl RunArgs {
    /// `MIGRATION_DATABASE_URL` when set and not blank, else
    /// `DATABASE_URL` (`ds_store::migrate::migration_url`), and the
    /// variable it came from.
    fn url(&self) -> anyhow::Result<(&str, &'static str)> {
        let database_url = self.database_url.as_ref().map_or("", Secret::expose);
        let (url, var) = migrate::migration_url(
            database_url,
            self.migration_database_url.as_ref().map(Secret::expose),
        );
        anyhow::ensure!(
            !url.trim().is_empty(),
            "set {} (the schema owner) or DATABASE_URL",
            migrate::MIGRATION_DATABASE_URL_ENV
        );
        Ok((url, var))
    }
}

#[derive(Debug, Args)]
struct WaitArgs {
    /// The service's own connection; any role that may read
    /// `_sqlx_migrations`. The privileges are checked as this user.
    #[arg(long, env, hide_env_values = true)]
    database_url: Secret,
    /// Also wait for the grants `db-grants.yaml` gives this service's role.
    /// Without it, only for the migration.
    #[arg(long, env = "DS_MIGRATE_ROLE", value_enum)]
    role: Option<Role>,
}

/// The services that run the schema gate (`ds_store::schema::DbRole`), by
/// their key in `db-grants.yaml`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Role {
    Api,
    Aggregator,
    Enricher,
    Notifier,
    Writer,
}

impl From<Role> for DbRole {
    fn from(role: Role) -> Self {
        match role {
            Role::Api => Self::Api,
            Role::Aggregator => Self::Aggregator,
            Role::Enricher => Self::Enricher,
            Role::Notifier => Self::Notifier,
            Role::Writer => Self::Writer,
        }
    }
}

/// The application name `wait`'s connection reports in `pg_stat_activity`.
const WAIT_APPLICATION_NAME: &str = "ds-migrate-wait";

#[tokio::main]
async fn main() -> std::process::ExitCode {
    common::logging::exit_code(dispatch(Cli::parse()).await)
}

async fn dispatch(cli: Cli) -> anyhow::Result<()> {
    common::logging::init_with_filter(
        "ds-migrate",
        common::logging::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| common::logging::EnvFilter::new("info")),
    );
    match cli.command {
        Command::Run(args) => {
            let (url, var) = args.url()?;
            let options: PgConnectOptions = url
                .parse()
                .with_context(|| format!("could not parse {var}"))?;
            let deadline = Duration::from_secs(args.migration_connect_deadline_secs);
            run(options, MigrationSettings::from_env()?, deadline).await?;
            tracing::info!("migrations finished");
            Ok(())
        }
        Command::BackfillTrains(args) => backfill_trains(&args).await,
        Command::BackfillLineTrainSummaries(args) => backfill_line_train_summaries(&args).await,
        Command::Wait(args) => {
            let options: PgConnectOptions = args
                .database_url
                .expose()
                .parse()
                .context("could not parse DATABASE_URL")?;
            let applied = wait(options, &gate(args.role)).await?;
            tracing::info!(applied_migration = applied, "the schema is ready");
            Ok(())
        }
    }
}

/// The application name the backfills report in `pg_stat_activity`.
const BACKFILL_APPLICATION_NAME: &str = "ds-migrate-backfill";

/// A small pool for a backfill on [`RunArgs::url`], refusing the api role.
/// The first connection is retried for up to
/// `MIGRATION_CONNECT_DEADLINE_SECS` (`--migration-connect-deadline-secs`).
async fn backfill_pool(args: &RunArgs, tool: &str) -> anyhow::Result<sqlx::PgPool> {
    let (url, var) = args.url()?;
    let options: PgConnectOptions = url
        .parse()
        .with_context(|| format!("could not parse {var}"))?;
    let options = options
        .options(common::pg::DEAD_CLIENT_DETECTION_SETTINGS)
        .application_name(BACKFILL_APPLICATION_NAME);
    // The first connection is retried like `run`'s, for up to
    // `MIGRATION_CONNECT_DEADLINE_SECS`: these run as Jobs or one-off pods
    // on the same saturated node, through the same NetworkPolicy.
    let pool = common::startup::retry_until_ready_within(
        "postgres",
        common::startup::CONNECT_BACKOFF,
        Duration::from_secs(args.migration_connect_deadline_secs),
        || {
            PgPoolOptions::new()
                .max_connections(2)
                .connect_with(options.clone())
        },
    )
    .await
    .with_context(|| format!("could not connect with {var}"))?;
    let user = ds_store::maintenance::refuse_api_role(
        &pool,
        tool,
        "the schema owner (MIGRATION_DATABASE_URL)",
    )
    .await?;
    tracing::info!(user, "{tool}: connected");
    Ok(pool)
}

/// `ds-migrate backfill-trains`.
async fn backfill_trains(args: &RunArgs) -> anyhow::Result<()> {
    let pool = backfill_pool(args, "ds-migrate backfill-trains").await?;
    let report = migrate::legacy_backfill::run_backfill(&pool).await?;
    pool.close().await;
    tracing::info!(
        subscriptions_linked = report.subscriptions_linked,
        movement_events_linked = report.movement_events_linked,
        current_state_linked = report.current_state_linked,
        remaining_gaps = report.accepted_gaps,
        "backfill complete; remaining gaps carry no legacy identity, so the contract migration \
         loses nothing"
    );
    Ok(())
}

/// `ds-migrate backfill-line-train-summaries`.
async fn backfill_line_train_summaries(args: &SummariesArgs) -> anyhow::Result<()> {
    let lines = common::config::parse_lines(&args.lines_dir)?;
    anyhow::ensure!(
        !lines.is_empty(),
        "no line definitions found in {}: rows derived without the catalogue would carry no \
         on-line stops. Set LINES_DIR (or --lines-dir) to the repository's lines/ directory.",
        args.lines_dir
    );
    tracing::info!(count = lines.len(), lines_dir = %args.lines_dir, "loaded line catalogue");
    let pool = backfill_pool(&args.connection, "ds-migrate backfill-line-train-summaries").await?;
    let summary = ds_store::schedule::summaries::backfill_all(
        &pool,
        &lines,
        args.force,
        |line_id, service_date, rows, took| {
            tracing::info!(
                line_id,
                %service_date,
                rows,
                ms = took.as_millis(),
                "rewrote line_train_summaries"
            );
        },
    )
    .await?;
    pool.close().await;
    tracing::info!(
        populations = summary.populations,
        rewritten = summary.rewritten,
        rows = summary.rows,
        already_current = summary.current,
        "backfill complete"
    );
    Ok(())
}

/// The api's startup migration (`crates/api/src/main.rs`), on `options`:
/// the contract-migration guard (it MUST run before the migrations, see
/// `ds_store::migrate::contract`), then [`migrate::run`]. The guard's
/// connection, the first, is retried for up to `connect_deadline`
/// (`MIGRATION_CONNECT_DEADLINE_SECS`): the Job's `pg_isready` wait only
/// shows the server answers, and the connect itself can still be slow on a
/// saturated node.
async fn run(
    options: PgConnectOptions,
    settings: MigrationSettings,
    connect_deadline: Duration,
) -> anyhow::Result<()> {
    // Dead-client detection, as the api's migration connection has.
    let options = options.options(common::pg::DEAD_CLIENT_DETECTION_SETTINGS);
    let check_options = options
        .clone()
        .application_name(migrate::MIGRATION_APPLICATION_NAME);
    let check = common::startup::retry_until_ready_within(
        "postgres",
        common::startup::CONNECT_BACKOFF,
        connect_deadline,
        || {
            PgPoolOptions::new()
                .max_connections(1)
                .connect_with(check_options.clone())
        },
    )
    .await
    .context("could not connect for the contract-migration check")?;
    let ready = migrate::ensure_ready_for_contract_migration(&check).await;
    check.close().await;
    ready?;
    migrate::run(options, settings).await
}

/// The gate `wait` runs: this build's for `role`
/// ([`SchemaGate::for_role`]), or, with no role, the same with no
/// privileges (the migration only).
fn gate(role: Option<Role>) -> SchemaGate {
    match role {
        Some(role) => SchemaGate::for_role(role.into()),
        None => SchemaGate {
            privileges: &[],
            ..SchemaGate::for_role(DbRole::Api)
        },
    }
}

/// [`schema::wait_for_schema_with`] on one connection to `options`.
/// The pool is lazy, so a database that is not up yet is one more failed
/// check the gate retries until its deadline, not an immediate exit.
/// Returns the applied migration.
async fn wait(options: PgConnectOptions, gate: &SchemaGate) -> anyhow::Result<i64> {
    let options = options
        .options(common::pg::DEAD_CLIENT_DETECTION_SETTINGS)
        .application_name(WAIT_APPLICATION_NAME);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(schema::POLL_INTERVAL)
        .connect_lazy_with(options);
    let applied = schema::wait_for_schema_with(&pool, gate, None).await;
    pool.close().await;
    applied
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn the_cli_is_well_formed() {
        Cli::command().debug_assert();
    }

    /// The backfills moved here from the api image (phase 5 prep, Q2).
    #[test]
    fn the_backfill_subcommands_parse() {
        let cli = Cli::try_parse_from([
            "ds-migrate",
            "backfill-trains",
            "--migration-database-url",
            "postgres://owner@h/d",
        ])
        .unwrap();
        let Command::BackfillTrains(args) = cli.command else {
            panic!("expected backfill-trains");
        };
        assert_eq!(args.url().unwrap().0, "postgres://owner@h/d");

        let cli = Cli::try_parse_from([
            "ds-migrate",
            "backfill-line-train-summaries",
            "--force",
            "--lines-dir",
            "/x",
            "--database-url",
            "postgres://h/d",
        ])
        .unwrap();
        let Command::BackfillLineTrainSummaries(args) = cli.command else {
            panic!("expected backfill-line-train-summaries");
        };
        assert!(args.force);
        assert_eq!(args.lines_dir, "/x");
    }

    fn run_args(database_url: Option<&str>, migration_database_url: Option<&str>) -> RunArgs {
        RunArgs {
            database_url: database_url.map(Secret::from),
            migration_database_url: migration_database_url.map(Secret::from),
            migration_connect_deadline_secs: 120,
        }
    }

    #[test]
    fn run_prefers_the_migration_url_and_falls_back_to_database_url() {
        let app = "postgres://app@db/ds";
        let owner = "postgres://owner@db/ds";
        let both = run_args(Some(app), Some(owner));
        assert_eq!(
            both.url().unwrap(),
            (owner, migrate::MIGRATION_DATABASE_URL_ENV)
        );
        // The chart's Job sets only MIGRATION_DATABASE_URL.
        let owner_only = run_args(None, Some(owner));
        assert_eq!(
            owner_only.url().unwrap(),
            (owner, migrate::MIGRATION_DATABASE_URL_ENV)
        );
        let blank_owner = run_args(Some(app), Some("  "));
        assert_eq!(blank_owner.url().unwrap(), (app, "DATABASE_URL"));
        assert!(run_args(None, None).url().is_err());
        assert!(run_args(Some(""), Some(" ")).url().is_err());
    }

    #[test]
    fn run_reads_the_variables_the_chart_sets() {
        let cli = Cli::try_parse_from(["ds-migrate", "run"]);
        // Parses with or without the variables set; the URL check is
        // `RunArgs::url`'s.
        assert!(cli.is_ok(), "{cli:?}");
        let envs: Vec<String> = Cli::command()
            .find_subcommand("run")
            .unwrap()
            .get_arguments()
            .filter_map(|arg| arg.get_env().map(|env| env.to_string_lossy().into_owned()))
            .collect();
        assert_eq!(
            envs,
            [
                "DATABASE_URL",
                "MIGRATION_DATABASE_URL",
                "MIGRATION_CONNECT_DEADLINE_SECS"
            ]
        );
    }

    /// The chart sets `MIGRATION_CONNECT_DEADLINE_SECS`
    /// (`migrate.job.connectDeadlineSecs`, default 120); 0 is refused.
    #[test]
    fn run_takes_a_positive_connect_deadline() {
        let deadline = |args: &[&str]| -> Result<u64, clap::Error> {
            match Cli::try_parse_from(args)?.command {
                Command::Run(args) => Ok(args.migration_connect_deadline_secs),
                _ => unreachable!(),
            }
        };
        assert_eq!(deadline(&["ds-migrate", "run"]).unwrap(), 120);
        let flag = "--migration-connect-deadline-secs";
        assert_eq!(deadline(&["ds-migrate", "run", flag, "30"]).unwrap(), 30);
        assert!(deadline(&["ds-migrate", "run", flag, "0"]).is_err());
    }

    /// The whole `run` path against an already-migrated database: the
    /// contract check passes and nothing is pending. With the role split
    /// (`scripts/test-postgres-roles.py`) it migrates as the owner in
    /// `MIGRATION_DATABASE_URL`.
    #[tokio::test]
    #[ignore = "requires a live, migrated database; run with `DATABASE_URL=... cargo test -p \
                ds-migrate -- --ignored --test-threads=1`"]
    async fn run_is_a_no_op_on_a_migrated_database() {
        let args = RunArgs {
            database_url: std::env::var("DATABASE_URL").ok().map(Secret::from),
            migration_database_url: std::env::var(migrate::MIGRATION_DATABASE_URL_ENV)
                .ok()
                .map(Secret::from),
            migration_connect_deadline_secs: 10,
        };
        let options: PgConnectOptions = args.url().unwrap().0.parse().unwrap();
        run(
            options,
            MigrationSettings::default(),
            Duration::from_secs(args.migration_connect_deadline_secs),
        )
        .await
        .expect("an up-to-date database migrates cleanly");
    }

    #[test]
    fn wait_takes_a_role_from_the_flag_or_the_environment() {
        let parse = |args: &[&str]| match Cli::try_parse_from(args).unwrap().command {
            Command::Wait(args) => args.role,
            _ => unreachable!(),
        };
        let url = "--database-url=postgres://unused";
        assert_eq!(
            parse(&["ds-migrate", "wait", url, "--role", "writer"]),
            Some(Role::Writer)
        );
        assert!(Cli::try_parse_from(["ds-migrate", "wait", url, "--role", "owner"]).is_err());
        let envs: Vec<String> = Cli::command()
            .find_subcommand("wait")
            .unwrap()
            .get_arguments()
            .filter_map(|arg| arg.get_env().map(|env| env.to_string_lossy().into_owned()))
            .collect();
        assert_eq!(envs, ["DATABASE_URL", "DS_MIGRATE_ROLE"]);
    }

    #[test]
    fn every_role_maps_to_its_grants_key() {
        use clap::ValueEnum;
        for role in Role::value_variants() {
            let name = role.to_possible_value().unwrap();
            assert_eq!(DbRole::from(*role).key(), name.get_name());
        }
    }

    #[test]
    fn the_gate_checks_the_role_s_grants_or_only_the_migration() {
        let writer = gate(Some(Role::Writer));
        assert_eq!(writer.required_migration, schema::REQUIRED_MIGRATION);
        assert_eq!(writer.privileges, DbRole::Writer.required_privileges());
        assert!(!writer.privileges.is_empty());
        let none = gate(None);
        assert_eq!(none.required_migration, schema::REQUIRED_MIGRATION);
        assert!(none.privileges.is_empty());
        assert_eq!(none.deadline, schema::DEADLINE);
    }

    fn database_options() -> PgConnectOptions {
        std::env::var("DATABASE_URL")
            .expect("DATABASE_URL must be set to run this test")
            .parse()
            .unwrap()
    }

    /// `wait` returns at once on a migrated database, for the migration
    /// alone and with each role's grants as the `DATABASE_URL` user (CI:
    /// the superuser, then the role-split app role every service role is a
    /// member of).
    #[tokio::test]
    #[ignore = "requires a live, migrated database; run with `DATABASE_URL=... cargo test -p \
                ds-migrate -- --ignored --test-threads=1`"]
    async fn wait_passes_on_a_migrated_database() {
        use clap::ValueEnum;
        let roles = std::iter::once(None).chain(Role::value_variants().iter().copied().map(Some));
        for role in roles {
            let gate = SchemaGate {
                deadline: Duration::ZERO,
                ..gate(role)
            };
            let applied = wait(database_options(), &gate)
                .await
                .unwrap_or_else(|err| panic!("{role:?}: {err:#}"));
            assert!(applied >= schema::REQUIRED_MIGRATION, "{role:?}: {applied}");
        }
    }

    /// A migration newer than the database has fails at the deadline, and
    /// says which migration it needed.
    #[tokio::test]
    #[ignore = "requires a live, migrated database; run with `DATABASE_URL=... cargo test -p \
                ds-migrate -- --ignored --test-threads=1`"]
    async fn wait_fails_at_the_deadline_on_an_older_schema() {
        let gate = SchemaGate {
            required_migration: i64::MAX,
            deadline: Duration::ZERO,
            ..gate(None)
        };
        let err = wait(database_options(), &gate).await.unwrap_err();
        assert!(err.to_string().contains(&i64::MAX.to_string()), "{err:#}");
    }
}
