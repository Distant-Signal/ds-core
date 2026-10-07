//! `ds-migrate`: the schema migrator as its own binary (ingest architecture
//! spec §12.1, plan 1B.1). Built into the api image as
//! `/usr/local/bin/ds-migrate`; the chart's migrate hook Job
//! (`templates/migrate-job.yaml`, `migrate.job`) runs `ds-migrate run`.
//!
//! ```text
//!   MIGRATION_DATABASE_URL=postgres://owner@... ds-migrate run
//!   DATABASE_URL=postgres://...                 ds-migrate wait [--role writer]
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
//!   (one) and the migration connection (spec §5.3).
//! - `wait` blocks until the database has this build's schema: the schema
//!   gate the DB services run in-process (`ds_store::schema`, spec §12.2,
//!   plan 1B.2), from outside a service, for a script, an init container
//!   or an operator. With `--role` (or `DS_MIGRATE_ROLE`) it also waits
//!   for the grants `db-grants.yaml` gives that service's role, checked as
//!   the `DATABASE_URL` user, exactly as that service's own gate does;
//!   without it, only for the migration. It polls every 5 s and fails
//!   after 15 minutes (`ds_store::schema::DEADLINE`). One connection. The
//!   chart does not run it: the migrate Job runs `run`, and each service
//!   gates itself.
//!
//! Exit status 0 on success, 1 on any error (logged as one line).

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
            run(options, MigrationSettings::from_env()?).await?;
            tracing::info!("migrations finished");
            Ok(())
        }
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

/// The api's startup migration (`crates/api/src/main.rs`), on `options`:
/// the contract-migration guard (it MUST run before the migrations, see
/// `ds_store::migrate::contract`), then [`migrate::run`].
async fn run(options: PgConnectOptions, settings: MigrationSettings) -> anyhow::Result<()> {
    // Dead-client detection, as the api's migration connection has.
    let options = options.options(common::pg::DEAD_CLIENT_DETECTION_SETTINGS);
    let check = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            options
                .clone()
                .application_name(migrate::MIGRATION_APPLICATION_NAME),
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

    fn run_args(database_url: Option<&str>, migration_database_url: Option<&str>) -> RunArgs {
        RunArgs {
            database_url: database_url.map(Secret::from),
            migration_database_url: migration_database_url.map(Secret::from),
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
        assert_eq!(envs, ["DATABASE_URL", "MIGRATION_DATABASE_URL"]);
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
        };
        let options: PgConnectOptions = args.url().unwrap().0.parse().unwrap();
        run(options, MigrationSettings::default())
            .await
            .expect("an up-to-date database migrates cleanly");
    }

    #[test]
    fn wait_takes_a_role_from_the_flag_or_the_environment() {
        let parse = |args: &[&str]| match Cli::try_parse_from(args).unwrap().command {
            Command::Wait(args) => args.role,
            Command::Run(_) => unreachable!(),
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
                deadline: std::time::Duration::ZERO,
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
            deadline: std::time::Duration::ZERO,
            ..gate(None)
        };
        let err = wait(database_options(), &gate).await.unwrap_err();
        assert!(err.to_string().contains(&i64::MAX.to_string()), "{err:#}");
    }
}
