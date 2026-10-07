//! `ds-migrate`: the schema migrator as its own binary (ingest architecture
//! spec §12.1, plan 1B.1). Built into the api image as
//! `/usr/local/bin/ds-migrate`; the chart's migrate hook Job
//! (`templates/migrate-job.yaml`, `migrate.job`) runs `ds-migrate run`.
//!
//! ```text
//!   MIGRATION_DATABASE_URL=postgres://owner@... ds-migrate run
//!   DATABASE_URL=postgres://...                 ds-migrate wait
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
//! - `wait` will block until the database has this build's schema
//!   (`ds_store::schema::wait_for_schema`, plan 1B.2). Until 1B.2 lands it
//!   fails.
//!
//! Exit status 0 on success, 1 on any error (logged as one line).

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use common::secret::Secret;
use ds_store::migrate::{self, MigrationSettings};
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
    /// Wait until the database has this build's schema (plan 1B.2).
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
    /// `_sqlx_migrations`.
    #[arg(long, env, hide_env_values = true)]
    database_url: Secret,
}

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
        Command::Wait(args) => wait(&args),
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

/// TODO(1B.2): `ds_store::schema::wait_for_schema` does not exist yet
/// (another task adds it to `crates/ds-store/src/schema.rs`). Replace the
/// body with a pool on `args.database_url` (one connection) and
/// `ds_store::schema::wait_for_schema(&pool, <its deadline>)` (making this
/// `async` again), so `wait` returns once `_sqlx_migrations` reaches
/// `ds_store::schema::REQUIRED_MIGRATION`. Nothing calls `wait` yet: the
/// chart's Job runs `run`.
fn wait(_args: &WaitArgs) -> anyhow::Result<()> {
    anyhow::bail!(
        "`ds-migrate wait` needs ds_store::schema::wait_for_schema (plan 1B.2), which this \
         build does not have yet; this build needs migration {}",
        ds_store::schema::REQUIRED_MIGRATION
    )
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
    fn wait_fails_until_plan_1b2() {
        let err = wait(&WaitArgs {
            database_url: Secret::from("postgres://unused"),
        })
        .unwrap_err();
        assert!(err.to_string().contains("1B.2"), "{err}");
    }
}
