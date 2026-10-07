//! The pool for any DB service: `common::pg::PoolSettings` wrapped so it
//! records `db_pool_*` (spec §14.1), and the DB health probe that was
//! `api/src/data/db_health.rs` (plan task 1A.11).
//!
//! # Pool metrics
//!
//! All prefixed `distant_signal_` ([`common::metrics::metric_name`]). No
//! labels beyond `state`, which has two values, so the cardinality is fixed.
//! The scrape's target labels (`job`, `pod`) say which service a series
//! belongs to.
//!
//! | Metric | Kind | Labels | From |
//! |---|---|---|---|
//! | `db_pool_connections` | gauge | `state` (`idle`, `in_use`) | [`sample`], every [`SAMPLE_INTERVAL`] |
//! | `db_pool_max_connections` | gauge | | [`sample`] (`PoolOptions::get_max_connections`) |
//! | `db_pool_acquire_seconds` | histogram | | [`acquire`] and [`begin`] |
//! | `db_pool_acquire_timeouts_total` | counter | | [`acquire`] and [`begin`], on `PoolTimedOut` |
//!
//! `idle + in_use` is the pool's size (`PgPool::size`).
//!
//! `db_pool_acquire_seconds` has buckets from 1 ms to 5 s
//! (`common::metrics::SHARED_BUCKETS`), which `common::metrics::install`
//! and the api's recorder both apply; without them the exporter renders a
//! summary.
//!
//! **Acquire time is only seen through [`acquire`] and [`begin`].** sqlx has
//! no hook at the start of an acquire, so a query run straight on the pool
//! (`.execute(&pool)`) is not timed. Today the only caller is the health
//! probe, which acquires through [`acquire`] every [`PROBE_INTERVAL`]: a
//! regular sample of how long a connection takes to get. The direct writers
//! (phase 2) use the wrappers.
//!
//! **Why** (spec §14.1): the api exported no pool metrics, so a saturated
//! pool showed only as 503s. Phase 0b gives each service a connection limit
//! and phase 1B resizes the pools; these come first so both can be judged.
//!
//! # The health probe
//!
//! [`DbHealth`]: a cheap `SELECT 1` through the pool every
//! [`PROBE_INTERVAL`], exported as a 0/1 gauge plus a failure counter. The
//! caller names the series (the api keeps `api_db_up` and
//! `api_db_probe_failures_total`, which the chart's
//! `DistantSignalApiDatabaseDown` reads). Going through the pool, not a
//! dedicated connection, is deliberate: a pool that cannot hand out a
//! connection within [`PROBE_TIMEOUT`] is as unusable to the service as a
//! database that is down.

use std::ops::{Deref, DerefMut};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use common::metrics::metric_name;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgConnectOptions, PgPool};
use sqlx::{Postgres, Transaction};

/// `db_pool_connections{state}`: the pool's connections, idle or in use.
pub const POOL_CONNECTIONS_METRIC: &str = "db_pool_connections";
/// `db_pool_max_connections`: the pool's configured maximum.
pub const POOL_MAX_CONNECTIONS_METRIC: &str = "db_pool_max_connections";
/// `db_pool_acquire_seconds`: how long [`acquire`] and [`begin`] waited.
pub const POOL_ACQUIRE_SECONDS_METRIC: &str = "db_pool_acquire_seconds";
/// `db_pool_acquire_timeouts_total`: acquires that hit `acquire_timeout`.
pub const POOL_ACQUIRE_TIMEOUTS_METRIC: &str = "db_pool_acquire_timeouts_total";

/// How often a pool built by [`PoolSettings::connect_with`] is sampled.
pub const SAMPLE_INTERVAL: Duration = Duration::from_secs(15);

/// How often the health probe runs.
pub const PROBE_INTERVAL: Duration = Duration::from_secs(15);
/// How long one probe (acquire + `SELECT 1`) may take before it counts as a
/// failure. Above the pool's default 5s acquire timeout, so a busy pool
/// gets its full wait first.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// [`common::pg::PoolSettings`] (application name, timeouts, size; see
/// there), plus the `db_pool_*` metrics for the pools it builds. Derefs to
/// the inner settings, so its fields and methods are all reachable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolSettings(common::pg::PoolSettings);

impl PoolSettings {
    /// The defaults, without reading the environment.
    pub fn new(application_name: &str, max_connections: u32) -> Self {
        Self(common::pg::PoolSettings::new(
            application_name,
            max_connections,
        ))
    }

    /// The defaults overridden by the `DATABASE_*` variables; see
    /// [`common::pg::PoolSettings::from_env`].
    pub fn from_env(application_name: &str, default_max_connections: u32) -> Result<Self> {
        common::pg::PoolSettings::from_env(application_name, default_max_connections).map(Self)
    }

    /// The wrapped settings.
    pub fn into_inner(self) -> common::pg::PoolSettings {
        self.0
    }

    /// Connects a pool exactly as `common::pg::PoolSettings` does (its
    /// `pool_options` with its `connect_options` on top of `base`), then
    /// spawns [`sample_loop`] for it. Needs a Tokio runtime.
    ///
    /// The gauges are written through the global recorder on each tick, so
    /// a pool built before the recorder is installed (the api builds its
    /// pool before `axum-prometheus` installs one) still reports from the
    /// next tick on. Call [`register_metrics`] once the recorder exists so
    /// every series is there at 0 before the first tick.
    pub async fn connect_with(&self, base: PgConnectOptions) -> sqlx::Result<PgPool> {
        let pool = self
            .0
            .pool_options()
            .connect_with(self.0.connect_options(base))
            .await?;
        tokio::spawn(sample_loop(pool.clone()));
        Ok(pool)
    }

    /// Parses `database_url`, then [`Self::connect_with`].
    pub async fn connect(&self, database_url: &str) -> Result<PgPool> {
        let base: PgConnectOptions = database_url
            .parse()
            .context("could not parse DATABASE_URL")?;
        self.connect_with(base)
            .await
            .context("could not connect to the database")
    }
}

impl From<common::pg::PoolSettings> for PoolSettings {
    fn from(settings: common::pg::PoolSettings) -> Self {
        Self(settings)
    }
}

impl Deref for PoolSettings {
    type Target = common::pg::PoolSettings;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PoolSettings {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Registers every `db_pool_*` series at 0: both `db_pool_connections`
/// states, `db_pool_max_connections` and `db_pool_acquire_timeouts_total`
/// (so an alert's `increase()` sees the first timeout). The histogram has
/// no zero value to register; it appears with its first acquire.
pub fn register_metrics() {
    for state in ["idle", "in_use"] {
        metrics::gauge!(metric_name(POOL_CONNECTIONS_METRIC), "state" => state).set(0.0);
    }
    metrics::gauge!(metric_name(POOL_MAX_CONNECTIONS_METRIC)).set(0.0);
    metrics::counter!(metric_name(POOL_ACQUIRE_TIMEOUTS_METRIC)).increment(0);
}

/// Sets the `db_pool_connections` and `db_pool_max_connections` gauges
/// from `pool` now.
pub fn sample(pool: &PgPool) {
    let size = pool.size();
    let idle = u32::try_from(pool.num_idle()).unwrap_or(u32::MAX).min(size);
    metrics::gauge!(metric_name(POOL_CONNECTIONS_METRIC), "state" => "idle").set(f64::from(idle));
    metrics::gauge!(metric_name(POOL_CONNECTIONS_METRIC), "state" => "in_use")
        .set(f64::from(size - idle));
    metrics::gauge!(metric_name(POOL_MAX_CONNECTIONS_METRIC))
        .set(f64::from(pool.options().get_max_connections()));
}

/// [`sample`] every [`SAMPLE_INTERVAL`] until the pool is closed.
pub async fn sample_loop(pool: PgPool) {
    let mut interval = tokio::time::interval(SAMPLE_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    while !pool.is_closed() {
        interval.tick().await;
        sample(&pool);
    }
}

/// `pool.acquire()`, timed into `db_pool_acquire_seconds`; a
/// `PoolTimedOut` also counts in `db_pool_acquire_timeouts_total`.
pub async fn acquire(pool: &PgPool) -> sqlx::Result<PoolConnection<Postgres>> {
    let started = Instant::now();
    let result = pool.acquire().await;
    observe_acquire(started, &result);
    result
}

/// `pool.begin()`, timed like [`acquire`]. The time includes the `BEGIN`
/// round trip (sqlx acquires and begins in one call).
pub async fn begin(pool: &PgPool) -> sqlx::Result<Transaction<'static, Postgres>> {
    let started = Instant::now();
    let result = pool.begin().await;
    observe_acquire(started, &result);
    result
}

fn observe_acquire<T>(started: Instant, result: &sqlx::Result<T>) {
    metrics::histogram!(metric_name(POOL_ACQUIRE_SECONDS_METRIC))
        .record(started.elapsed().as_secs_f64());
    if matches!(result, Err(sqlx::Error::PoolTimedOut)) {
        metrics::counter!(metric_name(POOL_ACQUIRE_TIMEOUTS_METRIC)).increment(1);
    }
}

/// One probe: acquire (through [`acquire`]) and `SELECT 1`, within
/// `timeout`.
pub async fn probe(pool: &PgPool, timeout: Duration) -> Result<(), String> {
    let attempt = async {
        let mut conn = acquire(pool).await?;
        sqlx::query("SELECT 1").execute(&mut *conn).await
    };
    match tokio::time::timeout(timeout, attempt).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(err)) => Err(err.to_string()),
        Err(_) => Err(format!("no answer within {}s", timeout.as_secs())),
    }
}

/// A service's view of whether it can reach its database: a 0/1 gauge and
/// a probe-failure counter, named by the caller.
///
/// Build the handles once the metrics recorder is installed: a handle made
/// before then records nowhere.
#[derive(Clone)]
pub struct DbHealth {
    service: &'static str,
    up: metrics::Gauge,
    probe_failures: metrics::Counter,
}

impl DbHealth {
    /// `service` names the process in the log lines (`"api"`).
    pub fn new(
        service: &'static str,
        up: metrics::Gauge,
        probe_failures: metrics::Counter,
    ) -> Self {
        Self {
            service,
            up,
            probe_failures,
        }
    }

    /// Registers both series: the gauge at 0 ("not yet confirmed") and the
    /// counter at 0.
    pub fn register(&self) {
        self.up.set(0.0);
        self.probe_failures.increment(0);
    }

    /// Exports one probe's outcome. Logs only the transitions, so a long
    /// outage logs once rather than every [`PROBE_INTERVAL`].
    pub fn record(&self, outcome: &Result<(), String>, was_up: Option<bool>) -> bool {
        let up = outcome.is_ok();
        self.up.set(if up { 1.0 } else { 0.0 });
        if let Err(err) = outcome {
            self.probe_failures.increment(1);
            if was_up != Some(false) {
                tracing::error!(error = %err, "{} cannot query its database", self.service);
            }
        } else if was_up == Some(false) {
            tracing::info!("{} can query its database again", self.service);
        }
        up
    }

    /// The probe loop; never returns. Spawn it once, when metrics are on.
    pub async fn probe_loop(self, pool: PgPool) {
        let mut interval = tokio::time::interval(PROBE_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut was_up = None;
        loop {
            interval.tick().await;
            let outcome = probe(&pool, PROBE_TIMEOUT).await;
            was_up = Some(self.record(&outcome, was_up));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render(f: impl FnOnce()) -> String {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, f);
        handle.render()
    }

    /// `common::metrics::SHARED_BUCKETS` gives the acquire time its buckets
    /// by full name; the two must not drift apart.
    #[test]
    fn the_acquire_histogram_is_the_one_with_shared_buckets() {
        assert_eq!(
            metric_name(POOL_ACQUIRE_SECONDS_METRIC),
            common::metrics::DB_POOL_ACQUIRE_SECONDS
        );
        assert!(
            common::metrics::SHARED_BUCKETS
                .iter()
                .any(|(name, _)| *name == common::metrics::DB_POOL_ACQUIRE_SECONDS)
        );
    }

    fn health() -> DbHealth {
        DbHealth::new(
            "test",
            metrics::gauge!("test_db_up"),
            metrics::counter!("test_db_probe_failures_total"),
        )
    }

    #[test]
    fn the_pool_gauges_are_registered_at_0() {
        let rendered = render(register_metrics);
        for line in [
            "distant_signal_db_pool_connections{state=\"idle\"} 0",
            "distant_signal_db_pool_connections{state=\"in_use\"} 0",
            "distant_signal_db_pool_max_connections 0",
            "distant_signal_db_pool_acquire_timeouts_total 0",
        ] {
            assert!(rendered.contains(line), "{line} missing from:\n{rendered}");
        }
    }

    #[test]
    fn the_settings_wrap_common_pg_unchanged() {
        let settings = PoolSettings::new("distant-signal-test", 7);
        assert_eq!(
            settings.clone().into_inner(),
            common::pg::PoolSettings::new("distant-signal-test", 7)
        );
        assert_eq!(settings.max_connections, 7);
        assert_eq!(
            settings.acquire_timeout,
            common::pg::DEFAULT_ACQUIRE_TIMEOUT
        );
    }

    #[test]
    fn both_health_series_are_registered_before_any_probe() {
        let rendered = render(|| health().register());
        assert!(rendered.contains("test_db_up 0"), "{rendered}");
        assert!(
            rendered.contains("test_db_probe_failures_total 0"),
            "{rendered}"
        );
    }

    #[test]
    fn a_failed_probe_sets_the_gauge_to_0_and_counts_it() {
        let rendered = render(|| {
            let health = health();
            health.register();
            assert!(health.record(&Ok(()), None));
            assert!(!health.record(&Err("connection refused".to_owned()), Some(true)));
            assert!(!health.record(&Err("connection refused".to_owned()), Some(false)));
        });
        assert!(rendered.contains("test_db_up 0"), "{rendered}");
        assert!(
            rendered.contains("test_db_probe_failures_total 2"),
            "{rendered}"
        );
        let recovered = render(|| {
            health().record(&Ok(()), Some(false));
        });
        assert!(recovered.contains("test_db_up 1"), "{recovered}");
    }

    fn unreachable_pool() -> PgPool {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_secs(1))
            .connect_lazy(&format!("postgres://u:p@127.0.0.1:{port}/db"))
            .unwrap()
    }

    /// Nothing listening: the probe fails (fast, on a refused connection)
    /// instead of hanging.
    #[tokio::test]
    async fn the_probe_fails_against_an_unreachable_database() {
        assert!(
            probe(&unreachable_pool(), Duration::from_secs(5))
                .await
                .is_err()
        );
    }

    async fn live_pool(max_connections: u32) -> PgPool {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        PoolSettings::new("distant-signal-ds-store-pool-test", max_connections)
            .connect(&url)
            .await
            .expect("connect")
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    async fn the_probe_succeeds_against_a_live_database() {
        let pool = live_pool(2).await;
        assert_eq!(probe(&pool, PROBE_TIMEOUT).await, Ok(()));
    }

    /// `in_use` counts a connection while it is held, and drops back once
    /// it is returned.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    async fn in_use_rises_under_a_held_connection() {
        let pool = live_pool(3).await;
        let in_use = |rendered: &str| {
            assert!(
                rendered.contains("distant_signal_db_pool_max_connections 3"),
                "{rendered}"
            );
            rendered
                .lines()
                .find_map(|line| {
                    line.strip_prefix("distant_signal_db_pool_connections{state=\"in_use\"} ")
                })
                .unwrap_or_else(|| panic!("no in_use line in:\n{rendered}"))
                .parse::<u32>()
                .unwrap()
        };

        let held = pool.acquire().await.unwrap();
        let while_held = render(|| sample(&pool));
        assert_eq!(in_use(&while_held), 1, "{while_held}");

        drop(held);
        // The release is handed back to the pool asynchronously.
        let mut after = String::new();
        for _ in 0..50 {
            after = render(|| sample(&pool));
            if in_use(&after) == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(in_use(&after), 0, "{after}");
    }

    /// An exhausted pool: [`acquire`] times out, observes the wait and
    /// counts the timeout.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    async fn an_acquire_timeout_is_counted() {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let mut settings = PoolSettings::new("distant-signal-ds-store-pool-test", 1);
        settings.acquire_timeout = Duration::from_secs(1);
        let pool = settings.connect(&url).await.expect("connect");
        let held = pool.acquire().await.unwrap();

        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let result = {
            let _guard = metrics::set_default_local_recorder(&recorder);
            acquire(&pool).await
        };
        assert!(
            matches!(result, Err(sqlx::Error::PoolTimedOut)),
            "{result:?}"
        );
        let rendered = handle.render();
        assert!(
            rendered.contains("distant_signal_db_pool_acquire_timeouts_total 1"),
            "{rendered}"
        );
        assert!(
            rendered.contains("distant_signal_db_pool_acquire_seconds_count 1"),
            "{rendered}"
        );
        drop(held);
    }
}
