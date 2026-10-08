//! Named background loops, each guarded by a session advisory lock (spec
//! §10 and §12.3, plan 1B.6 and 1B.7).
//!
//! Every [`LoopSpec`] runs on its own interval. Before each tick the loop
//! asks the runner's [`LockSession`] for `pg_try_advisory_lock(key)`:
//!
//! - **held** (taken now, or still held from an earlier tick): the body
//!   runs;
//! - **held by another session** (a second writer replica, or the api's own
//!   loops during the 1B cutover): the tick is skipped and logged at debug;
//! - **error** (the lock connection is gone and could not be reopened): the
//!   tick is skipped and logged at warn.
//!
//! The ingest-writer and the api (`API_BACKGROUND_LOOPS`) both run their
//! train-domain loops through this runner, on the same keys
//! ([`common::advisory_locks`]), so during the cutover each sweep runs in
//! one process at a time.
//!
//! # Holding, not taking per tick
//!
//! A lock, once taken, is held for the life of the [`LockSession`]'s
//! connection; it is never released between ticks. So one runner owns a
//! loop until its process exits (the connection closes and Postgres
//! releases the lock), and a standby takes it over on its next tick. That
//! is what lets the writer and the api overlap safely during the cutover
//! (spec §12.3, "turn the writer on before the api off").
//!
//! The one exception is [`LoopRunner::run_once`] (the api's one-shot
//! CORPUS crosswalk check at startup): it takes the lock, runs the body and
//! releases the lock again, so it never keeps the writer's periodic loop
//! off that key for the rest of the api's life.
//!
//! # One connection for every lock
//!
//! The session is ONE connection for all of the runner's locks, however
//! many loops it runs; the bodies use the pool. Two kinds:
//!
//! - [`LockSession::new`] (the ingest-writer): opened from the pool's own
//!   connect options (same timeouts) but outside the pool, so one
//!   connection beyond the pool's `max_connections` (spec §6.6: the
//!   writer's pool is 6, its role limit 7);
//! - [`LockSession::from_pool`] (the api): checked out of the pool and
//!   held, so the api's connection budgets are unchanged and its pool has
//!   one connection fewer for requests while the loops run. It is closed,
//!   never returned to the pool, when the session lets it go, so no lock
//!   can leak to another user of the pool.
//!
//! Postgres advisory locks are re-entrant per session,
//! so the session remembers which keys it holds rather than asking again:
//! two runners in one process must each have their own session.
//!
//! Before a tick of a lock it already holds, the session pings its
//! connection. If the connection has died, Postgres has released every
//! lock on it, so the session reconnects and tries again at once. A
//! connection dying in the middle of a body can let another runner start
//! the same sweep before this body finishes; every sweep is idempotent, so
//! that window costs at most one duplicate pass.
//!
//! # Logging
//!
//! A body logs its own outcome (the sweeps' messages are the ones the api's
//! loops have always logged, so they read the same from either process).
//! The runner logs a failed body only at debug, so a failure is not logged
//! twice.
//!
//! # Metrics
//!
//! One family whichever process runs the loop (the api or the
//! ingest-writer; Prometheus's `job`/`pod` labels tell them apart), so a
//! dashboard or alert follows a sweep across the cutover:
//!
//! - `distant_signal_loop_cycles_total{cycle, result}` and
//!   `distant_signal_loop_last_success_timestamp_seconds{cycle}`
//!   ([`common::metrics::register_cycle`] with the service [`METRIC_SERVICE`]):
//!   the body's outcomes. A standby process's gauge stays at its start
//!   time, so read it as `max by (cycle)`;
//! - `distant_signal_loop_ticks_total{loop, outcome}`, outcome `ran`,
//!   `failed`, `skipped` (lock held elsewhere) or `lock_error`;
//! - `distant_signal_loop_lock_held{loop}`: 1 while this process holds the
//!   loop's lock (`sum by (loop)` is 1 while exactly one process runs it);
//! - `distant_signal_loop_seconds{loop}`: body duration.

use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use common::advisory_locks::LoopLock;
use common::metrics::metric_name;
use common::progress::Progress;
use sqlx::pool::PoolConnection;
use sqlx::{ConnectOptions, Connection, PgConnection, PgPool, Postgres};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

/// The `<service>` of [`common::metrics::register_cycle`]'s metric names,
/// and the prefix of the runner's own: `distant_signal_loop_*`.
pub const METRIC_SERVICE: &str = "loop";
const TICKS_METRIC: &str = "loop_ticks_total";
const LOCK_HELD_METRIC: &str = "loop_lock_held";
const SECONDS_METRIC: &str = "loop_seconds";

/// A loop body's future.
pub type LoopFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

/// A loop body: called once per tick it holds the lock for, with the pool.
pub type LoopBody = Arc<dyn Fn(PgPool) -> LoopFuture + Send + Sync>;

/// One named loop: its lock, its interval and its body.
#[derive(Clone)]
pub struct LoopSpec {
    pub lock: LoopLock,
    pub interval: Duration,
    body: LoopBody,
}

impl LoopSpec {
    pub fn new<F, Fut>(lock: LoopLock, interval: Duration, body: F) -> Self
    where
        F: Fn(PgPool) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        Self {
            lock,
            interval,
            body: Arc::new(move |pool| Box::pin(body(pool))),
        }
    }

    pub fn name(&self) -> &'static str {
        self.lock.name
    }

    /// The body alone, with no lock (to wrap one spec in another, as the
    /// lock tests do to watch the real sweeps).
    pub fn run_body(&self, pool: PgPool) -> LoopFuture {
        (self.body)(pool)
    }
}

impl fmt::Debug for LoopSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoopSpec")
            .field("lock", &self.lock)
            .field("interval", &self.interval)
            .finish_non_exhaustive()
    }
}

/// What one tick did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickOutcome {
    /// Held the lock; the body succeeded.
    Ran,
    /// Held the lock; the body returned an error (the body logged it).
    Failed,
    /// Another session holds the lock; the body did not run.
    Skipped,
    /// The lock session could not be (re)opened; the body did not run.
    LockError,
}

impl TickOutcome {
    const ALL: [Self; 4] = [Self::Ran, Self::Failed, Self::Skipped, Self::LockError];

    fn label(self) -> &'static str {
        match self {
            Self::Ran => "ran",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::LockError => "lock_error",
        }
    }
}

/// The connection holding a runner's loop locks. See the module docs.
pub struct LockSession {
    pool: PgPool,
    source: LockSource,
    state: Mutex<SessionState>,
}

/// Where a [`LockSession`] gets its connection.
enum LockSource {
    /// Opened from the pool's connect options, outside the pool: one
    /// connection beyond its `max_connections` (the ingest-writer, whose
    /// chart budget counts it).
    Dedicated { application_name: String },
    /// Checked out of the pool and held, closed (never returned) when the
    /// session drops it: one of the pool's own `max_connections` (the api,
    /// so no connection budget changes).
    Pooled,
}

/// The lock connection, either kind.
enum LockConn {
    Dedicated(PgConnection),
    Pooled(PoolConnection<Postgres>),
}

impl LockConn {
    fn raw(&mut self) -> &mut PgConnection {
        match self {
            Self::Dedicated(conn) => conn,
            Self::Pooled(conn) => conn,
        }
    }

    async fn close(self) -> Result<(), sqlx::Error> {
        match self {
            Self::Dedicated(conn) => conn.close().await,
            Self::Pooled(conn) => conn.close().await,
        }
    }
}

#[derive(Default)]
struct SessionState {
    conn: Option<LockConn>,
    held: HashSet<i64>,
}

impl SessionState {
    /// Forgets the connection and every lock on it. Dropping it closes its
    /// socket (a pooled one is marked `close_on_drop`, so it never goes back
    /// to the pool with a lock on it), so Postgres releases the locks.
    fn reset(&mut self) {
        self.conn = None;
        self.held.clear();
    }
}

impl LockSession {
    /// A dedicated connection, opened from `pool`'s connect options and
    /// reported in `pg_stat_activity` as `application_name`: one beyond the
    /// pool.
    pub fn new(pool: PgPool, application_name: &str) -> Self {
        Self {
            pool,
            source: LockSource::Dedicated {
                application_name: application_name.to_owned(),
            },
            state: Mutex::new(SessionState::default()),
        }
    }

    /// A connection checked out of `pool` and held: it counts in the pool's
    /// own `max_connections` (one fewer for everything else while held) and
    /// shows as the pool's `application_name`.
    pub fn from_pool(pool: PgPool) -> Self {
        Self {
            pool,
            source: LockSource::Pooled,
            state: Mutex::new(SessionState::default()),
        }
    }

    /// Whether this session holds `key` now: already held (and the
    /// connection still answers), or newly taken with
    /// `pg_try_advisory_lock`. `Ok(false)` means another session holds it.
    pub async fn try_hold(&self, key: i64) -> Result<bool> {
        let mut guard = self.state.lock().await;
        let state = &mut *guard;
        if state.held.contains(&key) {
            let alive = match state.conn.as_mut() {
                Some(conn) => conn.raw().ping().await.is_ok(),
                None => false,
            };
            if alive {
                return Ok(true);
            }
            tracing::warn!(
                key,
                "the advisory-lock connection was lost, and every lock with it; reconnecting"
            );
            state.reset();
        }
        let result = self.take(state, key).await;
        if result.is_err() {
            state.reset();
        }
        result
    }

    async fn open(&self) -> Result<LockConn> {
        match &self.source {
            LockSource::Dedicated { application_name } => {
                let options = (*self.pool.connect_options())
                    .clone()
                    .application_name(application_name);
                let conn = options
                    .connect()
                    .await
                    .context("could not open the advisory-lock connection")?;
                Ok(LockConn::Dedicated(conn))
            }
            LockSource::Pooled => {
                let mut conn = self
                    .pool
                    .acquire()
                    .await
                    .context("could not check out the advisory-lock connection")?;
                conn.close_on_drop();
                Ok(LockConn::Pooled(conn))
            }
        }
    }

    async fn take(&self, state: &mut SessionState, key: i64) -> Result<bool> {
        if state.conn.is_none() {
            let conn = self.open().await?;
            state.held.clear();
            state.conn = Some(conn);
        }
        let Some(conn) = state.conn.as_mut() else {
            anyhow::bail!("the advisory-lock connection was just opened");
        };
        let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(key)
            .fetch_one(conn.raw())
            .await
            .context("pg_try_advisory_lock failed")?;
        if taken {
            state.held.insert(key);
        }
        Ok(taken)
    }

    /// Whether this session currently believes it holds `key`.
    pub async fn holds(&self, key: i64) -> bool {
        self.state.lock().await.held.contains(&key)
    }

    /// Releases `key` if this session holds it (`pg_advisory_unlock`). A
    /// failure drops the connection, which releases every lock on it.
    pub async fn release(&self, key: i64) {
        let mut guard = self.state.lock().await;
        let state = &mut *guard;
        if !state.held.remove(&key) {
            return;
        }
        let Some(conn) = state.conn.as_mut() else {
            return;
        };
        let result: Result<bool, sqlx::Error> = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
            .bind(key)
            .fetch_one(conn.raw())
            .await;
        if let Err(err) = result {
            tracing::warn!(
                key,
                error = ?err,
                "releasing an advisory lock failed; closing the lock connection to release it"
            );
            state.reset();
        }
    }

    /// Releases every lock and closes the connection (graceful shutdown).
    /// The explicit `pg_advisory_unlock_all` makes the release synchronous:
    /// a closed socket alone frees the locks only once the server backend
    /// has noticed and exited, a moment later.
    pub async fn close(&self) {
        let mut state = self.state.lock().await;
        if let Some(mut conn) = state.conn.take() {
            if let Err(err) = sqlx::query("SELECT pg_advisory_unlock_all()")
                .execute(conn.raw())
                .await
            {
                tracing::debug!(error = ?err, "releasing the loop locks failed; closing anyway");
            }
            if let Err(err) = conn.close().await {
                tracing::debug!(error = ?err, "closing the advisory-lock connection failed");
            }
        }
        state.held.clear();
    }
}

/// A set of loops sharing one [`LockSession`].
pub struct LoopRunner {
    pool: PgPool,
    session: Arc<LockSession>,
    loops: Vec<LoopSpec>,
}

impl LoopRunner {
    pub fn new(pool: PgPool, session: LockSession) -> Self {
        Self {
            pool,
            session: Arc::new(session),
            loops: Vec::new(),
        }
    }

    /// Adds a loop and registers its metrics at 0. Refuses a zero interval
    /// (`tokio::time::interval` panics on one) and a second loop with the
    /// same name or key.
    pub fn register(&mut self, spec: LoopSpec) -> Result<()> {
        anyhow::ensure!(
            !spec.interval.is_zero(),
            "loop {:?} has a zero interval",
            spec.name()
        );
        if let Some(existing) = self
            .loops
            .iter()
            .find(|l| l.name() == spec.name() || l.lock.key == spec.lock.key)
        {
            anyhow::bail!(
                "loop {:?} clashes with the registered loop {:?} (same name or lock key)",
                spec.name(),
                existing.name()
            );
        }
        register_metrics(spec.name());
        self.loops.push(spec);
        Ok(())
    }

    pub fn loops(&self) -> &[LoopSpec] {
        &self.loops
    }

    pub fn session(&self) -> &Arc<LockSession> {
        &self.session
    }

    /// One tick of `self.loops()[index]`, now: try the lock, then run the
    /// body if this session holds it.
    pub async fn tick(&self, index: usize) -> TickOutcome {
        tick(&self.pool, &self.session, &self.loops[index]).await
    }

    /// One tick of `spec`, which need not be registered, then releases its
    /// lock again unless this session held it before (a registered loop
    /// keeps its own). For a one-shot task that must not overlap the same
    /// loop elsewhere but must not keep it either: the api's CORPUS
    /// crosswalk check at startup, while the ingest-writer runs that check
    /// every 10 minutes.
    pub async fn run_once(&self, spec: &LoopSpec) -> TickOutcome {
        run_once(&self.pool, &self.session, spec).await
    }

    /// Spawns every loop on its own task. Each gets its own [`Progress`]
    /// (stalled once one body runs past `stall_after`); the caller polls
    /// [`RunningLoops::stalled`] to drive its health endpoint.
    pub fn spawn(self, stall_after: Duration) -> RunningLoops {
        let mut tasks = JoinSet::new();
        let mut progress = Vec::with_capacity(self.loops.len());
        for spec in self.loops {
            let loop_progress = Progress::new(stall_after);
            progress.push((spec.name(), loop_progress.clone()));
            tasks.spawn(run_loop(
                self.pool.clone(),
                Arc::clone(&self.session),
                spec,
                loop_progress,
            ));
        }
        RunningLoops {
            tasks,
            progress,
            session: self.session,
        }
    }
}

/// The spawned loops of a [`LoopRunner`].
pub struct RunningLoops {
    tasks: JoinSet<()>,
    progress: Vec<(&'static str, Progress)>,
    session: Arc<LockSession>,
}

impl RunningLoops {
    /// The loops whose current body has run past the stall budget, or whose
    /// task has died (it stops beating, so it goes stalled too).
    pub fn stalled(&self) -> Vec<&'static str> {
        self.progress
            .iter()
            .filter(|(_, progress)| progress.is_stalled())
            .map(|(name, _)| *name)
            .collect()
    }

    pub fn len(&self) -> usize {
        self.progress.len()
    }

    pub fn is_empty(&self) -> bool {
        self.progress.is_empty()
    }

    /// Keeps the loops running until every task has ended (which only a
    /// panic does), logging each that ends. For a process with no
    /// liveness wiring of its own (the api): spawn this and forget it.
    /// Dropping a [`RunningLoops`] instead aborts its loops.
    pub async fn run_until_all_end(mut self) {
        while let Some(result) = self.tasks.join_next().await {
            tracing::error!(error = ?result.err(), "a background loop task ended");
        }
    }

    /// [`Self::run_until_all_end`] until `stop` resolves, then
    /// [`Self::shutdown`]: for a process that keeps no liveness wiring of
    /// its own but still wants its locks released on SIGTERM (the api).
    pub async fn run_until(mut self, stop: impl Future<Output = ()>) {
        tokio::pin!(stop);
        loop {
            tokio::select! {
                () = &mut stop => break,
                result = self.tasks.join_next() => match result {
                    Some(result) => {
                        tracing::error!(error = ?result.err(), "a background loop task ended");
                    }
                    None => {
                        // Every loop has ended; keep the session until told
                        // to stop, then close it.
                        (&mut stop).await;
                        break;
                    }
                },
            }
        }
        self.shutdown().await;
    }

    /// Stops every loop (a body in flight is dropped mid-await; each is
    /// idempotent and transactional) and closes the lock session, so a
    /// standby can take the locks on its next tick.
    pub async fn shutdown(mut self) {
        self.tasks.shutdown().await;
        self.session.close().await;
    }
}

/// [`LoopRunner::run_once`] on a runner's session after the runner has
/// been spawned (take the session from [`LoopRunner::session`] first), so
/// the one-shot runs alongside the periodic loops.
pub async fn run_once(pool: &PgPool, session: &LockSession, spec: &LoopSpec) -> TickOutcome {
    let key = spec.lock.key;
    let held_before = session.holds(key).await;
    register_metrics(spec.name());
    let outcome = tick(pool, session, spec).await;
    if !held_before && session.holds(key).await {
        session.release(key).await;
        set_lock_held(spec.name(), false);
    }
    outcome
}

async fn run_loop(pool: PgPool, session: Arc<LockSession>, spec: LoopSpec, progress: Progress) {
    let mut interval = tokio::time::interval(spec.interval);
    // A slow tick delays the next one rather than bursting the missed ones
    // (the api's `sweep_interval` did the same).
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        progress.idle(interval.tick()).await;
        tick(&pool, &session, &spec).await;
        progress.beat();
    }
}

async fn tick(pool: &PgPool, session: &LockSession, spec: &LoopSpec) -> TickOutcome {
    let name = spec.name();
    let outcome = match session.try_hold(spec.lock.key).await {
        Ok(true) => {
            set_lock_held(name, true);
            let started = tokio::time::Instant::now();
            let result = (spec.body)(pool.clone()).await;
            metrics::histogram!(metric_name(SECONDS_METRIC), "loop" => name)
                .record(started.elapsed().as_secs_f64());
            common::metrics::record_cycle(METRIC_SERVICE, name, result.is_ok());
            match result {
                Ok(()) => TickOutcome::Ran,
                Err(err) => {
                    // The body has logged it (module docs, Logging).
                    tracing::debug!(loop_name = name, error = ?err, "background loop body failed");
                    TickOutcome::Failed
                }
            }
        }
        Ok(false) => {
            set_lock_held(name, false);
            tracing::debug!(
                loop_name = name,
                key = spec.lock.key,
                "advisory lock held by another session; skipping this tick"
            );
            TickOutcome::Skipped
        }
        Err(err) => {
            set_lock_held(name, false);
            tracing::warn!(
                loop_name = name,
                error = ?err,
                "could not check the loop's advisory lock; skipping this tick"
            );
            TickOutcome::LockError
        }
    };
    metrics::counter!(
        metric_name(TICKS_METRIC),
        "loop" => name,
        "outcome" => outcome.label()
    )
    .increment(1);
    outcome
}

fn register_metrics(name: &'static str) {
    common::metrics::register_cycle(METRIC_SERVICE, name);
    for outcome in TickOutcome::ALL {
        metrics::counter!(
            metric_name(TICKS_METRIC),
            "loop" => name,
            "outcome" => outcome.label()
        )
        .increment(0);
    }
    set_lock_held(name, false);
}

fn set_lock_held(name: &'static str, held: bool) {
    metrics::gauge!(metric_name(LOCK_HELD_METRIC), "loop" => name).set(if held {
        1.0
    } else {
        0.0
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOCK: LoopLock = LoopLock {
        name: "test",
        key: 1,
    };

    fn lazy_pool() -> PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://nobody@localhost/none")
            .unwrap()
    }

    fn noop(lock: LoopLock, interval: Duration) -> LoopSpec {
        LoopSpec::new(lock, interval, |_pool| async { Ok(()) })
    }

    #[test]
    fn the_metric_names_are_one_family_for_every_process() {
        assert_eq!(metric_name(TICKS_METRIC), "distant_signal_loop_ticks_total");
        assert_eq!(
            metric_name(LOCK_HELD_METRIC),
            "distant_signal_loop_lock_held"
        );
        assert_eq!(metric_name(SECONDS_METRIC), "distant_signal_loop_seconds");
        assert_eq!(
            metric_name(&format!("{METRIC_SERVICE}_cycles_total")),
            "distant_signal_loop_cycles_total"
        );
    }

    #[tokio::test]
    async fn register_refuses_a_zero_interval_and_clashes() {
        let pool = lazy_pool();
        let mut runner = LoopRunner::new(pool.clone(), LockSession::new(pool, "t"));
        assert!(runner.register(noop(LOCK, Duration::ZERO)).is_err());
        runner.register(noop(LOCK, Duration::from_secs(1))).unwrap();
        let same_key = LoopLock {
            name: "other",
            key: LOCK.key,
        };
        let same_name = LoopLock {
            name: LOCK.name,
            key: 2,
        };
        assert!(
            runner
                .register(noop(same_key, Duration::from_secs(1)))
                .is_err()
        );
        assert!(
            runner
                .register(noop(same_name, Duration::from_secs(1)))
                .is_err()
        );
        assert_eq!(runner.loops().len(), 1);
    }

    #[tokio::test]
    async fn an_unreachable_database_is_a_lock_error_and_the_body_does_not_run() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(200))
            // Port 1 on localhost: refused at once.
            .connect_lazy("postgres://nobody@127.0.0.1:1/none")
            .unwrap();
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&ran);
        let mut runner = LoopRunner::new(pool.clone(), LockSession::new(pool, "t"));
        runner
            .register(LoopSpec::new(LOCK, Duration::from_secs(1), move |_pool| {
                let flag = Arc::clone(&flag);
                async move {
                    flag.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                }
            }))
            .unwrap();
        assert_eq!(runner.tick(0).await, TickOutcome::LockError);
        assert_eq!(
            runner.run_once(&runner.loops()[0].clone()).await,
            TickOutcome::LockError
        );
        assert!(!ran.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!runner.session().holds(LOCK.key).await);
    }

    #[test]
    fn spawning_no_loops_reports_nothing_stalled() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let pool = lazy_pool();
            let runner = LoopRunner::new(pool.clone(), LockSession::new(pool, "t"));
            let running = runner.spawn(Duration::from_secs(60));
            assert!(running.is_empty());
            assert!(running.stalled().is_empty());
            running.shutdown().await;
        });
    }
}
