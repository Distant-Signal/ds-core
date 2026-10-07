//! The loop runner against a real Postgres (plan 1B.6 and 1B.7's DB tests).
//! Advisory locks are per database, so these fixed test keys only meet each
//! other. The `train_loops_*` tests run the real sweeps on their real keys,
//! so they need a migrated database.
//!
//! ```text
//! DATABASE_URL=postgres://... cargo test -p ds-store --test loop_locks \
//!     -- --ignored --test-threads=1
//! ```

#![expect(
    clippy::expect_used,
    reason = "test helpers outside #[test] fns: a panic is the failure"
)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::advisory_locks::{self, LoopLock};
use ds_store::loops::{
    CrsLineIndex, LockSession, LoopRunner, LoopSpec, TickOutcome, TrainLoopIntervals,
};
use sqlx::{Connection, PgConnection, PgPool};

fn database_url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test")
}

async fn pool() -> PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url())
        .await
        .expect("connect to postgres")
}

/// A runner with one loop on `lock` whose body counts its runs.
async fn counting_runner(
    lock: LoopLock,
    interval: Duration,
    application_name: &str,
) -> (LoopRunner, Arc<AtomicUsize>) {
    let pool = pool().await;
    let runs = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&runs);
    let mut runner = LoopRunner::new(pool.clone(), LockSession::new(pool, application_name));
    runner
        .register(LoopSpec::new(lock, interval, move |pool| {
            let counter = Arc::clone(&counter);
            async move {
                // A real round trip on the pool, as a sweep would make.
                sqlx::query("SELECT 1").execute(&pool).await?;
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }))
        .expect("register");
    (runner, runs)
}

fn test_lock(name: &'static str, ascii: [u8; 8]) -> LoopLock {
    LoopLock {
        name,
        key: i64::from_be_bytes(ascii),
    }
}

#[tokio::test]
#[ignore = "requires a live database (DATABASE_URL)"]
async fn two_runners_on_one_lock_run_the_body_once_per_tick() {
    let lock = test_lock("two_runners", *b"dststtwo");
    let (first, first_runs) =
        counting_runner(lock, Duration::from_secs(60), "ingest-writer-test-a").await;
    let (second, second_runs) =
        counting_runner(lock, Duration::from_secs(60), "ingest-writer-test-b").await;

    for round in 1..=3 {
        let outcomes = [first.tick(0).await, second.tick(0).await];
        assert_eq!(
            outcomes,
            [TickOutcome::Ran, TickOutcome::Skipped],
            "round {round}: the first runner took the lock and keeps it"
        );
        assert_eq!(first_runs.load(Ordering::SeqCst), round);
        assert_eq!(second_runs.load(Ordering::SeqCst), 0);
    }
    assert!(first.session().holds(lock.key).await);
    assert!(!second.session().holds(lock.key).await);

    // The holder goes away (a writer shut down): the standby takes over on
    // its next tick, and the old holder, reconnecting, now skips.
    first.session().close().await;
    assert_eq!(second.tick(0).await, TickOutcome::Ran);
    assert_eq!(first.tick(0).await, TickOutcome::Skipped);
    assert_eq!(second_runs.load(Ordering::SeqCst), 1);
    assert_eq!(first_runs.load(Ordering::SeqCst), 3);
    second.session().close().await;
    first.session().close().await;
}

#[tokio::test]
#[ignore = "requires a live database (DATABASE_URL)"]
async fn a_tick_is_skipped_while_another_session_holds_the_lock() {
    let lock = test_lock("held_elsewhere", *b"dststhld");
    let (runner, runs) =
        counting_runner(lock, Duration::from_secs(60), "ingest-writer-test-held").await;

    // Another holder: the api's loops during the cutover, say.
    let mut other = PgConnection::connect(&database_url()).await.unwrap();
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(lock.key)
        .execute(&mut other)
        .await
        .unwrap();

    assert_eq!(runner.tick(0).await, TickOutcome::Skipped);
    assert_eq!(runner.tick(0).await, TickOutcome::Skipped);
    assert_eq!(runs.load(Ordering::SeqCst), 0, "the body never ran");

    let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
        .bind(lock.key)
        .fetch_one(&mut other)
        .await
        .unwrap();
    assert!(released);
    assert_eq!(runner.tick(0).await, TickOutcome::Ran);
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    // Now the runner holds it, and the other session cannot take it.
    let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(lock.key)
        .fetch_one(&mut other)
        .await
        .unwrap();
    assert!(!taken, "the runner holds the lock between ticks");
    runner.session().close().await;
    let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(lock.key)
        .fetch_one(&mut other)
        .await
        .unwrap();
    assert!(taken, "closing the session released it");
    other.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a live database (DATABASE_URL)"]
async fn spawned_loops_tick_on_their_interval_and_only_one_runner_sweeps() {
    let lock = test_lock("interval", *b"dststint");
    let interval = Duration::from_millis(250);
    let (first, first_runs) = counting_runner(lock, interval, "ingest-writer-test-i1").await;
    let (second, second_runs) = counting_runner(lock, interval, "ingest-writer-test-i2").await;

    let first = first.spawn(Duration::from_secs(60));
    let second = second.spawn(Duration::from_secs(60));
    // Ticks at 0, 250, 500, 750, 1000 and 1250 ms.
    tokio::time::sleep(Duration::from_millis(1400)).await;
    first.shutdown().await;
    second.shutdown().await;

    let (first_runs, second_runs) = (
        first_runs.load(Ordering::SeqCst),
        second_runs.load(Ordering::SeqCst),
    );
    assert!(
        first_runs == 0 || second_runs == 0,
        "only the lock holder sweeps: {first_runs} and {second_runs}"
    );
    let total = first_runs + second_runs;
    assert!(
        (4..=7).contains(&total),
        "about six ticks in 1.4 s at 250 ms: {total}"
    );
}

#[tokio::test]
#[ignore = "requires a live database (DATABASE_URL)"]
async fn a_lost_lock_connection_is_reopened_and_the_lock_retaken() {
    let lock = test_lock("lost_session", *b"dststlst");
    let application_name = "ingest-writer-test-lost";
    let (runner, runs) = counting_runner(lock, Duration::from_secs(60), application_name).await;
    assert_eq!(runner.tick(0).await, TickOutcome::Ran);

    // Kill the lock session's backend: Postgres drops the lock with it.
    let mut admin = PgConnection::connect(&database_url()).await.unwrap();
    let terminated: Vec<bool> = sqlx::query_scalar(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE application_name = $1 AND datname = current_database()",
    )
    .bind(application_name)
    .fetch_all(&mut admin)
    .await
    .unwrap();
    assert_eq!(terminated, [true]);
    // pg_terminate_backend only signals; wait for the backend to exit.
    for _ in 0..50 {
        let left: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = $1")
                .bind(application_name)
                .fetch_one(&mut admin)
                .await
                .unwrap();
        if left == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    admin.close().await.unwrap();

    // The next tick notices (the ping fails), reconnects and retakes it.
    assert_eq!(runner.tick(0).await, TickOutcome::Ran);
    assert_eq!(runs.load(Ordering::SeqCst), 2);
    assert!(runner.session().holds(lock.key).await);
    runner.session().close().await;
}

async fn sized_pool(max_connections: u32) -> PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&database_url())
        .await
        .expect("connect to postgres")
}

fn train_intervals(every: Duration) -> TrainLoopIntervals {
    TrainLoopIntervals {
        schedule_match: every,
        reconciliation: every,
        schedule_enrichment_grace: chrono::Duration::minutes(30),
        backlog_match: every,
    }
}

/// The real line catalogue's index, as both processes build it.
fn index() -> CrsLineIndex {
    let lines = common::config::parse_lines(
        common::manifest_dir!()
            .join("../../lines")
            .to_str()
            .expect("utf-8 path"),
    )
    .expect("line catalogue");
    Arc::new(ds_store::sweeps::schedule_matching::crs_to_line_ids(&lines))
}

/// The api's runner as `crates/api/src/main.rs` builds it (`api_loop_runner`):
/// the three periodic sweeps on a lock connection held out of its pool.
fn api_runner(pool: PgPool, every: Duration) -> LoopRunner {
    let mut runner = LoopRunner::new(pool.clone(), LockSession::from_pool(pool));
    for spec in ds_store::loops::periodic_sweeps(train_intervals(every), &index()) {
        runner.register(spec).expect("register");
    }
    runner
}

/// The ingest-writer's runner as `ingest_writer::loops::register` builds it
/// (without the canary): the same three sweeps plus the CORPUS loop, on a
/// dedicated lock connection.
fn writer_runner(pool: PgPool, every: Duration) -> LoopRunner {
    let mut runner = LoopRunner::new(
        pool.clone(),
        LockSession::new(pool, "ingest-writer-test-train-locks"),
    );
    for spec in ds_store::loops::periodic_sweeps(train_intervals(every), &index()) {
        runner.register(spec).expect("register");
    }
    runner
        .register(ds_store::loops::corpus_crosswalk(every))
        .expect("register");
    runner
}

fn position(runner: &LoopRunner, lock: LoopLock) -> usize {
    runner
        .loops()
        .iter()
        .position(|spec| spec.lock == lock)
        .expect("registered")
}

const TRAIN_SWEEPS: [LoopLock; 3] = [
    advisory_locks::SCHEDULE_MATCH_SWEEP,
    advisory_locks::RECONCILIATION_SWEEP,
    advisory_locks::BACKLOG_MATCH_SWEEP,
];

/// Plan 1B.7: with `API_BACKGROUND_LOOPS` and `INGEST_WRITER_LOOPS` both on
/// (the cutover), each sweep runs in exactly one of the two per tick, and
/// keeps running there.
#[tokio::test]
#[ignore = "requires a live, migrated database (DATABASE_URL)"]
async fn train_loops_api_and_writer_run_each_sweep_once_per_tick() {
    let api = api_runner(sized_pool(4).await, Duration::from_secs(300));
    let writer = writer_runner(sized_pool(4).await, Duration::from_secs(300));

    // The api ticks first for schedule-match, the writer first for the
    // other two: whoever ticks first takes the lock and keeps it.
    for round in 1..=3 {
        for (n, lock) in TRAIN_SWEEPS.into_iter().enumerate() {
            let (a, w) = (position(&api, lock), position(&writer, lock));
            let (outcomes, want) = if n == 0 {
                let first = api.tick(a).await;
                (
                    (first, writer.tick(w).await),
                    (TickOutcome::Ran, TickOutcome::Skipped),
                )
            } else {
                let first = writer.tick(w).await;
                (
                    (api.tick(a).await, first),
                    (TickOutcome::Skipped, TickOutcome::Ran),
                )
            };
            assert_eq!(outcomes, want, "round {round}, {} (api, writer)", lock.name);
        }
    }
    let schedule = advisory_locks::SCHEDULE_MATCH_SWEEP;
    assert!(api.session().holds(schedule.key).await);
    assert!(!writer.session().holds(schedule.key).await);
    assert!(
        writer
            .session()
            .holds(advisory_locks::BACKLOG_MATCH_SWEEP.key)
            .await
    );

    // The cutover's last step: the api stops (API_BACKGROUND_LOOPS=false
    // and a restart closes its lock connection). The writer takes over on
    // its next tick.
    api.session().close().await;
    assert_eq!(
        writer.tick(position(&writer, schedule)).await,
        TickOutcome::Ran
    );
    writer.session().close().await;
}

/// The api's one-shot CORPUS check at startup takes the CORPUS loop's lock
/// only for its one run: skipped while the writer holds it, and released
/// afterwards so the writer's 10-minute loop is never kept off it.
#[tokio::test]
#[ignore = "requires a live, migrated database (DATABASE_URL)"]
async fn train_loops_the_api_corpus_check_never_keeps_the_writers_lock() {
    let api = api_runner(sized_pool(4).await, Duration::from_secs(300));
    let writer = writer_runner(sized_pool(4).await, Duration::from_secs(300));
    let corpus = ds_store::loops::corpus_crosswalk(Duration::from_secs(600));
    let w = position(&writer, advisory_locks::CORPUS_CROSSWALK);

    assert_eq!(api.run_once(&corpus).await, TickOutcome::Ran);
    assert!(!api.session().holds(corpus.lock.key).await, "released");
    assert_eq!(writer.tick(w).await, TickOutcome::Ran);
    assert_eq!(api.run_once(&corpus).await, TickOutcome::Skipped);
    assert!(writer.session().holds(corpus.lock.key).await);
    api.session().close().await;
    writer.session().close().await;
}

/// Bookkeeping for [`watched`]: per loop, how many bodies ran in each
/// runner, how many are running now, and the most ever at once.
#[derive(Default)]
struct Watch {
    runs: HashMap<(&'static str, &'static str), usize>,
    in_flight: HashMap<&'static str, usize>,
    max_in_flight: HashMap<&'static str, usize>,
}

/// `spec` with its real body wrapped to record into `watch` as `runner`.
fn watched(spec: &LoopSpec, runner: &'static str, watch: &Arc<Mutex<Watch>>) -> LoopSpec {
    let inner = spec.clone();
    let watch = Arc::clone(watch);
    LoopSpec::new(spec.lock, spec.interval, move |pool| {
        let inner = inner.clone();
        let watch = Arc::clone(&watch);
        async move {
            let name = inner.name();
            {
                let mut w = watch.lock().expect("watch");
                *w.runs.entry((name, runner)).or_default() += 1;
                let in_flight = w.in_flight.entry(name).or_default();
                *in_flight += 1;
                let now = *in_flight;
                let max = w.max_in_flight.entry(name).or_default();
                *max = (*max).max(now);
            }
            let result = inner.run_body(pool).await;
            *watch
                .lock()
                .expect("watch")
                .in_flight
                .entry(name)
                .or_default() -= 1;
            result
        }
    })
}

/// `runner`'s loops, each watched, on a new runner with `session`.
fn rewrap(
    runner: &LoopRunner,
    name: &'static str,
    pool: PgPool,
    session: LockSession,
    watch: &Arc<Mutex<Watch>>,
) -> LoopRunner {
    let mut wrapped = LoopRunner::new(pool, session);
    for spec in runner.loops() {
        wrapped
            .register(watched(spec, name, watch))
            .expect("register");
    }
    wrapped
}

/// Plan 1B.7, concurrently: the api's and the writer's loops spawned side
/// by side on a short interval never run the same sweep body at once, and
/// each sweep runs in one of them only.
#[tokio::test]
#[ignore = "requires a live, migrated database (DATABASE_URL)"]
async fn train_loops_spawned_side_by_side_never_overlap() {
    let every = Duration::from_millis(100);
    let watch = Arc::new(Mutex::new(Watch::default()));
    let api_pool = sized_pool(4).await;
    let api = rewrap(
        &api_runner(api_pool.clone(), every),
        "api",
        api_pool.clone(),
        LockSession::from_pool(api_pool),
        &watch,
    );
    let writer_pool = sized_pool(4).await;
    let writer = rewrap(
        &writer_runner(writer_pool.clone(), every),
        "writer",
        writer_pool.clone(),
        LockSession::new(writer_pool, "ingest-writer-test-side-by-side"),
        &watch,
    );

    let api = api.spawn(Duration::from_secs(60));
    let writer = writer.spawn(Duration::from_secs(60));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    api.shutdown().await;
    writer.shutdown().await;

    let watch = watch.lock().expect("watch");
    for lock in TRAIN_SWEEPS {
        let api_runs = watch.runs.get(&(lock.name, "api")).copied().unwrap_or(0);
        let writer_runs = watch.runs.get(&(lock.name, "writer")).copied().unwrap_or(0);
        assert!(
            api_runs + writer_runs >= 3,
            "{}: the sweep kept running ({api_runs} + {writer_runs})",
            lock.name
        );
        assert!(
            api_runs == 0 || writer_runs == 0,
            "{}: one process holds the lock throughout ({api_runs} and {writer_runs})",
            lock.name
        );
        assert_eq!(
            watch.max_in_flight.get(lock.name).copied(),
            Some(1),
            "{}: never two bodies at once",
            lock.name
        );
    }
}
