//! What the ingest producers share now that they write Postgres or a Redis
//! stream directly (ingest architecture phase 5, release B: the api's
//! `/private/*` routes and the HTTP helpers that called them are gone):
//!
//! - [`classify_failure`]: whether a failed write may be retried
//!   ([`FailureClass`], [`DataRejected`]);
//! - a poller's startup cursor ([`CursorSource`], [`time_until_next_poll_for`]):
//!   when its data last landed, so a restart does not re-fetch fresh data;
//! - [`RDM_AUTH_HEADER_NAME`], the RDM API-key header the pollers send
//!   upstream.

use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::backoff::Backoff;
use crate::progress::Progress;

/// Why a downstream write failed, as far as retrying it is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// Retrying the same data can succeed: the database or Redis
    /// unreachable, a timeout, a lock or serialization failure, an upstream
    /// 5xx or 408/429. The data must stay pending and be retried for as
    /// long as it takes.
    Transient,
    /// The data itself was refused ([`DataRejected`], or an upstream 400,
    /// 413 or 422): the same write will fail the same way on every retry.
    Rejected,
}

/// A write that refused the data itself (a DB sink's class 22 or 23
/// SQLSTATE). [`classify_failure`] reads it as [`FailureClass::Rejected`],
/// as it does an HTTP 400, 413 or 422: the same data will fail the same way.
#[derive(Debug)]
pub struct DataRejected(pub String);

impl std::fmt::Display for DataRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "data rejected: {}", self.0)
    }
}

impl std::error::Error for DataRejected {}

/// Classifies a failed write. Only [`DataRejected`], or a response status
/// that means "this request body is bad", counts as [`FailureClass::Rejected`];
/// everything else -- including any error this function does not recognise
/// -- is [`FailureClass::Transient`], because wrongly treating an outage as
/// a rejection dead-letters healthy data, while the reverse only delays it.
pub fn classify_failure(err: &anyhow::Error) -> FailureClass {
    if err
        .chain()
        .any(|cause| cause.downcast_ref::<DataRejected>().is_some())
    {
        return FailureClass::Rejected;
    }
    let status = err.chain().find_map(|cause| {
        cause
            .downcast_ref::<reqwest::Error>()
            .and_then(reqwest::Error::status)
    });
    match status {
        Some(
            reqwest::StatusCode::BAD_REQUEST
            | reqwest::StatusCode::PAYLOAD_TOO_LARGE
            | reqwest::StatusCode::UNPROCESSABLE_ENTITY,
        ) => FailureClass::Rejected,
        _ => FailureClass::Transient,
    }
}

/// Header RDM uses for API-key auth, per RSPS5050 P-03-00 Rev A. How
/// confidently this is corroborated varies per poller/product — see each
/// poller's `main.rs` module docs for the specific gap, if any. Unrelated
/// to any DS-internal auth: this is RDM's own upstream credential.
pub const RDM_AUTH_HEADER_NAME: &str = "x-apikey";

/// How long a poller waits for its cursor source (Postgres or Redis) to
/// answer before giving up on it (SVC-09). After a node reboot the pollers
/// can start before it is up; treating that as "poll now" would spend the
/// poller's upstream fetch (once per 24h for the RDM stations/TOCs feeds) on
/// a write that could not succeed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiWait {
    pub backoff: Backoff,
    /// Give up (and fall back to the old "poll now") once this much time has
    /// gone by -- so a GET-side-only breakage can never stop polling.
    pub max_wait: Duration,
}

/// 2s doubling to 30s, for at most 10 minutes: a slow Postgres crash
/// recovery or Redis AOF load.
pub const API_STARTUP_WAIT: ApiWait = ApiWait {
    backoff: Backoff::new(Duration::from_secs(2), Duration::from_secs(30)),
    max_wait: Duration::from_secs(600),
};

/// A future that yields a poller's last-fetch time (see [`CursorSource`]).
pub type CursorFuture<'a> =
    std::pin::Pin<Box<dyn Future<Output = anyhow::Result<Option<DateTime<Utc>>>> + Send + 'a>>;

/// A reader of a poller's last-fetch time, called once per read (see
/// [`CursorSource`]).
pub type CursorFn<'a> = Box<dyn FnMut() -> CursorFuture<'a> + Send + 'a>;

/// Where a poller reads its startup cursor, "when did my data last land"
/// (ingest architecture spec §11.3, plan 4.6). [`time_until_next_poll_for`]
/// and `common::poller_loop` run the same logic, waits and tests whichever
/// it is:
///
/// - [`CursorSource::Db`]: a direct writer (`INGEST_SINK=db`, phase 2)
///   reads its own freshness or marker row with its own role.
/// - [`CursorSource::Stream`]: a stream producer (`INGEST_SINK=stream`,
///   phase 3) reads the `produced_at` of its own stream's newest entry
///   (`ingest_stream::last_produced_at`; `ingest_stream::stream_cursor`
///   builds this variant). An empty or missing stream is `None`: poll now.
///
/// `common` depends on neither sqlx nor the stream runtime, so the `Db` and
/// `Stream` readers are closures the poller builds ([`CursorSource::db`],
/// [`CursorSource::stream`]).
pub enum CursorSource<'a> {
    Stream(CursorFn<'a>),
    Db(CursorFn<'a>),
}

impl<'a> CursorSource<'a> {
    /// The poller's own freshness row, read by `read` (e.g.
    /// `ds_store::freshness::last_stations_fetch`).
    pub fn db<F, Fut>(mut read: F) -> Self
    where
        F: FnMut() -> Fut + Send + 'a,
        Fut: Future<Output = anyhow::Result<Option<DateTime<Utc>>>> + Send + 'a,
    {
        Self::Db(Box::new(move || Box::pin(read())))
    }

    /// The poller's own stream's newest `produced_at`, read by `read`.
    pub fn stream<F, Fut>(mut read: F) -> Self
    where
        F: FnMut() -> Fut + Send + 'a,
        Fut: Future<Output = anyhow::Result<Option<DateTime<Utc>>>> + Send + 'a,
    {
        Self::Stream(Box::new(move || Box::pin(read())))
    }

    /// `stream` or `db`, for logs.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Stream(_) => "stream",
            Self::Db(_) => "db",
        }
    }
}

impl std::fmt::Debug for CursorSource<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.kind())
    }
}

/// Reads a poller's last-fetch time: a [`CursorSource`], or any closure
/// returning the same future.
pub trait LastFetched {
    /// The last-fetch time, `None` if the data never landed.
    fn last_fetched(&mut self) -> impl Future<Output = anyhow::Result<Option<DateTime<Utc>>>>;
}

impl<F, Fut> LastFetched for F
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<Option<DateTime<Utc>>>>,
{
    fn last_fetched(&mut self) -> impl Future<Output = anyhow::Result<Option<DateTime<Utc>>>> {
        self()
    }
}

impl LastFetched for CursorSource<'_> {
    async fn last_fetched(&mut self) -> anyhow::Result<Option<DateTime<Utc>>> {
        match self {
            Self::Stream(read) | Self::Db(read) => read().await,
        }
    }
}

/// Reads the last-fetch time from any [`LastFetched`], a [`CursorSource`]
/// above all: retried on any failure with `wait.backoff` for up to
/// `wait.max_wait`, beating `progress` meanwhile (waiting for the database
/// or Redis is not a stall).
pub async fn wait_for_source<C: LastFetched + ?Sized>(
    source: &mut C,
    wait: &ApiWait,
    progress: Option<&Progress>,
) -> anyhow::Result<Option<DateTime<Utc>>> {
    let started = tokio::time::Instant::now();
    let mut failures: u32 = 0;
    loop {
        let err = match source.last_fetched().await {
            Ok(fetched_at) => {
                if failures > 0 {
                    tracing::info!(
                        waited_secs = started.elapsed().as_secs(),
                        "the last-fetch source is reachable again"
                    );
                }
                return Ok(fetched_at);
            }
            Err(err) => err,
        };
        let delay = wait.backoff.delay(failures);
        if started.elapsed() + delay > wait.max_wait {
            return Err(err);
        }
        tracing::warn!(
            error = ?err,
            attempt = failures + 1,
            retry_in_secs = delay.as_secs(),
            "the last-fetch source (database or stream) is not reachable yet; waiting for it before fetching upstream"
        );
        if let Some(progress) = progress {
            progress.beat();
        }
        tokio::time::sleep(delay).await;
        failures = failures.saturating_add(1);
    }
}

/// How long to wait before this process's first poll, so a restart does not
/// immediately re-fetch data that is still fresh: reads the last-fetch time
/// from `source` (any [`LastFetched`], a [`CursorSource`] above all),
/// waiting for it as [`wait_for_source`] does, then defers to the pure
/// [`duration_until_next_poll`]. A source still unreachable after
/// `wait.max_wait` logs a warning and returns `Duration::ZERO` ("poll
/// now"), the fallback since before this existed.
pub async fn time_until_next_poll_for<C: LastFetched + ?Sized>(
    source: &mut C,
    poll_interval: Duration,
    wait: &ApiWait,
    progress: Option<&Progress>,
) -> Duration {
    let fetched_at = match wait_for_source(source, wait, progress).await {
        Ok(fetched_at) => fetched_at,
        Err(err) => {
            tracing::warn!(
                error = ?err,
                max_wait_secs = wait.max_wait.as_secs(),
                "could not determine last-fetch time; polling immediately"
            );
            return Duration::ZERO;
        }
    };
    duration_until_next_poll(fetched_at, Utc::now(), poll_interval)
}

/// Backoff between retries of a failed poller write (the DB sinks retry
/// a [`FailureClass::Transient`] failure with it).
pub const POST_RETRY_BACKOFF: Backoff =
    Backoff::new(Duration::from_secs(2), Duration::from_secs(60));

/// `None` (never fetched) means "poll now" (`Duration::ZERO`). Otherwise,
/// the elapsed time since `fetched_at` is clamped to zero if it would be
/// negative (a `fetched_at` in the future — clock skew between hosts —
/// never underflows or panics); a `poll_interval` already exceeded by that
/// elapsed time means "poll now", otherwise the remainder is returned.
/// Return value is always `<= poll_interval` — this only ever delays the
/// *first* tick of a fresh process, so it can't compound across restarts.
///
/// Assumes restarts aren't pathologically frequent: if something else
/// were also wrong (e.g. a bug writing `fetched_at` persistently in the
/// future) *and* the process were crash-looping, every restart would
/// re-arm a full-interval delay before ever reaching a real poll. Two
/// simultaneous faults, not a risk from this function in isolation.
pub(crate) fn duration_until_next_poll(
    fetched_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    poll_interval: Duration,
) -> Duration {
    let Some(fetched_at) = fetched_at else {
        return Duration::ZERO;
    };
    let elapsed = (now - fetched_at).to_std().unwrap_or(Duration::ZERO);
    poll_interval.saturating_sub(elapsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FAST_WAIT: ApiWait = ApiWait {
        backoff: Backoff::new(Duration::from_millis(10), Duration::from_millis(20)),
        max_wait: Duration::from_secs(5),
    };

    #[test]
    fn a_data_rejection_is_classified_rejected_even_under_context() {
        assert_eq!(
            classify_failure(&anyhow::Error::from(DataRejected("23514".to_string()))),
            FailureClass::Rejected
        );
        assert_eq!(
            classify_failure(
                &anyhow::Error::from(DataRejected("23505 duplicate key".to_string()))
                    .context("writing incidents")
            ),
            FailureClass::Rejected,
            "a DB sink's data rejection is a rejection under a context layer too"
        );
    }

    #[test]
    fn an_unrecognised_error_is_transient() {
        assert_eq!(
            classify_failure(&anyhow::anyhow!("connection refused")),
            FailureClass::Transient
        );
    }

    /// Plan 4.6: the same `time_until_next_poll_for` behaviour whichever
    /// [`CursorSource`] the cursor comes from -- a fresh cursor delays the
    /// first poll, a failing source is waited for, `None` and a source that
    /// never answers mean "poll now".
    #[tokio::test]
    async fn every_cursor_source_gives_the_same_first_poll_delay() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};

        let day = Duration::from_secs(86_400);
        let hour_ago = Utc::now() - chrono::Duration::hours(1);

        // Fails twice (the database or Redis still starting), then answers.
        let flaky = |answer: Option<DateTime<Utc>>| {
            let calls = Arc::new(AtomicU32::new(0));
            move || {
                let calls = Arc::clone(&calls);
                async move {
                    if calls.fetch_add(1, Ordering::Relaxed) < 2 {
                        Err(anyhow::anyhow!("not reachable yet"))
                    } else {
                        Ok(answer)
                    }
                }
            }
        };
        for mut source in [
            CursorSource::db(flaky(Some(hour_ago))),
            CursorSource::stream(flaky(Some(hour_ago))),
        ] {
            let kind = source.kind();
            let delay = time_until_next_poll_for(&mut source, day, &FAST_WAIT, None).await;
            assert!(delay > Duration::from_secs(22 * 3600), "{kind}: {delay:?}");
        }

        // Never landed (an empty stream, no freshness row): poll now.
        for mut source in [
            CursorSource::db(flaky(None)),
            CursorSource::stream(flaky(None)),
        ] {
            let delay = time_until_next_poll_for(&mut source, day, &FAST_WAIT, None).await;
            assert_eq!(delay, Duration::ZERO, "{}", source.kind());
        }

        // A source that never answers: poll now once `max_wait` runs out.
        let wait = ApiWait {
            max_wait: Duration::from_millis(100),
            ..FAST_WAIT
        };
        let mut down = CursorSource::db(|| async { Err(anyhow::anyhow!("database down")) });
        assert_eq!(
            time_until_next_poll_for(&mut down, day, &wait, None).await,
            Duration::ZERO
        );
        assert_eq!(format!("{down:?}"), "db");
    }

    #[test]
    fn no_prior_fetch_means_poll_now() {
        let now: DateTime<Utc> = "2026-01-01T00:00:00Z".parse().unwrap();
        assert_eq!(
            duration_until_next_poll(None, now, Duration::from_secs(300)),
            Duration::ZERO
        );
    }

    #[test]
    fn recent_fetch_delays_by_the_remaining_interval() {
        let now: DateTime<Utc> = "2026-01-01T00:05:00Z".parse().unwrap();
        let fetched_at: DateTime<Utc> = "2026-01-01T00:00:30Z".parse().unwrap(); // 4m30s ago
        assert_eq!(
            duration_until_next_poll(Some(fetched_at), now, Duration::from_secs(300)),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn overdue_fetch_means_poll_now() {
        let now: DateTime<Utc> = "2026-01-01T00:10:00Z".parse().unwrap();
        let fetched_at: DateTime<Utc> = "2026-01-01T00:00:00Z".parse().unwrap(); // 10m ago
        assert_eq!(
            duration_until_next_poll(Some(fetched_at), now, Duration::from_secs(300)),
            Duration::ZERO
        );
    }

    #[test]
    fn fetch_time_in_the_future_is_treated_as_just_fetched_not_a_panic() {
        // Clock skew between the api and poller hosts shouldn't be able to
        // underflow or panic. A "future" fetched_at clamps elapsed time to
        // zero (rather than a negative duration), which means "treat it as
        // just fetched" — waiting the *full* interval, not zero. That's the
        // safe choice for this feature's actual goal (avoid wasting RDM
        // quota on a redundant fetch): if the clocks disagree, assume a
        // fetch genuinely just happened rather than assume it didn't.
        let now: DateTime<Utc> = "2026-01-01T00:00:00Z".parse().unwrap();
        let fetched_at: DateTime<Utc> = "2026-01-01T00:00:10Z".parse().unwrap(); // 10s "in the future"
        assert_eq!(
            duration_until_next_poll(Some(fetched_at), now, Duration::from_secs(300)),
            Duration::from_secs(300)
        );
    }
}
