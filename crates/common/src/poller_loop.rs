//! Shared poller `main()` loop scaffolding: install metrics (if enabled),
//! compute the first-tick delay via `ingest::time_until_next_poll`, then
//! loop forever recording `poller_cycle_duration_seconds`/
//! `poller_cycle_total` and logging cycle errors. Previously duplicated,
//! byte-identical apart from one metric label string, across
//! `poller-incidents`/`poller-stations`/`poller-tocs`/`poller-ldbws`/
//! `poller-tfl`'s own `main()` functions -- see
//! docs/superpowers/specs/2026-09-05-rust-service-deduplication-design.md
//! §3.1.
//!
//! `poll_once` and any pre-flight check stay in each poller's own
//! `main.rs`, called as today -- only this wrapper is shared. A poller
//! with cycle-to-cycle mutable state (`poller-tfl`'s own `DlrMatchState`)
//! keeps owning that state in its own `main()` and captures it by mutable
//! reference in the `cycle` closure passed in here -- ordinary `FnMut`
//! semantics, no change needed to this function's own signature.

use std::future::Future;
use std::time::Duration;

use crate::backoff::Backoff;
use crate::ingest;
use crate::oauth_client::OAuthTokenCache;
use crate::progress::Progress;

/// Builds the poll-cycle `tokio::time::Interval`, first ticking at `start`
/// and thereafter every `poll_interval` -- with `MissedTickBehavior::Delay`
/// rather than the default `Burst`.
///
/// `Burst` fires every missed tick back-to-back with zero gap once a cycle
/// overruns `poll_interval` (a slow upstream API, a timeout pile-up) --
/// directly multiplying calls against whatever rate-limited endpoint
/// `time_until_next_poll` exists to protect (Finding #2). `Delay` instead
/// waits a fresh `poll_interval` from whenever the overrun tick actually
/// completes, so a slow cycle never causes a burst of immediate follow-up
/// calls. Split out from `run_poll_loop` itself so this configuration is
/// directly assertable in a unit test, since the missed-tick BEHAVIOR
/// (skipping ticks under a real overrun) isn't practically observable
/// without a slow, flaky, real-time test.
fn poll_interval_with_delay_on_overrun(
    start: tokio::time::Instant,
    poll_interval: Duration,
) -> tokio::time::Interval {
    let mut interval = tokio::time::interval_at(start, poll_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
}

/// Retry timing for [`run_poll_loop`] (SVC-09). A parameter only so tests
/// can use millisecond timings; production always uses
/// [`DEFAULT_RETRY_POLICY`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Wait for `api` at startup, and again before a retry after a failed
    /// cycle, so no upstream fetch is spent while `api` is unreachable.
    pub api_wait: ingest::ApiWait,
    /// Delay before re-running a cycle that failed transiently, capped at
    /// the poll interval (so a frequent poller's schedule is unchanged).
    pub failed_cycle: Backoff,
}

/// 1 min doubling to 1 h between retries of a failed cycle: an api restart
/// costs a daily poller minutes, not a day, while a persistent failure
/// still re-fetches upstream at most about once an hour.
pub const DEFAULT_RETRY_POLICY: RetryPolicy = RetryPolicy {
    api_wait: ingest::API_STARTUP_WAIT,
    failed_cycle: Backoff::new(Duration::from_secs(60), Duration::from_secs(3600)),
};

/// How long a poller's ingest POST may retry a transient failure within one
/// cycle (`ingest::post_batch_retrying`): a quarter of the poll interval,
/// capped at 15 minutes. A 60s poller retries for 15s, so it never runs into
/// its next fresh fetch; a 24h poller keeps its once-a-day upstream fetch
/// alive through an api restart.
pub fn post_retry_budget(poll_interval: Duration) -> Duration {
    (poll_interval / 4).min(Duration::from_secs(15 * 60))
}

/// When to re-run a cycle that just failed: a data rejection (the same body
/// will be refused again) waits the normal interval; anything else retries
/// after `failed_cycle.delay(consecutive_failures)`, never later than the
/// normal interval.
fn delay_after_failed_cycle(
    policy: &RetryPolicy,
    poll_interval: Duration,
    class: ingest::FailureClass,
    consecutive_failures: u32,
) -> Duration {
    match class {
        ingest::FailureClass::Rejected => poll_interval,
        ingest::FailureClass::Transient => policy
            .failed_cycle
            .delay(consecutive_failures)
            .min(poll_interval),
    }
}

// Every poller's own single-call-site scaffolding function, threading
// every per-cycle config knob straight through -- same posture as
// `aggregator`/`full-coverage-consumer`/`schedule-ingest`'s own
// `#[allow(clippy::too_many_arguments)]` on their analogous top-level loop
// functions.
#[expect(
    clippy::too_many_arguments,
    reason = "each argument is an independent input from the single caller; a struct would only wrap them"
)]
pub async fn run_poll_loop<F, Fut>(
    poller_label: &'static str,
    client: &reqwest::Client,
    api_ingest_url: &str,
    internal_oauth: &OAuthTokenCache,
    poll_interval: Duration,
    metrics_enabled: bool,
    metrics_port: u16,
    progress: &Progress,
    cycle: F,
) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    if metrics_enabled {
        crate::metrics::install(metrics_port)?;
    }
    run_poll_loop_with(
        &DEFAULT_RETRY_POLICY,
        poller_label,
        client,
        api_ingest_url,
        internal_oauth,
        poll_interval,
        progress,
        cycle,
    )
    .await
}

/// [`run_poll_loop`] with the caller's own [`RetryPolicy`] instead of
/// [`DEFAULT_RETRY_POLICY`]. `poller-ldbws` passes one whose `api_wait`
/// gives up after a single attempt: it keeps sampling upstream through an
/// api outage and holds the unsent samples itself, so waiting for api
/// before each cycle would only lose samples.
#[expect(
    clippy::too_many_arguments,
    reason = "each argument is an independent input from the single caller; a struct would only wrap them"
)]
pub async fn run_poll_loop_with_policy<F, Fut>(
    policy: &RetryPolicy,
    poller_label: &'static str,
    client: &reqwest::Client,
    api_ingest_url: &str,
    internal_oauth: &OAuthTokenCache,
    poll_interval: Duration,
    metrics_enabled: bool,
    metrics_port: u16,
    progress: &Progress,
    cycle: F,
) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    if metrics_enabled {
        crate::metrics::install(metrics_port)?;
    }
    run_poll_loop_with(
        policy,
        poller_label,
        client,
        api_ingest_url,
        internal_oauth,
        poll_interval,
        progress,
        cycle,
    )
    .await
}

/// [`run_poll_loop`] with the last-fetch time read by `last_fetched`
/// instead of the api's GET on `api_ingest_url`: any
/// [`ingest::LastFetched`], a closure or an [`ingest::CursorSource`]. A
/// direct writer (ingest architecture phase 2, `INGEST_SINK=db`) reads its
/// own `ingest_freshness` row, a stream producer (phase 3) its stream's
/// newest entry. The startup wait and the wait before a retry use it the
/// same way, with the same [`DEFAULT_RETRY_POLICY`].
pub async fn run_poll_loop_with_cursor<C, F, Fut>(
    poller_label: &'static str,
    last_fetched: C,
    poll_interval: Duration,
    metrics_enabled: bool,
    metrics_port: u16,
    progress: &Progress,
    cycle: F,
) -> anyhow::Result<()>
where
    C: ingest::LastFetched,
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    run_poll_loop_with_policy_and_cursor(
        &DEFAULT_RETRY_POLICY,
        poller_label,
        last_fetched,
        poll_interval,
        metrics_enabled,
        metrics_port,
        progress,
        cycle,
    )
    .await
}

/// [`run_poll_loop_with_cursor`] with the caller's own [`RetryPolicy`]:
/// the combination of [`run_poll_loop_with_policy`] (a held-sample poller
/// such as `poller-ldbws`) and a direct writer's own last-fetch cursor.
#[expect(
    clippy::too_many_arguments,
    reason = "each argument is an independent input from the single caller; a struct would only wrap them"
)]
pub async fn run_poll_loop_with_policy_and_cursor<C, F, Fut>(
    policy: &RetryPolicy,
    poller_label: &'static str,
    last_fetched: C,
    poll_interval: Duration,
    metrics_enabled: bool,
    metrics_port: u16,
    progress: &Progress,
    cycle: F,
) -> anyhow::Result<()>
where
    C: ingest::LastFetched,
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    if metrics_enabled {
        crate::metrics::install(metrics_port)?;
    }
    run_cursor_loop_with(
        policy,
        poller_label,
        last_fetched,
        poll_interval,
        progress,
        cycle,
    )
    .await
}

/// [`run_poll_loop`] with the startup cursor read from `source` (ingest
/// architecture plan 4.6): the api's GET ([`ingest::CursorSource::Http`],
/// what [`run_poll_loop`] itself uses), the poller's own freshness row
/// (`Db`, a direct writer) or its own stream's newest entry (`Stream`, a
/// stream producer). Whichever it is, the first poll waits out what is
/// left of `poll_interval`, an unreachable source is waited for (bounded,
/// [`DEFAULT_RETRY_POLICY`]) at startup and before a retry after a failed
/// cycle, and a source that never answers means "poll now". The same loop
/// as [`run_poll_loop_with_cursor`], which it calls after logging the
/// source's kind.
pub async fn run_poll_loop_with_source<F, Fut>(
    poller_label: &'static str,
    source: ingest::CursorSource<'_>,
    poll_interval: Duration,
    metrics_enabled: bool,
    metrics_port: u16,
    progress: &Progress,
    cycle: F,
) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    tracing::info!(
        cursor_source = source.kind(),
        "reading the last-fetch cursor"
    );
    run_poll_loop_with_cursor(
        poller_label,
        source,
        poll_interval,
        metrics_enabled,
        metrics_port,
        progress,
        cycle,
    )
    .await
}

/// The `<service>` of [`crate::metrics::register_cycle`]'s metric names for
/// every poller: `distant_signal_poller_last_success_timestamp_seconds` and
/// `distant_signal_poller_cycles_total`, with the poller's label as
/// `cycle`. The latter repeats `poller_cycle_total{poller}` (which the
/// existing alerts read) under the shared helper's name; the gauge is the
/// new signal.
pub const LAST_SUCCESS_SERVICE: &str = "poller";

/// Registers both `result` series of `poller_cycle_total` at 0 for
/// `poller_label`, so an alert's `increase()` sees the first failure (or
/// success) after a pod start rather than the series merely appearing.
fn register_cycle_metrics(poller_label: &'static str) {
    // `poller_last_success_timestamp_seconds{cycle=<poller>}`: the chart's
    // DistantSignalPollerStale reads it (see `common::metrics::register_cycle`).
    crate::metrics::register_cycle(LAST_SUCCESS_SERVICE, poller_label);
    for result in ["success", "failure"] {
        metrics::counter!(
            crate::metrics::metric_name("poller_cycle_total"),
            "poller" => poller_label,
            "result" => result
        )
        .increment(0);
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "each argument is an independent input from the single caller; a struct would only wrap them"
)]
async fn run_poll_loop_with<F, Fut>(
    policy: &RetryPolicy,
    poller_label: &'static str,
    client: &reqwest::Client,
    api_ingest_url: &str,
    internal_oauth: &OAuthTokenCache,
    poll_interval: Duration,
    progress: &Progress,
    cycle: F,
) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    run_cursor_loop_with(
        policy,
        poller_label,
        ingest::CursorSource::http(client, api_ingest_url, internal_oauth),
        poll_interval,
        progress,
        cycle,
    )
    .await
}

/// The loop every entry point shares, over any [`ingest::LastFetched`]: a
/// [`ingest::CursorSource`], or a closure.
async fn run_cursor_loop_with<C, F, Fut>(
    policy: &RetryPolicy,
    poller_label: &'static str,
    mut last_fetched: C,
    poll_interval: Duration,
    progress: &Progress,
    cycle: F,
) -> anyhow::Result<()>
where
    C: ingest::LastFetched,
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    register_cycle_metrics(poller_label);
    let delay = ingest::time_until_next_poll_for(
        &mut last_fetched,
        poll_interval,
        &policy.api_wait,
        Some(progress),
    )
    .await;
    run_cycles(
        policy,
        poller_label,
        poll_interval,
        progress,
        delay,
        // An async closure: the retry wait borrows `last_fetched` mutably.
        async || {
            // A retry after a failure: don't spend an upstream fetch until
            // the last-fetch source (api, or the database for a direct
            // writer) answers again (bounded, so a GET-only breakage can't
            // stop polling).
            if let Err(err) =
                ingest::wait_for_source(&mut last_fetched, &policy.api_wait, Some(progress)).await
            {
                tracing::warn!(error = ?err, "last-fetch source still unreachable; retrying the poll anyway");
            }
        },
        cycle,
    )
    .await
}

/// The loop both entry points share: the first tick after `delay`, then
/// every `poll_interval`; `before_retry` runs before a cycle that follows a
/// failed one.
async fn run_cycles<F, Fut, R>(
    policy: &RetryPolicy,
    poller_label: &'static str,
    poll_interval: Duration,
    progress: &Progress,
    delay: Duration,
    mut before_retry: R,
    mut cycle: F,
) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
    R: AsyncFnMut(),
{
    if !delay.is_zero() {
        tracing::info!(
            delay_secs = delay.as_secs(),
            "data still fresh from a prior run; delaying first poll"
        );
    }
    let mut interval =
        poll_interval_with_delay_on_overrun(tokio::time::Instant::now() + delay, poll_interval);
    let mut consecutive_failures: u32 = 0;

    loop {
        progress.idle(interval.tick()).await;

        if consecutive_failures > 0 {
            before_retry().await;
        }

        let cycle_start = std::time::Instant::now();
        let result = cycle().await;
        progress.beat();
        metrics::histogram!(
            crate::metrics::metric_name("poller_cycle_duration_seconds"),
            "poller" => poller_label
        )
        .record(cycle_start.elapsed().as_secs_f64());
        metrics::counter!(
            crate::metrics::metric_name("poller_cycle_total"),
            "poller" => poller_label,
            "result" => if result.is_ok() { "success" } else { "failure" }
        )
        .increment(1);
        crate::metrics::record_cycle(LAST_SUCCESS_SERVICE, poller_label, result.is_ok());

        match result {
            Ok(()) => consecutive_failures = 0,
            Err(err) => {
                let retry_in = delay_after_failed_cycle(
                    policy,
                    poll_interval,
                    ingest::classify_failure(&err),
                    consecutive_failures,
                );
                consecutive_failures = consecutive_failures.saturating_add(1);
                if retry_in < poll_interval {
                    tracing::error!(
                        error = ?err,
                        retry_in_secs = retry_in.as_secs(),
                        "poll cycle failed; retrying soon"
                    );
                    interval.reset_after(retry_in);
                } else {
                    tracing::error!(error = ?err, "poll cycle failed; will retry next interval");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::oauth_client::{OAuthCredentials, OAuthTokenCache};

    async fn token_cache(server: &MockServer) -> OAuthTokenCache {
        Mock::given(method("POST"))
            .and(path("/token/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "fake-jwt",
                "expires_in": 300,
            })))
            .mount(server)
            .await;
        OAuthTokenCache::new(OAuthCredentials {
            token_url: format!("{}/token/", server.uri()),
            client_id: "test".to_string(),
            scope: "groups".to_string(),
            username: "test".to_string(),
            password: "test".to_string(),
        })
    }

    /// Both `result` series exist at 0 before any cycle runs, so the
    /// first failure after a pod start is an `increase()` Prometheus sees.
    #[test]
    fn both_cycle_result_series_are_registered_at_zero() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        register_cycle_metrics("test-poller");
        let rendered = handle.render();
        for result in ["success", "failure"] {
            let line = format!(
                "distant_signal_poller_cycle_total{{poller=\"test-poller\",result=\"{result}\"}} 0"
            );
            assert!(rendered.contains(&line), "missing {line} in:\n{rendered}");
        }
    }

    /// Finding #2 regression: the interval `run_poll_loop` ticks on must be
    /// configured with `MissedTickBehavior::Delay`, not the default
    /// `Burst`, so an overrun cycle doesn't fire a burst of back-to-back
    /// catch-up ticks against a rate-limited upstream. The behavior itself
    /// (skipping ticks under a real overrun) isn't practically assertable
    /// without a slow, timing-flaky test, so this asserts the configuration
    /// directly via `Interval::missed_tick_behavior()`.
    #[tokio::test]
    async fn poll_interval_defaults_to_delay_not_burst_on_a_missed_tick() {
        let interval = poll_interval_with_delay_on_overrun(
            tokio::time::Instant::now(),
            Duration::from_secs(60),
        );
        assert_eq!(
            interval.missed_tick_behavior(),
            tokio::time::MissedTickBehavior::Delay,
            "an overrun poll cycle must not burst-fire every missed tick back-to-back against a \
             rate-limited upstream"
        );
    }

    /// Not a full loop run (this function never returns) -- confirms the
    /// cycle closure is actually invoked and its result recorded, by
    /// racing the loop against a timeout and asserting at least one
    /// invocation happened. Mirrors this crate's existing
    /// `time_until_next_poll` tests' preference for a real (mocked) HTTP
    /// round trip over a fake clock abstraction.
    #[tokio::test]
    async fn run_poll_loop_invokes_the_cycle_closure_on_each_tick() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "fetchedAt": null
            })))
            .mount(&server)
            .await;
        let tokens = token_cache(&server).await;
        let client = reqwest::Client::new();
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_for_cycle = Arc::clone(&call_count);
        let ingest_url = format!("{}/ingest", server.uri());

        let progress = Progress::new(Duration::from_secs(60));
        let loop_future = run_poll_loop(
            "test",
            &client,
            &ingest_url,
            &tokens,
            Duration::from_millis(10),
            false,
            0,
            &progress,
            || {
                call_count_for_cycle.fetch_add(1, Ordering::Relaxed);
                async { Ok(()) }
            },
        );

        let _ = tokio::time::timeout(Duration::from_millis(100), loop_future).await;
        assert!(
            call_count.load(Ordering::Relaxed) >= 1,
            "the cycle closure must run at least once within 100ms at a 10ms interval"
        );
    }

    const FAST_POLICY: RetryPolicy = RetryPolicy {
        api_wait: ingest::ApiWait {
            backoff: Backoff::new(Duration::from_millis(10), Duration::from_millis(20)),
            max_wait: Duration::from_secs(5),
        },
        failed_cycle: Backoff::new(Duration::from_millis(50), Duration::from_millis(100)),
    };

    fn transient_post_failure() -> anyhow::Error {
        ingest::HttpStatusError {
            prefix: "ingestion POST failed",
            status: reqwest::StatusCode::BAD_GATEWAY,
            body: String::new(),
            retry_after: None,
        }
        .into()
    }

    /// Plan 4.6, the direct-writer (`Db`) and stream-producer (`Stream`)
    /// cursors: a fresh cursor delays the first poll, a stale one polls at
    /// once, and a failed cycle retries on the failed-cycle backoff after
    /// the cursor answers again.
    #[tokio::test]
    async fn the_loop_waits_out_a_fresh_cursor_from_any_source() {
        type Kind = fn(ingest::CursorFn<'static>) -> ingest::CursorSource<'static>;
        let kinds: [(&str, Kind); 2] = [
            ("db", ingest::CursorSource::Db),
            ("stream", ingest::CursorSource::Stream),
        ];
        let progress = Progress::new(Duration::from_secs(60));
        for (kind, make) in kinds {
            // Fetched just now with a daily interval: no poll within the test.
            let calls = Arc::new(AtomicUsize::new(0));
            let calls_in_cycle = Arc::clone(&calls);
            let fresh = make(Box::new(|| {
                Box::pin(async { Ok(Some(chrono::Utc::now())) })
            }));
            let loop_future = run_cursor_loop_with(
                &FAST_POLICY,
                "test",
                fresh,
                Duration::from_secs(86_400),
                &progress,
                || {
                    calls_in_cycle.fetch_add(1, Ordering::Relaxed);
                    async { Ok(()) }
                },
            );
            let _ = tokio::time::timeout(Duration::from_millis(200), loop_future).await;
            assert_eq!(calls.load(Ordering::Relaxed), 0, "{kind}: still fresh");

            // Never fetched: polls at once; the failure is retried soon.
            let calls = Arc::new(AtomicUsize::new(0));
            let calls_in_cycle = Arc::clone(&calls);
            let never = make(Box::new(|| Box::pin(async { Ok(None) })));
            let loop_future = run_cursor_loop_with(
                &FAST_POLICY,
                "test",
                never,
                Duration::from_secs(86_400),
                &progress,
                || {
                    let n = calls_in_cycle.fetch_add(1, Ordering::Relaxed);
                    async move {
                        if n == 0 {
                            Err(anyhow::anyhow!("database unavailable"))
                        } else {
                            Ok(())
                        }
                    }
                },
            );
            let _ = tokio::time::timeout(Duration::from_secs(2), loop_future).await;
            assert_eq!(
                calls.load(Ordering::Relaxed),
                2,
                "{kind}: first poll at once, one retry after the failure, then the daily interval"
            );
        }
    }

    /// SVC-09: a daily poller whose POST failed (api not up yet after a
    /// reboot) retries within the failed-cycle backoff, not a whole
    /// interval later.
    #[tokio::test]
    async fn a_failed_cycle_is_retried_soon_not_after_the_full_interval() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "fetchedAt": null
            })))
            .mount(&server)
            .await;
        let tokens = token_cache(&server).await;
        let client = reqwest::Client::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in_cycle = Arc::clone(&calls);
        let ingest_url = format!("{}/ingest", server.uri());
        let progress = Progress::new(Duration::from_secs(60));

        let loop_future = run_poll_loop_with(
            &FAST_POLICY,
            "test",
            &client,
            &ingest_url,
            &tokens,
            Duration::from_secs(86_400),
            &progress,
            || {
                let n = calls_in_cycle.fetch_add(1, Ordering::Relaxed);
                async move {
                    if n == 0 {
                        Err(transient_post_failure())
                    } else {
                        Ok(())
                    }
                }
            },
        );
        let _ = tokio::time::timeout(Duration::from_secs(2), loop_future).await;
        assert_eq!(
            calls.load(Ordering::Relaxed),
            2,
            "one failed cycle, one prompt retry, then back to the 24h interval"
        );
    }

    /// Every poller exports its last successful cycle through the shared
    /// helper: registered at start, left alone by a failure, and counted.
    #[tokio::test]
    async fn the_loop_exports_the_last_successful_cycle() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "fetchedAt": null
            })))
            .mount(&server)
            .await;
        let tokens = token_cache(&server).await;
        let client = reqwest::Client::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in_cycle = Arc::clone(&calls);
        let ingest_url = format!("{}/ingest", server.uri());
        let progress = Progress::new(Duration::from_secs(60));
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);

        let loop_future = run_poll_loop_with(
            &FAST_POLICY,
            "test-poller",
            &client,
            &ingest_url,
            &tokens,
            Duration::from_secs(86_400),
            &progress,
            || {
                let n = calls_in_cycle.fetch_add(1, Ordering::Relaxed);
                async move {
                    if n == 0 {
                        Err(transient_post_failure())
                    } else {
                        Ok(())
                    }
                }
            },
        );
        let _ = tokio::time::timeout(Duration::from_secs(2), loop_future).await;

        let rendered = handle.render();
        for series in [
            r#"distant_signal_poller_cycles_total{cycle="test-poller",result="failure"} 1"#,
            r#"distant_signal_poller_cycles_total{cycle="test-poller",result="success"} 1"#,
            r#"distant_signal_poller_last_success_timestamp_seconds{cycle="test-poller"}"#,
        ] {
            assert!(
                rendered.contains(series),
                "{series} missing from {rendered}"
            );
        }
    }

    /// A policy whose `api_wait` gives up at once (`poller-ldbws`'s) runs
    /// its cycles on schedule through an api outage instead of waiting for
    /// api first: here api never answers, and the cycle still runs at once,
    /// then again after the failed-cycle backoff.
    #[tokio::test]
    async fn a_zero_api_wait_policy_polls_through_an_api_outage() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/ingest"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let tokens = token_cache(&server).await;
        let client = reqwest::Client::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in_cycle = Arc::clone(&calls);
        let ingest_url = format!("{}/ingest", server.uri());
        let progress = Progress::new(Duration::from_secs(60));
        let policy = RetryPolicy {
            api_wait: ingest::ApiWait {
                backoff: Backoff::new(Duration::from_secs(30), Duration::from_secs(30)),
                max_wait: Duration::ZERO,
            },
            failed_cycle: Backoff::new(Duration::from_millis(50), Duration::from_millis(50)),
        };

        let loop_future = run_poll_loop_with(
            &policy,
            "test",
            &client,
            &ingest_url,
            &tokens,
            Duration::from_millis(100),
            &progress,
            || {
                calls_in_cycle.fetch_add(1, Ordering::Relaxed);
                async { Err(transient_post_failure()) }
            },
        );
        // With the default 10-minute wait, no cycle would run in this time.
        let _ = tokio::time::timeout(Duration::from_millis(500), loop_future).await;
        assert!(
            calls.load(Ordering::Relaxed) >= 3,
            "cycles kept running while api was down: {}",
            calls.load(Ordering::Relaxed)
        );
    }

    /// A data rejection will be refused again: it waits the normal interval.
    #[tokio::test]
    async fn a_rejected_cycle_waits_the_full_interval() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "fetchedAt": null
            })))
            .mount(&server)
            .await;
        let tokens = token_cache(&server).await;
        let client = reqwest::Client::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in_cycle = Arc::clone(&calls);
        let ingest_url = format!("{}/ingest", server.uri());
        let progress = Progress::new(Duration::from_secs(60));

        let loop_future = run_poll_loop_with(
            &FAST_POLICY,
            "test",
            &client,
            &ingest_url,
            &tokens,
            Duration::from_secs(86_400),
            &progress,
            || {
                calls_in_cycle.fetch_add(1, Ordering::Relaxed);
                async {
                    Err(ingest::HttpStatusError {
                        prefix: "ingestion POST failed",
                        status: reqwest::StatusCode::UNPROCESSABLE_ENTITY,
                        body: String::new(),
                        retry_after: None,
                    }
                    .into())
                }
            },
        );
        let _ = tokio::time::timeout(Duration::from_millis(500), loop_future).await;
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    /// A direct writer's cursor (plan 2b.1): a last fetch from just now
    /// delays the first poll by the interval; none polls at once, and the
    /// cursor is read again before the retry of a failed cycle.
    #[tokio::test]
    async fn the_cursor_loop_reads_the_last_fetch_from_its_closure() {
        async fn run(fetched_at: Option<chrono::DateTime<chrono::Utc>>) -> (usize, usize) {
            let reads = AtomicUsize::new(0);
            let calls = AtomicUsize::new(0);
            let progress = Progress::new(Duration::from_secs(60));
            let loop_future = run_cursor_loop_with(
                &FAST_POLICY,
                "test",
                || {
                    reads.fetch_add(1, Ordering::Relaxed);
                    async move { Ok(fetched_at) }
                },
                Duration::from_secs(86_400),
                &progress,
                || {
                    let n = calls.fetch_add(1, Ordering::Relaxed);
                    async move {
                        if n == 0 {
                            Err(transient_post_failure())
                        } else {
                            Ok(())
                        }
                    }
                },
            );
            let _ = tokio::time::timeout(Duration::from_millis(500), loop_future).await;
            (reads.load(Ordering::Relaxed), calls.load(Ordering::Relaxed))
        }

        assert_eq!(run(Some(chrono::Utc::now())).await, (1, 0));
        // Never fetched: polls at once, fails, reads the cursor again, retries.
        assert_eq!(run(None).await, (2, 2));
    }

    #[test]
    fn a_failed_cycle_retry_is_capped_at_the_poll_interval() {
        let minute = Duration::from_secs(60);
        for n in 0..40 {
            let d = delay_after_failed_cycle(
                &DEFAULT_RETRY_POLICY,
                minute,
                ingest::FailureClass::Transient,
                n,
            );
            assert!(d <= minute);
        }
        let day = Duration::from_secs(86_400);
        let first = delay_after_failed_cycle(
            &DEFAULT_RETRY_POLICY,
            day,
            ingest::FailureClass::Transient,
            0,
        );
        assert!(first <= Duration::from_secs(60), "{first:?}");
        let later = delay_after_failed_cycle(
            &DEFAULT_RETRY_POLICY,
            day,
            ingest::FailureClass::Transient,
            30,
        );
        assert!(later <= Duration::from_secs(3600), "{later:?}");
        assert_eq!(
            delay_after_failed_cycle(
                &DEFAULT_RETRY_POLICY,
                day,
                ingest::FailureClass::Rejected,
                0
            ),
            day
        );
    }

    #[test]
    fn post_retry_budget_scales_with_the_interval_and_is_capped() {
        assert_eq!(
            post_retry_budget(Duration::from_secs(60)),
            Duration::from_secs(15)
        );
        assert_eq!(
            post_retry_budget(Duration::from_secs(86_400)),
            Duration::from_secs(15 * 60)
        );
    }
}
