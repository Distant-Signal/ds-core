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

use crate::ingest;
use crate::oauth_client::OAuthTokenCache;

// Every poller's own single-call-site scaffolding function, threading
// every per-cycle config knob straight through -- same posture as
// `aggregator`/`full-coverage-consumer`/`schedule-ingest`'s own
// `#[allow(clippy::too_many_arguments)]` on their analogous top-level loop
// functions.
#[allow(clippy::too_many_arguments)]
pub async fn run_poll_loop<F, Fut>(
    poller_label: &'static str,
    client: &reqwest::Client,
    api_ingest_url: &str,
    internal_oauth: &OAuthTokenCache,
    poll_interval: Duration,
    metrics_enabled: bool,
    metrics_port: u16,
    mut cycle: F,
) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    if metrics_enabled {
        crate::metrics::install(metrics_port)?;
    }

    let delay =
        ingest::time_until_next_poll(client, api_ingest_url, internal_oauth, poll_interval).await;
    if !delay.is_zero() {
        tracing::info!(
            delay_secs = delay.as_secs(),
            "data still fresh from a prior run; delaying first poll"
        );
    }
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + delay, poll_interval);

    loop {
        interval.tick().await;

        let cycle_start = std::time::Instant::now();
        let result = cycle().await;
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

        if let Err(err) = result {
            tracing::error!(error = ?err, "poll cycle failed; will retry next interval");
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

        let loop_future = run_poll_loop(
            "test",
            &client,
            &ingest_url,
            &tokens,
            Duration::from_millis(10),
            false,
            0,
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
}
