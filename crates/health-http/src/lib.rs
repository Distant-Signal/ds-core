//! Shared health/readiness HTTP endpoint: `/healthz` backed by an
//! `AtomicBool`, plus a matching Prometheus readiness gauge update.
//! Previously duplicated near-verbatim across `trust-consumer`,
//! `full-coverage-consumer` (character-for-character identical apart from
//! one gauge-name string), and (a close structural cousin, deliberately
//! different readiness semantics) `movement-relay`. See
//! docs/superpowers/specs/2026-09-05-rust-service-deduplication-design.md
//! §3.3 for the full per-caller verification.
//!
//! `healthy_text`/`unhealthy_text` and `gauge_name` are parameters, not
//! hardcoded, so every real caller's own wire-visible `/healthz` response
//! body and Prometheus gauge name stay byte-for-byte unchanged after
//! adopting this shared module -- this is a pure refactor, not a
//! behavior-unifying one.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use axum::routing::get;

/// `true` once the caller's own connection/consumer is confirmed live;
/// `false` from startup and whenever disconnected. Shared, not
/// crate-local -- every real caller today used exactly this type alias
/// (`Arc<AtomicBool>`) under its own crate-local name.
pub type ConnectionState = Arc<AtomicBool>;

/// Liveness-by-progress for a consume loop: the loop calls [`Progress::beat`]
/// once per completed iteration, and `/healthz` reports unhealthy once no
/// beat has arrived for `stall_after`.
///
/// `ConnectionState` alone cannot catch a wedged loop: it is only written by
/// the feed's own `next_batch`, so a loop stuck forever inside an `await`
/// after that (a half-open HTTP connection with no timeout, the PL-1
/// incident) leaves it at its last value -- `true` -- and the liveness probe
/// never restarts the pod.
#[derive(Debug, Clone)]
pub struct Progress {
    origin: Instant,
    /// Milliseconds after `origin` of the last beat.
    last_beat_ms: Arc<AtomicU64>,
    stall_after: Duration,
}

impl Progress {
    /// Starts "just beaten", so a fresh process gets a full `stall_after`
    /// of grace before its first iteration has to complete.
    pub fn new(stall_after: Duration) -> Self {
        Self {
            origin: Instant::now(),
            last_beat_ms: Arc::new(AtomicU64::new(0)),
            stall_after,
        }
    }

    fn elapsed_ms(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// Records that the loop completed an iteration.
    pub fn beat(&self) {
        self.last_beat_ms
            .store(self.elapsed_ms(), Ordering::Relaxed);
    }

    /// How long since the last beat.
    pub fn since_last_beat(&self) -> Duration {
        Duration::from_millis(
            self.elapsed_ms()
                .saturating_sub(self.last_beat_ms.load(Ordering::Relaxed)),
        )
    }

    pub fn is_stalled(&self) -> bool {
        self.since_last_beat() > self.stall_after
    }
}

/// [`spawn`], plus a [`Progress`] watchdog: `/healthz` answers 503 with
/// `"stalled"` whenever no iteration has completed within `stall_after`,
/// whatever the connection state says.
pub fn spawn_with_progress(
    bind_url: String,
    healthy_text: &'static str,
    unhealthy_text: &'static str,
    stall_after: Duration,
) -> (ConnectionState, Progress) {
    let state: ConnectionState = Arc::new(AtomicBool::new(false));
    let progress = Progress::new(stall_after);
    serve(
        bind_url,
        Arc::clone(&state),
        Some(progress.clone()),
        healthy_text,
        unhealthy_text,
    );
    (state, progress)
}

/// Creates a fresh `ConnectionState` and starts the `/healthz` server.
/// Matches `trust-consumer`/`full-coverage-consumer`'s own current call
/// shape (`health::spawn(bind_url)`).
pub fn spawn(
    bind_url: String,
    healthy_text: &'static str,
    unhealthy_text: &'static str,
) -> ConnectionState {
    let state: ConnectionState = Arc::new(AtomicBool::new(false));
    spawn_with_state(bind_url, Arc::clone(&state), healthy_text, unhealthy_text);
    state
}

/// Starts the `/healthz` server against an already-constructed state.
/// Matches `movement-relay`'s own current call shape
/// (`health::spawn(bind_url, ready)`, where `ready` is created earlier and
/// owned by `RelayContext`).
pub fn spawn_with_state(
    bind_url: String,
    state: ConnectionState,
    healthy_text: &'static str,
    unhealthy_text: &'static str,
) {
    serve(bind_url, state, None, healthy_text, unhealthy_text);
}

fn serve(
    bind_url: String,
    state: ConnectionState,
    progress: Option<Progress>,
    healthy_text: &'static str,
    unhealthy_text: &'static str,
) {
    tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/healthz",
            get(move || {
                healthz(
                    Arc::clone(&state),
                    progress.clone(),
                    healthy_text,
                    unhealthy_text,
                )
            }),
        );
        let listener = match tokio::net::TcpListener::bind(&bind_url).await {
            Ok(listener) => listener,
            Err(err) => {
                tracing::error!(error = ?err, bind_url, "failed to bind health endpoint");
                return;
            }
        };
        if let Err(err) = axum::serve(listener, app).await {
            tracing::error!(error = ?err, "health endpoint server stopped");
        }
    });
}

async fn healthz(
    state: ConnectionState,
    progress: Option<Progress>,
    healthy_text: &'static str,
    unhealthy_text: &'static str,
) -> (StatusCode, &'static str) {
    if let Some(progress) = progress.filter(Progress::is_stalled) {
        tracing::warn!(
            since_last_beat_secs = progress.since_last_beat().as_secs(),
            stall_after_secs = progress.stall_after.as_secs(),
            "no consume-loop iteration has completed within the stall window; reporting unhealthy"
        );
        return (StatusCode::SERVICE_UNAVAILABLE, "stalled");
    }
    if state.load(Ordering::Relaxed) {
        (StatusCode::OK, healthy_text)
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, unhealthy_text)
    }
}

/// Centralizes every `ConnectionState` transition with a matching
/// Prometheus gauge update, so the `AtomicBool` and the readiness gauge
/// never drift out of sync. `gauge_name` is a parameter (e.g.
/// `"trust_consumer_ready"`, `"full_coverage_consumer_ready"`,
/// `"movement_relay_ready"`) instead of three copy-pasted hardcoded
/// strings -- every real caller passes the exact string it emits today.
pub fn set_connected(state: &ConnectionState, gauge_name: &str, connected: bool) {
    state.store(connected, Ordering::Relaxed);
    metrics::gauge!(common::metrics::metric_name(gauge_name)).set(if connected {
        1.0
    } else {
        0.0
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn set_connected_updates_the_shared_atomic_state() {
        let state: ConnectionState = Arc::new(AtomicBool::new(false));

        set_connected(&state, "test_ready", true);
        assert!(state.load(Ordering::Relaxed));

        set_connected(&state, "test_ready", false);
        assert!(!state.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn healthz_reports_the_caller_supplied_text_for_each_state() {
        let state: ConnectionState = Arc::new(AtomicBool::new(false));
        let (status, body) = healthz(Arc::clone(&state), None, "connected", "disconnected").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "disconnected");

        state.store(true, Ordering::Relaxed);
        let (status, body) = healthz(Arc::clone(&state), None, "connected", "disconnected").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "connected");
    }

    /// PL-1: a connected consumer whose loop has stopped completing
    /// iterations must go unhealthy, so the liveness probe restarts it.
    #[tokio::test]
    async fn healthz_goes_unhealthy_when_the_loop_stops_making_progress() {
        let state: ConnectionState = Arc::new(AtomicBool::new(true));
        let progress = Progress::new(Duration::from_millis(100));

        let (status, _) = healthz(
            Arc::clone(&state),
            Some(progress.clone()),
            "connected",
            "disconnected",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "within the startup grace period");

        tokio::time::sleep(Duration::from_millis(250)).await;
        let (status, body) = healthz(
            Arc::clone(&state),
            Some(progress.clone()),
            "connected",
            "disconnected",
        )
        .await;
        assert_eq!(
            (status, body),
            (StatusCode::SERVICE_UNAVAILABLE, "stalled"),
            "still 'connected', but nothing has completed: a wedge"
        );

        progress.beat();
        let (status, _) = healthz(
            Arc::clone(&state),
            Some(progress.clone()),
            "connected",
            "disconnected",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "a fresh beat recovers");
    }
}
