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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::http::StatusCode;
use axum::routing::get;

/// `true` once the caller's own connection/consumer is confirmed live;
/// `false` from startup and whenever disconnected. Shared, not
/// crate-local -- every real caller today used exactly this type alias
/// (`Arc<AtomicBool>`) under its own crate-local name.
pub type ConnectionState = Arc<AtomicBool>;

/// Liveness-by-progress; see [`common::progress`]. Re-exported so every
/// caller keeps using `health_http::Progress`.
pub use common::progress::Progress;

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

/// The health listener for a background worker (SVC-08/INF-9), from its
/// `HealthArgs`: `/livez` for the liveness probe (progress only) and
/// `/healthz` for readiness (also 503 `"connecting"` until the caller flips
/// the returned state, once its initial database/Redis connection is up).
pub fn spawn_worker(args: &common::service_args::HealthArgs) -> (ConnectionState, Progress) {
    spawn_with_progress(
        args.health_bind_url.clone(),
        "ok",
        "connecting",
        args.stall_after(),
    )
}

/// [`spawn_worker`] for a worker with no persistent connection to wait for
/// (the pollers): ready at once, so `/healthz` and `/livez` both reflect
/// loop progress only.
pub fn spawn_liveness(args: &common::service_args::HealthArgs) -> Progress {
    let (state, progress) = spawn_worker(args);
    state.store(true, Ordering::Relaxed);
    progress
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
        let livez_progress = progress.clone();
        let app = axum::Router::new()
            .route(
                "/healthz",
                get(move || {
                    healthz(
                        Arc::clone(&state),
                        progress.clone(),
                        healthy_text,
                        unhealthy_text,
                    )
                }),
            )
            .route("/livez", get(move || livez(livez_progress.clone())));
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
            stall_after_secs = progress.stall_after().as_secs(),
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

/// Liveness only: 503 `"stalled"` once the loop has stopped making
/// progress, 200 `"alive"` otherwise -- whatever the connection state says.
///
/// `/healthz` is readiness-shaped (503 until the caller is connected), which
/// is wrong for a liveness probe on a service that is still retrying its
/// initial connection (INF-5): restarting it would only add CrashLoopBackOff
/// delay. A caller with no progress watchdog is always alive.
async fn livez(progress: Option<Progress>) -> (StatusCode, &'static str) {
    match progress.filter(Progress::is_stalled) {
        Some(_) => (StatusCode::SERVICE_UNAVAILABLE, "stalled"),
        None => (StatusCode::OK, "alive"),
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

    /// INF-5: while a service is still retrying its initial connection,
    /// readiness (`/healthz`) is false but liveness (`/livez`) must stay true,
    /// or the liveness probe would restart it into CrashLoopBackOff.
    #[tokio::test]
    async fn livez_ignores_the_connection_state_but_not_a_stall() {
        let progress = Progress::new(Duration::from_millis(100));
        let state: ConnectionState = Arc::new(AtomicBool::new(false));
        let (status, _) = healthz(Arc::clone(&state), Some(progress.clone()), "c", "d").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "not ready yet");
        assert_eq!(
            livez(Some(progress.clone())).await,
            (StatusCode::OK, "alive")
        );

        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            livez(Some(progress.clone())).await,
            (StatusCode::SERVICE_UNAVAILABLE, "stalled")
        );
        assert_eq!(livez(None).await, (StatusCode::OK, "alive"));
    }
}
