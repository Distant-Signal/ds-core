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

use axum::http::StatusCode;
use axum::routing::get;

/// `true` once the caller's own connection/consumer is confirmed live;
/// `false` from startup and whenever disconnected. Shared, not
/// crate-local -- every real caller today used exactly this type alias
/// (`Arc<AtomicBool>`) under its own crate-local name.
pub type ConnectionState = Arc<AtomicBool>;

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
    tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/healthz",
            get(move || healthz(Arc::clone(&state), healthy_text, unhealthy_text)),
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
    healthy_text: &'static str,
    unhealthy_text: &'static str,
) -> (StatusCode, &'static str) {
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
        let (status, body) = healthz(Arc::clone(&state), "connected", "disconnected").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "disconnected");

        state.store(true, Ordering::Relaxed);
        let (status, body) = healthz(Arc::clone(&state), "connected", "disconnected").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "connected");
    }
}
