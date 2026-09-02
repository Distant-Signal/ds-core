//! Shared Prometheus metrics installer for the six binaries that have no
//! HTTP server of their own today (`aggregator`, `enricher`, and the five
//! pollers) -- mirrors `crates/common::ingest`'s precedent of being "the
//! one place that changes" for boilerplate every one of those binaries
//! would otherwise repeat (`crates/common/src/ingest.rs`'s own module doc).
//! `api` does NOT call `install` (see
//! docs/superpowers/plans/2026-08-29-metrics.md's Task 2): it already has
//! an axum listener to attach `axum-prometheus`'s middleware to instead,
//! and composes the same underlying `metrics` facade through that crate.
//!
//! See docs/superpowers/specs/2026-08-29-metrics-design.md's Architecture
//! section for the full reasoning behind this split.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::{Context, Result};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};

/// Every metric this app emits by hand is prefixed `distant_signal_`, so it
/// can never collide with `metrics-exporter-prometheus`'s own process-level
/// defaults (e.g. `process_cpu_seconds_total`) or a future metric from an
/// unrelated process sharing the same Prometheus instance. Callers build a
/// metric's full name through this function rather than hand-writing the
/// prefix at each of the (many) call sites across six crates, so the one
/// place that changes if the prefix itself ever does is this function, not
/// every call site.
pub fn metric_name(suffix: &str) -> String {
    format!("distant_signal_{suffix}")
}

/// Default histogram bucket boundaries applied to every metric recorded
/// via this module's install functions, covering roughly 50ms to 2
/// minutes -- wide enough for a poll cycle or an aggregator cycle without
/// per-metric tuning. Without an explicit bucket set,
/// `metrics-exporter-prometheus` renders every histogram as a rolling
/// 60-second-window summary instead of a true Prometheus histogram -- its
/// quantiles silently read 0 once the last observation ages out of that
/// window, which is misleading for any poller whose cycle is longer than
/// 60s (five of the seven binaries this module serves). See this branch's
/// final whole-branch review, Critical finding #1.
const DEFAULT_BUCKETS: &[f64] = &[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0];

/// Installs the process-global Prometheus recorder and starts its embedded
/// HTTP listener on `0.0.0.0:<port>`, serving `/metrics` in Prometheus text
/// exposition format. Must be called exactly once, near the top of `main`,
/// before any `counter!`/`histogram!`/`gauge!` call and before the caller's
/// real work begins -- `metrics`'s macros are silent no-ops against no
/// installed recorder, dropping every observation rather than erroring, if
/// this hasn't run yet.
///
/// No `axum` dependency: `metrics-exporter-prometheus`'s
/// `with_http_listener` spins up its own minimal `hyper`-based listener, so
/// this doesn't pull a web framework into six crates that have never
/// needed one -- confirmed against the crate's docs.rs page as part of
/// this feature's design pass
/// (docs/superpowers/specs/2026-08-29-metrics-design.md).
///
/// Every histogram recorded through the recorder this installs gets
/// [`DEFAULT_BUCKETS`] unless `install_with_buckets` was given an explicit
/// per-metric override for it.
pub fn install(port: u16) -> Result<()> {
    install_with_buckets(port, &[])
}

/// Like [`install`], but additionally overrides the module-wide
/// [`DEFAULT_BUCKETS`] for specific metrics by exact name -- e.g. so a
/// histogram tracking calls to an endpoint with a known request timeout can
/// have buckets extending past that timeout, making a call that's *about*
/// to time out show up as "slow" rather than invisible until it becomes a
/// binary failure.
///
/// `bucket_overrides` is `(full_metric_name, bucket_boundaries)` pairs. Only
/// `enricher` currently needs this (its LLM-call duration histogram,
/// against `config.llm_request_timeout_secs`); every other caller of
/// `install` has no such tuned-timeout metric and keeps using the plain,
/// no-argument `install` -- see
/// docs/superpowers/plans/2026-08-29-metrics.md's Task 9 for why this is a
/// second function rather than a breaking signature change to `install`
/// itself.
pub fn install_with_buckets(port: u16, bucket_overrides: &[(&str, &[f64])]) -> Result<()> {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
    let mut builder = PrometheusBuilder::new()
        .with_http_listener(addr)
        // Global default first; the per-metric `set_buckets_for_metric`
        // calls below take precedence over it for the names they match.
        .set_buckets(DEFAULT_BUCKETS)
        .context("failed to set the default histogram buckets")?;
    for (name, buckets) in bucket_overrides {
        builder = builder
            .set_buckets_for_metric(Matcher::Full((*name).to_string()), buckets)
            .context("failed to set histogram bucket overrides")?;
    }
    builder
        .install()
        .context("failed to install the Prometheus metrics exporter")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_name_adds_the_shared_prefix() {
        assert_eq!(
            metric_name("poller_cycle_total"),
            "distant_signal_poller_cycle_total"
        );
    }

    #[test]
    fn metric_name_does_not_detect_or_strip_an_already_prefixed_suffix() {
        // Documents current behavior rather than testing a real
        // requirement: metric_name always prepends, it never inspects its
        // input. Callers are responsible for passing a bare suffix (e.g.
        // "poller_cycle_total", not "distant_signal_poller_cycle_total").
        assert_eq!(
            metric_name("distant_signal_poller_cycle_total"),
            "distant_signal_distant_signal_poller_cycle_total"
        );
    }

    // `install` is not unit-tested here -- see this plan's Global
    // Constraints ("Testing convention for metrics") for why: it sets a
    // process-global recorder exactly once per process, which doesn't
    // compose with Rust's default concurrent, same-binary test execution.
    // Verified instead by a manual curl against a running binary's
    // /metrics endpoint, and implicitly by every downstream task's own
    // manual verification step, each of which depends on `install` having
    // actually started a listener.
}
