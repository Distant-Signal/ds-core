//! Shared HTTP ingestion contract between the RDM pollers
//! (`crates/poller-incidents`, `crates/poller-stations`, `crates/poller-tocs`,
//! `crates/poller-ldbws`) and the `api` crate's `/private/*` endpoints
//! (`crates/api/src/routes/ingest.rs`, gated by `crates/api/src/auth.rs`).
//!
//! Single source of truth for the two header names both sides must agree
//! on, plus the POST-batch-and-log pattern every poller repeats once per
//! poll cycle. Previously each poller (and `api`, for the internal-token
//! header) independently redefined these constants/logic; this module is
//! the one place that changes if either header name or the POST contract
//! ever needs to.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Shared-secret header every poller sends and `api`'s
/// `require_internal_token` middleware (`crates/api/src/auth.rs`) checks.
pub const INTERNAL_TOKEN_HEADER: &str = "x-internal-token";

/// Header RDM uses for API-key auth, per RSPS5050 P-03-00 Rev A. How
/// confidently this is corroborated varies per poller/product — see each
/// poller's `main.rs` module docs for the specific gap, if any.
pub const RDM_AUTH_HEADER_NAME: &str = "x-apikey";

/// POSTs `items` as a JSON array to `url` with the internal-token header,
/// then logs and returns `Ok(())` on a 2xx response, or bails with an
/// `anyhow::Error` (including status + response body) otherwise.
///
/// `noun` is used only in the success log line (e.g. `"incidents"`,
/// `"stations"`, `"tocs"`) — callers pass their own plural label.
pub async fn post_batch<T: Serialize>(
    client: &reqwest::Client,
    url: &str,
    internal_token: &str,
    items: &[T],
    noun: &str,
) -> anyhow::Result<()> {
    let response = client
        .post(url)
        .header(INTERNAL_TOKEN_HEADER, internal_token)
        .json(items)
        .send()
        .await?;

    if response.status().is_success() {
        tracing::info!(count = items.len(), "posted {noun} to ingestion API");
        Ok(())
    } else {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        anyhow::bail!("ingestion POST failed: {status} {text}");
    }
}

/// Wire contract for the GET side of each `/private/*` ingest route (see
/// `crates/api/src/routes/ingest.rs`) — shared, not redefined per-side, so
/// a future rename can't silently drift out of sync between the `api`
/// crate (which `Serialize`s it) and this module (which `Deserialize`s
/// it). A drift would fail closed anyway (`serde` treats a missing key as
/// `None` → "poll now", the safe direction) but there's no reason to rely
/// on that when a shared type makes it impossible in the first place.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastFetchedResponse {
    pub fetched_at: Option<DateTime<Utc>>,
}

/// How long to wait before this process's first poll, so a restart doesn't
/// immediately re-fetch data that's still fresh from before it. GETs `url`
/// — the same URL the poller POSTs its batches to; the two share one route,
/// distinguished by method, see `crates/api/src/routes/ingest.rs` — to
/// learn the last successful fetch time, then defers to the pure
/// [`duration_until_next_poll`] to do the actual math.
///
/// A failed freshness check (network error, `api` not yet reachable, bad
/// response) logs a warning and returns `Duration::ZERO` — "poll now" is
/// this process's behavior before this function existed at all, so on
/// error it's the safe fallback, not a new failure mode.
pub async fn time_until_next_poll(
    client: &reqwest::Client,
    url: &str,
    internal_token: &str,
    poll_interval: Duration,
) -> Duration {
    let fetched_at = match fetch_last_fetched(client, url, internal_token).await {
        Ok(fetched_at) => fetched_at,
        Err(err) => {
            tracing::warn!(error = ?err, "could not determine last-fetch time; polling immediately");
            return Duration::ZERO;
        }
    };
    duration_until_next_poll(fetched_at, Utc::now(), poll_interval)
}

async fn fetch_last_fetched(
    client: &reqwest::Client,
    url: &str,
    internal_token: &str,
) -> anyhow::Result<Option<DateTime<Utc>>> {
    let response = client
        .get(url)
        .header(INTERNAL_TOKEN_HEADER, internal_token)
        .send()
        .await?
        .error_for_status()?;
    let body: LastFetchedResponse = response.json().await?;
    Ok(body.fetched_at)
}

/// `None` (never fetched) means "poll now" (`Duration::ZERO`). Otherwise,
/// the elapsed time since `fetched_at` is clamped to zero if it would be
/// negative (a `fetched_at` in the future — clock skew between hosts —
/// never underflows or panics); a poll_interval already exceeded by that
/// elapsed time means "poll now", otherwise the remainder is returned.
/// Return value is always `<= poll_interval` — this only ever delays the
/// *first* tick of a fresh process, so it can't compound across restarts.
///
/// Assumes restarts aren't pathologically frequent: if something else
/// were also wrong (e.g. a bug writing `fetched_at` persistently in the
/// future) *and* the process were crash-looping, every restart would
/// re-arm a full-interval delay before ever reaching a real poll. Two
/// simultaneous faults, not a risk from this function in isolation.
fn duration_until_next_poll(fetched_at: Option<DateTime<Utc>>, now: DateTime<Utc>, poll_interval: Duration) -> Duration {
    let Some(fetched_at) = fetched_at else {
        return Duration::ZERO;
    };
    let elapsed = (now - fetched_at).to_std().unwrap_or(Duration::ZERO);
    poll_interval.saturating_sub(elapsed)
}

#[cfg(test)]
mod tests {
    use super::*;

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
