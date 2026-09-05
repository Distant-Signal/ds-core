//! Shared HTTP ingestion contract between the RDM pollers
//! (`crates/poller-incidents`, `crates/poller-stations`, `crates/poller-tocs`,
//! `crates/poller-ldbws`), plus `crates/poller-tfl` (not an RDM feed, but a
//! `post_batch`/`time_until_next_poll` consumer all the same), and the `api`
//! crate's `/private/*` endpoints (`crates/api/src/routes/ingest.rs`, gated
//! by `crates/api/src/auth.rs`).
//!
//! Single source of truth for the POST-batch-and-log pattern every real
//! caller repeats once per poll/reload cycle. Every request carries a
//! standard `Authorization: Bearer <token>` header (RFC 6750), the token
//! obtained from `crate::oauth_client::OAuthTokenCache` -- see
//! docs/superpowers/specs/2026-09-02-internal-service-oauth2-design.md
//! Decision 5. Previously this carried a bespoke shared-secret custom
//! header; that scheme is retired, not kept alongside this one (no
//! dual-acceptance window -- see that document's Decision 5 and this
//! plan's own Global Constraints).

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::oauth_client::OAuthTokenCache;

/// Header RDM uses for API-key auth, per RSPS5050 P-03-00 Rev A. How
/// confidently this is corroborated varies per poller/product — see each
/// poller's `main.rs` module docs for the specific gap, if any. Unrelated
/// to internal-service auth (this is RDM's own upstream credential, not a
/// credential DS's own `/private/*` routes check).
pub const RDM_AUTH_HEADER_NAME: &str = "x-apikey";

/// GET + bearer-token + deserialize -- the shape every "fetch one typed
/// resource from `api`'s `/private/*` routes" caller repeats. Previously
/// duplicated with no shared logic beyond this (already trivial) shape
/// across `poller-ldbws::fetch_sample_stations`,
/// `trust_consumer::queries::{fetch_active_tracked_trains,fetch_stanox_crs}`,
/// and `full_coverage_consumer::queries::fetch_stanox_crs`.
pub async fn get_json<T: DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
    tokens: &OAuthTokenCache,
) -> anyhow::Result<T> {
    let token = tokens.get_token(client).await?;
    let response = client
        .get(url)
        .bearer_auth(&token)
        .send()
        .await?
        .error_for_status()?;
    Ok(response.json().await?)
}

/// Single-object POST + bearer-token -- deliberately distinct from
/// `post_batch`'s array-wrapping shape (wrapping a single record in a
/// one-element slice would change the wire shape, not match it).
/// Previously duplicated across `schedule-ingest::main::post_ingest` and
/// `schedule-reference::main::post_schedule_line_population`.
pub async fn post_json<T: Serialize>(
    client: &reqwest::Client,
    url: &str,
    tokens: &OAuthTokenCache,
    body: &T,
) -> anyhow::Result<()> {
    let token = tokens.get_token(client).await?;
    let response = client
        .post(url)
        .bearer_auth(&token)
        .json(body)
        .send()
        .await?;

    if response.status().is_success() {
        Ok(())
    } else {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        anyhow::bail!("POST failed: {status} {text}");
    }
}

/// POSTs `items` as a JSON array to `url` with a fresh
/// `Authorization: Bearer` token from `tokens`, then logs and returns
/// `Ok(())` on a 2xx response, or bails with an `anyhow::Error` (including
/// status + response body) otherwise.
///
/// `noun` is used only in the success log line (e.g. `"incidents"`,
/// `"stations"`, `"tocs"`) — callers pass their own plural label. Left as
/// its own inline implementation rather than delegating to [`post_json`]:
/// doing so would route this function's failure path through
/// `post_json`'s own error message (`"POST failed: ..."`) instead of this
/// function's existing `"ingestion POST failed: ..."`, a real (if narrow)
/// log-text change for every existing caller — see
/// docs/superpowers/plans/2026-09-05-rust-service-deduplication-plan.md
/// Task F1 Step 2 for the full analysis. This keeps `post_batch`'s
/// existing behavior byte-for-byte unchanged.
pub async fn post_batch<T: Serialize>(
    client: &reqwest::Client,
    url: &str,
    tokens: &OAuthTokenCache,
    items: &[T],
    noun: &str,
) -> anyhow::Result<()> {
    let token = tokens.get_token(client).await?;
    let response = client
        .post(url)
        .bearer_auth(&token)
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
    tokens: &OAuthTokenCache,
    poll_interval: Duration,
) -> Duration {
    let fetched_at = match fetch_last_fetched(client, url, tokens).await {
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
    tokens: &OAuthTokenCache,
) -> anyhow::Result<Option<DateTime<Utc>>> {
    let body: LastFetchedResponse = get_json(client, url, tokens).await?;
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
fn duration_until_next_poll(
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

    #[tokio::test]
    async fn get_json_deserializes_a_successful_response() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "fake-jwt",
                "expires_in": 300,
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/thing"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "value": 42
            })))
            .mount(&server)
            .await;

        let tokens =
            crate::oauth_client::OAuthTokenCache::new(crate::oauth_client::OAuthCredentials {
                token_url: format!("{}/token/", server.uri()),
                client_id: "c".to_string(),
                scope: "groups".to_string(),
                username: "u".to_string(),
                password: "p".to_string(),
            });
        let client = reqwest::Client::new();

        #[derive(serde::Deserialize)]
        struct Thing {
            value: u32,
        }
        let thing: Thing = get_json(&client, &format!("{}/thing", server.uri()), &tokens)
            .await
            .unwrap();
        assert_eq!(thing.value, 42);
    }

    #[tokio::test]
    async fn post_json_posts_the_body_and_returns_ok_on_success() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "fake-jwt",
                "expires_in": 300,
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/thing"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let tokens =
            crate::oauth_client::OAuthTokenCache::new(crate::oauth_client::OAuthCredentials {
                token_url: format!("{}/token/", server.uri()),
                client_id: "c".to_string(),
                scope: "groups".to_string(),
                username: "u".to_string(),
                password: "p".to_string(),
            });
        let client = reqwest::Client::new();

        #[derive(serde::Serialize)]
        struct Thing {
            value: u32,
        }
        post_json(
            &client,
            &format!("{}/thing", server.uri()),
            &tokens,
            &Thing { value: 1 },
        )
        .await
        .unwrap();
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
