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

/// Called by every helper below right after a request made with a cached
/// bearer token comes back 401 or 403 (Finding #4): the API rejected the
/// token itself (revocation, signing-key rotation, clock skew), so the
/// cache entry is invalidated to force a fresh fetch on the very next call,
/// rather than presenting the same rejected token again until its normal
/// `refresh_at` deadline (up to `expires_in - 30s` away). Any other status
/// (a 5xx, a 404, a plain network error) leaves the cache untouched --
/// those don't indicate the token itself was the problem.
fn invalidate_on_auth_rejection(tokens: &OAuthTokenCache, status: reqwest::StatusCode) {
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
        tokens.invalidate();
    }
}

/// Connect timeout for [`consumer_http_client`]: `api` is in-cluster, so a
/// TCP connect that has not completed in this long is a dead peer, not a
/// slow one.
pub const CONSUMER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Whole-request timeout for [`consumer_http_client`] -- the same
/// `REQUEST_TIMEOUT` pattern every poller already uses (30s there), doubled
/// because a movement-stream consumer's largest legitimate POST is bigger:
/// a PEL replay hands over up to 1,000 stream entries at once, and
/// `trust-backlog-consumer` posts every surviving row of that in a single
/// request that `api` upserts row by row. Without any timeout (the previous
/// `reqwest::Client::new()`), one half-open connection to `api` wedged a
/// consumer forever: no XACK, no XAUTOCLAIM sweep, and a `/healthz` that
/// still said 200.
pub const CONSUMER_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// The HTTP client the three `movement-events` consumers (`trust-consumer`,
/// `trust-backlog-consumer`, `full-coverage-consumer`) talk to `api` and the
/// OAuth token endpoint with. See [`CONSUMER_REQUEST_TIMEOUT`].
pub fn consumer_http_client() -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(CONSUMER_CONNECT_TIMEOUT)
        .timeout(CONSUMER_REQUEST_TIMEOUT)
        .build()
}

/// A non-2xx response from one of this module's POST helpers. Kept as a
/// typed error (rather than a bare `anyhow!` string) so a caller can tell a
/// data rejection from an outage -- see [`classify_failure`]. `Display` is
/// the exact text the helpers used to `bail!` with, so log lines are
/// unchanged.
#[derive(Debug)]
pub struct HttpStatusError {
    pub prefix: &'static str,
    pub status: reqwest::StatusCode,
    pub body: String,
}

impl std::fmt::Display for HttpStatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {} {}", self.prefix, self.status, self.body)
    }
}

impl std::error::Error for HttpStatusError {}

/// Why a downstream write failed, as far as retrying it is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// Retrying the same data can succeed: `api` unreachable, a timeout, a
    /// 5xx, an auth failure, a 404 mid-deploy, a 408/429. The data must stay
    /// pending and be retried for as long as it takes.
    Transient,
    /// `api` explicitly refused the data itself (400, 413 or 422): the same
    /// request will fail the same way on every retry.
    Rejected,
}

/// Classifies an error from this module's helpers. Only a response status
/// that means "this request body is bad" counts as [`FailureClass::Rejected`];
/// everything else -- including any error this function does not recognise
/// -- is [`FailureClass::Transient`], because wrongly treating an outage as
/// a rejection dead-letters healthy data, while the reverse only delays it.
pub fn classify_failure(err: &anyhow::Error) -> FailureClass {
    let status = err.chain().find_map(|cause| {
        cause
            .downcast_ref::<HttpStatusError>()
            .map(|e| e.status)
            .or_else(|| {
                cause
                    .downcast_ref::<reqwest::Error>()
                    .and_then(reqwest::Error::status)
            })
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
    let response = client.get(url).bearer_auth(&token).send().await?;
    invalidate_on_auth_rejection(tokens, response.status());
    let response = response.error_for_status()?;
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
        invalidate_on_auth_rejection(tokens, status);
        let body = response.text().await.unwrap_or_default();
        Err(HttpStatusError {
            prefix: "POST failed",
            status,
            body,
        }
        .into())
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
    post_batch_with_timeout(client, url, tokens, items, noun, None).await
}

/// [`post_batch`], optionally overriding the client's own request timeout
/// for this one request (`reqwest::RequestBuilder::timeout`) -- for a call
/// known to legitimately take longer than the client-wide default.
pub async fn post_batch_with_timeout<T: Serialize>(
    client: &reqwest::Client,
    url: &str,
    tokens: &OAuthTokenCache,
    items: &[T],
    noun: &str,
    timeout: Option<Duration>,
) -> anyhow::Result<()> {
    let token = tokens.get_token(client).await?;
    let mut request = client.post(url).bearer_auth(&token).json(items);
    if let Some(timeout) = timeout {
        request = request.timeout(timeout);
    }
    let response = request.send().await?;

    if response.status().is_success() {
        tracing::info!(count = items.len(), "posted {noun} to ingestion API");
        Ok(())
    } else {
        let status = response.status();
        invalidate_on_auth_rejection(tokens, status);
        let body = response.text().await.unwrap_or_default();
        Err(HttpStatusError {
            prefix: "ingestion POST failed",
            status,
            body,
        }
        .into())
    }
}

/// Like [`post_batch`], but also deserializes a 2xx response body as `R`,
/// for the routes whose reply carries more than "it worked" (e.g.
/// `/private/trust-event-backlog`'s per-row `rejected` list). Same bearer
/// token handling and the same `"ingestion POST failed: ..."` error text.
pub async fn post_batch_for_response<T: Serialize, R: DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
    tokens: &OAuthTokenCache,
    items: &[T],
    noun: &str,
) -> anyhow::Result<R> {
    let token = tokens.get_token(client).await?;
    let response = client
        .post(url)
        .bearer_auth(&token)
        .json(items)
        .send()
        .await?;

    if response.status().is_success() {
        tracing::info!(count = items.len(), "posted {noun} to ingestion API");
        Ok(response.json().await?)
    } else {
        let status = response.status();
        invalidate_on_auth_rejection(tokens, status);
        let body = response.text().await.unwrap_or_default();
        Err(HttpStatusError {
            prefix: "ingestion POST failed",
            status,
            body,
        }
        .into())
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

/// Wire contract for `POST /private/schedule-reference-publishes` — the
/// completion marker `crates/schedule-reference` writes for itself, ONCE
/// per delivery, only after every one of that cycle's products has
/// published successfully.
///
/// Shared here rather than redefined per-side for the same reason
/// [`LastFetchedResponse`] is, and deliberately carrying the delivery's
/// DIRECTORY NAME verbatim (`YYYYMMDDTHHMMSSZ`, see
/// `schedule-reference::discovery::CompleteDelivery::dir_name`) rather than
/// a `DateTime` that would have to be re-formatted back into that shape on
/// read: the string this marker stores is the exact string
/// `schedule-reference::poll_once` compares against, so round-tripping it
/// through a timestamp would insert a lossy conversion between "what was
/// published" and "what a restart compares" for no benefit.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleReferencePublishRequest {
    pub delivery: String,
}

/// Wire contract for `GET /private/schedule-reference-publishes` — the read
/// side of [`ScheduleReferencePublishRequest`], returning the most recently
/// COMPLETED delivery's directory name, or `None` if this producer has
/// never completed a full publish cycle (a fresh deployment).
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastCompletedPublishResponse {
    pub delivery: Option<String>,
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

    /// Finding #4 regression, exercised at the real call path
    /// (`get_json`, as every `/private/*` GET caller uses it): a 401
    /// response using a cached token must invalidate that cache entry, so
    /// the very next call refetches instead of presenting the same
    /// rejected token again until its normal `refresh_at` deadline (still
    /// ~300s away here). Observed via wiremock's `.expect(2)` on the token
    /// endpoint -- it fails the test on `Drop` unless the token endpoint is
    /// actually hit a second time.
    #[tokio::test]
    async fn get_json_invalidates_the_cached_token_on_a_401_response() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "fake-jwt",
                "expires_in": 300,
            })))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/thing"))
            .respond_with(ResponseTemplate::new(401))
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
            #[allow(dead_code)]
            value: u32,
        }

        let first: anyhow::Result<Thing> =
            get_json(&client, &format!("{}/thing", server.uri()), &tokens).await;
        assert!(first.is_err(), "a 401 must surface as an Err");

        let second: anyhow::Result<Thing> =
            get_json(&client, &format!("{}/thing", server.uri()), &tokens).await;
        assert!(
            second.is_err(),
            "the endpoint still 401s regardless (this test doesn't change that) -- the real \
             assertion is wiremock's `.expect(2)` on the token endpoint above, which fails \
             unless invalidate() forced this second call to refetch"
        );
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

    fn status_error(status: u16) -> anyhow::Error {
        HttpStatusError {
            prefix: "ingestion POST failed",
            status: reqwest::StatusCode::from_u16(status).unwrap(),
            body: "body".to_string(),
        }
        .into()
    }

    #[test]
    fn only_a_data_rejection_status_is_classified_rejected() {
        for status in [400, 413, 422] {
            assert_eq!(
                classify_failure(&status_error(status)),
                FailureClass::Rejected,
                "{status}"
            );
        }
        for status in [401, 403, 404, 408, 409, 429, 500, 502, 503, 504] {
            assert_eq!(
                classify_failure(&status_error(status)),
                FailureClass::Transient,
                "{status}"
            );
        }
    }

    #[test]
    fn an_unrecognised_or_wrapped_error_is_classified_correctly() {
        assert_eq!(
            classify_failure(&anyhow::anyhow!("connection refused")),
            FailureClass::Transient
        );
        assert_eq!(
            classify_failure(&status_error(422).context("posting train events")),
            FailureClass::Rejected,
            "a context layer must not hide the status"
        );
    }

    #[test]
    fn the_status_error_keeps_the_old_log_text() {
        assert_eq!(
            status_error(500).to_string(),
            "ingestion POST failed: 500 Internal Server Error body"
        );
    }

    /// PL-1: a server that accepts the connection but never answers must
    /// not hang the caller -- the request times out, as a transient error.
    #[tokio::test]
    async fn a_hung_api_times_out_as_a_transient_failure() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Accept and hold connections open, never writing a byte.
        let _server = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        let client = reqwest::Client::builder()
            .connect_timeout(CONSUMER_CONNECT_TIMEOUT)
            .timeout(Duration::from_millis(300))
            .build()
            .unwrap();
        let started = std::time::Instant::now();
        let err = client
            .post(format!("http://{addr}/private/train-events"))
            .body("[]")
            .send()
            .await
            .expect_err("a hung api must time out, not hang");
        assert!(err.is_timeout(), "{err:?}");
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(
            classify_failure(&anyhow::Error::from(err)),
            FailureClass::Transient
        );
    }

    #[test]
    fn the_consumer_client_builds() {
        consumer_http_client().unwrap();
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
