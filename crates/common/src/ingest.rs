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

use crate::backoff::Backoff;
use crate::oauth_client::OAuthTokenCache;
use crate::progress::Progress;

/// Called by every helper below right after a request made with a cached
/// bearer token comes back 401 or 403 (Finding #4): the API rejected the
/// token itself (revocation, signing-key rotation, clock skew), so the
/// cache entry is invalidated to force a fresh fetch on the very next call,
/// rather than presenting the same rejected token again until its normal
/// `refresh_at` deadline (up to `expires_in - 30s` away). Any other status
/// (a 5xx, a 404, a plain network error) leaves the cache untouched --
/// those don't indicate the token itself was the problem.
///
/// Public for the few callers that need a request shape none of the helpers
/// below cover (full-coverage-consumer's conditional GET), so every request
/// made with a cached token honours the same rule (PL-15b).
pub fn invalidate_on_auth_rejection(tokens: &OAuthTokenCache, status: reqwest::StatusCode) {
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
    /// The response's `Retry-After`, when it was a number of seconds. `api`
    /// sends one with every 503 (database unavailable, load shedding) and
    /// 429; see [`retry_after`].
    pub retry_after: Option<Duration>,
}

/// A `Retry-After` header's delay, when it is delta-seconds (all `api`
/// sends). An HTTP-date or garbage is ignored: the caller's own backoff
/// still applies.
pub fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}

/// The `Retry-After` of a 503 or 429 anywhere in `err`'s chain: how long
/// `api` asked to be left alone. Pass it to
/// [`Backoff::delay_honouring`](crate::backoff::Backoff::delay_honouring).
pub fn retry_after(err: &anyhow::Error) -> Option<Duration> {
    err.chain().find_map(|cause| {
        cause
            .downcast_ref::<HttpStatusError>()
            .filter(|e| {
                e.status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                    || e.status == reqwest::StatusCode::TOO_MANY_REQUESTS
            })
            .and_then(|e| e.retry_after)
    })
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

/// A write that refused the data itself, from a sink that is not an `api`
/// POST (ingest architecture phase 2's DB sinks: a class 22 or 23
/// SQLSTATE). [`classify_failure`] reads it as [`FailureClass::Rejected`],
/// as it does a 400, 413 or 422: the same data will fail the same way.
#[derive(Debug)]
pub struct DataRejected(pub String);

impl std::fmt::Display for DataRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "data rejected: {}", self.0)
    }
}

impl std::error::Error for DataRejected {}

/// Classifies an error from this module's helpers. Only a response status
/// that means "this request body is bad" counts as [`FailureClass::Rejected`];
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
    if !response.status().is_success() {
        return Err(status_error("GET failed", response).await.into());
    }
    Ok(response.json().await?)
}

/// The [`HttpStatusError`] for a non-2xx `response`: its status,
/// `Retry-After` and body.
async fn status_error(prefix: &'static str, response: reqwest::Response) -> HttpStatusError {
    let status = response.status();
    let retry_after = parse_retry_after(response.headers());
    let body = response.text().await.unwrap_or_default();
    HttpStatusError {
        prefix,
        status,
        body,
        retry_after,
    }
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
        invalidate_on_auth_rejection(tokens, response.status());
        Err(status_error("POST failed", response).await.into())
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
    post_counted_with_timeout(client, url, tokens, items, items.len(), noun, timeout).await
}

/// [`post_batch_with_timeout`] for a body that is not a bare JSON array
/// (e.g. [`crate::IncidentSnapshot`], which wraps its items with metadata):
/// `count` is the item count the success log line reports. Same token
/// handling and the same `"ingestion POST failed: ..."` error text.
async fn post_counted_with_timeout<B: Serialize + ?Sized>(
    client: &reqwest::Client,
    url: &str,
    tokens: &OAuthTokenCache,
    body: &B,
    count: usize,
    noun: &str,
    timeout: Option<Duration>,
) -> anyhow::Result<()> {
    let token = tokens.get_token(client).await?;
    let mut request = client.post(url).bearer_auth(&token).json(body);
    if let Some(timeout) = timeout {
        request = request.timeout(timeout);
    }
    let response = request.send().await?;

    if response.status().is_success() {
        tracing::info!(count, "posted {noun} to ingestion API");
        Ok(())
    } else {
        invalidate_on_auth_rejection(tokens, response.status());
        Err(status_error("ingestion POST failed", response).await.into())
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
        invalidate_on_auth_rejection(tokens, response.status());
        Err(status_error("ingestion POST failed", response).await.into())
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

/// How long a poller waits for `api` to answer before giving up on it
/// (SVC-09). After a node reboot the pollers start before `api` (which
/// waits for Postgres, then runs migrations), so the first freshness GET
/// fails; treating that as "poll now" spent the poller's upstream fetch
/// (once per 24h for the RDM stations/TOCs feeds) on a POST that could not
/// succeed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiWait {
    pub backoff: Backoff,
    /// Give up (and fall back to the old "poll now") once this much time has
    /// gone by -- so a GET-side-only breakage can never stop polling.
    pub max_wait: Duration,
}

/// 2s doubling to 30s, for at most 10 minutes: `api`'s own startup budget
/// (startupProbe 300s) plus a slow Postgres crash recovery.
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
/// - [`CursorSource::Http`]: the api's `GET` on the poller's own ingest URL
///   ([`LastFetchedResponse`]), today's behaviour and every poller's
///   default (`INGEST_SINK=http`).
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
    Http {
        client: &'a reqwest::Client,
        url: &'a str,
        tokens: &'a OAuthTokenCache,
    },
    Stream(CursorFn<'a>),
    Db(CursorFn<'a>),
}

impl<'a> CursorSource<'a> {
    /// The api's `GET` on `url` (the poller's ingest URL).
    pub fn http(client: &'a reqwest::Client, url: &'a str, tokens: &'a OAuthTokenCache) -> Self {
        Self::Http {
            client,
            url,
            tokens,
        }
    }

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

    /// `http`, `stream` or `db`, for logs.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Http { .. } => "http",
            Self::Stream(_) => "stream",
            Self::Db(_) => "db",
        }
    }
}

impl std::fmt::Debug for CursorSource<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http { url, .. } => f.debug_struct("Http").field("url", url).finish(),
            Self::Stream(_) | Self::Db(_) => f.write_str(self.kind()),
        }
    }
}

/// Where an internal reader reads its reference data (ingest architecture
/// spec §11, plan phase 4): `POPULATION_SOURCE`, `STANOX_CRS_SOURCE`,
/// `TRACKED_TRAINS_SOURCE`, `SAMPLE_STATIONS_SOURCE`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum ReadSource {
    /// The api's `GET /private/*` route (today's behaviour).
    #[default]
    Http,
    /// Postgres directly, as the reader's own read-only role.
    Db,
}

/// Reads a poller's last-fetch time: a [`CursorSource`], or any closure
/// returning the same future (what the older closure-taking helpers here
/// and in `poller_loop` accept).
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
            Self::Http {
                client,
                url,
                tokens,
            } => fetch_last_fetched(client, url, tokens).await,
            Self::Stream(read) | Self::Db(read) => read().await,
        }
    }
}

/// GETs the last-fetch time from `url` (see [`time_until_next_poll`]),
/// retrying any failure with `wait.backoff` for up to `wait.max_wait`.
/// `progress`, when given, is beaten on every failed attempt -- waiting for
/// `api` is not a stall.
pub async fn wait_for_last_fetched(
    client: &reqwest::Client,
    url: &str,
    tokens: &OAuthTokenCache,
    wait: &ApiWait,
    progress: Option<&Progress>,
) -> anyhow::Result<Option<DateTime<Utc>>> {
    wait_for_source(&mut CursorSource::http(client, url, tokens), wait, progress).await
}

/// [`wait_for_last_fetched`] for any source of the last-fetch time: the
/// api's GET, or (a direct writer, ingest architecture phase 2) the
/// database's `ingest_freshness` row. `fetch` is retried on any failure
/// with `wait.backoff` for up to `wait.max_wait`.
pub async fn wait_for_cursor<F, Fut>(
    mut fetch: F,
    wait: &ApiWait,
    progress: Option<&Progress>,
) -> anyhow::Result<Option<DateTime<Utc>>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<Option<DateTime<Utc>>>>,
{
    wait_for_source(&mut fetch, wait, progress).await
}

/// [`wait_for_last_fetched`] for any [`LastFetched`], a [`CursorSource`]
/// above all: retried on any failure with `wait.backoff` for up to
/// `wait.max_wait`, beating `progress` meanwhile.
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
        let delay = wait.backoff.delay_honouring(failures, retry_after(&err));
        if started.elapsed() + delay > wait.max_wait {
            return Err(err);
        }
        tracing::warn!(
            error = ?err,
            attempt = failures + 1,
            retry_in_secs = delay.as_secs(),
            "the last-fetch source (api or database) is not reachable yet; waiting for it before fetching upstream"
        );
        if let Some(progress) = progress {
            progress.beat();
        }
        tokio::time::sleep(delay).await;
        failures = failures.saturating_add(1);
    }
}

/// How long to wait before this process's first poll, so a restart doesn't
/// immediately re-fetch data that's still fresh from before it. GETs `url`
/// — the same URL the poller POSTs its batches to; the two share one route,
/// distinguished by method, see `crates/api/src/routes/ingest.rs` — to
/// learn the last successful fetch time, then defers to the pure
/// [`duration_until_next_poll`] to do the actual math.
///
/// A failed freshness check is retried for up to [`API_STARTUP_WAIT`]
/// (SVC-09: `api` is typically still starting); only if `api` is still
/// unreachable after that does this log a warning and return
/// `Duration::ZERO` -- "poll now" is this process's behavior before this
/// function existed at all, so it stays the fallback.
pub async fn time_until_next_poll(
    client: &reqwest::Client,
    url: &str,
    tokens: &OAuthTokenCache,
    poll_interval: Duration,
) -> Duration {
    time_until_next_poll_waiting(client, url, tokens, poll_interval, &API_STARTUP_WAIT, None).await
}

/// [`time_until_next_poll`] with an explicit [`ApiWait`] and an optional
/// liveness [`Progress`] to beat while waiting.
pub async fn time_until_next_poll_waiting(
    client: &reqwest::Client,
    url: &str,
    tokens: &OAuthTokenCache,
    poll_interval: Duration,
    wait: &ApiWait,
    progress: Option<&Progress>,
) -> Duration {
    time_until_next_poll_for(
        &mut CursorSource::http(client, url, tokens),
        poll_interval,
        wait,
        progress,
    )
    .await
}

/// [`time_until_next_poll_waiting`] for any source of the last-fetch time
/// (see [`wait_for_cursor`]).
pub async fn time_until_next_poll_from<F, Fut>(
    mut fetch: F,
    poll_interval: Duration,
    wait: &ApiWait,
    progress: Option<&Progress>,
) -> Duration
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<Option<DateTime<Utc>>>>,
{
    time_until_next_poll_for(&mut fetch, poll_interval, wait, progress).await
}

/// [`time_until_next_poll_waiting`] for any [`LastFetched`], a
/// [`CursorSource`] above all: the same wait, the same "poll now" fallback
/// and the same arithmetic ([`duration_until_next_poll`]) whichever source
/// the cursor comes from (plan 4.6).
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

/// Backoff between retries of a failed poller ingest POST, see
/// [`post_batch_retrying`].
pub const POST_RETRY_BACKOFF: Backoff =
    Backoff::new(Duration::from_secs(2), Duration::from_secs(60));

/// [`post_batch`], retrying a [`FailureClass::Transient`] failure (api
/// unreachable, a timeout, a 5xx, an auth hiccup) with
/// [`POST_RETRY_BACKOFF`] for up to `budget`, WITHOUT re-fetching the data
/// from upstream (SVC-09). A [`FailureClass::Rejected`] failure is returned
/// at once: the same body will be refused the same way.
///
/// Pollers size `budget` with `poller_loop::post_retry_budget`, so a
/// frequent poller never spends longer retrying than its next fresh fetch
/// would take to arrive, while a daily one keeps its expensive upstream
/// fetch alive through an api restart.
pub async fn post_batch_retrying<T: Serialize>(
    client: &reqwest::Client,
    url: &str,
    tokens: &OAuthTokenCache,
    items: &[T],
    noun: &str,
    budget: Duration,
) -> anyhow::Result<()> {
    post_batch_retrying_with(client, url, tokens, items, noun, budget, POST_RETRY_BACKOFF).await
}

/// [`post_batch_retrying`] for a non-array body, see
/// [`post_counted_with_timeout`]: `count` is only what the success log line
/// reports.
pub async fn post_counted_retrying<B: Serialize + ?Sized>(
    client: &reqwest::Client,
    url: &str,
    tokens: &OAuthTokenCache,
    body: &B,
    count: usize,
    noun: &str,
    budget: Duration,
) -> anyhow::Result<()> {
    post_counted_retrying_with(
        client,
        url,
        tokens,
        body,
        count,
        noun,
        budget,
        POST_RETRY_BACKOFF,
    )
    .await
}

async fn post_batch_retrying_with<T: Serialize>(
    client: &reqwest::Client,
    url: &str,
    tokens: &OAuthTokenCache,
    items: &[T],
    noun: &str,
    budget: Duration,
    backoff: Backoff,
) -> anyhow::Result<()> {
    post_counted_retrying_with(
        client,
        url,
        tokens,
        items,
        items.len(),
        noun,
        budget,
        backoff,
    )
    .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "the shared body of the two retrying POSTs; a struct would only wrap them"
)]
async fn post_counted_retrying_with<B: Serialize + ?Sized>(
    client: &reqwest::Client,
    url: &str,
    tokens: &OAuthTokenCache,
    body: &B,
    count: usize,
    noun: &str,
    budget: Duration,
    backoff: Backoff,
) -> anyhow::Result<()> {
    let started = tokio::time::Instant::now();
    let mut failures: u32 = 0;
    loop {
        let err =
            match post_counted_with_timeout(client, url, tokens, body, count, noun, None).await {
                Ok(()) => return Ok(()),
                Err(err) => err,
            };
        if classify_failure(&err) == FailureClass::Rejected {
            return Err(err);
        }
        // At least api's Retry-After on a 503 (database unavailable).
        let delay = backoff.delay_honouring(failures, retry_after(&err));
        if started.elapsed() + delay > budget {
            return Err(err);
        }
        tracing::warn!(
            error = ?err,
            attempt = failures + 1,
            retry_in_secs = delay.as_secs(),
            "ingestion POST of {noun} failed; retrying without re-fetching upstream"
        );
        tokio::time::sleep(delay).await;
        failures = failures.saturating_add(1);
    }
}

pub(crate) async fn fetch_last_fetched(
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
#[expect(
    clippy::items_after_statements,
    reason = "test code: fixtures sit next to their use"
)]
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

        let tokens = OAuthTokenCache::new(crate::oauth_client::OAuthCredentials {
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

        let tokens = OAuthTokenCache::new(crate::oauth_client::OAuthCredentials {
            token_url: format!("{}/token/", server.uri()),
            client_id: "c".to_string(),
            scope: "groups".to_string(),
            username: "u".to_string(),
            password: "p".to_string(),
        });
        let client = reqwest::Client::new();

        #[derive(serde::Deserialize)]
        struct Thing {
            #[expect(dead_code, reason = "the field exists only so the JSON deserializes")]
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

        let tokens = OAuthTokenCache::new(crate::oauth_client::OAuthCredentials {
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
            retry_after: None,
        }
        .into()
    }

    #[test]
    fn retry_after_is_read_from_a_503_or_429_only() {
        let mut headers = reqwest::header::HeaderMap::new();
        assert_eq!(parse_retry_after(&headers), None);
        headers.insert(reqwest::header::RETRY_AFTER, "30".parse().unwrap());
        assert_eq!(parse_retry_after(&headers), Some(Duration::from_secs(30)));
        headers.insert(
            reqwest::header::RETRY_AFTER,
            "Wed, 21 Oct 2015 07:28:00 GMT".parse().unwrap(),
        );
        assert_eq!(parse_retry_after(&headers), None, "HTTP-dates are ignored");

        let with = |status: u16| -> anyhow::Error {
            anyhow::Error::from(HttpStatusError {
                prefix: "ingestion POST failed",
                status: reqwest::StatusCode::from_u16(status).unwrap(),
                body: String::new(),
                retry_after: Some(Duration::from_secs(30)),
            })
            .context("posting stations")
        };
        assert_eq!(retry_after(&with(503)), Some(Duration::from_secs(30)));
        assert_eq!(retry_after(&with(429)), Some(Duration::from_secs(30)));
        assert_eq!(retry_after(&with(500)), None);
        assert_eq!(retry_after(&anyhow::anyhow!("connection refused")), None);
    }

    /// A GET answered 503 keeps its status (still transient) and its
    /// Retry-After.
    #[tokio::test]
    async fn get_json_carries_a_503s_retry_after() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, ResponseTemplate};

        let (server, tokens) = svc09_server_and_tokens().await;
        Mock::given(method("GET"))
            .and(path("/private/thing"))
            .respond_with(
                ResponseTemplate::new(503)
                    .insert_header("Retry-After", "30")
                    .set_body_string(r#"{"error":"service_unavailable","retryable":true}"#),
            )
            .mount(&server)
            .await;
        let err = get_json::<serde_json::Value>(
            &reqwest::Client::new(),
            &format!("{}/private/thing", server.uri()),
            &tokens,
        )
        .await
        .unwrap_err();
        assert_eq!(classify_failure(&err), FailureClass::Transient);
        assert_eq!(retry_after(&err), Some(Duration::from_secs(30)));
        assert!(err.to_string().contains("503"), "{err}");
    }

    /// A transient POST failure answered 503 + Retry-After waits at least
    /// that long before retrying, however short the backoff.
    #[tokio::test]
    async fn a_503_post_retry_waits_for_retry_after() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, ResponseTemplate};

        let (server, tokens) = svc09_server_and_tokens().await;
        Mock::given(method("POST"))
            .and(path("/private/tocs"))
            .respond_with(ResponseTemplate::new(503).insert_header("Retry-After", "1"))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/private/tocs"))
            .respond_with(ResponseTemplate::new(200))
            .with_priority(2)
            .mount(&server)
            .await;
        let started = std::time::Instant::now();
        post_batch_retrying_with(
            &reqwest::Client::new(),
            &format!("{}/private/tocs", server.uri()),
            &tokens,
            &["TOC"],
            "TOCs",
            Duration::from_secs(5),
            FAST_WAIT.backoff,
        )
        .await
        .expect("the retry succeeds");
        assert!(
            started.elapsed() >= Duration::from_secs(1),
            "waited only {:?}",
            started.elapsed()
        );
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
        assert_eq!(
            classify_failure(
                &anyhow::Error::from(DataRejected("23505 duplicate key".to_string()))
                    .context("writing incidents")
            ),
            FailureClass::Rejected,
            "a DB sink's data rejection is a rejection too"
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

    async fn svc09_server_and_tokens() -> (wiremock::MockServer, OAuthTokenCache) {
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
        let tokens = OAuthTokenCache::new(crate::oauth_client::OAuthCredentials {
            token_url: format!("{}/token/", server.uri()),
            client_id: "c".to_string(),
            scope: "groups".to_string(),
            username: "u".to_string(),
            password: "p".to_string(),
        });
        (server, tokens)
    }

    const FAST_WAIT: ApiWait = ApiWait {
        backoff: Backoff::new(Duration::from_millis(10), Duration::from_millis(20)),
        max_wait: Duration::from_secs(5),
    };

    /// SVC-09: a freshness GET that fails because `api` is still starting
    /// is retried until `api` answers, instead of being treated as "poll
    /// now" (which spent the daily upstream fetch on a doomed POST).
    #[tokio::test]
    async fn the_freshness_check_waits_for_api_to_come_up() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, ResponseTemplate};

        let (server, tokens) = svc09_server_and_tokens().await;
        let fetched_at = Utc::now() - chrono::Duration::hours(1);
        Mock::given(method("GET"))
            .and(path("/private/tocs"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(3)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/private/tocs"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "fetchedAt": fetched_at })),
            )
            .with_priority(2)
            .mount(&server)
            .await;

        let delay = time_until_next_poll_waiting(
            &reqwest::Client::new(),
            &format!("{}/private/tocs", server.uri()),
            &tokens,
            Duration::from_secs(86_400),
            &FAST_WAIT,
            None,
        )
        .await;
        // Fetched an hour ago on a 24h interval: ~23h to go, NOT "poll now".
        assert!(delay > Duration::from_secs(22 * 3600), "{delay:?}");
    }

    /// Plan 4.6: the same `time_until_next_poll` behaviour whichever
    /// [`CursorSource`] the cursor comes from -- a fresh cursor delays the
    /// first poll, a failing source is waited for, `None` and a source that
    /// never answers mean "poll now".
    #[tokio::test]
    async fn every_cursor_source_gives_the_same_first_poll_delay() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};

        use wiremock::matchers::{method, path};
        use wiremock::{Mock, ResponseTemplate};

        let day = Duration::from_secs(86_400);
        let hour_ago = Utc::now() - chrono::Duration::hours(1);
        let (server, tokens) = svc09_server_and_tokens().await;
        Mock::given(method("GET"))
            .and(path("/private/tocs"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(2)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/private/tocs"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "fetchedAt": hour_ago })),
            )
            .with_priority(2)
            .mount(&server)
            .await;
        let client = reqwest::Client::new();
        let url = format!("{}/private/tocs", server.uri());

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
        let sources = [
            CursorSource::http(&client, &url, &tokens),
            CursorSource::db(flaky(Some(hour_ago))),
            CursorSource::stream(flaky(Some(hour_ago))),
        ];
        for mut source in sources {
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

    /// The fallback is unchanged: once `max_wait` runs out, poll now.
    #[tokio::test]
    async fn the_freshness_check_still_falls_back_to_poll_now_after_max_wait() {
        use wiremock::matchers::method;
        use wiremock::{Mock, ResponseTemplate};

        let (server, tokens) = svc09_server_and_tokens().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let wait = ApiWait {
            max_wait: Duration::from_millis(100),
            ..FAST_WAIT
        };
        let delay = time_until_next_poll_waiting(
            &reqwest::Client::new(),
            &format!("{}/private/tocs", server.uri()),
            &tokens,
            Duration::from_secs(86_400),
            &wait,
            None,
        )
        .await;
        assert_eq!(delay, Duration::ZERO);
    }

    /// SVC-09: a POST that fails because `api` is down is retried with the
    /// SAME body -- the upstream data is not fetched again.
    #[tokio::test]
    async fn a_transient_post_failure_is_retried_without_refetching() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, ResponseTemplate};

        let (server, tokens) = svc09_server_and_tokens().await;
        Mock::given(method("POST"))
            .and(path("/private/tocs"))
            .respond_with(ResponseTemplate::new(502))
            .up_to_n_times(2)
            .with_priority(1)
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/private/tocs"))
            .respond_with(ResponseTemplate::new(200))
            .with_priority(2)
            .expect(1)
            .mount(&server)
            .await;

        post_batch_retrying_with(
            &reqwest::Client::new(),
            &format!("{}/private/tocs", server.uri()),
            &tokens,
            &["TOC"],
            "TOCs",
            Duration::from_secs(5),
            FAST_WAIT.backoff,
        )
        .await
        .expect("the third attempt succeeds");
    }

    /// The incidents poller's snapshot is an object, not an array: it must
    /// reach the api as-is (not wrapped in a one-element array), and get
    /// the same transient-failure retry as a batch.
    #[tokio::test]
    async fn a_counted_object_body_is_posted_unwrapped_and_retried() {
        use wiremock::matchers::{body_json, method, path};
        use wiremock::{Mock, ResponseTemplate};

        let (server, tokens) = svc09_server_and_tokens().await;
        let snapshot = crate::IncidentSnapshot {
            incidents: Vec::new(),
            complete: true,
            skipped: 0,
        };
        let expected = serde_json::json!({"incidents": [], "complete": true, "skipped": 0});
        Mock::given(method("POST"))
            .and(path("/private/incidents"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(1)
            .with_priority(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/private/incidents"))
            .and(body_json(&expected))
            .respond_with(ResponseTemplate::new(200))
            .with_priority(2)
            .expect(1)
            .mount(&server)
            .await;

        post_counted_retrying_with(
            &reqwest::Client::new(),
            &format!("{}/private/incidents", server.uri()),
            &tokens,
            &snapshot,
            0,
            "incidents",
            Duration::from_secs(5),
            FAST_WAIT.backoff,
        )
        .await
        .expect("the second attempt succeeds");
    }

    #[tokio::test]
    async fn a_rejected_post_is_not_retried() {
        use wiremock::matchers::method;
        use wiremock::{Mock, ResponseTemplate};

        let (server, tokens) = svc09_server_and_tokens().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(422))
            .expect(1)
            .mount(&server)
            .await;
        let err = post_batch_retrying_with(
            &reqwest::Client::new(),
            &format!("{}/private/tocs", server.uri()),
            &tokens,
            &["TOC"],
            "TOCs",
            Duration::from_secs(5),
            FAST_WAIT.backoff,
        )
        .await
        .expect_err("422 is a data rejection");
        assert_eq!(classify_failure(&err), FailureClass::Rejected);
    }

    #[tokio::test]
    async fn post_retries_stop_at_the_budget() {
        let client = reqwest::Client::new();
        // Nothing listens here: every attempt is a connection error.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let (_server, tokens) = svc09_server_and_tokens().await;
        let started = std::time::Instant::now();
        let err = post_batch_retrying_with(
            &client,
            &format!("http://{addr}/private/tocs"),
            &tokens,
            &["TOC"],
            "TOCs",
            Duration::from_millis(200),
            FAST_WAIT.backoff,
        )
        .await
        .expect_err("api never comes up");
        assert_eq!(classify_failure(&err), FailureClass::Transient);
        assert!(started.elapsed() < Duration::from_secs(2));
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
