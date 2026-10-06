//! Keyless Google Cloud credentials: an `external_account` credential
//! configuration (Google workload identity federation) turned into OAuth
//! access tokens, with no long-lived key anywhere.
//!
//! For schedule-ingest's GCS bucket reader in
//! `scheduleFeed.bucket.auth=workloadIdentity` mode (see
//! docs/superpowers/specs/2026-10-02-schedule-feed-gcs-landing-design.md,
//! "Keyless reader credentials"). `object_store` 0.14.2 reads only
//! `service_account` and `authorized_user` credential files, so the reader
//! wraps an [`ExternalAccountTokenSource`] in an
//! `object_store::CredentialProvider<Credential = GcpCredential>` and passes
//! it to `GoogleCloudStorageBuilder::with_credentials`.
//!
//! The flow, on every refresh:
//!
//! 1. read the subject token from `credential_source.file` (a
//!    kubelet-projected service-account token, rotated by the kubelet, so
//!    re-read every time);
//! 2. exchange it at Google STS (`token_url`, RFC 8693 token exchange, a
//!    JWT subject token) for a federated access token;
//! 3. with `service_account_impersonation_url` set (the usual case),
//!    exchange that at IAM Credentials `generateAccessToken` for the
//!    reader service account's access token (an hour by default).
//!
//! Tokens are cached until a margin before they expire (the larger of the
//! configured skew and 10% of the lifetime) and refreshed single-flight; a
//! refresh that fails inside the margin keeps the cached token until it
//! expires. Every token is a [`Secret`] and never logged.
//!
//! Metrics, shaped like the enricher's `enricher_llm_token_exchange_total`:
//! `gcp_token_exchange_total{stage="sts"|"impersonation", outcome}` and
//! `gcp_token_remaining_seconds`, both under the shared `distant_signal_`
//! prefix ([`crate::metrics::metric_name`]).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use serde::Deserialize;

use crate::secret::Secret;

/// `gcp_token_exchange_total{stage, outcome}`.
pub const EXCHANGE_METRIC: &str = "gcp_token_exchange_total";
/// `gcp_token_remaining_seconds`: the cached access token's remaining
/// lifetime, set at every exchange and every [`ExternalAccountTokenSource::access_token`].
pub const REMAINING_METRIC: &str = "gcp_token_remaining_seconds";

/// Every `outcome` label of [`EXCHANGE_METRIC`], registered at 0:
///
/// - `token_file_error`: the subject token file is missing, unreadable or
///   empty;
/// - `invalid_grant`, `invalid_target`, `invalid_request`: STS's OAuth
///   error (the token's issuer, audience or subject isn't trusted; the
///   provider in `audience` doesn't exist or is disabled; a malformed
///   request);
/// - `unauthenticated` (401), `permission_denied` (403): IAM Credentials
///   refused the federated token, or the principal may not impersonate the
///   service account (the binding is gone);
/// - `http_error` (any other non-2xx), `timeout`, `error` (connection or
///   response failure).
pub const EXCHANGE_OUTCOMES: [&str; 10] = [
    "success",
    "token_file_error",
    "invalid_grant",
    "invalid_target",
    "invalid_request",
    "unauthenticated",
    "permission_denied",
    "http_error",
    "timeout",
    "error",
];

/// The OAuth scope requested: what `object_store`'s own GCS credentials
/// use. IAM on the bucket, not the scope, is what confines the reader.
pub const DEFAULT_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

const TOKEN_EXCHANGE_GRANT: &str = "urn:ietf:params:oauth:grant-type:token-exchange";
const ACCESS_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:access_token";
/// Subject token types this source accepts (a k8s projected token is a JWT;
/// `id_token` is the OIDC spelling of the same thing).
const SUBJECT_TOKEN_TYPES: [&str; 2] = [
    "urn:ietf:params:oauth:token-type:jwt",
    "urn:ietf:params:oauth:token-type:id_token",
];

/// Lifetime assumed for a response that states none. Neither endpoint does
/// that; short on purpose.
const FALLBACK_LIFETIME: Duration = Duration::from_secs(300);
/// Ceiling on any stated lifetime (generateAccessToken allows at most 12 h).
const MAX_LIFETIME: Duration = Duration::from_secs(12 * 60 * 60);
/// A token this close to expiry is not handed out even as a fallback.
const MIN_REMAINING: Duration = Duration::from_secs(5);
/// Longest server-supplied error description kept in a log line.
const MAX_DESCRIPTION_CHARS: usize = 200;

/// Which endpoint an exchange (or its failure) belongs to: the `stage`
/// label of [`EXCHANGE_METRIC`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    /// Google STS (`token_url`).
    Sts,
    /// IAM Credentials `generateAccessToken`.
    Impersonation,
}

impl Stage {
    pub fn label(self) -> &'static str {
        match self {
            Self::Sts => "sts",
            Self::Impersonation => "impersonation",
        }
    }
}

/// No access token could be minted. `outcome` is one of
/// [`EXCHANGE_OUTCOMES`] other than `success`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CredentialError {
    pub stage: Stage,
    pub outcome: &'static str,
}

impl CredentialError {
    /// Whether this says the reader's access itself is gone (no token file,
    /// an untrusted token, no impersonation binding) rather than a
    /// transient failure: the bucket reader's "revoked access" state.
    pub fn is_access_revoked(self) -> bool {
        matches!(
            self.outcome,
            "token_file_error"
                | "invalid_grant"
                | "invalid_target"
                | "unauthenticated"
                | "permission_denied"
        )
    }
}

impl std::fmt::Display for CredentialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "no Google access token: {} exchange failed ({})",
            self.stage.label(),
            self.outcome
        )
    }
}

impl std::error::Error for CredentialError {}

/// The `external_account` credential configuration (the JSON
/// `GOOGLE_APPLICATION_CREDENTIALS` names). Only a file-sourced subject
/// token is supported: the URL, executable and AWS sources are refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalAccountConfig {
    /// The workload identity pool provider's resource name
    /// (`//iam.googleapis.com/projects/<n>/locations/global/workloadIdentityPools/<pool>/providers/<provider>`).
    pub audience: String,
    pub subject_token_type: String,
    /// Google STS, `https://sts.googleapis.com/v1/token`.
    pub token_url: String,
    /// `https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/<email>:generateAccessToken`,
    /// or `None` to use the federated token itself.
    pub service_account_impersonation_url: Option<String>,
    /// The subject token file (`credential_source.file`).
    pub token_file: PathBuf,
    /// `credential_source.format.subject_token_field_name` for a JSON token
    /// file; `None` for a plain-text one (a projected k8s token).
    pub json_field: Option<String>,
}

#[derive(Deserialize)]
struct RawConfig {
    #[serde(rename = "type")]
    kind: String,
    audience: String,
    subject_token_type: String,
    token_url: String,
    #[serde(default)]
    service_account_impersonation_url: Option<String>,
    credential_source: RawCredentialSource,
}

#[derive(Deserialize)]
struct RawCredentialSource {
    #[serde(default)]
    file: Option<PathBuf>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    executable: Option<serde_json::Value>,
    #[serde(default)]
    environment_id: Option<String>,
    #[serde(default)]
    format: Option<RawFormat>,
}

#[derive(Deserialize)]
struct RawFormat {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    subject_token_field_name: Option<String>,
}

impl ExternalAccountConfig {
    /// Reads and validates the configuration file.
    pub fn from_file(path: &std::path::Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading the credential configuration {}", path.display()))?;
        Self::from_json(&raw)
    }

    /// Parses and validates a configuration. Callers should also apply
    /// [`Self::check_google_endpoints`] outside tests, so a configuration
    /// can't send the subject token to anything but Google.
    pub fn from_json(raw: &str) -> anyhow::Result<Self> {
        let raw: RawConfig =
            serde_json::from_str(raw).context("the credential configuration is not valid JSON")?;
        if raw.kind != "external_account" {
            bail!(
                "the credential configuration's type is {:?}, not \"external_account\"",
                raw.kind
            );
        }
        if !SUBJECT_TOKEN_TYPES.contains(&raw.subject_token_type.as_str()) {
            bail!(
                "subject_token_type {:?} is not supported (only a JWT subject token)",
                raw.subject_token_type
            );
        }
        let source = raw.credential_source;
        if source.url.is_some() || source.executable.is_some() || source.environment_id.is_some() {
            bail!("only a file credential_source is supported (no url, executable or aws)");
        }
        let token_file = source
            .file
            .filter(|f| !f.as_os_str().is_empty())
            .context("credential_source.file is required")?;
        let json_field = match source.format {
            None => None,
            Some(format) if format.kind == "text" => None,
            Some(format) if format.kind == "json" => Some(
                format
                    .subject_token_field_name
                    .filter(|f| !f.is_empty())
                    .context("a json credential_source.format needs subject_token_field_name")?,
            ),
            Some(format) => bail!(
                "credential_source.format.type {:?} is not text or json",
                format.kind
            ),
        };
        if raw.audience.is_empty() {
            bail!("audience is required");
        }
        Ok(Self {
            audience: raw.audience,
            subject_token_type: raw.subject_token_type,
            token_url: raw.token_url,
            service_account_impersonation_url: raw
                .service_account_impersonation_url
                .filter(|u| !u.is_empty()),
            token_file,
            json_field,
        })
    }

    /// Refuses a `token_url` or impersonation URL that isn't https on a
    /// `googleapis.com` host, so a bad configuration can't send the
    /// cluster's token anywhere else.
    pub fn check_google_endpoints(&self) -> anyhow::Result<()> {
        let urls = std::iter::once(("token_url", &self.token_url)).chain(
            self.service_account_impersonation_url
                .iter()
                .map(|u| ("service_account_impersonation_url", u)),
        );
        for (name, raw) in urls {
            let url = url::Url::parse(raw).with_context(|| format!("{name} is not a URL"))?;
            let google = url
                .host_str()
                .is_some_and(|h| h == "googleapis.com" || h.ends_with(".googleapis.com"));
            if url.scheme() != "https" || !google {
                bail!("{name} must be an https://*.googleapis.com URL, got {raw:?}");
            }
        }
        Ok(())
    }
}

/// Knobs of an [`ExternalAccountTokenSource`].
#[derive(Debug, Clone)]
pub struct TokenSourceSettings {
    /// Lower bound of the refresh margin; the margin is the larger of this
    /// and 10% of the token's lifetime.
    pub refresh_skew: Duration,
    /// Per-request timeout of each token-endpoint call.
    pub exchange_timeout: Duration,
    /// OAuth scope requested.
    pub scope: String,
}

impl Default for TokenSourceSettings {
    fn default() -> Self {
        Self {
            refresh_skew: Duration::from_secs(60),
            exchange_timeout: Duration::from_secs(10),
            scope: DEFAULT_SCOPE.to_string(),
        }
    }
}

/// Monotonic and wall clocks, offset together. Production never advances
/// it; tests do (pausing tokio's clock would fire reqwest's timeouts).
#[derive(Debug, Clone, Default)]
pub struct Clock {
    offset_ms: Arc<AtomicU64>,
}

impl Clock {
    fn offset(&self) -> Duration {
        Duration::from_millis(self.offset_ms.load(Ordering::Relaxed))
    }

    fn now(&self) -> Instant {
        Instant::now() + self.offset()
    }

    fn unix_now(&self) -> u64 {
        (SystemTime::now() + self.offset())
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs())
    }

    /// Moves both clocks forward (tests).
    pub fn advance(&self, by: Duration) {
        let millis = u64::try_from(by.as_millis()).unwrap_or(u64::MAX);
        self.offset_ms.fetch_add(millis, Ordering::Relaxed);
    }
}

struct CachedToken {
    token: Secret,
    refresh_at: Instant,
    expires_at: Instant,
}

#[derive(Default)]
struct RefreshState {
    /// The last failed refresh and when it ended: callers already waiting
    /// for the lock get this instead of repeating the exchange.
    last_failure: Option<(Instant, CredentialError)>,
}

/// Mints, caches and renews access tokens from an
/// [`ExternalAccountConfig`].
pub struct ExternalAccountTokenSource {
    config: ExternalAccountConfig,
    settings: TokenSourceSettings,
    http: reqwest::Client,
    clock: Clock,
    /// A std mutex: never held across an await.
    cached: std::sync::Mutex<Option<CachedToken>>,
    /// Single-flight refresh.
    refresh: tokio::sync::Mutex<RefreshState>,
}

impl std::fmt::Debug for ExternalAccountTokenSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalAccountTokenSource")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// STS's response (RFC 8693). No `Debug`: it holds a token.
#[derive(Deserialize)]
struct StsResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
}

/// generateAccessToken's response. No `Debug`: it holds a token.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ImpersonationResponse {
    access_token: String,
    expire_time: String,
}

/// A non-2xx body's error code and description: STS's flat OAuth shape
/// (`{"error": "invalid_grant", "error_description": ...}`) or Google's
/// API shape (`{"error": {"status": "PERMISSION_DENIED", "message": ...}}`).
#[derive(Debug, Default, PartialEq, Eq)]
struct ErrorBody {
    code: Option<String>,
    description: Option<String>,
}

fn parse_error_body(body: &str) -> ErrorBody {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return ErrorBody::default();
    };
    let text =
        |v: Option<&serde_json::Value>| v.and_then(serde_json::Value::as_str).map(str::to_string);
    match value.get("error") {
        Some(serde_json::Value::String(code)) => ErrorBody {
            code: Some(code.clone()),
            description: text(value.get("error_description")),
        },
        Some(object @ serde_json::Value::Object(_)) => ErrorBody {
            code: text(object.get("status")),
            description: text(object.get("message")),
        },
        _ => ErrorBody::default(),
    }
}

fn error_outcome(status: u16, body: &ErrorBody) -> &'static str {
    match (status, body.code.as_deref()) {
        (_, Some("invalid_grant")) => "invalid_grant",
        (_, Some("invalid_target")) => "invalid_target",
        (_, Some("invalid_request")) => "invalid_request",
        (401, _) | (_, Some("UNAUTHENTICATED")) => "unauthenticated",
        (403, _) | (_, Some("PERMISSION_DENIED")) => "permission_denied",
        _ => "http_error",
    }
}

fn record_exchange(stage: Stage, outcome: &'static str) {
    metrics::counter!(
        crate::metrics::metric_name(EXCHANGE_METRIC),
        "stage" => stage.label(),
        "outcome" => outcome
    )
    .increment(1);
}

/// Counts a failed exchange and returns it.
fn fail(stage: Stage, outcome: &'static str) -> CredentialError {
    record_exchange(stage, outcome);
    CredentialError { stage, outcome }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "a token lifetime in seconds is far below 2^52"
)]
fn record_remaining(remaining: Duration) {
    metrics::gauge!(crate::metrics::metric_name(REMAINING_METRIC)).set(remaining.as_secs() as f64);
}

/// `(refresh_at, expires_at)` for a token issued in a request started at
/// `t0` that lasts `lifetime` (capped at [`MAX_LIFETIME`]).
fn deadlines(t0: Instant, lifetime: Duration, skew: Duration) -> (Instant, Instant) {
    let lifetime = lifetime.min(MAX_LIFETIME);
    let margin = skew.max(lifetime / 10);
    (t0 + lifetime.saturating_sub(margin), t0 + lifetime)
}

impl ExternalAccountTokenSource {
    pub fn new(
        config: ExternalAccountConfig,
        settings: TokenSourceSettings,
    ) -> anyhow::Result<Self> {
        Self::with_clock(config, settings, Clock::default())
    }

    /// Like [`Self::new`], with a clock tests can advance.
    pub fn with_clock(
        config: ExternalAccountConfig,
        settings: TokenSourceSettings,
        clock: Clock,
    ) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(settings.exchange_timeout)
            .build()?;
        let source = Self {
            config,
            settings,
            http,
            clock,
            cached: std::sync::Mutex::new(None),
            refresh: tokio::sync::Mutex::new(RefreshState::default()),
        };
        source.register_metrics();
        Ok(source)
    }

    /// Every `{stage, outcome}` this configuration can produce, at 0.
    fn register_metrics(&self) {
        let stages: &[Stage] = if self.config.service_account_impersonation_url.is_some() {
            &[Stage::Sts, Stage::Impersonation]
        } else {
            &[Stage::Sts]
        };
        for stage in stages {
            for outcome in EXCHANGE_OUTCOMES {
                metrics::counter!(
                    crate::metrics::metric_name(EXCHANGE_METRIC),
                    "stage" => stage.label(),
                    "outcome" => outcome
                )
                .increment(0);
            }
        }
        record_remaining(Duration::ZERO);
    }

    fn lock_cached(&self) -> std::sync::MutexGuard<'_, Option<CachedToken>> {
        self.cached
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn fresh_token(&self) -> Option<Secret> {
        let now = self.clock.now();
        let cached = self.lock_cached();
        let token = cached.as_ref().filter(|t| now < t.refresh_at)?;
        record_remaining(token.expires_at.saturating_duration_since(now));
        Some(token.token.clone())
    }

    fn unexpired_token(&self) -> Option<Secret> {
        let now = self.clock.now();
        let cached = self.lock_cached();
        let token = cached
            .as_ref()
            .filter(|t| now + MIN_REMAINING < t.expires_at)?;
        Some(token.token.clone())
    }

    /// The access token for the next GCS request: the cached one, or a new
    /// one (single-flight). A failed refresh falls back to the cached token
    /// while it is still valid.
    pub async fn access_token(&self) -> Result<Secret, CredentialError> {
        if let Some(token) = self.fresh_token() {
            return Ok(token);
        }
        let waiting_since = self.clock.now();
        let mut state = self.refresh.lock().await;
        if let Some(token) = self.fresh_token() {
            return Ok(token);
        }
        if let Some((at, failure)) = state.last_failure
            && at >= waiting_since
        {
            return self.unexpired_token().ok_or(failure);
        }
        match self.refresh_locked().await {
            Ok(token) => {
                state.last_failure = None;
                Ok(token)
            }
            Err(failure) => {
                state.last_failure = Some((self.clock.now(), failure));
                match self.unexpired_token() {
                    Some(token) => {
                        tracing::warn!(
                            stage = failure.stage.label(),
                            outcome = failure.outcome,
                            "Google token refresh failed; using the cached token until it expires"
                        );
                        Ok(token)
                    }
                    None => Err(failure),
                }
            }
        }
    }

    /// GCS answered 401 to `rejected`: forget it, so the next
    /// [`Self::access_token`] exchanges again. A no-op unless it is still
    /// the cached token, so concurrent 401s cause one refresh. (A 403 is
    /// IAM refusing a valid token: don't invalidate, treat it as revoked
    /// access.)
    pub fn invalidate(&self, rejected: &Secret) {
        let mut cached = self.lock_cached();
        if cached
            .as_ref()
            .is_some_and(|t| t.token.expose() == rejected.expose())
        {
            *cached = None;
            record_remaining(Duration::ZERO);
            tracing::info!("GCS rejected the access token; it will be re-exchanged");
        }
    }

    async fn refresh_locked(&self) -> Result<Secret, CredentialError> {
        let subject = self.read_subject_token()?;
        let t0 = self.clock.now();
        let (federated, federated_lifetime) = self.sts_exchange(&subject).await?;
        let (token, lifetime) = match &self.config.service_account_impersonation_url {
            None => (federated, federated_lifetime),
            Some(url) => {
                let t1 = self.clock.now();
                let (token, lifetime) = self.impersonate(url, &federated).await?;
                // Measure from this request's start, not STS's.
                (
                    token,
                    lifetime.saturating_sub(t1.saturating_duration_since(t0)),
                )
            }
        };
        let (refresh_at, expires_at) = deadlines(t0, lifetime, self.settings.refresh_skew);
        record_remaining(expires_at.saturating_duration_since(self.clock.now()));
        *self.lock_cached() = Some(CachedToken {
            token: token.clone(),
            refresh_at,
            expires_at,
        });
        Ok(token)
    }

    /// The subject token, re-read every time (the kubelet rotates it).
    fn read_subject_token(&self) -> Result<Secret, CredentialError> {
        let failure = |reason: &str| {
            tracing::warn!(
                path = %self.config.token_file.display(),
                reason,
                "cannot read the workload identity subject token"
            );
            record_exchange(Stage::Sts, "token_file_error");
            CredentialError {
                stage: Stage::Sts,
                outcome: "token_file_error",
            }
        };
        let raw = std::fs::read_to_string(&self.config.token_file)
            .map_err(|err| failure(&err.to_string()))?;
        let token = match &self.config.json_field {
            None => raw.trim().to_string(),
            Some(field) => serde_json::from_str::<serde_json::Value>(&raw)
                .ok()
                .and_then(|v| v.get(field)?.as_str().map(str::to_string))
                .ok_or_else(|| failure("the JSON token file lacks the subject token field"))?,
        };
        if token.is_empty() {
            return Err(failure("the file is empty"));
        }
        Ok(Secret::new(token))
    }

    async fn sts_exchange(&self, subject: &Secret) -> Result<(Secret, Duration), CredentialError> {
        let form = [
            ("grant_type", TOKEN_EXCHANGE_GRANT),
            ("audience", self.config.audience.as_str()),
            ("scope", self.settings.scope.as_str()),
            ("requested_token_type", ACCESS_TOKEN_TYPE),
            ("subject_token", subject.expose()),
            (
                "subject_token_type",
                self.config.subject_token_type.as_str(),
            ),
        ];
        let request = self.http.post(&self.config.token_url).form(&form);
        let body = self.send(Stage::Sts, request, &[subject]).await?;
        let response: StsResponse = serde_json::from_str(&body).map_err(|_| {
            tracing::warn!(stage = "sts", "unreadable STS token response");
            fail(Stage::Sts, "error")
        })?;
        record_exchange(Stage::Sts, "success");
        let lifetime = response
            .expires_in
            .map_or(FALLBACK_LIFETIME, Duration::from_secs);
        Ok((Secret::new(response.access_token), lifetime))
    }

    async fn impersonate(
        &self,
        url: &str,
        federated: &Secret,
    ) -> Result<(Secret, Duration), CredentialError> {
        let request = self
            .http
            .post(url)
            .bearer_auth(federated.expose())
            .json(&serde_json::json!({ "scope": [self.settings.scope] }));
        let body = self
            .send(Stage::Impersonation, request, &[federated])
            .await?;
        let response: ImpersonationResponse = serde_json::from_str(&body).map_err(|_| {
            tracing::warn!(
                stage = "impersonation",
                "unreadable generateAccessToken response"
            );
            fail(Stage::Impersonation, "error")
        })?;
        let lifetime = chrono::DateTime::parse_from_rfc3339(&response.expire_time)
            .ok()
            .and_then(|at| u64::try_from(at.timestamp()).ok())
            .map_or(FALLBACK_LIFETIME, |at| {
                Duration::from_secs(at.saturating_sub(self.clock.unix_now()))
            });
        record_exchange(Stage::Impersonation, "success");
        Ok((Secret::new(response.access_token), lifetime))
    }

    /// Sends one token request; the 2xx body, or a classified failure.
    /// `redact` are the credentials in the request, scrubbed from any
    /// echoed error text.
    async fn send(
        &self,
        stage: Stage,
        request: reqwest::RequestBuilder,
        redact: &[&Secret],
    ) -> Result<String, CredentialError> {
        let response = request.send().await.map_err(|err| {
            let outcome = if err.is_timeout() { "timeout" } else { "error" };
            tracing::warn!(
                stage = stage.label(),
                outcome,
                error = %err.without_url(),
                "Google token request failed"
            );
            fail(stage, outcome)
        })?;
        let status = response.status();
        let body = response.text().await.map_err(|err| {
            let outcome = if err.is_timeout() { "timeout" } else { "error" };
            fail(stage, outcome)
        })?;
        if status.is_success() {
            return Ok(body);
        }
        let parsed = parse_error_body(&body);
        let outcome = error_outcome(status.as_u16(), &parsed);
        let description = parsed.description.map(|d| {
            let mut shown: String = d.chars().take(MAX_DESCRIPTION_CHARS).collect();
            for secret in redact {
                if !secret.is_empty() {
                    shown = shown.replace(secret.expose(), "[redacted]");
                }
            }
            shown
        });
        tracing::warn!(
            stage = stage.label(),
            status = status.as_u16(),
            error_code = parsed.code.as_deref(),
            error_description = description.as_deref(),
            outcome,
            "Google token request rejected"
        );
        Err(fail(stage, outcome))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use wiremock::matchers::{body_string_contains, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    const K8S_TOKEN: &str = "k8s.projected.jwt";
    const AUDIENCE: &str = "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/k3s/providers/mine-bringer";
    const IMPERSONATE: &str =
        "/v1/projects/-/serviceAccounts/reader@p.iam.gserviceaccount.com:generateAccessToken";

    fn token_file(contents: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        file
    }

    fn config_json(server: &MockServer, file: &std::path::Path, impersonate: bool) -> String {
        let mut json = serde_json::json!({
            "type": "external_account",
            "audience": AUDIENCE,
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": format!("{}/v1/token", server.uri()),
            "credential_source": { "file": file, "format": { "type": "text" } }
        });
        if impersonate {
            json["service_account_impersonation_url"] =
                format!("{}{IMPERSONATE}", server.uri()).into();
        }
        json.to_string()
    }

    fn source(
        server: &MockServer,
        file: &std::path::Path,
        impersonate: bool,
    ) -> (ExternalAccountTokenSource, Clock) {
        let config =
            ExternalAccountConfig::from_json(&config_json(server, file, impersonate)).unwrap();
        let clock = Clock::default();
        let settings = TokenSourceSettings {
            exchange_timeout: Duration::from_secs(2),
            ..TokenSourceSettings::default()
        };
        (
            ExternalAccountTokenSource::with_clock(config, settings, clock.clone()).unwrap(),
            clock,
        )
    }

    fn sts_ok(token: &str, expires_in: u64) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": token,
            "issued_token_type": ACCESS_TOKEN_TYPE,
            "token_type": "Bearer",
            "expires_in": expires_in
        }))
    }

    fn impersonation_ok(token: &str, lifetime: Duration) -> ResponseTemplate {
        let expire = chrono::Utc::now() + chrono::Duration::from_std(lifetime).unwrap();
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "accessToken": token,
            "expireTime": expire.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        }))
    }

    #[tokio::test]
    async fn exchanges_then_impersonates_and_caches() {
        let server = MockServer::start().await;
        let file = token_file(&format!("{K8S_TOKEN}\n"));
        Mock::given(method("POST"))
            .and(path("/v1/token"))
            .and(body_string_contains(
                "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange",
            ))
            .and(body_string_contains("subject_token=k8s.projected.jwt"))
            .and(body_string_contains(
                "subject_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Ajwt",
            ))
            .and(body_string_contains(
                "requested_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Aaccess_token",
            ))
            .and(body_string_contains(
                "audience=%2F%2Fiam.googleapis.com%2Fprojects%2F123",
            ))
            .and(body_string_contains(
                "scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform",
            ))
            .respond_with(sts_ok("federated", 3600))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(IMPERSONATE))
            .and(header("authorization", "Bearer federated"))
            .and(body_string_contains("cloud-platform"))
            .respond_with(impersonation_ok("ya29.reader", Duration::from_secs(3600)))
            .expect(1)
            .mount(&server)
            .await;
        let (source, _) = source(&server, file.path(), true);
        assert_eq!(source.access_token().await.unwrap().expose(), "ya29.reader");
        // Cached: no second exchange (the mocks' expect(1) checks on drop).
        assert_eq!(source.access_token().await.unwrap().expose(), "ya29.reader");
        assert!(!format!("{source:?}").contains("ya29"));
    }

    #[tokio::test]
    async fn without_impersonation_the_federated_token_is_used() {
        let server = MockServer::start().await;
        let file = token_file(K8S_TOKEN);
        Mock::given(method("POST"))
            .and(path("/v1/token"))
            .respond_with(sts_ok("federated", 3600))
            .expect(1)
            .mount(&server)
            .await;
        let (source, _) = source(&server, file.path(), false);
        assert_eq!(source.access_token().await.unwrap().expose(), "federated");
    }

    /// Refreshed a margin before expiry, re-reading the rotated token file.
    #[tokio::test]
    async fn refreshes_before_expiry_with_the_rotated_subject_token() {
        let server = MockServer::start().await;
        let file = token_file("first.jwt");
        Mock::given(method("POST"))
            .and(path("/v1/token"))
            .and(body_string_contains("subject_token=first.jwt"))
            .respond_with(sts_ok("fed-1", 3600))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/token"))
            .and(body_string_contains("subject_token=second.jwt"))
            .respond_with(sts_ok("fed-2", 3600))
            .expect(1)
            .mount(&server)
            .await;
        let (source, clock) = source(&server, file.path(), false);
        assert_eq!(source.access_token().await.unwrap().expose(), "fed-1");
        std::fs::write(file.path(), "second.jwt").unwrap();
        // 3600 s lifetime: margin max(60 s, 360 s) = 360 s, so 3200 s in is
        // still fresh and 3250 s in is not.
        clock.advance(Duration::from_secs(3200));
        assert_eq!(source.access_token().await.unwrap().expose(), "fed-1");
        clock.advance(Duration::from_secs(50));
        assert_eq!(source.access_token().await.unwrap().expose(), "fed-2");
    }

    /// Inside the margin a failed refresh keeps the cached token; past
    /// expiry the failure is returned.
    #[tokio::test]
    async fn a_failed_refresh_falls_back_to_the_unexpired_token() {
        let server = MockServer::start().await;
        let file = token_file(K8S_TOKEN);
        Mock::given(method("POST"))
            .and(path("/v1/token"))
            .respond_with(sts_ok("fed-1", 3600))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        let (source, clock) = source(&server, file.path(), false);
        assert_eq!(source.access_token().await.unwrap().expose(), "fed-1");
        Mock::given(method("POST"))
            .and(path("/v1/token"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        clock.advance(Duration::from_secs(3300));
        assert_eq!(source.access_token().await.unwrap().expose(), "fed-1");
        clock.advance(Duration::from_secs(400));
        let err = source.access_token().await.unwrap_err();
        assert_eq!(err.outcome, "http_error");
        assert!(!err.is_access_revoked());
    }

    #[tokio::test]
    async fn concurrent_callers_share_one_exchange() {
        let server = MockServer::start().await;
        let file = token_file(K8S_TOKEN);
        Mock::given(method("POST"))
            .and(path("/v1/token"))
            .respond_with(sts_ok("fed", 3600).set_delay(Duration::from_millis(100)))
            .expect(1)
            .mount(&server)
            .await;
        let (source, _) = source(&server, file.path(), false);
        let source = Arc::new(source);
        let calls: Vec<_> = (0..8)
            .map(|_| {
                let source = Arc::clone(&source);
                tokio::spawn(async move { source.access_token().await })
            })
            .collect();
        for call in calls {
            assert_eq!(call.await.unwrap().unwrap().expose(), "fed");
        }
    }

    /// A 401 from GCS invalidates the token it was given (once), so the
    /// next call exchanges again; a stale token's invalidation is a no-op.
    #[tokio::test]
    async fn invalidate_forces_a_new_exchange() {
        let server = MockServer::start().await;
        let file = token_file(K8S_TOKEN);
        Mock::given(method("POST"))
            .and(path("/v1/token"))
            .respond_with(sts_ok("fed-1", 3600))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/token"))
            .respond_with(sts_ok("fed-2", 3600))
            .mount(&server)
            .await;
        let (source, _) = source(&server, file.path(), false);
        let first = source.access_token().await.unwrap();
        source.invalidate(&first);
        let second = source.access_token().await.unwrap();
        assert_eq!(second.expose(), "fed-2");
        source.invalidate(&first);
        assert_eq!(source.access_token().await.unwrap().expose(), "fed-2");
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    /// Failures are classified, counted and never echo a token.
    #[tokio::test]
    async fn failures_are_classified_and_counted() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let server = MockServer::start().await;
        let file = token_file(K8S_TOKEN);
        // STS refuses the subject token, echoing it.
        Mock::given(method("POST"))
            .and(path("/v1/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "invalid_grant",
                "error_description": format!("The audience in ID Token [{K8S_TOKEN}] does not match")
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        let (source, _) = source(&server, file.path(), true);
        let err = source.access_token().await.unwrap_err();
        assert_eq!(
            err,
            CredentialError {
                stage: Stage::Sts,
                outcome: "invalid_grant"
            }
        );
        assert!(err.is_access_revoked());

        // STS fine, impersonation binding gone.
        Mock::given(method("POST"))
            .and(path("/v1/token"))
            .respond_with(sts_ok("federated", 3600))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(IMPERSONATE))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "error": { "code": 403, "status": "PERMISSION_DENIED",
                           "message": "Permission 'iam.serviceAccounts.getAccessToken' denied" }
            })))
            .mount(&server)
            .await;
        let err = source.access_token().await.unwrap_err();
        assert_eq!(
            err,
            CredentialError {
                stage: Stage::Impersonation,
                outcome: "permission_denied"
            }
        );

        // The token file disappears.
        let path = file.path().to_path_buf();
        drop(file);
        assert!(!path.exists());
        let err = source.access_token().await.unwrap_err();
        assert_eq!(err.outcome, "token_file_error");

        let rendered = handle.render();
        let metric = "distant_signal_gcp_token_exchange_total";
        for (stage, outcome, count) in [
            ("sts", "invalid_grant", 1),
            ("sts", "success", 1),
            ("sts", "token_file_error", 1),
            ("impersonation", "permission_denied", 1),
            ("impersonation", "success", 0),
            ("impersonation", "unauthenticated", 0),
        ] {
            let line = format!("{metric}{{stage=\"{stage}\",outcome=\"{outcome}\"}} {count}");
            assert!(rendered.contains(&line), "missing {line} in\n{rendered}");
        }
        assert!(rendered.contains("distant_signal_gcp_token_remaining_seconds 0"));
    }

    #[test]
    fn error_descriptions_and_outcomes() {
        for (status, body, outcome) in [
            (400, r#"{"error":"invalid_grant"}"#, "invalid_grant"),
            (400, r#"{"error":"invalid_target"}"#, "invalid_target"),
            (400, r#"{"error":"invalid_request"}"#, "invalid_request"),
            (
                401,
                r#"{"error":{"status":"UNAUTHENTICATED"}}"#,
                "unauthenticated",
            ),
            (403, "not json", "permission_denied"),
            (500, "", "http_error"),
        ] {
            assert_eq!(
                error_outcome(status, &parse_error_body(body)),
                outcome,
                "{body}"
            );
        }
    }

    #[test]
    fn config_parsing_and_validation() {
        let good = serde_json::json!({
            "type": "external_account",
            "audience": AUDIENCE,
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": "https://sts.googleapis.com/v1/token",
            "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/r@p.iam.gserviceaccount.com:generateAccessToken",
            "credential_source": {
                "file": "/var/run/secrets/distant-signal/gcs-token/token",
                "format": { "type": "text" }
            }
        });
        let config = ExternalAccountConfig::from_json(&good.to_string()).unwrap();
        assert_eq!(
            config.token_file,
            PathBuf::from("/var/run/secrets/distant-signal/gcs-token/token")
        );
        assert_eq!(config.json_field, None);
        config.check_google_endpoints().unwrap();

        let with = |pointer: &str, value: serde_json::Value| {
            let mut json = good.clone();
            *json.pointer_mut(pointer).unwrap() = value;
            ExternalAccountConfig::from_json(&json.to_string())
        };
        assert!(with("/type", "service_account".into()).is_err());
        assert!(
            with(
                "/subject_token_type",
                "urn:ietf:params:aws:token-type:aws4_request".into()
            )
            .is_err()
        );
        assert!(
            with(
                "/credential_source",
                serde_json::json!({ "url": "http://x" })
            )
            .is_err()
        );
        assert!(with("/credential_source", serde_json::json!({})).is_err());
        assert!(
            with(
                "/credential_source/format",
                serde_json::json!({ "type": "json" })
            )
            .is_err()
        );
        let json_file = with(
            "/credential_source/format",
            serde_json::json!({ "type": "json", "subject_token_field_name": "id_token" }),
        )
        .unwrap();
        assert_eq!(json_file.json_field.as_deref(), Some("id_token"));
        for bad in [
            "http://sts.googleapis.com/v1/token",
            "https://evil.example.com/v1/token",
            "https://googleapis.com.evil.example/v1/token",
        ] {
            let config = with("/token_url", bad.into()).unwrap();
            assert!(config.check_google_endpoints().is_err(), "{bad}");
        }
    }

    #[test]
    fn deadlines_use_the_larger_of_skew_and_a_tenth() {
        let t0 = Instant::now();
        let (refresh, expiry) = deadlines(t0, Duration::from_secs(3600), Duration::from_secs(60));
        assert_eq!(refresh - t0, Duration::from_secs(3240));
        assert_eq!(expiry - t0, Duration::from_secs(3600));
        let (refresh, _) = deadlines(t0, Duration::from_secs(300), Duration::from_secs(60));
        assert_eq!(refresh - t0, Duration::from_secs(240));
        let (_, expiry) = deadlines(t0, Duration::from_secs(10 * 86_400), Duration::ZERO);
        assert_eq!(expiry - t0, MAX_LIFETIME);
    }
}
