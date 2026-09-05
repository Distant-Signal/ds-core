//! Client-credentials OAuth2 token fetch + cache, shared by every real
//! internal caller of `api`'s `/private/*` routes. Hand-rolled (not the
//! `oauth2` crate) -- see
//! docs/superpowers/specs/2026-09-02-internal-service-oauth2-design.md
//! Decision 4: `crates/api` pins reqwest 0.12 (required by
//! `oauth2 5.0`/`openidconnect 4.0`'s `AsyncHttpClient` impl), while
//! `common` and every one of its 8 real callers pin reqwest 0.13.4 --
//! pulling `oauth2` into `common` would add a second, incompatible
//! reqwest major version to every caller's dependency tree, with no way
//! to share one `reqwest::Client` instance between a caller's normal HTTP
//! calls and its token-exchange calls. This is one `POST` with a small
//! form-encoded body and one JSON response -- a narrow, fully
//! RFC-6749-§4.3.2-specified shape, well within this codebase's
//! existing hand-roll-narrow-things posture (see
//! `crates/api/src/auth.rs`'s `constant_time_eq`/`parse_cookie`).

use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Deserialize;

/// Every real caller's own OAuth2 client-credentials config -- mirrors the
/// design's Decision 6 field table exactly: `token_url`/`client_id`/`scope`
/// are shared (the same value repeated per binary, like the old shared
/// secret this design retired was before it), `username`/`password` are
/// per-service and are the actual secret.
#[derive(Clone)]
pub struct OAuthCredentials {
    pub token_url: String,
    pub client_id: String,
    pub scope: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

struct CachedToken {
    access_token: String,
    /// The instant this cache entry should be treated as stale -- NOT the
    /// raw `expires_in` deadline. Refreshed early (`REFRESH_MARGIN`) so a
    /// caller essentially never presents an already-expired token to
    /// `api` in the steady state.
    refresh_at: Instant,
}

/// Refresh this many seconds before the token's own `expires_in` -- a
/// fixed safety margin, matching this codebase's existing preference for
/// a flat constant over a percentage-of-lifetime calculation (see
/// `crates/common::ingest`'s own `duration_until_next_poll`, a similarly
/// fixed-window design). If a token's own `expires_in` is shorter than
/// this margin, `Duration::saturating_sub` clamps the result to zero --
/// the very next call refetches, never underflows or panics.
const REFRESH_MARGIN: Duration = Duration::from_secs(30);

/// Caches the last-fetched access token and its refresh deadline. Guarded
/// by a `std::sync::Mutex`, not `tokio::sync::Mutex`: the critical section
/// (checking/updating the cached value) never awaits while holding the
/// lock -- the token-fetch POST itself happens outside the guard, in
/// `fetch_token` -- so a blocking mutex is correct and simpler.
pub struct OAuthTokenCache {
    credentials: OAuthCredentials,
    cached: Mutex<Option<CachedToken>>,
}

impl OAuthTokenCache {
    pub fn new(credentials: OAuthCredentials) -> Self {
        Self {
            credentials,
            cached: Mutex::new(None),
        }
    }

    /// Returns a currently-valid bearer token: the cached one if it still
    /// has comfortable headroom before its own refresh deadline
    /// (`fresh_cached_token`), or a freshly fetched one otherwise
    /// (re-cached for the next call). Callers pass their own
    /// `reqwest::Client` -- this type holds no client of its own, matching
    /// every existing `common::ingest` call site's shape (client already
    /// threaded through as a parameter).
    pub async fn get_token(&self, client: &reqwest::Client) -> anyhow::Result<String> {
        if let Some(token) = self.fresh_cached_token() {
            return Ok(token);
        }
        let (access_token, expires_in) = self.fetch_token(client).await?;
        let refresh_at =
            Instant::now() + Duration::from_secs(expires_in).saturating_sub(REFRESH_MARGIN);
        let token_for_return = access_token.clone();
        *self
            .cached
            .lock()
            .expect("oauth token cache mutex poisoned") = Some(CachedToken {
            access_token,
            refresh_at,
        });
        Ok(token_for_return)
    }

    fn fresh_cached_token(&self) -> Option<String> {
        let guard = self
            .cached
            .lock()
            .expect("oauth token cache mutex poisoned");
        let cached = guard.as_ref()?;
        (Instant::now() < cached.refresh_at).then(|| cached.access_token.clone())
    }

    async fn fetch_token(&self, client: &reqwest::Client) -> anyhow::Result<(String, u64)> {
        let response = client
            .post(&self.credentials.token_url)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", self.credentials.client_id.as_str()),
                ("username", self.credentials.username.as_str()),
                ("password", self.credentials.password.as_str()),
                ("scope", self.credentials.scope.as_str()),
            ])
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("oauth2 token fetch failed: {status} {text}");
        }
        let body: TokenResponse = response.json().await?;
        Ok((body.access_token, body.expires_in))
    }
}

/// Every real caller's own copy of the 5 `internal_oauth_*` CLI/env flags
/// (identical field names, types, and the one real default --
/// `internal_oauth_scope`'s `"groups"` -- across all 9 real callers,
/// confirmed byte-for-byte identical in
/// docs/superpowers/specs/2026-09-05-rust-service-deduplication-design.md
/// §3.2). `#[command(flatten)]` this into a `Config` struct to gain these
/// 5 flags with their existing `--internal-oauth-*`/`INTERNAL_OAUTH_*`
/// names unchanged.
#[derive(Debug, Clone, clap::Args)]
pub struct InternalOAuthArgs {
    #[arg(long, env)]
    pub internal_oauth_token_url: String,
    #[arg(long, env)]
    pub internal_oauth_client_id: String,
    #[arg(long, env, default_value = "groups")]
    pub internal_oauth_scope: String,
    /// This service's own Authentik service-account credential --
    /// per-service, distinct from every other caller's.
    #[arg(long, env)]
    pub internal_oauth_username: String,
    #[arg(long, env)]
    pub internal_oauth_password: String,
}

impl InternalOAuthArgs {
    /// Builds the `OAuthTokenCache` every real caller previously
    /// hand-constructed identically at its own call site (9 byte-for-byte
    /// copies of `OAuthTokenCache::new(OAuthCredentials { ... })`).
    pub fn token_cache(&self) -> OAuthTokenCache {
        OAuthTokenCache::new(OAuthCredentials {
            token_url: self.internal_oauth_token_url.clone(),
            client_id: self.internal_oauth_client_id.clone(),
            scope: self.internal_oauth_scope.clone(),
            username: self.internal_oauth_username.clone(),
            password: self.internal_oauth_password.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn credentials(token_url: String) -> OAuthCredentials {
        OAuthCredentials {
            token_url,
            client_id: "distant-signal-internal".to_string(),
            scope: "groups".to_string(),
            username: "svc-poller-incidents".to_string(),
            password: "app-password".to_string(),
        }
    }

    #[test]
    fn token_cache_builds_from_the_flattened_args_unchanged() {
        let args = InternalOAuthArgs {
            internal_oauth_token_url: "http://auth.invalid/token".to_string(),
            internal_oauth_client_id: "distant-signal-internal".to_string(),
            internal_oauth_scope: "groups".to_string(),
            internal_oauth_username: "svc-test".to_string(),
            internal_oauth_password: "app-password".to_string(),
        };
        // token_cache() itself has no externally observable state beyond
        // constructing an OAuthTokenCache -- this just confirms it doesn't
        // panic and produces a real cache (get_token's own network-hitting
        // behavior is already covered by OAuthTokenCache's existing tests
        // above, which this method threads through unchanged).
        let _cache = args.token_cache();
    }

    async fn mock_token_endpoint(server: &MockServer, expires_in: u64, expect_calls: u64) {
        Mock::given(method("POST"))
            .and(path("/token/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "fake-jwt-access-token",
                "expires_in": expires_in,
                "token_type": "Bearer",
            })))
            .expect(expect_calls)
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn a_fresh_cached_token_is_reused_not_refetched() {
        let server = MockServer::start().await;
        mock_token_endpoint(&server, 300, 1).await;
        let cache = OAuthTokenCache::new(credentials(format!("{}/token/", server.uri())));
        let client = reqwest::Client::new();

        let first = cache.get_token(&client).await.unwrap();
        let second = cache.get_token(&client).await.unwrap();

        assert_eq!(first, "fake-jwt-access-token");
        assert_eq!(second, "fake-jwt-access-token");
        // wiremock's `.expect(1)` (asserted on Drop) fails the test if the
        // mock was hit more than once -- the real assertion here.
    }

    #[tokio::test]
    async fn a_token_near_its_own_expiry_triggers_a_fresh_fetch() {
        let server = MockServer::start().await;
        // expires_in (5s) is well under REFRESH_MARGIN (30s), so
        // `refresh_at` saturates to "now" -- the cached entry is
        // immediately stale, and the second call must refetch.
        mock_token_endpoint(&server, 5, 2).await;
        let cache = OAuthTokenCache::new(credentials(format!("{}/token/", server.uri())));
        let client = reqwest::Client::new();

        cache.get_token(&client).await.unwrap();
        cache.get_token(&client).await.unwrap();
    }

    #[tokio::test]
    async fn a_failed_fetch_returns_err_and_does_not_poison_the_cache_for_the_next_call() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token/"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        mock_token_endpoint(&server, 300, 1).await;
        let cache = OAuthTokenCache::new(credentials(format!("{}/token/", server.uri())));
        let client = reqwest::Client::new();

        let first = cache.get_token(&client).await;
        assert!(
            first.is_err(),
            "the mocked 500 must surface as an Err, not a panic"
        );

        let second = cache.get_token(&client).await;
        assert_eq!(
            second.unwrap(),
            "fake-jwt-access-token",
            "a failed fetch must not poison the cache -- the next call retries cleanly"
        );
    }
}
