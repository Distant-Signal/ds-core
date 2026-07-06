//! Shared HTTP ingestion contract between the RDM pollers
//! (`crates/poller-incidents`, `crates/poller-stations`, `crates/poller-tocs`)
//! and the `api` crate's `/private/*` endpoints
//! (`crates/api/src/routes/ingest.rs`, gated by `crates/api/src/auth.rs`).
//!
//! Single source of truth for the two header names both sides must agree
//! on, plus the POST-batch-and-log pattern every poller repeats once per
//! poll cycle. Previously each poller (and `api`, for the internal-token
//! header) independently redefined these constants/logic; this module is
//! the one place that changes if either header name or the POST contract
//! ever needs to.

use serde::Serialize;

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
