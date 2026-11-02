//! The domain-free runtime every Distant Signal service shares: startup and
//! retry, logging, metrics, service arguments, secrets, the Redis and
//! Postgres connection settings, advisory-lock keys and the outbound
//! `User-Agent`. Nothing here knows about rail; the domain types live in
//! `common`, which depends on this crate (repo-split spec §5.8).

pub mod advisory_locks;
pub mod backoff;
pub mod heap;
pub mod log_once;
pub mod logging;
pub mod metrics;
#[cfg(feature = "http")]
pub mod outbound_endpoint_guard;
#[cfg(feature = "postgres")]
pub mod pg;
pub mod progress;
pub mod redis_auth;
#[cfg(feature = "redis")]
pub mod redis_conn;
#[cfg(feature = "redis")]
pub mod redis_group;
pub mod secret;
pub mod service_args;
pub mod startup;
pub mod user_agent;
