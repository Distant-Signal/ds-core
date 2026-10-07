//! Redis AUTH for every crate that talks to Redis (INF-3, chart
//! `redis.auth`).
//!
//! The chart keeps `REDIS_URL` credential-free and, when `redis.auth.enabled`
//! is set, passes the password separately as `REDIS_PASSWORD` from a Secret.
//! Each service combines the two once at startup with
//! [`redis_url_with_password`] and hands the result to `redis::Client::open`.
//! Keeping the password out of `REDIS_URL` means:
//!
//! - it never appears in the rendered Deployment (the URL is a plain env
//!   `value`), only in a `secretKeyRef`;
//! - a password containing URL-reserved characters (`@`, `:`, `/`, `#`, ...)
//!   still works: it is percent-encoded here, and the redis crate decodes it
//!   again when it parses the URL.
//!
//! The password is sent as `AUTH default <password>` (the two-argument ACL
//! form), not the legacy `AUTH <password>`: the username `default` is filled
//! in when the URL has none. That matters for the rollout. Redis 6+ (the
//! chart runs 7.4) ACCEPTS `AUTH default <anything>` while the default user
//! still has no password (`nopass`), but REJECTS the one-argument form with
//! "AUTH <password> called without any password configured". So clients can
//! be given the password before the server starts requiring it, and nothing
//! breaks in between (see the chart's `redis.auth` values comment).
//!
//! With no password (unset or empty) the URL is returned untouched, not even
//! parsed, so a deployment without `REDIS_PASSWORD` behaves exactly as
//! before.
//!
//! Per-client ACL users (chart `redis.acl`, ingest architecture phase 0c):
//! a client given its own user gets `REDIS_USERNAME` (a plain value) next
//! to its own `REDIS_PASSWORD`, combined by [`redis_url_with_credentials`]
//! into `redis://<user>:<password>@host`. With `REDIS_USERNAME` unset or
//! empty that function is exactly [`redis_url_with_password`], so nothing
//! changes for a client that has not opted in.
//!
//! The combined URL is returned as a [`Secret`] so a `Debug` print cannot
//! leak it. Error messages here never include the URL or the password.

use anyhow::{Result, bail};

use crate::secret::Secret;

/// ACL user the password is presented for when `REDIS_URL` names none.
/// `requirepass` sets this user's password.
pub const DEFAULT_REDIS_USER: &str = "default";

/// `redis_url` with `password` applied as its userinfo.
///
/// - `password` `None` or empty: `redis_url` unchanged.
/// - Otherwise `redis_url` must be a `redis://` or `rediss://` URL without a
///   password of its own (two sources for one credential is a
///   misconfiguration, so it is refused rather than silently picking one).
///   A username already in the URL is kept (an ACL user); with none,
///   [`DEFAULT_REDIS_USER`] is used.
pub fn redis_url_with_password(redis_url: &str, password: Option<&Secret>) -> Result<Secret> {
    let Some(password) = password.filter(|p| !p.is_empty()) else {
        return Ok(Secret::new(redis_url));
    };

    // Deliberately no `.context(redis_url)`: the URL may already carry
    // credentials of its own.
    let Ok(mut url) = url::Url::parse(redis_url) else {
        bail!("REDIS_URL is not a valid URL (value not shown)");
    };
    if !matches!(url.scheme(), "redis" | "rediss") {
        bail!(
            "REDIS_PASSWORD is set, but REDIS_URL uses the `{}` scheme; it is only supported \
             with redis:// or rediss:// URLs",
            url.scheme()
        );
    }
    if url.password().is_some() {
        bail!(
            "REDIS_URL already contains a password and REDIS_PASSWORD is also set; \
             set only one of them"
        );
    }
    if url.username().is_empty() && url.set_username(DEFAULT_REDIS_USER).is_err() {
        bail!("REDIS_URL cannot carry credentials (it has no host)");
    }
    // `Url::set_password` percent-encodes the userinfo; the redis crate
    // percent-decodes it when parsing, so any byte sequence round-trips.
    if url.set_password(Some(password.expose())).is_err() {
        bail!("REDIS_URL cannot carry credentials (it has no host)");
    }
    Ok(Secret::new(String::from(url)))
}

/// `REDIS_USERNAME` from the environment: `None` when unset or empty. For a
/// binary whose clap `Config` does not declare the variable (the api).
pub fn username_from_env() -> Option<String> {
    std::env::var("REDIS_USERNAME")
        .ok()
        .filter(|user| !user.is_empty())
}

/// `redis_url` with an ACL `username` and `password` applied as its
/// userinfo (`REDIS_USERNAME` plus `REDIS_PASSWORD`).
///
/// - `username` `None` or empty: exactly [`redis_url_with_password`], so a
///   client without `REDIS_USERNAME` behaves as before (the `default` user).
/// - Otherwise a non-empty `password` is required (the chart's ACL users all
///   have one), and `redis_url` must be a `redis://` or `rediss://` URL
///   carrying no credentials of its own: two sources for one credential are
///   refused rather than silently picking one.
pub fn redis_url_with_credentials(
    redis_url: &str,
    username: Option<&str>,
    password: Option<&Secret>,
) -> Result<Secret> {
    let Some(username) = username.filter(|u| !u.is_empty()) else {
        return redis_url_with_password(redis_url, password);
    };
    let Some(password) = password.filter(|p| !p.is_empty()) else {
        bail!("REDIS_USERNAME is set but REDIS_PASSWORD is not; an ACL user needs its password");
    };
    let Ok(mut url) = url::Url::parse(redis_url) else {
        bail!("REDIS_URL is not a valid URL (value not shown)");
    };
    if !matches!(url.scheme(), "redis" | "rediss") {
        bail!(
            "REDIS_USERNAME is set, but REDIS_URL uses the `{}` scheme; it is only supported \
             with redis:// or rediss:// URLs",
            url.scheme()
        );
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!(
            "REDIS_URL already contains credentials and REDIS_USERNAME is also set; \
             set only one of them"
        );
    }
    // Both are percent-encoded here and decoded again by the redis crate.
    if url.set_username(username).is_err() || url.set_password(Some(password.expose())).is_err() {
        bail!("REDIS_URL cannot carry credentials (it has no host)");
    }
    Ok(Secret::new(String::from(url)))
}

#[cfg(test)]
mod tests {
    use redis::IntoConnectionInfo;

    use super::*;

    fn info(url: &Secret) -> redis::ConnectionInfo {
        url.expose()
            .into_connection_info()
            .expect("redis parses the URL")
    }

    #[test]
    fn no_password_leaves_the_url_untouched() {
        for url in [
            "redis://redis:6379",
            "redis://distant-signal-redis:6379",
            "unix:///run/redis.sock",
            "not a url at all",
        ] {
            assert_eq!(redis_url_with_password(url, None).unwrap().expose(), url);
            assert_eq!(
                redis_url_with_password(url, Some(&Secret::default()))
                    .unwrap()
                    .expose(),
                url,
                "an empty REDIS_PASSWORD counts as unset"
            );
        }
    }

    #[test]
    fn password_is_applied_for_the_default_user() {
        let url = redis_url_with_password(
            "redis://distant-signal-redis:6379",
            Some(&Secret::new("hunter2")),
        )
        .unwrap();
        assert_eq!(
            url.expose(),
            "redis://default:hunter2@distant-signal-redis:6379"
        );
        let info = info(&url);
        assert_eq!(info.redis.username.as_deref(), Some("default"));
        assert_eq!(info.redis.password.as_deref(), Some("hunter2"));
    }

    #[test]
    fn reserved_characters_round_trip_through_the_redis_crate() {
        let password = "p@ss:w/rd#1?x=y&z% é\"'";
        let url =
            redis_url_with_password("redis://cache.example:6380/2", Some(&Secret::new(password)))
                .unwrap();
        let info = info(&url);
        assert_eq!(info.redis.password.as_deref(), Some(password));
        assert_eq!(info.redis.username.as_deref(), Some("default"));
        assert_eq!(info.redis.db, 2);
        match info.addr {
            redis::ConnectionAddr::Tcp(host, port) => {
                assert_eq!((host.as_str(), port), ("cache.example", 6380));
            }
            other => panic!("unexpected addr {other:?}"),
        }
    }

    #[test]
    fn an_acl_username_in_the_url_is_kept() {
        let url =
            redis_url_with_password("redis://app@redis:6379", Some(&Secret::new("pw"))).unwrap();
        let info = info(&url);
        assert_eq!(info.redis.username.as_deref(), Some("app"));
        assert_eq!(info.redis.password.as_deref(), Some("pw"));
    }

    #[test]
    fn refuses_two_passwords_and_non_tcp_schemes_without_echoing_them() {
        let err = redis_url_with_password("redis://:inline@redis:6379", Some(&Secret::new("pw")))
            .unwrap_err()
            .to_string();
        assert!(err.contains("set only one"), "{err}");
        assert!(!err.contains("inline") && !err.contains("pw@"), "{err}");

        let err = redis_url_with_password("unix:///run/redis.sock", Some(&Secret::new("pw")))
            .unwrap_err()
            .to_string();
        assert!(err.contains("`unix` scheme"), "{err}");

        let err = redis_url_with_password("::secret-ish::", Some(&Secret::new("pw")))
            .unwrap_err()
            .to_string();
        assert!(!err.contains("secret-ish"), "{err}");
    }

    #[test]
    fn credentials_round_trip_through_the_redis_crate() {
        let url = redis_url_with_credentials(
            "redis://distant-signal-redis:6379",
            Some("movement-relay"),
            Some(&Secret::new("s3cr3t")),
        )
        .unwrap();
        assert_eq!(
            url.expose(),
            "redis://movement-relay:s3cr3t@distant-signal-redis:6379"
        );
        let info = info(&url);
        assert_eq!(info.redis.username.as_deref(), Some("movement-relay"));
        assert_eq!(info.redis.password.as_deref(), Some("s3cr3t"));
        match info.addr {
            redis::ConnectionAddr::Tcp(host, port) => {
                assert_eq!((host.as_str(), port), ("distant-signal-redis", 6379));
            }
            other => panic!("unexpected addr {other:?}"),
        }
    }

    #[test]
    fn reserved_characters_in_user_and_password_round_trip() {
        let user = "odd user@x:y";
        let password = "p@ss:w/rd#1?x=y&z% é";
        let url = redis_url_with_credentials(
            "redis://cache.example:6380/2",
            Some(user),
            Some(&Secret::new(password)),
        )
        .unwrap();
        let info = info(&url);
        assert_eq!(info.redis.username.as_deref(), Some(user));
        assert_eq!(info.redis.password.as_deref(), Some(password));
        assert_eq!(info.redis.db, 2);
    }

    #[test]
    fn an_absent_or_empty_username_is_the_password_only_path() {
        for user in [None, Some("")] {
            assert_eq!(
                redis_url_with_credentials("redis://redis:6379", user, None)
                    .unwrap()
                    .expose(),
                "redis://redis:6379",
                "no user, no password: untouched"
            );
            let url =
                redis_url_with_credentials("redis://redis:6379", user, Some(&Secret::new("pw")))
                    .unwrap();
            assert_eq!(url.expose(), "redis://default:pw@redis:6379");
        }
    }

    #[test]
    fn a_username_needs_a_password_and_a_credential_free_url() {
        for password in [None, Some(Secret::default())] {
            let err =
                redis_url_with_credentials("redis://redis:6379", Some("api"), password.as_ref())
                    .unwrap_err()
                    .to_string();
            assert!(err.contains("REDIS_PASSWORD is not"), "{err}");
        }
        for url in ["redis://someone@redis:6379", "redis://:inline@redis:6379"] {
            let err = redis_url_with_credentials(url, Some("api"), Some(&Secret::new("pw")))
                .unwrap_err()
                .to_string();
            assert!(err.contains("set only one"), "{err}");
            assert!(!err.contains("inline") && !err.contains("someone"), "{err}");
        }
        let err = redis_url_with_credentials(
            "unix:///run/redis.sock",
            Some("api"),
            Some(&Secret::new("pw")),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("`unix` scheme"), "{err}");
        let err = redis_url_with_credentials("::nope::", Some("api"), Some(&Secret::new("pw")))
            .unwrap_err()
            .to_string();
        assert!(!err.contains("nope"), "{err}");
    }

    #[test]
    fn debug_of_the_result_hides_the_password() {
        let url =
            redis_url_with_password("redis://redis:6379", Some(&Secret::new("hunter2"))).unwrap();
        assert!(!format!("{url:?}").contains("hunter2"));
    }
}
