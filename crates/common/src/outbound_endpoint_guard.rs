//! Shared SSRF guard for any URL this codebase's own server-side code is
//! about to make an outbound request to on a caller's behalf -- originally
//! written for `crates/api/src/data/notifications.rs`'s Web Push
//! `endpoint` (see [`validate_outbound_url`]'s own doc comment for the
//! full DNS-rebinding rationale), and moved here so `crates/notifier`
//! (which has no dependency on `crates/api` and never will -- they're
//! separate deployables) can run the SAME check again immediately before
//! it actually issues that outbound POST, not just once at registration
//! time.
//!
//! **Why registration-time validation alone isn't enough.** A hostile (or
//! merely rebound) DNS name can resolve to a public IP the moment a user
//! registers a push subscription -- passing `crates/api`'s check -- and
//! later resolve to an internal/private/loopback address by the time
//! `crates/notifier` actually sends to it, potentially hours or days
//! afterward. Re-running the exact same resolve-and-check here, right
//! before the real send, closes that window: a blind POST (VAPID headers,
//! encrypted body) can never reach an internal host from inside the
//! notifier pod, no matter when the attacker flips their DNS record.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Rejects any `url` a malicious (or merely careless) caller could use to
/// make a server-side outbound POST land somewhere inside the cluster's
/// own network instead of at a real external service -- an SSRF vector.
///
/// Two checks, both required:
///   1. Scheme must be `https` -- every legitimate caller of this function
///      (Web Push `endpoint`s today) only ever issues `https`, and
///      requiring TLS also means the host below is at least nominally
///      reachable as a real internet service.
///   2. The host must resolve -- via a REAL DNS lookup, not a string
///      pattern match on the hostname -- to only public IP addresses.
///      Resolving is deliberate, not merely parsing an IP literal out of
///      the URL: a hostname like `attacker.example` can resolve to
///      `10.0.0.7` just as easily as an IP-literal URL can name it
///      directly, and a string check on the HOSTNAME alone (e.g. "does it
///      look like `10.x.x.x`") would miss that entirely -- exactly the
///      DNS-rebinding gap this function is written to close. Every IPv4
///      and IPv6 private/loopback/link-local/multicast (and a few other
///      non-public) range is rejected; see [`is_disallowed_ip`].
///
/// Deliberately NOT also restricted to a small allowlist of known hosts --
/// see `crates/api/src/data/notifications.rs`'s own historical doc comment
/// for why (browsers that route push through microG/UnifiedPush
/// distributors or self-hosted relays legitimately use arbitrary
/// operator-chosen hosts). The scheme + real-DNS-resolved-private-range
/// check above is judged sufficient on its own.
pub async fn validate_outbound_url(url: &str) -> Result<(), String> {
    let parsed = url::Url::parse(url).map_err(|_| "endpoint must be a valid URL".to_string())?;

    if parsed.scheme() != "https" {
        return Err("endpoint must use https".to_string());
    }

    let host = parsed
        .host()
        .ok_or_else(|| "endpoint must have a host".to_string())?
        .to_owned();
    let port = parsed.port_or_known_default().unwrap_or(443);

    // `url::Host` is matched on directly (an already-typed IPv4/IPv6
    // literal needs no resolution at all -- and skipping `lookup_host` for
    // those avoids any ambiguity around whether the OS resolver treats an
    // IPv6 literal's `[...]` bracket syntax as part of the hostname). A
    // domain name gets a REAL async DNS resolution, not a hostname string
    // match -- see this function's own doc comment on why that distinction
    // matters (DNS rebinding).
    let addrs: Vec<IpAddr> = match host {
        url::Host::Ipv4(v4) => vec![IpAddr::V4(v4)],
        url::Host::Ipv6(v6) => vec![IpAddr::V6(v6)],
        url::Host::Domain(domain) => tokio::net::lookup_host((domain.as_str(), port))
            .await
            .map_err(|_| "endpoint host does not resolve".to_string())?
            .map(|socket_addr| socket_addr.ip())
            .collect(),
    };
    if addrs.is_empty() {
        return Err("endpoint host does not resolve".to_string());
    }

    if addrs.iter().any(|addr| is_disallowed_ip(*addr)) {
        return Err("endpoint host resolves to a disallowed address".to_string());
    }
    Ok(())
}

/// Every IPv4 and IPv6 non-public range worth rejecting an SSRF-candidate
/// URL over. An IPv6 address that is really an IPv4-mapped address
/// (`::ffff:a.b.c.d`) is unwrapped and re-checked against the IPv4 rules
/// below rather than sailing through the IPv6 branch unexamined -- the
/// same "attacker picks the representation that evades the filter" concern
/// [`validate_outbound_url`]'s own doc comment raises about DNS rebinding,
/// applied to address FORM instead of resolution timing.
pub fn is_disallowed_ip(addr: IpAddr) -> bool {
    match addr {
        IpAddr::V4(v4) => is_disallowed_ipv4(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(mapped) => is_disallowed_ipv4(mapped),
            None => {
                v6.is_loopback()
                    || v6.is_unspecified()
                    || v6.is_multicast()
                    || is_unique_local_v6(v6)
                    || is_link_local_v6(v6)
            }
        },
    }
}

fn is_disallowed_ipv4(v4: Ipv4Addr) -> bool {
    v4.is_private() // RFC1918: 10/8, 172.16/12, 192.168/16
        || v4.is_loopback() // 127/8
        || v4.is_link_local() // 169.254/16
        || v4.is_multicast()
        || v4.is_broadcast()
        || v4.is_unspecified() // 0.0.0.0
        || v4.is_documentation() // 192.0.2/24, 198.51.100/24, 203.0.113/24
        || is_carrier_grade_nat_v4(v4) // 100.64/10 (RFC6598)
}

/// `std::net::Ipv4Addr` has no stable `is_shared` yet -- this is that
/// range's own check, kept as a tiny standalone function rather than an
/// inline expression so its RFC citation has somewhere to live.
fn is_carrier_grade_nat_v4(v4: Ipv4Addr) -> bool {
    let [a, b, ..] = v4.octets();
    a == 100 && (64..=127).contains(&b) // 100.64.0.0/10
}

/// fc00::/7 -- IPv6 Unique Local Addresses (RFC4193), the IPv6 rough
/// equivalent of RFC1918.
fn is_unique_local_v6(v6: Ipv6Addr) -> bool {
    (v6.segments()[0] & 0xfe00) == 0xfc00
}

/// fe80::/10 -- IPv6 link-local (RFC4291).
fn is_link_local_v6(v6: Ipv6Addr) -> bool {
    (v6.segments()[0] & 0xffc0) == 0xfe80
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_ipv4_is_allowed() {
        assert!(!is_disallowed_ip(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
    }

    #[test]
    fn rfc1918_ipv4_is_disallowed() {
        assert!(is_disallowed_ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 7))));
        assert!(is_disallowed_ip(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))));
        assert!(is_disallowed_ip(IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1))));
    }

    #[test]
    fn loopback_is_disallowed() {
        assert!(is_disallowed_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))));
        assert!(is_disallowed_ip(IpAddr::V6(Ipv6Addr::LOCALHOST)));
    }

    #[test]
    fn carrier_grade_nat_v4_is_disallowed() {
        assert!(is_disallowed_ip(IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1))));
        assert!(!is_disallowed_ip(IpAddr::V4(Ipv4Addr::new(100, 128, 0, 1))));
    }

    #[test]
    fn ipv4_mapped_ipv6_is_checked_against_ipv4_rules() {
        // ::ffff:10.0.0.7 -- a private IPv4 address wearing an IPv6 mapped
        // representation. Must still be caught, not sail through the IPv6
        // branch unexamined.
        let mapped = Ipv4Addr::new(10, 0, 0, 7).to_ipv6_mapped();
        assert!(is_disallowed_ip(IpAddr::V6(mapped)));
    }

    #[test]
    fn unique_local_v6_is_disallowed() {
        assert!(is_disallowed_ip(IpAddr::V6(Ipv6Addr::new(
            0xfd00, 0, 0, 0, 0, 0, 0, 1
        ))));
    }

    #[test]
    fn link_local_v6_is_disallowed() {
        assert!(is_disallowed_ip(IpAddr::V6(Ipv6Addr::new(
            0xfe80, 0, 0, 0, 0, 0, 0, 1
        ))));
    }

    #[tokio::test]
    async fn a_non_https_scheme_is_rejected() {
        let err = validate_outbound_url("http://example.com/push")
            .await
            .expect_err("http must be rejected");
        assert!(err.contains("https"));
    }

    #[tokio::test]
    async fn an_ip_literal_in_a_private_range_is_rejected_with_no_dns_lookup_needed() {
        let err = validate_outbound_url("https://10.0.0.7/push")
            .await
            .expect_err("a private IPv4 literal must be rejected");
        assert!(err.contains("disallowed"));
    }
}
