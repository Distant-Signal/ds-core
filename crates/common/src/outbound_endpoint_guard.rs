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
/// URL over. An IPv6 address that carries an IPv4 address (the mapped
/// `::ffff:a.b.c.d` form, the deprecated compatible `::a.b.c.d` form, NAT64
/// `64:ff9b::/96`, 6to4 `2002::/16` and Teredo `2001::/32`) has that IPv4
/// address extracted and re-checked against the IPv4 rules below rather than
/// sailing through the IPv6 branch unexamined -- the same "attacker picks the
/// representation that evades the filter" concern [`validate_outbound_url`]'s
/// own doc comment raises about DNS rebinding, applied to address FORM
/// instead of resolution timing (COMMON-1 / SVC-03: `2002:0a00:0007::`
/// is 10.0.0.7 reached through a 6to4 relay).
pub fn is_disallowed_ip(addr: IpAddr) -> bool {
    match addr {
        IpAddr::V4(v4) => is_disallowed_ipv4(v4),
        IpAddr::V6(v6) => is_disallowed_ipv6(v6),
    }
}

fn is_disallowed_ipv6(v6: Ipv6Addr) -> bool {
    if let Some(mapped) = v6.to_ipv4_mapped() {
        return is_disallowed_ipv4(mapped);
    }
    if v6.is_loopback()
        || v6.is_unspecified()
        || v6.is_multicast()
        || is_unique_local_v6(v6)
        || is_link_local_v6(v6)
        || is_site_local_v6(v6)
        || is_documentation_v6(v6)
        || is_discard_only_v6(v6)
        || is_local_use_nat64_v6(v6)
    {
        return true;
    }
    embedded_ipv4s(v6)
        .into_iter()
        .flatten()
        .any(is_disallowed_ipv4)
}

/// The IPv4 addresses an IPv6 address hands traffic on to, for the
/// transition forms that embed one. Up to two (Teredo names a server and a
/// client).
fn embedded_ipv4s(v6: Ipv6Addr) -> [Option<Ipv4Addr>; 2] {
    let s = v6.segments();
    let v4 = |hi: u16, lo: u16| Ipv4Addr::from((u32::from(hi) << 16) | u32::from(lo));
    match s {
        // ::a.b.c.d (RFC4291 "IPv4-compatible", deprecated). `::` and `::1`
        // are caught above as unspecified/loopback before reaching here.
        [0, 0, 0, 0, 0, 0, hi, lo] => [Some(v4(hi, lo)), None],
        // 64:ff9b::/96 -- NAT64 well-known prefix (RFC6052): the last 32 bits.
        [0x0064, 0xff9b, 0, 0, 0, 0, hi, lo] => [Some(v4(hi, lo)), None],
        // ::ffff:0:a.b.c.d -- SIIT "IPv4-translated" (RFC2765, ::ffff:0:0/96).
        [0, 0, 0, 0, 0xffff, 0, hi, lo] => [Some(v4(hi, lo)), None],
        // 2002::/16 -- 6to4 (RFC3056): bits 16..48.
        [0x2002, hi, lo, ..] => [Some(v4(hi, lo)), None],
        // 2001::/32 -- Teredo (RFC4380): the server in bits 32..64, the
        // client in the last 32 bits, bitwise inverted.
        [0x2001, 0, server_hi, server_lo, _, _, client_hi, client_lo] => [
            Some(v4(server_hi, server_lo)),
            Some(v4(!client_hi, !client_lo)),
        ],
        _ => [None, None],
    }
}

fn is_disallowed_ipv4(v4: Ipv4Addr) -> bool {
    let [a, b, c, _] = v4.octets();
    v4.is_private() // RFC1918: 10/8, 172.16/12, 192.168/16
        || v4.is_loopback() // 127/8
        || v4.is_link_local() // 169.254/16
        || v4.is_multicast()
        || v4.is_documentation() // 192.0.2/24, 198.51.100/24, 203.0.113/24
        || is_carrier_grade_nat_v4(v4) // 100.64/10 (RFC6598)
        || a == 0 // 0.0.0.0/8 "this network" (RFC1122), not just 0.0.0.0
        || (a == 192 && b == 0 && c == 0) // 192.0.0.0/24 IETF protocol assignments (RFC6890)
        || (a == 198 && (b & 0xfe) == 18) // 198.18.0.0/15 benchmarking (RFC2544)
        || a >= 240 // 240.0.0.0/4 reserved, including 255.255.255.255 broadcast
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

/// fec0::/10 -- IPv6 site-local (RFC3879 deprecated it; never public).
fn is_site_local_v6(v6: Ipv6Addr) -> bool {
    (v6.segments()[0] & 0xffc0) == 0xfec0
}

/// 2001:db8::/32 -- IPv6 documentation (RFC3849).
fn is_documentation_v6(v6: Ipv6Addr) -> bool {
    let s = v6.segments();
    s[0] == 0x2001 && s[1] == 0x0db8
}

/// 100::/64 -- IPv6 discard-only (RFC6666).
fn is_discard_only_v6(v6: Ipv6Addr) -> bool {
    let s = v6.segments();
    s[0] == 0x0100 && s[1] == 0 && s[2] == 0 && s[3] == 0
}

/// 64:ff9b:1::/48 -- NAT64 local-use prefix (RFC8215). Operator-defined
/// translation inside one network, so never a legitimate public target.
fn is_local_use_nat64_v6(v6: Ipv6Addr) -> bool {
    let s = v6.segments();
    s[0] == 0x0064 && s[1] == 0xff9b && s[2] == 0x0001
}

/// fe80::/10 -- IPv6 link-local (RFC4291).
fn is_link_local_v6(v6: Ipv6Addr) -> bool {
    (v6.segments()[0] & 0xffc0) == 0xfe80
}

/// A `reqwest` DNS resolver that refuses to hand the connector any
/// address [`is_disallowed_ip`] rejects -- the connect-time half of this
/// module's SSRF guard.
///
/// **Why [`validate_outbound_url`] alone still wasn't enough (2026-09-27,
/// L9 follow-up).** Validating and then sending resolves the name TWICE:
/// once in [`validate_outbound_url`], and again inside the HTTP client when
/// it connects. A rebinding name with a 0-second TTL can answer with a
/// public address the first time and a private one the second, which is
/// the classic DNS-rebinding shape, so a validate-then-send sequence never
/// actually constrains where the connection goes. Doing the check inside the
/// resolver the connector itself uses means the addresses that get checked
/// are exactly the addresses that get dialled. IP-literal URLs never reach a
/// resolver at all, which is why callers still run [`validate_outbound_url`]
/// first (it checks literals directly).
///
/// Any disallowed address in the answer rejects the whole name, same as
/// [`validate_outbound_url`] -- filtering out just the bad ones would let
/// a mixed answer through.
#[derive(Debug, Default, Clone, Copy)]
pub struct PublicOnlyResolver;

impl reqwest::dns::Resolve for PublicOnlyResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_owned();
        Box::pin(async move {
            let addrs: Vec<std::net::SocketAddr> =
                tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            if addrs.is_empty() {
                return Err(format!("{host} does not resolve").into());
            }
            if addrs.iter().any(|addr| is_disallowed_ip(addr.ip())) {
                return Err(format!(
                    "{host} resolves to a disallowed (private/internal/loopback) address"
                )
                .into());
            }
            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// A `reqwest::ClientBuilder` for outbound requests to caller-supplied
/// URLs. What it guarantees at connect time:
///
/// - every host NAME it dials was resolved through [`PublicOnlyResolver`],
///   so the addresses checked are exactly the addresses connected to;
/// - no redirect following (a redirect is a second, unvalidated URL);
/// - no proxy, including one picked up from `HTTPS_PROXY`/`ALL_PROXY` in the
///   environment (COMMON-1 / SVC-03). Through a proxy the resolver only ever
///   sees the proxy's own host, and the proxy dials the caller's target
///   unchecked, so the resolver guard would silently stop applying.
///
/// IP-literal URLs bypass any resolver, so callers must still run
/// [`validate_outbound_url`] (or [`is_disallowed_ip`] on the literal) before
/// sending. A literal can't be rebound, so that check has no
/// time-of-check/time-of-use gap.
pub fn public_only_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .dns_resolver(PublicOnlyResolver)
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
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

    fn v4(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(a, b, c, d))
    }

    fn v6(s: &str) -> IpAddr {
        IpAddr::V6(s.parse().expect("valid IPv6 literal"))
    }

    /// COMMON-1 / SVC-03: the reserved IPv4 ranges the first version missed.
    #[test]
    fn reserved_ipv4_ranges_are_disallowed() {
        for ip in [
            v4(0, 0, 0, 0),
            v4(0, 1, 2, 3), // 0.0.0.0/8, not just 0.0.0.0
            v4(0, 255, 255, 255),
            v4(192, 0, 0, 1), // 192.0.0.0/24
            v4(192, 0, 0, 255),
            v4(198, 18, 0, 1), // 198.18.0.0/15
            v4(198, 19, 255, 254),
            v4(240, 0, 0, 1), // 240.0.0.0/4
            v4(250, 1, 2, 3),
            v4(255, 255, 255, 255),
        ] {
            assert!(is_disallowed_ip(ip), "{ip} should be disallowed");
        }
    }

    /// The neighbours of each new range stay allowed, so the masks are the
    /// right width.
    #[test]
    fn public_neighbours_of_the_reserved_ipv4_ranges_are_allowed() {
        for ip in [
            v4(1, 0, 0, 1),
            v4(192, 0, 1, 1),    // just past 192.0.0.0/24
            v4(198, 17, 255, 1), // just before 198.18.0.0/15
            v4(198, 20, 0, 1),   // just past it
            v4(223, 255, 255, 1),
        ] {
            assert!(!is_disallowed_ip(ip), "{ip} should be allowed");
        }
    }

    /// NAT64, 6to4, Teredo and IPv4-compatible addresses are judged by the
    /// IPv4 address they carry.
    #[test]
    fn ipv6_transition_forms_are_checked_against_their_embedded_ipv4() {
        for ip in [
            v6("64:ff9b::a00:7"),               // NAT64 -> 10.0.0.7
            v6("64:ff9b::7f00:1"),              // NAT64 -> 127.0.0.1
            v6("64:ff9b::a9fe:a9fe"),           // NAT64 -> 169.254.169.254
            v6("2002:a00:7::"),                 // 6to4 -> 10.0.0.7
            v6("2002:7f00:1:1::1"),             // 6to4 -> 127.0.0.1
            v6("2002:c0a8:101::1"),             // 6to4 -> 192.168.1.1
            v6("2001:0:a00:7::1"),              // Teredo server 10.0.0.7
            v6("2001:0:808:808:0:0:f5ff:fff8"), // Teredo client !f5ff:fff8 = 10.0.0.7
            v6("::a00:7"),                      // IPv4-compatible 10.0.0.7
        ] {
            assert!(is_disallowed_ip(ip), "{ip} should be disallowed");
        }
        for ip in [
            v6("64:ff9b::808:808"), // NAT64 -> 8.8.8.8
            v6("2002:808:808::1"),  // 6to4 -> 8.8.8.8
            // Teredo, server 8.8.8.8 and client !f7f7:f7f7 = 8.8.8.8
            v6("2001:0:808:808:0:0:f7f7:f7f7"),
        ] {
            assert!(!is_disallowed_ip(ip), "{ip} should be allowed");
        }
    }

    #[test]
    fn the_ipv4_translated_form_is_checked_against_its_embedded_ipv4() {
        assert!(is_disallowed_ip(v6("::ffff:0:a00:7"))); // 10.0.0.7
        assert!(is_disallowed_ip(v6("::ffff:0:7f00:1"))); // 127.0.0.1
        assert!(is_disallowed_ip(v6("::ffff:0:a9fe:a9fe"))); // 169.254.169.254
        assert!(!is_disallowed_ip(v6("::ffff:0:808:808"))); // 8.8.8.8
    }

    #[test]
    fn other_non_public_ipv6_ranges_are_disallowed() {
        for ip in [
            v6("2001:db8::1"),  // documentation
            v6("100::1"),       // discard-only
            v6("64:ff9b:1::a"), // NAT64 local-use
            v6("fec0::1"),      // site-local
            v6("ff02::1"),      // multicast
            v6("::"),
        ] {
            assert!(is_disallowed_ip(ip), "{ip} should be disallowed");
        }
        assert!(!is_disallowed_ip(v6("2606:4700:4700::1111")));
        assert!(!is_disallowed_ip(v6("2001:4860:4860::8888")));
    }

    #[tokio::test]
    async fn a_6to4_literal_embedding_a_private_ipv4_is_rejected() {
        let err = validate_outbound_url("https://[2002:a00:7::1]/push")
            .await
            .expect_err("6to4 wrapping 10.0.0.7 must be rejected");
        assert!(err.contains("disallowed"));
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

    /// L9 follow-up: the connector's own resolver refuses a name that
    /// resolves to loopback, so a send can never reach one no matter what
    /// an earlier validation saw.
    #[tokio::test]
    async fn the_public_only_resolver_refuses_a_name_resolving_to_loopback() {
        use reqwest::dns::Resolve;
        let name: reqwest::dns::Name = "localhost".parse().expect("valid name");
        let err = match PublicOnlyResolver.resolve(name).await {
            Ok(_) => panic!("localhost must not resolve through the public-only resolver"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("disallowed"), "{err}");
    }

    /// End to end through a real client: the request fails at resolution,
    /// before any connection is attempted.
    #[tokio::test]
    async fn a_public_only_client_never_connects_to_a_name_resolving_to_loopback() {
        let client = public_only_client_builder().build().expect("client");
        let err = client
            .post("http://localhost:9/push")
            .send()
            .await
            .expect_err("must not connect");
        let chain = {
            let mut out = String::new();
            let mut source: Option<&dyn std::error::Error> = Some(&err);
            while let Some(e) = source {
                out.push_str(&e.to_string());
                out.push('\n');
                source = e.source();
            }
            out
        };
        assert!(chain.contains("disallowed"), "{chain}");
    }

    /// L9: the public-only client never follows a redirect -- a `Location`
    /// is a second URL nothing validated. A local server 302s to a second
    /// local server; the client must hand back the 302 itself and the
    /// redirect target must never see a connection. (IP literals skip the
    /// resolver, which is what lets this test reach 127.0.0.1 at all.)
    #[tokio::test]
    async fn a_public_only_client_does_not_follow_a_redirect() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let target = TcpListener::bind("127.0.0.1:0").await.expect("bind target");
        let target_addr = target.local_addr().expect("target addr");
        let target_hits = Arc::new(AtomicUsize::new(0));
        let hits = Arc::clone(&target_hits);
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = target.accept().await {
                hits.fetch_add(1, Ordering::SeqCst);
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                let _ = stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                    .await;
            }
        });

        let redirector = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind redirector");
        let redirector_addr = redirector.local_addr().expect("redirector addr");
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = redirector.accept().await {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                let response = format!(
                    "HTTP/1.1 302 Found\r\nlocation: http://{target_addr}/internal\r\n\
                     content-length: 0\r\nconnection: close\r\n\r\n"
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });

        let client = public_only_client_builder().build().expect("client");
        let response = client
            .post(format!("http://{redirector_addr}/push"))
            .send()
            .await
            .expect("the redirect response itself is returned");
        assert_eq!(response.status(), reqwest::StatusCode::FOUND);
        assert_eq!(
            target_hits.load(Ordering::SeqCst),
            0,
            "the redirect target must never be contacted"
        );
    }
}
