//! JWKS URL validation: SSRF defense for the JWKS fetch path.
//!
//! Two layers, both applied on the initial URL and every redirect hop:
//!
//! 1. **Syntactic** (`validate_jwks_url`): scheme must be `https` and the
//!    host must be a DNS name — never an IP literal. This is the cheap
//!    pre-resolve gate that stops classic `http://169.254.169.254/...`
//!    and `https://127.0.0.1/` shapes dead.
//! 2. **Resolved-address** (`validate_resolved_addrs`): after DNS, every
//!    returned address (AAAA + A both) must be a global unicast. Any
//!    loopback, private, link-local, unique-local, multicast,
//!    unspecified, or reserved address fails the whole resolution.
//!
//! The pair defends cloud-metadata endpoints (IMDS, GCP/Azure metadata),
//! RFC1918 internal services, loopback admin surfaces, and split-horizon
//! DNS tricks where a public-looking name resolves to a private IP.
//!
//! DNS rebinding between our resolution and reqwest's own resolution is
//! mitigated by pinning the resolved address into the request (see
//! `fetch` module's custom resolver).
//!
//! Error messages intentionally do NOT echo the offending URL back —
//! external input flowing into log lines is its own exfil surface.

use std::net::IpAddr;

use reqwest::Url;

/// URL validation errors for JWKS endpoints.
#[derive(Debug, thiserror::Error)]
pub enum UrlValidationError {
    #[error("JWKS URL is malformed")]
    Malformed,
    #[error("JWKS URL must use https scheme")]
    NonHttps,
    #[error("JWKS URL has no host component")]
    NoHost,
    #[error("JWKS URL must use a DNS name, not an IP literal")]
    IpLiteral,
    #[error("JWKS URL resolves to a non-global address (loopback/private/link-local/reserved)")]
    PrivateAddress,
}

/// Validate a JWKS URL string syntactically (pre-resolve).
pub fn validate_jwks_url(url: &str) -> Result<(), UrlValidationError> {
    let parsed = Url::parse(url).map_err(|_| UrlValidationError::Malformed)?;
    validate_parsed_url(&parsed)
}

/// Same as [`validate_jwks_url`] but on an already-parsed `Url`. Used by
/// the redirect-policy hook.
pub fn validate_parsed_url(parsed: &Url) -> Result<(), UrlValidationError> {
    if parsed.scheme() != "https" {
        return Err(UrlValidationError::NonHttps);
    }
    // `domain()` returns Some iff the host is a DNS name (not IP literal).
    match parsed.domain() {
        Some(d) if !d.is_empty() => Ok(()),
        Some(_) => Err(UrlValidationError::NoHost),
        None if parsed.host_str().is_some() => Err(UrlValidationError::IpLiteral),
        None => Err(UrlValidationError::NoHost),
    }
}

/// Validate every resolved IP address for a JWKS hostname.
///
/// ALL addresses must pass — if ANY is private/loopback/etc., the whole
/// resolution fails. This prevents AAAA/A split-horizon attacks where
/// the attacker publishes a public A record and a link-local AAAA.
pub fn validate_resolved_addrs(addrs: &[IpAddr]) -> Result<(), UrlValidationError> {
    if addrs.is_empty() {
        return Err(UrlValidationError::PrivateAddress);
    }
    for ip in addrs {
        if !is_global_unicast(ip) {
            return Err(UrlValidationError::PrivateAddress);
        }
    }
    Ok(())
}

/// Is this IP a routable, global unicast address?
fn is_global_unicast(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            !(v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_multicast()
                || v4.is_unspecified()
                || v4.is_documentation()
                // 0.0.0.0/8 "this network"
                || v4.octets()[0] == 0
                // 100.64.0.0/10 CGNAT
                || (v4.octets()[0] == 100 && (v4.octets()[1] & 0xC0) == 64)
                // 169.254/16 link-local (covered by is_link_local but belt+braces)
                || (v4.octets()[0] == 169 && v4.octets()[1] == 254)
                // 192.0.0.0/24 IETF reserved
                || (v4.octets()[0] == 192 && v4.octets()[1] == 0 && v4.octets()[2] == 0)
                // 198.18/15 benchmarking
                || (v4.octets()[0] == 198 && (v4.octets()[1] & 0xFE) == 18)
                // 240/4 reserved
                || v4.octets()[0] >= 240)
        }
        IpAddr::V6(v6) => {
            let seg = v6.segments();
            !(v6.is_loopback()
                || v6.is_multicast()
                || v6.is_unspecified()
                // fe80::/10 link-local
                || (seg[0] & 0xffc0) == 0xfe80
                // fc00::/7 unique-local
                || (seg[0] & 0xfe00) == 0xfc00
                // ::ffff:0:0/96 IPv4-mapped — re-check as IPv4
                || v6.to_ipv4_mapped().is_some_and(|v4| !is_global_unicast(&IpAddr::V4(v4)))
                // 2001:db8::/32 documentation
                || (seg[0] == 0x2001 && seg[1] == 0x0db8))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn https_public_dns_passes() {
        validate_jwks_url("https://auth.example.com/.well-known/jwks.json").unwrap();
    }

    #[test]
    fn http_scheme_rejected() {
        assert!(matches!(
            validate_jwks_url("http://example.com/"),
            Err(UrlValidationError::NonHttps)
        ));
    }

    #[test]
    fn ip_literal_rejected() {
        for u in [
            "https://127.0.0.1/",
            "https://169.254.169.254/",
            "https://10.0.0.1/",
            "https://[::1]/",
            "https://[fe80::1]/",
        ] {
            assert!(matches!(
                validate_jwks_url(u),
                Err(UrlValidationError::IpLiteral)
            ));
        }
    }

    #[test]
    fn private_addrs_fail() {
        for ip in [
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1)),
        ] {
            assert!(validate_resolved_addrs(std::slice::from_ref(&ip)).is_err());
        }
    }

    #[test]
    fn public_addr_passes() {
        validate_resolved_addrs(&[IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34))]).unwrap();
    }

    #[test]
    fn mixed_addrs_fail_if_any_private() {
        let addrs = [
            IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
        ];
        assert!(validate_resolved_addrs(&addrs).is_err());
    }
}
