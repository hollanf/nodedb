//! IP/CIDR blacklist matching.
//!
//! Checks if a client IP address falls within any blacklisted CIDR range.
//! Used at connection accept time, before authentication.

use std::net::IpAddr;

/// A parsed CIDR range for IP blacklist matching.
#[derive(Debug, Clone)]
pub struct CidrRange {
    /// Network address.
    network: IpAddr,
    /// Prefix length (0-32 for IPv4, 0-128 for IPv6).
    prefix_len: u8,
    /// Pre-computed network mask.
    mask: u128,
}

impl CidrRange {
    /// Parse a CIDR string like `"10.0.0.0/8"` or `"192.168.1.100"` (single host).
    pub fn parse(s: &str) -> Option<Self> {
        if let Some((addr_str, prefix_str)) = s.split_once('/') {
            let addr: IpAddr = addr_str.parse().ok()?;
            let prefix_len: u8 = prefix_str.parse().ok()?;
            let max_prefix = if addr.is_ipv4() { 32 } else { 128 };
            if prefix_len > max_prefix {
                return None;
            }
            let total_bits: u32 = if addr.is_ipv4() { 32 } else { 128 };
            let mask = if prefix_len == 0 {
                0
            } else if prefix_len as u32 == total_bits {
                u128::MAX
            } else {
                u128::MAX << (total_bits - prefix_len as u32)
            };
            Some(Self {
                network: addr,
                prefix_len,
                mask,
            })
        } else {
            // Single IP — treat as /32 (IPv4) or /128 (IPv6).
            let addr: IpAddr = s.parse().ok()?;
            let prefix_len = if addr.is_ipv4() { 32 } else { 128 };
            Some(Self {
                network: addr,
                prefix_len,
                mask: u128::MAX,
            })
        }
    }

    /// The prefix length (e.g., 24 for /24).
    pub fn prefix_len(&self) -> u8 {
        self.prefix_len
    }

    /// Check if an IP address falls within this CIDR range.
    pub fn contains(&self, ip: &IpAddr) -> bool {
        let ip_bits = ip_to_u128(ip);
        let net_bits = ip_to_u128(&self.network);
        (ip_bits & self.mask) == (net_bits & self.mask)
    }
}

/// Check if an IP address matches any CIDR entry in a blacklist.
///
/// `entries` is a list of CIDR strings (e.g., `["10.0.0.0/8", "192.168.1.100"]`).
/// Returns the first matching CIDR string, or `None` if no match.
pub fn check_ip_against_cidrs<'a>(ip_str: &str, entries: &'a [String]) -> Option<&'a str> {
    let ip: IpAddr = ip_str.parse().ok()?;
    for entry in entries {
        if let Some(cidr) = CidrRange::parse(entry)
            && cidr.contains(&ip)
        {
            return Some(entry);
        }
    }
    None
}

/// Convert an IP address to a u128 for bitwise comparison.
///
/// IPv4 addresses are placed in the lower 32 bits (not IPv4-mapped IPv6)
/// so that IPv4 prefix masks (shifted by 32 - prefix_len) work correctly.
fn ip_to_u128(ip: &IpAddr) -> u128 {
    match ip {
        IpAddr::V4(v4) => u128::from(u32::from_be_bytes(v4.octets())),
        IpAddr::V6(v6) => u128::from_be_bytes(v6.octets()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_ip_match() {
        let cidr = CidrRange::parse("192.168.1.100").unwrap();
        assert!(cidr.contains(&"192.168.1.100".parse().unwrap()));
        assert!(!cidr.contains(&"192.168.1.101".parse().unwrap()));
    }

    #[test]
    fn cidr_range_match() {
        let cidr = CidrRange::parse("10.0.0.0/8").unwrap();
        assert!(cidr.contains(&"10.0.0.1".parse().unwrap()));
        assert!(cidr.contains(&"10.255.255.255".parse().unwrap()));
        assert!(!cidr.contains(&"11.0.0.1".parse().unwrap()));
    }

    #[test]
    fn cidr_24_match() {
        let cidr = CidrRange::parse("192.168.1.0/24").unwrap();
        assert!(cidr.contains(&"192.168.1.1".parse().unwrap()));
        assert!(cidr.contains(&"192.168.1.254".parse().unwrap()));
        assert!(!cidr.contains(&"192.168.2.1".parse().unwrap()));
    }

    #[test]
    fn check_against_list() {
        let entries = vec!["10.0.0.0/8".into(), "192.168.1.100".into()];
        assert_eq!(
            check_ip_against_cidrs("10.0.0.5", &entries),
            Some("10.0.0.0/8")
        );
        assert_eq!(
            check_ip_against_cidrs("192.168.1.100", &entries),
            Some("192.168.1.100")
        );
        assert_eq!(check_ip_against_cidrs("172.16.0.1", &entries), None);
    }

    #[test]
    fn invalid_cidr_returns_none() {
        assert!(CidrRange::parse("not-an-ip").is_none());
        assert!(CidrRange::parse("10.0.0.0/33").is_none());
    }
}
