//! `nodedb join-token --create` — mint a short-lived HMAC-signed
//! token that a joining node can present to a bootstrap endpoint to
//! receive `(ca_cert, node_cert, node_key, cluster_secret)` without
//! out-of-band cred provisioning.
//!
//! Token format (opaque to callers, printed as hex):
//! ```text
//! [for_node: u64 LE | expiry_unix_secs: u64 LE | mac: 32 bytes]
//! ```
//! The MAC is HMAC-SHA256 over `for_node || expiry` keyed by the
//! cluster's persisted `cluster_secret`. Verification happens in the
//! bootstrap endpoint handler — see `L.4` follow-up issue for that
//! side of the protocol.

use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::Sha256;

const CLUSTER_SECRET_LEN: usize = 32;
const TOKEN_HEADER_LEN: usize = 8 + 8;
const TOKEN_MAC_LEN: usize = 32;

pub fn create(data_dir: &Path, for_node: u64, ttl: Duration) -> Result<(), String> {
    let secret_path = data_dir.join("tls").join("cluster_secret.bin");
    let secret = read_cluster_secret(&secret_path)?;

    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("system clock before UNIX epoch: {e}"))?
        + ttl;
    let expiry_secs = expiry.as_secs();

    let mut buf = Vec::with_capacity(TOKEN_HEADER_LEN + TOKEN_MAC_LEN);
    buf.extend_from_slice(&for_node.to_le_bytes());
    buf.extend_from_slice(&expiry_secs.to_le_bytes());

    let mut mac =
        <Hmac<Sha256>>::new_from_slice(&secret).map_err(|e| format!("hmac key length: {e}"))?;
    mac.update(&buf);
    let tag = mac.finalize().into_bytes();
    buf.extend_from_slice(&tag);

    let hex = hex_encode(&buf);
    println!("join token (expires at unix {expiry_secs}):");
    println!("{hex}");
    println!();
    println!("usage on joiner:");
    println!("  NODEDB_JOIN_TOKEN={hex} nodedb <config.toml>");
    println!("  (the bootstrap-listener RPC that consumes this token is a follow-up —");
    println!("   current joiners still need out-of-band cred provisioning. Token format");
    println!("   is frozen so the consumer side can be wired without reprint.)");
    Ok(())
}

fn read_cluster_secret(path: &Path) -> Result<[u8; CLUSTER_SECRET_LEN], String> {
    let bytes =
        fs::read(path).map_err(|e| format!("read cluster secret {}: {e}", path.display()))?;
    if bytes.len() != CLUSTER_SECRET_LEN {
        return Err(format!(
            "cluster secret {} has {} bytes, expected {CLUSTER_SECRET_LEN}",
            path.display(),
            bytes.len()
        ));
    }
    let mut out = [0u8; CLUSTER_SECRET_LEN];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Verify a hex-encoded token against the cluster secret. Returns the
/// bound `(for_node, expiry_secs)` on success, or an error string.
/// Consumed by the bootstrap-listener handler (L.4 follow-up) — kept
/// here so the format has exactly one source of truth.
pub fn verify_token(token_hex: &str, secret: &[u8; 32]) -> Result<(u64, u64), String> {
    if token_hex.len() != (TOKEN_HEADER_LEN + TOKEN_MAC_LEN) * 2 {
        return Err("token wrong length".into());
    }
    let mut bytes = Vec::with_capacity(TOKEN_HEADER_LEN + TOKEN_MAC_LEN);
    let src = token_hex.as_bytes();
    for chunk in src.chunks(2) {
        let hi = hex_digit(chunk[0])?;
        let lo = hex_digit(chunk[1])?;
        bytes.push((hi << 4) | lo);
    }
    let (body, tag) = bytes.split_at(TOKEN_HEADER_LEN);
    let mut mac =
        <Hmac<Sha256>>::new_from_slice(secret).map_err(|e| format!("hmac key length: {e}"))?;
    mac.update(body);
    mac.verify_slice(tag)
        .map_err(|_| "invalid token MAC".to_string())?;
    let for_node = u64::from_le_bytes(body[..8].try_into().unwrap());
    let expiry = u64::from_le_bytes(body[8..].try_into().unwrap());
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if now > expiry {
        return Err("token expired".into());
    }
    Ok((for_node, expiry))
}

fn hex_digit(b: u8) -> Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(10 + b - b'a'),
        b'A'..=b'F' => Ok(10 + b - b'A'),
        other => Err(format!("invalid hex char: {:?}", other as char)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn verify_accepts_fresh_token_rejects_tampered() {
        let secret = [0x11u8; 32];
        let for_node = 42u64;
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 60;

        let mut body = Vec::new();
        body.extend_from_slice(&for_node.to_le_bytes());
        body.extend_from_slice(&expiry.to_le_bytes());
        let mut mac = <Hmac<Sha256>>::new_from_slice(&secret).unwrap();
        mac.update(&body);
        let tag = mac.finalize().into_bytes();
        body.extend_from_slice(&tag);
        let hex = hex_encode(&body);

        let (got_node, got_exp) = verify_token(&hex, &secret).unwrap();
        assert_eq!(got_node, for_node);
        assert_eq!(got_exp, expiry);

        // Flip a byte in the MAC portion.
        let mut tampered = hex.clone();
        let flip = tampered.len() - 4;
        tampered.replace_range(flip..flip + 2, "00");
        assert!(verify_token(&tampered, &secret).is_err());

        // Wrong secret.
        let other_secret = [0x22u8; 32];
        assert!(verify_token(&hex, &other_secret).is_err());
    }

    #[test]
    fn verify_rejects_expired() {
        let secret = [0xAAu8; 32];
        let expiry = 1u64; // very old
        let mut body = Vec::new();
        body.extend_from_slice(&1u64.to_le_bytes());
        body.extend_from_slice(&expiry.to_le_bytes());
        let mut mac = <Hmac<Sha256>>::new_from_slice(&secret).unwrap();
        mac.update(&body);
        body.extend_from_slice(&mac.finalize().into_bytes());
        let hex = hex_encode(&body);
        let err = verify_token(&hex, &secret).unwrap_err();
        assert!(err.contains("expired"), "got: {err}");
    }

    #[test]
    fn create_writes_hex_token_for_real_secret() {
        use std::io::Write;
        let td = tempfile::tempdir().unwrap();
        let tls_dir = td.path().join("tls");
        std::fs::create_dir_all(&tls_dir).unwrap();
        let mut f = std::fs::File::create(tls_dir.join("cluster_secret.bin")).unwrap();
        f.write_all(&[0x5Au8; 32]).unwrap();
        drop(f);
        // Smoke test: succeeds and writes expected amount of output.
        create(td.path(), 9, Duration::from_secs(600)).unwrap();
    }
}
