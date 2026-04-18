//! `nodedb rotate-ca --stage` and `nodedb rotate-ca --finalize`.
//!
//! The two-phase rotation ceremony described in L.4:
//!
//! 1. `--stage` generates a fresh CA on disk. The new CA cert is
//!    written to `data_dir/tls/ca.d/<fp>.crt` so this node starts
//!    trusting it immediately on next reload. The operator then
//!    uses a cluster admin API (or this command against a running
//!    peer) to propose `MetadataEntry::CaTrustChange { add }` — we
//!    print the fingerprint + DER path so the operator can feed it
//!    to that API. (The direct in-binary proposal would require a
//!    running local node; keeping this command offline-capable
//!    means staging works even against a freshly-generated data
//!    dir before the server is up.)
//!
//! 2. `--finalize --remove <fp>` prints the `CaTrustChange { remove }`
//!    proposal body. Same story: the operator hands it to the
//!    cluster-admin API.
//!
//! The in-binary "propose directly" path is intentionally not
//! implemented here — these rotation events are rare, and forcing a
//! deliberate hand-off through an admin API matches how operators
//! already propagate other sensitive changes.

use std::path::Path;

use nodedb_cluster::{ca_fingerprint, ca_fingerprint_hex, generate_node_credentials};

pub fn stage(data_dir: &Path) -> Result<(), String> {
    let tls_dir = data_dir.join("tls");
    let ca_dir = tls_dir.join("ca.d");
    std::fs::create_dir_all(&ca_dir)
        .map_err(|e| format!("create ca.d dir {}: {e}", ca_dir.display()))?;

    // Generate a throwaway CA whose sole purpose is to mint a new
    // trust anchor. The generated node cert/key are *not* written
    // here — this command only stages a new anchor for the cluster.
    // Node cert reissue happens via regen-certs (same CA) or a
    // repeat bootstrap on joiners (new CA via join-token).
    let (ca, _creds) = generate_node_credentials(nodedb_cluster::transport::config::SNI_HOSTNAME)
        .map_err(|e| format!("generate new CA: {e}"))?;
    let ca_der = ca.cert_der();
    let fp = ca_fingerprint(&ca_der);
    let fp_hex = ca_fingerprint_hex(&fp);

    let path = ca_dir.join(format!("{fp_hex}.crt"));
    write_pem(&path, ca_der.as_ref())
        .map_err(|e| format!("write staged CA {}: {e}", path.display()))?;

    println!("staged new CA:");
    println!("  fingerprint (first 8 bytes, hex): {fp_hex}");
    println!("  path: {}", path.display());
    println!();
    println!("next steps:");
    println!(
        "  1. copy this file to every peer's {}/tls/ca.d/ (same fingerprint filename)",
        data_dir.display()
    );
    println!(
        "  2. call the cluster admin endpoint to propose CaTrustChange {{ add_ca_cert: <DER> }}"
    );
    println!("  3. reissue node certs signed by the new CA on each node");
    println!(
        "  4. run `nodedb rotate-ca --finalize --remove <old_fingerprint>` to retire the old CA"
    );

    Ok(())
}

pub fn finalize(remove_fingerprint: &str) -> Result<(), String> {
    // Decode the hex fingerprint — accept both the short 8-byte form
    // (16 hex chars) produced by `ca_fingerprint_hex` and the full
    // 32-byte form (64 hex chars) so operators copy-pasting either
    // variant succeed.
    let fp_bytes = parse_fingerprint(remove_fingerprint)?;
    println!("prepared finalize proposal:");
    println!(
        "  CaTrustChange {{ remove_ca_fingerprint: {} }}",
        ca_fingerprint_hex(&fp_bytes)
    );
    println!();
    println!(
        "next step: feed this fingerprint to the cluster admin endpoint to commit the removal."
    );
    println!(
        "  every node's applier deletes tls/ca.d/<fp>.crt on commit and rebuilds rustls trust."
    );
    Ok(())
}

fn parse_fingerprint(s: &str) -> Result<[u8; 32], String> {
    let trimmed = s.trim();
    let bytes = hex_decode(trimmed).map_err(|e| format!("fingerprint is not valid hex: {e}"))?;
    let mut out = [0u8; 32];
    match bytes.len() {
        8 => out[..8].copy_from_slice(&bytes),
        32 => out.copy_from_slice(&bytes),
        n => {
            return Err(format!(
                "fingerprint must be 8 bytes (16 hex chars) or 32 bytes (64 hex chars), got {n}"
            ));
        }
    }
    Ok(out)
}

fn hex_decode(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) {
        return Err("odd number of hex chars".into());
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    for chunk in bytes.chunks(2) {
        let hi = hex_digit(chunk[0])?;
        let lo = hex_digit(chunk[1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_digit(b: u8) -> Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(10 + b - b'a'),
        b'A'..=b'F' => Ok(10 + b - b'A'),
        other => Err(format!("invalid hex char: {:?}", other as char)),
    }
}

fn write_pem(path: &Path, der: &[u8]) -> std::io::Result<()> {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode(der);
    let mut pem = String::with_capacity(b64.len() + 64);
    pem.push_str("-----BEGIN CERTIFICATE-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        pem.push_str(std::str::from_utf8(chunk).expect("base64 is ascii"));
        pem.push('\n');
    }
    pem.push_str("-----END CERTIFICATE-----\n");
    std::fs::write(path, pem)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_fingerprint_accepts_short_and_full() {
        let short = "0123456789abcdef";
        let got_short = parse_fingerprint(short).unwrap();
        assert_eq!(
            &got_short[..8],
            &[0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef]
        );
        assert_eq!(&got_short[8..], &[0u8; 24]);

        let full = "0".repeat(64);
        let got_full = parse_fingerprint(&full).unwrap();
        assert_eq!(got_full, [0u8; 32]);
    }

    #[test]
    fn parse_fingerprint_rejects_wrong_length() {
        assert!(parse_fingerprint("deadbeef").is_err()); // 4 bytes
        assert!(parse_fingerprint("a".repeat(65).as_str()).is_err()); // odd
        assert!(parse_fingerprint("xyz").is_err()); // not hex
    }
}
