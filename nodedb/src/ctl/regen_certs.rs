//! `nodedb regen-certs` — reissue the per-node certificate under the
//! existing CA.
//!
//! The CA cert + private key must be present at `data_dir/tls/ca.crt`
//! and `data_dir/tls/ca.key` respectively (the CA key is *not* kept
//! by `bootstrap_credentials` today — ephemeral CA — so this
//! subcommand errors out with a clear message when called on a node
//! that auto-bootstrapped without a persisted CA key).
//!
//! This is the "same-CA, new node cert" path used after a node-cert
//! compromise or expiry. For a *CA* rotation, operators use
//! `nodedb rotate-ca --stage` and `--finalize`.

use std::path::Path;

pub fn run(data_dir: &Path, node_id: u64) -> Result<(), String> {
    let tls_dir = data_dir.join("tls");
    let ca_cert_path = tls_dir.join("ca.crt");
    let ca_key_path = tls_dir.join("ca.key");

    if !ca_cert_path.exists() {
        return Err(format!(
            "CA cert not found at {}. Cannot reissue without the CA that signed the old cert. \
             If the cluster was bootstrapped with an ephemeral CA, you must rotate-ca --stage \
             + --finalize instead.",
            ca_cert_path.display()
        ));
    }
    if !ca_key_path.exists() {
        return Err(format!(
            "CA private key not found at {}. The current bootstrap path generates an ephemeral \
             CA whose private key is discarded after use; `regen-certs` requires a persisted \
             CA key. Use `rotate-ca` instead, which generates a brand-new CA and propagates it \
             through the cluster's overlap-window trust protocol.",
            ca_key_path.display()
        ));
    }

    // TODO(L.4 follow-up): call rcgen directly to load the CA keypair
    // from `ca.key`, then issue a new leaf for SAN=node-{node_id} +
    // SAN=nodedb. For the initial L.4 cut we surface the constraint
    // above so operators pick the right tool; the full reissue is a
    // follow-up once `generate_node_credentials` persists the CA key.
    let _ = node_id;
    Err(
        "regen-certs is stubbed pending CA-key persistence in generate_node_credentials. \
         Track: L.4 follow-up. Use rotate-ca for the current rotation flow."
            .into(),
    )
}
