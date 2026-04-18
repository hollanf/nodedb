//! Subcommand dispatch table.

use std::path::PathBuf;

use super::args::parse_flags;
use super::{join_token, regen_certs, rotate_ca};

/// All operator subcommands. Constructed by [`parse_subcommand`] from
/// the raw `std::env::args()` tail so `main()` can decide before it
/// touches any config or tracing state.
#[derive(Debug)]
pub enum Subcommand {
    /// Reissue the per-node cert under the existing CA. Restart required.
    RegenCerts { data_dir: PathBuf, node_id: u64 },
    /// Stage a new CA: generate + register for cluster-wide trust.
    RotateCaStage { data_dir: PathBuf },
    /// Finalize a rotation: remove the named CA fingerprint from trust.
    RotateCaFinalize { remove_fingerprint: String },
    /// Issue a short-lived HMAC-signed token for a joining node.
    JoinTokenCreate {
        data_dir: PathBuf,
        for_node: u64,
        ttl: std::time::Duration,
    },
}

/// Parse `args` (the tail of `std::env::args()` after the binary name)
/// into a subcommand. Returns `Ok(None)` when the first arg doesn't
/// look like a subcommand (i.e. it's a config-file path and the
/// caller should fall through to the server bootstrap path).
pub fn parse_subcommand(args: &[String]) -> Result<Option<Subcommand>, String> {
    let Some(first) = args.first() else {
        return Ok(None);
    };
    // Known subcommand names — anything else is assumed to be a config
    // file path so the `nodedb /etc/nodedb.toml` spelling keeps working.
    let name = first.as_str();
    if !matches!(
        name,
        "regen-certs" | "rotate-ca" | "join-token" | "help" | "--help" | "-h"
    ) {
        return Ok(None);
    }
    if matches!(name, "help" | "--help" | "-h") {
        print_usage();
        std::process::exit(0);
    }

    let tail = &args[1..];
    match name {
        "regen-certs" => {
            let flags = parse_flags(tail)?;
            let data_dir = PathBuf::from(super::args::required(&flags, "data-dir")?);
            let node_id: u64 = super::args::required(&flags, "node-id")?
                .parse()
                .map_err(|_| "--node-id must be an integer".to_string())?;
            Ok(Some(Subcommand::RegenCerts { data_dir, node_id }))
        }
        "rotate-ca" => {
            // Sub-mode: `--stage` or `--finalize --remove <fp>`.
            let flags = parse_flags(tail)?;
            if flags.contains_key("stage") {
                let data_dir = PathBuf::from(super::args::required(&flags, "data-dir")?);
                Ok(Some(Subcommand::RotateCaStage { data_dir }))
            } else if flags.contains_key("finalize") {
                let remove = super::args::required(&flags, "remove")?.clone();
                Ok(Some(Subcommand::RotateCaFinalize {
                    remove_fingerprint: remove,
                }))
            } else {
                Err("rotate-ca requires --stage or --finalize --remove <fingerprint>".into())
            }
        }
        "join-token" => {
            let flags = parse_flags(tail)?;
            if !flags.contains_key("create") {
                return Err("join-token requires --create".into());
            }
            let data_dir = PathBuf::from(super::args::required(&flags, "data-dir")?);
            let for_node: u64 = super::args::required(&flags, "for-node")?
                .parse()
                .map_err(|_| "--for-node must be an integer".to_string())?;
            let ttl =
                super::args::parse_duration(flags.get("ttl").map(|s| s.as_str()).unwrap_or("10m"))?;
            Ok(Some(Subcommand::JoinTokenCreate {
                data_dir,
                for_node,
                ttl,
            }))
        }
        _ => unreachable!("name checked above"),
    }
}

/// Execute a parsed subcommand. Returns process exit code.
pub fn run_subcommand(cmd: Subcommand) -> i32 {
    let result = match cmd {
        Subcommand::RegenCerts { data_dir, node_id } => regen_certs::run(&data_dir, node_id),
        Subcommand::RotateCaStage { data_dir } => rotate_ca::stage(&data_dir),
        Subcommand::RotateCaFinalize { remove_fingerprint } => {
            rotate_ca::finalize(&remove_fingerprint)
        }
        Subcommand::JoinTokenCreate {
            data_dir,
            for_node,
            ttl,
        } => join_token::create(&data_dir, for_node, ttl),
    };
    match result {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("error: {e}");
            1
        }
    }
}

fn print_usage() {
    println!(
        "nodedb — NodeDB server + operator tooling

USAGE:
    nodedb [CONFIG_FILE]                     Run the server (default mode)
    nodedb regen-certs --data-dir D --node-id N
                                             Reissue this node's cert under the existing CA
    nodedb rotate-ca --stage --data-dir D    Generate a new CA, write to ca.d/, emit staged bundle
    nodedb rotate-ca --finalize --remove FP  Ask the running node to remove CA with fingerprint FP
    nodedb join-token --create --data-dir D --for-node N [--ttl 10m]
                                             Emit a one-time HMAC token for a joining node
    nodedb help                              Print this message"
    );
}
