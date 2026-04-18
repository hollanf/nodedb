//! Minimal flag parser shared by all subcommands.
//!
//! The operator CLI only needs a handful of long-form flags, each with
//! a single value: `--data-dir /var/nodedb`, `--node-id 3`, `--ttl
//! 10m`, `--remove <fp>`, `--for-node 5`. Pulling in clap would be
//! heavy for this surface; a ~30-line parser is enough and keeps the
//! binary's compile-time cost unchanged.

use std::collections::HashMap;

/// Parse a list of `--key value` arguments into a map. Each flag must
/// start with `--` and every flag consumes exactly the next argument
/// as its value. Repeated flags overwrite. Returns an error message
/// for unrecognised tokens (positional args, single-dash shortflags)
/// so operators get a clear message instead of silent ignore.
pub fn parse_flags(args: &[String]) -> Result<HashMap<String, String>, String> {
    let mut map = HashMap::new();
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if let Some(name) = a.strip_prefix("--") {
            let value = args
                .get(i + 1)
                .cloned()
                .ok_or_else(|| format!("flag --{name} requires a value"))?;
            map.insert(name.to_string(), value);
            i += 2;
        } else {
            return Err(format!("unexpected argument: {a}"));
        }
    }
    Ok(map)
}

/// Require a flag and return its value, or produce a uniform error.
pub fn required<'a>(flags: &'a HashMap<String, String>, name: &str) -> Result<&'a String, String> {
    flags
        .get(name)
        .ok_or_else(|| format!("missing required flag --{name}"))
}

/// Parse a human-friendly duration string (`10s`, `5m`, `2h`). No
/// fractional units. Used by `--ttl`.
pub fn parse_duration(s: &str) -> Result<std::time::Duration, String> {
    let (num_str, unit) = s.split_at(
        s.find(|c: char| !c.is_ascii_digit())
            .ok_or_else(|| format!("duration needs a unit suffix (s/m/h): {s}"))?,
    );
    let n: u64 = num_str
        .parse()
        .map_err(|_| format!("bad duration number: {s}"))?;
    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        other => return Err(format!("unknown duration unit {other:?} (expected s/m/h)")),
    };
    Ok(std::time::Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_kv_flags() {
        let args = vec![
            "--data-dir".into(),
            "/var/nodedb".into(),
            "--node-id".into(),
            "3".into(),
        ];
        let m = parse_flags(&args).unwrap();
        assert_eq!(m.get("data-dir").unwrap(), "/var/nodedb");
        assert_eq!(m.get("node-id").unwrap(), "3");
    }

    #[test]
    fn rejects_lone_flag() {
        assert!(parse_flags(&["--data-dir".into()]).is_err());
    }

    #[test]
    fn rejects_positional() {
        assert!(parse_flags(&["positional".into()]).is_err());
    }

    #[test]
    fn duration_units() {
        use std::time::Duration;
        assert_eq!(parse_duration("10s").unwrap(), Duration::from_secs(10));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert!(parse_duration("10x").is_err());
        assert!(parse_duration("10").is_err());
    }
}
