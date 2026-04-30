//! Parse CREATE/DROP/ALTER/DESCRIBE/SHOW SEQUENCE.

use super::helpers::extract_name_after_if_exists;
use crate::ddl_ast::statement::NodedbStatement;
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    _trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    (|| -> Result<Option<NodedbStatement>, SqlError> {
        if upper.starts_with("CREATE SEQUENCE ") {
            let if_not_exists = upper.contains("IF NOT EXISTS");
            let name = match extract_name_after_if_exists(parts, "SEQUENCE") {
                None => return Ok(None),
                Some(r) => r?,
            };
            let options = parse_sequence_options(parts, &name);
            return Ok(Some(NodedbStatement::CreateSequence {
                name,
                if_not_exists,
                start: options.start,
                increment: options.increment,
                min_value: options.min_value,
                max_value: options.max_value,
                cycle: options.cycle,
                cache: options.cache,
                format_template_raw: options.format_template_raw,
                reset_period_raw: options.reset_period_raw,
                gap_free: options.gap_free,
                scope: options.scope,
            }));
        }
        if upper.starts_with("DROP SEQUENCE ") {
            let if_exists = upper.contains("IF EXISTS");
            let name = match extract_name_after_if_exists(parts, "SEQUENCE") {
                None => return Ok(None),
                Some(r) => r?,
            };
            return Ok(Some(NodedbStatement::DropSequence { name, if_exists }));
        }
        if upper.starts_with("ALTER SEQUENCE ") {
            let name = parts.get(2).map(|s| s.to_lowercase()).unwrap_or_default();
            let action = parts.get(3).map(|s| s.to_uppercase()).unwrap_or_default();
            let with_value = match action.as_str() {
                "RESTART" => {
                    if parts
                        .get(4)
                        .map(|s| s.eq_ignore_ascii_case("WITH"))
                        .unwrap_or(false)
                    {
                        parts.get(5).map(|s| s.to_string())
                    } else {
                        None
                    }
                }
                "FORMAT" => parts
                    .get(4)
                    .map(|s| s.trim_matches('\'').trim_matches('"').to_string()),
                _ => None,
            };
            return Ok(Some(NodedbStatement::AlterSequence {
                name,
                action,
                with_value,
            }));
        }
        if upper.starts_with("DESCRIBE SEQUENCE ") {
            let name = match parts.get(2) {
                None => return Ok(None),
                Some(s) => s.to_string(),
            };
            return Ok(Some(NodedbStatement::DescribeSequence { name }));
        }
        if upper == "SHOW SEQUENCES" || upper.starts_with("SHOW SEQUENCES") {
            return Ok(Some(NodedbStatement::ShowSequences));
        }
        Ok(None)
    })()
    .transpose()
}

// ── Option parsing ────────────────────────────────────────────────────────────

struct SequenceOptions {
    start: Option<i64>,
    increment: Option<i64>,
    min_value: Option<i64>,
    max_value: Option<i64>,
    cycle: bool,
    cache: Option<i64>,
    format_template_raw: Option<String>,
    reset_period_raw: Option<String>,
    gap_free: bool,
    scope: Option<String>,
}

/// Scan option tokens from `parts` starting after the sequence name.
fn parse_sequence_options(parts: &[&str], name: &str) -> SequenceOptions {
    let mut opts = SequenceOptions {
        start: None,
        increment: None,
        min_value: None,
        max_value: None,
        cycle: false,
        cache: None,
        format_template_raw: None,
        reset_period_raw: None,
        gap_free: false,
        scope: None,
    };

    // Find the token after the sequence name.
    let name_lower = name.to_lowercase();
    let start_idx = parts
        .iter()
        .position(|p| p.to_lowercase() == name_lower)
        .map(|i| i + 1)
        .unwrap_or(parts.len());

    let upper: Vec<String> = parts.iter().map(|p| p.to_uppercase()).collect();

    let mut i = start_idx;
    while i < parts.len() {
        match upper[i].as_str() {
            "START" => {
                i += 1;
                if i < parts.len() && upper[i] == "WITH" {
                    i += 1;
                }
                if i < parts.len() {
                    opts.start = parts[i].parse::<i64>().ok();
                }
            }
            "INCREMENT" => {
                i += 1;
                if i < parts.len() && upper[i] == "BY" {
                    i += 1;
                }
                if i < parts.len() {
                    opts.increment = parts[i].parse::<i64>().ok();
                }
            }
            "MINVALUE" => {
                i += 1;
                if i < parts.len() {
                    opts.min_value = parts[i].parse::<i64>().ok();
                }
            }
            "MAXVALUE" => {
                i += 1;
                if i < parts.len() {
                    opts.max_value = parts[i].parse::<i64>().ok();
                }
            }
            "CYCLE" => {
                opts.cycle = true;
            }
            "NO" => {
                i += 1;
                if i < parts.len() && upper[i] == "CYCLE" {
                    opts.cycle = false;
                }
            }
            "CACHE" => {
                i += 1;
                if i < parts.len() {
                    opts.cache = parts[i].parse::<i64>().ok();
                }
            }
            "FORMAT" => {
                i += 1;
                if i < parts.len() {
                    opts.format_template_raw =
                        Some(parts[i].trim_matches('\'').trim_matches('"').to_string());
                }
            }
            "RESET" => {
                i += 1;
                if i < parts.len() {
                    opts.reset_period_raw = Some(parts[i].to_string());
                }
            }
            "GAP_FREE" => {
                opts.gap_free = true;
            }
            "SCOPE" => {
                i += 1;
                if i < parts.len() {
                    opts.scope = Some(parts[i].to_uppercase());
                }
            }
            _ => {}
        }
        i += 1;
    }

    opts
}
