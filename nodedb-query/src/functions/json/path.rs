//! Lightweight JSONPath parser.
//!
//! Handles: `$`, `$.key`, `$.a.b`, `$.arr[0]`, `$.a.arr[0].b`.
//! Filter expressions (`?(@>5)`) and recursive descent (`..`) are
//! explicitly rejected with a typed error.

/// A single step in a parsed JSONPath.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PathStep {
    /// Object key access: `$.key`
    Key(String),
    /// Array index access: `$.arr[0]`
    Index(usize),
}

/// Error returned when a JSONPath expression cannot be parsed.
#[derive(Debug, thiserror::Error)]
pub enum PathError {
    #[error("JSONPath must start with '$', got: {input:?}")]
    MissingDollar { input: String },

    #[error("unsupported JSONPath expression (filter/recursive not supported): {input:?}")]
    Unsupported { input: String },

    #[error("invalid array index in JSONPath: {segment:?}")]
    InvalidIndex { segment: String },

    #[error("empty key segment in JSONPath: {input:?}")]
    EmptyKey { input: String },

    #[error("malformed JSONPath segment: {segment:?}")]
    Malformed { segment: String },
}

/// Parse a JSONPath string into a list of steps.
///
/// Accepted forms: `$`, `$.key`, `$.a.b.c`, `$.arr[0]`, `$.a[1].b`.
/// Returns `Err(PathError::Unsupported)` for filter expressions.
pub(super) fn parse_jsonpath(path: &str) -> Result<Vec<PathStep>, PathError> {
    if !path.starts_with('$') {
        return Err(PathError::MissingDollar {
            input: path.to_string(),
        });
    }

    // Reject recursive descent.
    if path.contains("..") {
        return Err(PathError::Unsupported {
            input: path.to_string(),
        });
    }
    // Reject filter expressions.
    if path.contains("?(") {
        return Err(PathError::Unsupported {
            input: path.to_string(),
        });
    }

    let rest = &path[1..]; // strip leading `$`
    if rest.is_empty() {
        return Ok(vec![]);
    }
    if !rest.starts_with('.') {
        return Err(PathError::Malformed {
            segment: rest.to_string(),
        });
    }

    // Strip the leading `.` after `$`.
    let rest = &rest[1..];
    if rest.is_empty() {
        return Err(PathError::EmptyKey {
            input: path.to_string(),
        });
    }

    let mut steps = Vec::new();

    // Split on `.` but handle bracket notation `key[n]` within each segment.
    for segment in rest.split('.') {
        if segment.is_empty() {
            return Err(PathError::EmptyKey {
                input: path.to_string(),
            });
        }
        // Reject filter fragments.
        if segment.contains("?(") {
            return Err(PathError::Unsupported {
                input: path.to_string(),
            });
        }

        // A segment may contain bracket index(es): `arr[0]`, `arr[0][1]`.
        if segment.contains('[') {
            parse_segment_with_brackets(segment, path, &mut steps)?;
        } else {
            steps.push(PathStep::Key(segment.to_string()));
        }
    }

    Ok(steps)
}

fn parse_segment_with_brackets(
    segment: &str,
    full_path: &str,
    steps: &mut Vec<PathStep>,
) -> Result<(), PathError> {
    // Find the first `[`.
    let bracket_pos = segment.find('[').unwrap();
    let key_part = &segment[..bracket_pos];
    let rest = &segment[bracket_pos..];

    if !key_part.is_empty() {
        steps.push(PathStep::Key(key_part.to_string()));
    }

    // Parse `[n]` sequences.
    let mut cursor = rest;
    while !cursor.is_empty() {
        if !cursor.starts_with('[') {
            return Err(PathError::Malformed {
                segment: segment.to_string(),
            });
        }
        let close = cursor.find(']').ok_or_else(|| PathError::Malformed {
            segment: segment.to_string(),
        })?;
        let idx_str = &cursor[1..close];
        let idx = idx_str
            .parse::<usize>()
            .map_err(|_| PathError::InvalidIndex {
                segment: segment.to_string(),
            })?;
        steps.push(PathStep::Index(idx));
        cursor = &cursor[close + 1..];
    }

    if steps.is_empty() {
        return Err(PathError::EmptyKey {
            input: full_path.to_string(),
        });
    }
    Ok(())
}

/// Walk a `Value` following the given path steps.
///
/// Returns `None` if any step is missing or the value type is wrong.
pub(super) fn walk_path<'a>(
    mut current: &'a nodedb_types::Value,
    steps: &[PathStep],
) -> Option<&'a nodedb_types::Value> {
    for step in steps {
        current = match step {
            PathStep::Key(k) => match current {
                nodedb_types::Value::Object(map) => map.get(k.as_str())?,
                _ => return None,
            },
            PathStep::Index(i) => match current {
                nodedb_types::Value::Array(arr) => arr.get(*i)?,
                _ => return None,
            },
        };
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_root() {
        assert_eq!(parse_jsonpath("$").unwrap(), vec![]);
    }

    #[test]
    fn parse_single_key() {
        assert_eq!(
            parse_jsonpath("$.key").unwrap(),
            vec![PathStep::Key("key".into())]
        );
    }

    #[test]
    fn parse_nested_keys() {
        assert_eq!(
            parse_jsonpath("$.a.b.c").unwrap(),
            vec![
                PathStep::Key("a".into()),
                PathStep::Key("b".into()),
                PathStep::Key("c".into()),
            ]
        );
    }

    #[test]
    fn parse_array_index() {
        assert_eq!(
            parse_jsonpath("$.arr[0]").unwrap(),
            vec![PathStep::Key("arr".into()), PathStep::Index(0)]
        );
    }

    #[test]
    fn parse_mixed() {
        assert_eq!(
            parse_jsonpath("$.a.arr[1].b").unwrap(),
            vec![
                PathStep::Key("a".into()),
                PathStep::Key("arr".into()),
                PathStep::Index(1),
                PathStep::Key("b".into()),
            ]
        );
    }

    #[test]
    fn reject_missing_dollar() {
        assert!(matches!(
            parse_jsonpath("key"),
            Err(PathError::MissingDollar { .. })
        ));
    }

    #[test]
    fn reject_recursive_descent() {
        assert!(matches!(
            parse_jsonpath("$..key"),
            Err(PathError::Unsupported { .. })
        ));
    }

    #[test]
    fn reject_filter_expr() {
        assert!(matches!(
            parse_jsonpath("$.arr[?(@>5)]"),
            Err(PathError::Unsupported { .. })
        ));
    }

    #[test]
    fn reject_empty_segment() {
        assert!(matches!(
            parse_jsonpath("$.a..b"),
            Err(PathError::Unsupported { .. })
        ));
    }

    #[test]
    fn reject_bad_bracket() {
        assert!(matches!(
            parse_jsonpath("$.arr[bad]"),
            Err(PathError::InvalidIndex { .. })
        ));
    }

    #[test]
    fn reject_unclosed_bracket() {
        assert!(matches!(
            parse_jsonpath("$.arr[0"),
            Err(PathError::Malformed { .. })
        ));
    }
}
