//! Parsing helpers shared by `drop_sequence` and `show`.

/// Parse a DROP target from parts: extracts the name and `IF EXISTS` flag.
///
/// `skip` is the index of the first token after the command keyword
/// (e.g. `skip=2` for `DROP SEQUENCE <name>`).
pub fn parse_drop_target(parts: &[&str], skip: usize) -> (String, bool) {
    let rest = &parts[skip..];
    if rest.len() >= 3
        && rest[0].eq_ignore_ascii_case("IF")
        && rest[1].eq_ignore_ascii_case("EXISTS")
    {
        (rest[2].to_lowercase(), true)
    } else if let Some(name) = rest.first() {
        (name.to_lowercase(), false)
    } else {
        (String::new(), false)
    }
}
