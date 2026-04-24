//! Key layout for the versioned document and index tables.

/// 20-digit zero-pad for i64 lexicographic ordering under reverse-scan.
pub fn format_sys_from(sys_from_ms: i64) -> String {
    format!("{sys_from_ms:020}")
}

/// Build a versioned document key. Returns an error if `doc_id` contains
/// a NUL byte — NUL is reserved as the version separator.
pub fn versioned_doc_key(
    tenant: u32,
    coll: &str,
    doc_id: &str,
    sys_from_ms: i64,
) -> crate::Result<String> {
    if doc_id.as_bytes().contains(&0) {
        return Err(crate::Error::BadRequest {
            detail: "document id may not contain NUL byte".into(),
        });
    }
    Ok(format!(
        "{tenant}:{coll}:{doc_id}\x00{}",
        format_sys_from(sys_from_ms)
    ))
}

/// Prefix matching every version of a single doc_id — used by reverse-scan.
pub fn doc_prefix(tenant: u32, coll: &str, doc_id: &str) -> String {
    format!("{tenant}:{coll}:{doc_id}\x00")
}

/// Upper-bound exclusive companion of [`doc_prefix`]: because `\x00` is
/// the minimum byte, `\x01` is the next-greater separator and bounds all
/// suffixes for this doc_id cleanly.
pub fn doc_prefix_end(tenant: u32, coll: &str, doc_id: &str) -> String {
    format!("{tenant}:{coll}:{doc_id}\x01")
}

/// Prefix matching every version of every doc_id in a collection.
pub fn coll_prefix(tenant: u32, coll: &str) -> String {
    format!("{tenant}:{coll}:")
}

/// Upper-bound exclusive companion of [`coll_prefix`].
pub fn coll_prefix_end(tenant: u32, coll: &str) -> String {
    format!("{tenant}:{coll};")
}

/// Extract `sys_from_ms` from a versioned key. Returns `None` if the key
/// has no NUL separator (defensive — should not happen for keys produced
/// by [`versioned_doc_key`]).
pub fn parse_sys_from(key: &str) -> Option<i64> {
    let (_, suffix) = key.rsplit_once('\x00')?;
    suffix.parse().ok()
}

/// Extract `doc_id` slice from a versioned key (between the `{coll}:` and
/// `\x00` boundaries). Returns the whole remainder when parsing fails.
pub fn parse_doc_id<'a>(key: &'a str, tenant: u32, coll: &str) -> Option<&'a str> {
    let prefix = format!("{tenant}:{coll}:");
    let rest = key.strip_prefix(&prefix)?;
    let (id, _) = rest.rsplit_once('\x00')?;
    Some(id)
}
