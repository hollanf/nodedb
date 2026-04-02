//! Tenant-scoped identifiers for cross-tenant isolation.
//!
//! All tenant data in the Data Plane is scoped by tenant_id using the
//! `"{tid}:{name}"` convention. This module centralizes the construction
//! and parsing of scoped identifiers to ensure consistency and prevent
//! cross-tenant visibility.

/// Construct a tenant-scoped collection key: `"{tid}:{collection}"`.
///
/// Used for config lookups, inverted index scoping, and secondary index keys.
#[inline]
pub fn scoped_collection(tid: u32, collection: &str) -> String {
    format!("{tid}:{collection}")
}

/// Construct a tenant-scoped node ID: `"{tid}:{node_id}"`.
///
/// Used for graph CSR adjacency and edge store lookups.
#[inline]
pub fn scoped_node(tid: u32, node_id: &str) -> String {
    format!("{tid}:{node_id}")
}

/// Strip the `"{tid}:"` prefix from a scoped identifier for client-facing output.
#[inline]
pub fn unscoped(scoped: &str) -> &str {
    scoped.find(':').map(|i| &scoped[i + 1..]).unwrap_or(scoped)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_collection_format() {
        assert_eq!(scoped_collection(42, "orders"), "42:orders");
    }

    #[test]
    fn scoped_node_format() {
        assert_eq!(scoped_node(1, "doc-123"), "1:doc-123");
    }

    #[test]
    fn unscoped_strips_prefix() {
        assert_eq!(unscoped("42:orders"), "orders");
        assert_eq!(unscoped("1:doc-123"), "doc-123");
    }

    #[test]
    fn unscoped_no_prefix_passthrough() {
        assert_eq!(unscoped("no_prefix"), "no_prefix");
    }
}
