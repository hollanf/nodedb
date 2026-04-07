//! Push WHERE filters through Joins into individual Scan nodes.

use crate::types::*;

/// Push filters down through plan nodes where possible.
pub fn optimize(plan: SqlPlan) -> SqlPlan {
    // Currently a no-op — filters are already attached to scans during planning.
    // Future: split join predicates and push table-specific filters to each side.
    plan
}
