//! Usage metering configuration: cost definitions, dimensions, units.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Metering configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeteringConfig {
    /// Whether usage metering is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// Flush interval in seconds for aggregating per-core counters.
    #[serde(default = "default_flush_interval")]
    pub flush_interval_secs: u64,

    /// Per-operation costs in tokens (unified currency).
    /// Key = operation name, value = token cost.
    #[serde(default = "default_operation_costs")]
    pub operation_costs: HashMap<String, u64>,

    /// Custom metering dimensions beyond the built-in ones.
    #[serde(default)]
    pub custom_dimensions: Vec<CustomDimension>,
}

/// A custom metering dimension.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomDimension {
    pub name: String,
    pub unit: String,
}

impl Default for MeteringConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            flush_interval_secs: default_flush_interval(),
            operation_costs: default_operation_costs(),
            custom_dimensions: Vec::new(),
        }
    }
}

fn default_flush_interval() -> u64 {
    60
}

fn default_operation_costs() -> HashMap<String, u64> {
    let mut m = HashMap::new();
    // Read Units (RU)
    m.insert("point_get".into(), 1);
    m.insert("kv_get".into(), 1);
    m.insert("document_scan".into(), 5);
    m.insert("vector_search".into(), 20);
    m.insert("text_search".into(), 10);
    m.insert("hybrid_search".into(), 25);
    m.insert("graph_hop".into(), 10);
    m.insert("graph_path".into(), 15);
    m.insert("aggregate".into(), 10);
    // Write Units (WU)
    m.insert("point_put".into(), 2);
    m.insert("point_delete".into(), 1);
    m.insert("batch_insert".into(), 10);
    m.insert("kv_put".into(), 2);
    m.insert("vector_insert".into(), 5);
    m.insert("edge_put".into(), 3);
    m
}

/// Built-in metering dimension names.
pub struct MeterDimensions;

impl MeterDimensions {
    pub const AUTH_USER_ID: &'static str = "auth_user_id";
    pub const ORG_ID: &'static str = "org_id";
    pub const TENANT_ID: &'static str = "tenant_id";
    pub const COLLECTION: &'static str = "collection";
    pub const ENGINE: &'static str = "engine";
    pub const OPERATION: &'static str = "operation";
    pub const SCOPE: &'static str = "scope";
}
