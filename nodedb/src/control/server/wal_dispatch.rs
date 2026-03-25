//! WAL append logic for write operations.
//!
//! Serializes write plans as MessagePack and appends to the appropriate
//! WAL record type. Read operations are no-ops.

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use crate::wal::manager::WalManager;

/// Append a write operation to the WAL for single-node durability.
///
/// Serializes the write as MessagePack and appends to the appropriate
/// WAL record type. Read operations are no-ops (return Ok immediately).
pub fn wal_append_if_write(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: &PhysicalPlan,
) -> crate::Result<()> {
    match plan {
        PhysicalPlan::PointPut {
            collection,
            document_id,
            value,
        } => {
            let entry = rmp_serde::to_vec(&(collection, document_id, value)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal point put: {e}"),
                }
            })?;
            wal.append_put(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::PointDelete {
            collection,
            document_id,
        } => {
            let entry = rmp_serde::to_vec(&(collection, document_id)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal point delete: {e}"),
                }
            })?;
            wal.append_delete(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::VectorInsert {
            collection,
            vector,
            dim,
            field_name: _,
            doc_id: _,
        } => {
            let entry = rmp_serde::to_vec(&(collection, vector, dim)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal vector insert: {e}"),
                }
            })?;
            wal.append_vector_put(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::VectorBatchInsert {
            collection,
            vectors,
            dim,
        } => {
            let entry = rmp_serde::to_vec(&(collection, vectors, dim)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal vector batch insert: {e}"),
                }
            })?;
            wal.append_vector_put(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::VectorDelete {
            collection,
            vector_id,
        } => {
            let entry = rmp_serde::to_vec(&(collection, vector_id)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal vector delete: {e}"),
                }
            })?;
            wal.append_vector_delete(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::CrdtApply { delta, .. } => {
            wal.append_crdt_delta(tenant_id, vshard_id, delta)?;
        }
        PhysicalPlan::EdgePut {
            src_id,
            label,
            dst_id,
            properties,
        } => {
            let entry = rmp_serde::to_vec(&(src_id, label, dst_id, properties)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal edge put: {e}"),
                }
            })?;
            wal.append_put(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::EdgeDelete {
            src_id,
            label,
            dst_id,
        } => {
            let entry = rmp_serde::to_vec(&(src_id, label, dst_id)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal edge delete: {e}"),
                }
            })?;
            wal.append_delete(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::SetVectorParams {
            collection,
            m,
            ef_construction,
            metric,
            index_type,
            pq_m,
            ivf_cells,
            ivf_nprobe,
        } => {
            let entry = rmp_serde::to_vec(&(
                collection,
                m,
                ef_construction,
                metric,
                index_type,
                pq_m,
                ivf_cells,
                ivf_nprobe,
            ))
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal set vector params: {e}"),
            })?;
            wal.append_vector_params(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::TimeseriesIngest {
            collection,
            payload,
            format: _,
        } => {
            // Use TimeseriesBatch WAL record. Payload is already the raw ILP/samples bytes.
            // Wrap with collection name for replay routing.
            let wal_payload = rmp_serde::to_vec(&(collection, payload)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal timeseries batch: {e}"),
                }
            })?;
            wal.append_timeseries_batch(tenant_id, vshard_id, &wal_payload)?;
        }
        // Read operations and control commands: no WAL needed.
        _ => {}
    }
    Ok(())
}
