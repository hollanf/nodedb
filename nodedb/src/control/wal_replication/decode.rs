//! Convert committed ReplicatedWrite entries back to PhysicalPlan for Data Plane execution.

use super::types::{ReplicatedEntry, ReplicatedWrite};
use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{CrdtOp, DocumentOp, GraphOp, KvOp, VectorOp};
use crate::types::{TenantId, VShardId};

///
/// Returns `None` if the data is not a valid ReplicatedEntry (e.g., ConfChange or no-op).
pub fn from_replicated_entry(data: &[u8]) -> Option<(TenantId, VShardId, PhysicalPlan)> {
    let entry = ReplicatedEntry::from_bytes(data)?;
    let plan = to_physical_plan(&entry.write);
    Some((
        TenantId::new(entry.tenant_id),
        VShardId::new(entry.vshard_id),
        plan,
    ))
}

/// Convert a ReplicatedWrite back into a PhysicalPlan for Data Plane execution.
fn to_physical_plan(write: &ReplicatedWrite) -> PhysicalPlan {
    match write {
        ReplicatedWrite::PointPut {
            collection,
            document_id,
            value,
        } => PhysicalPlan::Document(DocumentOp::PointPut {
            collection: collection.clone(),
            document_id: document_id.clone(),
            value: value.clone(),
        }),
        ReplicatedWrite::PointInsert {
            collection,
            document_id,
            value,
            if_absent,
        } => PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: collection.clone(),
            document_id: document_id.clone(),
            value: value.clone(),
            if_absent: *if_absent,
        }),
        ReplicatedWrite::PointDelete {
            collection,
            document_id,
        } => PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: collection.clone(),
            document_id: document_id.clone(),
        }),
        ReplicatedWrite::PointUpdate {
            collection,
            document_id,
            updates,
        } => PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: collection.clone(),
            document_id: document_id.clone(),
            updates: updates.clone(),
            returning: false,
        }),
        ReplicatedWrite::VectorInsert {
            collection,
            vector,
            dim,
            field_name,
            doc_id,
        } => PhysicalPlan::Vector(VectorOp::Insert {
            collection: collection.clone(),
            vector: vector.clone(),
            dim: *dim,
            field_name: field_name.clone(),
            doc_id: doc_id.clone(),
        }),
        ReplicatedWrite::VectorBatchInsert {
            collection,
            vectors,
            dim,
        } => PhysicalPlan::Vector(VectorOp::BatchInsert {
            collection: collection.clone(),
            vectors: vectors.clone(),
            dim: *dim,
        }),
        ReplicatedWrite::VectorDelete {
            collection,
            vector_id,
        } => PhysicalPlan::Vector(VectorOp::Delete {
            collection: collection.clone(),
            vector_id: *vector_id,
        }),
        ReplicatedWrite::SetVectorParams {
            collection,
            m,
            ef_construction,
            metric,
            index_type,
            pq_m,
            ivf_cells,
            ivf_nprobe,
        } => PhysicalPlan::Vector(VectorOp::SetParams {
            collection: collection.clone(),
            m: *m,
            ef_construction: *ef_construction,
            metric: metric.clone(),
            index_type: index_type.clone(),
            pq_m: *pq_m,
            ivf_cells: *ivf_cells,
            ivf_nprobe: *ivf_nprobe,
        }),
        ReplicatedWrite::CrdtApply {
            collection,
            document_id,
            delta,
            peer_id,
        } => PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: collection.clone(),
            document_id: document_id.clone(),
            delta: delta.clone(),
            peer_id: *peer_id,
            mutation_id: 0,
        }),
        ReplicatedWrite::EdgePut {
            collection,
            src_id,
            label,
            dst_id,
            properties,
        } => PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: collection.clone(),
            src_id: src_id.clone(),
            label: label.clone(),
            dst_id: dst_id.clone(),
            properties: properties.clone(),
        }),
        ReplicatedWrite::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
        } => PhysicalPlan::Graph(GraphOp::EdgeDelete {
            collection: collection.clone(),
            src_id: src_id.clone(),
            label: label.clone(),
            dst_id: dst_id.clone(),
        }),
        ReplicatedWrite::KvPut {
            collection,
            key,
            value,
            ttl_ms,
        } => PhysicalPlan::Kv(KvOp::Put {
            collection: collection.clone(),
            key: key.clone(),
            value: value.clone(),
            ttl_ms: *ttl_ms,
        }),
        ReplicatedWrite::KvDelete { collection, keys } => PhysicalPlan::Kv(KvOp::Delete {
            collection: collection.clone(),
            keys: keys.clone(),
        }),
        ReplicatedWrite::KvBatchPut {
            collection,
            entries,
            ttl_ms,
        } => PhysicalPlan::Kv(KvOp::BatchPut {
            collection: collection.clone(),
            entries: entries.clone(),
            ttl_ms: *ttl_ms,
        }),
        ReplicatedWrite::KvExpire {
            collection,
            key,
            ttl_ms,
        } => PhysicalPlan::Kv(KvOp::Expire {
            collection: collection.clone(),
            key: key.clone(),
            ttl_ms: *ttl_ms,
        }),
        ReplicatedWrite::KvPersist { collection, key } => PhysicalPlan::Kv(KvOp::Persist {
            collection: collection.clone(),
            key: key.clone(),
        }),
        ReplicatedWrite::KvIncr {
            collection,
            key,
            delta,
            ttl_ms,
        } => PhysicalPlan::Kv(KvOp::Incr {
            collection: collection.clone(),
            key: key.clone(),
            delta: *delta,
            ttl_ms: *ttl_ms,
        }),
        ReplicatedWrite::KvIncrFloat {
            collection,
            key,
            delta,
        } => PhysicalPlan::Kv(KvOp::IncrFloat {
            collection: collection.clone(),
            key: key.clone(),
            delta: *delta,
        }),
        ReplicatedWrite::KvCas {
            collection,
            key,
            expected,
            new_value,
        } => PhysicalPlan::Kv(KvOp::Cas {
            collection: collection.clone(),
            key: key.clone(),
            expected: expected.clone(),
            new_value: new_value.clone(),
        }),
        ReplicatedWrite::KvGetSet {
            collection,
            key,
            new_value,
        } => PhysicalPlan::Kv(KvOp::GetSet {
            collection: collection.clone(),
            key: key.clone(),
            new_value: new_value.clone(),
        }),
        ReplicatedWrite::KvRegisterSortedIndex {
            collection,
            index_name,
            sort_columns,
            key_column,
            window_type,
            window_timestamp_column,
            window_start_ms,
            window_end_ms,
        } => PhysicalPlan::Kv(KvOp::RegisterSortedIndex {
            collection: collection.clone(),
            index_name: index_name.clone(),
            sort_columns: sort_columns.clone(),
            key_column: key_column.clone(),
            window_type: window_type.clone(),
            window_timestamp_column: window_timestamp_column.clone(),
            window_start_ms: *window_start_ms,
            window_end_ms: *window_end_ms,
        }),
        ReplicatedWrite::KvDropSortedIndex { index_name } => {
            PhysicalPlan::Kv(KvOp::DropSortedIndex {
                index_name: index_name.clone(),
            })
        }
    }
}
