//! High-level native protocol client implementing the `NodeDb` trait.
//!
//! Wraps a connection pool and translates trait calls into native protocol
//! opcodes. Also exposes SQL/DDL methods not covered by the trait.

use std::collections::HashMap;

use async_trait::async_trait;

use nodedb_types::document::Document;
use nodedb_types::error::{ErrorDetails, NodeDbError, NodeDbResult};
use nodedb_types::filter::{EdgeFilter, MetadataFilter};
use nodedb_types::id::{EdgeId, NodeId};
use nodedb_types::protocol::{OpCode, TextFields};
use nodedb_types::result::{QueryResult, SearchResult, SubGraph};
use nodedb_types::value::Value;

use super::pool::{Pool, PoolConfig};
use super::response_parse::{json_to_value, parse_search_results, parse_subgraph_response};
use crate::traits::NodeDb;

/// Native protocol client for NodeDB.
///
/// Connects via the binary MessagePack protocol. Supports all operations:
/// SQL, DDL, direct Data Plane ops, transactions, session parameters.
pub struct NativeClient {
    pool: Pool,
}

impl NativeClient {
    /// Create a client with the given pool configuration.
    pub fn new(config: PoolConfig) -> Self {
        Self {
            pool: Pool::new(config),
        }
    }

    /// Connect to a NodeDB server with default settings.
    pub fn connect(addr: &str) -> Self {
        Self::new(PoolConfig {
            addr: addr.to_string(),
            ..Default::default()
        })
    }

    /// Execute a SQL query and return structured results.
    ///
    /// Retries once with a fresh connection on I/O failure.
    pub async fn query(&self, sql: &str) -> NodeDbResult<QueryResult> {
        let mut conn = self.pool.acquire().await?;
        match conn.execute_sql(sql).await {
            Ok(r) => Ok(r),
            Err(e) if is_connection_error(&e) => {
                drop(conn);
                let mut conn = self.pool.acquire().await?;
                conn.execute_sql(sql).await
            }
            Err(e) => Err(e),
        }
    }

    /// Execute a DDL command.
    pub async fn ddl(&self, sql: &str) -> NodeDbResult<QueryResult> {
        let mut conn = self.pool.acquire().await?;
        match conn.execute_ddl(sql).await {
            Ok(r) => Ok(r),
            Err(e) if is_connection_error(&e) => {
                drop(conn);
                let mut conn = self.pool.acquire().await?;
                conn.execute_ddl(sql).await
            }
            Err(e) => Err(e),
        }
    }

    /// Begin a transaction.
    pub async fn begin(&self) -> NodeDbResult<()> {
        let mut conn = self.pool.acquire().await?;
        conn.begin().await
    }

    /// Commit the current transaction.
    pub async fn commit(&self) -> NodeDbResult<()> {
        let mut conn = self.pool.acquire().await?;
        conn.commit().await
    }

    /// Rollback the current transaction.
    pub async fn rollback(&self) -> NodeDbResult<()> {
        let mut conn = self.pool.acquire().await?;
        conn.rollback().await
    }

    /// Set a session parameter.
    pub async fn set_parameter(&self, key: &str, value: &str) -> NodeDbResult<()> {
        let mut conn = self.pool.acquire().await?;
        conn.set_parameter(key, value).await
    }

    /// Show a session parameter.
    pub async fn show_parameter(&self, key: &str) -> NodeDbResult<String> {
        let mut conn = self.pool.acquire().await?;
        conn.show_parameter(key).await
    }

    /// Ping the server.
    pub async fn ping(&self) -> NodeDbResult<()> {
        let mut conn = self.pool.acquire().await?;
        conn.ping().await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl NodeDb for NativeClient {
    async fn vector_search(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
        _filter: Option<&MetadataFilter>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        let mut conn = self.pool.acquire().await?;
        let resp = conn
            .send(
                OpCode::VectorSearch,
                TextFields {
                    collection: Some(collection.to_string()),
                    query_vector: Some(query.to_vec()),
                    top_k: Some(k as u64),
                    ..Default::default()
                },
            )
            .await?;
        parse_search_results(&resp)
    }

    async fn vector_insert(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        let meta_json = metadata
            .map(|d| {
                let obj: HashMap<String, Value> = d.fields;
                sonic_rs::to_string(&obj).unwrap_or_else(|_| "{}".into())
            })
            .unwrap_or_else(|| "{}".into());
        let arr_str = format_f32_array(embedding);
        // Escape single quotes in values to prevent SQL injection.
        let escaped_id = id.replace('\'', "''");
        let escaped_meta = meta_json.replace('\'', "''");
        let sql = format!(
            "INSERT INTO \"{}\" (id, embedding, metadata) VALUES ('{}', {}, '{}')",
            collection.replace('"', "\"\""),
            escaped_id,
            arr_str,
            escaped_meta,
        );
        let mut conn = self.pool.acquire().await?;
        conn.execute_sql(&sql).await?;
        Ok(())
    }

    async fn vector_delete(&self, collection: &str, id: &str) -> NodeDbResult<()> {
        let escaped_id = id.replace('\'', "''");
        let sql = format!(
            "DELETE FROM \"{}\" WHERE id = '{}'",
            collection.replace('"', "\"\""),
            escaped_id,
        );
        let mut conn = self.pool.acquire().await?;
        conn.execute_sql(&sql).await?;
        Ok(())
    }

    async fn graph_traverse(
        &self,
        start: &NodeId,
        depth: u8,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<SubGraph> {
        let mut conn = self.pool.acquire().await?;
        let resp = conn
            .send(
                OpCode::GraphHop,
                TextFields {
                    start_node: Some(start.as_str().to_string()),
                    depth: Some(depth as u64),
                    edge_label: edge_filter.and_then(|f| f.labels.first().cloned()),
                    ..Default::default()
                },
            )
            .await?;
        parse_subgraph_response(&resp)
    }

    async fn graph_insert_edge(
        &self,
        from: &NodeId,
        to: &NodeId,
        edge_type: &str,
        properties: Option<Document>,
    ) -> NodeDbResult<EdgeId> {
        let props_json = properties.and_then(|d| serde_json::to_value(d.fields).ok());
        let mut conn = self.pool.acquire().await?;
        conn.send(
            OpCode::EdgePut,
            TextFields {
                from_node: Some(from.as_str().to_string()),
                to_node: Some(to.as_str().to_string()),
                edge_type: Some(edge_type.to_string()),
                properties: props_json,
                ..Default::default()
            },
        )
        .await?;
        Ok(EdgeId::from_components(
            from.as_str(),
            to.as_str(),
            edge_type,
        ))
    }

    async fn graph_delete_edge(&self, edge_id: &EdgeId) -> NodeDbResult<()> {
        let parts: Vec<&str> = edge_id.as_str().splitn(3, "--").collect();
        if parts.len() < 3 {
            return Err(NodeDbError::bad_request(format!(
                "invalid edge ID format: {}",
                edge_id.as_str()
            )));
        }
        let src = parts[0];
        let rest = parts[1];
        let (label, dst) = rest
            .split_once("-->")
            .ok_or_else(|| NodeDbError::bad_request("invalid edge ID"))?;

        let mut conn = self.pool.acquire().await?;
        conn.send(
            OpCode::EdgeDelete,
            TextFields {
                from_node: Some(src.to_string()),
                to_node: Some(dst.to_string()),
                edge_type: Some(label.to_string()),
                ..Default::default()
            },
        )
        .await?;
        Ok(())
    }

    async fn document_get(&self, collection: &str, id: &str) -> NodeDbResult<Option<Document>> {
        let mut conn = self.pool.acquire().await?;
        let resp = conn
            .send(
                OpCode::PointGet,
                TextFields {
                    collection: Some(collection.to_string()),
                    document_id: Some(id.to_string()),
                    ..Default::default()
                },
            )
            .await?;

        let rows = resp.rows.unwrap_or_default();
        if rows.is_empty() {
            return Ok(None);
        }

        let json_text = rows[0].first().and_then(|v| v.as_str()).unwrap_or("{}");
        let mut doc = Document::new(id);
        if let Ok(obj) = serde_json::from_str::<HashMap<String, serde_json::Value>>(json_text) {
            for (k, v) in obj {
                doc.set(&k, json_to_value(v));
            }
        }
        Ok(Some(doc))
    }

    async fn document_put(&self, collection: &str, doc: Document) -> NodeDbResult<()> {
        let data = sonic_rs::to_vec(&doc.fields)
            .map_err(|e| NodeDbError::serialization("json", format!("doc serialize: {e}")))?;
        let mut conn = self.pool.acquire().await?;
        conn.send(
            OpCode::PointPut,
            TextFields {
                collection: Some(collection.to_string()),
                document_id: Some(doc.id.clone()),
                data: Some(data),
                ..Default::default()
            },
        )
        .await?;
        Ok(())
    }

    async fn document_delete(&self, collection: &str, id: &str) -> NodeDbResult<()> {
        let mut conn = self.pool.acquire().await?;
        conn.send(
            OpCode::PointDelete,
            TextFields {
                collection: Some(collection.to_string()),
                document_id: Some(id.to_string()),
                ..Default::default()
            },
        )
        .await?;
        Ok(())
    }

    async fn execute_sql(&self, query: &str, _params: &[Value]) -> NodeDbResult<QueryResult> {
        self.query(query).await
    }
}

// ─── Internal helpers ──────────────────────────────────────────────

fn format_f32_array(arr: &[f32]) -> String {
    let inner: Vec<String> = arr.iter().map(|v| format!("{v}")).collect();
    format!("ARRAY[{}]", inner.join(","))
}

/// Check if an error is a connection-level failure (worth retrying).
fn is_connection_error(e: &NodeDbError) -> bool {
    matches!(
        e.details(),
        ErrorDetails::SyncConnectionFailed | ErrorDetails::Storage
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_f32_array_works() {
        let arr = [0.1f32, 0.2, 0.3];
        let s = format_f32_array(&arr);
        assert!(s.starts_with("ARRAY["));
        assert!(s.contains("0.1"));
        assert!(s.ends_with(']'));
    }
}
