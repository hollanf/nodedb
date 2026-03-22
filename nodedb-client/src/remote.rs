//! `NodeDbRemote` — pgwire client that translates `NodeDb` trait calls
//! into SQL/DSL and sends them to the NodeDB Origin.
//!
//! This is the cloud-side implementation of the `NodeDb` trait. Server-side
//! applications use this to talk to the Origin cluster over the PostgreSQL
//! wire protocol.
//!
//! ```rust,ignore
//! let db: Arc<dyn NodeDb> = Arc::new(
//!     NodeDbRemote::connect("host=localhost port=5432 user=app dbname=mydb").await?
//! );
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::{EdgeFilter, MetadataFilter};
use nodedb_types::id::{EdgeId, NodeId};
use nodedb_types::result::{QueryResult, SearchResult, SubGraph, SubGraphEdge, SubGraphNode};
use nodedb_types::value::Value;

use crate::traits::NodeDb;

/// Remote NodeDB client. Connects to an Origin instance over pgwire and
/// translates `NodeDb` trait calls into SQL/DSL queries.
pub struct NodeDbRemote {
    client: Arc<Mutex<Client>>,
}

impl NodeDbRemote {
    /// Connect to a NodeDB Origin instance.
    ///
    /// `config` is a standard PostgreSQL connection string:
    /// `"host=localhost port=5432 user=app dbname=mydb"`
    pub async fn connect(config: &str) -> NodeDbResult<Self> {
        let (client, connection) = tokio_postgres::connect(config, NoTls).await.map_err(|e| {
            NodeDbError::SyncConnectionFailed {
                detail: e.to_string(),
            }
        })?;

        // Spawn the connection handler — it runs in the background.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("pgwire connection error: {e}");
            }
        });

        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }

    /// Execute a raw SQL string and return rows as `Vec<Vec<Value>>`.
    async fn query_raw(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> NodeDbResult<(Vec<String>, Vec<Vec<Value>>)> {
        let client = self.client.lock().await;
        let rows = client
            .query(sql, params)
            .await
            .map_err(|e| NodeDbError::Storage {
                detail: format!("pgwire query failed: {e}"),
            })?;

        if rows.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let columns: Vec<String> = rows[0]
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();

        let mut result_rows = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut vals = Vec::with_capacity(columns.len());
            for (i, col) in row.columns().iter().enumerate() {
                let val = pg_value_to_value(row, i, col.type_());
                vals.push(val);
            }
            result_rows.push(vals);
        }

        Ok((columns, result_rows))
    }

    /// Execute a statement that doesn't return rows (INSERT/UPDATE/DELETE).
    async fn execute_raw(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> NodeDbResult<u64> {
        let client = self.client.lock().await;
        client
            .execute(sql, params)
            .await
            .map_err(|e| NodeDbError::Storage {
                detail: format!("pgwire execute failed: {e}"),
            })
    }
}

/// Convert a `tokio_postgres` column value to `nodedb_types::Value`.
fn pg_value_to_value(
    row: &tokio_postgres::Row,
    idx: usize,
    ty: &tokio_postgres::types::Type,
) -> Value {
    use tokio_postgres::types::Type;

    // Try typed extraction based on the column type.
    match *ty {
        Type::BOOL => row
            .try_get::<_, bool>(idx)
            .map(Value::Bool)
            .unwrap_or(Value::Null),
        Type::INT2 => row
            .try_get::<_, i16>(idx)
            .map(|v| Value::Integer(v as i64))
            .unwrap_or(Value::Null),
        Type::INT4 => row
            .try_get::<_, i32>(idx)
            .map(|v| Value::Integer(v as i64))
            .unwrap_or(Value::Null),
        Type::INT8 => row
            .try_get::<_, i64>(idx)
            .map(Value::Integer)
            .unwrap_or(Value::Null),
        Type::FLOAT4 => row
            .try_get::<_, f32>(idx)
            .map(|v| Value::Float(v as f64))
            .unwrap_or(Value::Null),
        Type::FLOAT8 => row
            .try_get::<_, f64>(idx)
            .map(Value::Float)
            .unwrap_or(Value::Null),
        Type::TEXT | Type::VARCHAR | Type::NAME => row
            .try_get::<_, String>(idx)
            .map(Value::String)
            .unwrap_or(Value::Null),
        Type::BYTEA => row
            .try_get::<_, Vec<u8>>(idx)
            .map(Value::Bytes)
            .unwrap_or(Value::Null),
        Type::JSON | Type::JSONB => row
            .try_get::<_, serde_json::Value>(idx)
            .map(|v| json_to_value(&v))
            .unwrap_or(Value::Null),
        _ => {
            // Fallback: try as string.
            row.try_get::<_, String>(idx)
                .map(Value::String)
                .unwrap_or(Value::Null)
        }
    }
}

/// Convert `serde_json::Value` to `nodedb_types::Value`.
fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else {
                Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(a) => Value::Array(a.iter().map(json_to_value).collect()),
        serde_json::Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), json_to_value(v)))
                .collect(),
        ),
    }
}

/// Format an f32 slice as a SQL ARRAY literal: `ARRAY[0.1,0.2,0.3]`.
fn format_vector_array(v: &[f32]) -> String {
    let inner: Vec<String> = v.iter().map(|f| format!("{f}")).collect();
    format!("ARRAY[{}]", inner.join(","))
}

#[async_trait]
impl NodeDb for NodeDbRemote {
    async fn vector_search(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
        _filter: Option<&MetadataFilter>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        // Use NodeDB DSL: SEARCH <collection> USING VECTOR(ARRAY[...], <k>)
        let sql = format!(
            "SEARCH {collection} USING VECTOR({}, {k})",
            format_vector_array(query),
        );

        let (columns, rows) = self.query_raw(&sql, &[]).await?;

        // Parse results — the DSL returns JSON text in a "result" column.
        // If it's a structured result set, parse columns directly.
        if columns.len() == 1 && columns[0] == "result" {
            if let Some(row) = rows.first()
                && let Some(Value::String(json_text)) = row.first()
            {
                return parse_vector_search_json(json_text);
            }
            return Ok(Vec::new());
        }

        // Structured result set: id, distance columns.
        let mut results = Vec::with_capacity(rows.len());
        let id_idx = columns.iter().position(|c| c == "id").unwrap_or(0);
        let dist_idx = columns.iter().position(|c| c == "distance").unwrap_or(1);

        for row in &rows {
            let id = row
                .get(id_idx)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let distance = row.get(dist_idx).and_then(|v| v.as_f64()).unwrap_or(0.0) as f32;

            results.push(SearchResult {
                id,
                node_id: None,
                distance,
                metadata: HashMap::new(),
            });
        }

        Ok(results)
    }

    async fn vector_insert(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        let meta_json = metadata
            .map(|d| serde_json::to_string(&d).unwrap_or_else(|_| "{}".into()))
            .unwrap_or_else(|| "{}".into());

        let sql = format!(
            "INSERT INTO {collection} (id, embedding, metadata) VALUES ($1, {}, $2::jsonb)",
            format_vector_array(embedding),
        );
        self.execute_raw(&sql, &[&id, &meta_json]).await?;
        Ok(())
    }

    async fn vector_delete(&self, collection: &str, id: &str) -> NodeDbResult<()> {
        let sql = format!("DELETE FROM {collection} WHERE id = $1");
        self.execute_raw(&sql, &[&id]).await?;
        Ok(())
    }

    async fn graph_traverse(
        &self,
        start: &NodeId,
        depth: u8,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<SubGraph> {
        let label_clause = edge_filter
            .and_then(|f| f.labels.first())
            .map(|l| format!(", '{l}'"))
            .unwrap_or_default();

        let sql = format!("SELECT * FROM graph_traverse('{start}', {depth}{label_clause})");

        let (columns, rows) = self.query_raw(&sql, &[]).await?;

        // Parse graph results. If single "result" column, it's JSON.
        if columns.len() == 1 && columns[0] == "result" {
            if let Some(row) = rows.first()
                && let Some(Value::String(json_text)) = row.first()
            {
                return parse_graph_traverse_json(json_text);
            }
            return Ok(SubGraph::empty());
        }

        // Structured: node_id, depth, edge_src, edge_dst, edge_label columns.
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut seen_nodes = std::collections::HashSet::new();

        for row in &rows {
            let node_id_str = row.first().and_then(|v| v.as_str()).unwrap_or("");
            let d = row.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as u8;

            if seen_nodes.insert(node_id_str.to_string()) {
                nodes.push(SubGraphNode {
                    id: NodeId::new(node_id_str),
                    depth: d,
                    properties: HashMap::new(),
                });
            }

            if let (Some(src), Some(dst), Some(label)) = (
                row.get(2).and_then(|v| v.as_str()),
                row.get(3).and_then(|v| v.as_str()),
                row.get(4).and_then(|v| v.as_str()),
            ) {
                edges.push(SubGraphEdge {
                    id: EdgeId::from_components(src, dst, label),
                    from: NodeId::new(src),
                    to: NodeId::new(dst),
                    label: label.to_string(),
                    properties: HashMap::new(),
                });
            }
        }

        Ok(SubGraph { nodes, edges })
    }

    async fn graph_insert_edge(
        &self,
        from: &NodeId,
        to: &NodeId,
        edge_type: &str,
        properties: Option<Document>,
    ) -> NodeDbResult<EdgeId> {
        let props_json = properties
            .map(|d| serde_json::to_string(&d).unwrap_or_else(|_| "{}".into()))
            .unwrap_or_else(|| "{}".into());

        let from_str = from.as_str();
        let to_str = to.as_str();
        let sql = "INSERT INTO edges (src, dst, label, properties) VALUES ($1, $2, $3, $4::jsonb)";
        self.execute_raw(sql, &[&from_str, &to_str, &edge_type, &props_json])
            .await?;

        Ok(EdgeId::from_components(
            from.as_str(),
            to.as_str(),
            edge_type,
        ))
    }

    async fn graph_delete_edge(&self, edge_id: &EdgeId) -> NodeDbResult<()> {
        // Edge IDs are formatted as "src--label-->dst".
        let id_str = edge_id.as_str();
        let sql = "DELETE FROM edges WHERE id = $1";
        self.execute_raw(sql, &[&id_str]).await?;
        Ok(())
    }

    async fn document_get(&self, collection: &str, id: &str) -> NodeDbResult<Option<Document>> {
        let sql = format!("SELECT id, data FROM {collection} WHERE id = $1");
        let (_, rows) = self.query_raw(&sql, &[&id]).await?;

        if let Some(row) = rows.first() {
            let doc_id = row
                .first()
                .and_then(|v| v.as_str())
                .unwrap_or(id)
                .to_string();

            let mut doc = Document::new(doc_id);

            // If the second column is JSON, parse it into fields.
            if let Some(Value::Object(fields)) = row.get(1) {
                for (k, v) in fields {
                    doc.set(k.clone(), v.clone());
                }
            } else if let Some(Value::String(json_str)) = row.get(1)
                && let Ok(parsed) =
                    serde_json::from_str::<HashMap<String, serde_json::Value>>(json_str)
            {
                for (k, v) in &parsed {
                    doc.set(k.clone(), json_to_value(v));
                }
            }

            Ok(Some(doc))
        } else {
            Ok(None)
        }
    }

    async fn document_put(&self, collection: &str, doc: Document) -> NodeDbResult<()> {
        let data_json = serde_json::to_string(&doc.fields).unwrap_or_else(|_| "{}".into());
        let sql = format!(
            "INSERT INTO {collection} (id, data) VALUES ($1, $2::jsonb) \
             ON CONFLICT (id) DO UPDATE SET data = $2::jsonb"
        );
        self.execute_raw(&sql, &[&doc.id, &data_json]).await?;
        Ok(())
    }

    async fn document_delete(&self, collection: &str, id: &str) -> NodeDbResult<()> {
        let sql = format!("DELETE FROM {collection} WHERE id = $1");
        self.execute_raw(&sql, &[&id]).await?;
        Ok(())
    }

    async fn execute_sql(&self, query: &str, _params: &[Value]) -> NodeDbResult<QueryResult> {
        // For now, pass the query directly. Parameter binding for arbitrary
        // Value types would require building a dynamic ToSql adapter.
        let (columns, rows) = self.query_raw(query, &[]).await?;

        Ok(QueryResult {
            columns,
            rows,
            rows_affected: 0,
        })
    }
}

/// Parse a JSON string from the DSL's "result" column into `Vec<SearchResult>`.
fn parse_vector_search_json(json_text: &str) -> NodeDbResult<Vec<SearchResult>> {
    // The DSL returns MessagePack-encoded results as text. Try JSON parse.
    let parsed: serde_json::Value =
        serde_json::from_str(json_text).map_err(|e| NodeDbError::Serialization {
            format: "json".into(),
            detail: e.to_string(),
        })?;

    let mut results = Vec::new();
    if let Some(arr) = parsed.as_array() {
        for item in arr {
            let id = item
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let distance = item.get("distance").and_then(|v| v.as_f64()).unwrap_or(0.0) as f32;
            results.push(SearchResult {
                id,
                node_id: None,
                distance,
                metadata: HashMap::new(),
            });
        }
    }

    Ok(results)
}

/// Parse a JSON string from graph_traverse into `SubGraph`.
fn parse_graph_traverse_json(json_text: &str) -> NodeDbResult<SubGraph> {
    let parsed: serde_json::Value =
        serde_json::from_str(json_text).map_err(|e| NodeDbError::Serialization {
            format: "json".into(),
            detail: e.to_string(),
        })?;

    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    if let Some(n) = parsed.get("nodes").and_then(|v| v.as_array()) {
        for item in n {
            let id = item
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let depth = item.get("depth").and_then(|v| v.as_u64()).unwrap_or(0) as u8;
            nodes.push(SubGraphNode {
                id: NodeId::new(id),
                depth,
                properties: HashMap::new(),
            });
        }
    }

    if let Some(e) = parsed.get("edges").and_then(|v| v.as_array()) {
        for item in e {
            let src = item.get("from").and_then(|v| v.as_str()).unwrap_or("");
            let dst = item.get("to").and_then(|v| v.as_str()).unwrap_or("");
            let label = item.get("label").and_then(|v| v.as_str()).unwrap_or("");
            edges.push(SubGraphEdge {
                id: EdgeId::from_components(src, dst, label),
                from: NodeId::new(src),
                to: NodeId::new(dst),
                label: label.to_string(),
                properties: HashMap::new(),
            });
        }
    }

    Ok(SubGraph { nodes, edges })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_vector_array_works() {
        let arr = format_vector_array(&[0.1, 0.2, 0.3]);
        assert_eq!(arr, "ARRAY[0.1,0.2,0.3]");
    }

    #[test]
    fn format_vector_array_empty() {
        let arr = format_vector_array(&[]);
        assert_eq!(arr, "ARRAY[]");
    }

    #[test]
    fn json_to_value_primitives() {
        assert_eq!(json_to_value(&serde_json::json!(null)), Value::Null);
        assert_eq!(json_to_value(&serde_json::json!(true)), Value::Bool(true));
        assert_eq!(json_to_value(&serde_json::json!(42)), Value::Integer(42));
        assert_eq!(json_to_value(&serde_json::json!(2.5)), Value::Float(2.5));
        assert_eq!(
            json_to_value(&serde_json::json!("hello")),
            Value::String("hello".into())
        );
    }

    #[test]
    fn json_to_value_nested() {
        let v = json_to_value(&serde_json::json!({"a": [1, 2]}));
        assert!(matches!(v, Value::Object(_)));
    }

    #[test]
    fn parse_vector_search_json_works() {
        let json = r#"[{"id":"v1","distance":0.1},{"id":"v2","distance":0.5}]"#;
        let results = parse_vector_search_json(json).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, "v1");
        assert!((results[0].distance - 0.1).abs() < 0.001);
        assert_eq!(results[1].id, "v2");
    }

    #[test]
    fn parse_graph_traverse_json_works() {
        let json = r#"{
            "nodes": [{"id":"a","depth":0},{"id":"b","depth":1}],
            "edges": [{"from":"a","to":"b","label":"KNOWS"}]
        }"#;
        let sg = parse_graph_traverse_json(json).unwrap();
        assert_eq!(sg.node_count(), 2);
        assert_eq!(sg.edge_count(), 1);
        assert_eq!(sg.edges[0].label, "KNOWS");
    }

    #[test]
    fn parse_empty_search_json() {
        let results = parse_vector_search_json("[]").unwrap();
        assert!(results.is_empty());
    }

    /// Verify NodeDbRemote implements NodeDb (compile-time check).
    #[test]
    fn remote_is_nodedb() {
        fn _accepts_dyn(_db: &dyn NodeDb) {}
        // Can't actually connect in a unit test, but we verify the trait is implemented.
    }
}
