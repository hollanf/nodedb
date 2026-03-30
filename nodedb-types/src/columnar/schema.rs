//! Strict document and columnar schemas with shared operations trait.

use serde::{Deserialize, Serialize};

use super::column_type::ColumnDef;
use crate::columnar::ColumnType;

/// Shared schema operations (eliminates duplication between Strict and Columnar).
pub trait SchemaOps {
    fn columns(&self) -> &[ColumnDef];

    fn column_index(&self, name: &str) -> Option<usize> {
        self.columns().iter().position(|c| c.name == name)
    }

    fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns().iter().find(|c| c.name == name)
    }

    fn primary_key_columns(&self) -> Vec<&ColumnDef> {
        self.columns().iter().filter(|c| c.primary_key).collect()
    }

    fn len(&self) -> usize {
        self.columns().len()
    }

    fn is_empty(&self) -> bool {
        self.columns().is_empty()
    }
}

/// Schema for a strict document collection (Binary Tuple serialization).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrictSchema {
    pub columns: Vec<ColumnDef>,
    pub version: u16,
}

/// Schema for a columnar collection (compressed segment files).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarSchema {
    pub columns: Vec<ColumnDef>,
    pub version: u16,
}

/// Schema validation errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaError {
    #[error("schema must have at least one column")]
    Empty,
    #[error("duplicate column name: '{0}'")]
    DuplicateColumn(String),
    #[error("VECTOR dimension must be positive, got 0 for column '{0}'")]
    ZeroVectorDim(String),
    #[error("primary key column '{0}' must be NOT NULL")]
    NullablePrimaryKey(String),
}

fn validate_columns(columns: &[ColumnDef]) -> Result<(), SchemaError> {
    if columns.is_empty() {
        return Err(SchemaError::Empty);
    }
    let mut seen = std::collections::HashSet::with_capacity(columns.len());
    for col in columns {
        if !seen.insert(&col.name) {
            return Err(SchemaError::DuplicateColumn(col.name.clone()));
        }
        if col.primary_key && col.nullable {
            return Err(SchemaError::NullablePrimaryKey(col.name.clone()));
        }
        if let ColumnType::Vector(0) = col.column_type {
            return Err(SchemaError::ZeroVectorDim(col.name.clone()));
        }
    }
    Ok(())
}

impl SchemaOps for StrictSchema {
    fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }
}

impl SchemaOps for ColumnarSchema {
    fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }
}

impl StrictSchema {
    pub fn new(columns: Vec<ColumnDef>) -> Result<Self, SchemaError> {
        validate_columns(&columns)?;
        Ok(Self {
            columns,
            version: 1,
        })
    }

    /// Count of variable-length columns (determines offset table size).
    pub fn variable_column_count(&self) -> usize {
        self.columns
            .iter()
            .filter(|c| c.column_type.is_variable_length())
            .count()
    }

    /// Total fixed-field byte size (for Binary Tuple layout computation).
    pub fn fixed_fields_size(&self) -> usize {
        self.columns
            .iter()
            .filter_map(|c| c.column_type.fixed_size())
            .sum()
    }

    /// Null bitmap size in bytes.
    pub fn null_bitmap_size(&self) -> usize {
        self.columns.len().div_ceil(8)
    }
}

impl ColumnarSchema {
    pub fn new(columns: Vec<ColumnDef>) -> Result<Self, SchemaError> {
        validate_columns(&columns)?;
        Ok(Self {
            columns,
            version: 1,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::ColumnType;

    #[test]
    fn strict_schema_validation() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("name", ColumnType::String),
        ]);
        assert!(schema.is_ok());
        assert!(StrictSchema::new(vec![]).is_err());
    }

    #[test]
    fn schema_ops_trait() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("name", ColumnType::String),
            ColumnDef::nullable("balance", ColumnType::Decimal),
        ])
        .unwrap();
        assert_eq!(schema.len(), 3);
        assert_eq!(schema.column_index("balance"), Some(2));
        assert!(schema.column("nonexistent").is_none());
        assert_eq!(schema.primary_key_columns().len(), 1);
    }

    #[test]
    fn strict_layout_helpers() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("name", ColumnType::String),
            ColumnDef::nullable("balance", ColumnType::Decimal),
            ColumnDef::nullable("bio", ColumnType::String),
        ])
        .unwrap();
        assert_eq!(schema.null_bitmap_size(), 1);
        assert_eq!(schema.fixed_fields_size(), 8 + 16);
        assert_eq!(schema.variable_column_count(), 2);
    }

    #[test]
    fn columnar_schema_validation() {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("time", ColumnType::Timestamp),
            ColumnDef::nullable("cpu", ColumnType::Float64),
        ]);
        assert!(schema.is_ok());
        assert_eq!(schema.unwrap().len(), 2);
    }

    #[test]
    fn nullable_pk_rejected() {
        let cols = vec![ColumnDef {
            name: "id".into(),
            column_type: ColumnType::Int64,
            nullable: true,
            default: None,
            primary_key: true,
            modifiers: Vec::new(),
        }];
        assert!(matches!(
            StrictSchema::new(cols),
            Err(SchemaError::NullablePrimaryKey(_))
        ));
    }

    #[test]
    fn zero_vector_dim_rejected() {
        let cols = vec![ColumnDef::required("emb", ColumnType::Vector(0))];
        assert!(matches!(
            StrictSchema::new(cols),
            Err(SchemaError::ZeroVectorDim(_))
        ));
    }
}
