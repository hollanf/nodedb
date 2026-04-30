//! ColumnType and ColumnDef — the atomic building blocks of typed schemas.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::value::Value;

/// Typed column definition for strict document and columnar collections.
///
/// `#[non_exhaustive]` — this enum grows with each type system expansion
/// (e.g. T3-07 adds `Decimal { precision, scale }`, T4-10 splits
/// `Timestamp`/`TimestampTz`). External exhaustive `match` arms must handle
/// future variants via a typed error arm rather than `_ => unreachable!()`.
#[non_exhaustive]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[serde(tag = "type", content = "params")]
pub enum ColumnType {
    Int64,
    Float64,
    String,
    Bool,
    Bytes,
    /// Naive (no-timezone) timestamp with microsecond precision. OID 1114.
    Timestamp,
    /// UTC (timezone-aware) timestamp with microsecond precision. OID 1184.
    Timestamptz,
    /// System-assigned timestamp (bitemporal `system_from_ms`). Same 8-byte
    /// layout as `Timestamp`, but tagged distinctly so the planner and DDL
    /// layer can reject user-supplied values — the column is populated by the
    /// engine from HLC at commit.
    SystemTimestamp,
    /// Arbitrary-precision decimal with explicit precision and scale.
    ///
    /// `precision`: total significant digits, 1–38. `scale`: digits after the
    /// decimal point, 0–precision. Default when unspecified: `{38, 10}`.
    Decimal {
        precision: u8,
        scale: u8,
    },
    Geometry,
    /// Fixed-dimension float32 vector.
    Vector(u32),
    Uuid,
    /// Arbitrary nested data stored as inline MessagePack.
    /// Variable-length. Accepts any Value type.
    Json,
    /// ULID: 16-byte Crockford Base32-encoded sortable ID.
    Ulid,
    /// Duration: signed microsecond precision (i64 internally).
    Duration,
    /// Ordered array of values. Variable-length, inline MessagePack.
    Array,
    /// Ordered set (auto-deduplicated). Variable-length, inline MessagePack.
    Set,
    /// Compiled regex pattern. Stored as string internally.
    Regex,
    /// Bounded range of values. Variable-length, inline MessagePack.
    Range,
    /// Typed reference to another record (`table:id`). Variable-length, inline MessagePack.
    Record,
}

impl ColumnType {
    /// Whether this type has a fixed byte size in Binary Tuple layout.
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            Self::Int64
            | Self::Float64
            | Self::Timestamp
            | Self::Timestamptz
            | Self::SystemTimestamp
            | Self::Duration => Some(8),
            Self::Bool => Some(1),
            Self::Decimal { .. } | Self::Uuid | Self::Ulid => Some(16),
            Self::Vector(dim) => Some(*dim as usize * 4),
            Self::String
            | Self::Bytes
            | Self::Geometry
            | Self::Json
            | Self::Array
            | Self::Set
            | Self::Regex
            | Self::Range
            | Self::Record => None,
        }
    }

    /// Whether this type is variable-length (requires offset table entry).
    pub fn is_variable_length(&self) -> bool {
        self.fixed_size().is_none()
    }

    /// Return the canonical PostgreSQL type OID for this column type.
    ///
    /// This is the single authoritative mapping between NodeDB `ColumnType`
    /// variants and PostgreSQL wire-protocol OIDs. All pgwire code must derive
    /// OIDs from this method — no local string-matching tables.
    ///
    /// Choices for non-native types:
    /// - `Geometry` → `25` (TEXT): no standard pg geometry OID; PostGIS uses
    ///   its own extension OID which we cannot claim. TEXT lets clients at least
    ///   see the WKT/WKB string.
    /// - `Vector(_)` → `1021` (FLOAT4_ARRAY): closest built-in pg type for a
    ///   fixed-dimension float32 vector; pgvector uses a custom OID, which we
    ///   avoid to stay dependency-free.
    /// - `Array`, `Set`, `Range`, `Record`, `Regex` → `114` (JSON): these are
    ///   variable-length MessagePack-encoded structures; JSON is the safest
    ///   generic text OID for clients that need to read the value as a string.
    pub fn to_pg_oid(&self) -> u32 {
        match self {
            Self::Bool => 16,
            Self::Bytes => 17,
            Self::Int64 => 20,
            Self::Float64 => 701,
            Self::String => 25,
            Self::Timestamp | Self::SystemTimestamp => 1114,
            Self::Timestamptz => 1184,
            Self::Decimal { .. } => 1700,
            Self::Uuid | Self::Ulid => 2950,
            Self::Json => 3802,
            Self::Duration => 1186,
            // No standard built-in OID for geometry; TEXT lets clients read WKT.
            Self::Geometry => 25,
            // FLOAT4_ARRAY (1021) is the closest built-in for fixed float32 vectors.
            Self::Vector(_) => 1021,
            // Variable-length structured types: expose as JSONB so clients can
            // parse the serialized representation.
            Self::Array | Self::Set | Self::Range | Self::Record | Self::Regex => 3802,
        }
    }

    /// Whether a `Value` is compatible with this column type.
    ///
    /// Accepts both native Value types (e.g., `Value::DateTime` for Timestamp)
    /// AND coercion sources from SQL input (e.g., `Value::String` for Timestamp).
    /// Null is accepted for any type — nullability is enforced at schema level.
    pub fn accepts(&self, value: &Value) -> bool {
        matches!(
            (self, value),
            (Self::Int64, Value::Integer(_))
                | (Self::Float64, Value::Float(_) | Value::Integer(_))
                | (Self::String, Value::String(_))
                | (Self::Bool, Value::Bool(_))
                | (Self::Bytes, Value::Bytes(_))
                | (
                    Self::Timestamp,
                    Value::NaiveDateTime(_) | Value::Integer(_) | Value::String(_)
                )
                | (
                    Self::Timestamptz,
                    Value::DateTime(_) | Value::Integer(_) | Value::String(_)
                )
                | (
                    Self::SystemTimestamp,
                    Value::DateTime(_) | Value::Integer(_)
                )
                | (
                    Self::Decimal { .. },
                    Value::Decimal(_) | Value::String(_) | Value::Float(_) | Value::Integer(_)
                )
                | (Self::Geometry, Value::Geometry(_) | Value::String(_))
                | (Self::Vector(_), Value::Array(_) | Value::Bytes(_))
                | (Self::Uuid, Value::Uuid(_) | Value::String(_))
                | (Self::Ulid, Value::Ulid(_) | Value::String(_))
                | (
                    Self::Duration,
                    Value::Duration(_) | Value::Integer(_) | Value::String(_)
                )
                | (Self::Array, Value::Array(_))
                | (Self::Set, Value::Set(_) | Value::Array(_))
                | (Self::Regex, Value::Regex(_) | Value::String(_))
                | (Self::Range, Value::Range { .. })
                | (Self::Record, Value::Record { .. } | Value::String(_))
                | (Self::Json, _)
                | (_, Value::Null)
        )
    }
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int64 => f.write_str("BIGINT"),
            Self::Float64 => f.write_str("FLOAT64"),
            Self::String => f.write_str("TEXT"),
            Self::Bool => f.write_str("BOOL"),
            Self::Bytes => f.write_str("BYTES"),
            Self::Timestamp => f.write_str("TIMESTAMP"),
            Self::Timestamptz => f.write_str("TIMESTAMPTZ"),
            Self::SystemTimestamp => f.write_str("SYSTEM_TIMESTAMP"),
            Self::Decimal { precision, scale } => write!(f, "DECIMAL({precision},{scale})"),
            Self::Geometry => f.write_str("GEOMETRY"),
            Self::Vector(dim) => write!(f, "VECTOR({dim})"),
            Self::Uuid => f.write_str("UUID"),
            Self::Json => f.write_str("JSON"),
            Self::Ulid => f.write_str("ULID"),
            Self::Duration => f.write_str("DURATION"),
            Self::Array => f.write_str("ARRAY"),
            Self::Set => f.write_str("SET"),
            Self::Regex => f.write_str("REGEX"),
            Self::Range => f.write_str("RANGE"),
            Self::Record => f.write_str("RECORD"),
        }
    }
}

impl FromStr for ColumnType {
    type Err = ColumnTypeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let upper = s.trim().to_uppercase();

        // NUMERIC(p,s) / DECIMAL(p,s) special case.
        if upper.starts_with("NUMERIC") || upper.starts_with("DECIMAL") {
            let base = if upper.starts_with("NUMERIC") {
                "NUMERIC"
            } else {
                "DECIMAL"
            };
            let rest = upper[base.len()..].trim();
            if rest.is_empty() {
                // Bare NUMERIC / DECIMAL → defaults.
                return Ok(Self::Decimal {
                    precision: 38,
                    scale: 10,
                });
            }
            if rest.starts_with('(') && rest.ends_with(')') {
                let inner = &rest[1..rest.len() - 1];
                let parts: Vec<&str> = inner.splitn(2, ',').collect();
                let precision: u8 = parts[0]
                    .trim()
                    .parse()
                    .map_err(|_| ColumnTypeParseError::InvalidDecimalParams(rest.to_string()))?;
                let scale: u8 = parts
                    .get(1)
                    .map(|p| p.trim())
                    .unwrap_or("0")
                    .parse()
                    .map_err(|_| ColumnTypeParseError::InvalidDecimalParams(rest.to_string()))?;
                if precision == 0 || precision > 38 {
                    return Err(ColumnTypeParseError::InvalidDecimalParams(format!(
                        "precision {precision} out of range 1-38"
                    )));
                }
                if scale > precision {
                    return Err(ColumnTypeParseError::InvalidDecimalParams(format!(
                        "scale {scale} must be <= precision {precision}"
                    )));
                }
                return Ok(Self::Decimal { precision, scale });
            }
            return Err(ColumnTypeParseError::InvalidDecimalParams(rest.to_string()));
        }

        // VECTOR(N) special case.
        if upper.starts_with("VECTOR") {
            let inner = upper
                .trim_start_matches("VECTOR")
                .trim()
                .trim_start_matches('(')
                .trim_end_matches(')')
                .trim();
            if inner.is_empty() {
                return Err(ColumnTypeParseError::InvalidVectorDim("empty".into()));
            }
            let dim: u32 = inner
                .parse()
                .map_err(|_| ColumnTypeParseError::InvalidVectorDim(inner.into()))?;
            if dim == 0 {
                return Err(ColumnTypeParseError::InvalidVectorDim("0".into()));
            }
            return Ok(Self::Vector(dim));
        }

        match upper.as_str() {
            "BIGINT" | "INT64" | "INTEGER" | "INT" => Ok(Self::Int64),
            "FLOAT64" | "DOUBLE" | "REAL" | "FLOAT" => Ok(Self::Float64),
            "TEXT" | "STRING" | "VARCHAR" => Ok(Self::String),
            "BOOL" | "BOOLEAN" => Ok(Self::Bool),
            "BYTES" | "BYTEA" | "BLOB" => Ok(Self::Bytes),
            "TIMESTAMP" => Ok(Self::Timestamp),
            "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => Ok(Self::Timestamptz),
            "SYSTEM_TIMESTAMP" | "SYSTEMTIMESTAMP" => Ok(Self::SystemTimestamp),
            "GEOMETRY" => Ok(Self::Geometry),
            "UUID" => Ok(Self::Uuid),
            "JSON" | "JSONB" => Ok(Self::Json),
            "ULID" => Ok(Self::Ulid),
            "DURATION" => Ok(Self::Duration),
            "ARRAY" => Ok(Self::Array),
            "SET" => Ok(Self::Set),
            "REGEX" => Ok(Self::Regex),
            "RANGE" => Ok(Self::Range),
            "RECORD" => Ok(Self::Record),
            "DATETIME" => Err(ColumnTypeParseError::UseTimestamp),
            other => Err(ColumnTypeParseError::Unknown(other.to_string())),
        }
    }
}

/// Error from parsing a column type string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ColumnTypeParseError {
    #[error("unknown column type: '{0}'")]
    Unknown(String),
    #[error("'DATETIME' is not a valid type — use 'TIMESTAMP' instead")]
    UseTimestamp,
    #[error("invalid VECTOR dimension: '{0}' (must be a positive integer)")]
    InvalidVectorDim(String),
    #[error(
        "invalid DECIMAL/NUMERIC params: '{0}' (expected DECIMAL(precision, scale) with precision 1-38 and scale <= precision)"
    )]
    InvalidDecimalParams(String),
}

/// Column-level modifiers that designate special engine roles.
///
/// These tell the engine which column serves a specialized purpose.
/// Extensible for future column roles (e.g., `PartitionKey`, `SortKey`).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
#[repr(u8)]
pub enum ColumnModifier {
    /// This column is the time-partitioning key (timeseries profile).
    /// Exactly one required for timeseries collections.
    TimeKey = 0,
    /// This column has an automatic R-tree spatial index (spatial profile).
    /// Exactly one required for spatial collections.
    SpatialIndex = 1,
}

/// A single column definition in a strict document or columnar schema.
///
/// `#[non_exhaustive]` — new fields may be added (e.g. column-level
/// compression hints, foreign-key metadata).
#[non_exhaustive]
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ColumnDef {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
    pub default: Option<String>,
    pub primary_key: bool,
    /// Column-level modifiers (TIME_KEY, SPATIAL_INDEX, etc.).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub modifiers: Vec<ColumnModifier>,
    /// GENERATED ALWAYS AS expression (serialized SqlExpr JSON).
    /// When set, this column is computed at write time, not supplied by the user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated_expr: Option<String>,
    /// Column names this generated column depends on.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub generated_deps: Vec<String>,
    /// Schema version at which this column was added. Original columns have
    /// version 1 (the default). Columns added via `ALTER ADD COLUMN` record
    /// the schema version after the bump so the reader can build a physical
    /// sub-schema for tuples written under older versions.
    #[serde(default = "default_added_at_version")]
    pub added_at_version: u32,
}

fn default_added_at_version() -> u32 {
    1
}

impl ColumnDef {
    pub fn required(name: impl Into<String>, column_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            column_type,
            nullable: false,
            default: None,
            primary_key: false,
            modifiers: Vec::new(),
            generated_expr: None,
            generated_deps: Vec::new(),
            added_at_version: 1,
        }
    }

    pub fn nullable(name: impl Into<String>, column_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            column_type,
            nullable: true,
            default: None,
            primary_key: false,
            modifiers: Vec::new(),
            generated_expr: None,
            generated_deps: Vec::new(),
            added_at_version: 1,
        }
    }

    pub fn with_primary_key(mut self) -> Self {
        self.primary_key = true;
        self.nullable = false;
        self
    }

    /// Check if this column has the TIME_KEY modifier.
    pub fn is_time_key(&self) -> bool {
        self.modifiers.contains(&ColumnModifier::TimeKey)
    }

    /// Check if this column has the SPATIAL_INDEX modifier.
    pub fn is_spatial_index(&self) -> bool {
        self.modifiers.contains(&ColumnModifier::SpatialIndex)
    }

    pub fn with_default(mut self, expr: impl Into<String>) -> Self {
        self.default = Some(expr.into());
        self
    }
}

impl fmt::Display for ColumnDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.column_type)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        if self.primary_key {
            write!(f, " PRIMARY KEY")?;
        }
        if let Some(ref d) = self.default {
            write!(f, " DEFAULT {d}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodedb_types_datetime_epoch() -> crate::datetime::NdbDateTime {
        crate::datetime::NdbDateTime::from_micros(0)
    }

    #[test]
    fn to_pg_oid_stable() {
        // Every variant must have a stable, non-zero OID.
        assert_eq!(ColumnType::Bool.to_pg_oid(), 16);
        assert_eq!(ColumnType::Bytes.to_pg_oid(), 17);
        assert_eq!(ColumnType::Int64.to_pg_oid(), 20);
        assert_eq!(ColumnType::String.to_pg_oid(), 25);
        assert_eq!(ColumnType::Float64.to_pg_oid(), 701);
        assert_eq!(ColumnType::Timestamp.to_pg_oid(), 1114);
        assert_eq!(ColumnType::Timestamptz.to_pg_oid(), 1184);
        assert_eq!(ColumnType::SystemTimestamp.to_pg_oid(), 1114);
        assert_eq!(ColumnType::Duration.to_pg_oid(), 1186);
        assert_eq!(
            ColumnType::Decimal {
                precision: 38,
                scale: 10
            }
            .to_pg_oid(),
            1700
        );
        assert_eq!(ColumnType::Uuid.to_pg_oid(), 2950);
        assert_eq!(ColumnType::Ulid.to_pg_oid(), 2950);
        assert_eq!(ColumnType::Json.to_pg_oid(), 3802);
        // Pragmatic choices.
        assert_eq!(ColumnType::Geometry.to_pg_oid(), 25); // TEXT
        assert_eq!(ColumnType::Vector(768).to_pg_oid(), 1021); // FLOAT4_ARRAY
        // Structured variable-length types → JSONB.
        assert_eq!(ColumnType::Array.to_pg_oid(), 3802);
        assert_eq!(ColumnType::Set.to_pg_oid(), 3802);
        assert_eq!(ColumnType::Range.to_pg_oid(), 3802);
        assert_eq!(ColumnType::Record.to_pg_oid(), 3802);
        assert_eq!(ColumnType::Regex.to_pg_oid(), 3802);
    }

    #[test]
    fn parse_system_timestamp() {
        assert_eq!(
            "SYSTEM_TIMESTAMP".parse::<ColumnType>().unwrap(),
            ColumnType::SystemTimestamp
        );
        assert_eq!(
            "SystemTimestamp".parse::<ColumnType>().unwrap(),
            ColumnType::SystemTimestamp
        );
        assert_eq!(ColumnType::SystemTimestamp.fixed_size(), Some(8));
        assert!(!ColumnType::SystemTimestamp.is_variable_length());
        assert_eq!(ColumnType::SystemTimestamp.to_string(), "SYSTEM_TIMESTAMP");
        // System timestamp rejects raw SQL string input — it's engine-assigned.
        assert!(!ColumnType::SystemTimestamp.accepts(&Value::String("2024-01-01".into())));
        assert!(ColumnType::SystemTimestamp.accepts(&Value::Integer(1_700_000_000)));
    }

    #[test]
    fn parse_canonical() {
        assert_eq!("BIGINT".parse::<ColumnType>().unwrap(), ColumnType::Int64);
        assert_eq!(
            "FLOAT64".parse::<ColumnType>().unwrap(),
            ColumnType::Float64
        );
        assert_eq!("TEXT".parse::<ColumnType>().unwrap(), ColumnType::String);
        assert_eq!("BOOL".parse::<ColumnType>().unwrap(), ColumnType::Bool);
        assert_eq!(
            "TIMESTAMP".parse::<ColumnType>().unwrap(),
            ColumnType::Timestamp
        );
        assert_eq!(
            "TIMESTAMPTZ".parse::<ColumnType>().unwrap(),
            ColumnType::Timestamptz
        );
        assert_eq!(
            "TIMESTAMP WITH TIME ZONE".parse::<ColumnType>().unwrap(),
            ColumnType::Timestamptz
        );
        assert_eq!(
            "GEOMETRY".parse::<ColumnType>().unwrap(),
            ColumnType::Geometry
        );
        assert_eq!("UUID".parse::<ColumnType>().unwrap(), ColumnType::Uuid);
    }

    #[test]
    fn parse_vector() {
        assert_eq!(
            "VECTOR(768)".parse::<ColumnType>().unwrap(),
            ColumnType::Vector(768)
        );
        assert!("VECTOR(0)".parse::<ColumnType>().is_err());
    }

    #[test]
    fn display_roundtrip() {
        for ct in [
            ColumnType::Int64,
            ColumnType::Float64,
            ColumnType::String,
            ColumnType::Timestamp,
            ColumnType::Timestamptz,
            ColumnType::Vector(768),
            ColumnType::Decimal {
                precision: 10,
                scale: 2,
            },
            ColumnType::Decimal {
                precision: 38,
                scale: 10,
            },
        ] {
            let s = ct.to_string();
            let parsed: ColumnType = s.parse().unwrap();
            assert_eq!(parsed, ct);
        }
    }

    #[test]
    fn decimal_parse_with_params() {
        assert_eq!(
            "NUMERIC(10,2)".parse::<ColumnType>().unwrap(),
            ColumnType::Decimal {
                precision: 10,
                scale: 2
            }
        );
        assert_eq!(
            "DECIMAL(38,10)".parse::<ColumnType>().unwrap(),
            ColumnType::Decimal {
                precision: 38,
                scale: 10
            }
        );
        // Bare forms → defaults.
        assert_eq!(
            "NUMERIC".parse::<ColumnType>().unwrap(),
            ColumnType::Decimal {
                precision: 38,
                scale: 10
            }
        );
        assert_eq!(
            "DECIMAL".parse::<ColumnType>().unwrap(),
            ColumnType::Decimal {
                precision: 38,
                scale: 10
            }
        );
    }

    #[test]
    fn decimal_parse_invalid() {
        // scale > precision.
        assert!("DECIMAL(5,6)".parse::<ColumnType>().is_err());
        // precision 0.
        assert!("DECIMAL(0,0)".parse::<ColumnType>().is_err());
        // precision > 38.
        assert!("DECIMAL(39,0)".parse::<ColumnType>().is_err());
    }

    #[test]
    fn decimal_fixed_size() {
        assert_eq!(
            ColumnType::Decimal {
                precision: 10,
                scale: 2
            }
            .fixed_size(),
            Some(16)
        );
    }

    #[test]
    fn decimal_to_pg_oid_is_1700() {
        assert_eq!(
            ColumnType::Decimal {
                precision: 10,
                scale: 2
            }
            .to_pg_oid(),
            1700
        );
    }

    #[test]
    fn accepts_native_values() {
        assert!(ColumnType::Int64.accepts(&Value::Integer(42)));
        assert!(ColumnType::Float64.accepts(&Value::Float(42.0)));
        assert!(ColumnType::Float64.accepts(&Value::Integer(42))); // coercion
        assert!(ColumnType::String.accepts(&Value::String("x".into())));
        assert!(ColumnType::Bool.accepts(&Value::Bool(true)));
        assert!(ColumnType::Bytes.accepts(&Value::Bytes(vec![1])));
        assert!(
            ColumnType::Uuid.accepts(&Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()))
        );
        assert!(
            ColumnType::Decimal {
                precision: 38,
                scale: 10
            }
            .accepts(&Value::Decimal(rust_decimal::Decimal::ZERO))
        );

        // Timestamp accepts NaiveDateTime; Timestamptz accepts DateTime.
        let naive = Value::NaiveDateTime(nodedb_types_datetime_epoch());
        let tz = Value::DateTime(nodedb_types_datetime_epoch());
        assert!(ColumnType::Timestamp.accepts(&naive));
        assert!(!ColumnType::Timestamp.accepts(&tz));
        assert!(ColumnType::Timestamptz.accepts(&tz));
        assert!(!ColumnType::Timestamptz.accepts(&naive));

        // Null accepted for any type.
        assert!(ColumnType::Int64.accepts(&Value::Null));

        // Wrong types rejected.
        assert!(!ColumnType::Int64.accepts(&Value::String("x".into())));
        assert!(!ColumnType::Bool.accepts(&Value::Integer(1)));
    }

    #[test]
    fn accepts_coercion_sources() {
        // SQL input coercion: strings for Timestamp, Uuid, Geometry, Decimal.
        assert!(ColumnType::Timestamp.accepts(&Value::String("2024-01-01".into())));
        assert!(ColumnType::Timestamp.accepts(&Value::Integer(1_700_000_000)));
        // Timestamptz accepts DateTime (TZ-aware).
        assert!(ColumnType::Timestamptz.accepts(&Value::String("2024-01-01T00:00:00Z".into())));
        assert!(ColumnType::Timestamptz.accepts(&Value::Integer(1_700_000_000)));
        assert!(ColumnType::Uuid.accepts(&Value::String(
            "550e8400-e29b-41d4-a716-446655440000".into()
        )));
        assert!(
            ColumnType::Decimal {
                precision: 10,
                scale: 2
            }
            .accepts(&Value::String("99.95".into()))
        );
        assert!(
            ColumnType::Decimal {
                precision: 10,
                scale: 2
            }
            .accepts(&Value::Float(99.95))
        );
        assert!(ColumnType::Geometry.accepts(&Value::String("POINT(0 0)".into())));
    }

    #[test]
    fn column_def_display() {
        let col = ColumnDef::required("id", ColumnType::Int64).with_primary_key();
        assert_eq!(col.to_string(), "id BIGINT NOT NULL PRIMARY KEY");
    }
}
