//! MessagePack encode/decode for [`ErrorDetails`].
//!
//! # Wire format
//!
//! Every variant encodes as a 2-element MessagePack array:
//!
//! ```text
//! [discriminant: u16, fields: map{u8 → value}]
//! ```
//!
//! Unit variants write an empty map (`fixmap(0)`).
//! Named-field variants write a `fixmap` or `map16` with consecutive `u8`
//! keys starting at 1.
//!
//! Unknown discriminants on decode return `ErrorDetails::Internal` rather than
//! an error, so old clients handle future variants gracefully.
//!
//! # Discriminant table (stable — never reorder)
//!
//! | Tag | Variant                   |
//! |----:|---------------------------|
//! |   1 | ConstraintViolation       |
//! |   2 | WriteConflict             |
//! |   3 | DeadlineExceeded          |
//! |   4 | PrevalidationRejected     |
//! |   5 | AppendOnlyViolation       |
//! |   6 | BalanceViolation          |
//! |   7 | PeriodLocked              |
//! |   8 | StateTransitionViolation  |
//! |   9 | TransitionCheckViolation  |
//! |  10 | TypeGuardViolation        |
//! |  11 | RetentionViolation        |
//! |  12 | LegalHoldActive           |
//! |  13 | TypeMismatch              |
//! |  14 | Overflow                  |
//! |  15 | InsufficientBalance       |
//! |  16 | RateExceeded              |
//! |  17 | CollectionNotFound        |
//! |  18 | DocumentNotFound          |
//! |  19 | CollectionDraining        |
//! |  20 | CollectionDeactivated     |
//! |  21 | PlanError                 |
//! |  22 | FanOutExceeded            |
//! |  23 | SqlNotEnabled             |
//! |  24 | AuthorizationDenied       |
//! |  25 | AuthExpired               |
//! |  26 | SyncConnectionFailed      |
//! |  27 | SyncDeltaRejected         |
//! |  28 | ShapeSubscriptionFailed   |
//! |  29 | Storage                   |
//! |  30 | SegmentCorrupted          |
//! |  31 | ColdStorage               |
//! |  32 | Wal                       |
//! |  33 | Serialization             |
//! |  34 | Codec                     |
//! |  35 | Config                    |
//! |  36 | BadRequest                |
//! |  37 | NoLeader                  |
//! |  38 | NotLeader                 |
//! |  39 | MigrationInProgress       |
//! |  40 | NodeUnreachable           |
//! |  41 | Cluster                   |
//! |  42 | MemoryExhausted           |
//! |  43 | Encryption                |
//! |  44 | Array                     |
//! |  45 | Bridge                    |
//! |  46 | Dispatch                  |
//! |  47 | Internal                  |
//! |  48 | UnsupportedOpcode         |

use zerompk::{FromMessagePack, Read, ToMessagePack, Write};

use crate::sync::compensation::CompensationHint;

use super::details::ErrorDetails;

// ── Discriminant constants ─────────────────────────────────────────────────

const TAG_CONSTRAINT_VIOLATION: u16 = 1;
const TAG_WRITE_CONFLICT: u16 = 2;
const TAG_DEADLINE_EXCEEDED: u16 = 3;
const TAG_PREVALIDATION_REJECTED: u16 = 4;
const TAG_APPEND_ONLY_VIOLATION: u16 = 5;
const TAG_BALANCE_VIOLATION: u16 = 6;
const TAG_PERIOD_LOCKED: u16 = 7;
const TAG_STATE_TRANSITION_VIOLATION: u16 = 8;
const TAG_TRANSITION_CHECK_VIOLATION: u16 = 9;
const TAG_TYPE_GUARD_VIOLATION: u16 = 10;
const TAG_RETENTION_VIOLATION: u16 = 11;
const TAG_LEGAL_HOLD_ACTIVE: u16 = 12;
const TAG_TYPE_MISMATCH: u16 = 13;
const TAG_OVERFLOW: u16 = 14;
const TAG_INSUFFICIENT_BALANCE: u16 = 15;
const TAG_RATE_EXCEEDED: u16 = 16;
const TAG_COLLECTION_NOT_FOUND: u16 = 17;
const TAG_DOCUMENT_NOT_FOUND: u16 = 18;
const TAG_COLLECTION_DRAINING: u16 = 19;
const TAG_COLLECTION_DEACTIVATED: u16 = 20;
const TAG_PLAN_ERROR: u16 = 21;
const TAG_FAN_OUT_EXCEEDED: u16 = 22;
const TAG_SQL_NOT_ENABLED: u16 = 23;
const TAG_AUTHORIZATION_DENIED: u16 = 24;
const TAG_AUTH_EXPIRED: u16 = 25;
const TAG_SYNC_CONNECTION_FAILED: u16 = 26;
const TAG_SYNC_DELTA_REJECTED: u16 = 27;
const TAG_SHAPE_SUBSCRIPTION_FAILED: u16 = 28;
const TAG_STORAGE: u16 = 29;
const TAG_SEGMENT_CORRUPTED: u16 = 30;
const TAG_COLD_STORAGE: u16 = 31;
const TAG_WAL: u16 = 32;
const TAG_SERIALIZATION: u16 = 33;
const TAG_CODEC: u16 = 34;
const TAG_CONFIG: u16 = 35;
const TAG_BAD_REQUEST: u16 = 36;
const TAG_NO_LEADER: u16 = 37;
const TAG_NOT_LEADER: u16 = 38;
const TAG_MIGRATION_IN_PROGRESS: u16 = 39;
const TAG_NODE_UNREACHABLE: u16 = 40;
const TAG_CLUSTER: u16 = 41;
const TAG_MEMORY_EXHAUSTED: u16 = 42;
const TAG_ENCRYPTION: u16 = 43;
const TAG_ARRAY: u16 = 44;
const TAG_BRIDGE: u16 = 45;
const TAG_DISPATCH: u16 = 46;
const TAG_INTERNAL: u16 = 47;
const TAG_UNSUPPORTED_OPCODE: u16 = 48;

// ── Encode helpers ─────────────────────────────────────────────────────────

/// Write a unit variant: `[tag, {}]`.
#[inline]
fn write_unit<W: Write>(writer: &mut W, tag: u16) -> zerompk::Result<()> {
    writer.write_array_len(2)?;
    writer.write_u16(tag)?;
    writer.write_map_len(0)
}

/// Write a 1-field variant: `[tag, {1: field1}]`.
#[inline]
fn write1<W, T>(writer: &mut W, tag: u16, v1: &T) -> zerompk::Result<()>
where
    W: Write,
    T: ToMessagePack,
{
    writer.write_array_len(2)?;
    writer.write_u16(tag)?;
    writer.write_map_len(1)?;
    writer.write_u8(1)?;
    v1.write(writer)
}

/// Write a 2-field variant: `[tag, {1: field1, 2: field2}]`.
#[inline]
fn write2<W, T1, T2>(writer: &mut W, tag: u16, v1: &T1, v2: &T2) -> zerompk::Result<()>
where
    W: Write,
    T1: ToMessagePack,
    T2: ToMessagePack,
{
    writer.write_array_len(2)?;
    writer.write_u16(tag)?;
    writer.write_map_len(2)?;
    writer.write_u8(1)?;
    v1.write(writer)?;
    writer.write_u8(2)?;
    v2.write(writer)
}

/// Write a 3-field variant: `[tag, {1: f1, 2: f2, 3: f3}]`.
#[inline]
fn write3<W, T1, T2, T3>(writer: &mut W, tag: u16, v1: &T1, v2: &T2, v3: &T3) -> zerompk::Result<()>
where
    W: Write,
    T1: ToMessagePack,
    T2: ToMessagePack,
    T3: ToMessagePack,
{
    writer.write_array_len(2)?;
    writer.write_u16(tag)?;
    writer.write_map_len(3)?;
    writer.write_u8(1)?;
    v1.write(writer)?;
    writer.write_u8(2)?;
    v2.write(writer)?;
    writer.write_u8(3)?;
    v3.write(writer)
}

// ── Decode helpers ─────────────────────────────────────────────────────────

/// Read the 2-element outer array and return `(tag, field_count)`.
#[inline]
fn read_header<'a, R: Read<'a>>(reader: &mut R) -> zerompk::Result<(u16, usize)> {
    let outer = reader.read_array_len()?;
    if outer != 2 {
        return Err(zerompk::Error::ArrayLengthMismatch {
            expected: 2,
            actual: outer,
        });
    }
    let tag = reader.read_u16()?;
    let field_count = reader.read_map_len()?;
    Ok((tag, field_count))
}

/// Skip all remaining fields in a variant payload map.
#[inline]
fn skip_fields<'a, R: Read<'a>>(reader: &mut R, count: usize) -> zerompk::Result<()> {
    for _ in 0..count {
        // Skip key (always a u8 in our format).
        reader.read_u8()?;
        // Skip value.
        reader.skip_value()?;
    }
    Ok(())
}

/// Skip one arbitrary MessagePack value.
fn skip_one<'a, R: Read<'a>>(reader: &mut R) -> zerompk::Result<()> {
    reader.skip_value()
}

// ── ToMessagePack impl ─────────────────────────────────────────────────────

impl ToMessagePack for ErrorDetails {
    fn write<W: Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        match self {
            ErrorDetails::ConstraintViolation { collection } => {
                write1(writer, TAG_CONSTRAINT_VIOLATION, collection)
            }
            ErrorDetails::WriteConflict {
                collection,
                document_id,
            } => write2(writer, TAG_WRITE_CONFLICT, collection, document_id),
            ErrorDetails::DeadlineExceeded => write_unit(writer, TAG_DEADLINE_EXCEEDED),
            ErrorDetails::PrevalidationRejected { constraint } => {
                write1(writer, TAG_PREVALIDATION_REJECTED, constraint)
            }
            ErrorDetails::AppendOnlyViolation { collection } => {
                write1(writer, TAG_APPEND_ONLY_VIOLATION, collection)
            }
            ErrorDetails::BalanceViolation { collection } => {
                write1(writer, TAG_BALANCE_VIOLATION, collection)
            }
            ErrorDetails::PeriodLocked { collection } => {
                write1(writer, TAG_PERIOD_LOCKED, collection)
            }
            ErrorDetails::StateTransitionViolation { collection } => {
                write1(writer, TAG_STATE_TRANSITION_VIOLATION, collection)
            }
            ErrorDetails::TransitionCheckViolation { collection } => {
                write1(writer, TAG_TRANSITION_CHECK_VIOLATION, collection)
            }
            ErrorDetails::TypeGuardViolation { collection } => {
                write1(writer, TAG_TYPE_GUARD_VIOLATION, collection)
            }
            ErrorDetails::RetentionViolation { collection } => {
                write1(writer, TAG_RETENTION_VIOLATION, collection)
            }
            ErrorDetails::LegalHoldActive { collection } => {
                write1(writer, TAG_LEGAL_HOLD_ACTIVE, collection)
            }
            ErrorDetails::TypeMismatch { collection } => {
                write1(writer, TAG_TYPE_MISMATCH, collection)
            }
            ErrorDetails::Overflow { collection } => write1(writer, TAG_OVERFLOW, collection),
            ErrorDetails::InsufficientBalance { collection } => {
                write1(writer, TAG_INSUFFICIENT_BALANCE, collection)
            }
            ErrorDetails::RateExceeded { gate } => write1(writer, TAG_RATE_EXCEEDED, gate),
            ErrorDetails::CollectionNotFound { collection } => {
                write1(writer, TAG_COLLECTION_NOT_FOUND, collection)
            }
            ErrorDetails::DocumentNotFound {
                collection,
                document_id,
            } => write2(writer, TAG_DOCUMENT_NOT_FOUND, collection, document_id),
            ErrorDetails::CollectionDraining { collection } => {
                write1(writer, TAG_COLLECTION_DRAINING, collection)
            }
            ErrorDetails::CollectionDeactivated {
                collection,
                retention_expires_at_ns,
                undrop_hint,
            } => write3(
                writer,
                TAG_COLLECTION_DEACTIVATED,
                collection,
                retention_expires_at_ns,
                undrop_hint,
            ),
            ErrorDetails::PlanError { phase, detail } => {
                write2(writer, TAG_PLAN_ERROR, phase, detail)
            }
            ErrorDetails::FanOutExceeded {
                shards_touched,
                limit,
            } => write2(writer, TAG_FAN_OUT_EXCEEDED, shards_touched, limit),
            ErrorDetails::SqlNotEnabled => write_unit(writer, TAG_SQL_NOT_ENABLED),
            ErrorDetails::AuthorizationDenied { resource } => {
                write1(writer, TAG_AUTHORIZATION_DENIED, resource)
            }
            ErrorDetails::AuthExpired => write_unit(writer, TAG_AUTH_EXPIRED),
            ErrorDetails::SyncConnectionFailed => write_unit(writer, TAG_SYNC_CONNECTION_FAILED),
            ErrorDetails::SyncDeltaRejected { compensation } => {
                writer.write_array_len(2)?;
                writer.write_u16(TAG_SYNC_DELTA_REJECTED)?;
                // `compensation` is `Option<CompensationHint>` — 1 field.
                writer.write_map_len(1)?;
                writer.write_u8(1)?;
                compensation.write(writer)
            }
            ErrorDetails::ShapeSubscriptionFailed { shape_id } => {
                write1(writer, TAG_SHAPE_SUBSCRIPTION_FAILED, shape_id)
            }
            ErrorDetails::Storage {
                component,
                op,
                detail,
            } => write3(writer, TAG_STORAGE, component, op, detail),
            ErrorDetails::SegmentCorrupted {
                segment_id,
                corruption,
                detail,
            } => write3(writer, TAG_SEGMENT_CORRUPTED, segment_id, corruption, detail),
            ErrorDetails::ColdStorage {
                backend,
                op,
                detail,
            } => write3(writer, TAG_COLD_STORAGE, backend, op, detail),
            ErrorDetails::Wal { stage, detail } => write2(writer, TAG_WAL, stage, detail),
            ErrorDetails::Serialization { format } => write1(writer, TAG_SERIALIZATION, format),
            ErrorDetails::Codec { codec, op, detail } => {
                write3(writer, TAG_CODEC, codec, op, detail)
            }
            ErrorDetails::Config => write_unit(writer, TAG_CONFIG),
            ErrorDetails::BadRequest => write_unit(writer, TAG_BAD_REQUEST),
            ErrorDetails::NoLeader => write_unit(writer, TAG_NO_LEADER),
            ErrorDetails::NotLeader { leader_addr } => write1(writer, TAG_NOT_LEADER, leader_addr),
            ErrorDetails::MigrationInProgress => write_unit(writer, TAG_MIGRATION_IN_PROGRESS),
            ErrorDetails::NodeUnreachable => write_unit(writer, TAG_NODE_UNREACHABLE),
            ErrorDetails::Cluster => write_unit(writer, TAG_CLUSTER),
            ErrorDetails::MemoryExhausted { engine } => {
                write1(writer, TAG_MEMORY_EXHAUSTED, engine)
            }
            ErrorDetails::Encryption { cipher, detail } => {
                write2(writer, TAG_ENCRYPTION, cipher, detail)
            }
            ErrorDetails::Array { array } => write1(writer, TAG_ARRAY, array),
            ErrorDetails::Bridge { plane, op, detail } => {
                write3(writer, TAG_BRIDGE, plane, op, detail)
            }
            ErrorDetails::Dispatch { stage, detail } => {
                write2(writer, TAG_DISPATCH, stage, detail)
            }
            ErrorDetails::UnsupportedOpcode { byte } => {
                write1(writer, TAG_UNSUPPORTED_OPCODE, byte)
            }
            ErrorDetails::Internal { component, detail } => {
                write2(writer, TAG_INTERNAL, component, detail)
            }
        }
    }
}

// ── FromMessagePack impl ───────────────────────────────────────────────────

impl<'a> FromMessagePack<'a> for ErrorDetails {
    fn read<R: Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let (tag, field_count) = read_header(reader)?;
        match tag {
            TAG_CONSTRAINT_VIOLATION => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::ConstraintViolation { collection })
            }
            TAG_WRITE_CONFLICT => {
                let (collection, document_id) = read2_str(reader, field_count)?;
                Ok(ErrorDetails::WriteConflict {
                    collection,
                    document_id,
                })
            }
            TAG_DEADLINE_EXCEEDED => {
                skip_fields(reader, field_count)?;
                Ok(ErrorDetails::DeadlineExceeded)
            }
            TAG_PREVALIDATION_REJECTED => {
                let (constraint,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::PrevalidationRejected { constraint })
            }
            TAG_APPEND_ONLY_VIOLATION => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::AppendOnlyViolation { collection })
            }
            TAG_BALANCE_VIOLATION => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::BalanceViolation { collection })
            }
            TAG_PERIOD_LOCKED => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::PeriodLocked { collection })
            }
            TAG_STATE_TRANSITION_VIOLATION => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::StateTransitionViolation { collection })
            }
            TAG_TRANSITION_CHECK_VIOLATION => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::TransitionCheckViolation { collection })
            }
            TAG_TYPE_GUARD_VIOLATION => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::TypeGuardViolation { collection })
            }
            TAG_RETENTION_VIOLATION => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::RetentionViolation { collection })
            }
            TAG_LEGAL_HOLD_ACTIVE => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::LegalHoldActive { collection })
            }
            TAG_TYPE_MISMATCH => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::TypeMismatch { collection })
            }
            TAG_OVERFLOW => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::Overflow { collection })
            }
            TAG_INSUFFICIENT_BALANCE => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::InsufficientBalance { collection })
            }
            TAG_RATE_EXCEEDED => {
                let (gate,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::RateExceeded { gate })
            }
            TAG_COLLECTION_NOT_FOUND => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::CollectionNotFound { collection })
            }
            TAG_DOCUMENT_NOT_FOUND => {
                let (collection, document_id) = read2_str(reader, field_count)?;
                Ok(ErrorDetails::DocumentNotFound {
                    collection,
                    document_id,
                })
            }
            TAG_COLLECTION_DRAINING => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::CollectionDraining { collection })
            }
            TAG_COLLECTION_DEACTIVATED => {
                let (collection, retention_expires_at_ns, undrop_hint) =
                    read_collection_deactivated(reader, field_count)?;
                Ok(ErrorDetails::CollectionDeactivated {
                    collection,
                    retention_expires_at_ns,
                    undrop_hint,
                })
            }
            TAG_PLAN_ERROR => {
                let (phase, detail) = read2_str_tolerant(reader, field_count)?;
                Ok(ErrorDetails::PlanError { phase, detail })
            }
            TAG_FAN_OUT_EXCEEDED => {
                let (shards_touched, limit) = read_fan_out(reader, field_count)?;
                Ok(ErrorDetails::FanOutExceeded {
                    shards_touched,
                    limit,
                })
            }
            TAG_SQL_NOT_ENABLED => {
                skip_fields(reader, field_count)?;
                Ok(ErrorDetails::SqlNotEnabled)
            }
            TAG_AUTHORIZATION_DENIED => {
                let (resource,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::AuthorizationDenied { resource })
            }
            TAG_AUTH_EXPIRED => {
                skip_fields(reader, field_count)?;
                Ok(ErrorDetails::AuthExpired)
            }
            TAG_SYNC_CONNECTION_FAILED => {
                skip_fields(reader, field_count)?;
                Ok(ErrorDetails::SyncConnectionFailed)
            }
            TAG_SYNC_DELTA_REJECTED => {
                let compensation = read_sync_delta_rejected(reader, field_count)?;
                Ok(ErrorDetails::SyncDeltaRejected { compensation })
            }
            TAG_SHAPE_SUBSCRIPTION_FAILED => {
                let (shape_id,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::ShapeSubscriptionFailed { shape_id })
            }
            TAG_STORAGE => {
                let (component, op, detail) = read3_str_tolerant(reader, field_count)?;
                Ok(ErrorDetails::Storage {
                    component,
                    op,
                    detail,
                })
            }
            TAG_SEGMENT_CORRUPTED => {
                let (segment_id, corruption, detail) =
                    read_segment_corrupted_tolerant(reader, field_count)?;
                Ok(ErrorDetails::SegmentCorrupted {
                    segment_id,
                    corruption,
                    detail,
                })
            }
            TAG_COLD_STORAGE => {
                let (backend, op, detail) = read3_str_tolerant(reader, field_count)?;
                Ok(ErrorDetails::ColdStorage {
                    backend,
                    op,
                    detail,
                })
            }
            TAG_WAL => {
                let (stage, detail) = read2_str_tolerant(reader, field_count)?;
                Ok(ErrorDetails::Wal { stage, detail })
            }
            TAG_SERIALIZATION => {
                let (format,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::Serialization { format })
            }
            TAG_CODEC => {
                let (codec, op, detail) = read3_str_tolerant(reader, field_count)?;
                Ok(ErrorDetails::Codec { codec, op, detail })
            }
            TAG_CONFIG => {
                skip_fields(reader, field_count)?;
                Ok(ErrorDetails::Config)
            }
            TAG_BAD_REQUEST => {
                skip_fields(reader, field_count)?;
                Ok(ErrorDetails::BadRequest)
            }
            TAG_NO_LEADER => {
                skip_fields(reader, field_count)?;
                Ok(ErrorDetails::NoLeader)
            }
            TAG_NOT_LEADER => {
                let (leader_addr,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::NotLeader { leader_addr })
            }
            TAG_MIGRATION_IN_PROGRESS => {
                skip_fields(reader, field_count)?;
                Ok(ErrorDetails::MigrationInProgress)
            }
            TAG_NODE_UNREACHABLE => {
                skip_fields(reader, field_count)?;
                Ok(ErrorDetails::NodeUnreachable)
            }
            TAG_CLUSTER => {
                skip_fields(reader, field_count)?;
                Ok(ErrorDetails::Cluster)
            }
            TAG_MEMORY_EXHAUSTED => {
                let (engine,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::MemoryExhausted { engine })
            }
            TAG_ENCRYPTION => {
                let (cipher, detail) = read2_str_tolerant(reader, field_count)?;
                Ok(ErrorDetails::Encryption { cipher, detail })
            }
            TAG_ARRAY => {
                let (array,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::Array { array })
            }
            TAG_BRIDGE => {
                let (plane, op, detail) = read3_str_tolerant(reader, field_count)?;
                Ok(ErrorDetails::Bridge { plane, op, detail })
            }
            TAG_DISPATCH => {
                let (stage, detail) = read2_str_tolerant(reader, field_count)?;
                Ok(ErrorDetails::Dispatch { stage, detail })
            }
            TAG_UNSUPPORTED_OPCODE => {
                let byte = read_u8_field(reader, field_count)?;
                Ok(ErrorDetails::UnsupportedOpcode { byte })
            }
            TAG_INTERNAL => {
                let (component, detail) = read2_str_tolerant(reader, field_count)?;
                Ok(ErrorDetails::Internal { component, detail })
            }
            // Unknown future variant — skip payload, treat as Internal.
            _unknown => {
                skip_fields(reader, field_count)?;
                Ok(ErrorDetails::Internal {
                    component: "unspecified".into(),
                    detail: "unspecified".into(),
                })
            }
        }
    }
}

// ── Read helpers ──────────────────────────────────────────────────────────

fn read_u8_field<'a, R: Read<'a>>(reader: &mut R, field_count: usize) -> zerompk::Result<u8> {
    if field_count < 1 {
        return Err(zerompk::Error::InvalidMarker(0));
    }
    let _k = reader.read_u8()?;
    let v = reader.read_u8()?;
    for _ in 1..field_count {
        reader.read_u8()?;
        skip_one(reader)?;
    }
    Ok(v)
}

fn read1_str<'a, R: Read<'a>>(reader: &mut R, field_count: usize) -> zerompk::Result<(String,)> {
    if field_count < 1 {
        return Err(zerompk::Error::InvalidMarker(0));
    }
    let _k = reader.read_u8()?;
    let v = reader.read_string()?.into_owned();
    // Skip any unknown extra fields.
    for _ in 1..field_count {
        reader.read_u8()?;
        skip_one(reader)?;
    }
    Ok((v,))
}

fn read2_str<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(String, String)> {
    if field_count < 2 {
        return Err(zerompk::Error::InvalidMarker(0));
    }
    let _k1 = reader.read_u8()?;
    let v1 = reader.read_string()?.into_owned();
    let _k2 = reader.read_u8()?;
    let v2 = reader.read_string()?.into_owned();
    for _ in 2..field_count {
        reader.read_u8()?;
        skip_one(reader)?;
    }
    Ok((v1, v2))
}

fn read_collection_deactivated<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(String, u64, String)> {
    if field_count < 3 {
        return Err(zerompk::Error::InvalidMarker(0));
    }
    let _k1 = reader.read_u8()?;
    let collection = reader.read_string()?.into_owned();
    let _k2 = reader.read_u8()?;
    let retention_expires_at_ns = reader.read_u64()?;
    let _k3 = reader.read_u8()?;
    let undrop_hint = reader.read_string()?.into_owned();
    for _ in 3..field_count {
        reader.read_u8()?;
        skip_one(reader)?;
    }
    Ok((collection, retention_expires_at_ns, undrop_hint))
}

fn read_fan_out<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(u16, u16)> {
    if field_count < 2 {
        return Err(zerompk::Error::InvalidMarker(0));
    }
    let _k1 = reader.read_u8()?;
    let shards_touched = reader.read_u16()?;
    let _k2 = reader.read_u8()?;
    let limit = reader.read_u16()?;
    for _ in 2..field_count {
        reader.read_u8()?;
        skip_one(reader)?;
    }
    Ok((shards_touched, limit))
}

/// Read 2 string fields, tolerating `field_count < 2` by filling missing
/// fields with `"unspecified"`.
fn read2_str_tolerant<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(String, String)> {
    let v1 = if field_count >= 1 {
        reader.read_u8()?;
        reader.read_string()?.into_owned()
    } else {
        "unspecified".into()
    };
    let v2 = if field_count >= 2 {
        reader.read_u8()?;
        reader.read_string()?.into_owned()
    } else {
        "unspecified".into()
    };
    for _ in 2..field_count {
        reader.read_u8()?;
        skip_one(reader)?;
    }
    Ok((v1, v2))
}

/// Read 3 string fields, tolerating `field_count < 3` by filling missing
/// fields with `"unspecified"`.
fn read3_str_tolerant<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(String, String, String)> {
    let v1 = if field_count >= 1 {
        reader.read_u8()?;
        reader.read_string()?.into_owned()
    } else {
        "unspecified".into()
    };
    let v2 = if field_count >= 2 {
        reader.read_u8()?;
        reader.read_string()?.into_owned()
    } else {
        "unspecified".into()
    };
    let v3 = if field_count >= 3 {
        reader.read_u8()?;
        reader.read_string()?.into_owned()
    } else {
        "unspecified".into()
    };
    for _ in 3..field_count {
        reader.read_u8()?;
        skip_one(reader)?;
    }
    Ok((v1, v2, v3))
}

/// Read `SegmentCorrupted` fields: (segment_id: u64, corruption: String, detail: String).
/// Tolerates `field_count < 3`; missing `segment_id` defaults to `0`.
fn read_segment_corrupted_tolerant<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(u64, String, String)> {
    let segment_id = if field_count >= 1 {
        reader.read_u8()?;
        reader.read_u64()?
    } else {
        0
    };
    let corruption = if field_count >= 2 {
        reader.read_u8()?;
        reader.read_string()?.into_owned()
    } else {
        "unspecified".into()
    };
    let detail = if field_count >= 3 {
        reader.read_u8()?;
        reader.read_string()?.into_owned()
    } else {
        "unspecified".into()
    };
    for _ in 3..field_count {
        reader.read_u8()?;
        skip_one(reader)?;
    }
    Ok((segment_id, corruption, detail))
}

fn read_sync_delta_rejected<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<Option<CompensationHint>> {
    if field_count < 1 {
        return Ok(None);
    }
    let _k = reader.read_u8()?;
    let compensation = Option::<CompensationHint>::read(reader)?;
    for _ in 1..field_count {
        reader.read_u8()?;
        skip_one(reader)?;
    }
    Ok(compensation)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(d: &ErrorDetails) -> ErrorDetails {
        let bytes = zerompk::to_msgpack_vec(d).expect("encode");
        zerompk::from_msgpack(&bytes).expect("decode")
    }

    #[test]
    fn unit_variant_roundtrip() {
        for v in [
            ErrorDetails::DeadlineExceeded,
            ErrorDetails::SqlNotEnabled,
            ErrorDetails::AuthExpired,
            ErrorDetails::SyncConnectionFailed,
            ErrorDetails::Config,
            ErrorDetails::BadRequest,
            ErrorDetails::NoLeader,
            ErrorDetails::MigrationInProgress,
            ErrorDetails::NodeUnreachable,
            ErrorDetails::Cluster,
        ] {
            assert_eq!(roundtrip(&v), v, "unit variant roundtrip failed: {v:?}");
        }
    }

    // ── 10 enriched variant roundtrip tests ───────────────────────────────────

    #[test]
    fn storage_enriched_roundtrip() {
        let v = ErrorDetails::Storage {
            component: "redb".into(),
            op: "write".into(),
            detail: "disk full".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn segment_corrupted_enriched_roundtrip() {
        let v = ErrorDetails::SegmentCorrupted {
            segment_id: 42,
            corruption: "crc_mismatch".into(),
            detail: "footer checksum invalid".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn cold_storage_enriched_roundtrip() {
        let v = ErrorDetails::ColdStorage {
            backend: "s3".into(),
            op: "get_object".into(),
            detail: "403 forbidden".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn wal_enriched_roundtrip() {
        let v = ErrorDetails::Wal {
            stage: "fsync".into(),
            detail: "io_uring submission failed".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn codec_enriched_roundtrip() {
        let v = ErrorDetails::Codec {
            codec: "alp".into(),
            op: "encode".into(),
            detail: "unsupported exponent range".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn encryption_enriched_roundtrip() {
        let v = ErrorDetails::Encryption {
            cipher: "aes_gcm".into(),
            detail: "authentication tag mismatch".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn plan_error_enriched_roundtrip() {
        let v = ErrorDetails::PlanError {
            phase: "logical".into(),
            detail: "ambiguous column reference".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn bridge_enriched_roundtrip() {
        let v = ErrorDetails::Bridge {
            plane: "data".into(),
            op: "dispatch".into(),
            detail: "ring buffer full".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn dispatch_enriched_roundtrip() {
        let v = ErrorDetails::Dispatch {
            stage: "route".into(),
            detail: "vshard not found".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn internal_enriched_roundtrip() {
        let v = ErrorDetails::Internal {
            component: "compaction".into(),
            detail: "unreachable state".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    // ── 10 backward-compat tests (old-style payload with field_count = 0) ─────

    /// Manually build a `[tag, {}]` payload (zero fields) and verify that
    /// decode fills all string fields with "unspecified" and segment_id with 0.
    fn decode_zero_fields(tag: u16) -> ErrorDetails {
        let mut buf = Vec::new();
        let mut w = zerompk::Writer::new(&mut buf);
        w.write_array_len(2).unwrap();
        w.write_u16(tag).unwrap();
        w.write_map_len(0).unwrap();
        zerompk::from_msgpack(&buf).expect("decode")
    }

    #[test]
    fn storage_compat_zero_fields() {
        let v = decode_zero_fields(TAG_STORAGE);
        assert_eq!(
            v,
            ErrorDetails::Storage {
                component: "unspecified".into(),
                op: "unspecified".into(),
                detail: "unspecified".into(),
            }
        );
    }

    #[test]
    fn segment_corrupted_compat_zero_fields() {
        let v = decode_zero_fields(TAG_SEGMENT_CORRUPTED);
        assert_eq!(
            v,
            ErrorDetails::SegmentCorrupted {
                segment_id: 0,
                corruption: "unspecified".into(),
                detail: "unspecified".into(),
            }
        );
    }

    #[test]
    fn cold_storage_compat_zero_fields() {
        let v = decode_zero_fields(TAG_COLD_STORAGE);
        assert_eq!(
            v,
            ErrorDetails::ColdStorage {
                backend: "unspecified".into(),
                op: "unspecified".into(),
                detail: "unspecified".into(),
            }
        );
    }

    #[test]
    fn wal_compat_zero_fields() {
        let v = decode_zero_fields(TAG_WAL);
        assert_eq!(
            v,
            ErrorDetails::Wal {
                stage: "unspecified".into(),
                detail: "unspecified".into(),
            }
        );
    }

    #[test]
    fn codec_compat_zero_fields() {
        let v = decode_zero_fields(TAG_CODEC);
        assert_eq!(
            v,
            ErrorDetails::Codec {
                codec: "unspecified".into(),
                op: "unspecified".into(),
                detail: "unspecified".into(),
            }
        );
    }

    #[test]
    fn encryption_compat_zero_fields() {
        let v = decode_zero_fields(TAG_ENCRYPTION);
        assert_eq!(
            v,
            ErrorDetails::Encryption {
                cipher: "unspecified".into(),
                detail: "unspecified".into(),
            }
        );
    }

    #[test]
    fn plan_error_compat_zero_fields() {
        let v = decode_zero_fields(TAG_PLAN_ERROR);
        assert_eq!(
            v,
            ErrorDetails::PlanError {
                phase: "unspecified".into(),
                detail: "unspecified".into(),
            }
        );
    }

    #[test]
    fn bridge_compat_zero_fields() {
        let v = decode_zero_fields(TAG_BRIDGE);
        assert_eq!(
            v,
            ErrorDetails::Bridge {
                plane: "unspecified".into(),
                op: "unspecified".into(),
                detail: "unspecified".into(),
            }
        );
    }

    #[test]
    fn dispatch_compat_zero_fields() {
        let v = decode_zero_fields(TAG_DISPATCH);
        assert_eq!(
            v,
            ErrorDetails::Dispatch {
                stage: "unspecified".into(),
                detail: "unspecified".into(),
            }
        );
    }

    #[test]
    fn internal_compat_zero_fields() {
        let v = decode_zero_fields(TAG_INTERNAL);
        assert_eq!(
            v,
            ErrorDetails::Internal {
                component: "unspecified".into(),
                detail: "unspecified".into(),
            }
        );
    }

    #[test]
    fn single_string_field_roundtrip() {
        let variants = vec![
            ErrorDetails::ConstraintViolation {
                collection: "orders".into(),
            },
            ErrorDetails::AppendOnlyViolation {
                collection: "ledger".into(),
            },
            ErrorDetails::CollectionNotFound {
                collection: "users".into(),
            },
            ErrorDetails::AuthorizationDenied {
                resource: "orders.*".into(),
            },
            ErrorDetails::MemoryExhausted {
                engine: "vector".into(),
            },
            ErrorDetails::Array {
                array: "arr1".into(),
            },
            ErrorDetails::NotLeader {
                leader_addr: "10.0.0.1:6432".into(),
            },
        ];
        for v in variants {
            assert_eq!(roundtrip(&v), v, "single-string roundtrip failed: {v:?}");
        }
    }

    #[test]
    fn two_string_field_roundtrip() {
        let v = ErrorDetails::WriteConflict {
            collection: "orders".into(),
            document_id: "ord-42".into(),
        };
        assert_eq!(roundtrip(&v), v);

        let v2 = ErrorDetails::DocumentNotFound {
            collection: "users".into(),
            document_id: "u-99".into(),
        };
        assert_eq!(roundtrip(&v2), v2);
    }

    #[test]
    fn collection_deactivated_roundtrip() {
        let v = ErrorDetails::CollectionDeactivated {
            collection: "old_logs".into(),
            retention_expires_at_ns: 1_700_000_000_000_u64,
            undrop_hint: "UNDROP COLLECTION old_logs".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn fan_out_exceeded_roundtrip() {
        let v = ErrorDetails::FanOutExceeded {
            shards_touched: 100,
            limit: 50,
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn sync_delta_rejected_with_hint_roundtrip() {
        let v = ErrorDetails::SyncDeltaRejected {
            compensation: Some(CompensationHint::UniqueViolation {
                field: "email".into(),
                conflicting_value: "a@b.com".into(),
            }),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn sync_delta_rejected_no_hint_roundtrip() {
        let v = ErrorDetails::SyncDeltaRejected { compensation: None };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn serialization_roundtrip() {
        let v = ErrorDetails::Serialization {
            format: "msgpack".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }
}
