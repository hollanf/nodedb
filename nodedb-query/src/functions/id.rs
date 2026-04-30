//! ID generation and detection scalar functions.

use super::shared::{bool_id_check, num_arg};
use nodedb_types::Value;

pub(super) fn try_eval(name: &str, args: &[Value]) -> Option<Value> {
    let v = match name {
        "uuid" | "uuid_v4" | "gen_random_uuid" => Value::String(nodedb_types::id_gen::uuid_v4()),
        "uuid_v7" => Value::String(nodedb_types::id_gen::uuid_v7()),
        "ulid" => Value::String(nodedb_types::id_gen::ulid()),
        "cuid2" => Value::String(nodedb_types::id_gen::cuid2()),
        "nanoid" => {
            let len = num_arg(args, 0).map(|n| n as usize);
            match len {
                Some(l) => Value::String(nodedb_types::id_gen::nanoid_with_length(l)),
                None => Value::String(nodedb_types::id_gen::nanoid()),
            }
        }
        "is_uuid" => bool_id_check(args, nodedb_types::id_gen::is_uuid),
        "is_ulid" => bool_id_check(args, nodedb_types::id_gen::is_ulid),
        "is_cuid2" => bool_id_check(args, nodedb_types::id_gen::is_cuid2),
        "is_nanoid" => bool_id_check(args, nodedb_types::id_gen::is_nanoid),
        "id_type" => args
            .first()
            .and_then(|v| v.as_str())
            .map_or(Value::String("custom".into()), |s| {
                Value::String(nodedb_types::id_gen::detect_id_type(s).as_str().to_string())
            }),
        "uuid_version" => args
            .first()
            .and_then(|v| v.as_str())
            .map_or(Value::Integer(0), |s| {
                Value::Integer(nodedb_types::id_gen::uuid_version(s) as i64)
            }),
        "ulid_timestamp" => args
            .first()
            .and_then(|v| v.as_str())
            .and_then(nodedb_types::id_gen::ulid_timestamp_ms)
            .map_or(Value::Null, |ms| Value::Integer(ms as i64)),
        _ => return None,
    };
    Some(v)
}
