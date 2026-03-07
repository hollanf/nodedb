use datafusion::arrow::array::ArrayRef;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::ColumnarValue;

/// Navigate a `serde_json::Value` by dot-separated path (e.g. `$.user.email` or `user.items[0]`).
pub(crate) fn navigate_json<'a>(
    value: &'a serde_json::Value,
    path: &str,
) -> Option<&'a serde_json::Value> {
    let path = path
        .strip_prefix("$.")
        .unwrap_or(path.strip_prefix('$').unwrap_or(path));

    if path.is_empty() {
        return Some(value);
    }

    let mut current = value;
    for segment in path.split('.') {
        if let Some(idx_start) = segment.find('[') {
            let field = &segment[..idx_start];
            if !field.is_empty() {
                current = current.get(field)?;
            }
            let idx_str = segment[idx_start + 1..].strip_suffix(']')?;
            let idx: usize = idx_str.parse().ok()?;
            current = current.get(idx)?;
        } else {
            current = current.get(segment)?;
        }
    }

    Some(current)
}

/// Navigate an `rmpv::Value` by dot-separated path (e.g. `$.user.email` or `user.items[0]`).
pub(crate) fn navigate_rmpv<'a>(value: &'a rmpv::Value, path: &str) -> Option<&'a rmpv::Value> {
    let path = path
        .strip_prefix("$.")
        .unwrap_or(path.strip_prefix('$').unwrap_or(path));

    if path.is_empty() {
        return Some(value);
    }

    let mut current = value;
    for segment in path.split('.') {
        if let Some(idx_start) = segment.find('[') {
            let field = &segment[..idx_start];
            if !field.is_empty() {
                current = msgpack_get_field(current, field)?;
            }
            let idx_str = segment[idx_start + 1..].strip_suffix(']')?;
            let idx: usize = idx_str.parse().ok()?;
            current = current.as_array()?.get(idx)?;
        } else {
            current = msgpack_get_field(current, segment)?;
        }
    }

    Some(current)
}

/// Look up a field in a MessagePack map by string key.
fn msgpack_get_field<'a>(value: &'a rmpv::Value, key: &str) -> Option<&'a rmpv::Value> {
    let map = value.as_map()?;
    for (k, v) in map {
        if let Some(k_str) = k.as_str() {
            if k_str == key {
                return Some(v);
            }
        }
    }
    None
}

/// Convert an `rmpv::Value` to its string representation.
pub(crate) fn rmpv_to_string(v: &rmpv::Value) -> String {
    match v {
        rmpv::Value::String(s) => s.as_str().unwrap_or("").to_string(),
        rmpv::Value::Boolean(b) => b.to_string(),
        rmpv::Value::Integer(i) => i.to_string(),
        rmpv::Value::F32(f) => f.to_string(),
        rmpv::Value::F64(f) => f.to_string(),
        rmpv::Value::Nil => String::new(),
        other => format!("{other}"),
    }
}

/// Expand a `ColumnarValue` (scalar or array) into an `ArrayRef` of `num_rows` length.
pub(crate) fn expand_to_array(col: &ColumnarValue, num_rows: usize) -> DfResult<ArrayRef> {
    match col {
        ColumnarValue::Array(a) => Ok(a.clone()),
        ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows),
    }
}

#[cfg(test)]
pub(crate) mod test_util {
    /// Convert a `serde_json::Value` to MessagePack bytes.
    pub fn to_msgpack(val: &serde_json::Value) -> Vec<u8> {
        let rmpv_val = json_to_rmpv(val);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &rmpv_val).unwrap();
        buf
    }

    /// Convert a `serde_json::Value` to an `rmpv::Value`.
    pub fn json_to_rmpv(val: &serde_json::Value) -> rmpv::Value {
        match val {
            serde_json::Value::Null => rmpv::Value::Nil,
            serde_json::Value::Bool(b) => rmpv::Value::Boolean(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    rmpv::Value::Integer(rmpv::Integer::from(i))
                } else {
                    rmpv::Value::F64(n.as_f64().unwrap())
                }
            }
            serde_json::Value::String(s) => rmpv::Value::String(rmpv::Utf8String::from(s.as_str())),
            serde_json::Value::Array(arr) => {
                rmpv::Value::Array(arr.iter().map(json_to_rmpv).collect())
            }
            serde_json::Value::Object(obj) => rmpv::Value::Map(
                obj.iter()
                    .map(|(k, v)| {
                        (
                            rmpv::Value::String(rmpv::Utf8String::from(k.as_str())),
                            json_to_rmpv(v),
                        )
                    })
                    .collect(),
            ),
        }
    }
}
