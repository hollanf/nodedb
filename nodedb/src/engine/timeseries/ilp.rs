//! InfluxDB Line Protocol v2 parser (write path only).
//!
//! Parses lines of the form:
//! `measurement,tag1=val1,tag2=val2 field1=1.0,field2="str" timestamp_ns`
//!
//! No Flux query language — queries go through standard SQL/pgwire.
//!
//! Zero-copy: borrows from the input string where possible.

/// A parsed ILP line.
#[derive(Debug, PartialEq)]
pub struct IlpLine<'a> {
    /// Measurement name (the "table" or "collection").
    pub measurement: &'a str,
    /// Tag key-value pairs (sorted by key for consistent hashing).
    pub tags: Vec<(&'a str, &'a str)>,
    /// Field key-value pairs.
    pub fields: Vec<(&'a str, FieldValue<'a>)>,
    /// Timestamp in nanoseconds (None = server-assigned).
    pub timestamp_ns: Option<i64>,
}

/// Field value types in ILP.
#[derive(Debug, PartialEq)]
pub enum FieldValue<'a> {
    Float(f64),
    Int(i64),
    UInt(u64),
    Str(&'a str),
    Bool(bool),
}

/// Parse errors.
#[derive(Debug, PartialEq)]
pub enum IlpError {
    /// Empty or whitespace-only line.
    EmptyLine,
    /// Missing measurement name.
    MissingMeasurement,
    /// No fields found (at least one field is required).
    MissingFields,
    /// Invalid field value.
    InvalidFieldValue(String),
    /// Invalid timestamp.
    InvalidTimestamp(String),
    /// Malformed tag.
    InvalidTag(String),
}

impl std::fmt::Display for IlpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyLine => write!(f, "empty line"),
            Self::MissingMeasurement => write!(f, "missing measurement name"),
            Self::MissingFields => write!(f, "no fields found"),
            Self::InvalidFieldValue(s) => write!(f, "invalid field value: {s}"),
            Self::InvalidTimestamp(s) => write!(f, "invalid timestamp: {s}"),
            Self::InvalidTag(s) => write!(f, "invalid tag: {s}"),
        }
    }
}

/// Parsed measurement name and sorted tag list.
type MeasurementTags<'a> = (&'a str, Vec<(&'a str, &'a str)>);

/// Parse a single ILP line.
///
/// Format: `measurement[,tag=val]* field=val[,field=val]* [timestamp]`
pub fn parse_line(line: &str) -> Result<IlpLine<'_>, IlpError> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return Err(IlpError::EmptyLine);
    }

    // Split into: measurement+tags, fields, optional timestamp.
    // First space separates measurement+tags from fields.
    let first_space = line.find(' ').ok_or(IlpError::MissingFields)?;
    let measurement_tags = &line[..first_space];
    let rest = line[first_space + 1..].trim_start();

    // Parse measurement and tags.
    let (measurement, tags) = parse_measurement_tags(measurement_tags)?;

    // Split fields from optional timestamp (last space).
    let (fields_str, timestamp_ns) = if let Some(last_space) = rest.rfind(' ') {
        let maybe_ts = &rest[last_space + 1..];
        match maybe_ts.parse::<i64>() {
            Ok(ts) => (&rest[..last_space], Some(ts)),
            Err(_) => (rest, None), // Not a valid timestamp — treat all as fields.
        }
    } else {
        (rest, None)
    };

    let fields = parse_fields(fields_str)?;
    if fields.is_empty() {
        return Err(IlpError::MissingFields);
    }

    Ok(IlpLine {
        measurement,
        tags,
        fields,
        timestamp_ns,
    })
}

/// Parse a batch of ILP lines. Skips empty lines and comments.
pub fn parse_batch(input: &str) -> Vec<Result<IlpLine<'_>, IlpError>> {
    input
        .lines()
        .filter(|l| !l.trim().is_empty() && !l.trim().starts_with('#'))
        .map(parse_line)
        .collect()
}

fn parse_measurement_tags(s: &str) -> Result<MeasurementTags<'_>, IlpError> {
    let mut parts = s.splitn(2, ',');
    let measurement = parts.next().ok_or(IlpError::MissingMeasurement)?;
    if measurement.is_empty() {
        return Err(IlpError::MissingMeasurement);
    }

    let mut tags = Vec::new();
    if let Some(tag_str) = parts.next() {
        for tag in tag_str.split(',') {
            if tag.is_empty() {
                continue;
            }
            let eq = tag
                .find('=')
                .ok_or_else(|| IlpError::InvalidTag(tag.to_string()))?;
            let key = &tag[..eq];
            let val = &tag[eq + 1..];
            if key.is_empty() {
                return Err(IlpError::InvalidTag(tag.to_string()));
            }
            tags.push((key, val));
        }
        tags.sort_by_key(|(k, _)| *k);
    }

    Ok((measurement, tags))
}

fn parse_fields<'a>(s: &'a str) -> Result<Vec<(&'a str, FieldValue<'a>)>, IlpError> {
    let mut fields = Vec::new();
    for field in s.split(',') {
        let field = field.trim();
        if field.is_empty() {
            continue;
        }
        let eq = field
            .find('=')
            .ok_or_else(|| IlpError::InvalidFieldValue(field.to_string()))?;
        let key = &field[..eq];
        let val_str = &field[eq + 1..];
        if key.is_empty() {
            return Err(IlpError::InvalidFieldValue(field.to_string()));
        }
        let value = parse_field_value(val_str)?;
        fields.push((key, value));
    }
    Ok(fields)
}

fn parse_field_value(s: &str) -> Result<FieldValue<'_>, IlpError> {
    if s.is_empty() {
        return Err(IlpError::InvalidFieldValue("empty value".into()));
    }

    // String: "..."
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        return Ok(FieldValue::Str(&s[1..s.len() - 1]));
    }

    // Bool.
    match s {
        "t" | "T" | "true" | "True" | "TRUE" => return Ok(FieldValue::Bool(true)),
        "f" | "F" | "false" | "False" | "FALSE" => return Ok(FieldValue::Bool(false)),
        _ => {}
    }

    // Integer: ends with 'i'.
    if let Some(num) = s.strip_suffix('i') {
        return num
            .parse::<i64>()
            .map(FieldValue::Int)
            .map_err(|e| IlpError::InvalidFieldValue(format!("{s}: {e}")));
    }

    // Unsigned integer: ends with 'u'.
    if let Some(num) = s.strip_suffix('u') {
        return num
            .parse::<u64>()
            .map(FieldValue::UInt)
            .map_err(|e| IlpError::InvalidFieldValue(format!("{s}: {e}")));
    }

    // Float (default).
    s.parse::<f64>()
        .map(FieldValue::Float)
        .map_err(|e| IlpError::InvalidFieldValue(format!("{s}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_metric() {
        let line =
            parse_line("cpu,host=server01,region=us-west value=0.64 1434055562000000000").unwrap();
        assert_eq!(line.measurement, "cpu");
        assert_eq!(line.tags, vec![("host", "server01"), ("region", "us-west")]);
        assert_eq!(line.fields.len(), 1);
        assert_eq!(line.fields[0].0, "value");
        assert_eq!(line.fields[0].1, FieldValue::Float(0.64));
        assert_eq!(line.timestamp_ns, Some(1434055562000000000));
    }

    #[test]
    fn no_tags() {
        let line = parse_line("cpu value=0.64 1434055562000000000").unwrap();
        assert_eq!(line.measurement, "cpu");
        assert!(line.tags.is_empty());
    }

    #[test]
    fn no_timestamp() {
        let line = parse_line("cpu,host=a value=1.0").unwrap();
        assert_eq!(line.measurement, "cpu");
        assert_eq!(line.timestamp_ns, None);
    }

    #[test]
    fn multiple_fields() {
        let line = parse_line("weather temp=72.5,humidity=45i,sunny=true 1000").unwrap();
        assert_eq!(line.fields.len(), 3);
        assert_eq!(line.fields[0], ("temp", FieldValue::Float(72.5)));
        assert_eq!(line.fields[1], ("humidity", FieldValue::Int(45)));
        assert_eq!(line.fields[2], ("sunny", FieldValue::Bool(true)));
    }

    #[test]
    fn string_field() {
        let line = parse_line(r#"log,host=web01 message="hello world" 1000"#).unwrap();
        assert_eq!(line.fields[0], ("message", FieldValue::Str("hello world")));
    }

    #[test]
    fn unsigned_int() {
        let line = parse_line("disk bytes_free=1024u 1000").unwrap();
        assert_eq!(line.fields[0], ("bytes_free", FieldValue::UInt(1024)));
    }

    #[test]
    fn tags_sorted() {
        let line = parse_line("cpu,z=3,a=1,m=2 value=1.0 1000").unwrap();
        assert_eq!(line.tags, vec![("a", "1"), ("m", "2"), ("z", "3")]);
    }

    #[test]
    fn empty_line() {
        assert_eq!(parse_line("").unwrap_err(), IlpError::EmptyLine);
        assert_eq!(parse_line("  ").unwrap_err(), IlpError::EmptyLine);
    }

    #[test]
    fn comment_line() {
        assert_eq!(
            parse_line("# this is a comment").unwrap_err(),
            IlpError::EmptyLine
        );
    }

    #[test]
    fn missing_fields() {
        assert_eq!(parse_line("cpu").unwrap_err(), IlpError::MissingFields);
    }

    #[test]
    fn batch_parse() {
        let input = "cpu,host=a value=1.0 1000\n\
                     # comment\n\
                     mem,host=b used=512i 2000\n\
                     \n\
                     disk free=100.0 3000";
        let results = parse_batch(input);
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.is_ok()));
    }

    #[test]
    fn negative_values() {
        let line = parse_line("temp,loc=outside value=-12.5 1000").unwrap();
        assert_eq!(line.fields[0].1, FieldValue::Float(-12.5));
    }

    #[test]
    fn scientific_notation() {
        let line = parse_line("sensor reading=1.5e10 1000").unwrap();
        assert_eq!(line.fields[0].1, FieldValue::Float(1.5e10));
    }
}
