//! OTLP protobuf message types (subset for ingest).
//!
//! Covers: ExportMetricsServiceRequest, ExportTraceServiceRequest,
//! ExportLogsServiceRequest and their nested types.
//! Based on: <https://opentelemetry.io/docs/specs/otlp/>

// ── Common ───────────────────────────────────────────────────────────────

#[derive(Clone, PartialEq, prost::Message)]
pub struct KeyValue {
    #[prost(string, tag = "1")]
    pub key: String,
    #[prost(message, optional, tag = "2")]
    pub value: Option<AnyValue>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct AnyValue {
    #[prost(oneof = "any_value::Value", tags = "1, 2, 3, 4, 5, 6")]
    pub value: Option<any_value::Value>,
}

pub mod any_value {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Value {
        #[prost(string, tag = "1")]
        StringValue(String),
        #[prost(bool, tag = "2")]
        BoolValue(bool),
        #[prost(int64, tag = "3")]
        IntValue(i64),
        #[prost(double, tag = "4")]
        DoubleValue(f64),
        #[prost(message, tag = "5")]
        ArrayValue(super::ArrayValue),
        #[prost(message, tag = "6")]
        KvlistValue(super::KeyValueList),
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ArrayValue {
    #[prost(message, repeated, tag = "1")]
    pub values: Vec<AnyValue>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct KeyValueList {
    #[prost(message, repeated, tag = "1")]
    pub values: Vec<KeyValue>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Resource {
    #[prost(message, repeated, tag = "1")]
    pub attributes: Vec<KeyValue>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct InstrumentationScope {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub version: String,
}

// ── Metrics ──────────────────────────────────────────────────────────────

#[derive(Clone, PartialEq, prost::Message)]
pub struct ExportMetricsServiceRequest {
    #[prost(message, repeated, tag = "1")]
    pub resource_metrics: Vec<ResourceMetrics>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ExportMetricsServiceResponse {}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ResourceMetrics {
    #[prost(message, optional, tag = "1")]
    pub resource: Option<Resource>,
    #[prost(message, repeated, tag = "2")]
    pub scope_metrics: Vec<ScopeMetrics>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ScopeMetrics {
    #[prost(message, optional, tag = "1")]
    pub scope: Option<InstrumentationScope>,
    #[prost(message, repeated, tag = "2")]
    pub metrics: Vec<Metric>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Metric {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub description: String,
    #[prost(string, tag = "3")]
    pub unit: String,
    #[prost(oneof = "metric::Data", tags = "5, 7, 9, 10, 11")]
    pub data: Option<metric::Data>,
}

pub mod metric {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Data {
        #[prost(message, tag = "5")]
        Gauge(super::Gauge),
        #[prost(message, tag = "7")]
        Sum(super::Sum),
        #[prost(message, tag = "9")]
        Histogram(super::Histogram),
        #[prost(message, tag = "10")]
        ExponentialHistogram(super::ExponentialHistogram),
        #[prost(message, tag = "11")]
        Summary(super::Summary),
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Gauge {
    #[prost(message, repeated, tag = "1")]
    pub data_points: Vec<NumberDataPoint>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Sum {
    #[prost(message, repeated, tag = "1")]
    pub data_points: Vec<NumberDataPoint>,
    #[prost(bool, tag = "3")]
    pub is_monotonic: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct NumberDataPoint {
    #[prost(message, repeated, tag = "7")]
    pub attributes: Vec<KeyValue>,
    #[prost(fixed64, tag = "3")]
    pub time_unix_nano: u64,
    #[prost(oneof = "number_data_point::Value", tags = "4, 6")]
    pub value: Option<number_data_point::Value>,
}

pub mod number_data_point {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Value {
        #[prost(double, tag = "4")]
        AsDouble(f64),
        #[prost(sfixed64, tag = "6")]
        AsInt(i64),
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Histogram {
    #[prost(message, repeated, tag = "1")]
    pub data_points: Vec<HistogramDataPoint>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct HistogramDataPoint {
    #[prost(message, repeated, tag = "9")]
    pub attributes: Vec<KeyValue>,
    #[prost(fixed64, tag = "3")]
    pub time_unix_nano: u64,
    #[prost(fixed64, tag = "4")]
    pub count: u64,
    #[prost(double, optional, tag = "5")]
    pub sum: Option<f64>,
    #[prost(fixed64, repeated, tag = "6")]
    pub bucket_counts: Vec<u64>,
    #[prost(double, repeated, tag = "7")]
    pub explicit_bounds: Vec<f64>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ExponentialHistogram {
    #[prost(message, repeated, tag = "1")]
    pub data_points: Vec<ExponentialHistogramDataPoint>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ExponentialHistogramDataPoint {
    #[prost(message, repeated, tag = "1")]
    pub attributes: Vec<KeyValue>,
    #[prost(fixed64, tag = "3")]
    pub time_unix_nano: u64,
    #[prost(fixed64, tag = "4")]
    pub count: u64,
    #[prost(double, optional, tag = "5")]
    pub sum: Option<f64>,
    #[prost(sint32, tag = "6")]
    pub scale: i32,
    #[prost(message, optional, tag = "8")]
    pub positive: Option<ExponentialBuckets>,
    #[prost(message, optional, tag = "9")]
    pub negative: Option<ExponentialBuckets>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ExponentialBuckets {
    #[prost(sint32, tag = "1")]
    pub offset: i32,
    #[prost(uint64, repeated, tag = "2")]
    pub bucket_counts: Vec<u64>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Summary {
    #[prost(message, repeated, tag = "1")]
    pub data_points: Vec<SummaryDataPoint>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct SummaryDataPoint {
    #[prost(message, repeated, tag = "7")]
    pub attributes: Vec<KeyValue>,
    #[prost(fixed64, tag = "3")]
    pub time_unix_nano: u64,
    #[prost(fixed64, tag = "4")]
    pub count: u64,
    #[prost(double, tag = "5")]
    pub sum: f64,
}

// ── Traces ───────────────────────────────────────────────────────────────

#[derive(Clone, PartialEq, prost::Message)]
pub struct ExportTraceServiceRequest {
    #[prost(message, repeated, tag = "1")]
    pub resource_spans: Vec<ResourceSpans>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ExportTraceServiceResponse {}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ResourceSpans {
    #[prost(message, optional, tag = "1")]
    pub resource: Option<Resource>,
    #[prost(message, repeated, tag = "2")]
    pub scope_spans: Vec<ScopeSpans>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ScopeSpans {
    #[prost(message, optional, tag = "1")]
    pub scope: Option<InstrumentationScope>,
    #[prost(message, repeated, tag = "2")]
    pub spans: Vec<Span>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Span {
    #[prost(bytes = "vec", tag = "1")]
    pub trace_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub span_id: Vec<u8>,
    #[prost(string, tag = "4")]
    pub name: String,
    #[prost(enumeration = "SpanKind", tag = "6")]
    pub kind: i32,
    #[prost(fixed64, tag = "7")]
    pub start_time_unix_nano: u64,
    #[prost(fixed64, tag = "8")]
    pub end_time_unix_nano: u64,
    #[prost(message, repeated, tag = "9")]
    pub attributes: Vec<KeyValue>,
    #[prost(message, optional, tag = "15")]
    pub status: Option<SpanStatus>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, prost::Enumeration)]
#[repr(i32)]
pub enum SpanKind {
    Unspecified = 0,
    Internal = 1,
    Server = 2,
    Client = 3,
    Producer = 4,
    Consumer = 5,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct SpanStatus {
    #[prost(string, tag = "2")]
    pub message: String,
    #[prost(enumeration = "StatusCode", tag = "3")]
    pub code: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, prost::Enumeration)]
#[repr(i32)]
pub enum StatusCode {
    Unset = 0,
    Ok = 1,
    Error = 2,
}

// ── Logs ─────────────────────────────────────────────────────────────────

#[derive(Clone, PartialEq, prost::Message)]
pub struct ExportLogsServiceRequest {
    #[prost(message, repeated, tag = "1")]
    pub resource_logs: Vec<ResourceLogs>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ExportLogsServiceResponse {}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ResourceLogs {
    #[prost(message, optional, tag = "1")]
    pub resource: Option<Resource>,
    #[prost(message, repeated, tag = "2")]
    pub scope_logs: Vec<ScopeLogs>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ScopeLogs {
    #[prost(message, optional, tag = "1")]
    pub scope: Option<InstrumentationScope>,
    #[prost(message, repeated, tag = "2")]
    pub log_records: Vec<LogRecord>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct LogRecord {
    #[prost(fixed64, tag = "1")]
    pub time_unix_nano: u64,
    #[prost(enumeration = "SeverityNumber", tag = "2")]
    pub severity_number: i32,
    #[prost(string, tag = "3")]
    pub severity_text: String,
    #[prost(message, optional, tag = "5")]
    pub body: Option<AnyValue>,
    #[prost(message, repeated, tag = "6")]
    pub attributes: Vec<KeyValue>,
    #[prost(bytes = "vec", tag = "9")]
    pub trace_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "10")]
    pub span_id: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, prost::Enumeration)]
#[repr(i32)]
pub enum SeverityNumber {
    Unspecified = 0,
    Trace = 1,
    Debug = 5,
    Info = 9,
    Warn = 13,
    Error = 17,
    Fatal = 21,
}

// ── Helpers ──────────────────────────────────────────────────────────────

impl KeyValue {
    pub fn string_value(&self) -> &str {
        match &self.value {
            Some(AnyValue {
                value: Some(any_value::Value::StringValue(s)),
            }) => s.as_str(),
            _ => "",
        }
    }
}

impl NumberDataPoint {
    pub fn as_f64(&self) -> f64 {
        match &self.value {
            Some(number_data_point::Value::AsDouble(v)) => *v,
            Some(number_data_point::Value::AsInt(v)) => *v as f64,
            None => 0.0,
        }
    }

    pub fn timestamp_ms(&self) -> i64 {
        (self.time_unix_nano / 1_000_000) as i64
    }
}

/// Extract resource attributes as `tag=value` pairs for ILP.
pub fn resource_tags(resource: &Option<Resource>) -> Vec<(String, String)> {
    resource.as_ref().map_or_else(Vec::new, |r| {
        r.attributes
            .iter()
            .map(|kv| (kv.key.clone(), kv.string_value().to_string()))
            .collect()
    })
}
