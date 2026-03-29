//! OpenTelemetry Protocol (OTLP) ingest and export.
//!
//! Ingest: HTTP receivers at `/v1/metrics`, `/v1/traces`, `/v1/logs`.
//! Export: Push NodeDB's own traces/metrics to an OTLP collector.

pub mod exporter;
pub mod grpc;
pub mod proto;
pub mod receiver;

pub use exporter::ExporterConfig;
pub use receiver::OtelConfig;
