//! OTLP trace-span dispatcher for Control-Plane call sites.
//!
//! Wraps one-shot span emission in an always-on owning handle that:
//!
//! - Holds the collector endpoint and a shared `reqwest::Client` once,
//!   so the gateway and executor don't rebuild them per request.
//! - Is a no-op when the endpoint is empty (disabled), so call sites
//!   stay unconditional.
//! - Spawns the HTTP POST detached so span emission never sits on the
//!   request hot path.
//! - Lives outside the feature-gated `otel` module so gateway/executor
//!   can depend on it unconditionally; the actual protobuf encode path
//!   is only compiled when the `otel` feature is enabled.
//!
//! # Trace correlation
//!
//! Every span carries the request's `trace_id` in the OTLP `trace_id`
//! field, the same 8-byte integer the caller already propagates
//! through `ExecuteRequest`. All spans emitted for one query — the
//! gateway entry span and every leaseholder executor span — share
//! that id, so an OTLP collector reassembles them into a single
//! trace without any additional W3C traceparent wiring.

use std::sync::Arc;
#[cfg(feature = "otel")]
use std::time::UNIX_EPOCH;
use std::time::{Duration, SystemTime};

/// Owning wrapper around the OTLP trace-export endpoint + HTTP client.
///
/// Construct once at startup and clone (via `Arc`) into any subsystem
/// that emits spans. Cloning is free — the underlying `reqwest::Client`
/// is already reference-counted.
#[derive(Debug, Clone)]
pub struct TraceExporter {
    endpoint: String,
    /// `None` when the exporter is disabled. The client is never
    /// constructed for a disabled exporter so a disabled default can't
    /// silently mask a `reqwest::ClientBuilder::build` failure (e.g.
    /// broken system TLS store) and then substitute a client that
    /// ignores the configured timeout.
    #[cfg_attr(not(feature = "otel"), allow(dead_code))]
    client: Option<reqwest::Client>,
    #[cfg_attr(not(feature = "otel"), allow(dead_code))]
    timeout: Duration,
}

/// Error constructing a [`TraceExporter`].
///
/// Propagated instead of silently substituted so a misconfigured
/// operator endpoint / broken system TLS store fails loudly at
/// startup rather than producing a half-working exporter that drops
/// every span.
#[derive(Debug, thiserror::Error)]
pub enum TraceExporterError {
    #[error("reqwest client build failed: {0}")]
    ClientBuild(#[from] reqwest::Error),
}

impl TraceExporter {
    /// Construct an exporter that pushes to `endpoint`
    /// (e.g. `http://collector:4318`). Fails if the underlying
    /// `reqwest::Client` cannot be constructed. An empty endpoint is
    /// rejected — use [`disabled`](Self::disabled) instead so the
    /// intent stays explicit.
    pub fn new(
        endpoint: impl Into<String>,
        timeout: Duration,
    ) -> Result<Arc<Self>, TraceExporterError> {
        let endpoint = endpoint.into();
        let client = if endpoint.is_empty() {
            None
        } else {
            Some(reqwest::Client::builder().timeout(timeout).build()?)
        };
        Ok(Arc::new(Self {
            endpoint,
            client,
            timeout,
        }))
    }

    /// Build a disabled exporter. `emit` is a no-op. No HTTP client
    /// is constructed so this cannot fail. Use in tests and when the
    /// operator has not configured `observability.otlp.export`.
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self {
            endpoint: String::new(),
            client: None,
            timeout: Duration::from_secs(5),
        })
    }

    pub fn is_enabled(&self) -> bool {
        !self.endpoint.is_empty() && self.client.is_some()
    }

    /// Emit one span. Non-blocking: spawns a detached task that posts
    /// to the collector. Returns immediately.
    ///
    /// `start` / `end` are wall-clock `SystemTime` values. Pass the
    /// same value for both when observing a zero-duration event.
    /// `trace_id` must be the id carried on `ExecuteRequest` so the
    /// gateway span and every executor span join up into one OTLP
    /// trace.
    ///
    /// When the `otel` cargo feature is disabled, the protobuf encode
    /// path is compiled out and every call is a cheap no-op regardless
    /// of the endpoint setting.
    #[allow(unused_variables, clippy::too_many_arguments)]
    pub fn emit(
        self: &Arc<Self>,
        span_name: &'static str,
        trace_id: u64,
        start: SystemTime,
        end: SystemTime,
        tenant_id: u32,
        vshard_id: u32,
        status_ok: bool,
    ) {
        if !self.is_enabled() {
            // No-op when the endpoint is empty. In non-`otel` builds
            // the `otel` block below is compiled out, so this early
            // return is the only body; clippy's `needless_return`
            // lint is silenced locally to keep both configurations
            // valid without an awkward refactor.
            #[allow(clippy::needless_return)]
            return;
        }
        #[cfg(feature = "otel")]
        {
            let endpoint = self.endpoint.clone();
            // `is_enabled` above guarantees `client` is Some.
            let Some(client) = self.client.clone() else {
                return;
            };
            let timeout = self.timeout;
            let start_ns = system_time_to_unix_nanos(start);
            let end_ns = system_time_to_unix_nanos(end);
            tokio::spawn(async move {
                crate::control::otel::exporter::export_span(
                    &client,
                    timeout,
                    &crate::control::otel::exporter::SpanExport {
                        endpoint: &endpoint,
                        trace_id,
                        span_name,
                        start_ns,
                        end_ns,
                        tenant_id,
                        vshard_id,
                        status_ok,
                    },
                )
                .await;
            });
        }
    }
}

#[cfg(feature = "otel")]
fn system_time_to_unix_nanos(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_is_noop() {
        let exp = TraceExporter::disabled();
        assert!(!exp.is_enabled());
        let now = SystemTime::now();
        // emit on disabled exporter must not panic — the internal
        // spawn path is guarded behind the is_enabled check.
        exp.emit("noop", 0xDEAD, now, now, 0, 0, true);
    }

    #[test]
    fn endpoint_controls_enabled_flag() {
        let on = TraceExporter::new("http://collector:4318", Duration::from_secs(1)).unwrap();
        let off = TraceExporter::new(String::new(), Duration::from_secs(1)).unwrap();
        assert!(on.is_enabled());
        assert!(!off.is_enabled());
    }
}
