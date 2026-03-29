//! DataFusion UDF/UDAF/UDWF wrappers for timeseries SQL functions.
//!
//! Shared by both Origin (server) and Lite (embedded). Call
//! [`register_timeseries_udfs`] once per `SessionContext`.

mod aggregate;
pub mod approx;
mod helpers;
mod window_basic;
mod window_rate;
mod window_smooth;

use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{AggregateUDF, WindowUDF};

pub use aggregate::{TsCorrelateUdaf, TsPercentileUdaf, TsStddevUdaf};
pub use approx::{ApproxCountDistinctUdaf, ApproxCountUdaf, ApproxPercentileUdaf, ApproxTopkUdaf};
pub use window_basic::{TsDeltaUdwf, TsInterpolateUdwf, TsLagUdwf, TsLeadUdwf, TsRankUdwf};
pub use window_rate::{TsDerivativeUdwf, TsRateUdwf};
pub use window_smooth::{TsEmaUdwf, TsMovingAvgUdwf};

/// Register all 12 timeseries SQL functions on a DataFusion session.
pub fn register_timeseries_udfs(ctx: &SessionContext) {
    // Window functions (9).
    ctx.register_udwf(WindowUDF::new_from_impl(TsRateUdwf::new()));
    ctx.register_udwf(WindowUDF::new_from_impl(TsDerivativeUdwf::new()));
    ctx.register_udwf(WindowUDF::new_from_impl(TsMovingAvgUdwf::new()));
    ctx.register_udwf(WindowUDF::new_from_impl(TsEmaUdwf::new()));
    ctx.register_udwf(WindowUDF::new_from_impl(TsDeltaUdwf::new()));
    ctx.register_udwf(WindowUDF::new_from_impl(TsInterpolateUdwf::new()));
    ctx.register_udwf(WindowUDF::new_from_impl(TsLagUdwf::new()));
    ctx.register_udwf(WindowUDF::new_from_impl(TsLeadUdwf::new()));
    ctx.register_udwf(WindowUDF::new_from_impl(TsRankUdwf::new()));

    // Aggregate functions (3).
    ctx.register_udaf(AggregateUDF::new_from_impl(TsPercentileUdaf::new()));
    ctx.register_udaf(AggregateUDF::new_from_impl(TsStddevUdaf::new()));
    ctx.register_udaf(AggregateUDF::new_from_impl(TsCorrelateUdaf::new()));

    // Approximate aggregate functions (4).
    ctx.register_udaf(AggregateUDF::new_from_impl(ApproxCountDistinctUdaf::new()));
    ctx.register_udaf(AggregateUDF::new_from_impl(ApproxPercentileUdaf::new()));
    ctx.register_udaf(AggregateUDF::new_from_impl(ApproxTopkUdaf::new()));
    ctx.register_udaf(AggregateUDF::new_from_impl(ApproxCountUdaf::new()));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_does_not_panic() {
        use datafusion::execution::FunctionRegistry;
        let ctx = SessionContext::new();
        register_timeseries_udfs(&ctx);

        // Window functions.
        assert!(ctx.udwf("ts_rate").is_ok());
        assert!(ctx.udwf("ts_derivative").is_ok());
        assert!(ctx.udwf("ts_moving_avg").is_ok());
        assert!(ctx.udwf("ts_ema").is_ok());
        assert!(ctx.udwf("ts_delta").is_ok());
        assert!(ctx.udwf("ts_interpolate").is_ok());
        assert!(ctx.udwf("ts_lag").is_ok());
        assert!(ctx.udwf("ts_lead").is_ok());
        assert!(ctx.udwf("ts_rank").is_ok());

        // Aggregate functions.
        assert!(ctx.udaf("ts_percentile").is_ok());
        assert!(ctx.udaf("ts_stddev").is_ok());
        assert!(ctx.udaf("ts_correlate").is_ok());

        // Approximate aggregate functions.
        assert!(ctx.udaf("approx_count_distinct").is_ok());
        assert!(ctx.udaf("approx_percentile").is_ok());
        assert!(ctx.udaf("approx_topk").is_ok());
        assert!(ctx.udaf("approx_count").is_ok());
    }
}
