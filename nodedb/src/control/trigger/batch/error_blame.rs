//! Per-row error accumulator for batched trigger execution.
//!
//! When a BEFORE trigger raises an exception on some rows in a batch,
//! we don't hard-fail the entire batch. Instead, we capture which rows
//! failed and why, allowing the caller to roll back with per-row context.

/// A single row-level error from batched trigger execution.
#[derive(Debug, Clone)]
pub struct RowError {
    /// Row identifier (document ID / primary key).
    pub row_id: String,
    /// The error message from the trigger.
    pub error: String,
    /// Index of this row in the original batch.
    pub batch_index: usize,
}

/// Accumulates per-row errors during batched trigger execution.
///
/// Hard errors (panic, stack overflow) still abort the entire batch immediately.
/// Soft errors (RAISE EXCEPTION based on row data) are captured per-row.
#[derive(Debug, Default)]
pub struct ErrorAccumulator {
    errors: Vec<RowError>,
}

impl ErrorAccumulator {
    pub fn new() -> Self {
        Self { errors: Vec::new() }
    }

    /// Record a row-level error.
    pub fn record(&mut self, row_id: &str, batch_index: usize, error: &str) {
        self.errors.push(RowError {
            row_id: row_id.to_string(),
            error: error.to_string(),
            batch_index,
        });
    }

    /// Check if any errors were recorded.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Number of failed rows.
    pub fn error_count(&self) -> usize {
        self.errors.len()
    }

    /// Consume the accumulator and return all errors.
    pub fn into_errors(self) -> Vec<RowError> {
        self.errors
    }

    /// Build a combined error message for all failed rows.
    pub fn to_combined_error(&self) -> String {
        if self.errors.is_empty() {
            return String::new();
        }
        let mut msg = format!("{} row(s) failed trigger validation:\n", self.errors.len());
        for (i, err) in self.errors.iter().enumerate() {
            if i >= 10 {
                msg.push_str(&format!("  ... and {} more\n", self.errors.len() - 10));
                break;
            }
            msg.push_str(&format!("  row '{}': {}\n", err.row_id, err.error));
        }
        msg
    }

    /// Build a boolean mask: true = row succeeded, false = row failed.
    /// `total_rows` is the batch size.
    pub fn success_mask(&self, total_rows: usize) -> Vec<bool> {
        let mut mask = vec![true; total_rows];
        for err in &self.errors {
            if err.batch_index < total_rows {
                mask[err.batch_index] = false;
            }
        }
        mask
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_accumulator() {
        let acc = ErrorAccumulator::new();
        assert!(!acc.has_errors());
        assert_eq!(acc.error_count(), 0);
        assert!(acc.to_combined_error().is_empty());
    }

    #[test]
    fn record_and_report() {
        let mut acc = ErrorAccumulator::new();
        acc.record("doc-1", 0, "total cannot be negative");
        acc.record("doc-5", 4, "customer not active");
        assert!(acc.has_errors());
        assert_eq!(acc.error_count(), 2);
        let msg = acc.to_combined_error();
        assert!(msg.contains("2 row(s)"));
        assert!(msg.contains("doc-1"));
        assert!(msg.contains("doc-5"));
    }

    #[test]
    fn success_mask() {
        let mut acc = ErrorAccumulator::new();
        acc.record("r2", 1, "bad");
        acc.record("r4", 3, "bad");
        let mask = acc.success_mask(5);
        assert_eq!(mask, vec![true, false, true, false, true]);
    }

    #[test]
    fn combined_error_truncates_at_10() {
        let mut acc = ErrorAccumulator::new();
        for i in 0..15 {
            acc.record(&format!("r{i}"), i, "err");
        }
        let msg = acc.to_combined_error();
        assert!(msg.contains("15 row(s)"));
        assert!(msg.contains("and 5 more"));
    }
}
