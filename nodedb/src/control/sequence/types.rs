//! Sequence runtime types.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

/// Lock-free handle for a single sequence's counter on this node.
///
/// `nextval` uses `AtomicI64::fetch_add` — zero contention, no mutex.
pub struct SequenceHandle {
    /// Current counter value. Starts at `start_value - increment` so the
    /// first `fetch_add(increment)` returns `start_value`.
    counter: AtomicI64,
    /// Whether `nextval` has been called at least once.
    called: AtomicBool,
    /// Sequence definition (immutable after creation, replaced on ALTER).
    pub def: crate::control::security::catalog::sequence_types::StoredSequence,
}

impl SequenceHandle {
    /// Create a new handle from a sequence definition and optional persisted state.
    pub fn new(
        def: crate::control::security::catalog::sequence_types::StoredSequence,
        state: Option<crate::control::security::catalog::sequence_types::SequenceState>,
    ) -> Self {
        let (initial_counter, called) = if let Some(s) = state {
            if s.is_called {
                (s.current_value, true)
            } else {
                // Not yet called — position so first nextval returns start_value.
                (def.start_value - def.increment, false)
            }
        } else {
            // No persisted state — position so first nextval returns start_value.
            (def.start_value - def.increment, false)
        };

        Self {
            counter: AtomicI64::new(initial_counter),
            called: AtomicBool::new(called),
            def,
        }
    }

    /// Advance to the next value (lock-free).
    ///
    /// Returns the new value, or an error if the sequence is exhausted
    /// and CYCLE is not enabled.
    pub fn nextval(&self) -> Result<i64, SequenceError> {
        let increment = self.def.increment;
        let prev = self.counter.fetch_add(increment, Ordering::Relaxed);
        let new_val = prev + increment;

        self.called.store(true, Ordering::Relaxed);

        // Check bounds.
        if increment > 0 && new_val > self.def.max_value {
            if self.def.cycle {
                // Wrap around to min_value.
                self.counter.store(self.def.min_value, Ordering::Relaxed);
                return Ok(self.def.min_value);
            }
            // Undo the increment.
            self.counter.store(prev, Ordering::Relaxed);
            return Err(SequenceError::Exhausted {
                name: self.def.name.clone(),
            });
        }
        if increment < 0 && new_val < self.def.min_value {
            if self.def.cycle {
                self.counter.store(self.def.max_value, Ordering::Relaxed);
                return Ok(self.def.max_value);
            }
            self.counter.store(prev, Ordering::Relaxed);
            return Err(SequenceError::Exhausted {
                name: self.def.name.clone(),
            });
        }

        Ok(new_val)
    }

    /// Advance the sequence by N values in one atomic operation.
    ///
    /// Returns a Vec of N consecutive values. Uses a single `fetch_add`
    /// for the entire batch — zero contention for bulk inserts.
    pub fn nextval_batch(&self, n: usize) -> Result<Vec<i64>, SequenceError> {
        if n == 0 {
            return Ok(Vec::new());
        }

        let increment = self.def.increment;
        let total_advance = increment * n as i64;
        let prev = self.counter.fetch_add(total_advance, Ordering::Relaxed);

        self.called.store(true, Ordering::Relaxed);

        let mut values = Vec::with_capacity(n);
        for i in 0..n {
            values.push(prev + increment * (i as i64 + 1));
        }

        // Check bounds on the last value (n > 0 guaranteed above).
        let last = values[n - 1];
        if increment > 0 && last > self.def.max_value {
            if self.def.cycle {
                // Wrap: reset counter to min_value + remainder.
                self.counter.store(self.def.min_value, Ordering::Relaxed);
                // Regenerate values from min_value.
                values.clear();
                for i in 0..n {
                    values.push(self.def.min_value + increment * i as i64);
                }
                self.counter.store(
                    self.def.min_value + increment * (n as i64 - 1),
                    Ordering::Relaxed,
                );
                return Ok(values);
            }
            // Rollback.
            self.counter.store(prev, Ordering::Relaxed);
            return Err(SequenceError::Exhausted {
                name: self.def.name.clone(),
            });
        }
        if increment < 0 && last < self.def.min_value {
            if self.def.cycle {
                self.counter.store(self.def.max_value, Ordering::Relaxed);
                values.clear();
                for i in 0..n {
                    values.push(self.def.max_value + increment * i as i64);
                }
                self.counter.store(
                    self.def.max_value + increment * (n as i64 - 1),
                    Ordering::Relaxed,
                );
                return Ok(values);
            }
            self.counter.store(prev, Ordering::Relaxed);
            return Err(SequenceError::Exhausted {
                name: self.def.name.clone(),
            });
        }

        Ok(values)
    }

    /// Get the last value returned by nextval (for currval).
    pub fn currval(&self) -> Result<i64, SequenceError> {
        if !self.called.load(Ordering::Relaxed) {
            return Err(SequenceError::NotYetCalled {
                name: self.def.name.clone(),
            });
        }
        Ok(self.counter.load(Ordering::Relaxed))
    }

    /// Set the counter to a specific value (for setval).
    pub fn setval(&self, value: i64) -> Result<i64, SequenceError> {
        if value < self.def.min_value || value > self.def.max_value {
            return Err(SequenceError::OutOfRange {
                name: self.def.name.clone(),
                value,
                min: self.def.min_value,
                max: self.def.max_value,
            });
        }
        self.counter.store(value, Ordering::Relaxed);
        self.called.store(true, Ordering::Relaxed);
        Ok(value)
    }

    /// Get current counter value for persistence.
    pub fn current_value(&self) -> i64 {
        self.counter.load(Ordering::Relaxed)
    }

    /// Whether nextval has been called.
    pub fn is_called(&self) -> bool {
        self.called.load(Ordering::Relaxed)
    }
}

/// Sequence operation errors.
#[derive(Debug, Clone)]
pub enum SequenceError {
    /// Sequence reached max/min and CYCLE is off.
    Exhausted { name: String },
    /// currval called before nextval in this session.
    NotYetCalled { name: String },
    /// setval value outside [min, max] range.
    OutOfRange {
        name: String,
        value: i64,
        min: i64,
        max: i64,
    },
    /// Sequence not found.
    NotFound { name: String },
    /// Sequence already exists.
    AlreadyExists { name: String },
    /// Invalid definition.
    InvalidDefinition { detail: String },
}

impl std::fmt::Display for SequenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SequenceError::Exhausted { name } => {
                write!(f, "nextval: reached maximum value for sequence \"{name}\"")
            }
            SequenceError::NotYetCalled { name } => {
                write!(
                    f,
                    "currval of sequence \"{name}\" is not yet defined in this session"
                )
            }
            SequenceError::OutOfRange {
                name,
                value,
                min,
                max,
            } => {
                write!(
                    f,
                    "setval: value {value} is outside allowed range [{min}, {max}] for sequence \"{name}\""
                )
            }
            SequenceError::NotFound { name } => {
                write!(f, "sequence \"{name}\" does not exist")
            }
            SequenceError::AlreadyExists { name } => {
                write!(f, "sequence \"{name}\" already exists")
            }
            SequenceError::InvalidDefinition { detail } => {
                write!(f, "invalid sequence definition: {detail}")
            }
        }
    }
}

impl std::error::Error for SequenceError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::sequence_types::StoredSequence;

    fn make_handle(start: i64, inc: i64, min: i64, max: i64, cycle: bool) -> SequenceHandle {
        let mut def = StoredSequence::new(1, "test".into(), "admin".into());
        def.start_value = start;
        def.increment = inc;
        def.min_value = min;
        def.max_value = max;
        def.cycle = cycle;
        SequenceHandle::new(def, None)
    }

    #[test]
    fn basic_nextval() {
        let h = make_handle(1, 1, 1, 100, false);
        assert_eq!(h.nextval().unwrap(), 1);
        assert_eq!(h.nextval().unwrap(), 2);
        assert_eq!(h.nextval().unwrap(), 3);
    }

    #[test]
    fn currval_before_nextval() {
        let h = make_handle(1, 1, 1, 100, false);
        assert!(h.currval().is_err());
    }

    #[test]
    fn currval_after_nextval() {
        let h = make_handle(1, 1, 1, 100, false);
        h.nextval().unwrap();
        assert_eq!(h.currval().unwrap(), 1);
        h.nextval().unwrap();
        assert_eq!(h.currval().unwrap(), 2);
    }

    #[test]
    fn exhausted_no_cycle() {
        let h = make_handle(1, 1, 1, 3, false);
        assert_eq!(h.nextval().unwrap(), 1);
        assert_eq!(h.nextval().unwrap(), 2);
        assert_eq!(h.nextval().unwrap(), 3);
        assert!(h.nextval().is_err());
    }

    #[test]
    fn cycle_wraps_around() {
        let h = make_handle(1, 1, 1, 3, true);
        assert_eq!(h.nextval().unwrap(), 1);
        assert_eq!(h.nextval().unwrap(), 2);
        assert_eq!(h.nextval().unwrap(), 3);
        assert_eq!(h.nextval().unwrap(), 1); // wraps
    }

    #[test]
    fn descending_sequence() {
        let h = make_handle(10, -1, 8, 10, false);
        assert_eq!(h.nextval().unwrap(), 10);
        assert_eq!(h.nextval().unwrap(), 9);
        assert_eq!(h.nextval().unwrap(), 8);
        assert!(h.nextval().is_err());
    }

    #[test]
    fn setval_in_range() {
        let h = make_handle(1, 1, 1, 100, false);
        assert_eq!(h.setval(50).unwrap(), 50);
        assert_eq!(h.currval().unwrap(), 50);
        assert_eq!(h.nextval().unwrap(), 51);
    }

    #[test]
    fn setval_out_of_range() {
        let h = make_handle(1, 1, 1, 100, false);
        assert!(h.setval(101).is_err());
        assert!(h.setval(0).is_err());
    }

    #[test]
    fn increment_by_10() {
        let h = make_handle(10, 10, 1, 100, false);
        assert_eq!(h.nextval().unwrap(), 10);
        assert_eq!(h.nextval().unwrap(), 20);
        assert_eq!(h.nextval().unwrap(), 30);
    }
}
