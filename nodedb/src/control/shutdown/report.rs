//! Post-shutdown report returned from [`super::LoopRegistry::shutdown_all`].
//!
//! Split into its own file so D.1's sequencer can extend the
//! laggard concept to cover startup-phase durations without
//! crossing file-size limits on `registry.rs`.

use std::fmt;
use std::time::Duration;

/// Outcome of `LoopRegistry::shutdown_all`. Every registered
/// loop is accounted for in exactly one of the two vectors.
#[derive(Debug, Clone)]
pub struct ShutdownReport {
    /// Loops that joined within the deadline, in the order
    /// they were registered.
    pub exited_clean: Vec<&'static str>,
    /// Loops that exceeded the deadline.
    pub laggards: Vec<LaggardReport>,
    /// Total wall-clock duration from the start of
    /// `shutdown_all` to the last handle being disposed.
    pub total: Duration,
}

impl ShutdownReport {
    /// Whether every loop exited within the deadline.
    pub fn is_clean(&self) -> bool {
        self.laggards.is_empty()
    }

    /// Total number of loops accounted for.
    pub fn loop_count(&self) -> usize {
        self.exited_clean.len() + self.laggards.len()
    }
}

impl fmt::Display for ShutdownReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "shutdown: {} clean / {} laggards / {:?}",
            self.exited_clean.len(),
            self.laggards.len(),
            self.total
        )?;
        for l in &self.laggards {
            write!(f, "\n  {l}")?;
        }
        Ok(())
    }
}

/// Single laggard entry.
///
/// Two distinct durations are reported so operators can tell a
/// loop that has been alive forever (flaky long-runner) from
/// one that only just registered and already missed the
/// shutdown deadline (stuck-in-init).
#[derive(Debug, Clone)]
pub struct LaggardReport {
    /// The `&'static str` name passed to `spawn_loop`.
    pub name: &'static str,
    /// How long the loop had been registered before `shutdown_all`
    /// gave up on it — i.e. total lifetime of the loop. A
    /// large `uptime` + short `wait_elapsed` means a healthy
    /// long-runner that just ignored the signal. A small
    /// `uptime` means the loop hadn't finished initializing.
    pub uptime: Duration,
    /// How long `shutdown_all` actually waited for *this*
    /// handle after the shared deadline began. Always ≤ the
    /// `shutdown_all` deadline argument.
    pub wait_elapsed: Duration,
    /// `true` if the handle was async and was `.abort()`'d;
    /// `false` for blocking handles (tokio cannot abort a
    /// blocking thread).
    pub aborted: bool,
}

impl fmt::Display for LaggardReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "laggard \"{}\" uptime={:?} wait={:?} aborted={}",
            self.name, self.uptime, self.wait_elapsed, self.aborted
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_formats_clean_report() {
        let r = ShutdownReport {
            exited_clean: vec!["a", "b"],
            laggards: vec![],
            total: Duration::from_millis(12),
        };
        let s = r.to_string();
        assert!(s.contains("2 clean"));
        assert!(s.contains("0 laggards"));
        assert!(r.is_clean());
        assert_eq!(r.loop_count(), 2);
    }

    #[test]
    fn display_formats_laggards() {
        let r = ShutdownReport {
            exited_clean: vec!["a"],
            laggards: vec![LaggardReport {
                name: "wal_catchup",
                uptime: Duration::from_secs(2),
                wait_elapsed: Duration::from_millis(900),
                aborted: true,
            }],
            total: Duration::from_millis(900),
        };
        let s = r.to_string();
        assert!(s.contains("1 clean"));
        assert!(s.contains("1 laggards"));
        assert!(s.contains("wal_catchup"));
        assert!(s.contains("uptime=2s"));
        assert!(s.contains("wait=900ms"));
        assert!(s.contains("aborted=true"));
        assert!(!r.is_clean());
        assert_eq!(r.loop_count(), 2);
    }
}
