//! Per-thread jemalloc arena pinning for Thread-per-Core isolation.
//!
//! In a TPC architecture, multiple cores sharing jemalloc's default arenas
//! creates cross-core contention on arena locks — defeating the purpose of
//! shared-nothing isolation.
//!
//! This module pins each Data Plane core to a dedicated jemalloc arena at
//! startup, ensuring all allocations on that core are served from thread-local
//! memory with zero lock contention.
//!
//! The Control Plane (Tokio) uses jemalloc's default arena assignment, which
//! suits its work-stealing thread pool model.

use crate::error::{MemError, Result};

/// Pin the calling thread to a dedicated jemalloc arena.
///
/// Call this once at Data Plane core startup, before any allocations.
/// Each core should get a unique `arena_index` (typically core ID).
///
/// Returns the arena index that was assigned.
pub fn pin_thread_arena(arena_index: u32) -> Result<u32> {
    let narenas = read_narenas()?;

    let target_arena = if (arena_index as usize) < narenas {
        arena_index
    } else {
        create_arena()?
    };

    set_thread_arena(target_arena)?;
    Ok(target_arena)
}

/// Query the current number of jemalloc arenas.
fn read_narenas() -> Result<usize> {
    tikv_jemalloc_ctl::arenas::narenas::read()
        .map(|v| v as usize)
        .map_err(|e| MemError::Jemalloc(format!("failed to read narenas: {e:?}")))
}

/// Create a new jemalloc arena. Returns the new arena's index.
fn create_arena() -> Result<u32> {
    // SAFETY: `arenas.create` is a standard jemalloc mallctl that creates a new
    // arena and returns its unsigned index. No pointers are involved.
    let arena_idx: u32 = unsafe { tikv_jemalloc_ctl::raw::read(b"arenas.create\0") }
        .map_err(|e| MemError::Jemalloc(format!("failed to create arena: {e:?}")))?;
    Ok(arena_idx)
}

/// Pin the calling thread to a specific arena.
fn set_thread_arena(arena: u32) -> Result<()> {
    // SAFETY: `thread.arena` is a standard jemalloc mallctl that pins the
    // calling thread to the specified arena index.
    unsafe { tikv_jemalloc_ctl::raw::write(b"thread.arena\0", arena) }
        .map_err(|e| MemError::Jemalloc(format!("failed to set thread arena: {e:?}")))?;
    Ok(())
}

/// Read the arena index the calling thread is currently pinned to.
pub fn current_thread_arena() -> Result<u32> {
    // SAFETY: `thread.arena` read returns the current arena index for this thread.
    let arena: u32 = unsafe { tikv_jemalloc_ctl::raw::read(b"thread.arena\0") }
        .map_err(|e| MemError::Jemalloc(format!("failed to read thread arena: {e:?}")))?;
    Ok(arena)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_and_read_arena() {
        let assigned = pin_thread_arena(0).unwrap();
        assert_eq!(assigned, 0);

        let current = current_thread_arena().unwrap();
        assert_eq!(current, 0);
    }

    #[test]
    fn create_and_pin_new_arena() {
        let narenas = read_narenas().unwrap();
        let assigned = pin_thread_arena(narenas as u32 + 100).unwrap();

        let current = current_thread_arena().unwrap();
        assert_eq!(current, assigned);
    }
}
