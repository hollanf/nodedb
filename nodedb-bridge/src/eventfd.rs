//! Cross-runtime wake signaling via Linux eventfd.
//!
//! eventfd is the only safe primitive for waking across Tokio and Glommio/monoio:
//!
//! - Both runtimes can poll a file descriptor.
//! - No `Send` requirement on the waker itself — just read/write an fd.
//! - Coalescing: multiple writes produce a single readable event.
//!
//! ## Usage
//!
//! Two EventFd instances per bridge channel:
//!
//! - `producer_wake`: Consumer writes → Producer reads (queue was full, now has space)
//! - `consumer_wake`: Producer writes → Consumer reads (queue was empty, now has data)
//!
//! The runtime-specific integration (registering the fd with epoll/io_uring) is
//! done by the caller. This module only provides raw fd-based signaling.

use std::io;
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd, RawFd};

/// A cross-runtime wake signal backed by a Linux eventfd.
///
/// Write to signal, read to consume. Multiple signals coalesce into one.
/// The fd can be registered with any event loop (epoll, io_uring, kqueue fallback).
pub struct EventFd {
    fd: OwnedFd,
}

impl EventFd {
    /// Create a new eventfd in semaphore mode.
    ///
    /// `EFD_NONBLOCK` ensures reads/writes never block the calling thread.
    /// `EFD_CLOEXEC` prevents fd leaks across fork/exec.
    pub fn new() -> io::Result<Self> {
        // SAFETY: eventfd2 is a standard Linux syscall. Flags are valid.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fd is a valid file descriptor returned by eventfd().
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        Ok(Self { fd })
    }

    /// Signal the other side to wake up.
    ///
    /// Writes 1 to the eventfd counter. Multiple writes accumulate but
    /// a single read clears all pending signals.
    pub fn notify(&self) -> io::Result<()> {
        let val: u64 = 1;
        // SAFETY: writing 8 bytes to a valid eventfd.
        let ret = unsafe {
            libc::write(
                self.fd.as_raw_fd(),
                &val as *const u64 as *const libc::c_void,
                8,
            )
        };
        if ret < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    /// Consume the pending signal count, returning the accumulated value.
    ///
    /// Returns `Ok(0)` if no signal was pending (EAGAIN on non-blocking fd).
    /// Returns `Ok(n)` where n is the accumulated signal count.
    pub fn try_read(&self) -> io::Result<u64> {
        let mut val: u64 = 0;
        // SAFETY: reading 8 bytes from a valid eventfd.
        let ret = unsafe {
            libc::read(
                self.fd.as_raw_fd(),
                &mut val as *mut u64 as *mut libc::c_void,
                8,
            )
        };
        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                Ok(0)
            } else {
                Err(err)
            }
        } else {
            Ok(val)
        }
    }

    /// Get the raw file descriptor for registration with an event loop.
    ///
    /// The caller can register this fd with:
    /// - Tokio: `AsyncFd::new()`
    /// - Glommio: `GlommioDma::from_raw_fd()` or similar
    /// - io_uring: `IORING_OP_READ` on the fd
    pub fn as_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

// SAFETY: eventfd is a kernel object. The fd can be shared across threads.
// Writes are atomic (8-byte writes to eventfd are guaranteed atomic by Linux).
unsafe impl Send for EventFd {}
unsafe impl Sync for EventFd {}

/// A pair of eventfds for bidirectional wake signaling across the bridge.
///
/// ```text
/// Producer (Tokio)          Consumer (TPC)
///    │                          │
///    │── notify(consumer_wake) ──→│  "queue has data"
///    │                          │
///    │←── notify(producer_wake) ──│  "queue has space"
/// ```
pub struct WakePair {
    /// Producer reads this to know the consumer freed space.
    pub producer_wake: EventFd,
    /// Consumer reads this to know the producer enqueued data.
    pub consumer_wake: EventFd,
}

impl WakePair {
    /// Create a new wake pair.
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            producer_wake: EventFd::new()?,
            consumer_wake: EventFd::new()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notify_and_read() {
        let efd = EventFd::new().unwrap();

        // No pending signal.
        assert_eq!(efd.try_read().unwrap(), 0);

        // Signal once.
        efd.notify().unwrap();
        assert_eq!(efd.try_read().unwrap(), 1);

        // Consumed — nothing pending.
        assert_eq!(efd.try_read().unwrap(), 0);
    }

    #[test]
    fn multiple_notifies_accumulate() {
        let efd = EventFd::new().unwrap();

        efd.notify().unwrap();
        efd.notify().unwrap();
        efd.notify().unwrap();

        // Single read returns accumulated count.
        assert_eq!(efd.try_read().unwrap(), 3);
        assert_eq!(efd.try_read().unwrap(), 0);
    }

    #[test]
    fn wake_pair_bidirectional() {
        let pair = WakePair::new().unwrap();

        // Producer signals consumer.
        pair.consumer_wake.notify().unwrap();
        assert_eq!(pair.consumer_wake.try_read().unwrap(), 1);

        // Consumer signals producer.
        pair.producer_wake.notify().unwrap();
        assert_eq!(pair.producer_wake.try_read().unwrap(), 1);
    }

    #[test]
    fn fd_is_valid() {
        let efd = EventFd::new().unwrap();
        assert!(efd.as_fd() >= 0);
    }
}
