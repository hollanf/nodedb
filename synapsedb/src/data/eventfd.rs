//! Thin wrapper around Linux `eventfd` for TPC core wake signaling.
//!
//! When the Control Plane pushes a request into the SPSC ring buffer, it
//! writes to the eventfd to wake the Data Plane core from `libc::poll`.
//! This replaces the 50µs busy-poll sleep with an interrupt-driven wake.

use std::os::unix::io::RawFd;

/// An eventfd file descriptor for cross-thread wake signaling.
///
/// `!Send` and `!Sync` — each core owns its own EventFd on the Data Plane side.
/// The Control Plane holds a cloneable `EventFdNotifier` (which is `Send + Sync`).
pub struct EventFd {
    fd: RawFd,
}

impl EventFd {
    /// Create a new eventfd in semaphore mode (EFD_SEMAPHORE).
    pub fn new() -> crate::Result<Self> {
        // SAFETY: eventfd2 is a standard Linux syscall. EFD_SEMAPHORE makes
        // each read decrement by 1, EFD_NONBLOCK prevents blocking reads.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_SEMAPHORE) };
        if fd < 0 {
            return Err(crate::Error::Io(std::io::Error::last_os_error()));
        }
        Ok(Self { fd })
    }

    /// Get the raw fd for use with `libc::poll`.
    pub fn as_raw_fd(&self) -> RawFd {
        self.fd
    }

    /// Drain all pending notifications (non-blocking).
    ///
    /// Returns the number of signals accumulated since the last drain.
    /// Returns 0 if no signals are pending.
    pub fn drain(&self) -> u64 {
        let mut buf = 0u64;
        // SAFETY: reading 8 bytes from an eventfd is the documented API.
        let ret = unsafe {
            libc::read(
                self.fd,
                &mut buf as *mut u64 as *mut libc::c_void,
                std::mem::size_of::<u64>(),
            )
        };
        if ret == 8 { buf } else { 0 }
    }

    /// Block until a signal arrives, with a timeout.
    ///
    /// Returns `true` if a signal was received, `false` on timeout.
    pub fn poll_wait(&self, timeout_ms: i32) -> bool {
        let mut pfd = libc::pollfd {
            fd: self.fd,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: standard poll syscall on a valid fd.
        let ret = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
        ret > 0 && (pfd.revents & libc::POLLIN) != 0
    }

    /// Create a `Send + Sync` notifier handle for the Control Plane.
    pub fn notifier(&self) -> EventFdNotifier {
        EventFdNotifier { fd: self.fd }
    }
}

impl Drop for EventFd {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}

/// A `Send + Sync` handle for signaling an eventfd from the Control Plane.
///
/// The underlying fd is owned by the `EventFd` on the Data Plane side.
/// The notifier only writes to it — it does not close the fd on drop.
#[derive(Clone, Copy)]
pub struct EventFdNotifier {
    fd: RawFd,
}

// SAFETY: eventfd write is thread-safe and atomic for 8-byte writes.
unsafe impl Send for EventFdNotifier {}
unsafe impl Sync for EventFdNotifier {}

impl EventFdNotifier {
    /// Signal the Data Plane core to wake up.
    pub fn notify(&self) {
        let val: u64 = 1;
        // SAFETY: writing 8 bytes to an eventfd is the documented API.
        // This is atomic and thread-safe per the Linux man page.
        unsafe {
            libc::write(
                self.fd,
                &val as *const u64 as *const libc::c_void,
                std::mem::size_of::<u64>(),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_signal() {
        let efd = EventFd::new().unwrap();
        let notifier = efd.notifier();

        // No signals yet.
        assert_eq!(efd.drain(), 0);

        // Signal and drain.
        notifier.notify();
        assert_eq!(efd.drain(), 1);

        // Drained — should be 0 again.
        assert_eq!(efd.drain(), 0);
    }

    #[test]
    fn multiple_signals_accumulate() {
        let efd = EventFd::new().unwrap();
        let notifier = efd.notifier();

        notifier.notify();
        notifier.notify();
        notifier.notify();

        // EFD_SEMAPHORE mode: each read returns 1, decrements by 1.
        assert_eq!(efd.drain(), 1);
        assert_eq!(efd.drain(), 1);
        assert_eq!(efd.drain(), 1);
        assert_eq!(efd.drain(), 0);
    }

    #[test]
    fn poll_wait_timeout() {
        let efd = EventFd::new().unwrap();
        // No signal — should timeout quickly.
        assert!(!efd.poll_wait(1));
    }

    #[test]
    fn poll_wait_signaled() {
        let efd = EventFd::new().unwrap();
        let notifier = efd.notifier();

        notifier.notify();
        assert!(efd.poll_wait(100));
    }

    #[test]
    fn notifier_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EventFdNotifier>();
    }
}
