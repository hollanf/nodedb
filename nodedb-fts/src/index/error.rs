//! Typed errors for `FtsIndex` operations.
//!
//! `FtsIndexError<E>` wraps both FTS-layer errors (e.g. surrogate out of range)
//! and backend storage errors (`E = B::Error`). Callers that only use in-memory
//! backends (tests, WASM) will have `E = std::convert::Infallible`.

use thiserror::Error;

use nodedb_types::Surrogate;

/// Maximum `Surrogate` value that can be safely indexed.
///
/// FTS posting blocks store doc IDs as `u32` on disk (delta-encoded, bitpacked).
/// The in-memory memtable uses the surrogate's raw `u32` as a direct index into
/// per-doc fieldnorm arrays (`fieldnorms[surrogate.0 as usize]`). A surrogate
/// near `u32::MAX` would cause the fieldnorm array to be resized to ~4 GiB,
/// exhausting process memory.
///
/// The cap is set to `u32::MAX - 1` — the same ceiling as the graph CSR node-id
/// policy. `u32::MAX` itself is reserved to make "invalid" sentinels
/// representable in the `u32` space without aliasing a real doc. The shared
/// `Surrogate::ZERO` sentinel (value 0) is also rejected at indexing time because
/// it is the "unassigned" marker used by the Control Plane.
///
/// 4 billion documents per FTS index per collection is well beyond any practical
/// workload. Collections approaching this limit should be partitioned.
pub const MAX_INDEXABLE_SURROGATE: u32 = u32::MAX - 1;

/// Errors returned by `FtsIndex` write operations.
///
/// `E` is the backend error type (`B::Error`). Backend errors are wrapped in
/// `FtsIndexError::Backend` so callers get a single error type.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FtsIndexError<E: std::fmt::Display> {
    /// The supplied `Surrogate` is outside the indexable range `1..=MAX_INDEXABLE_SURROGATE`.
    ///
    /// Either the zero sentinel (`Surrogate::ZERO`, meaning "not yet assigned")
    /// or a value at `u32::MAX` was passed to `index_document`. The Control
    /// Plane surrogate allocator must ensure surrogates are in range before
    /// dispatching indexing operations.
    #[error(
        "surrogate {surrogate} is out of the indexable range \
         1..={MAX_INDEXABLE_SURROGATE}; \
         the zero value is the unassigned sentinel and u32::MAX is reserved"
    )]
    SurrogateOutOfRange { surrogate: Surrogate },

    /// An underlying backend storage operation failed.
    #[error("FTS backend error: {0}")]
    Backend(E),
}

impl<E: std::fmt::Display> FtsIndexError<E> {
    /// Wrap a backend error.
    pub(crate) fn backend(e: E) -> Self {
        Self::Backend(e)
    }
}
