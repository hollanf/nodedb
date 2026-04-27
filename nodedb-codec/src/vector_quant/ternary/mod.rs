//! BitNet b1.58 ternary quantization codec.
//!
//! Encodes FP32 vectors into ternary trits `{-1, 0, +1}` using per-vector
//! mean-absolute-value (absmean) scaling. Two storage layouts are supported:
//!
//! - **Cold** ([`QuantMode::TernaryPacked`]): 5 trits/byte via base-3 packing
//!   (1.6 bpw). Optimal for disk; decompressed on page-in.
//! - **Hot** ([`QuantMode::TernarySimd`]): 2 bpw, 4 trits/byte, suitable for
//!   direct SIMD load. [`VectorCodec::encode`] produces hot format by default.
//!
//! [`QuantMode::TernaryPacked`]: crate::vector_quant::layout::QuantMode::TernaryPacked
//! [`QuantMode::TernarySimd`]: crate::vector_quant::layout::QuantMode::TernarySimd
//! [`VectorCodec::encode`]: crate::vector_quant::codec::VectorCodec::encode

pub mod codec;
pub mod packing;
pub mod simd;

pub use codec::{TernaryCodec, TernaryQuantized, TernaryQuery};
pub use packing::{cold_to_hot, pack_cold, pack_hot, quantize, unpack_cold, unpack_hot};
pub use simd::ternary_dot;
