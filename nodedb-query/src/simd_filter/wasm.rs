// WASM SIMD128 path for u32 filtering.
//
// `core::arch::wasm32::u32x4_extract_lane` requires a const-generic lane
// index, which can't be a loop variable in stable Rust. Writing a manual
// 4-lane unroll for every comparator was deemed not worth the complexity
// — LLVM auto-vectorizes the scalar path on `wasm32` with `simd128` well
// enough that the gap is small. So this module just delegates to scalar
// for both equality and inequality on wasm32.

#[cfg(target_arch = "wasm32")]
use super::scalar::{scalar_eq_u32, scalar_ne_u32};

#[cfg(target_arch = "wasm32")]
pub(super) fn wasm_eq_u32(values: &[u32], target: u32) -> Vec<u64> {
    scalar_eq_u32(values, target)
}

#[cfg(target_arch = "wasm32")]
pub(super) fn wasm_ne_u32(values: &[u32], target: u32) -> Vec<u64> {
    scalar_ne_u32(values, target)
}
