//! Bitemporal helpers — placeholder for AS-OF read variants if added.
//!
//! Current bitemporal write/read/delete behavior lives alongside the
//! non-bitemporal paths in [`put`], [`get`], [`delete`] and branches on
//! `is_bitemporal(collection)`. This file exists so future bitemporal-only
//! API (e.g. explicit AS-OF entry points) lands here without growing the
//! sibling files.
