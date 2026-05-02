//! Document / sparse engine operations dispatched to the Data Plane.

pub mod op;
pub mod types;

pub use op::DocumentOp;
pub use types::{
    BalancedDef, EnforcementOptions, GeneratedColumnSpec, MaterializedSumBinding, PeriodLockConfig,
    RegisteredIndex, RegisteredIndexState, ReturningColumns, ReturningItem, ReturningSpec,
    StorageMode, UpdateValue,
};
