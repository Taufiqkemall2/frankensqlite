//! MVCC page-level versioning for concurrent writers.
//!
//! This crate is intentionally small in early phases: it defines the core MVCC
//! primitives and the cross-process witness/lock-table coordination types.

pub mod core_types;
pub mod invariants;
pub mod lifecycle;
pub mod witness_hierarchy;

pub use core_types::{
    CommitIndex, CommitLog, CommitRecord, InProcessPageLockTable, LOCK_TABLE_SHARDS, PageBuf,
    Transaction, TransactionMode, TransactionState, VersionArena, VersionIdx,
};
pub use invariants::{
    SerializedWriteMutex, TxnManager, VersionStore, idx_to_version_pointer, visible,
};
pub use lifecycle::{BeginKind, CommitResponse, MvccError, Savepoint, TransactionManager};
pub use witness_hierarchy::{
    HotWitnessIndexDerivationV1, HotWitnessIndexSizingV1, WitnessHierarchyConfigV1,
    WitnessHotIndexManifestV1, WitnessSizingError,
};
