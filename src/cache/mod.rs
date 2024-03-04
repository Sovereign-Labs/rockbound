//! All structs related to caching layer of Rockbound.

pub mod cache_container;
pub mod cache_db;
pub mod change_set;

/// Id of ChangeSet/snapshot/cache layer
pub type SnapshotId = u64;
