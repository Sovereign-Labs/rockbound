//! All structs related to caching layer of Rockbound.

mod async_cache_db;
pub mod cache_container;
pub mod cache_db;
pub mod change_set;

pub use async_cache_db::AsyncCacheDb;

/// Id of ChangeSet/snapshot/cache layer
pub type SnapshotId = u64;
