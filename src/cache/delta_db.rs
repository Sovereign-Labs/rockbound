#![allow(dead_code)]
//! This module contains next iteration of [`crate::cache::cache_db::CacheDb`]
use crate::cache::change_set::ChangeSet;
use crate::DB;
use std::sync::{Arc, Mutex};

/// Intermediate step between [`crate::cache::cache_db::CacheDb`] and future DeltaDbReader
/// Supports "local writes". And for historical readings uses `Vec<Arc<ChangeSet>`
#[derive(Debug)]
pub struct DeltaDb {
    /// Local writes are collected here.
    local_cache: Mutex<ChangeSet>,
    /// Set of not finalized changes in **reverse** order.
    previous_data: Vec<Arc<ChangeSet>>,
    /// Reading finalized data from here.
    db: DB,
}
