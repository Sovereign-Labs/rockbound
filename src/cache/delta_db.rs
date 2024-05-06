#![allow(dead_code)]
//! This module contains next iteration of [`crate::cache::cache_db::CacheDb`]
use crate::cache::change_set::ChangeSet;
use crate::cache::SnapshotId;
use crate::{DB, PaginatedResponse, Schema, SchemaBatch, SeekKeyEncoder};
use std::sync::{Arc, Mutex};
use crate::schema::{KeyCodec, ValueCodec};

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

impl DeltaDb {
    /// Creates new [`DeltaDb`] with given `id`, `db` and `uncommited_changes`.
    /// `uncommited_changes` should be in reverse order.
    pub fn new(id: SnapshotId, db: DB, uncommited_changes: Vec<Arc<ChangeSet>>) -> Self {
        Self {
            local_cache: Mutex::new(ChangeSet::new(id)),
            previous_data: uncommited_changes,
            db,
        }
    }

    /// Store a value in local cache.
    pub fn put<S: Schema>(
        &self,
        key: &impl KeyCodec<S>,
        value: &impl ValueCodec<S>,
    ) -> anyhow::Result<()> {
        self.local_cache
            .lock()
            .expect("Local ChangeSet lock must not be poisoned")
            .operations
            .put(key, value)
    }

    /// Delete given key from local cache
    pub fn delete<S: Schema>(&self, key: &impl KeyCodec<S>) -> anyhow::Result<()> {
        self.local_cache
            .lock()
            .expect("Local ChangeSet lock must not be poisoned")
            .operations
            .delete(key)
    }

    /// Writes many operations at once in local cache, atomically.
    pub fn write_many(&self, batch: SchemaBatch) -> anyhow::Result<()> {
        let mut inner = self
            .local_cache
            .lock()
            .expect("Local SchemaBatch lock must not be poisoned");
        inner.operations.merge(batch);
        Ok(())
    }

    /// Get a value from current snapshot, its parents or underlying database
    pub fn read<S: Schema>(&self, key: &impl KeyCodec<S>) -> anyhow::Result<Option<S::Value>> {
        // Some(Operation) means that key was touched,
        // but in case of deletion we early return None
        // Only in case of not finding operation for key,
        // we go deeper

        // Hold local cache lock explicitly, so reads are atomic
        let local_cache = self
            .local_cache
            .lock()
            .expect("SchemaBatch lock should not be poisoned");

        // 1. Check in cache
        if let Some(operation) = local_cache.get(key)? {
            return operation.decode_value::<S>();
        }

        todo!()
    }

    /// Get value of largest key written value for given [`Schema`]
    pub fn get_largest<S: Schema>(&self) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        todo!()
    }

    /// Get largest value in [`Schema`] that is smaller than give `seek_key`
    pub fn get_prev<S: Schema>(
        &self,
        _seek_key: &impl SeekKeyEncoder<S>,
    ) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        todo!()
    }


    /// Get `n` keys >= `seek_key`
    pub fn get_n_from_first_match<S: Schema>(
        &self,
        _seek_key: &impl SeekKeyEncoder<S>,
        _n: usize,
    ) -> anyhow::Result<PaginatedResponse<S>> {
        todo!()
    }

    /// Collects all key-value pairs in given range, from smallest to largest.
    pub fn collect_in_range<S: Schema, Sk: SeekKeyEncoder<S>>(
        &self,
        _range: std::ops::Range<Sk>,
    ) -> anyhow::Result<Vec<(S::Key, S::Value)>> {
        todo!()
    }

    // ---------------------------------------------------------------------------------------------
    // ---------------------------------------------------------------------------------------------
    /// Get a clone of internal ChangeSet
    pub fn clone_change_set(&self) -> ChangeSet {
        let change_set = self
            .local_cache
            .lock()
            .expect("Local change set lock is poisoned");
        change_set.clone()
    }
}
