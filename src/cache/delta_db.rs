#![allow(dead_code)]
//! This module contains next iteration of [`crate::cache::cache_db::CacheDb`]
use crate::cache::change_set::ChangeSet;
use crate::cache::SnapshotId;
use crate::iterator::{RawDbIter, ScanDirection};
use crate::schema::{KeyCodec, ValueCodec};
use crate::{
    Operation, PaginatedResponse, Schema, SchemaBatch, SchemaKey, SeekKeyEncoder, DB,
};
use std::iter::Peekable;
use std::sync::{Arc, Mutex};

/// Intermediate step between [`crate::cache::cache_db::CacheDb`] and future DeltaDbReader
/// Supports "local writes". And for historical readings uses `Vec<Arc<ChangeSet>`
#[derive(Debug)]
pub struct DeltaDb {
    /// Local writes are collected here.
    local_cache: Mutex<ChangeSet>,
    /// Set of not finalized changes in **reverse** order.
    snapshots: Vec<Arc<ChangeSet>>,
    /// Reading finalized data from here.
    db: DB,
}

impl DeltaDb {
    /// Creates new [`DeltaDb`] with given `id`, `db` and `uncommited_changes`.
    /// `uncommited_changes` should be in reverse order.
    pub fn new(id: SnapshotId, db: DB, uncommited_changes: Vec<Arc<ChangeSet>>) -> Self {
        Self {
            local_cache: Mutex::new(ChangeSet::new(id)),
            snapshots: uncommited_changes,
            db,
        }
    }

    /// Set a new value to a given key.
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

    /// Delete given key. Deletion of the key is going to be propagated to the DB eventually.
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

    /// Get a value, wherever it is.
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

        // 2. Check in snapshots, in reverse order
        for snapshot in self.snapshots.iter() {
            if let Some(operation) = snapshot.get::<S>(key)? {
                return operation.decode_value::<S>();
            }
        }

        // 3. Check in DB
        self.db.get(key)
    }

    /// Get a value of the largest key written value for given [`Schema`].
    pub fn get_largest<S: Schema>(&self) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        todo!()
    }

    /// Get the largest value in [`Schema`] that is smaller than give `seek_key`.
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


    //
    fn iter_rev<S: Schema>(&self) -> anyhow::Result<()> {
        // Local iter
        let change_set = self
            .local_cache
            .lock()
            .expect("Local cache lock must not be poisoned");
        let local_cache_iter = change_set.iter::<S>().rev();
        let _local_cache_iter = local_cache_iter.peekable();

        // Snapshot iterators
        let _snapshot_iterators = self.snapshots.iter().map(|snapshot| {
            let snapshot_iter = snapshot.iter::<S>().rev();
            snapshot_iter.peekable()
        }).collect::<Vec<_>>();


        // Db Iter
        let _db_iter = self.db.raw_iter::<S>(ScanDirection::Backward)?;

        Ok(())
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

struct DeltaDbIter<'a, LocalCacheIter, SnapshotIter>
where
    LocalCacheIter: Iterator<Item = (&'a SchemaKey, &'a Operation)>,
    SnapshotIter: Iterator<Item = (&'a SchemaKey, &'a Operation)>,
{
    local_cache_iter: Peekable<LocalCacheIter>,
    snapshot_iterators: Vec<Peekable<SnapshotIter>>,
    db_iter: Peekable<RawDbIter<'a>>,
    direction: ScanDirection,
}


impl<'a, LocalCacheIter, SnapshotIter> DeltaDbIter<'a, LocalCacheIter, SnapshotIter>
    where
        LocalCacheIter: Iterator<Item = (&'a SchemaKey, &'a Operation)>,
        SnapshotIter: Iterator<Item = (&'a SchemaKey, &'a Operation)>,
{
    fn new(
        local_cache_iter: LocalCacheIter,
        db_iter: RawDbIter<'a>,
        snapshot_iterators: Vec<SnapshotIter>,
        direction: ScanDirection,
    ) -> Self {
        Self {
            local_cache_iter: local_cache_iter.peekable(),
            snapshot_iterators: snapshot_iterators.into_iter().map(|iter| iter.peekable()).collect(),
            db_iter: db_iter.peekable(),
            direction,
        }

    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use rocksdb::DEFAULT_COLUMN_FAMILY_NAME;

    use super::*;
    use crate::schema::KeyEncoder;
    use crate::test::{TestCompositeField, TestField};
    use crate::{define_schema, SchemaKey, SchemaValue, DB};

    define_schema!(TestSchema, TestCompositeField, TestField, "TestCF");

    fn open_db(dir: impl AsRef<Path>) -> DB {
        let column_families = vec![DEFAULT_COLUMN_FAMILY_NAME, TestSchema::COLUMN_FAMILY_NAME];
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        DB::open(dir, "test", column_families, &db_opts).expect("Failed to open DB.")
    }

    // Test utils
    fn encode_key(key: &TestCompositeField) -> SchemaKey {
        <TestCompositeField as KeyEncoder<TestSchema>>::encode_key(key).unwrap()
    }

    fn encode_value(value: &TestField) -> SchemaValue {
        <TestField as ValueCodec<TestSchema>>::encode_value(value).unwrap()
    }

    fn put_value(delta_db: &DeltaDb, key: u32, value: u32) {
        delta_db
            .put::<TestSchema>(&TestCompositeField(key, 0, 0), &TestField(value))
            .unwrap();
    }

    fn check_value(delta_db: &DeltaDb, key: u32, expected_value: Option<u32>) {
        let actual_value = delta_db
            .read::<TestSchema>(&TestCompositeField(key, 0, 0))
            .unwrap()
            .map(|v| v.0);
        assert_eq!(expected_value, actual_value);
    }

    fn delete_key(delta_db: &DeltaDb, key: u32) {
        delta_db
            .delete::<TestSchema>(&TestCompositeField(key, 0, 0))
            .unwrap();
    }

    // End of test utils

    mod local_cache {
        use super::*;
        #[test]
        fn test_delta_db_put_get_delete() {
            // Simple lifecycle of a key-value pair:
            // 1. Put
            let tmpdir = tempfile::tempdir().unwrap();
            let db = open_db(tmpdir.path());

            let delta_db = DeltaDb::new(0, db, vec![]);

            // Not existing
            check_value(&delta_db, 1, None);
            // Simple value
            put_value(&delta_db, 1, 10);
            check_value(&delta_db, 1, Some(10));

            // Delete
            delete_key(&delta_db, 1);
            check_value(&delta_db, 1, None);
        }
    }

    mod reading {

        #[test]
        #[ignore = "TBD"]
        fn get_largest() {}

        #[test]
        #[ignore = "TBD"]
        fn get_prev() {}
    }

    mod iteration {}
}
