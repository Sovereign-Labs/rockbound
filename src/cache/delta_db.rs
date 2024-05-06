#![allow(dead_code)]
//! This module contains the next iteration of [`crate::cache::cache_db::CacheDb`]
use crate::cache::change_set::{ChangeSet, ChangeSetIter};
use crate::cache::SnapshotId;
use crate::iterator::{RawDbIter, ScanDirection};
use crate::schema::{KeyCodec, ValueCodec};
use crate::{
    Operation, PaginatedResponse, Schema, SchemaBatch, SchemaKey, SchemaValue, SeekKeyEncoder, DB,
};
use std::cmp::Ordering;
use std::iter::{Peekable, Rev};
use std::sync::{Arc, Mutex};

/// Intermediate step between [`crate::cache::cache_db::CacheDb`] and future DeltaDbReader
/// Supports "local writes". And for historical reading it uses `Vec<Arc<ChangeSet>`
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
        // but in case of deletion, we early return None
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
    fn iter_rev<S: Schema>(
        &self,
    ) -> anyhow::Result<DeltaDbIter<Rev<ChangeSetIter>, Rev<ChangeSetIter>>>
    {
        // Local iter
        let change_set = self
            .local_cache
            .lock()
            .expect("Local cache lock must not be poisoned");
        let local_cache_iter = change_set.iter::<S>().rev();

        // Snapshot iterators
        let snapshot_iterators = self
            .snapshots
            .iter()
            .map(|snapshot| snapshot.iter::<S>().rev())
            .collect::<Vec<_>>();

        // Db Iter
        let db_iter = self.db.raw_iter::<S>(ScanDirection::Backward)?;

        Ok(DeltaDbIter::new(
            local_cache_iter,
            snapshot_iterators,
            db_iter,
            ScanDirection::Backward,
        ))
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
        snapshot_iterators: Vec<SnapshotIter>,
        db_iter: RawDbIter<'a>,
        direction: ScanDirection,
    ) -> Self {
        Self {
            local_cache_iter: local_cache_iter.peekable(),
            snapshot_iterators: snapshot_iterators
                .into_iter()
                .map(|iter| iter.peekable())
                .collect(),
            db_iter: db_iter.peekable(),
            direction,
        }
    }
}

#[derive(Debug)]
enum DataLocation {
    Db,
    // Index inside `snapshot_iterators`
    Snapshot(usize),
    LocalCache,
}

impl<'a, LocalCacheIter, SnapshotIter> Iterator for DeltaDbIter<'a, LocalCacheIter, SnapshotIter>
where
    LocalCacheIter: Iterator<Item = (&'a SchemaKey, &'a Operation)>,
    SnapshotIter: Iterator<Item = (&'a SchemaKey, &'a Operation)>,
{
    // I wish
    // type Item = (&'a SchemaKey, &'a Operation);
    type Item = (SchemaKey, SchemaValue);

    fn next(&mut self) -> Option<Self::Item> {
        let next_values_size = self
            .snapshot_iterators
            .len()
            .checked_add(2)
            .unwrap_or_default();
        // In case of equal next key, this vector contains all iterator locations with this key.
        // It is filled with order: DB, Snapshots, LocalCache
        // Right most location has the most priority, so last value is taken
        // And all "other" locations are progressed without taking a value.
        let mut next_value_locations: Vec<DataLocation> = Vec::with_capacity(next_values_size);

        loop {
            // Reference to the last next value observed somewhere.
            // Used for comparing between iterators.

            let mut next_value: Option<&SchemaKey> = None;
            next_value_locations.clear();

            // Pick next values from each iterator
            // Going from DB to Snapshots to Local Cache
            if let Some((db_key, _)) = self.db_iter.peek() {
                next_value_locations.push(DataLocation::Db);
                next_value = Some(db_key);
            };

            // For each snapshot, pick it next value.
            // Update "global" next value accordingly.
            for (idx, iter) in self.snapshot_iterators.iter_mut().enumerate() {
                if let Some(&(peeked_key, _)) = iter.peek() {
                    let data_location = DataLocation::Snapshot(idx);
                    match next_value {
                        None => {
                            next_value_locations.push(data_location);
                            next_value = Some(peeked_key);
                        }
                        Some(next_key) => {
                            match (&self.direction, peeked_key.cmp(next_key)) {
                                (_, Ordering::Equal) => {
                                    next_value_locations.push(data_location);
                                }
                                (ScanDirection::Backward, Ordering::Greater)
                                | (ScanDirection::Forward, Ordering::Less) => {
                                    next_value = Some(peeked_key);
                                    next_value_locations.clear();
                                    next_value_locations.push(data_location);
                                }
                                // Key is not important for given an iterator direction
                                _ => {}
                            }
                        }
                    };
                }
            }

            // Pick the next value from local cache
            if let Some(&(peeked_key, _)) = self.local_cache_iter.peek() {
                match next_value {
                    None => {
                        next_value_locations.push(DataLocation::LocalCache);
                    }
                    Some(next_key) => {
                        match (&self.direction, peeked_key.cmp(next_key)) {
                            (_, Ordering::Equal) => {
                                next_value_locations.push(DataLocation::LocalCache);
                            }
                            (ScanDirection::Backward, Ordering::Greater)
                            | (ScanDirection::Forward, Ordering::Less) => {
                                next_value_locations.clear();
                                next_value_locations.push(DataLocation::LocalCache);
                            }
                            // Key is not important for given an iterator direction
                            _ => {}
                        }
                    }
                }
            }

            // All next values are observed at this point
            // Handling actual change of the iterator state.
            if let Some(latest_next_location) = next_value_locations.pop() {
                // First, move all iterators to the next position
                for location in &next_value_locations {
                    match location {
                        DataLocation::Db => {
                            let _ = self.db_iter.next().unwrap();
                        }
                        DataLocation::Snapshot(idx) => {
                            let _ = self.snapshot_iterators[*idx].next().unwrap();
                        }
                        DataLocation::LocalCache => {
                            let _ = self.local_cache_iter.next().unwrap();
                        }
                    }
                }

                // Handle next value
                match latest_next_location {
                    DataLocation::Db => {
                        let (key, value) = self.db_iter.next().unwrap();
                        return Some((key, value));
                    }
                    DataLocation::Snapshot(idx) => {
                        let (key, operation) = self.snapshot_iterators[idx].next().unwrap();
                        match operation {
                            Operation::Put { value } => {
                                return Some((key.to_vec(), value.to_vec()))
                            }
                            Operation::Delete => continue,
                        }
                    }
                    DataLocation::LocalCache => {
                        let (key, operation) = self.local_cache_iter.next().unwrap();
                        match operation {
                            Operation::Put { value } => {
                                return Some((key.to_vec(), value.to_vec()))
                            }
                            Operation::Delete => continue,
                        }
                    }
                }
            } else {
                break;
            }
        }
        None
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

    mod iteration {
        use crate::cache::delta_db::tests::open_db;
        use crate::cache::delta_db::DeltaDb;

        #[test]
        fn test_empty_iterator() {
            let tmpdir = tempfile::tempdir().unwrap();
            let db = open_db(tmpdir.path());

            let delta_db = DeltaDb::new(0, db, vec![]);

            let iterator = delta_db.iter_rev()?;
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
}
