//! This module is main entry point into cache layering subsystem.
use std::iter::Peekable;
use std::sync::Mutex;

use crate::cache::cache_container::CacheContainer;
use crate::cache::change_set::ChangeSet;
use crate::cache::{PaginatedResponse, SnapshotId};
use crate::iterator::ScanDirection;
use crate::schema::KeyDecoder;
use crate::{
    KeyCodec, Operation, ReadOnlyLock, Schema, SchemaBatch, SchemaKey, SchemaValue, SeekKeyEncoder,
    ValueCodec,
};

/// Cache layer that stores all writes locally and also able to access "previous" operations.
#[derive(Debug)]
pub struct CacheDb {
    local_cache: Mutex<ChangeSet>,
    db: ReadOnlyLock<CacheContainer>,
}

impl CacheDb {
    /// Create new [`CacheDb`] pointing to given [`CacheContainer`]
    pub fn new(id: SnapshotId, cache_container: ReadOnlyLock<CacheContainer>) -> Self {
        Self {
            local_cache: Mutex::new(ChangeSet::new(id)),
            db: cache_container,
        }
    }

    /// Store a value in local cache
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

    /// Overwrites inner cache with new, while retaining reference to parent
    pub fn overwrite_change_set(&self, other: CacheDb) {
        let mut this_cache = self.local_cache.lock().unwrap();
        let other_cache = other.local_cache.into_inner().unwrap();
        *this_cache = other_cache;
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

        // 2. Check parent
        let parent = self.db.read().expect("Parent lock must not be poisoned");
        parent.get::<S>(local_cache.id(), key)
    }

    /// Get a value from current snapshot, its parents or underlying database
    pub async fn read_async<S: Schema>(
        &self,
        key: &impl KeyCodec<S>,
    ) -> anyhow::Result<Option<S::Value>> {
        tokio::task::block_in_place(|| self.read(key))
    }

    /// Get value of largest key written value for given [`Schema`]
    pub fn get_largest<S: Schema>(&self) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        let change_set = self
            .local_cache
            .lock()
            .expect("SchemaBatch lock must not be poisoned");
        let local_cache_iter = change_set.iter::<S>().rev();

        let parent = self.db.read().expect("Parent lock must not be poisoned");

        let parent_iter = parent.rev_iter::<S>(change_set.id())?;

        let mut combined_iter: CacheDbIter<'_, _, _> = CacheDbIter {
            local_cache_iter: local_cache_iter.peekable(),
            parent_iter: parent_iter.peekable(),
            direction: ScanDirection::Backward,
        };

        if let Some((key, value)) = combined_iter.next() {
            let key = S::Key::decode_key(&key)?;
            let value = S::Value::decode_value(&value)?;
            return Ok(Some((key, value)));
        }

        Ok(None)
    }

    /// Get value of largest key written value for given [`Schema`]
    pub async fn get_largest_async<S: Schema>(&self) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        tokio::task::block_in_place(|| self.get_largest::<S>())
    }

    /// Get largest value in [`Schema`] that is smaller than give `seek_key`
    pub fn get_prev<S: Schema>(
        &self,
        seek_key: &impl SeekKeyEncoder<S>,
    ) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        let seek_key = seek_key.encode_seek_key()?;
        let range = ..=seek_key;
        let change_set = self
            .local_cache
            .lock()
            .expect("Local cache lock must not be poisoned");
        let local_cache_iter = change_set.iter_range::<S>(range.clone()).rev();

        let parent = self
            .db
            .read()
            .expect("Parent snapshots lock must not be poisoned");
        let parent_iter = parent.rev_iter_range::<S>(change_set.id(), range)?;

        let mut combined_iter: CacheDbIter<'_, _, _> = CacheDbIter {
            local_cache_iter: local_cache_iter.peekable(),
            parent_iter: parent_iter.peekable(),
            direction: ScanDirection::Backward,
        };

        if let Some((key, value)) = combined_iter.next() {
            let key = S::Key::decode_key(&key)?;
            let value = S::Value::decode_value(&value)?;
            return Ok(Some((key, value)));
        }
        Ok(None)
    }

    /// Get largest value in [`Schema`] that is smaller than give `seek_key`
    pub async fn get_prev_async<S: Schema>(
        &self,
        seek_key: &impl SeekKeyEncoder<S>,
    ) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        tokio::task::block_in_place(|| self.get_prev(seek_key))
    }

    /// Get `n` keys >= `seek_key`
    pub fn get_n_from_first_match<S: Schema>(
        &self,
        seek_key: &impl SeekKeyEncoder<S>,
        n: usize,
    ) -> anyhow::Result<PaginatedResponse<S>> {
        let seek_key = seek_key.encode_seek_key()?;
        let range = seek_key..;
        let change_set = self
            .local_cache
            .lock()
            .expect("Local cache lock must not be poisoned");
        let local_cache_iter = change_set.iter_range::<S>(range.clone());

        let parent = self
            .db
            .read()
            .expect("Parent snapshots lock must not be poisoned");
        let parent_iter = parent.iter_range::<S>(change_set.id(), range)?;

        let mut combined_iter: CacheDbIter<'_, _, _> = CacheDbIter {
            local_cache_iter: local_cache_iter.peekable(),
            parent_iter: parent_iter.peekable(),
            direction: ScanDirection::Forward,
        };

        let results: Vec<(S::Key, S::Value)> = combined_iter
            .by_ref()
            .filter_map(|(key_bytes, value_bytes)| {
                let key = S::Key::decode_key(&key_bytes).ok()?;
                let value = S::Value::decode_value(&value_bytes).ok()?;
                Some((key, value))
            })
            .take(n)
            .collect();

        let next_start_key: Option<S::Key> = combined_iter
            .next()
            .and_then(|(key_bytes, _)| S::Key::decode_key(&key_bytes).ok());

        Ok(PaginatedResponse {
            key_value: results,
            next: next_start_key,
        })
    }

    /// Get `n` keys >= `seek_key`
    pub async fn get_n_from_first_match_async<S: Schema>(
        &self,
        seek_key: &impl SeekKeyEncoder<S>,
        n: usize,
    ) -> anyhow::Result<PaginatedResponse<S>> {
        tokio::task::block_in_place(|| self.get_n_from_first_match(seek_key, n))
    }

    /// Get a clone of internal ChangeSet
    pub fn clone_change_set(&self) -> ChangeSet {
        let change_set = self
            .local_cache
            .lock()
            .expect("Local change set lock is poisoned");
        change_set.clone()
    }

    /// Collects all key-value pairs in given range, from smallest to largest.
    pub fn collect_in_range<S: Schema, Sk: SeekKeyEncoder<S>>(
        &self,
        range: std::ops::Range<Sk>,
    ) -> anyhow::Result<Vec<(S::Key, S::Value)>> {
        let lower_bound = range.start.encode_seek_key()?;
        let upper_bound = range.end.encode_seek_key()?;
        let range = lower_bound..upper_bound;
        let change_set = self
            .local_cache
            .lock()
            .expect("Local cache lock must not be poisoned");
        let local_cache_iter = change_set.iter_range::<S>(range.clone());

        let parent = self
            .db
            .read()
            .expect("Parent snapshots lock must not be poisoned");
        let parent_iter = parent.iter_range::<S>(change_set.id(), range)?;

        let combined_iter: CacheDbIter<'_, _, _> = CacheDbIter {
            local_cache_iter: local_cache_iter.peekable(),
            parent_iter: parent_iter.peekable(),
            direction: ScanDirection::Forward,
        };

        let result = combined_iter
            .map(|(key, value)| {
                let key = S::Key::decode_key(&key).unwrap();
                let value = S::Value::decode_value(&value).unwrap();
                (key, value)
            })
            .collect();

        Ok(result)
    }

    /// Collects all key-value pairs in given range, from smallest to largest.
    pub async fn collect_in_range_async<S: Schema, Sk: SeekKeyEncoder<S>>(
        &self,
        range: std::ops::Range<Sk>,
    ) -> anyhow::Result<Vec<(S::Key, S::Value)>> {
        tokio::task::block_in_place(|| self.collect_in_range(range))
    }
}

/// Iterator over [`CacheDb`] that combines local cache and parent iterators.
/// Prioritizes local cache over parent in case of equal keys.
/// This is because local cache operations are considered to be newer.
struct CacheDbIter<'a, LocalIter, ParentIter>
where
    LocalIter: Iterator<Item = (&'a SchemaKey, &'a Operation)>,
    ParentIter: Iterator<Item = (SchemaKey, SchemaValue)>,
{
    local_cache_iter: Peekable<LocalIter>,
    parent_iter: Peekable<ParentIter>,
    direction: ScanDirection,
}

impl<'a, LocalIter, ParentIter> Iterator for CacheDbIter<'a, LocalIter, ParentIter>
where
    LocalIter: Iterator<Item = (&'a SchemaKey, &'a Operation)>,
    ParentIter: Iterator<Item = (SchemaKey, SchemaValue)>,
{
    type Item = (SchemaKey, SchemaValue);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let Some(&local) = self.local_cache_iter.peek() else {
                // Local iterator is exhausted; all reads go to the parent.
                return self.parent_iter.next();
            };

            if let Some(parent) = self.parent_iter.peek() {
                // Check if the next parent key has priority over the local key, based on
                // the scan direction.
                if ((local.0 > &parent.0) && (self.direction == ScanDirection::Forward))
                    || ((local.0 < &parent.0) && (self.direction == ScanDirection::Backward))
                {
                    return self.parent_iter.next();
                } else if local.0 == &parent.0 {
                    // If keys are equal, we must consume the next parent value
                    // before we can read from the local cache (or they will
                    // get out of sync).
                    self.parent_iter.next();
                }
            }

            // At this point, we've determined that we wish to read from the
            // local cache.
            self.local_cache_iter.next();
            match local.1 {
                // No value to read from the local cache, skip it.
                Operation::Delete => continue,
                Operation::Put { value } => return Some((local.0.clone(), value.clone())),
            }
        }
    }
}

impl From<CacheDb> for ChangeSet {
    fn from(value: CacheDb) -> Self {
        value
            .local_cache
            .into_inner()
            .expect("Internal cache lock is poisoned")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::{Arc, RwLock};

    use rocksdb::DEFAULT_COLUMN_FAMILY_NAME;

    use super::*;
    use crate::cache::cache_db::CacheContainer;
    use crate::schema::KeyEncoder;
    use crate::test::{TestCompositeField, TestField};
    use crate::{define_schema, DB};
    define_schema!(TestSchema, TestCompositeField, TestField, "TestCF");

    mod local_cache {
        use super::*;

        #[test]
        fn test_concurrent_operations() {
            let tmpdir = tempfile::tempdir().unwrap();
            let db = open_db(tmpdir.path());

            let cache_container = CacheContainer::orphan(db);

            let cache_db = Arc::new(CacheDb::new(
                0,
                Arc::new(RwLock::new(cache_container)).into(),
            ));

            let mut handles = vec![];
            for i in 0..10 {
                let cache_db_clone = cache_db.clone();
                let handle = std::thread::spawn(move || {
                    for j in 0..10 {
                        let key = TestCompositeField(j + 1, j + 2, j + 3);
                        let value = TestField(i * 10 + j + 4);
                        cache_db_clone.put::<TestSchema>(&key, &value).unwrap();
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().expect("Thread panicked");
            }
            for j in 0..10 {
                let key = TestCompositeField(j + 1, j + 2, j + 3);
                let value = cache_db.read::<TestSchema>(&key).unwrap().unwrap().0;
                let possible_range = (j + 4)..(100 + j + 4);
                assert!(possible_range.contains(&value));
                let rem = value - (j + 4);
                assert_eq!(0, rem % 10);
            }
        }
    }

    fn encode_key(key: &TestCompositeField) -> SchemaKey {
        <TestCompositeField as KeyEncoder<TestSchema>>::encode_key(key).unwrap()
    }

    fn encode_value(value: &TestField) -> SchemaValue {
        <TestField as ValueCodec<TestSchema>>::encode_value(value).unwrap()
    }

    fn open_db(dir: impl AsRef<Path>) -> DB {
        let column_families = vec![DEFAULT_COLUMN_FAMILY_NAME, TestSchema::COLUMN_FAMILY_NAME];
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        DB::open(dir, "test", column_families, &db_opts).expect("Failed to open DB.")
    }

    #[test]
    fn test_db_snapshot_iterator_empty() {
        let tmpdir = tempfile::tempdir().unwrap();
        let db = open_db(tmpdir.path());

        let local_cache = SchemaBatch::new();
        let local_cache_iter = local_cache.iter::<TestSchema>().peekable();

        let cache_container = CacheContainer::orphan(db);
        let parent_iter = cache_container.iter::<TestSchema>(0).unwrap().peekable();

        let combined_iter: CacheDbIter<'_, _, _> = CacheDbIter {
            local_cache_iter,
            parent_iter,
            direction: ScanDirection::Forward,
        };

        let values: Vec<(SchemaKey, SchemaValue)> = combined_iter.collect();
        assert!(values.is_empty())
    }

    #[test]
    fn test_db_snapshot_iterator_values() {
        let tmpdir = tempfile::tempdir().unwrap();
        let db = open_db(tmpdir.path());

        let k0 = TestCompositeField(0, 0, 0);
        let k1 = TestCompositeField(0, 1, 0);
        let k2 = TestCompositeField(0, 1, 2);
        let k3 = TestCompositeField(3, 1, 0);
        let k4 = TestCompositeField(3, 2, 0);

        let mut older_change_set = ChangeSet::new(0);
        older_change_set
            .operations
            .put::<TestSchema>(&k2, &TestField(2))
            .unwrap();
        older_change_set
            .operations
            .put::<TestSchema>(&k1, &TestField(1))
            .unwrap();
        older_change_set
            .operations
            .put::<TestSchema>(&k4, &TestField(4))
            .unwrap();
        older_change_set
            .operations
            .put::<TestSchema>(&k3, &TestField(3))
            .unwrap();

        let to_parent: Arc<RwLock<HashMap<SnapshotId, SnapshotId>>> =
            Arc::new(RwLock::new(Default::default()));
        {
            let mut parent = to_parent.write().unwrap();
            parent.insert(1, 0);
        }
        let mut cache_container = CacheContainer::new(db, to_parent.into());
        cache_container.add_snapshot(older_change_set).unwrap();

        let parent_iter = cache_container.iter::<TestSchema>(1).unwrap();

        let mut local_change_set = ChangeSet::new(1);

        local_change_set
            .operations
            .delete::<TestSchema>(&k3)
            .unwrap();
        local_change_set
            .operations
            .put::<TestSchema>(&k0, &TestField(100))
            .unwrap();
        local_change_set
            .operations
            .put::<TestSchema>(&k1, &TestField(10))
            .unwrap();
        local_change_set
            .operations
            .put::<TestSchema>(&k2, &TestField(20))
            .unwrap();

        let local_cache_iter = local_change_set.iter::<TestSchema>();

        let combined_iter: CacheDbIter<'_, _, _> = CacheDbIter {
            local_cache_iter: local_cache_iter.peekable(),
            parent_iter: parent_iter.peekable(),
            direction: ScanDirection::Forward,
        };

        let actual_values: Vec<(SchemaKey, SchemaValue)> = combined_iter.collect();
        let expected_values = vec![
            (encode_key(&k0), encode_value(&TestField(100))),
            (encode_key(&k1), encode_value(&TestField(10))),
            (encode_key(&k2), encode_value(&TestField(20))),
            (encode_key(&k4), encode_value(&TestField(4))),
        ];

        assert_eq!(expected_values, actual_values);
    }

    fn put_value(cache_db: &CacheDb, key: u32, value: u32) {
        cache_db
            .put::<TestSchema>(&TestCompositeField(key, 0, 0), &TestField(value))
            .unwrap();
    }

    fn check_value(cache_db: &CacheDb, key: u32, expected_value: Option<u32>) {
        let actual_value = cache_db
            .read::<TestSchema>(&TestCompositeField(key, 0, 0))
            .unwrap()
            .map(|v| v.0);
        assert_eq!(expected_value, actual_value);
    }

    #[test]
    fn test_iterators_complete() {
        let tmpdir = tempfile::tempdir().unwrap();
        let db = open_db(tmpdir.path());

        let to_parent: Arc<RwLock<HashMap<SnapshotId, SnapshotId>>> =
            Arc::new(RwLock::new(Default::default()));
        {
            let mut parent = to_parent.write().unwrap();
            parent.insert(2, 1);
            parent.insert(3, 2);
            parent.insert(4, 3);
        }

        let cache_container = Arc::new(RwLock::new(CacheContainer::new(db, to_parent.into())));

        let db_values = CacheDb::new(0, cache_container.clone().into());

        put_value(&db_values, 2, 1);
        put_value(&db_values, 4, 1);
        put_value(&db_values, 3, 9);
        {
            let mut cache_container = cache_container.write().unwrap();
            cache_container.add_snapshot(db_values.into()).unwrap();
            cache_container.commit_snapshot(&0).unwrap();
            assert!(cache_container.is_empty());
        }

        let cache_db_1 = CacheDb::new(1, cache_container.clone().into());
        put_value(&cache_db_1, 1, 8);
        put_value(&cache_db_1, 4, 2);
        put_value(&cache_db_1, 5, 7);
        put_value(&cache_db_1, 8, 3);
        {
            let mut cache_container = cache_container.write().unwrap();
            cache_container.add_snapshot(cache_db_1.into()).unwrap();
        }
        let cache_db_2 = CacheDb::new(2, cache_container.clone().into());
        put_value(&cache_db_2, 2, 6);
        cache_db_2
            .delete::<TestSchema>(&TestCompositeField(4, 0, 0))
            .unwrap();
        put_value(&cache_db_2, 9, 4);
        put_value(&cache_db_2, 10, 2);
        {
            let mut cache_container = cache_container.write().unwrap();
            cache_container.add_snapshot(cache_db_2.into()).unwrap();
        }

        let cache_db_3 = CacheDb::new(3, cache_container.into());
        put_value(&cache_db_3, 1, 2);
        put_value(&cache_db_3, 8, 6);
        cache_db_3
            .delete::<TestSchema>(&TestCompositeField(9, 0, 0))
            .unwrap();
        put_value(&cache_db_3, 12, 1);

        // Checking
        check_value(&cache_db_3, 5, Some(7));
        check_value(&cache_db_3, 4, None);

        let actual_kv_sorted = cache_db_3
            .collect_in_range::<TestSchema, TestCompositeField>(
                TestCompositeField(0, 0, 0)..TestCompositeField(100, 0, 0),
            )
            .unwrap()
            .into_iter()
            .map(|(k, v)| (k.0, v.0))
            .collect::<Vec<(u32, u32)>>();

        let expected_kv_sorted = vec![(1, 2), (2, 6), (3, 9), (5, 7), (8, 6), (10, 2), (12, 1)];

        assert_eq!(expected_kv_sorted, actual_kv_sorted);
    }

    // TODO: Proptest here
}
