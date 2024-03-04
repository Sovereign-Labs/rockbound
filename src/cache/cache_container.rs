//! CacheContainer module responsible for correct traversal of cache layers.
use std::cmp::Ordering;
use std::collections::{btree_map, HashMap};
use std::iter::{Peekable, Rev};

use crate::cache::cache_container::DataLocation::Snapshot;
use crate::cache::change_set::ChangeSet;
use crate::cache::SnapshotId;
use crate::iterator::ScanDirection;
use crate::schema::{KeyCodec, ValueCodec};
use crate::{Operation, RawDbIter, ReadOnlyLock, Schema, SchemaKey, SchemaValue, DB};

/// Holds collection of [`ChangeSet`]'s associated with particular Snapshot
/// and knows how to traverse them.
/// Managed externally.
/// Should be managed carefully, because discrepancy between `snapshots` and `to_parent` leads to panic
/// Ideally owner of writable reference to parent nad owner of cache container manages both correctly.
#[derive(Debug)]
pub struct CacheContainer {
    db: DB,
    /// Set of [`ChangeSet`]s of data per individual database per snapshot
    snapshots: HashMap<SnapshotId, ChangeSet>,
    /// Hierarchical
    /// Shared between all CacheContainers and managed by StorageManager
    to_parent: ReadOnlyLock<HashMap<SnapshotId, SnapshotId>>,
}

impl CacheContainer {
    /// Create CacheContainer pointing go given DB and Snapshot ID relations
    pub fn new(db: DB, to_parent: ReadOnlyLock<HashMap<SnapshotId, SnapshotId>>) -> Self {
        Self {
            db,
            snapshots: HashMap::new(),
            to_parent,
        }
    }

    /// Create instance of snapshot manager, when it does not have connection to snapshots tree
    /// So it only reads from database.
    #[cfg(feature = "test-utils")]
    pub fn orphan(db: DB) -> Self {
        Self {
            db,
            snapshots: HashMap::new(),
            to_parent: std::sync::Arc::new(std::sync::RwLock::new(Default::default())).into(),
        }
    }

    /// Adds Snapshot to the collection.
    /// Please note that caller must update its own reference of `to_parent`
    /// After adding snapshot.
    /// Ideally these operations should be atomic
    pub fn add_snapshot(&mut self, snapshot: ChangeSet) -> anyhow::Result<()> {
        let snapshot_id = snapshot.id();
        if self.snapshots.contains_key(&snapshot_id) {
            anyhow::bail!("Attempt to double save same snapshot id={}", snapshot_id);
        }
        self.snapshots.insert(snapshot_id, snapshot);
        Ok(())
    }

    /// Removes snapshot from collection
    /// This should happen **after** `to_parent` is updated
    pub fn discard_snapshot(&mut self, snapshot_id: &SnapshotId) -> Option<ChangeSet> {
        self.snapshots.remove(snapshot_id)
    }

    /// Writes snapshot to the underlying database
    /// Snapshot id should be removed from `to_parent` atomically.
    pub fn commit_snapshot(&mut self, snapshot_id: &SnapshotId) -> anyhow::Result<()> {
        let snapshot = self.snapshots.remove(snapshot_id).ok_or_else(|| {
            anyhow::anyhow!("Attempt to commit unknown snapshot id={}", snapshot_id)
        })?;
        self.db.write_schemas(snapshot.into())
    }

    /// Indicates, if CacheContainer has any snapshots in memory.
    /// Does not mean that underlying database is empty.
    pub fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }

    /// Helper method to check if snapshot with given ID has been added already
    pub fn contains_snapshot(&self, snapshot_id: &SnapshotId) -> bool {
        self.snapshots.contains_key(snapshot_id)
    }

    pub(crate) fn get<S: Schema>(
        &self,
        mut snapshot_id: SnapshotId,
        key: &impl KeyCodec<S>,
    ) -> anyhow::Result<Option<S::Value>> {
        while let Some(parent_snapshot_id) = self.to_parent.read().unwrap().get(&snapshot_id) {
            let parent_snapshot = self
                .snapshots
                .get(parent_snapshot_id)
                .expect("Inconsistency between `self.snapshots` and `self.to_parent`: snapshot is missing in container");

            // Some operation has been found
            if let Some(operation) = parent_snapshot.get(key)? {
                return match operation {
                    Operation::Put { value } => Ok(Some(S::Value::decode_value(value)?)),
                    Operation::Delete => Ok(None),
                };
            }

            snapshot_id = *parent_snapshot_id;
        }
        self.db.get(key)
    }

    fn snapshot_iterators<S: Schema>(
        &self,
        mut snapshot_id: SnapshotId,
    ) -> Vec<btree_map::Iter<SchemaKey, Operation>> {
        let mut snapshot_iterators = vec![];
        let to_parent = self.to_parent.read().unwrap();
        while let Some(parent_snapshot_id) = to_parent.get(&snapshot_id) {
            let parent_snapshot = self
                .snapshots
                .get(parent_snapshot_id)
                .expect("Inconsistency between `self.snapshots` and `self.to_parent`");

            snapshot_iterators.push(parent_snapshot.iter::<S>());

            snapshot_id = *parent_snapshot_id;
        }

        snapshot_iterators.reverse();
        snapshot_iterators
    }

    // Allow this unused method, for future use
    #[allow(dead_code)]
    pub(crate) fn iter<S: Schema>(
        &self,
        snapshot_id: SnapshotId,
    ) -> anyhow::Result<CacheContainerIter<btree_map::Iter<SchemaKey, Operation>>> {
        let snapshot_iterators = self.snapshot_iterators::<S>(snapshot_id);
        let db_iter = self.db.raw_iter::<S>(ScanDirection::Forward)?;

        Ok(CacheContainerIter::new(
            db_iter,
            snapshot_iterators,
            ScanDirection::Forward,
        ))
    }

    /// Returns iterator over keys in given [`Schema`] among all snapshots and DB in reverse lexicographical order
    pub(crate) fn rev_iter<S: Schema>(
        &self,
        snapshot_id: SnapshotId,
    ) -> anyhow::Result<CacheContainerIter<Rev<btree_map::Iter<SchemaKey, Operation>>>> {
        let snapshot_iterators = self
            .snapshot_iterators::<S>(snapshot_id)
            .into_iter()
            .map(Iterator::rev)
            .collect();
        let db_iter = self.db.raw_iter::<S>(ScanDirection::Backward)?;

        Ok(CacheContainerIter::new(
            db_iter,
            snapshot_iterators,
            ScanDirection::Backward,
        ))
    }

    pub(crate) fn iter_range<S: Schema>(
        &self,
        mut snapshot_id: SnapshotId,
        range: impl std::ops::RangeBounds<SchemaKey> + Clone,
    ) -> anyhow::Result<CacheContainerIter<btree_map::Range<SchemaKey, Operation>>> {
        let mut snapshot_iterators = vec![];
        let to_parent = self.to_parent.read().unwrap();
        while let Some(parent_snapshot_id) = to_parent.get(&snapshot_id) {
            let parent_snapshot = self
                .snapshots
                .get(parent_snapshot_id)
                .expect("Inconsistency between `self.snapshots` and `self.to_parent`");

            snapshot_iterators.push(parent_snapshot.iter_range::<S>(range.clone()));

            snapshot_id = *parent_snapshot_id;
        }

        snapshot_iterators.reverse();

        let db_iter = self.db.raw_iter_range::<S>(range, ScanDirection::Forward)?;
        Ok(CacheContainerIter::new(
            db_iter,
            snapshot_iterators,
            ScanDirection::Forward,
        ))
    }

    pub(crate) fn rev_iter_range<S: Schema>(
        &self,
        mut snapshot_id: SnapshotId,
        range: impl std::ops::RangeBounds<SchemaKey> + Clone,
    ) -> anyhow::Result<CacheContainerIter<Rev<btree_map::Range<SchemaKey, Operation>>>> {
        let mut snapshot_iterators = vec![];
        let to_parent = self.to_parent.read().unwrap();
        while let Some(parent_snapshot_id) = to_parent.get(&snapshot_id) {
            let parent_snapshot = self
                .snapshots
                .get(parent_snapshot_id)
                .expect("Inconsistency between `self.snapshots` and `self.to_parent`");

            snapshot_iterators.push(parent_snapshot.iter_range::<S>(range.clone()).rev());
            snapshot_id = *parent_snapshot_id;
        }

        snapshot_iterators.reverse();
        let db_iter = self
            .db
            .raw_iter_range::<S>(range, ScanDirection::Backward)?;

        Ok(CacheContainerIter::new(
            db_iter,
            snapshot_iterators,
            ScanDirection::Backward,
        ))
    }
}

/// [`Iterator`] over keys in given [`Schema`] in all snapshots in reverse
/// lexicographical order.
pub(crate) struct CacheContainerIter<'a, SnapshotIter>
where
    SnapshotIter: Iterator<Item = (&'a SchemaKey, &'a Operation)>,
{
    db_iter: Peekable<RawDbIter<'a>>,
    snapshot_iterators: Vec<Peekable<SnapshotIter>>,
    next_value_locations: Vec<DataLocation>,
    direction: ScanDirection,
}

impl<'a, SnapshotIter> CacheContainerIter<'a, SnapshotIter>
where
    SnapshotIter: Iterator<Item = (&'a SchemaKey, &'a Operation)>,
{
    fn new(
        db_iter: RawDbIter<'a>,
        snapshot_iterators: Vec<SnapshotIter>,
        direction: ScanDirection,
    ) -> Self {
        let max_values_size = snapshot_iterators.len().checked_add(1).unwrap_or_default();
        Self {
            db_iter: db_iter.peekable(),
            snapshot_iterators: snapshot_iterators
                .into_iter()
                .map(|iter| iter.peekable())
                .collect(),
            next_value_locations: Vec::with_capacity(max_values_size),
            direction,
        }
    }
}

#[derive(Debug)]
enum DataLocation {
    Db,
    // Index inside `snapshot_iterators`
    Snapshot(usize),
}

impl<'a, SnapshotIter> Iterator for CacheContainerIter<'a, SnapshotIter>
where
    SnapshotIter: Iterator<Item = (&'a SchemaKey, &'a Operation)>,
{
    type Item = (SchemaKey, SchemaValue);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let mut next_value: Option<&SchemaKey> = None;
            self.next_value_locations.clear();
            if let Some((db_key, _)) = self.db_iter.peek() {
                self.next_value_locations.push(DataLocation::Db);
                next_value = Some(db_key);
            };

            for (idx, iter) in self.snapshot_iterators.iter_mut().enumerate() {
                if let Some(&(peeked_key, _)) = iter.peek() {
                    match next_value {
                        None => {
                            self.next_value_locations.push(Snapshot(idx));
                            next_value = Some(peeked_key);
                        }
                        Some(next_key) => match (&self.direction, peeked_key.cmp(next_key)) {
                            (ScanDirection::Backward, Ordering::Greater)
                            | (ScanDirection::Forward, Ordering::Less) => {
                                next_value = Some(peeked_key);
                                self.next_value_locations.clear();
                                self.next_value_locations.push(Snapshot(idx));
                            }
                            (_, Ordering::Equal) => {
                                self.next_value_locations.push(Snapshot(idx));
                            }
                            _ => {}
                        },
                    };
                }
            }

            // Rightmost location is from the latest snapshot, so it has priority over all past snapshot/DB
            if let Some(latest_next_location) = self.next_value_locations.pop() {
                // Move all iterators to next value
                for location in &self.next_value_locations {
                    match location {
                        DataLocation::Db => {
                            let _ = self.db_iter.next().unwrap();
                        }
                        Snapshot(idx) => {
                            let _ = self.snapshot_iterators[*idx].next().unwrap();
                        }
                    }
                }

                // Handle next value
                match latest_next_location {
                    DataLocation::Db => {
                        let (key, value) = self.db_iter.next().unwrap();
                        return Some((key, value));
                    }
                    Snapshot(idx) => {
                        let (key, operation) = self.snapshot_iterators[idx].next().unwrap();
                        match operation {
                            Operation::Put { value } => {
                                return Some((key.to_vec(), value.to_vec()))
                            }
                            Operation::Delete => continue,
                        }
                    }
                };
            } else {
                break;
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock, RwLockReadGuard};

    use proptest::prelude::*;

    use crate::cache::cache_container::{CacheContainer, CacheContainerIter};
    use crate::cache::cache_db::CacheDb;
    use crate::iterator::ScanDirection;
    use crate::schema::{KeyDecoder, KeyEncoder, Schema, ValueCodec};
    use crate::test::TestField;
    use crate::{define_schema, Operation, SchemaBatch, SchemaKey, SchemaValue, DB};

    const DUMMY_STATE_CF: &str = "DummyStateCF";

    define_schema!(DummyStateSchema, TestField, TestField, DUMMY_STATE_CF);
    type S = DummyStateSchema;

    fn create_test_db(path: &std::path::Path) -> DB {
        let tables = vec![DUMMY_STATE_CF.to_string()];
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        DB::open(path, "test_db", tables, &db_opts).unwrap()
    }

    #[test]
    fn test_empty() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = create_test_db(tempdir.path());
        let snapshot_manager =
            CacheContainer::new(db, Arc::new(RwLock::new(HashMap::new())).into());
        assert!(snapshot_manager.is_empty());
    }

    #[test]
    fn test_add_and_discard_snapshot() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = create_test_db(tempdir.path());
        let to_parent = Arc::new(RwLock::new(HashMap::new()));
        let snapshot_manager = Arc::new(RwLock::new(CacheContainer::new(
            db,
            to_parent.clone().into(),
        )));

        let snapshot_id = 1;
        let db_snapshot = CacheDb::new(snapshot_id, snapshot_manager.clone().into());

        {
            let mut snapshot_manager = snapshot_manager.write().unwrap();
            snapshot_manager.add_snapshot(db_snapshot.into()).unwrap();
            assert!(!snapshot_manager.is_empty());
            snapshot_manager.discard_snapshot(&snapshot_id);
            assert!(snapshot_manager.is_empty());
        }
    }

    #[test]
    #[should_panic(expected = "Attempt to double save same snapshot")]
    fn test_add_twice() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = create_test_db(tempdir.path());
        let to_parent = Arc::new(RwLock::new(HashMap::new()));
        let snapshot_manager = Arc::new(RwLock::new(CacheContainer::new(
            db,
            to_parent.clone().into(),
        )));

        let snapshot_id = 1;
        // Both share the same ID
        let db_snapshot_1 = CacheDb::new(snapshot_id, snapshot_manager.clone().into());
        let db_snapshot_2 = CacheDb::new(snapshot_id, snapshot_manager.clone().into());

        {
            let mut snapshot_manager = snapshot_manager.write().unwrap();
            snapshot_manager.add_snapshot(db_snapshot_1.into()).unwrap();
            assert!(!snapshot_manager.is_empty());
            snapshot_manager.add_snapshot(db_snapshot_2.into()).unwrap();
        }
    }

    #[test]
    #[should_panic(expected = "Attempt to commit unknown snapshot")]
    fn test_commit_unknown() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = create_test_db(tempdir.path());
        let to_parent = Arc::new(RwLock::new(HashMap::new()));
        let mut cache_container = CacheContainer::new(db, to_parent.clone().into());

        cache_container.commit_snapshot(&1).unwrap();
    }

    #[test]
    fn test_discard_unknown() {
        // Discarding unknown snapshots are fine.
        // As it possible that caller didn't save it previously.
        let tempdir = tempfile::tempdir().unwrap();
        let db = create_test_db(tempdir.path());
        let to_parent = Arc::new(RwLock::new(HashMap::new()));
        let mut cache_container = CacheContainer::new(db, to_parent.clone().into());

        cache_container.discard_snapshot(&1);
    }

    #[test]
    fn test_commit_snapshot() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = create_test_db(tempdir.path());
        let to_parent = Arc::new(RwLock::new(HashMap::new()));
        let snapshot_manager = Arc::new(RwLock::new(CacheContainer::new(
            db,
            to_parent.clone().into(),
        )));

        let snapshot_id = 1;
        let db_snapshot = CacheDb::new(snapshot_id, snapshot_manager.clone().into());

        {
            let mut snapshot_manager = snapshot_manager.write().unwrap();
            snapshot_manager.add_snapshot(db_snapshot.into()).unwrap();
            let result = snapshot_manager.commit_snapshot(&snapshot_id);
            assert!(result.is_ok());
            assert!(snapshot_manager.is_empty());
        }
    }

    #[test]
    fn test_query_unknown_snapshot_id() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = create_test_db(tempdir.path());
        let to_parent = Arc::new(RwLock::new(HashMap::new()));
        let snapshot_manager = CacheContainer::new(db, to_parent.clone().into());
        assert_eq!(None, snapshot_manager.get::<S>(1, &TestField(1)).unwrap());
    }

    #[test]
    fn test_query_genesis_snapshot() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = create_test_db(tempdir.path());
        let to_parent = Arc::new(RwLock::new(HashMap::new()));

        let one = TestField(1);
        let two = TestField(2);
        let three = TestField(3);

        let mut db_data = SchemaBatch::new();
        db_data.put::<S>(&one, &one).unwrap();
        db_data.put::<S>(&three, &three).unwrap();
        db.write_schemas(db_data).unwrap();

        let snapshot_manager = Arc::new(RwLock::new(CacheContainer::new(
            db,
            to_parent.clone().into(),
        )));

        let db_snapshot = CacheDb::new(1, snapshot_manager.clone().into());
        db_snapshot.put::<S>(&two, &two).unwrap();
        db_snapshot.delete::<S>(&three).unwrap();

        {
            let mut snapshot_manager = snapshot_manager.write().unwrap();
            snapshot_manager.add_snapshot(db_snapshot.into()).unwrap();

            // Effectively querying database:
            assert_eq!(Some(one), snapshot_manager.get::<S>(1, &one).unwrap());
            assert_eq!(None, snapshot_manager.get::<S>(1, &two).unwrap());
            assert_eq!(Some(three), snapshot_manager.get::<S>(1, &three).unwrap());
        }
    }

    #[test]
    fn test_query_lifecycle() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = create_test_db(tempdir.path());
        let to_parent = Arc::new(RwLock::new(HashMap::new()));
        {
            //            / -> 6 -> 7
            // DB -> 1 -> 2 -> 3
            //       \ -> 4 -> 5
            let mut edit = to_parent.write().unwrap();
            edit.insert(3, 2);
            edit.insert(2, 1);
            edit.insert(4, 1);
            edit.insert(5, 4);
            edit.insert(6, 2);
            edit.insert(7, 6);
        }

        let f1 = TestField(1);
        let f2 = TestField(2);
        let f3 = TestField(3);
        let f4 = TestField(4);
        let f5 = TestField(5);
        let f6 = TestField(6);
        let f7 = TestField(7);
        let f8 = TestField(8);

        let mut db_data = SchemaBatch::new();
        db_data.put::<S>(&f1, &f1).unwrap();
        db.write_schemas(db_data).unwrap();

        let snapshot_manager = Arc::new(RwLock::new(CacheContainer::new(
            db,
            to_parent.clone().into(),
        )));

        // Operations:
        // | snapshot_id | key | operation |
        // | DB          |   1 |  write(1) |
        // | 1           |   2 |  write(2) |
        // | 1           |   3 |  write(4) |
        // | 2           |   1 |  write(5) |
        // | 2           |   2 |   delete  |
        // | 4           |   3 |  write(6) |
        // | 6           |   1 |  write(7) |
        // | 6           |   2 |  write(8) |

        // 1
        let db_snapshot = CacheDb::new(1, snapshot_manager.clone().into());
        db_snapshot.put::<S>(&f2, &f2).unwrap();
        db_snapshot.put::<S>(&f3, &f4).unwrap();
        {
            let mut snapshot_manager = snapshot_manager.write().unwrap();
            snapshot_manager.add_snapshot(db_snapshot.into()).unwrap();
        }

        // 2
        let db_snapshot = CacheDb::new(2, snapshot_manager.clone().into());
        db_snapshot.put::<S>(&f1, &f5).unwrap();
        db_snapshot.delete::<S>(&f2).unwrap();
        {
            let mut snapshot_manager = snapshot_manager.write().unwrap();
            snapshot_manager.add_snapshot(db_snapshot.into()).unwrap();
        }

        // 3
        let db_snapshot = CacheDb::new(3, snapshot_manager.clone().into());
        {
            let mut snapshot_manager = snapshot_manager.write().unwrap();
            snapshot_manager.add_snapshot(db_snapshot.into()).unwrap();
        }

        // 4
        let db_snapshot = CacheDb::new(4, snapshot_manager.clone().into());
        db_snapshot.put::<S>(&f3, &f6).unwrap();
        {
            let mut snapshot_manager = snapshot_manager.write().unwrap();
            snapshot_manager.add_snapshot(db_snapshot.into()).unwrap();
        }

        // 5
        let db_snapshot = CacheDb::new(5, snapshot_manager.clone().into());
        {
            let mut snapshot_manager = snapshot_manager.write().unwrap();
            snapshot_manager.add_snapshot(db_snapshot.into()).unwrap();
        }

        // 6
        let db_snapshot = CacheDb::new(6, snapshot_manager.clone().into());
        db_snapshot.put::<S>(&f1, &f7).unwrap();
        db_snapshot.put::<S>(&f2, &f8).unwrap();
        {
            let mut snapshot_manager = snapshot_manager.write().unwrap();
            snapshot_manager.add_snapshot(db_snapshot.into()).unwrap();
        }

        // 7
        let db_snapshot = CacheDb::new(7, snapshot_manager.clone().into());
        {
            let mut snapshot_manager = snapshot_manager.write().unwrap();
            snapshot_manager.add_snapshot(db_snapshot.into()).unwrap();
        }

        // View:
        // | from s_id   | key | value |
        // | 3           |   1 |     5 |
        // | 3           |   2 |  None |
        // | 3           |   3 |     4 |
        // | 5           |   1 |     1 |
        // | 5           |   2 |     2 |
        // | 5           |   3 |     6 |
        // | 7           |   1 |     7 |
        // | 7           |   2 |     8 |
        // | 7           |   3 |     4 |
        let snapshot_manager = snapshot_manager.read().unwrap();
        assert_eq!(Some(f5), snapshot_manager.get::<S>(3, &f1).unwrap());
        assert_eq!(None, snapshot_manager.get::<S>(3, &f2).unwrap());
        assert_eq!(Some(f4), snapshot_manager.get::<S>(3, &f3).unwrap());
        assert_eq!(Some(f1), snapshot_manager.get::<S>(5, &f1).unwrap());
        assert_eq!(Some(f2), snapshot_manager.get::<S>(5, &f2).unwrap());
        assert_eq!(Some(f6), snapshot_manager.get::<S>(5, &f3).unwrap());

        assert_eq!(Some(f7), snapshot_manager.get::<S>(7, &f1).unwrap());
        assert_eq!(Some(f8), snapshot_manager.get::<S>(7, &f2).unwrap());
        assert_eq!(Some(f4), snapshot_manager.get::<S>(7, &f3).unwrap());
    }

    fn collect_actual_values<'a, I: Iterator<Item = (&'a SchemaKey, &'a Operation)>>(
        iterator: CacheContainerIter<'a, I>,
    ) -> Vec<(TestField, TestField)> {
        iterator
            .into_iter()
            .map(|(k, v)| {
                let key = <<S as Schema>::Key as KeyDecoder<S>>::decode_key(&k).unwrap();
                let value = <<S as Schema>::Value as ValueCodec<S>>::decode_value(&v).unwrap();
                (key, value)
            })
            .collect()
    }

    fn encode_key(field: &TestField) -> SchemaKey {
        <TestField as KeyEncoder<S>>::encode_key(field).unwrap()
    }

    #[test]
    fn test_iterator() {
        let tempdir = tempfile::tempdir().unwrap();
        let db = create_test_db(tempdir.path());
        let to_parent = Arc::new(RwLock::new(HashMap::new()));
        {
            // DB -> 1 -> 2 -> 3
            let mut edit = to_parent.write().unwrap();
            edit.insert(2, 1);
            edit.insert(3, 2);
            edit.insert(4, 3);
        }

        let f0 = TestField(0);
        let f1 = TestField(1);
        let f2 = TestField(2);
        let f3 = TestField(3);
        let f4 = TestField(4);
        let f5 = TestField(5);
        let f6 = TestField(6);
        let f7 = TestField(7);
        let f8 = TestField(8);
        let f9 = TestField(9);
        let f10 = TestField(10);
        let f11 = TestField(11);
        let f12 = TestField(12);

        let mut db_data = SchemaBatch::new();
        db_data.put::<S>(&f3, &f9).unwrap();
        db_data.put::<S>(&f2, &f1).unwrap();
        db_data.put::<S>(&f4, &f1).unwrap();
        db_data.put::<S>(&f0, &f1).unwrap();
        db_data.put::<S>(&f11, &f9).unwrap();
        db.write_schemas(db_data).unwrap();
        // DB Data
        // | key | value |
        // |   3 |     9 |
        // |   2 |     1 |
        // |   4 |     1 |
        // |   0 |     1 |
        // |  11 |     9 |

        let snapshot_manager = Arc::new(RwLock::new(CacheContainer::new(
            db,
            to_parent.clone().into(),
        )));

        // Operations:
        // | snapshot_id | key |  operation |
        // |           1 |   1 |   write(8) |
        // |           1 |   5 |   write(7) |
        // |           1 |   8 |   write(3) |
        // |           1 |   4 |   write(2) |
        // |           2 |  10 |   write(2) |
        // |           2 |   9 |   write(4) |
        // |           2 |   4 |     delete |
        // |           2 |   2 |   write(6) |
        // |           3 |   8 |   write(6) |
        // |           3 |   9 |     delete |
        // |           3 |  12 |   write(1) |
        // |           3 |   1 |   write(2) |

        // 1
        let db_snapshot = CacheDb::new(1, snapshot_manager.clone().into());
        db_snapshot.put::<S>(&f1, &f8).unwrap();
        db_snapshot.put::<S>(&f5, &f7).unwrap();
        db_snapshot.put::<S>(&f8, &f3).unwrap();
        db_snapshot.put::<S>(&f4, &f2).unwrap();
        {
            let mut snapshot_manager = snapshot_manager.write().unwrap();
            snapshot_manager.add_snapshot(db_snapshot.into()).unwrap();
        }

        // 2
        let db_snapshot = CacheDb::new(2, snapshot_manager.clone().into());
        db_snapshot.put::<S>(&f10, &f2).unwrap();
        db_snapshot.put::<S>(&f9, &f4).unwrap();
        db_snapshot.delete::<S>(&f4).unwrap();
        db_snapshot.put::<S>(&f2, &f6).unwrap();
        {
            let mut snapshot_manager = snapshot_manager.write().unwrap();
            snapshot_manager.add_snapshot(db_snapshot.into()).unwrap();
        }

        // 3
        let db_snapshot = CacheDb::new(3, snapshot_manager.clone().into());
        db_snapshot.put::<S>(&f8, &f6).unwrap();
        db_snapshot.delete::<S>(&f9).unwrap();
        db_snapshot.put::<S>(&f12, &f1).unwrap();
        db_snapshot.put::<S>(&f1, &f2).unwrap();
        {
            let mut snapshot_manager = snapshot_manager.write().unwrap();
            snapshot_manager.add_snapshot(db_snapshot.into()).unwrap();
        }

        // Expected Order
        // | key | value |
        // |  12 |     1 |
        // |  10 |     2 |
        // |   8 |     6 |
        // |   5 |     7 |
        // |   3 |     9 |
        // |   2 |     6 |
        // |   1 |     2 |

        let snapshot_manager = snapshot_manager.read().unwrap();

        // Full iterator
        {
            let mut expected_fields = vec![
                (f0, f1),
                (f1, f2),
                (f2, f6),
                (f3, f9),
                (f5, f7),
                (f8, f6),
                (f10, f2),
                (f11, f9),
                (f12, f1),
            ];

            let forward_iterator = snapshot_manager.iter::<S>(4).unwrap();
            let actual_fields = collect_actual_values(forward_iterator);

            assert_eq!(
                expected_fields, actual_fields,
                "forward iterator is incorrect"
            );

            expected_fields.reverse();

            let backward_iterator = snapshot_manager.rev_iter::<S>(4).unwrap();
            let actual_fields = collect_actual_values(backward_iterator);
            assert_eq!(
                expected_fields, actual_fields,
                "backward iterator is incorrect"
            );
        }

        // Range iterator
        {
            let lower_bound = encode_key(&f2);
            let upper_bound = encode_key(&f10);

            // Full range
            let mut expected_fields = vec![(f2, f6), (f3, f9), (f5, f7), (f8, f6)];

            let forward_iterator = snapshot_manager
                .iter_range::<S>(4, lower_bound.clone()..upper_bound.clone())
                .unwrap();
            let actual_fields = collect_actual_values(forward_iterator);
            assert_eq!(
                expected_fields, actual_fields,
                "forward full range iterator is incorrect"
            );
            let backward_iterator = snapshot_manager
                .rev_iter_range::<S>(4, lower_bound.clone()..upper_bound.clone())
                .unwrap();
            let actual_fields = collect_actual_values(backward_iterator);
            expected_fields.reverse();
            assert_eq!(
                expected_fields, actual_fields,
                "backward full range iterator is incorrect"
            );

            // Only lower bound
            let mut expected_fields = vec![
                (f2, f6),
                (f3, f9),
                (f5, f7),
                (f8, f6),
                (f10, f2),
                (f11, f9),
                (f12, f1),
            ];
            let forward_iterator = snapshot_manager
                .iter_range::<S>(4, lower_bound.clone()..)
                .unwrap();
            let actual_fields = collect_actual_values(forward_iterator);
            assert_eq!(
                expected_fields, actual_fields,
                "forward low range iterator is incorrect"
            );
            let backward_iterator = snapshot_manager
                .rev_iter_range::<S>(4, lower_bound.clone()..)
                .unwrap();
            let actual_fields = collect_actual_values(backward_iterator);
            expected_fields.reverse();
            assert_eq!(
                expected_fields, actual_fields,
                "backward low range iterator is incorrect"
            );

            // Only upper bound
            let mut expected_fields =
                vec![(f0, f1), (f1, f2), (f2, f6), (f3, f9), (f5, f7), (f8, f6)];
            let forward_iterator = snapshot_manager
                .iter_range::<S>(4, ..upper_bound.clone())
                .unwrap();
            let actual_fields = collect_actual_values(forward_iterator);
            assert_eq!(
                expected_fields, actual_fields,
                "forward high range iterator is incorrect"
            );
            let backward_iterator = snapshot_manager
                .rev_iter_range::<S>(4, ..upper_bound.clone())
                .unwrap();
            let actual_fields = collect_actual_values(backward_iterator);
            expected_fields.reverse();
            assert_eq!(
                expected_fields, actual_fields,
                "backward high range iterator is incorrect"
            );
        }
    }

    fn test_iterator_ranges(numbers: Vec<u32>) {
        // Spreads all numbers into 10 changesets, where first 3 are saved to the database
        // Each number should be above 3 and below u32::MAX - 3
        assert!(numbers.len() >= 100);

        // Some numbers for ranges
        let min = *numbers.iter().min().unwrap();
        let below_min = min
            .checked_sub(3)
            .expect("test input is not in defined range");
        let max = *numbers.iter().max().unwrap();
        let above_max = max
            .checked_add(3)
            .expect("test input is not in defined range");
        let middle = *numbers.get(numbers.len() / 2).unwrap();

        let range_pairs = vec![
            (min, middle),
            (below_min, middle),
            (middle, max),
            (middle, above_max),
            (min, max),
            (below_min, above_max),
        ];

        // Prepare cache container
        let tempdir = tempfile::tempdir().unwrap();
        let db = create_test_db(tempdir.path());
        let to_parent = Arc::new(RwLock::new(HashMap::new()));
        {
            let mut to_parent = to_parent.write().unwrap();
            for id in 4..=10 {
                to_parent.insert(id, id - 1);
            }
        }

        let cache_container = Arc::new(RwLock::new(CacheContainer::new(
            db,
            to_parent.clone().into(),
        )));

        // Creating snapshots
        let n_chunks = 10; // Number of chunks you want

        let chunk_size = (numbers.len() + n_chunks - 1) / n_chunks; // Calculate size of each chunk
        let chunks: Vec<_> = numbers.chunks(chunk_size).map(|c| c.to_vec()).collect();

        for (idx, chunk) in chunks.iter().enumerate() {
            let snapshot_id = idx as u64;
            let db_snapshot = CacheDb::new(snapshot_id, cache_container.clone().into());
            for item in chunk {
                db_snapshot
                    .put::<S>(&TestField(*item), &TestField(*item))
                    .unwrap();
            }

            {
                let mut cache_container = cache_container.write().unwrap();
                cache_container.add_snapshot(db_snapshot.into()).unwrap();
                if idx < 3 {
                    cache_container.commit_snapshot(&snapshot_id).unwrap();
                }
            }
        }

        for (low, high) in range_pairs {
            let low = TestField(low);
            let high = TestField(high);
            let range = encode_key(&low)..encode_key(&high);
            let range_inclusive = encode_key(&low)..=encode_key(&high);
            let range_to = ..encode_key(&high);
            let range_to_inclusive = ..=encode_key(&high);
            let range_from = encode_key(&low)..;
            let range_full = ..;

            check_range(cache_container.read().unwrap(), range);
            check_range(cache_container.read().unwrap(), range_inclusive);
            check_range(cache_container.read().unwrap(), range_to);
            check_range(cache_container.read().unwrap(), range_to_inclusive);
            check_range(cache_container.read().unwrap(), range_from);
            check_range(cache_container.read().unwrap(), range_full);
        }
    }

    fn check_range<R: std::ops::RangeBounds<SchemaKey> + Clone>(
        cache_container: RwLockReadGuard<CacheContainer>,
        range: R,
    ) {
        let iterator_forward = cache_container.iter_range::<S>(10, range.clone()).unwrap();
        validate_iterator(iterator_forward, range.clone(), ScanDirection::Forward);
        let iterator_backward = cache_container
            .rev_iter_range::<S>(10, range.clone())
            .unwrap();
        validate_iterator(iterator_backward, range.clone(), ScanDirection::Backward);
    }

    fn validate_iterator<I, R>(iterator: I, range: R, direction: ScanDirection)
    where
        I: Iterator<Item = (SchemaKey, SchemaValue)>,
        R: std::ops::RangeBounds<SchemaKey>,
    {
        let mut prev_key: Option<TestField> = None;
        for (key, _) in iterator {
            assert!(range.contains(&key));
            let key = <<S as Schema>::Key as KeyDecoder<S>>::decode_key(&key).unwrap();
            if let Some(prev_key) = prev_key {
                match direction {
                    ScanDirection::Forward => {
                        assert!(key.0 >= prev_key.0)
                    }
                    ScanDirection::Backward => {
                        assert!(key.0 <= prev_key.0)
                    }
                };
            }
            prev_key = Some(key);
        }
    }

    #[test]
    fn check_proptest_case() {
        let numbers: Vec<u32> = (10..=113).collect();
        test_iterator_ranges(numbers);
    }

    proptest! {
        #[test]
        fn cache_container_iter_range(input in prop::collection::vec(4u32..10_000, 101..200)) {
            test_iterator_ranges(input);
        }

        #[test]
        fn cache_container_iter_range_tight_values(input in prop::collection::vec(4u32..10, 101..200)) {
            test_iterator_ranges(input);
        }

        fn cache_container_iter_range_uniq_numbers(input in prop::collection::hash_set(4u32..10_000, 101..200)) {
            let input: Vec<u32> = input.into_iter().collect();
            test_iterator_ranges(input);
        }
    }
}
