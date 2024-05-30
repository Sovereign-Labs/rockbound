#![allow(dead_code)]
//! This module contains the next iteration of [`crate::cache::cache_db::CacheDb`]
use crate::cache::change_set::ChangeSetIter;
use crate::iterator::{RawDbIter, ScanDirection};
use crate::schema::{KeyCodec, KeyDecoder, ValueCodec};
use crate::{
    Operation, PaginatedResponse, Schema, SchemaBatch, SchemaKey, SchemaValue, SeekKeyEncoder, DB,
};
use std::cmp::Ordering;
use std::iter::{Peekable, Rev};
use std::sync::Arc;

/// Intermediate step between [`crate::cache::cache_db::CacheDb`] and future DeltaDbReader
/// Supports "local writes". And for historical reading it uses `Vec<Arc<ChangeSet>`
#[derive(Debug)]
pub struct DeltaReader {
    /// Set of not finalized changes in chronological order.
    /// Meaning that the first snapshot in the vector is the oldest and the latest is the most recent.
    /// If keys are equal, value from more recent snapshot is taken.
    snapshots: Vec<Arc<SchemaBatch>>,
    /// Reading finalized data from here.
    db: DB,
}

impl DeltaReader {
    /// Creates new [`DeltaReader`] with given `id`, `db` and `uncommited_changes`.
    /// `uncommited_changes` should be in chronological order.
    pub fn new(db: DB, uncommited_changes: Vec<Arc<SchemaBatch>>) -> Self {
        Self {
            snapshots: uncommited_changes,
            db,
        }
    }

    /// Get a value, wherever it is.
    pub async fn get<S: Schema>(&self, key: &impl KeyCodec<S>) -> anyhow::Result<Option<S::Value>> {
        // Some(Operation) means that key was touched,
        // but in case of deletion, we early return None
        // Only in case of not finding operation for a key,
        // we go deeper

        tokio::task::block_in_place(|| {
            // 1. Check in snapshots, in reverse order
            for snapshot in self.snapshots.iter() {
                if let Some(operation) = snapshot.get_operation::<S>(key)? {
                    return operation.decode_value::<S>();
                }
            }

            // 2. Check in DB
            self.db.get(key)
        })
    }

    /// Get a value of the largest key written value for given [`Schema`].
    pub async fn get_largest<S: Schema>(&self) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        let mut iterator = self.iter_rev::<S>()?;
        if let Some((key, value)) = iterator.next() {
            let key = S::Key::decode_key(&key)?;
            let value = S::Value::decode_value(&value)?;
            return Ok(Some((key, value)));
        }
        Ok(None)
    }

    /// Get the largest value in [`Schema`] that is smaller than give `seek_key`.
    pub async fn get_prev<S: Schema>(
        &self,
        _seek_key: &impl SeekKeyEncoder<S>,
    ) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        todo!()
    }

    /// Get `n` keys >= `seek_key`
    pub async fn get_n_from_first_match<S: Schema>(
        &self,
        _seek_key: &impl SeekKeyEncoder<S>,
        _n: usize,
    ) -> anyhow::Result<PaginatedResponse<S>> {
        todo!()
    }

    /// Collects all key-value pairs in given range, from smallest to largest.
    pub async fn collect_in_range<S: Schema, Sk: SeekKeyEncoder<S>>(
        &self,
        _range: std::ops::Range<Sk>,
    ) -> anyhow::Result<Vec<(S::Key, S::Value)>> {
        todo!()
    }

    //
    fn iter_rev<S: Schema>(&self) -> anyhow::Result<DeltaReaderIter<Rev<ChangeSetIter>>> {
        // Snapshot iterators.
        // Snapshots are in natural order, but k/v are in reversed.
        // Snapshots needs to be in natural order, so chronology is always preserved.
        let snapshot_iterators = self
            .snapshots
            .iter()
            .map(|snapshot| snapshot.iter::<S>().rev())
            .collect::<Vec<_>>();

        // Database iterator
        let db_iter = self.db.raw_iter::<S>(ScanDirection::Backward)?;

        Ok(DeltaReaderIter::new(
            snapshot_iterators,
            db_iter,
            ScanDirection::Backward,
        ))
    }
}

struct DeltaReaderIter<'a, SnapshotIter>
where
    SnapshotIter: Iterator<Item = (&'a SchemaKey, &'a Operation)>,
{
    snapshot_iterators: Vec<Peekable<SnapshotIter>>,
    db_iter: Peekable<RawDbIter<'a>>,
    direction: ScanDirection,
}

impl<'a, SnapshotIter> DeltaReaderIter<'a, SnapshotIter>
where
    SnapshotIter: Iterator<Item = (&'a SchemaKey, &'a Operation)>,
{
    fn new(
        snapshot_iterators: Vec<SnapshotIter>,
        db_iter: RawDbIter<'a>,
        direction: ScanDirection,
    ) -> Self {
        Self {
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
}

impl<'a, SnapshotIter> Iterator for DeltaReaderIter<'a, SnapshotIter>
where
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
        // Right most location has the most priority, so the last value is taken
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

            // All next values are observed at this point
            // Handling actual change of the iterator state.
            // Rightmost value in the next locations is the most recent, so it is taken.
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
    use crate::schema::ValueCodec;
    use crate::test::{TestCompositeField, TestField};
    use crate::{define_schema, SchemaKey, SchemaValue, DB};
    use proptest::prelude::*;

    define_schema!(TestSchema, TestCompositeField, TestField, "TestCF");

    type S = TestSchema;

    fn open_db(dir: impl AsRef<Path>) -> DB {
        let column_families = vec![DEFAULT_COLUMN_FAMILY_NAME, TestSchema::COLUMN_FAMILY_NAME];
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        DB::open(dir, "test", column_families, &db_opts).expect("Failed to open DB.")
    }

    // Test utils
    fn encode_key(key: &TestCompositeField) -> SchemaKey {
        <TestCompositeField as KeyEncoder<S>>::encode_key(key).unwrap()
    }

    fn encode_value(value: &TestField) -> SchemaValue {
        <TestField as ValueCodec<S>>::encode_value(value).unwrap()
    }

    async fn check_value(delta_db: &DeltaReader, key: u32, expected_value: Option<u32>) {
        let actual_value = delta_db
            .get::<S>(&TestCompositeField(key, 0, 0))
            .await
            .unwrap()
            .map(|v| v.0);
        assert_eq!(expected_value, actual_value);
    }

    // End of test utils

    #[tokio::test]
    async fn test_empty() {
        let tmpdir = tempfile::tempdir().unwrap();
        let db = open_db(tmpdir.path());

        let delta_db = DeltaReader::new(db, vec![]);

        let values: Vec<_> = delta_db.iter_rev::<S>().unwrap().collect();
        assert!(values.is_empty());

        let largest = delta_db.get_largest::<S>().await.unwrap();

        assert!(largest.is_none());

        // let key = TestCompositeField::MAX;
        // let prev = delta_db.get_prev::<S>(&key).await.unwrap();
        // assert!(prev.is_none());
    }

    #[test]
    fn test_simple_iterator() {
        let tmpdir = tempfile::tempdir().unwrap();
        let db = open_db(tmpdir.path());

        let k0 = TestCompositeField(0, 0, 0);
        let k1 = TestCompositeField(0, 1, 0);
        let k2 = TestCompositeField(0, 1, 2);
        let k3 = TestCompositeField(3, 1, 0);
        let k4 = TestCompositeField(3, 2, 0);

        let mut schema_batch_0 = SchemaBatch::new();
        schema_batch_0.put::<S>(&k0, &TestField(0)).unwrap();
        schema_batch_0.put::<S>(&k1, &TestField(1)).unwrap();
        schema_batch_0.put::<S>(&k4, &TestField(4)).unwrap();
        db.write_schemas(&schema_batch_0).unwrap();

        let mut schema_batch_1 = SchemaBatch::new();
        schema_batch_1.put::<S>(&k2, &TestField(2)).unwrap();
        schema_batch_1.put::<S>(&k3, &TestField(3)).unwrap();

        let delta_db = DeltaReader::new(db, vec![Arc::new(schema_batch_1)]);

        let values: Vec<_> = delta_db.iter_rev::<S>().unwrap().collect();
        assert_eq!(5, values.len());

        assert!(
            values.windows(2).all(|w| w[0].0 >= w[1].0),
            "iter_rev should be sorted in reversed order"
        );
    }

    fn execute_rev_iterator(
        db_entries: Vec<(TestCompositeField, TestField)>,
        snapshots: Vec<Vec<(TestCompositeField, TestField)>>,
    ) -> Vec<(SchemaKey, SchemaValue)> {
        let tmpdir = tempfile::tempdir().unwrap();
        let db = open_db(tmpdir.path());

        let mut db_batch = SchemaBatch::new();
        for (key, value) in db_entries {
            db_batch.put::<S>(&key, &value).unwrap();
        }
        db.write_schemas(&db_batch).unwrap();

        let mut schema_batches = Vec::new();
        for snapshot in snapshots {
            let mut schema_batch = SchemaBatch::new();
            for (key, value) in snapshot {
                schema_batch.put::<S>(&key, &value).unwrap();
            }
            schema_batches.push(Arc::new(schema_batch));
        }

        let delta_db = DeltaReader::new(db, schema_batches);

        let values: Vec<_> = delta_db.iter_rev::<S>().unwrap().collect();
        values
    }

    fn execute_deletions(
        db_entries: Vec<(TestCompositeField, TestField)>,
        snapshots: Vec<Vec<(TestCompositeField, TestField)>>,
    ) -> Vec<(SchemaKey, SchemaValue)> {
        let tmpdir = tempfile::tempdir().unwrap();
        let db = open_db(tmpdir.path());

        // All keys will be "deleted" in the latest snapshot,
        //  so the resulting iterator should yield an empty result
        let mut latest_schema_batch = SchemaBatch::new();

        let mut db_batch = SchemaBatch::new();
        for (key, value) in db_entries {
            db_batch.put::<S>(&key, &value).unwrap();
            latest_schema_batch.delete::<S>(&key).unwrap();
        }
        db.write_schemas(&db_batch).unwrap();

        let mut schema_batches = Vec::new();
        for snapshot in snapshots {
            let mut schema_batch = SchemaBatch::new();
            for (key, value) in snapshot {
                schema_batch.put::<S>(&key, &value).unwrap();
                latest_schema_batch.delete::<S>(&key).unwrap();
            }
            schema_batches.push(Arc::new(schema_batch));
        }

        schema_batches.push(Arc::new(latest_schema_batch));

        let delta_db = DeltaReader::new(db, schema_batches);

        let values: Vec<_> = delta_db.iter_rev::<S>().unwrap().collect();
        values
    }

    proptest! {
        #[test]
        fn proptest_rev_iterator_simple((db_entries, snapshots) in generate_db_entries_and_snapshots()) {
            let values = execute_rev_iterator(db_entries, snapshots);
            prop_assert!(
                values.windows(2).all(|w| w[0].0 >= w[1].0),
                "iter_rev should be sorted in reversed order"
            );
       }

        #[test]
        fn proptest_rev_iterator_everything_was_deleted((db_entries, snapshots) in generate_db_entries_and_snapshots()) {
            let values = execute_deletions(db_entries, snapshots);
            prop_assert!(values.is_empty());
       }
    }

    fn generate_db_entries_and_snapshots() -> impl Strategy<
        Value = (
            Vec<(TestCompositeField, TestField)>,
            Vec<Vec<(TestCompositeField, TestField)>>,
        ),
    > {
        let entries = prop::collection::vec((any::<TestCompositeField>(), any::<TestField>()), 50);
        let snapshots = prop::collection::vec(
            prop::collection::vec((any::<TestCompositeField>(), any::<TestField>()), 10),
            4,
        );
        (entries, snapshots)
    }

    #[test]
    #[ignore = "TBD"]
    fn get_largest() {}

    #[test]
    #[ignore = "TBD"]
    fn get_prev() {}
}
