#![allow(dead_code)]
//! This module contains the next iteration of [`crate::cache::cache_db::CacheDb`]
use crate::iterator::{RawDbIter, ScanDirection};
use crate::schema::{KeyCodec, KeyDecoder, ValueCodec};
use crate::{
    Operation, PaginatedResponse, Schema, SchemaBatch, SchemaKey, SchemaValue, SeekKeyEncoder, DB,
};
use std::cmp::Ordering;
use std::collections::btree_map;
use std::iter::{Peekable, Rev};
use std::sync::Arc;

/// Iterator type returned by [`DeltaReader::iter`].
pub type SnapshotIter<'a> = btree_map::Iter<'a, SchemaKey, Operation>;
/// Range type returned by [`DeltaReader::iter_range`].
pub type SnapshotIterRange<'a> = btree_map::Range<'a, SchemaKey, Operation>;

/// Intermediate step between [`crate::cache::cache_db::CacheDb`] and future DeltaDbReader
/// Supports "local writes". And for historical reading it uses `Vec<Arc<ChangeSet>`
#[derive(Debug)]
pub struct DeltaReader {
    /// Set of not finalized changes in chronological order.
    /// Meaning that the first snapshot in the vector is the oldest and the latest is the most recent.
    /// If keys are equal, the value from more recent snapshot is taken.
    snapshots: Vec<Arc<SchemaBatch>>,
    /// Reading finalized data from here.
    db: DB,
}

impl DeltaReader {
    /// Creates new [`DeltaReader`] with given [`DB`] and vector with uncommited snapshots of [`SchemaBatch`].
    /// Snapshots should be in chronological order.
    pub fn new(db: DB, snapshots: Vec<Arc<SchemaBatch>>) -> Self {
        Self { snapshots, db }
    }

    /// Get a value, wherever it is.
    pub async fn get<S: Schema>(&self, key: &impl KeyCodec<S>) -> anyhow::Result<Option<S::Value>> {
        // Some(Operation) means that key was touched,
        // but in case of deletion, we early return None
        // Only in case of not finding operation for a key,
        // we go deeper

        tokio::task::block_in_place(|| {
            for snapshot in self.snapshots.iter().rev() {
                if let Some(operation) = snapshot.get_operation::<S>(key)? {
                    return operation.decode_value::<S>();
                }
            }

            self.db.get(key)
        })
    }

    /// Get a value of the largest key written value for given [`Schema`].
    pub async fn get_largest<S: Schema>(&self) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        tokio::task::block_in_place(|| {
            let mut iterator = self.iter_rev::<S>()?;
            if let Some((key, value)) = iterator.next() {
                let key = S::Key::decode_key(&key)?;
                let value = S::Value::decode_value(&value)?;
                return Ok(Some((key, value)));
            }
            Ok(None)
        })
    }

    /// Get the largest value in [`Schema`] that is smaller than give `seek_key`.
    pub async fn get_prev<S: Schema>(
        &self,
        seek_key: &impl SeekKeyEncoder<S>,
    ) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        let seek_key = seek_key.encode_seek_key()?;
        let range = ..=seek_key;

        tokio::task::block_in_place(|| {
            let mut iterator = self.iter_rev_range::<S>(range)?;
            if let Some((key, value)) = iterator.next() {
                let key = S::Key::decode_key(&key)?;
                let value = S::Value::decode_value(&value)?;
                return Ok(Some((key, value)));
            }
            Ok(None)
        })
    }

    /// Get `n` keys >= `seek_key`
    pub async fn get_n_from_first_match<S: Schema>(
        &self,
        seek_key: &impl SeekKeyEncoder<S>,
        n: usize,
    ) -> anyhow::Result<PaginatedResponse<S>> {
        let seek_key = seek_key.encode_seek_key()?;
        let range = seek_key..;

        tokio::task::block_in_place(|| {
            let mut iterator = self.iter_range::<S>(range)?;
            let results: Vec<(S::Key, S::Value)> = iterator
                .by_ref()
                .filter_map(|(key_bytes, value_bytes)| {
                    let key = S::Key::decode_key(&key_bytes).ok()?;
                    let value = S::Value::decode_value(&value_bytes).ok()?;
                    Some((key, value))
                })
                .take(n)
                .collect();

            let next_start_key = match iterator.next().map(|(key_bytes, _)| key_bytes) {
                None => None,
                Some(key_bytes) => Some(S::Key::decode_key(&key_bytes)?),
            };

            Ok(PaginatedResponse {
                key_value: results,
                next: next_start_key,
            })
        })
    }

    /// Collects all key-value pairs in given range, from smallest to largest.
    pub async fn collect_in_range<S: Schema, Sk: SeekKeyEncoder<S>>(
        &self,
        range: std::ops::Range<Sk>,
    ) -> anyhow::Result<Vec<(S::Key, S::Value)>> {
        let lower_bound = range.start.encode_seek_key()?;
        let upper_bound = range.end.encode_seek_key()?;
        let range = lower_bound..upper_bound;

        tokio::task::block_in_place(|| {
            let result = self
                .iter_range::<S>(range)?
                .map(|(key, value)| {
                    let key = S::Key::decode_key(&key).unwrap();
                    let value = S::Value::decode_value(&value).unwrap();
                    (key, value)
                })
                .collect();
            Ok(result)
        })
    }

    fn iter<S: Schema>(&self) -> anyhow::Result<DeltaReaderIter<SnapshotIter>> {
        let snapshot_iterators = self
            .snapshots
            .iter()
            .map(|snapshot| snapshot.iter::<S>())
            .collect::<Vec<_>>();

        let db_iter = self.db.raw_iter::<S>(ScanDirection::Forward)?;

        Ok(DeltaReaderIter::new(
            snapshot_iterators,
            db_iter,
            ScanDirection::Forward,
        ))
    }

    fn iter_range<S: Schema>(
        &self,
        range: impl std::ops::RangeBounds<SchemaKey> + Clone,
    ) -> anyhow::Result<DeltaReaderIter<SnapshotIterRange>> {
        let snapshot_iterators = self
            .snapshots
            .iter()
            .map(|snapshot| snapshot.iter_range::<S>(range.clone()))
            .collect::<Vec<_>>();

        let db_iter = self.db.raw_iter_range::<S>(range, ScanDirection::Forward)?;

        Ok(DeltaReaderIter::new(
            snapshot_iterators,
            db_iter,
            ScanDirection::Backward,
        ))
    }

    //
    fn iter_rev<S: Schema>(&self) -> anyhow::Result<DeltaReaderIter<Rev<SnapshotIter>>> {
        // Snapshot iterators.
        // Snapshots are in natural order, but k/v are in reversed.
        // Snapshots need to be in natural order, so chronology is always preserved.
        let snapshot_iterators = self
            .snapshots
            .iter()
            .map(|snapshot| snapshot.iter::<S>().rev())
            .collect::<Vec<_>>();

        let db_iter = self.db.raw_iter::<S>(ScanDirection::Backward)?;

        Ok(DeltaReaderIter::new(
            snapshot_iterators,
            db_iter,
            ScanDirection::Backward,
        ))
    }

    fn iter_rev_range<S: Schema>(
        &self,
        range: impl std::ops::RangeBounds<SchemaKey> + Clone,
    ) -> anyhow::Result<DeltaReaderIter<Rev<SnapshotIterRange>>> {
        let snapshot_iterators = self
            .snapshots
            .iter()
            .map(|snapshot| snapshot.iter_range::<S>(range.clone()).rev())
            .collect::<Vec<_>>();

        let db_iter = self
            .db
            .raw_iter_range::<S>(range, ScanDirection::Backward)?;

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
            .checked_add(1)
            .unwrap_or_default();
        // In case of equal next key, this vector contains all iterator locations with this key.
        // It is filled with order: DB, Snapshots.
        // Right most location has the most priority, so the last value is taken.
        // And all "other" locations are progressed without taking a value.
        let mut next_value_locations: Vec<DataLocation> = Vec::with_capacity(next_values_size);

        loop {
            // Reference to the last next value observed somewhere.
            // Used for comparing between iterators.
            let mut next_value: Option<&SchemaKey> = None;
            next_value_locations.clear();

            // Going to DB first.
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

            // All next values are observed at this point.
            // Handling actual change of the iterator state.
            // The rightmost value in the next locations is the most recent, so it is taken.
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
    use tokio::runtime::Runtime;

    use super::*;
    use crate::schema::KeyEncoder;
    use crate::schema::ValueCodec;
    use crate::test::{TestCompositeField, TestField};
    use crate::{define_schema, SchemaKey, SchemaValue, DB};
    use proptest::prelude::*;

    define_schema!(TestSchema, TestCompositeField, TestField, "TestCF");

    type S = TestSchema;

    // Minimal fields
    const FIELD_1: TestCompositeField = TestCompositeField(0, 1, 0);
    const FIELD_2: TestCompositeField = TestCompositeField(0, 1, 2);
    const FIELD_3: TestCompositeField = TestCompositeField(3, 4, 0);
    const FIELD_4: TestCompositeField = TestCompositeField(3, 7, 0);
    // Extra fields
    const FIELD_5: TestCompositeField = TestCompositeField(4, 2, 0);
    const FIELD_6: TestCompositeField = TestCompositeField(4, 2, 1);
    const FIELD_7: TestCompositeField = TestCompositeField(4, 3, 0);

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

    async fn check_value(delta_reader: &DeltaReader, key: u32, expected_value: Option<u32>) {
        let actual_value = delta_reader
            .get::<S>(&TestCompositeField(key, 0, 0))
            .await
            .unwrap()
            .map(|v| v.0);
        assert_eq!(expected_value, actual_value);
    }

    // DeltaReader with a simple set of known values. Useful for minimal checks.
    // Contains only writes.
    fn build_simple_delta_reader(path: impl AsRef<Path>) -> DeltaReader {
        let db = open_db(path.as_ref());
        let mut db_data = SchemaBatch::new();
        db_data.put::<S>(&FIELD_1, &TestField(1)).unwrap();
        db_data.put::<S>(&FIELD_4, &TestField(4)).unwrap();
        db.write_schemas(&db_data).unwrap();

        let mut snapshot_1 = SchemaBatch::new();
        snapshot_1.put::<S>(&FIELD_2, &TestField(2)).unwrap();
        snapshot_1.put::<S>(&FIELD_3, &TestField(3)).unwrap();

        DeltaReader::new(db, vec![Arc::new(snapshot_1)])
    }

    // DeltaReader with a known set of values, but more complex combination, includes deletion.
    // DB contains fields 1, 4, 5, and 7.
    // Have 3 snapshots, value is equal to field_id * 10^snapshot_level:
    // 1. Written: field 6.
    // 2. Written: fields 2, 7. Deleted: field 5.
    // 3. Written: fields 3, 6. Deleted: fields 1, 7.
    fn build_elaborate_delta_reader(path: impl AsRef<Path>) -> DeltaReader {
        let db = open_db(path.as_ref());
        let mut schema_batch_0 = SchemaBatch::new();
        schema_batch_0.put::<S>(&FIELD_1, &TestField(1)).unwrap();
        schema_batch_0.put::<S>(&FIELD_4, &TestField(4)).unwrap();
        schema_batch_0.put::<S>(&FIELD_5, &TestField(5)).unwrap();
        schema_batch_0.put::<S>(&FIELD_7, &TestField(7)).unwrap();
        db.write_schemas(&schema_batch_0).unwrap();

        let mut snapshot_1 = SchemaBatch::new();
        snapshot_1.put::<S>(&FIELD_6, &TestField(60)).unwrap();

        let mut snapshot_2 = SchemaBatch::new();
        snapshot_2.put::<S>(&FIELD_2, &TestField(200)).unwrap();
        snapshot_2.put::<S>(&FIELD_7, &TestField(700)).unwrap();
        snapshot_2.delete::<S>(&FIELD_5).unwrap();

        let mut snapshot_3 = SchemaBatch::new();
        snapshot_3.put::<S>(&FIELD_3, &TestField(3000)).unwrap();
        snapshot_3.put::<S>(&FIELD_6, &TestField(6000)).unwrap();
        snapshot_3.delete::<S>(&FIELD_1).unwrap();
        snapshot_3.delete::<S>(&FIELD_7).unwrap();

        DeltaReader::new(
            db,
            vec![
                Arc::new(snapshot_1),
                Arc::new(snapshot_2),
                Arc::new(snapshot_3),
            ],
        )
    }
    // End of test utils

    #[tokio::test(flavor = "multi_thread")]
    async fn test_empty() {
        let tmpdir = tempfile::tempdir().unwrap();
        let db = open_db(tmpdir.path());

        let delta_reader = DeltaReader::new(db, vec![]);

        let largest = delta_reader.get_largest::<S>().await.unwrap();
        assert!(largest.is_none());

        let key = TestCompositeField::MAX;
        let prev = delta_reader.get_prev::<S>(&key).await.unwrap();
        assert!(prev.is_none());

        let value_1 = delta_reader.get::<S>(&FIELD_1).await.unwrap();
        assert!(value_1.is_none());

        let values: Vec<_> = delta_reader.iter::<S>().unwrap().collect();
        assert!(values.is_empty());

        let full_range =
            encode_key(&TestCompositeField::MIN)..=encode_key(&TestCompositeField::MAX);
        let values: Vec<_> = delta_reader
            .iter_range::<S>(full_range.clone())
            .unwrap()
            .collect();
        assert!(values.is_empty());

        let values: Vec<_> = delta_reader.iter_rev::<S>().unwrap().collect();
        assert!(values.is_empty());
        let values: Vec<_> = delta_reader
            .iter_rev_range::<S>(full_range)
            .unwrap()
            .collect();
        assert!(values.is_empty());

        let paginated_response = delta_reader
            .get_n_from_first_match::<S>(&TestCompositeField::MAX, 100)
            .await
            .unwrap();
        assert!(paginated_response.key_value.is_empty());
        assert!(paginated_response.next.is_none());

        let values: Vec<_> = delta_reader
            .collect_in_range::<S, TestCompositeField>(
                TestCompositeField::MIN..TestCompositeField::MAX,
            )
            .await
            .unwrap();
        assert!(values.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_simple() {
        let tmpdir = tempfile::tempdir().unwrap();
        let delta_reader = build_simple_delta_reader(tmpdir.path());

        // From DB
        let value_1 = delta_reader.get::<S>(&FIELD_1).await.unwrap();
        assert_eq!(Some(TestField(1)), value_1);

        // From Snapshot
        let value_2 = delta_reader.get::<S>(&FIELD_2).await.unwrap();
        assert_eq!(Some(TestField(2)), value_2);

        let not_found = delta_reader.get::<S>(&FIELD_7).await.unwrap();
        assert!(not_found.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_elaborate() {
        let tmpdir = tempfile::tempdir().unwrap();
        let delta_reader = build_elaborate_delta_reader(tmpdir.path());

        // From DB
        let value = delta_reader.get::<S>(&FIELD_4).await.unwrap();
        assert_eq!(Some(TestField(4)), value);

        // From the most recent snapshot
        let value = delta_reader.get::<S>(&FIELD_3).await.unwrap();
        assert_eq!(Some(TestField(3000)), value);
        let value = delta_reader.get::<S>(&FIELD_6).await.unwrap();
        assert_eq!(Some(TestField(6000)), value);

        // From middle snapshot
        let value = delta_reader.get::<S>(&FIELD_2).await.unwrap();
        assert_eq!(Some(TestField(200)), value);

        // Deleted values
        let value = delta_reader.get::<S>(&FIELD_7).await.unwrap();
        assert!(value.is_none());
        let value = delta_reader.get::<S>(&FIELD_5).await.unwrap();
        assert!(value.is_none());

        // Not found
        let value = delta_reader
            .get::<S>(&TestCompositeField::MIN)
            .await
            .unwrap();
        assert!(value.is_none());
        let value = delta_reader
            .get::<S>(&TestCompositeField::MAX)
            .await
            .unwrap();
        assert!(value.is_none());
        let value = delta_reader
            .get::<S>(&TestCompositeField(3, 5, 1))
            .await
            .unwrap();
        assert!(value.is_none());
    }

    // Checks that values returned by iterator are sorted.
    fn basic_check_iterator(delta_reader: &DeltaReader, expected_len: usize) {
        let values: Vec<_> = delta_reader.iter::<S>().unwrap().collect();
        assert_eq!(expected_len, values.len());
        assert!(
            values.windows(2).all(|w| w[0].0 <= w[1].0),
            "iter should be sorted"
        );

        let values_rev: Vec<_> = delta_reader.iter_rev::<S>().unwrap().collect();
        assert_eq!(expected_len, values_rev.len());

        assert!(
            values_rev.windows(2).all(|w| w[0].0 >= w[1].0),
            "iter_rev should be sorted in reversed order"
        );
    }

    #[test]
    fn iterator_simple() {
        let tmpdir = tempfile::tempdir().unwrap();
        let delta_reader = build_simple_delta_reader(tmpdir.path());
        basic_check_iterator(&delta_reader, 4);
    }

    #[test]
    fn iterator_elaborate() {
        let tmpdir = tempfile::tempdir().unwrap();
        let delta_reader = build_elaborate_delta_reader(tmpdir.path());

        basic_check_iterator(&delta_reader, 4);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_largest_simple() {
        let tmpdir = tempfile::tempdir().unwrap();
        let delta_reader = build_simple_delta_reader(tmpdir.path());

        let largest = delta_reader.get_largest::<S>().await.unwrap();
        assert!(largest.is_some(), "largest value is not found");
        let (largest_key, largest_value) = largest.unwrap();

        assert_eq!(FIELD_4, largest_key);
        assert_eq!(TestField(4), largest_value);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_largest_elaborate() {
        let tmpdir = tempfile::tempdir().unwrap();
        let delta_reader = build_elaborate_delta_reader(tmpdir.path());

        let largest = delta_reader.get_largest::<S>().await.unwrap();
        assert!(largest.is_some(), "largest value is not found");
        let (largest_key, largest_value) = largest.unwrap();

        assert_eq!(FIELD_6, largest_key);
        assert_eq!(TestField(6000), largest_value);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_prev_simple() {
        let tmpdir = tempfile::tempdir().unwrap();
        let delta_reader = build_simple_delta_reader(tmpdir.path());

        // MIN, should not find anything
        let prev = delta_reader
            .get_prev::<S>(&TestCompositeField::MIN)
            .await
            .unwrap();
        assert!(prev.is_none());

        // Should get the lowest value in
        let prev = delta_reader.get_prev::<S>(&FIELD_1).await.unwrap();
        assert!(prev.is_some());
        let (prev_key, prev_value) = prev.unwrap();
        assert_eq!(FIELD_1, prev_key);
        assert_eq!(TestField(1), prev_value);

        // Some value in the middle
        let (prev_key, prev_value) = delta_reader.get_prev::<S>(&FIELD_3).await.unwrap().unwrap();
        assert_eq!(FIELD_3, prev_key);
        assert_eq!(TestField(3), prev_value);

        // Value in between
        let (prev_key, prev_value) = delta_reader
            .get_prev::<S>(&TestCompositeField(3, 5, 8))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(FIELD_3, prev_key);
        assert_eq!(TestField(3), prev_value);

        // MAX Should return the largest value
        let (prev_key, prev_value) = delta_reader
            .get_prev::<S>(&TestCompositeField::MAX)
            .await
            .unwrap()
            .unwrap();
        let (largest_key, largest_value) = delta_reader.get_largest::<S>().await.unwrap().unwrap();
        assert_eq!(largest_key, prev_key);
        assert_eq!(largest_value, prev_value);
    }

    fn build_delta_reader_from(
        path: impl AsRef<Path>,
        db_entries: &[(TestCompositeField, TestField)],
        snapshots: &[Vec<(TestCompositeField, TestField)>],
    ) -> DeltaReader {
        let db = open_db(path.as_ref());

        let mut db_batch = SchemaBatch::new();
        for (key, value) in db_entries {
            db_batch.put::<S>(key, value).unwrap();
        }
        db.write_schemas(&db_batch).unwrap();

        let mut schema_batches = Vec::new();
        for snapshot in snapshots {
            let mut schema_batch = SchemaBatch::new();
            for (key, value) in snapshot {
                schema_batch.put::<S>(key, value).unwrap();
            }
            schema_batches.push(Arc::new(schema_batch));
        }

        DeltaReader::new(db, schema_batches)
    }

    fn execute_with_all_deleted_in_last_snapshot(
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

        let delta_reader = DeltaReader::new(db, schema_batches);

        let values: Vec<_> = delta_reader.iter_rev::<S>().unwrap().collect();
        values
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
            8,
        );
        (entries, snapshots)
    }

    proptest! {
        #[test]
        fn proptest_get((db_entries, snapshots) in generate_db_entries_and_snapshots()) {
            let tmpdir = tempfile::tempdir().unwrap();
            let db_reader = build_delta_reader_from(tmpdir.path(), &db_entries, &snapshots);
            let rt = Runtime::new().unwrap();
            let _ = rt.block_on(async {
                for (k, _) in &db_entries {
                    let value = db_reader.get::<S>(k).await;
                    prop_assert!(value.is_ok());
                    let value = value.unwrap();
                    prop_assert!(value.is_some());
                }
                Ok(())
            });
        }

        #[test]
        fn proptest_rev_iterator_simple((db_entries, snapshots) in generate_db_entries_and_snapshots()) {
            let tmpdir = tempfile::tempdir().unwrap();
            let delta_reader = build_delta_reader_from(tmpdir.path(), &db_entries, &snapshots);

            let iterator = delta_reader.iter::<S>();
            prop_assert!(iterator.is_ok());
            let values: Vec<_> = iterator.unwrap().collect();
            prop_assert!(
                values.windows(2).all(|w| w[0].0 <= w[1].0),
                "iter should be sorted"
            );

            let iterator_rev = delta_reader.iter_rev::<S>();
            prop_assert!(iterator_rev.is_ok());
            let values_rev: Vec<_> = iterator_rev.unwrap().collect();
            prop_assert!(
                values_rev.windows(2).all(|w| w[0].0 >= w[1].0),
                "iter_rev should be sorted in reversed order"
            );
       }

        #[test]
        fn proptest_rev_iterator_everything_was_deleted((db_entries, snapshots) in generate_db_entries_and_snapshots()) {
            let values = execute_with_all_deleted_in_last_snapshot(db_entries, snapshots);
            prop_assert!(values.is_empty());
       }
    }
}
