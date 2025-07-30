//! Module with [`DeltaReader`], a handler of [`DB`] and the list of uncommited snapshots.
use crate::cache::PaginatedResponse;
use crate::iterator::{RawDbIter, ScanDirection};
use crate::schema::{KeyCodec, KeyDecoder, ValueCodec};
use crate::{Operation, Schema, SchemaBatch, SchemaKey, SchemaValue, SeekKeyEncoder, DB};
use std::cmp::Ordering;
use std::collections::btree_map;
use std::iter::{Peekable, Rev};
use std::sync::Arc;

/// Iterator type returned by [`DeltaReader::iter`].
pub type SnapshotIter<'a> = btree_map::Iter<'a, SchemaKey, Operation>;
/// Range type returned by [`DeltaReader::iter_range`].
pub type SnapshotIterRange<'a> = btree_map::Range<'a, SchemaKey, Operation>;

/// Read-only data provider that supports a list of snapshots on top of [`DB`].
/// Maintains total ordering and respects uncommited deletions.
/// Should not write to underlying [`DB`].
#[derive(Debug, Clone)]
pub struct DeltaReader {
    /// Set of not commited changes in chronological order.
    /// Meaning that the first snapshot in the vector is the oldest and the latest is the most recent.
    /// If keys are equal, the value from more recent snapshot is taken.
    snapshots: Vec<Arc<SchemaBatch>>,
    /// Reading finalized data from here.
    db: Arc<DB>,
}

impl DeltaReader {
    /// Creates new [`DeltaReader`] with given [`DB`] and vector with uncommited snapshots of [`SchemaBatch`].
    /// Snapshots should be in chronological order.
    pub fn new(db: Arc<DB>, snapshots: Vec<Arc<SchemaBatch>>) -> Self {
        Self { snapshots, db }
    }

    /// Get a value for given [`Schema`]. If value has been deleted in uncommitted changes, returns None.
    pub fn get<S: Schema>(&self, key: &impl KeyCodec<S>) -> anyhow::Result<Option<S::Value>> {
        // Some(Operation) means that key was touched,
        // but in case of deletion, we early return None
        // Only in case of not finding operation for a key,
        // we go deeper
        for snapshot in self.snapshots.iter().rev() {
            if let Some(operation) = snapshot.get_operation::<S>(key)? {
                return operation.decode_value::<S>();
            }
        }

        self.db.get(key)
    }

    /// Async version of [`DeltaReader::get`].
    pub async fn get_async<S: Schema>(
        &self,
        key: &impl KeyCodec<S>,
    ) -> anyhow::Result<Option<S::Value>> {
        tokio::task::block_in_place(|| self.get(key))
    }

    /// Get the value of the smallest key written value for given [`Schema`].
    pub fn get_smallest<S: Schema>(&self) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        let mut iterator = self.iter::<S>()?;
        if let Some((key, value)) = iterator.next() {
            let key = S::Key::decode_key(&key)?;
            let value = S::Value::decode_value(&value)?;
            return Ok(Some((key, value)));
        }
        Ok(None)
    }

    /// Async version of [`DeltaReader::get_smallest`].
    pub async fn get_smallest_async<S: Schema>(
        &self,
    ) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        tokio::task::block_in_place(|| self.get_smallest::<S>())
    }

    /// Get a value of the largest key written value for given [`Schema`].
    pub fn get_largest<S: Schema>(&self) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        let mut iterator = self.iter_rev::<S>()?;
        if let Some((key, value)) = iterator.next() {
            let key = S::Key::decode_key(&key)?;
            let value = S::Value::decode_value(&value)?;
            return Ok(Some((key, value)));
        }
        Ok(None)
    }

    /// Async version [`DeltaReader::get_largest`].
    pub async fn get_largest_async<S: Schema>(&self) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        tokio::task::block_in_place(|| self.get_largest::<S>())
    }

    /// Get the largest value in [`Schema`] that is smaller or equal given `seek_key`.
    pub fn get_prev<S: Schema>(
        &self,
        seek_key: &impl SeekKeyEncoder<S>,
    ) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        let seek_key = seek_key.encode_seek_key()?;
        let range = ..=seek_key;

        let mut iterator = self.iter_rev_range::<S>(range)?;
        if let Some((key, value)) = iterator.next() {
            let key = S::Key::decode_key(&key)?;
            let value = S::Value::decode_value(&value)?;
            return Ok(Some((key, value)));
        }
        Ok(None)
    }

    /// Async version of [`DeltaReader::get_prev`].
    pub async fn get_prev_async<S: Schema>(
        &self,
        seek_key: &impl SeekKeyEncoder<S>,
    ) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        tokio::task::block_in_place(|| self.get_prev(seek_key))
    }

    /// Get `n` keys >= `seek_key`, paginated.
    pub fn get_n_from_first_match<S: Schema>(
        &self,
        seek_key: &impl SeekKeyEncoder<S>,
        n: usize,
    ) -> anyhow::Result<PaginatedResponse<S>> {
        let seek_key = seek_key.encode_seek_key()?;
        let range = seek_key..;

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
    }

    /// Async version of  [`DeltaReader::get_n_from_first_match`].
    pub async fn get_n_from_first_match_async<S: Schema>(
        &self,
        seek_key: &impl SeekKeyEncoder<S>,
        n: usize,
    ) -> anyhow::Result<PaginatedResponse<S>> {
        tokio::task::block_in_place(|| self.get_n_from_first_match::<S>(seek_key, n))
    }

    /// Collects all key-value pairs in given range: `smallest..largest`.
    pub fn collect_in_range<S: Schema, Sk: SeekKeyEncoder<S>>(
        &self,
        range: std::ops::Range<Sk>,
    ) -> anyhow::Result<Vec<(S::Key, S::Value)>> {
        let lower_bound = range.start.encode_seek_key()?;
        let upper_bound = range.end.encode_seek_key()?;
        let range = lower_bound..upper_bound;

        let result = self
            .iter_range::<S>(range)?
            .map(|(key, value)| {
                let key = S::Key::decode_key(&key).unwrap();
                let value = S::Value::decode_value(&value).unwrap();
                (key, value)
            })
            .collect();
        Ok(result)
    }

    /// Async version of [`DeltaReader::collect_in_range`].
    pub async fn collect_in_range_async<S: Schema, Sk: SeekKeyEncoder<S>>(
        &self,
        range: std::ops::Range<Sk>,
    ) -> anyhow::Result<Vec<(S::Key, S::Value)>> {
        tokio::task::block_in_place(|| self.collect_in_range(range))
    }

    #[allow(dead_code)]
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
            ScanDirection::Forward,
        ))
    }

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

/// Core logic of [`DeltaReader`]. Handles matching keys and progresses underlying iterators.
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

            // For each snapshot, pick its next value.
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
                            Operation::Delete | Operation::DeleteRange { .. } => continue,
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
    use std::collections::BTreeMap;
    use std::path::Path;

    use rocksdb::DEFAULT_COLUMN_FAMILY_NAME;
    use tokio::runtime::Runtime;

    use super::*;
    use crate::schema::KeyEncoder;
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

    fn open_db(dir: impl AsRef<Path>) -> Arc<DB> {
        let column_families = vec![DEFAULT_COLUMN_FAMILY_NAME, TestSchema::COLUMN_FAMILY_NAME];
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        Arc::new(DB::open(dir, "test", column_families, &db_opts).expect("Failed to open DB."))
    }

    // Test utils
    fn encode_key(key: &TestCompositeField) -> SchemaKey {
        <TestCompositeField as KeyEncoder<S>>::encode_key(key).unwrap()
    }

    fn decode_key(key: &SchemaKey) -> TestCompositeField {
        <<S as Schema>::Key as KeyDecoder<S>>::decode_key(key).unwrap()
    }

    fn decode_value(value: &SchemaValue) -> TestField {
        <<S as Schema>::Value as ValueCodec<S>>::decode_value(value).unwrap()
    }

    // A DeltaReader with a known set of sample values, includes deletion.
    // DB contains fields 1, 4, 5, and 7.
    // Have 3 snapshots, value is equal to field_id * 10^snapshot_level:
    // 1. Written: field 6.
    // 2. Written: fields 2, 7. Deleted: field 5.
    // 3. Written: fields 3, 6. Deleted: fields 1, 7.
    // The final available values are:
    // [(2, 200), (3, 3000), (4, 4), (6, 6000)]
    // Idea behind each field:
    // FIELD_1: Value from DB deleted in snapshot
    // FIELD_2: Value written in previous snapshot
    // FIELD_3: Value written in the most recent snapshot (traversal to the most recent)
    // FIELD_4: Value written in DB and wasn't touched since (traversal to DB)
    // FIELD_5: Value was deleted before a recent snapshot.
    // FIELD_6: Value from snapshot has been overwritten.
    // FIELD_7: Value has been deleted in the most recent snapshot.
    fn build_sample_delta_reader(path: impl AsRef<Path>) -> DeltaReader {
        let db = open_db(path.as_ref());
        let mut schema_batch_0 = SchemaBatch::new();
        schema_batch_0.put::<S>(&FIELD_1, &TestField(1)).unwrap();
        schema_batch_0.put::<S>(&FIELD_4, &TestField(4)).unwrap();
        schema_batch_0.put::<S>(&FIELD_5, &TestField(5)).unwrap();
        schema_batch_0.put::<S>(&FIELD_7, &TestField(7)).unwrap();
        db.write_schemas(schema_batch_0).unwrap();

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
    async fn empty() {
        let tmpdir = tempfile::tempdir().unwrap();
        let db = open_db(tmpdir.path());

        let delta_reader = DeltaReader::new(db, vec![]);

        let largest = delta_reader.get_largest_async::<S>().await.unwrap();
        assert!(largest.is_none());

        let key = TestCompositeField::MAX;
        let prev = delta_reader.get_prev_async::<S>(&key).await.unwrap();
        assert!(prev.is_none());

        let value_1 = delta_reader.get_async::<S>(&FIELD_1).await.unwrap();
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
            .get_n_from_first_match_async::<S>(&TestCompositeField::MAX, 100)
            .await
            .unwrap();
        assert!(paginated_response.key_value.is_empty());
        assert!(paginated_response.next.is_none());

        let values: Vec<_> = delta_reader
            .collect_in_range_async::<S, TestCompositeField>(
                TestCompositeField::MIN..TestCompositeField::MAX,
            )
            .await
            .unwrap();
        assert!(values.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_from_sample() {
        let tmpdir = tempfile::tempdir().unwrap();
        let delta_reader = build_sample_delta_reader(tmpdir.path());

        // From DB
        let value = delta_reader.get_async::<S>(&FIELD_4).await.unwrap();
        assert_eq!(Some(TestField(4)), value);

        // From the most recent snapshot
        let value = delta_reader.get_async::<S>(&FIELD_3).await.unwrap();
        assert_eq!(Some(TestField(3000)), value);
        let value = delta_reader.get_async::<S>(&FIELD_6).await.unwrap();
        assert_eq!(Some(TestField(6000)), value);

        // From middle snapshot
        let value = delta_reader.get_async::<S>(&FIELD_2).await.unwrap();
        assert_eq!(Some(TestField(200)), value);

        // Deleted values
        let value = delta_reader.get_async::<S>(&FIELD_7).await.unwrap();
        assert!(value.is_none());
        let value = delta_reader.get_async::<S>(&FIELD_5).await.unwrap();
        assert!(value.is_none());

        // Not found
        let value = delta_reader
            .get_async::<S>(&TestCompositeField::MIN)
            .await
            .unwrap();
        assert!(value.is_none());
        let value = delta_reader
            .get_async::<S>(&TestCompositeField::MAX)
            .await
            .unwrap();
        assert!(value.is_none());
        let value = delta_reader
            .get_async::<S>(&TestCompositeField(3, 5, 1))
            .await
            .unwrap();
        assert!(value.is_none());
    }

    fn basic_check_iterator_range(
        delta_reader: &DeltaReader,
        expected_len: usize,
        range: impl std::ops::RangeBounds<SchemaKey> + Clone + std::fmt::Debug,
    ) {
        let values: Vec<_> = delta_reader
            .iter_range::<S>(range.clone())
            .unwrap()
            .collect();
        assert_eq!(
            expected_len,
            values.len(),
            "length do no match for iter_range: {:?} ",
            range
        );
        assert!(
            values.windows(2).all(|w| w[0].0 <= w[1].0),
            "iter should be sorted for range: {:?}",
            range
        );
        let values_rev: Vec<_> = delta_reader
            .iter_rev_range::<S>(range.clone())
            .unwrap()
            .collect();
        assert_eq!(
            expected_len,
            values_rev.len(),
            "length do no match for iter_rev_range:{:?}",
            range
        );
        assert!(
            values_rev.windows(2).all(|w| w[0].0 >= w[1].0),
            "iter_rev should be sorted in reversed order for range: {:?}",
            range
        );
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

        // Effectively, all these ranges are full, so iterator should return all possible values
        let min_key = encode_key(&TestCompositeField::MIN);
        let max_key = encode_key(&TestCompositeField::MAX);

        basic_check_iterator_range(delta_reader, expected_len, ..);
        basic_check_iterator_range(delta_reader, expected_len, min_key.clone()..);
        basic_check_iterator_range(delta_reader, expected_len, min_key.clone()..max_key.clone());
        basic_check_iterator_range(
            delta_reader,
            expected_len,
            min_key.clone()..=max_key.clone(),
        );
        basic_check_iterator_range(delta_reader, expected_len, ..max_key.clone());
        basic_check_iterator_range(delta_reader, expected_len, ..=max_key.clone());
    }

    #[test]
    fn iterator_sample() {
        let tmpdir = tempfile::tempdir().unwrap();
        let delta_reader = build_sample_delta_reader(tmpdir.path());

        basic_check_iterator(&delta_reader, 4);

        let range = encode_key(&FIELD_1)..encode_key(&FIELD_2);
        basic_check_iterator_range(&delta_reader, 0, range);
        let range = ..encode_key(&FIELD_2);
        basic_check_iterator_range(&delta_reader, 0, range);
        let range = encode_key(&FIELD_1)..=encode_key(&FIELD_2);
        basic_check_iterator_range(&delta_reader, 1, range);
        let range = encode_key(&FIELD_3)..=encode_key(&FIELD_6);
        basic_check_iterator_range(&delta_reader, 3, range);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_largest_sample() {
        let tmpdir = tempfile::tempdir().unwrap();
        let delta_reader = build_sample_delta_reader(tmpdir.path());

        let largest = delta_reader.get_largest_async::<S>().await.unwrap();
        assert!(largest.is_some(), "largest value is not found");
        let (largest_key, largest_value) = largest.unwrap();

        assert_eq!(FIELD_6, largest_key);
        assert_eq!(TestField(6000), largest_value);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_prev_sample() {
        let tmpdir = tempfile::tempdir().unwrap();
        let delta_reader = build_sample_delta_reader(tmpdir.path());

        // MIN, should not find anything
        let prev = delta_reader
            .get_prev_async::<S>(&TestCompositeField::MIN)
            .await
            .unwrap();
        assert!(prev.is_none());

        // Should get the lowest value in
        let prev = delta_reader.get_prev_async::<S>(&FIELD_1).await.unwrap();
        assert!(prev.is_none());

        let prev = delta_reader.get_prev_async::<S>(&FIELD_2).await.unwrap();
        let (prev_key, prev_value) = prev.unwrap();
        assert_eq!(FIELD_2, prev_key);
        assert_eq!(TestField(200), prev_value);

        // Some value in the middle
        let (prev_key, prev_value) = delta_reader
            .get_prev_async::<S>(&FIELD_3)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(FIELD_3, prev_key);
        assert_eq!(TestField(3000), prev_value);

        // Value in between
        let (prev_key, prev_value) = delta_reader
            .get_prev_async::<S>(&TestCompositeField(3, 5, 8))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(FIELD_3, prev_key);
        assert_eq!(TestField(3000), prev_value);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_n_sample() {
        let tmpdir = tempfile::tempdir().unwrap();
        let delta_reader = build_sample_delta_reader(tmpdir.path());

        let paginated_response = delta_reader
            .get_n_from_first_match_async::<S>(&TestCompositeField::MIN, 2)
            .await
            .unwrap();
        assert_eq!(2, paginated_response.key_value.len());
        let expected_first_page = vec![(FIELD_2, TestField(200)), (FIELD_3, TestField(3000))];
        assert_eq!(expected_first_page, paginated_response.key_value);
        assert_eq!(Some(FIELD_4), paginated_response.next);

        let paginated_response = delta_reader
            .get_n_from_first_match_async::<S>(&FIELD_4, 2)
            .await
            .unwrap();
        assert_eq!(2, paginated_response.key_value.len());
        let expected_first_page = vec![(FIELD_4, TestField(4)), (FIELD_6, TestField(6000))];
        assert_eq!(expected_first_page, paginated_response.key_value);
        assert!(paginated_response.next.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn collect_in_range_sample() {
        let tmpdir = tempfile::tempdir().unwrap();
        let delta_reader = build_sample_delta_reader(tmpdir.path());

        let all_values: Vec<_> = delta_reader
            .iter::<S>()
            .unwrap()
            .map(|(k, v)| (decode_key(&k), decode_value(&v)))
            .collect();
        assert_eq!(4, all_values.len());

        let test_cases = vec![
            (FIELD_2..FIELD_3, ..1),
            (FIELD_2..FIELD_4, ..2),
            (FIELD_2..FIELD_5, ..3),
            (FIELD_2..FIELD_6, ..3),
            (FIELD_2..FIELD_7, ..4),
        ];

        for (field_range, expected_range) in test_cases {
            let range_values = delta_reader
                .collect_in_range_async::<S, TestCompositeField>(field_range.clone())
                .await
                .unwrap();

            assert_eq!(
                all_values[expected_range].to_vec(),
                range_values,
                "failed for range {:?}",
                field_range
            )
        }
    }

    fn build_delta_reader_from(
        path: impl AsRef<Path>,
        db_entries: &[(TestCompositeField, TestField)],
        snapshots: &[Vec<(TestCompositeField, TestField)>],
    ) -> DeltaReader {
        let db = open_db(path.as_ref());

        let mut all_kv: BTreeMap<TestCompositeField, TestField> = BTreeMap::new();

        let mut db_batch = SchemaBatch::new();
        for (key, value) in db_entries {
            db_batch.put::<S>(key, value).unwrap();
            all_kv.insert(key.clone(), *value);
        }
        db.write_schemas(db_batch).unwrap();

        let mut schema_batches = Vec::new();
        for snapshot in snapshots {
            let mut schema_batch = SchemaBatch::new();
            for (key, value) in snapshot {
                schema_batch.put::<S>(key, value).unwrap();
                all_kv.insert(key.clone(), *value);
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
        db.write_schemas(db_batch).unwrap();

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
            let delta_reader = build_delta_reader_from(tmpdir.path(), &db_entries, &snapshots);

            // Building a reference for all K/V for validation.
            let mut all_kv: BTreeMap<TestCompositeField, TestField> = BTreeMap::new();
            for (key, value) in db_entries {
                all_kv.insert(key, value);
            }
            for snapshot in snapshots {
                for (key, value) in snapshot {
                    all_kv.insert(key.clone(), value);
                }
            }

            let rt = Runtime::new().unwrap();
            let _ = rt.block_on(async {

                let largest = delta_reader.get_largest_async::<S>().await.unwrap();
                let prev = delta_reader
                            .get_prev_async::<S>(&TestCompositeField::MAX)
                            .await;
                prop_assert!(prev.is_ok());
                let prev = prev.unwrap();
                prop_assert_eq!(largest.clone(), prev);
                match all_kv.iter().max() {
                    None => {
                        prop_assert!(largest.is_none());
                    },
                    Some((k, v)) => {
                        prop_assert_eq!(Some((k.clone(), *v)), largest);
                    }
                }

                for (key, expected_value) in all_kv.into_iter() {
                    let value = delta_reader.get_async::<S>(&key).await;
                    prop_assert!(value.is_ok());
                    let value = value.unwrap();
                    prop_assert_eq!(Some(expected_value), value);
                    let prev_value = delta_reader.get_prev_async::<S>(&key).await;
                    prop_assert!(prev_value.is_ok());
                    let prev_value = prev_value.unwrap();
                    prop_assert_eq!(Some((key, expected_value)), prev_value);
                }

                Ok(())
            });
        }

        #[test]
        fn proptest_iterator_only_writes((db_entries, snapshots) in generate_db_entries_and_snapshots()) {
            let tmpdir = tempfile::tempdir().unwrap();
            let delta_reader = build_delta_reader_from(tmpdir.path(), &db_entries, &snapshots);

            // Check ordering.
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

            // Building a reference for all K/V for validation if the basic check is passed.
            let mut all_kv: BTreeMap<TestCompositeField, TestField> = BTreeMap::new();
            for (key, value) in db_entries {
                all_kv.insert(key, value);
            }
            for snapshot in snapshots {
                for (key, value) in snapshot {
                    all_kv.insert(key.clone(), value);
                }
            }

            let rt = Runtime::new().unwrap();
            let _ = rt.block_on(async {
                // get_n_from
                for n in 1..4 {
                    let mut next_key = Some(TestCompositeField::MIN);
                    while let Some(actual_next_key) = next_key {
                        let paginated_response = delta_reader
                            .get_n_from_first_match_async::<S>(&actual_next_key, n)
                            .await;
                        prop_assert!(paginated_response.is_ok());
                        let paginated_response = paginated_response.unwrap();
                        if paginated_response.key_value.is_empty() {
                            break;
                        }
                        let first_on_page = paginated_response.key_value[0].clone();
                        prop_assert!(first_on_page.0 >= actual_next_key);
                        next_key = paginated_response.next;
                    }
                }
                // Early return on empty values.
                if values.is_empty() {
                    return Ok(())
                }

                // collect_range, checking full ranges
                let all_values: Vec<_> = values
                    .into_iter()
                    .map(|(k, v)| (decode_key(&k), decode_value(&v)))
                    .collect();

                let (min_key, _) = all_kv.iter().min().unwrap();
                let (max_key, _) = all_kv.iter().max().unwrap();

                let full_ranges = vec![
                    TestCompositeField::MIN..TestCompositeField::MAX,
                    min_key.clone()..TestCompositeField::MAX,
                ];
                // last key is definitely excluded
                let def_chopped_ranges = vec![
                    TestCompositeField::MIN..max_key.clone(),
                    min_key.clone()..max_key.clone(),
                ];

                for range in def_chopped_ranges {
                     let range_values = delta_reader
                        .collect_in_range_async::<S, TestCompositeField>(range)
                        .await;
                    prop_assert!(range_values.is_ok());
                    let range_values = range_values.unwrap();
                    prop_assert_eq!(
                        &all_values[..all_values.len()],
                        &range_values[..]
                    );
                }

                for range in full_ranges {
                     let range_values = delta_reader
                        .collect_in_range_async::<S, TestCompositeField>(range)
                        .await;
                    prop_assert!(range_values.is_ok());
                    let range_values = range_values.unwrap();

                    let last_key = all_values.iter().last().map(|(k, _v)| k);
                    let expected_values = if last_key == Some(&TestCompositeField::MAX) {
                        &all_values[..all_values.len()]
                    } else {
                        &all_values[..]
                    };
                    prop_assert_eq!(expected_values, &range_values[..]);
                }

                Ok(())
            });
       }

       #[test]
       fn proptest_rev_iterator_everything_was_deleted((db_entries, snapshots) in generate_db_entries_and_snapshots()) {
           let values = execute_with_all_deleted_in_last_snapshot(db_entries, snapshots);
           prop_assert!(values.is_empty());
       }
    }
}
