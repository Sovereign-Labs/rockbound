#![allow(dead_code)]
//! This module contains the next iteration of [`crate::cache::cache_db::CacheDb`]
use crate::cache::change_set::ChangeSetIter;
use crate::iterator::{RawDbIter, ScanDirection};
use crate::schema::KeyCodec;
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
    /// Set of not finalized changes in **reverse** order.
    snapshots: Vec<Arc<SchemaBatch>>,
    /// Reading finalized data from here.
    db: DB,
}

impl DeltaReader {
    /// Creates new [`DeltaReader`] with given `id`, `db` and `uncommited_changes`.
    /// `uncommited_changes` should be in reverse order.
    pub fn new(db: DB, uncommited_changes: Vec<Arc<SchemaBatch>>) -> Self {
        Self {
            snapshots: uncommited_changes,
            db,
        }
    }

    /// Get a value, wherever it is.
    pub fn get<S: Schema>(&self, key: &impl KeyCodec<S>) -> anyhow::Result<Option<S::Value>> {
        // Some(Operation) means that key was touched,
        // but in case of deletion, we early return None
        // Only in case of not finding operation for a key,
        // we go deeper

        // 1. Check in snapshots, in reverse order
        for snapshot in self.snapshots.iter() {
            if let Some(operation) = snapshot.get_operation::<S>(key)? {
                return operation.decode_value::<S>();
            }
        }

        // 2. Check in DB
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
    fn iter_rev<S: Schema>(&self) -> anyhow::Result<DeltaReaderIter<Rev<ChangeSetIter>>> {
        // Snapshot iterators
        let snapshot_iterators = self
            .snapshots
            .iter()
            .map(|snapshot| snapshot.iter::<S>().rev())
            .collect::<Vec<_>>();

        // Db Iter
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

    fn check_value(delta_db: &DeltaReader, key: u32, expected_value: Option<u32>) {
        let actual_value = delta_db
            .get::<TestSchema>(&TestCompositeField(key, 0, 0))
            .unwrap()
            .map(|v| v.0);
        assert_eq!(expected_value, actual_value);
    }

    // End of test utils

    #[test]
    fn test_empty_iterator() {
        let tmpdir = tempfile::tempdir().unwrap();
        let db = open_db(tmpdir.path());

        let delta_db = DeltaReader::new(db, vec![]);

        let _iterator = delta_db.iter_rev::<TestSchema>().unwrap();
    }

    #[test]
    #[ignore = "TBD"]
    fn get_largest() {}

    #[test]
    #[ignore = "TBD"]
    fn get_prev() {}
}
