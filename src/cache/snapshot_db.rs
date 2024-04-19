//! Snapshot-aware DB accessor.
#![allow(dead_code)]

use std::collections::btree_map;
use std::iter::{Peekable, Rev};
use std::sync::Arc;

use crate::iterator::{RawDbIter, ScanDirection};
use crate::schema::KeyCodec;
use crate::{Operation, Schema, SchemaBatch, SchemaKey, SchemaValue, SeekKeyEncoder, DB};

/// Combines functionality of [`crate::cache::cache_container::CacheContainer`]
/// and [`crate::cache::cache_db::CacheDb`]
#[derive(Debug, Clone)]
pub struct SnapshotDb {
    /// Rocks DB
    /// TODO: Read only instance
    db: Arc<DB>,
    /// Change sets, in chronological order,
    /// Means that key in higher index has the most recent data.
    snapshots: Vec<Arc<SchemaBatch>>,
}

impl SnapshotDb {
    /// New Proper instance
    pub fn new(db: Arc<DB>, snapshots: Vec<Arc<SchemaBatch>>) -> Self {
        Self { db, snapshots }
    }

    /// TBD
    pub fn get<S: Schema>(&self, _key: &impl KeyCodec<S>) -> anyhow::Result<Option<S::Value>> {
        todo!()
    }

    /// TBD
    pub fn get_largest<S: Schema>(&self) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        todo!()
    }

    /// TBD
    pub fn get_prev<S: Schema>(
        &self,
        _seek_key: &impl SeekKeyEncoder<S>,
    ) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        todo!()
    }

    /// TBD
    pub fn iter<S: Schema>(
        &self,
    ) -> anyhow::Result<SnapshotDbIterator<'_, btree_map::Iter<SchemaKey, Operation>>> {
        todo!()
        //     let db_iter = self.db.raw_iter::<S>(ScanDirection::Forward)?;
        //     let snapshot_iterators = self
        //         .snapshots
        //         .iter()
        //         .rev()
        //         .map(|snapshot| snapshot.iter::<S>())
        //         .collect();
        //     Ok(SnapshotDbIterator::new(
        //         db_iter,
        //         snapshot_iterators,
        //         ScanDirection::Forward,
        //     ))
    }

    pub(crate) fn rev_iter<S: Schema>(
        &self,
    ) -> anyhow::Result<SnapshotDbIterator<'_, Rev<btree_map::Iter<SchemaKey, Operation>>>> {
        todo!()
        //     let db_iter = self.db.raw_iter::<S>(ScanDirection::Backward)?;
        //     let snapshot_iterators = self
        //         .snapshots
        //         .iter()
        //         .map(|snapshot| snapshot.iter::<S>())
        //         .collect();
        //     Ok(SnapshotDbIterator::new(
        //         db_iter,
        //         snapshot_iterators,
        //         ScanDirection::Backward,
        //     ))
    }
}

// TODO: Create iterator for SnapshotDb
#[derive(Debug)]
enum DataLocation {
    Db,
    // Index inside `snapshot_iterators`
    Snapshot(usize),
}

/// TBD
pub struct SnapshotDbIterator<'a, SnapshotIter>
where
    SnapshotIter: Iterator<Item = (&'a SchemaKey, &'a Operation)>,
{
    db_iter: Peekable<RawDbIter<'a>>,
    snapshot_iterators: Vec<Peekable<SnapshotIter>>,
    next_value_locations: Vec<DataLocation>,
    direction: ScanDirection,
}

impl<'a, SnapshotIter> SnapshotDbIterator<'a, SnapshotIter>
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

impl<'a, SnapshotIter> Iterator for SnapshotDbIterator<'a, SnapshotIter>
where
    SnapshotIter: Iterator<Item = (&'a SchemaKey, &'a Operation)>,
{
    type Item = (SchemaKey, SchemaValue);

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
