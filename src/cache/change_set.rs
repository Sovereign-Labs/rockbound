//! Collection of writes in given snapshot/cache layer
//! Data in ChangeSet is written inside crate, public access allows only reading.
use std::collections::btree_map;

use crate::cache::SnapshotId;
use crate::{KeyCodec, Operation, Schema, SchemaBatch, SchemaKey};

/// Iterator type returned by [`ChangeSet::iter`].
pub type ChangeSetIter<'a> = btree_map::Iter<'a, SchemaKey, Operation>;
/// Range type returned by [`ChangeSet::iter_range`].
pub type ChangeSetRange<'a> = btree_map::Range<'a, SchemaKey, Operation>;

/// Collection of all writes with associated [`SnapshotId`]
#[derive(Debug, Clone)]
pub struct ChangeSet {
    id: SnapshotId,
    pub(crate) operations: SchemaBatch,
}

impl ChangeSet {
    pub(crate) fn new(id: SnapshotId) -> Self {
        Self {
            id,
            operations: SchemaBatch::default(),
        }
    }

    /// Create new `ChangeSet
    pub fn new_with_operations(id: SnapshotId, operations: SchemaBatch) -> Self {
        Self { id, operations }
    }

    /// Get value from its own cache
    pub fn get<S: Schema>(&self, key: &impl KeyCodec<S>) -> anyhow::Result<Option<&Operation>> {
        self.operations.get(key)
    }

    /// Get the ID of this [`ChangeSet`].
    pub fn id(&self) -> SnapshotId {
        self.id
    }

    /// Iterate over all operations in snapshot in lexicographic order
    pub fn iter<S: Schema>(&self) -> ChangeSetIter {
        self.operations.iter::<S>()
    }

    /// Iterate over operations in lower_bound..upper_bound range in lexicographic order
    pub fn iter_range<S: Schema>(
        &self,
        range: impl std::ops::RangeBounds<SchemaKey>,
    ) -> ChangeSetRange {
        self.operations.iter_range::<S>(range)
    }
}

impl From<ChangeSet> for SchemaBatch {
    fn from(value: ChangeSet) -> Self {
        value.operations
    }
}
