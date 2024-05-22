// SPDX-License-Identifier: Apache-2.0
// Adapted from aptos-core/schemadb

//! This library implements a schematized DB on top of [RocksDB](https://rocksdb.org/). It makes
//! sure all data passed in and out are structured according to predefined schemas and prevents
//! access to raw keys and values. This library also enforces a set of specific DB options,
//! like custom comparators and schema-to-column-family mapping.
//!
//! It requires that different kinds of key-value pairs be stored in separate column
//! families.  To use this library to store a kind of key-value pairs, the user needs to use the
//! [`define_schema!`] macro to define the schema name, the types of key and value, and name of the
//! column family.
#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod cache;

mod db;
mod iterator;
mod metrics;
pub mod schema;
mod schema_batch;

#[cfg(feature = "test-utils")]
pub mod test;

use std::sync::{Arc, LockResult, RwLock, RwLockReadGuard};

pub use db::DB;
pub use iterator::{SchemaIterator, SeekKeyEncoder};
pub use rocksdb;
pub use rocksdb::DEFAULT_COLUMN_FAMILY_NAME;
use thiserror::Error;

use crate::iterator::RawDbIter;
pub use crate::schema::Schema;
use crate::schema::{KeyCodec, ValueCodec};
pub use crate::schema_batch::SchemaBatch;

/// Readability alias for a key in the DB.
pub type SchemaKey = Vec<u8>;
/// Readability alias for a value in the DB.
pub type SchemaValue = Vec<u8>;

/// Represents operation written to the database.
#[cfg_attr(feature = "arbitrary", derive(proptest_derive::Arbitrary))]
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Operation {
    /// Writing a value to the DB.
    Put {
        /// Value to write
        value: SchemaValue,
    },
    /// Deleting a value
    Delete,
}

impl Operation {
    /// Returns [`S::Value`] if the operation is [`Operation::Put`] and `None` if [`Operation::Delete`].
    fn decode_value<S: Schema>(&self) -> anyhow::Result<Option<S::Value>> {
        match self {
            Operation::Put { value } => {
                let value = S::Value::decode_value(value)?;
                Ok(Some(value))
            }
            Operation::Delete => Ok(None),
        }
    }
}

fn is_range_bounds_inverse(range: &impl std::ops::RangeBounds<SchemaKey>) -> bool {
    use std::ops::Bound;

    match (range.start_bound(), range.end_bound()) {
        (Bound::Unbounded, _) => false,
        (_, Bound::Unbounded) => false,
        (
            Bound::Excluded(start) | Bound::Included(start),
            Bound::Excluded(end) | Bound::Included(end),
        ) => start > end,
    }
}

/// An error that occurred during (de)serialization of a [`Schema`]'s keys or
/// values.
#[derive(Error, Debug)]
pub enum CodecError {
    /// Unable to deserialize a key because it has a different length than
    /// expected.
    #[error("Invalid key length. Expected {expected:}, got {got:}")]
    #[allow(missing_docs)] // The fields' names are self-explanatory.
    InvalidKeyLength { expected: usize, got: usize },
    /// Some other error occurred when (de)serializing a key or value. Inspect
    /// the inner [`anyhow::Error`] for more details.
    #[error(transparent)]
    Wrapped(#[from] anyhow::Error),
    /// I/O error.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// For now we always use synchronous writes. This makes sure that once the operation returns
/// `Ok(())` the data is persisted even if the machine crashes. In the future we might consider
/// selectively turning this off for some non-critical writes to improve performance.
fn default_write_options() -> rocksdb::WriteOptions {
    let mut opts = rocksdb::WriteOptions::default();
    opts.set_sync(true);
    opts
}

/// Wrapper around `RwLock` that only allows read access.
/// This type implies that wrapped type suppose to be used only for reading.
/// It is useful to indicate that user of this type can only do reading.
/// This also implies that that inner `Arc<RwLock<T>>` is a clone and some other part can do writing.
#[derive(Debug, Clone)]
pub struct ReadOnlyLock<T> {
    lock: Arc<RwLock<T>>,
}

impl<T> ReadOnlyLock<T> {
    /// Create new [`ReadOnlyLock`] from [`Arc<RwLock<T>>`].
    pub fn new(lock: Arc<RwLock<T>>) -> Self {
        Self { lock }
    }

    /// Acquires a read lock on the underlying [`RwLock`].
    pub fn read(&self) -> LockResult<RwLockReadGuard<'_, T>> {
        self.lock.read()
    }
}

impl<T> From<Arc<RwLock<T>>> for ReadOnlyLock<T> {
    fn from(value: Arc<RwLock<T>>) -> Self {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_db_debug_output() {
        let tmpdir = tempfile::tempdir().unwrap();
        let column_families = vec![DEFAULT_COLUMN_FAMILY_NAME];

        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let db = DB::open(tmpdir.path(), "test_db_debug", column_families, &db_opts)
            .expect("Failed to open DB.");

        let db_debug = format!("{:?}", db);
        assert!(db_debug.contains("test_db_debug"));
        assert!(db_debug.contains(tmpdir.path().to_str().unwrap()));
    }

    #[test]
    fn test_range_inverse() {
        assert!(is_range_bounds_inverse(&(vec![4]..vec![3])));
        assert!(is_range_bounds_inverse(&(vec![4]..=vec![3])));
        // Not inverse, but empty
        assert!(!is_range_bounds_inverse(&(vec![3]..vec![3])));
        // Not inverse
        assert!(!is_range_bounds_inverse(&(vec![3]..=vec![3])));
    }
}
