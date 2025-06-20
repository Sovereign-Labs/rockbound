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

mod iterator;
mod metrics;
pub mod schema;
mod schema_batch;

mod config;
#[cfg(feature = "test-utils")]
pub mod test;

pub use config::{gen_rocksdb_options, RocksdbConfig};

use std::{path::Path, time::Duration};

use anyhow::format_err;
use iterator::ScanDirection;
pub use iterator::{SchemaIterator, SeekKeyEncoder};
use metrics::{
    SCHEMADB_BATCH_COMMIT_BYTES, SCHEMADB_BATCH_COMMIT_LATENCY_SECONDS, SCHEMADB_DELETES,
    SCHEMADB_DELETE_RANGE, SCHEMADB_GET_BYTES, SCHEMADB_GET_LATENCY_SECONDS, SCHEMADB_PUT_BYTES,
};
pub use rocksdb;
use rocksdb::ReadOptions;
pub use rocksdb::DEFAULT_COLUMN_FAMILY_NAME;
use thiserror::Error;
use tracing::{info, warn};

use crate::iterator::RawDbIter;
pub use crate::schema::Schema;
use crate::schema::{ColumnFamilyName, KeyCodec, ValueCodec};
pub use crate::schema_batch::SchemaBatch;

/// This DB is a schematized RocksDB wrapper where all data passed in and out are typed according to
/// [`Schema`]s.
#[derive(Debug)]
pub struct DB {
    name: &'static str, // for logging
    inner: rocksdb::DB,
}

impl DB {
    /// Opens a database backed by RocksDB, using the provided column family names and default
    /// column family options.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn open(
        path: impl AsRef<Path>,
        name: &'static str,
        column_families: impl IntoIterator<Item = impl Into<String>>,
        db_opts: &rocksdb::Options,
    ) -> anyhow::Result<Self> {
        let db = DB::open_with_cfds(
            db_opts,
            path,
            name,
            column_families.into_iter().map(|cf_name| {
                let mut cf_opts = rocksdb::Options::default();
                cf_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
                rocksdb::ColumnFamilyDescriptor::new(cf_name, cf_opts)
            }),
        )?;
        Ok(db)
    }

    /// Opens a database backed by RocksDB, using the provided column family names and default
    /// column family options.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn open_with_ttl(
        path: impl AsRef<Path>,
        name: &'static str,
        column_families: impl IntoIterator<Item = impl Into<String>>,
        db_opts: &rocksdb::Options,
        ttl: Duration,
    ) -> anyhow::Result<Self> {
        let db = DB::open_with_cfds_and_ttl(
            db_opts,
            path,
            name,
            column_families.into_iter().map(|cf_name| {
                let mut cf_opts = rocksdb::Options::default();
                cf_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
                rocksdb::ColumnFamilyDescriptor::new(cf_name, cf_opts)
            }),
            ttl,
        )?;
        Ok(db)
    }

    /// Open RocksDB with the provided column family descriptors and ttl.
    /// This allows the caller to configure options for each column family.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn open_with_cfds_and_ttl(
        db_opts: &rocksdb::Options,
        path: impl AsRef<Path>,
        name: &'static str,
        cfds: impl IntoIterator<Item = rocksdb::ColumnFamilyDescriptor>,
        ttl: Duration,
    ) -> anyhow::Result<DB> {
        let inner = rocksdb::DB::open_cf_descriptors_with_ttl(db_opts, path, cfds, ttl)?;
        Ok(Self::log_construct(name, inner))
    }

    /// Open RocksDB with the provided column family descriptors.
    /// This allows the caller to configure options for each column family.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn open_with_cfds(
        db_opts: &rocksdb::Options,
        path: impl AsRef<Path>,
        name: &'static str,
        cfds: impl IntoIterator<Item = rocksdb::ColumnFamilyDescriptor>,
    ) -> anyhow::Result<DB> {
        let inner = with_error_logging(
            || rocksdb::DB::open_cf_descriptors(db_opts, path, cfds),
            "open_with_cfds",
        )?;
        Ok(Self::log_construct(name, inner))
    }

    /// Open db in readonly mode. This db is completely static, so any writes that occur on the primary
    /// after it has been opened will not be visible to the readonly instance.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn open_cf_readonly(
        opts: &rocksdb::Options,
        path: impl AsRef<Path>,
        name: &'static str,
        cfs: Vec<ColumnFamilyName>,
    ) -> anyhow::Result<DB> {
        let error_if_log_file_exists = false;
        let inner = with_error_logging(
            || rocksdb::DB::open_cf_for_read_only(opts, path, cfs, error_if_log_file_exists),
            "open_cf_readonly",
        )?;

        Ok(Self::log_construct(name, inner))
    }

    /// Open db in secondary mode. A secondary db does not support writes, but can be dynamically caught up
    /// to the primary instance by a manual call. See <https://github.com/facebook/rocksdb/wiki/Read-only-and-Secondary-instances>
    /// for more details.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn open_cf_as_secondary<P: AsRef<Path>>(
        opts: &rocksdb::Options,
        primary_path: P,
        secondary_path: P,
        name: &'static str,
        cfs: Vec<ColumnFamilyName>,
    ) -> anyhow::Result<DB> {
        let inner = with_error_logging(
            || rocksdb::DB::open_cf_as_secondary(opts, primary_path, secondary_path, cfs),
            "open_cf_as_secondary",
        )?;
        Ok(Self::log_construct(name, inner))
    }

    fn log_construct(name: &'static str, inner: rocksdb::DB) -> DB {
        info!(rocksdb_name = name, path = %inner.path().display(), "Opened RocksDB");
        DB { name, inner }
    }

    /// Name of the database that can be used for logging or metrics or tracing.
    #[inline]
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Reads single record by key.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn get<S: Schema>(
        &self,
        schema_key: &impl KeyCodec<S>,
    ) -> anyhow::Result<Option<S::Value>> {
        with_error_logging::<_, _, anyhow::Error>(
            || {
                let _timer = SCHEMADB_GET_LATENCY_SECONDS
                    .with_label_values(&[S::COLUMN_FAMILY_NAME])
                    .start_timer();

                let k = schema_key.encode_key()?;
                let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;

                let result = self.inner.get_pinned_cf(cf_handle, k)?;
                SCHEMADB_GET_BYTES
                    .with_label_values(&[S::COLUMN_FAMILY_NAME])
                    .observe(result.as_ref().map_or(0.0, |v| v.len() as f64));

                result
                    .map(|raw_value| <S::Value as ValueCodec<S>>::decode_value(&raw_value))
                    .transpose()
                    .map_err(|err| err.into())
            },
            "get",
        )
    }

    #[tracing::instrument(skip_all, level = "error")]
    /// Reads a single record by key asynchronously.
    pub async fn get_async<S: Schema>(
        &self,
        schema_key: &impl KeyCodec<S>,
    ) -> anyhow::Result<Option<S::Value>> {
        tokio::task::block_in_place(|| self.get(schema_key))
    }

    /// Writes single record.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn put<S: Schema>(
        &self,
        key: &impl KeyCodec<S>,
        value: &impl ValueCodec<S>,
    ) -> anyhow::Result<()> {
        // Not necessary to use a batch, but we'd like a central place to bump counters.
        // Used in tests only anyway.
        with_error_logging(
            || {
                let mut batch = SchemaBatch::new();
                batch.put::<S>(key, value)?;
                self.write_schemas_inner(&batch)
            },
            "put",
        )
    }

    /// Writes a single record asynchronously.
    #[tracing::instrument(skip_all, level = "error")]
    pub async fn put_async<S: Schema>(
        &self,
        key: &impl KeyCodec<S>,
        value: &impl ValueCodec<S>,
    ) -> anyhow::Result<()> {
        tokio::task::block_in_place(|| self.put(key, value))
    }

    /// Delete a single key from the database.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn delete<S: Schema>(&self, key: &impl KeyCodec<S>) -> anyhow::Result<()> {
        // Not necessary to use a batch, but we'd like a central place to bump counters.
        // Used in tests only anyway.
        with_error_logging(
            || {
                let mut batch = SchemaBatch::new();
                batch.delete::<S>(key)?;
                self.write_schemas_inner(&batch)
            },
            "delete",
        )
    }

    /// Delete a single key from the database asynchronously.
    #[tracing::instrument(skip_all, level = "error")]
    pub async fn delete_async<S: Schema>(&self, key: &impl KeyCodec<S>) -> anyhow::Result<()> {
        tokio::task::block_in_place(|| self.delete(key))
    }

    /// Removes the database entries in the range `["from", "to")` using default write options.
    ///
    /// Note that this operation will be done lexicographic on the *encoding* of the seek keys. It is
    /// up to the table creator to ensure that the lexicographic ordering of the encoded seek keys matches the
    /// logical ordering of the type.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn delete_range<S: Schema>(
        &self,
        from: &impl SeekKeyEncoder<S>,
        to: &impl SeekKeyEncoder<S>,
    ) -> anyhow::Result<()> {
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        with_error_logging::<_, _, anyhow::Error>(
            || {
                let from = from.encode_seek_key()?;
                let to = to.encode_seek_key()?;
                self.inner.delete_range_cf(cf_handle, from, to)?;
                Ok(())
            },
            "delete_range",
        )
    }

    /// Removes the database entries in the range `["from", "to")` using default write options asynchronously.
    ///
    /// Note that this operation will be done lexicographic on the *encoding* of the seek keys. It is
    /// up to the table creator to ensure that the lexicographic ordering of the encoded seek keys matches the
    /// logical ordering of the type.
    #[tracing::instrument(skip_all, level = "error")]
    pub async fn delete_range_async<S: Schema>(
        &self,
        from: &impl SeekKeyEncoder<S>,
        to: &impl SeekKeyEncoder<S>,
    ) -> anyhow::Result<()> {
        tokio::task::block_in_place(|| self.delete_range(from, to))
    }

    fn iter_with_direction<S: Schema>(
        &self,
        opts: ReadOptions,
        direction: ScanDirection,
    ) -> anyhow::Result<SchemaIterator<S>> {
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        Ok(SchemaIterator::new(
            self.inner.raw_iterator_cf_opt(cf_handle, opts),
            direction,
        ))
    }

    /// Returns a forward [`SchemaIterator`] on a certain schema with the default read options.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn iter<S: Schema>(&self) -> anyhow::Result<SchemaIterator<S>> {
        self.iter_with_direction::<S>(Default::default(), ScanDirection::Forward)
    }

    /// Returns a range based [`SchemaIterator`] for the schema with the default read options.
    pub fn iter_range<S: Schema>(
        &self,
        from: &impl SeekKeyEncoder<S>,
        to: &impl SeekKeyEncoder<S>,
    ) -> anyhow::Result<SchemaIterator<S>> {
        with_error_logging(
            || {
                let mut opts = ReadOptions::default();
                opts.set_iterate_lower_bound(from.encode_seek_key()?);
                opts.set_iterate_upper_bound(to.encode_seek_key()?);
                self.iter_with_direction::<S>(opts, ScanDirection::Forward)
            },
            "iter_range",
        )
    }

    ///  Returns a [`RawDbIter`] which allows to iterate over raw values in specified [`ScanDirection`].
    pub(crate) fn raw_iter<S: Schema>(
        &self,
        direction: ScanDirection,
    ) -> anyhow::Result<RawDbIter> {
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        Ok(RawDbIter::new(&self.inner, cf_handle, .., direction))
    }

    /// Get a [`RawDbIter`] in given range and direction.
    pub(crate) fn raw_iter_range<S: Schema>(
        &self,
        range: impl std::ops::RangeBounds<SchemaKey>,
        direction: ScanDirection,
    ) -> anyhow::Result<RawDbIter> {
        if is_range_bounds_inverse(&range) {
            tracing::error!("[Rockbound]: error in raw_iter_range: lower_bound > upper_bound");
            anyhow::bail!("[Rockbound]: error in raw_iter_range: lower_bound > upper_bound");
        }
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        Ok(RawDbIter::new(&self.inner, cf_handle, range, direction))
    }

    /// Returns a forward [`SchemaIterator`] on a certain schema with the provided read options.
    pub fn iter_with_opts<S: Schema>(
        &self,
        opts: ReadOptions,
    ) -> anyhow::Result<SchemaIterator<S>> {
        self.iter_with_direction::<S>(opts, ScanDirection::Forward)
    }

    fn write_schemas_inner(&self, batch: &SchemaBatch) -> anyhow::Result<()> {
        let _timer = SCHEMADB_BATCH_COMMIT_LATENCY_SECONDS
            .with_label_values(&[self.name])
            .start_timer();
        let mut db_batch = rocksdb::WriteBatch::default();
        for (cf_name, rows) in batch.last_writes.iter() {
            let cf_handle = self.get_cf_handle(cf_name)?;
            for (key, operation) in rows {
                match operation {
                    Operation::Put { value } => db_batch.put_cf(cf_handle, key, value),
                    Operation::Delete => db_batch.delete_cf(cf_handle, key),
                    Operation::DeleteRange { .. } => {
                        warn!("Unexpected range operation found: {:?}", operation)
                    }
                }
            }
        }
        for (cf_name, operations) in batch.range_ops.iter() {
            let cf_handle = self.get_cf_handle(cf_name)?;
            for operation in operations {
                match operation {
                    Operation::DeleteRange { from, to } => {
                        db_batch.delete_range_cf(cf_handle, from, to)
                    }
                    _ => warn!(
                        "Unexpected non range based operation found: {:?}",
                        operation
                    ),
                }
            }
        }
        let serialized_size = db_batch.size_in_bytes();

        with_error_logging(
            || self.inner.write_opt(db_batch, &default_write_options()),
            "write_schemas::write_opt",
        )?;

        // Bump counters only after DB write succeeds.
        for (cf_name, rows) in batch.last_writes.iter() {
            for (key, operation) in rows {
                match operation {
                    Operation::Put { value } => {
                        SCHEMADB_PUT_BYTES
                            .with_label_values(&[cf_name])
                            .observe((key.len() + value.len()) as f64);
                    }
                    Operation::Delete => {
                        SCHEMADB_DELETES.with_label_values(&[cf_name]).inc();
                    }
                    Operation::DeleteRange { .. } => (),
                }
            }
        }
        for (cf_name, operations) in batch.range_ops.iter() {
            for operation in operations {
                if let Operation::DeleteRange { .. } = operation {
                    SCHEMADB_DELETE_RANGE.with_label_values(&[cf_name]).inc();
                }
            }
        }
        SCHEMADB_BATCH_COMMIT_BYTES
            .with_label_values(&[self.name])
            .observe(serialized_size as f64);

        Ok(())
    }

    #[tracing::instrument(skip_all, level = "error")]
    /// Writes a group of records wrapped in a [`SchemaBatch`].
    pub fn write_schemas(&self, batch: &SchemaBatch) -> anyhow::Result<()> {
        with_error_logging(|| self.write_schemas_inner(batch), "write_schemas")
    }

    #[tracing::instrument(skip_all, level = "error")]
    /// Writes a group of records wrapped in a [`SchemaBatch`] asynchronously.
    pub async fn write_schemas_async(&self, batch: &SchemaBatch) -> anyhow::Result<()> {
        tokio::task::block_in_place(|| {
            with_error_logging(|| self.write_schemas_inner(batch), "write_schemas_async")
        })
    }

    fn get_cf_handle(&self, cf_name: &str) -> anyhow::Result<&rocksdb::ColumnFamily> {
        with_error_logging(
            || {
                self.inner.cf_handle(cf_name).ok_or_else(|| {
                    format_err!(
                        "DB::cf_handle not found for column family name: {}",
                        cf_name
                    )
                })
            },
            "get_cf_handle",
        )
    }

    /// Flushes [MemTable](https://github.com/facebook/rocksdb/wiki/MemTable) data.
    /// This is only used for testing `get_approximate_sizes_cf` in unit tests.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn flush_cf(&self, cf_name: &str) -> anyhow::Result<()> {
        let handle = self.get_cf_handle(cf_name)?;
        with_error_logging(|| self.inner.flush_cf(handle), "flush_cf")
    }

    /// Trigger compaction. Primarily used for testing.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn trigger_compaction<S: Schema>(&self) -> anyhow::Result<()> {
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        self.inner
            .compact_range_cf::<&[u8], &[u8]>(&cf_handle, None, None);
        Ok(())
    }

    /// Returns the current RocksDB property value for the provided column family name
    /// and property name.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn get_property(&self, cf_name: &str, property_name: &str) -> anyhow::Result<u64> {
        with_error_logging(
            || {
                self.inner
                    .property_int_value_cf(self.get_cf_handle(cf_name)?, property_name)?
                    .ok_or_else(|| {
                        format_err!(
                            "Unable to get property \"{}\" of  column family \"{}\".",
                            property_name,
                            cf_name,
                        )
                    })
            },
            "get_property",
        )
    }

    /// Creates new physical DB checkpoint in directory specified by `path`.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn create_checkpoint<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        with_error_logging(
            || rocksdb::checkpoint::Checkpoint::new(&self.inner)?.create_checkpoint(path),
            "create_checkpoint",
        )
    }

    /// Creates new physical DB checkpoint in directory specified by `path` asynchronously.
    #[tracing::instrument(skip_all, level = "error")]
    pub async fn create_checkpoint_async<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        tokio::task::block_in_place(|| self.create_checkpoint(path))
    }
}

fn with_error_logging<F, T, E: Into<anyhow::Error>>(f: F, name: &str) -> anyhow::Result<T>
where
    F: FnOnce() -> Result<T, E>,
{
    let result = f().map_err(|e| e.into());
    if let Err(e) = &result {
        tracing::error!("[Rockbound] error during {}: {}", name, e);
    }
    result
}

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
    /// Deleting a range of values
    DeleteRange {
        /// Start of the range to delete
        from: SchemaKey,
        /// End of the range to delete
        to: SchemaKey,
    },
}

impl Operation {
    /// Returns [`S::Value`] if the operation is [`Operation::Put`] and `None` if [`Operation::Delete`].
    fn decode_value<S: Schema>(&self) -> anyhow::Result<Option<S::Value>> {
        match self {
            Operation::Put { value } => {
                let value = S::Value::decode_value(value)?;
                Ok(Some(value))
            }
            Operation::Delete | Operation::DeleteRange { .. } => Ok(None),
        }
    }
}

fn is_range_bounds_inverse(range: &impl std::ops::RangeBounds<SchemaKey>) -> bool {
    match (range.start_bound(), range.end_bound()) {
        (std::ops::Bound::Included(start), std::ops::Bound::Included(end)) => start > end,
        (std::ops::Bound::Included(start), std::ops::Bound::Excluded(end)) => start > end,
        (std::ops::Bound::Excluded(start), std::ops::Bound::Included(end)) => start > end,
        (std::ops::Bound::Excluded(start), std::ops::Bound::Excluded(end)) => start > end,
        (std::ops::Bound::Unbounded, _) => false,
        (_, std::ops::Bound::Unbounded) => false,
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

/// For now, we always use synchronous writes. This makes sure that once the operation returns
/// `Ok(())` the data is persisted even if the machine crashes. In the future we might consider
/// selectively turning this off for some non-critical writes to improve performance.
fn default_write_options() -> rocksdb::WriteOptions {
    let mut opts = rocksdb::WriteOptions::default();
    opts.set_sync(true);
    opts
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
