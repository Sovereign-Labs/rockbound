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
/// Provides a database for versioned key-value pairs with one live value and N historical values.
pub mod versioned_db;

mod config;
#[cfg(feature = "test-utils")]
pub mod test;

pub use config::{gen_rocksdb_options, RocksdbConfig};

use std::{borrow::Borrow, path::Path, sync::Arc};

use anyhow::format_err;
use iterator::ScanDirection;
pub use iterator::{SchemaIterator, SeekKeyEncoder};
use metrics::{
    SCHEMADB_BATCH_COMMIT_BYTES, SCHEMADB_BATCH_COMMIT_LATENCY_SECONDS, SCHEMADB_DELETES,
    SCHEMADB_DELETE_RANGE, SCHEMADB_GET_BYTES, SCHEMADB_GET_LATENCY_SECONDS, SCHEMADB_PUT_BYTES,
};
use parking_lot::RwLock;
use quick_cache::{sync::Cache, Equivalent, Weighter};
pub use rocksdb;
use rocksdb::ReadOptions;
pub use rocksdb::DEFAULT_COLUMN_FAMILY_NAME;
use thiserror::Error;
use tracing::{info, warn};

pub use crate::schema::Schema;
use crate::schema::{ColumnFamilyName, KeyCodec, ValueCodec};
pub use crate::schema_batch::SchemaBatch;
use crate::{iterator::RawDbIter, schema::KeyEncoder};

#[derive(Clone, Debug)]
struct BasicWeighter;

impl<T: Borrow<SchemaKey>> Weighter<(ColumnFamilyName, T), Option<SchemaValue>> for BasicWeighter {
    fn weight(&self, key: &(ColumnFamilyName, T), value: &Option<SchemaValue>) -> u64 {
        // 8 bytes for the pointer to the ColumnFamilyName (a 'static str) plus the key and value lengths
        // 3 words (24 bytes) each on the stack for the Vec<u8>s, plus their capacity
        8 + 24
            + 24
            + key.1.borrow().capacity() as u64
            + value.as_ref().map_or(0, |v| v.capacity()) as u64
    }
}

/// A newtype for tuple to allow implementing `Equivalent` for it.
#[derive(Debug, Hash)]
struct Pair<A, B>(pub A, pub B);
impl Equivalent<(ColumnFamilyName, SchemaKey)> for Pair<ColumnFamilyName, &[u8]> {
    fn equivalent(&self, rhs: &(ColumnFamilyName, SchemaKey)) -> bool {
        self.0 == rhs.0 && self.1 == rhs.1
    }
}

pub(crate) type DbCache = Cache<(ColumnFamilyName, SchemaKey), Option<SchemaValue>, BasicWeighter>;

/// This DB is a schematized RocksDB wrapper where all data passed in and out are typed according to
/// [`Schema`]s.
#[derive(Debug)]
pub struct DB {
    name: &'static str, // for logging
    // We use an RwLock to ensure consistency between the cache and the DB. Writes to the DB also grab the write lock.
    // on the cache.
    cache: RwLock<DbCache>,
    // All iteration circumvents the lock on the DB. This is fine, since we enforce that iterable column families are not cachable.
    db: Arc<rocksdb::DB>,
    cacheable_column_families: Vec<String>,
}

/// Returns the default column family descriptor. Includes LZ4 compression.
pub fn default_cf_descriptor(cf_name: impl Into<String>) -> rocksdb::ColumnFamilyDescriptor {
    let mut cf_opts = rocksdb::Options::default();
    cf_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    rocksdb::ColumnFamilyDescriptor::new(cf_name, cf_opts)
}

impl DB {
    /// Opens a database backed by RocksDB, using the provided column family names and default
    /// column family options. The opened DB does not support caching. If you need caching, use the `open_with_cfds` method instead.
    ///
    /// The `column_families` iterator contains the column family name and a boolean indicating whether the column family should be cached.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn open(
        path: impl AsRef<Path>,
        name: &'static str,
        column_families: impl IntoIterator<Item = impl Into<String>>,
        db_opts: &rocksdb::Options,
    ) -> anyhow::Result<Self> {
        let descriptors = column_families
            .into_iter()
            .map(|cf| default_cf_descriptor(cf.into()));
        let db = DB::open_with_cfds(db_opts, path, name, descriptors, vec![], 0)?;
        Ok(db)
    }

    /// Open RocksDB with the provided column family descriptors.
    /// This allows the caller to configure options for each column family.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn open_with_cfds(
        db_opts: &rocksdb::Options,
        path: impl AsRef<Path>,
        name: &'static str,
        cfds: impl IntoIterator<Item = rocksdb::ColumnFamilyDescriptor>,
        cacheable_column_families: Vec<String>,
        cache_size: usize,
    ) -> anyhow::Result<DB> {
        let inner = with_error_logging(
            || rocksdb::DB::open_cf_descriptors(db_opts, path, cfds),
            "open_with_cfds",
        )?;
        Ok(Self::log_construct(
            name,
            inner,
            cache_size,
            cacheable_column_families,
        ))
    }

    // Cache size estimation: we want to allocate about 1GB for cache. Estimate that slot keys are about 80 bytes and slot values
    // are around 400 bytes + 56 bytes of overhead on the stack (see weighter). Multiply by 1.5 to account for overhead (per quick-cache docs).
    // That gives estimated item capacity of 1GB / (80 + 400 + 56) * 1.5 ~= 1.2M.
    fn log_construct(
        name: &'static str,
        inner: rocksdb::DB,
        cache_size: usize,
        cacheable_column_families: Vec<String>,
    ) -> DB {
        info!(rocksdb_name = name, path = %inner.path().display(), "Opened RocksDB");
        DB {
            name,
            cache: RwLock::new(Cache::with_weighter(
                ((cache_size / (80 + 400 + 56)) * 3) / 2,
                cache_size as u64,
                BasicWeighter,
            )),
            db: Arc::new(inner),
            cacheable_column_families,
        }
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
        schema_key: &impl KeyEncoder<S>,
    ) -> anyhow::Result<Option<S::Value>> {
        self.get_raw::<S>(schema_key.encode_key()?.as_slice())
    }

    /// Reads single record by key.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn get_raw<S: Schema>(
        &self,
        encoded_schema_key: &[u8],
    ) -> anyhow::Result<Option<S::Value>> {
        with_error_logging::<_, _, anyhow::Error>(
            || {
                let _timer = SCHEMADB_GET_LATENCY_SECONDS
                    .with_label_values(&[S::COLUMN_FAMILY_NAME])
                    .start_timer();

                let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;

                // Only grab the read lock if we're accessing a cached column family.
                if S::SHOULD_CACHE {
                    let lock = self.cache.try_read();
                    // If the cache is locked for writing, just fall back to the DB. It provides consistency without locking
                    // Otherwise, we'll hold the lock until we've finished reading and (if necessary) modifying the cache.
                    // This will block any commits to the DB during this critical section, which is what we need; otherwise,
                    // a bad thread interleaving could cause us to read from the DB, get pre-empted while another thread writes, and then write
                    // the stale value to the cache
                    if let Some(cache) = lock.as_ref() {
                        let cache_result =
                            cache.get(&Pair(S::COLUMN_FAMILY_NAME, encoded_schema_key));
                        if let Some(result) = cache_result {
                            return result
                                .map(|v| <S::Value as ValueCodec<S>>::decode_value(&v))
                                .transpose()
                                .map_err(|err| err.into());
                        }
                    }

                    let result = self.db.get_pinned_cf(cf_handle, encoded_schema_key)?;
                    // If the cache is locked for writing, don't try to put the value, just return
                    if let Some(cache) = lock {
                        // Note: We have to deserialize the value while holding the read lock because the lifetime of the borrow is tied to `inner`.
                        // This prevents us from unifying the two branches.
                        // Note: We don't count bytes read from the cache
                        cache.insert(
                            (S::COLUMN_FAMILY_NAME, encoded_schema_key.to_vec()),
                            result.as_ref().map(|v| v.to_vec()),
                        );
                    }
                    return result
                        .map(|raw_value| <S::Value as ValueCodec<S>>::decode_value(&raw_value))
                        .transpose()
                        .map_err(|err| err.into());
                }

                let result = self.db.get_pinned_cf(cf_handle, encoded_schema_key)?;
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
                self.write_schemas_inner(batch)
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
                self.write_schemas_inner(batch)
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
                assert!(
                    !S::SHOULD_CACHE,
                    "Range deletes are incompatible with caching!"
                );
                self.db.delete_range_cf(cf_handle, from, to)?;
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
    ) -> anyhow::Result<SchemaIterator<'_, S>> {
        assert!(
            !S::SHOULD_CACHE,
            "Caching is incompatible with iterators! Cannot iterate over {}",
            S::COLUMN_FAMILY_NAME
        );
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        Ok(SchemaIterator::new(
            self.db.raw_iterator_cf_opt(cf_handle, opts),
            direction,
        ))
    }

    /// Returns a forward [`SchemaIterator`] on a certain schema with the default read options.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn iter<S: Schema>(&self) -> anyhow::Result<SchemaIterator<'_, S>> {
        self.iter_with_direction::<S>(Default::default(), ScanDirection::Forward)
    }

    /// Returns a range based [`SchemaIterator`] for the schema with the default read options.
    pub fn iter_range<S: Schema>(
        &self,
        from: &impl SeekKeyEncoder<S>,
        to: &impl SeekKeyEncoder<S>,
    ) -> anyhow::Result<SchemaIterator<'_, S>> {
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
    ) -> anyhow::Result<RawDbIter<'_>> {
        assert!(
            !S::SHOULD_CACHE,
            "Caching is incompatible with iterators! Cannot iterate over {}",
            S::COLUMN_FAMILY_NAME
        );
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        Ok(RawDbIter::new(&self.db, cf_handle, .., direction))
    }

    /// Get a [`RawDbIter`] in given range and direction.
    pub(crate) fn raw_iter_range<S: Schema>(
        &self,
        range: impl std::ops::RangeBounds<SchemaKey>,
        direction: ScanDirection,
    ) -> anyhow::Result<RawDbIter<'_>> {
        assert!(
            !S::SHOULD_CACHE,
            "Caching is incompatible with iterators! Cannot iterate over {}",
            S::COLUMN_FAMILY_NAME
        );
        if is_range_bounds_inverse(&range) {
            tracing::error!("[Rockbound]: error in raw_iter_range: lower_bound > upper_bound");
            anyhow::bail!("[Rockbound]: error in raw_iter_range: lower_bound > upper_bound");
        }
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        Ok(RawDbIter::new(&self.db, cf_handle, range, direction))
    }

    /// Iterator over a range of keys in a schema, allowing iteration over cached column families. This is only correct if a lock is held to ensure consistency.
    pub(crate) fn iter_range_allow_cached<'a, S: Schema>(
        &'a self,
        _guard: &parking_lot::RwLockReadGuard<'a, DbCache>,
        range: impl std::ops::RangeBounds<SchemaKey>,
        direction: ScanDirection,
    ) -> anyhow::Result<RawDbIter<'a>> {
        if is_range_bounds_inverse(&range) {
            tracing::error!("[Rockbound]: error in raw_iter_range: lower_bound > upper_bound");
            anyhow::bail!("[Rockbound]: error in raw_iter_range: lower_bound > upper_bound");
        }
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        Ok(RawDbIter::new(&self.db, cf_handle, range, direction))
    }

    /// Returns a forward [`SchemaIterator`] on a certain schema with the provided read options.
    pub fn iter_with_opts<S: Schema>(
        &self,
        opts: ReadOptions,
    ) -> anyhow::Result<SchemaIterator<'_, S>> {
        self.iter_with_direction::<S>(opts, ScanDirection::Forward)
    }

    fn write_schemas_inner(&self, batch: SchemaBatch) -> anyhow::Result<()> {
        let _timer = SCHEMADB_BATCH_COMMIT_LATENCY_SECONDS
            .with_label_values(&[self.name])
            .start_timer();
        // Update the next version to commit if relevant.
        // Block any readers while the DB isn't fully consistent
        let cache = self.cache.write();

        let mut db_batch = rocksdb::WriteBatch::default();
        let mut columns_written = Vec::with_capacity(batch.last_writes.len());
        for (cf_name, rows) in batch.last_writes.into_iter() {
            let should_cache = self
                .cacheable_column_families
                .iter()
                .any(|cf| cf == cf_name);
            let cf_handle = self.get_cf_handle(cf_name)?;
            let mut write_sizes = Vec::with_capacity(rows.len());
            let mut deletes_for_cf = 0;
            for (key, operation) in rows {
                match operation {
                    Operation::Put { value } => {
                        write_sizes.push(key.len() + value.len());
                        db_batch.put_cf(cf_handle, &key, &value);
                        if should_cache {
                            cache.insert((cf_name, key), Some(value));
                        }
                    }
                    Operation::Delete => {
                        db_batch.delete_cf(cf_handle, &key);
                        if should_cache {
                            cache.insert((cf_name, key), None);
                        }
                        deletes_for_cf += 1;
                    }
                    Operation::DeleteRange { .. } => {
                        warn!("Unexpected range operation found: {:?}", operation)
                    }
                }
            }
            columns_written.push((cf_name, write_sizes, deletes_for_cf));
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
            || self.db.write_opt(db_batch, &default_write_options()),
            "write_schemas::write_opt",
        )?;
        // Drop the write lock on the cache.
        drop(cache);

        // Bump counters only after DB write succeeds.
        for (cf_name, bytes, deletes) in columns_written {
            for write_size in bytes {
                SCHEMADB_PUT_BYTES
                    .with_label_values(&[cf_name])
                    .observe(write_size as f64);
            }
            SCHEMADB_DELETES
                .with_label_values(&[cf_name])
                .inc_by(deletes);
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
    pub fn write_schemas(&self, batch: SchemaBatch) -> anyhow::Result<()> {
        with_error_logging(|| self.write_schemas_inner(batch), "write_schemas")
    }

    #[tracing::instrument(skip_all, level = "error")]
    /// Writes a group of records wrapped in a [`SchemaBatch`] asynchronously.
    pub async fn write_schemas_async(&self, batch: SchemaBatch) -> anyhow::Result<()> {
        tokio::task::block_in_place(|| {
            with_error_logging(|| self.write_schemas_inner(batch), "write_schemas_async")
        })
    }

    fn get_cf_handle(&self, cf_name: &str) -> anyhow::Result<&rocksdb::ColumnFamily> {
        with_error_logging(
            || {
                self.db.cf_handle(cf_name).ok_or_else(|| {
                    format_err!("DB::cf_handle not found for column family name: {cf_name}",)
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
        // This has no effect on cache state, so it's safe to call without the lock.
        with_error_logging(|| self.db.flush_cf(handle), "flush_cf")
    }

    /// Trigger compaction. Primarily used for testing.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn trigger_compaction<S: Schema>(&self) -> anyhow::Result<()> {
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        // This has no effect on cache state, so it's safe to call without the lock.
        self.db
            .compact_range_cf::<&[u8], &[u8]>(&cf_handle, None, None);
        Ok(())
    }

    /// Returns the current RocksDB property value for the provided column family name
    /// and property name.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn get_property(&self, cf_name: &str, property_name: &str) -> anyhow::Result<u64> {
        with_error_logging(
            || {
                // This has no effect on cache state, so it's safe to call without the lock.
                self.db
                    .property_int_value_cf(self.get_cf_handle(cf_name)?, property_name)?
                    .ok_or_else(|| {
                        format_err!(
                            "Unable to get property \"{property_name}\" of  column family \"{cf_name}\".",
                        )
                    })
            },
            "get_property",
        )
    }

    /// Creates new physical DB checkpoint in directory specified by `path`.
    #[tracing::instrument(skip_all, level = "error")]
    pub fn create_checkpoint<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        // This has no effect on cache state, so it's safe to call without the lock.
        with_error_logging(
            || rocksdb::checkpoint::Checkpoint::new(&self.db)?.create_checkpoint(path),
            "create_checkpoint",
        )
    }

    /// Creates new physical DB checkpoint in directory specified by `path` asynchronously.
    #[tracing::instrument(skip_all, level = "error")]
    pub async fn create_checkpoint_async<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        tokio::task::block_in_place(|| self.create_checkpoint(path))
    }

    /// Sets the cache size for the DB.
    #[cfg(feature = "test-utils")]
    pub fn set_cache_size(&self, estimated_size: usize, weight_capacity: u64) {
        *self.cache.write() = Cache::with_weighter(estimated_size, weight_capacity, BasicWeighter);
    }

    /// Returns the number of cache hits.
    #[cfg(feature = "test-utils")]
    pub fn cache_hits(&self) -> u64 {
        self.cache.read().hits()
    }

    /// Returns the number of cache misses.
    #[cfg(feature = "test-utils")]
    pub fn cache_misses(&self) -> u64 {
        self.cache.read().misses()
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
pub enum Operation<K = SchemaKey, V = SchemaValue> {
    /// Writing a value to the DB.
    Put {
        /// Value to write
        value: V,
    },
    /// Deleting a value
    Delete,
    /// Deleting a range of values
    DeleteRange {
        /// Start of the range to delete
        from: K,
        /// End of the range to delete
        to: K,
    },
}

impl<K, V: AsRef<[u8]>> Operation<K, V> {
    /// Returns [`S::Value`] if the operation is [`Operation::Put`] and `None` if [`Operation::Delete`].
    fn decode_value<S: Schema>(&self) -> anyhow::Result<Option<S::Value>> {
        match self {
            Operation::Put { value } => {
                let value = S::Value::decode_value(value.as_ref())?;
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

        let db_debug = format!("{db:?}");
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
