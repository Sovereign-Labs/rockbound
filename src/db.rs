use std::path::Path;

use crate::iterator::ScanDirection;
use crate::iterator::{SchemaIterator, SeekKeyEncoder};
use crate::metrics::{
    SCHEMADB_BATCH_COMMIT_BYTES, SCHEMADB_BATCH_COMMIT_LATENCY_SECONDS, SCHEMADB_DELETES,
    SCHEMADB_GET_BYTES, SCHEMADB_GET_LATENCY_SECONDS, SCHEMADB_PUT_BYTES,
};
use crate::{default_write_options, is_range_bounds_inverse, Operation, SchemaKey};
use anyhow::format_err;
use rocksdb::ReadOptions;
use tracing::info;

use crate::iterator::RawDbIter;
use crate::schema::Schema;
use crate::schema::{ColumnFamilyName, KeyCodec, ValueCodec};
use crate::schema_batch::SchemaBatch;

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

    /// Open RocksDB with the provided column family descriptors.
    /// This allows to configure options for each column family.
    pub fn open_with_cfds(
        db_opts: &rocksdb::Options,
        path: impl AsRef<Path>,
        name: &'static str,
        cfds: impl IntoIterator<Item = rocksdb::ColumnFamilyDescriptor>,
    ) -> anyhow::Result<DB> {
        let inner = rocksdb::DB::open_cf_descriptors(db_opts, path, cfds)?;
        Ok(Self::log_construct(name, inner))
    }

    /// Open db in readonly mode. This db is completely static, so any writes that occur on the primary
    /// after it has been opened will not be visible to the readonly instance.
    pub fn open_cf_readonly(
        opts: &rocksdb::Options,
        path: impl AsRef<Path>,
        name: &'static str,
        cfs: Vec<ColumnFamilyName>,
    ) -> anyhow::Result<DB> {
        let error_if_log_file_exists = false;
        let inner = rocksdb::DB::open_cf_for_read_only(opts, path, cfs, error_if_log_file_exists)?;

        Ok(Self::log_construct(name, inner))
    }

    /// Open db in secondary mode. A secondary db is does not support writes, but can be dynamically caught up
    /// to the primary instance by a manual call. See <https://github.com/facebook/rocksdb/wiki/Read-only-and-Secondary-instances>
    /// for more details.
    pub fn open_cf_as_secondary<P: AsRef<Path>>(
        opts: &rocksdb::Options,
        primary_path: P,
        secondary_path: P,
        name: &'static str,
        cfs: Vec<ColumnFamilyName>,
    ) -> anyhow::Result<DB> {
        let inner = rocksdb::DB::open_cf_as_secondary(opts, primary_path, secondary_path, cfs)?;
        Ok(Self::log_construct(name, inner))
    }

    fn log_construct(name: &'static str, inner: rocksdb::DB) -> DB {
        info!(rocksdb_name = name, "Opened RocksDB");
        DB { name, inner }
    }

    /// Reads single record by key.
    pub fn get<S: Schema>(
        &self,
        schema_key: &impl KeyCodec<S>,
    ) -> anyhow::Result<Option<S::Value>> {
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
    }

    /// Reads a single record by key asynchronously.
    pub async fn get_async<S: Schema>(
        &self,
        schema_key: &impl KeyCodec<S>,
    ) -> anyhow::Result<Option<S::Value>> {
        tokio::task::block_in_place(|| self.get(schema_key))
    }

    /// Writes single record.
    pub fn put<S: Schema>(
        &self,
        key: &impl KeyCodec<S>,
        value: &impl ValueCodec<S>,
    ) -> anyhow::Result<()> {
        // Not necessary to use a batch, but we'd like a central place to bump counters.
        // Used in tests only anyway.
        let mut batch = SchemaBatch::new();
        batch.put::<S>(key, value)?;
        self.write_schemas(&batch)
    }

    /// Writes a single record asynchronously.
    pub async fn put_async<S: Schema>(
        &self,
        key: &impl KeyCodec<S>,
        value: &impl ValueCodec<S>,
    ) -> anyhow::Result<()> {
        tokio::task::block_in_place(|| self.put(key, value))
    }

    /// Delete a single key from the database.
    pub fn delete<S: Schema>(&self, key: &impl KeyCodec<S>) -> anyhow::Result<()> {
        // Not necessary to use a batch, but we'd like a central place to bump counters.
        // Used in tests only anyway.
        let mut batch = SchemaBatch::new();
        batch.delete::<S>(key)?;
        self.write_schemas(&batch)
    }

    /// Delete a single key from the database asynchronously.
    pub async fn delete_async<S: Schema>(&self, key: &impl KeyCodec<S>) -> anyhow::Result<()> {
        tokio::task::block_in_place(|| self.delete(key))
    }

    /// Removes the database entries in the range `["from", "to")` using default write options.
    ///
    /// Note that this operation will be done lexicographic on the *encoding* of the seek keys. It is
    /// up to the table creator to ensure that the lexicographic ordering of the encoded seek keys matches the
    /// logical ordering of the type.
    pub fn delete_range<S: Schema>(
        &self,
        from: &impl SeekKeyEncoder<S>,
        to: &impl SeekKeyEncoder<S>,
    ) -> anyhow::Result<()> {
        let cf_handle = self.get_cf_handle(S::COLUMN_FAMILY_NAME)?;
        let from = from.encode_seek_key()?;
        let to = to.encode_seek_key()?;
        self.inner.delete_range_cf(cf_handle, from, to)?;
        Ok(())
    }

    /// Removes the database entries in the range `["from", "to")` using default write options asynchronously.
    ///
    /// Note that this operation will be done lexicographic on the *encoding* of the seek keys. It is
    /// up to the table creator to ensure that the lexicographic ordering of the encoded seek keys matches the
    /// logical ordering of the type.
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
    pub fn iter<S: Schema>(&self) -> anyhow::Result<SchemaIterator<S>> {
        self.iter_with_direction::<S>(Default::default(), ScanDirection::Forward)
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
            anyhow::bail!("lower_bound > upper_bound");
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

    /// Writes a group of records wrapped in a [`SchemaBatch`].
    pub fn write_schemas(&self, batch: &SchemaBatch) -> anyhow::Result<()> {
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
                }
            }
        }
        let serialized_size = db_batch.size_in_bytes();

        self.inner.write_opt(db_batch, &default_write_options())?;

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
                }
            }
        }
        SCHEMADB_BATCH_COMMIT_BYTES
            .with_label_values(&[self.name])
            .observe(serialized_size as f64);

        Ok(())
    }

    /// Writes a group of records wrapped in a [`SchemaBatch`] asynchronously.
    pub async fn write_schemas_async(&self, batch: &SchemaBatch) -> anyhow::Result<()> {
        tokio::task::block_in_place(|| self.write_schemas(batch))
    }

    fn get_cf_handle(&self, cf_name: &str) -> anyhow::Result<&rocksdb::ColumnFamily> {
        self.inner.cf_handle(cf_name).ok_or_else(|| {
            format_err!(
                "DB::cf_handle not found for column family name: {}",
                cf_name
            )
        })
    }

    /// Flushes [MemTable](https://github.com/facebook/rocksdb/wiki/MemTable) data.
    /// This is only used for testing `get_approximate_sizes_cf` in unit tests.
    pub fn flush_cf(&self, cf_name: &str) -> anyhow::Result<()> {
        Ok(self.inner.flush_cf(self.get_cf_handle(cf_name)?)?)
    }

    /// Returns the current RocksDB property value for the provided column family name
    /// and property name.
    pub fn get_property(&self, cf_name: &str, property_name: &str) -> anyhow::Result<u64> {
        self.inner
            .property_int_value_cf(self.get_cf_handle(cf_name)?, property_name)?
            .ok_or_else(|| {
                format_err!(
                    "Unable to get property \"{}\" of  column family \"{}\".",
                    property_name,
                    cf_name,
                )
            })
    }

    /// Creates new physical DB checkpoint in directory specified by `path`.
    pub fn create_checkpoint<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        rocksdb::checkpoint::Checkpoint::new(&self.inner)?.create_checkpoint(path)?;
        Ok(())
    }

    /// Creates new physical DB checkpoint in directory specified by `path` asynchronously.
    pub async fn create_checkpoint_async<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        tokio::task::block_in_place(|| self.create_checkpoint(path))
    }
}
