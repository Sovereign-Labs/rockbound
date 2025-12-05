use std::{
    collections::{btree_map, BTreeMap}, hash::Hash, iter::Peekable, marker::PhantomData, sync::{atomic::{AtomicU64, Ordering}, Arc}
};

use anyhow::bail;
use parking_lot::{RwLock, RwLockReadGuard};
use quick_cache::sync::Cache;
use rocksdb::ColumnFamilyDescriptor;

use crate::{
    default_cf_descriptor, default_write_options, iterator::{RawDbIter, ScanDirection}, metrics::{SCHEMADB_BATCH_COMMIT_BYTES, SCHEMADB_BATCH_COMMIT_LATENCY_SECONDS, SCHEMADB_DELETES, SCHEMADB_PUT_BYTES}, schema::{ColumnFamilyName, KeyCodec, KeyDecoder, KeyEncoder, ValueCodec}, with_error_logging, BasicWeighter, CodecError, DbCache, Schema, SchemaBatch, DB
};
#[derive(Debug, Default)]
pub(crate) struct VersionMetadata;


impl Schema for VersionMetadata {
    const COLUMN_FAMILY_NAME: ColumnFamilyName = "version_metadata";
    const SHOULD_CACHE: bool = false;

    type Key = VersionedTableMetadataKey;
    type Value = u64;
}

/// A singleton key. Encodes to the empty vec.
#[derive(Debug, PartialEq, Clone, Ord, PartialOrd, Eq, Copy)]
pub enum VersionedTableMetadataKey {
    /// The latest version that has been committed.
    CommittedVersion,
    /// The newest version that has been pruned.
    PrunedVersion,
}

const COMMITTED_VERSION: [u8; 1] = [0];
const PRUNED_VERSION: [u8; 1] = [1];

impl VersionedTableMetadataKey {
    /// Converts the key into a byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            VersionedTableMetadataKey::CommittedVersion => &COMMITTED_VERSION,
            VersionedTableMetadataKey::PrunedVersion => &PRUNED_VERSION,
        }
    }

    /// Encodes the key into a byte vector.
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(match self {
            VersionedTableMetadataKey::CommittedVersion => COMMITTED_VERSION.to_vec(),
            VersionedTableMetadataKey::PrunedVersion => PRUNED_VERSION.to_vec(),
        })
    }

    /// Decodes the key from a byte vector.
    pub fn decode(data: &[u8]) -> Result<Self, CodecError> {
        if data.len() != 1 {
            return Err(CodecError::InvalidKeyLength {
                expected: 1,
                got: data.len(),
            });
        }
        Ok(match data[0] {
            val if val == COMMITTED_VERSION[0] => VersionedTableMetadataKey::CommittedVersion,
           val if val == PRUNED_VERSION[0] => VersionedTableMetadataKey::PrunedVersion,
            _ => {
                return Err(CodecError::Wrapped(anyhow::anyhow!(
                    "Invalid versioned table metadata key: {}",
                    data[0]
                )))
            }
        })
    }
}

impl AsRef<VersionedTableMetadataKey> for VersionedTableMetadataKey {
    fn as_ref(&self) -> &VersionedTableMetadataKey {
        self
    }
}

impl KeyEncoder<VersionMetadata> for VersionedTableMetadataKey {
    fn encode_key(&self) -> Result<Vec<u8>, CodecError> {
        self.encode()
    }
}

impl KeyDecoder<VersionMetadata> for VersionedTableMetadataKey {
    fn decode_key(data: &[u8]) -> Result<Self, CodecError> {
        Self::decode(data)
    }
}

impl ValueCodec<VersionMetadata> for u64 {
    fn encode_value(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.to_be_bytes().to_vec())
    }

    fn decode_value(data: &[u8]) -> Result<Self, CodecError> {
        Ok(u64::from_be_bytes(data.try_into().map_err(|_| {
            CodecError::InvalidKeyLength {
                expected: 8,
                got: data.len(),
            }
        })?))
    }
}

/// A versioned DB is a DB that stores data in a versioned column family.
///
/// Suppose the caller wants a versioned map from SlotKey to SlotValue. The implementation will generate several tables:
/// - A plain column family mapping SlotKey to SlotValue (the "live" column family). This can be cached.
/// - A historical column family mapping (SlotKey, Version) to SlotValue (the "historical" column family). Queries with an associated version retrieve the latest entry for that key whose value is less than or equal to the version.
///   Note that: pruning must leave at least one entry for each live key indicating at which version it was written.
/// - A pruning column family mapping Version => Vec<Key> telling us which entries were updated at each version
///
///
/// On each write, the implementation will:
/// - Put/delete the value in the live column family and its cache. (Note that range deletes are not yet supported.)
/// - Collect the keys and write them into the pruning column family
/// - (Open Question): Do we also duplicate the k/v pairs into the historical column family? or do we migrate them over to the historical column family?
///   - Suggested answer: We duplicate the keys into the other CF right away. This simplifies pruning and historical queries at the cost of maintaining two copies of the live data. We can have a config to disable the historical archive altogether, which should reduce space usage.
///
/// The alternative would be to use a more complicated multi-step commit where we...
/// - Read the current values of any keys to be modified from the live column family
/// - Modify the keys in the live column family
/// - Write the old values to the historical column family
///
/// One key problem with this approach is that we don't keep the record of when the column was written.
///
///
/// On each read, the implementation will:
/// - Read from the live column family
#[derive(Debug)]
pub struct VersionedDB<S: SchemaWithVersion> {
    /// Holds the live state and the committed versions metadata.
    ///
    /// # Warning
    /// This database *must* be written last so that we're in a consistent state on crash.
    /// When we recover, we'll read that version N-1 was the last committed version even if version N has been written
    /// to the archival DB - but that's fine because we'll reprocess block N and derive the same write set for archival state
    /// since the live_db is still correct. Conversely, if we were to commit archival state last then there would be no way to
    /// correctly re-execute the previous block..
    live_db: Arc<DB>,
    /// Holds *only* the archival and pruning data, inluding pruned version metadata.
    archival_db: Arc<DB>,
    cache: RwLock<Cache<S::Key, Option<S::Value>, BasicWeighter>>,
    // The number of the *lowest* version that is guaranteed to be available in the live db. We increment this value when we *start* committing to the database,
    // so the cache may not be valid for versions below this value.
    committed_archival_version: AtomicU64,
    _schema: S,
}

fn decode_version_metadata_value(data: &[u8]) -> Result<u64, CodecError> {
    Ok(u64::from_be_bytes(data.try_into().map_err(|_| {
        CodecError::InvalidKeyLength {
            expected: 8,
            got: data.len(),
        }
    })?))
}

impl<S: SchemaWithVersion> VersionedDB<S>
// This where clause shouldn't be needed since it's implied by the Schema trait, but Rust intentionally doesn't elaborate these bounds.
{
    /// Returns the oldest version that is available in the database.
    pub fn get_pruned_version(&self) -> anyhow::Result<Option<u64>> {
        self.archival_db
            .get_raw_with_cf_and_decoder::<u64>(S::VERSION_METADATA_COLUMN_FAMILY_NAME, VersionedTableMetadataKey::PrunedVersion.as_bytes(), &decode_version_metadata_value)
    }
    /// Returns the latest committed version in the database.
    pub fn get_committed_version(&self) -> anyhow::Result<Option<u64>> {
        self.live_db
            .get_raw_with_cf_and_decoder::<u64>(S::VERSION_METADATA_COLUMN_FAMILY_NAME, VersionedTableMetadataKey::CommittedVersion.as_bytes(), &decode_version_metadata_value)
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

impl<S: SchemaWithVersion> VersionedDB<S> {
    /// Returns a reference to the live database.
    pub fn live_db(&self) -> &DB {
        &self.live_db
    }
    /// Returns a reference to the archival database.
    pub fn archival_db(&self) -> &DB {
        &self.archival_db
    }
}

// #[derive(Debug)]
// /// A key suffixed with a version number
// pub struct VersionedKey<S: SchemaWithVersion, T>(pub T, pub u64, PhantomData<S>);
// impl<S: SchemaWithVersion, T> VersionedKey<S, T> {
//     /// Creates a new versioned key.
//     pub fn new(key: T, version: u64) -> Self {
//         Self(key, version, PhantomData)
//     }
// }

// impl<S: SchemaWithVersion, T: KeyEncoder<S>> KeyEncoder<S::HistoricalColumnFamily>
//     for VersionedKey<S, T>
// {
//     fn encode_key(&self) -> Result<Vec<u8>, CodecError> {
//         let mut key = self.0.encode_key()?;
//         key.extend_from_slice(&self.1.to_be_bytes());
//         Ok(key)
//     }
// }

// #[derive(Debug)]
// /// A key suffixed with a version number
// pub struct EncodedVersionedKey<S: SchemaWithVersion>(pub Vec<u8>, pub u64, PhantomData<S>);
// impl<S: SchemaWithVersion> EncodedVersionedKey<S> {
//     /// Creates a new versioned key.
//     pub fn new(key: Vec<u8>, version: u64) -> Self {
//         Self(key, version, PhantomData)
//     }
// }

// impl<S: SchemaWithVersion> KeyEncoder<S::HistoricalColumnFamily> for EncodedVersionedKey<S> {
//     fn encode_key(&self) -> Result<Vec<u8>, CodecError> {
//         let mut key = self.0.clone();
//         key.extend_from_slice(&self.1.to_be_bytes());
//         Ok(key)
//     }
// }

// impl<S: SchemaWithVersion> KeyDecoder<S::HistoricalColumnFamily> for VersionedKey<S, S::Key> {
//     fn decode_key(data: &[u8]) -> Result<Self, CodecError> {
//         let len = data.len();
//         let key_len = len.checked_sub(8).ok_or(CodecError::InvalidKeyLength {
//             expected: 8,
//             got: len,
//         })?;
//         let key = S::Key::decode_key(&data[..key_len])?;
//         let version =
//             u64::from_be_bytes(data[key_len..].try_into().expect(
//                 "key length was just checked to be 8 bytes but no longer is. This is a bug.",
//             ));
//         Ok(Self::new(key, version))
//     }
// }

/// A key *prefixed* with a version number for easy iteration by version.
#[derive(Debug)]
pub struct PrunableKey<S: SchemaWithVersion>(
    #[allow(unused)] // This is used internally by rocksdb for ordering - we just never look at it in rust code.
    u64, S::Key, PhantomData<S>);
impl<S: SchemaWithVersion> PrunableKey<S> {
    /// Creates a new prunable key.
    pub fn new(version: u64, key: S::Key) -> Self {
        Self(version, key, PhantomData)
    }
}

impl<S: SchemaWithVersion> PrunableKey<S> {
    fn decode_key(data: &[u8]) -> Result<Self, CodecError> {
        let len = data.len();
        if len < 8 {
            return Err(CodecError::InvalidKeyLength {
                expected: 8,
                got: len,
            });
        }
        let key = S::Key::decode_key(&data[8..])?;
        let version =
            u64::from_be_bytes(data[0..8].try_into().expect(
                "key length was just checked to be 8 bytes but no longer is. This is a bug.",
            ));
        Ok(Self::new(version, key))
    }
}

// impl<S: SchemaWithVersion, T: KeyEncoder<S>> KeyEncoder<S::PruningColumnFamily>
//     for PrunableKey<S, T>
// {
//     fn encode_key(&self) -> Result<Vec<u8>, CodecError> {
//         let mut key = self.0.to_be_bytes().to_vec();
//         key.extend_from_slice(&self.1.encode_key()?);
//         Ok(key)
//     }
// }

// /// An encoded key *prefixed* with a version number for easy iteration by version.
// #[derive(Debug)]
// pub struct EncodedPrunableKey<S: SchemaWithVersion>(Vec<u8>, u64, PhantomData<S>);
// impl<S: SchemaWithVersion> EncodedPrunableKey<S> {
//     /// Creates a new prunable key.
//     pub fn new(version: u64, key: Vec<u8>) -> Self {
//         Self(key, version, PhantomData)
//     }
// }

// impl<S: SchemaWithVersion> KeyEncoder<S::PruningColumnFamily> for EncodedPrunableKey<S> {
//     fn encode_key(&self) -> Result<Vec<u8>, CodecError> {
//         let mut key = self.0.clone();
//         key.extend_from_slice(&self.1.to_be_bytes());
//         Ok(key)
//     }
// }

// impl<S: SchemaWithVersion, T> PrunableKey<S, T> {
//     /// Converts a prunable key into a versioned key.
//     pub fn into_versioned_key(self) -> VersionedKey<S, T> {
//         VersionedKey::new(self.1, self.0)
//     }
// }

#[derive(Debug, Clone, Default)]
/// A batch of writes to a versioned schema.
pub struct VersionedSchemaBatch<S: Schema> {
    versioned_table_writes: BTreeMap<S::Key, Option<S::Value>>,
}

impl<S: Schema, T: IntoIterator<Item = (S::Key, Option<S::Value>)>> From<T>
    for VersionedSchemaBatch<S>
where S::Key: Ord
{
    fn from(iter: T) -> Self {
        Self {
            versioned_table_writes: iter.into_iter().collect(),
        }
    }
}

impl<S: Schema> VersionedSchemaBatch<S> 
where S::Key: Ord
{
    /// Puts a key-value pair into the batch.
    pub fn put_versioned(&mut self, key: S::Key, value: S::Value) {
        self.versioned_table_writes.insert(key, Some(value));
    }

    /// Deletes a key from the batch.
    pub fn delete_versioned(&mut self, key: S::Key) {
        self.versioned_table_writes
            .insert(key, None);
    }
}

fn live_versioned_column_family_descriptor(name: &str) -> ColumnFamilyDescriptor {
    let mut cf_opts: rocksdb::Options = rocksdb::Options::default();
    cf_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    // Use a 1GB block cache. TODO: Tune this value
    cf_opts.optimize_for_point_lookup(1024);
    rocksdb::ColumnFamilyDescriptor::new(name, cf_opts)
}

/// A marker trait showing that a type **HAS NOOP SERIALIZATION** implements `Clone` and `AsRef<[u8]>`, is cheaply cloneable.
/// No-op serialization means that key.encode_key() is the same as key.as_ref().to_vec()
pub trait VersionedSchemaKeyMarker: Clone + Ord + Hash  + AsRef<[u8]> {}
// impl VersionedSchemaKeyMarker for std::sync::Arc<Vec<u8>> {}


/// A specialized schema for values which have one "live" version and wish to automatically store a
/// (possibly truncated) history of all versions over time.
pub trait SchemaWithVersion: Clone + Schema<Key: VersionedSchemaKeyMarker, Value: Clone + AsRef<[u8]>> {
    /// The name of the column family for storing the historical values of the schema.
    const HISTORICAL_COLUMN_FAMILY_NAME: ColumnFamilyName;
    /// The name of the column family for storing the pruning keys of the schema.
    const PRUNING_COLUMN_FAMILY_NAME: ColumnFamilyName;
    /// The name of the column family for storing the version metadata of the schema.
    const VERSION_METADATA_COLUMN_FAMILY_NAME: ColumnFamilyName;
}

// /// A key for a versioned schema.
// #[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
// pub enum VersionedSchemaKey<V: SchemaWithVersion> {
//     /// A key that is currently live
//     Live(V::Key),
//     /// A key was written at a given version
//     Historical(V::Key, u64),
//     /// A key was written at a given version and is to be pruned
//     Pruning(V::Key, u64),
//     /// Metadata about the versioned schema
//     VersionMetadata(VersionedTableMetadataKey),
// }

/// A value for a versioned schema.
#[derive(Debug, Clone)]
pub enum VersionedValue<V: SchemaWithVersion> {
    /// A value that is currently live
    Live(V::Value),
    /// A value that was written at a given version
    Historical(V::Value),
    /// A key was written at a given version and is to be pruned
    Pruning,
    /// Metadata about the versioned schema
    VersionMetadata(u64),
}

struct KeyWithVersionPrefixAndSuffix{ version: u64, contents: Vec<u8> }

impl KeyWithVersionPrefixAndSuffix {
    pub fn new(version: u64) -> Self {
        Self { version, contents: version.to_be_bytes().to_vec() }
    }

    pub fn set_key(&mut self, key: &[u8]) {
        self.contents.truncate(8);
        self.contents.extend_from_slice(key);
        self.contents.extend_from_slice(&self.version.to_be_bytes());
    }

    pub fn live_key(&self) -> &[u8] {
        let len = self.contents.len();
        &self.contents[8..len - 8]
    }

    pub fn archival_key(&self) -> &[u8] {
        let len = self.contents.len();
        &self.contents[8..len]
    }

    pub fn pruning_key(&self) -> &[u8] {
        let len = self.contents.len();
        &self.contents[0..len-8]
    }
}


impl<V: SchemaWithVersion> VersionedDB<V>
where
    // VersionedTableMetadataKey: KeyCodec<V::VersionMetadatacolumn>,
    V::Key: Ord + Clone + std::hash::Hash + AsRef<[u8]>,
    V::Value: Clone + AsRef<[u8]>,
    V: Ord,
{
    /// Adds the column families for the versioned schema to the existing column families.
    pub fn add_column_families(
        existing_column_families: &mut Vec<ColumnFamilyDescriptor>,
        separate_archival_and_pruning: bool,
    ) -> anyhow::Result<()> {
        let historical_versioned_column_family = V::HISTORICAL_COLUMN_FAMILY_NAME;
        let pruning_column_family = V::PRUNING_COLUMN_FAMILY_NAME;
        let live_column_family = V::COLUMN_FAMILY_NAME;
        let metadata_column_family = V::VERSION_METADATA_COLUMN_FAMILY_NAME;
        for column in existing_column_families.iter() {
            if column.name() == metadata_column_family
                || column.name() == historical_versioned_column_family
                || column.name() == live_column_family
                || column.name() == pruning_column_family
            {
                bail!("{} column name is reserved for internal use", column.name());
            }
        }

        if !separate_archival_and_pruning {
            existing_column_families
                .push(default_cf_descriptor(historical_versioned_column_family));
            existing_column_families.push(default_cf_descriptor(pruning_column_family));
        }
        existing_column_families.push(live_versioned_column_family_descriptor(live_column_family));
        existing_column_families.push(default_cf_descriptor(metadata_column_family));
        Ok(())
    }

    /// Creates a new versioned DB from an existing DB.
    // Cache size estimation: we want to allocate about 1GB for cache. Estimate that slot keys are about 80 bytes and slot values
    // are around 400 bytes + 56 bytes of overhead on the stack (see weighter). Multiply by 1.5 to account for overhead (per quick-cache docs).
    // That gives estimated item capacity of 1GB / (80 + 400 + 56) * 1.5 ~= 1.2M.
    pub fn from_dbs(live_db: Arc<DB>, archival_db: Arc<DB>, cache_size: usize) -> anyhow::Result<Self> {
        let cache = RwLock::new(Cache::with_weighter(
            cache_size / (80 + 400 + 56),
            cache_size as u64,
            BasicWeighter,
        ));
        let committed_archival_version = AtomicU64::new(u64::MAX);
        Ok(Self {
            live_db,
            archival_db,
            cache,
            committed_archival_version,
            _schema: Default::default(),
        })
    }

    /// Returns an iterator over the pruning keys up to a given version.
    pub fn iter_pruning_keys_up_to_version(
        &self,
        version: u64,
    ) -> anyhow::Result<
        impl Iterator<Item = PrunableKey<V>> + use<'_, V>,
    > {
        // Because some keys are longer than 8 bytes, we use an exclusive range
        let first_version_to_keep = version.saturating_add(1);
        let range = ..first_version_to_keep.to_be_bytes().to_vec();
        Ok(self
            .archival_db
            .raw_iter_range_with_decode_fn::<V, PrunableKey<V>>(range, ScanDirection::Forward, &|(key, _value)| PrunableKey::decode_key(key).expect("DB Corruption: Failed to decode pruning key"))?)
    }

    /// Name of the database that can be used for logging or metrics or tracing.
    #[inline]
    pub fn name(&self) -> &'static str {
        self.live_db.name()
    }

    /// Returns the value of a key in the live column family.
    pub fn get_live_value(&self, key: &V::Key) -> anyhow::Result<Option<V::Value>> {
        let lock  = self.cache.try_read();
        if let Some(cache) = lock.as_ref() {
            let cache_result =
                cache.get(key);
            if let Some(result) = cache_result {
                return Ok(result);
            }
        }
        let result = self.live_db.get_raw::<V>(key.as_ref())?;
        // We can safely modify the cache while holding a read lock because we always hold a *write* lock while committing to the DB. 
        // Since we're currently holding a read lock, that means that the DB is not currently changing - so the value we just read cannot
        // be invalidated by a concurrent write. This makes it safe to insert.
        // 
        // Note that this analysis only holds because we've held the lock since the beginning of `get_live_value`. If we were to drop
        // the lock while we read from disk, this would not be sound.
        if let Some(cache) = lock {
            cache.insert(key.clone(), result.clone());
        }
        Ok(result)
    }

    /// Returns the latest committed version in the database from an atomic.
    pub fn fetch_latest_committed_archival_version(&self) -> u64 {
        self.committed_archival_version.load(Ordering::Acquire)
    }

    // TODO: Enable pruning!

    /// foo
    pub fn commit(&self, batch: &VersionedSchemaBatch<V>, version: u64) -> anyhow::Result<()> 
    where V::Value: AsRef<[u8]>, V::Key: AsRef<[u8]>,{
        let _timer = SCHEMADB_BATCH_COMMIT_LATENCY_SECONDS
            .with_label_values(&[self.live_db.name()])
            .start_timer();
        // Update the next version to commit if relevant.
        // Block any readers while the DB isn't fully consistent
        let cache = self.cache.write();
        // Optimization:
        // At various times we need the key *prefixed* with the version (for pruning), on its own (for live reads), and suffixed with the version (for archival reads).
        // To reduce allocations and memcopies, we create a single vector [version || key || version] and use it for all three cases, simply slicing the relevant subset.
        //
        // As a further optimization, we reuse a single `Vec` for all keys.

        let mut key_with_version = KeyWithVersionPrefixAndSuffix::new(version);

        // Create batches and column families
        let mut live_db_batch = rocksdb::WriteBatch::default();
        let mut archival_db_batch = rocksdb::WriteBatch::default();
        let live_cf_handle = self.live_db.get_cf_handle(V::COLUMN_FAMILY_NAME)?;
        let archival_cf_handle = self.archival_db.get_cf_handle(V::HISTORICAL_COLUMN_FAMILY_NAME)?;
        let pruning_cf_handle = self.archival_db.get_cf_handle(V::PRUNING_COLUMN_FAMILY_NAME)?;
        let metadata_cf_handle = self.live_db.get_cf_handle(V::VERSION_METADATA_COLUMN_FAMILY_NAME)?;

        //  Keys for metrics
        let mut live_put_bytes = 0;
        let mut deletes = 0;
        let mut archival_puts_bytes = 0;
        let mut pruning_puts_bytes = 0;

        // TODO: Assert version consistency before writing
        // FIXME: Handle caching
        for (key, value) in batch.versioned_table_writes.iter() {
            key_with_version.set_key(key.as_ref());
            println!("Writing live key with version: {}. {:?}", version, key.as_ref());
            match value {
                Some(value) => {
                    assert!(!key_with_version.live_key().is_empty(), "Live values may not have zero-length. This prevents confusion with placholders for deleted values.");
                    println!("Inserting live key: {:?}", key_with_version.live_key());
                    live_put_bytes += key_with_version.live_key().len() + value.as_ref().len();
                    live_db_batch.put_cf(live_cf_handle, key_with_version.live_key(), &value);
                    archival_puts_bytes += key_with_version.archival_key().len() + value.as_ref().len();
                    archival_db_batch.put_cf(archival_cf_handle, key_with_version.archival_key(), &value);
                    cache.insert(key.clone(), Some(value.clone()));
                }
                None => {
                    println!("Deleting live key: {:?}", key_with_version.live_key());
                    deletes +=1;
                    archival_puts_bytes += key_with_version.archival_key().len();
                    live_db_batch.delete_cf(live_cf_handle, &key);
                    archival_db_batch.put_cf(archival_cf_handle, key_with_version.archival_key(), &[]);
                    cache.insert(key.clone(), None);
                }
            }
            archival_db_batch.put_cf(pruning_cf_handle, key_with_version.pruning_key(), &[]);
            pruning_puts_bytes += key_with_version.pruning_key().len();
        }
        live_db_batch.put_cf(metadata_cf_handle, VersionedTableMetadataKey::CommittedVersion.as_bytes(), &version.to_be_bytes());
       
        let serialized_size = live_db_batch.size_in_bytes() + archival_db_batch.size_in_bytes();
        let archival_serialized_size = archival_db_batch.size_in_bytes();
        with_error_logging(
            || self.archival_db.write_opt(archival_db_batch, &default_write_options()),
            "write_versioned_schemas::write_archival_opt",
        )?;

        // Danger: This function is coupled with `get_historical_value`. That function assumes that if hte 
        self.committed_archival_version.store(version, Ordering::Release);

        with_error_logging(
            || self.live_db.write_opt(live_db_batch, &default_write_options()),
            "write_versioned_schemas::write_live_opt",
        )?;
        // Drop the write lock on the cache.
        drop(cache);


        // Track metrics after the writes succeed
        {
            SCHEMADB_PUT_BYTES
                .with_label_values(&[V::COLUMN_FAMILY_NAME])
                .observe(live_put_bytes as f64);
            SCHEMADB_DELETES
                .with_label_values(&[V::COLUMN_FAMILY_NAME])
                .inc_by(deletes);
            SCHEMADB_PUT_BYTES
                .with_label_values(&[V::HISTORICAL_COLUMN_FAMILY_NAME])
                .observe(archival_puts_bytes as f64);
            SCHEMADB_PUT_BYTES
                .with_label_values(&[V::PRUNING_COLUMN_FAMILY_NAME])
                .observe(pruning_puts_bytes as f64);
            
            SCHEMADB_BATCH_COMMIT_BYTES
                .with_label_values(&[self.live_db.name()])
                .observe(serialized_size as f64);
            SCHEMADB_BATCH_COMMIT_BYTES
                .with_label_values(&[self.archival_db.name()])
                .observe(archival_serialized_size as f64);
        }
        


        Ok(())
    }

    /// Returns the value of a key in the historical column family as of the given version.
    pub fn get_historical_value(
        &self,
        key_to_get: &impl KeyEncoder<V>,
        version: u64,
    ) -> anyhow::Result<Option<V::Value>> {
        let Some((value_bytes, _version)) =
            self.get_versioned_internal(key_to_get.encode_key()?, version)?
        else {
            return Ok(None);
        };
        if value_bytes.is_empty() {
            return Ok(None);
        }
        let value = V::Value::decode_value(&value_bytes)?;
        Ok(Some(value))
    }

    /// Returns the value of a key in the historical column family as of the given version.
    pub fn get_historical_value_raw(
        &self,
        pre_encoded_key_to_get: Vec<u8>,
        version: u64,
    ) -> anyhow::Result<Option<V::Value>> {
        let Some((value_bytes, _version)) =
            self.get_versioned_internal(pre_encoded_key_to_get, version)?
        else {
            return Ok(None);
        };
        if value_bytes.is_empty() {
            return Ok(None);
        }
        let value = V::Value::decode_value(&value_bytes)?;
        Ok(Some(value))
    }

    /// Returns the latest version at which the given key was written.
    pub fn get_version_for_key(
        &self,
        key_to_get: &impl KeyEncoder<V>,
        max_version: u64,
    ) -> anyhow::Result<Option<u64>> {
        let Some((_, version)) =
            self.get_versioned_internal(key_to_get.encode_key()?, max_version)?
        else {
            return Ok(None);
        };
        Ok(Some(version))
    }

    fn get_versioned_internal(
        &self,
        mut key_to_get: Vec<u8>,
        max_version: u64,
    ) -> anyhow::Result<Option<(Vec<u8>, u64)>> {
        key_to_get.extend_from_slice(&max_version.to_be_bytes());
        let range = ..=&key_to_get;
        // TODO(non-blocking): Make this more efficient eventually
        let mut iterator = self
            .archival_db
            .raw_iter_cf_with_decode_fn::<(Vec<u8>, Vec<u8>)>(V::HISTORICAL_COLUMN_FAMILY_NAME, range, ScanDirection::Backward, &|(key, value)| (key.to_vec(), value.to_vec()))?;
        if let Some((key, value_bytes)) = iterator.next() {
            // Safety: All keys are suffixed with an 8-byte version.
            let (key_bytes, version_bytes) = key.split_at(key.len() - 8);
            if key_bytes.len() != key_to_get.len() - 8
                || key_bytes != &key_to_get[..key_bytes.len()]
            {
                return Ok(None);
            }
            let version = u64::from_be_bytes(
                version_bytes
                    .try_into()
                    .expect("version bytes were 8 bytes but no longer are. This is a bug."),
            );
            debug_assert!(version <= max_version, "Unexpected version. Queried for less than or equal to version {max_version} but got version {version}");
            return Ok(Some((value_bytes, version)));
        }
        Ok(None)
    }
}

#[derive(Debug, Clone)]
/// A reader for a versioned schema.
pub struct VersionedDeltaReader<V: SchemaWithVersion> {
    db: Arc<VersionedDB<V>>,
    // The version of the underlying DB at the time this snapshot was taken.
    base_version: Option<u64>,
    // The snapshots to include in the reader, ordered from newest to oldest.
    snapshots: Vec<Arc<VersionedSchemaBatch<V>>>,
}

impl<V: SchemaWithVersion> VersionedDeltaReader<V>
where
    V::Key: Eq,
    V::Value: Clone,
{
    /// Creates a new versioned delta reader.
    ///
    /// Note that the snapshots are ordered from newest to oldest.
    pub fn new(
        db: Arc<VersionedDB<V>>,
        base_version: Option<u64>,
        snapshots: Vec<Arc<VersionedSchemaBatch<V>>>,
    ) -> Self {
        Self {
            snapshots,
            db,
            base_version,
        }
    }

    /// Returns the latest version that is available in the reader.
    pub fn latest_version(&self) -> Option<u64> {
        match self.base_version {
            Some(base_version) => Some(base_version + self.snapshots.len() as u64),
            None => self.snapshots.len().checked_sub(1).map(|len| len as u64),
        }
    }

    /// Returns the version of the DB at the time this reader was created.
    pub fn base_version(&self) -> Option<u64> {
        self.base_version
    }
}

type MaybePeekableBtreeMapRange<'a, K, V> = Option<Peekable<btree_map::Range<'a, K, V>>>;

/// An iterator over the live key/value pairs in a VersionedDB, including the current snapshots.
///
/// Internally, this struct has to peek at each snapshot and the db to find the next smallest key and returns that result.
pub struct VersionedDbIterator<'a, V: SchemaWithVersion> {
    // We hold a read lock over the db cache to prevent any writes to the table during iteration. If a key was written while we were iterating, we could get inconsistent results:
    //  - Iterator reads:  Key1 -> A,
    //  - Concurrent thread writes:: Key1 -> B, Key2 -> B
    //  - Iterator reads: Key2 -> B,
    // Now the caller sees a state of the world which never existed where Key1 -> A and Key2 -> B.
    _read_lock: RwLockReadGuard<'a, Cache<V::Key, Option<V::Value>, BasicWeighter>>,
    // Borrow the snapshots to keep the lifetimes tied together. We could clone, but this makes it easier to reason about.
    snapshots: Vec<MaybePeekableBtreeMapRange<'a, V::Key, Option<V::Value>>>,
    db_iterator: Option<Peekable<RawDbIter<'a>>>,
    prefix: V::Key,
    encoded_prefix: Vec<u8>,
}

enum LocationOfSmallestKey {
    Snapshot(usize),
    Db,
}

/// A trait for types that can be checked for a prefix.
pub trait HasPrefix<T = Self> {
    /// Checks if the current item has the given prefix.
    fn has_prefix(&self, prefix: &T) -> bool;
}

impl HasPrefix for Vec<u8> {
    fn has_prefix(&self, prefix: &Self) -> bool {
        self.starts_with(prefix)
    }
}

impl HasPrefix for Arc<Vec<u8>> {
    fn has_prefix(&self, prefix: &Self) -> bool {
        self.starts_with(prefix)
    }
}


impl<'a, V: SchemaWithVersion<Key = K>, K> Iterator for VersionedDbIterator<'a, V>
where
    K: Eq + std::hash::Hash + KeyEncoder<V> + KeyDecoder<V> + HasPrefix + Ord,
    V::Value: Clone,
{
    type Item = (K, Option<V::Value>);

    // Return the next key/value pair from the iterator.
    fn next(&mut self) -> Option<Self::Item> {
        // The body of this loop does two passes over the versioned DB contents (including all snapshots and the DB in each pass)
        // first pass: peek each iterator and find the smallest *key* available from any iterator.
        // second pass: advance each iterator whose peeked key is equal to the smallest key, keeping the first (newest) *value* that you find
        //
        // After these two passes, all iterators are consistent and we've saved the most recent value of the next key in lexicographic order into the variable `smallest_key`.
        // *but*, there's a wrinkle. Snapshots save deleted keys as `None`, while deleted keys are simply absent from the DB. To reflect this,
        // we don't want to return the k/v pair if the value is a `None` from a snapshot.
        //
        // To handle this, we simply run the core algorithm in a loop until we find a key/value pair whose value is not `None` and return it.
        loop {
            // Step 1: peek at each snapshot and the Db to find the global smallest key that matches our prefix.
            let mut smallest_key = None;
            // 1.1 Look at each snapshot
            for (idx, snapshot) in self.snapshots.iter_mut().enumerate() {
                if let Some((key, _value)) = snapshot.as_mut().and_then(|s| s.peek()) {
                    // If the next key from this snapsot doesn't match our prefix, ignore it
                    if !key.has_prefix(&self.prefix) {
                        continue;
                    }
                    // If this key is smaller than the current smallest key, update our view of the smallest key and remember which snapshot it came from.
                    if smallest_key
                        .as_ref()
                        .map(|(_smallest_idx, smallest_key)| *key < smallest_key)
                        .unwrap_or(true)
                    {
                        smallest_key = Some((LocationOfSmallestKey::Snapshot(idx), (*key).clone()));
                    }
                }
            }
            // 1.2 Look at the Db, doing the same thing
            let next_from_db = self.db_iterator.as_mut().and_then(|iter| iter.peek());
            let mut is_done_with_db = false;
            if let Some((key, _value)) = next_from_db {
                // If the next key from the Db doesn't match our prefix, ignore it
                if !key.starts_with(&self.encoded_prefix) {
                    is_done_with_db = true;
                // If this key is smaller than the current smallest key, update our view of the smallest key and remember that it came from the Db.
                } else {
                    let key =K::decode_key(key).expect("Failed to decode key from db during iteration. This is a type safety bug.") ;
                    if smallest_key
                    .as_ref()
                    .map(|(_smallest_idx, smallest_key)| key < *smallest_key)
                    .unwrap_or(true)
                    {
                        smallest_key = Some((LocationOfSmallestKey::Db, key));
                    }
                }
            }

            // If we didn't find any keys that matches our prefix, we're done.
            let (smallest_idx, smallest_key) = smallest_key?;
            let smallest_key = smallest_key.clone();

            // Step 2: advance the iterator that corresponds to the smallest key and get they key/value pair to return
            let (key_to_return, value_to_return) = match smallest_idx {
                LocationOfSmallestKey::Snapshot(idx) => {
                    let (raw_key, value )= self.snapshots[idx].as_mut().and_then(|s| s.next()).expect("key went missing from snapshot while iterating. We just checked that `peek` returned a key and `next` failed. This is a bug.");
                    (raw_key,  value.clone())
                }
                LocationOfSmallestKey::Db => {
                    let (_raw_key, value) = self.db_iterator.as_mut().and_then(|iter| iter.next()).expect("key went missing from DB while iterating. We just checked that `peek` returned a key and `next` failed. This is a bug.");
                    (&smallest_key, Some(V::Value::decode_value(&value).expect("Failed to decode value from db during iteration. This is a type safety bug.")))
                }
            };
            if is_done_with_db {
                self.db_iterator = None;
            }

            // Step 3: advance any iterators that are pointing to the same key we just returned. For example, if the newest snapshot contains key A and two of the older snapshots do as well,
            // we want to return the k/v pair from the newest snapshot and advance the other two iterators so that we don't return stale values of `A` on the next call to `next`.
            // Step 3.1: advance any snapshot iterators
            for snapshot in self.snapshots.iter_mut() {
                if let Some((key, _value)) = snapshot.as_mut().and_then(|s| s.peek()) {
                    if *key == &smallest_key {
                        let _ = snapshot.as_mut().and_then(|s| s.next());
                    }
                }
            }
            // Step 3.1 advance the db iterator
            if let Some((key, _value)) = self.db_iterator.as_mut().and_then(|iter| iter.peek()) {
                if K::decode_key(key).expect("Failed to decode key from db during iteration. This is a type safety bug.") == smallest_key {
                    let _ = self.db_iterator.as_mut().and_then(|iter| iter.next());
                }
            }

            if value_to_return.is_some() {
                // Return the value
                return Some((key_to_return.clone(), value_to_return));
            }
        }
    }
}

impl<V: SchemaWithVersion> VersionedDeltaReader<V>
where
    V::Value: Clone + AsRef<[u8]>,
    V: Ord,
    V::Key: Eq + std::hash::Hash + KeyEncoder<V> + KeyDecoder<V> + HasPrefix + Ord + AsRef<[u8]>,
{
    /// Construct an iterator over the versioned DB with a given prefix.
    ///
    /// Note that the returned iterator holds a read lock over the DB, so no writes can complete while it is active.
    pub fn iter_with_prefix(
        &self,
        prefix: V::Key,
    ) -> anyhow::Result<VersionedDbIterator<'_, V>> {
        let read_lock = self.db.cache.read();
        let version_on_disk = self.db.get_committed_version()?;
        println!("Version on disk: {:?}", version_on_disk);
        let raw_prefix = prefix.clone()..;
        let encoded_prefix = prefix.encode_key()?;
        let range = encoded_prefix.clone()..;
        let latest_version = self.latest_version();
        if version_on_disk.is_some_and(|v| v > latest_version.unwrap_or(0)) {
            return Err(anyhow::anyhow!("Version on disk is newer than the latest version in the reader. Cannot create an iterator which is guaranteed to be consistent with the version of this storage."));
        }
        // Setup: get a list of all the in-memory snapshots that are newer than the version on disk.
        // Our goal is to provide a consistent view of the state in this reader to the caller - so for each key that we discover we'll take the version from the newest snapshot,
        // looking at disk last. Ensuring we don't have any stale snapshots in our list makes this simpler.
        let snapshots: Vec<_> = self
            .snapshots
            .iter()
            .enumerate()
            .take_while(|(idx, _snapshot)| {
                let snapshot_version = latest_version.and_then(|v| v.checked_sub(*idx as u64));
                let Some(snapshot_version) = snapshot_version else {
                    return false;
                };
                if version_on_disk.is_some_and(|v| v >= snapshot_version) {
                    return false;
                }
                true
            })
            .map(|(_, snapshot)| {
                Some(
                    snapshot
                        .versioned_table_writes
                        .range(raw_prefix.clone())
                        .peekable(),
                )
            })
            .collect();

        let db_iterator = Some(
            self.db
                .live_db
                .iter_range_allow_cached::<V>(&read_lock, range, ScanDirection::Forward)?
                .peekable(),
        );
        Ok(VersionedDbIterator {
            _read_lock: read_lock,
            snapshots,
            db_iterator,
            prefix,
            encoded_prefix,
        })
    }

    /// Returns the latest value of a key in the reader.
    pub fn get_latest_borrowed(&self, key: &<V as Schema>::Key) -> anyhow::Result<Option<V::Value>> {
        for snapshot in self.snapshots.iter().rev() {
            if let Some(value) = snapshot.versioned_table_writes.get(key) {
                return Ok(value.clone());
            }
        }
        // The live value for any key is None if the DB is empty.
        let Some(own_version) = self.latest_version() else {
            return Ok(None);
        };
        // If the DB mutated underneath us such that the live version is now newer than this snapshot's version, we need to fetch from the historical table. This is much slower than fetching from the live table, but it should be a rare case.
        // Note that the data that was committed could come from a different fork even if it's at the same height as our snapshot. This is intentionally allowed for compatibiltiy with pre-existing
        // behavior of the system.
        let safe_db_version = self.db.fetch_latest_committed_archival_version();
        // If the value in the archival DB is newer than the version of this snapshot, then...
        // - It's not safe to fetch from the live table (that is probably also newer)
        // - The value we want is guaranteed to be in the archival DB, so we can fetch it from there.
        if safe_db_version > own_version {
            tracing::trace!(?safe_db_version, "DB is out of date, fetching 'live' values from historical table. Using latest version {:?}", own_version);
            // The data from the base version is guaranteed to match our data - but data from the latest version could be from a different fork that was committed
            return Ok(self.get_historical_borrowed(key, own_version)?);
        }

        let live_value = self.db.get_live_value(key)?;
        // If the DB has no committed version or if its latest version is less than the latest version we know about, then it hasn't changed underneath us in a way that would invalidate the read.
        let safe_db_version = self.db.fetch_latest_committed_archival_version();
        // If a write happened while we were fetching from the db, the results are unknown. Now that the state we want is guaranteed to be in the archival DB, we can fetch it safely from there.
        if safe_db_version > own_version {
            tracing::trace!(
                ?safe_db_version, "DB became out of date during a read. Fetching 'live' values from historical table. Using latest version {:?}",
                own_version
            );
            Ok(self.get_historical_borrowed(key, own_version)?)
        } else {
            Ok(live_value)
        }
    }

    /// Returns the global latest value for the given key.
    pub fn get_latest_borrowed_unbound(
        &self,
        key: &<V as Schema>::Key,
    ) -> anyhow::Result<Option<V::Value>> {
        let live_table_version = self.db.get_committed_version()?;
        let latest_version = self.latest_version();
        // If the "live" table is ahead of this storage, fetch directly from the live table.
        let Some(latest_version) = latest_version else {
            return self.db.get_live_value(key);
        };
        if live_table_version.is_some_and(|v| v > latest_version) {
            return self.db.get_live_value(key);
        }
        // Otherwise, look through the snapshots
        for (idx, snapshot) in self.snapshots.iter().rev().enumerate() {
            let snapshot_version = latest_version.saturating_sub(idx as u64);
            // If the snapshot is older than (or the same age as) the live table, we can stop looking.
            if live_table_version.is_some_and(|live_db_version| live_db_version >= snapshot_version)
            {
                break;
            }

            // Check the snapshot
            if let Some(value) = snapshot.versioned_table_writes.get(key) {
                return Ok(value.clone());
            }
        }
        // If we've looked through all the snapshots and not found the value, then the one in the db is newest. Take it.
        self.db.get_live_value(key)
    }

    /// Returns the value of a key in the historical column family as of the given version.
    pub fn get_historical_borrowed(
        &self,
        key: &<V as Schema>::Key,
        version: u64,
    ) -> Result<Option<V::Value>, HistoricalValueError> {
        let Some(newest_version) = self.latest_version() else {
            return Err(HistoricalValueError::FutureVersion {
                requested_version: version,
                latest_version: None,
            });
        };
        if version > newest_version {
            return Err(HistoricalValueError::FutureVersion {
                requested_version: version,
                latest_version: Some(newest_version),
            });
        }

        let mut version_of_current_snapshot = newest_version;
        for snapshot in self.snapshots.iter().rev() {
            if version_of_current_snapshot > version {
                version_of_current_snapshot -= 1;
                continue;
            }
            if let Some(value) = snapshot.versioned_table_writes.get(&key) {
                return Ok(value.clone());
            }
            version_of_current_snapshot -= 1;
        }
        let historical_value = self
            .db
            .get_historical_value_raw(key.encode_key().expect("Failed to encode key"), version)?;
        let oldest_available_version = self
            .db
            .get_pruned_version()?
            .and_then(|v| v.checked_add(1))
            .unwrap_or(0);
        if version < oldest_available_version {
            Err(HistoricalValueError::PrunedVersion {
                requested_version: version,
                oldest_available_version: Some(oldest_available_version),
            })
        } else {
            Ok(historical_value)
        }
    }
}

/// An error that occurs when querying for a historical value.
#[derive(Debug, thiserror::Error)]
pub enum HistoricalValueError {
    #[error("The requested version {requested_version} is newer than the newest version in the reader {latest_version:?}")]
    /// The requested version is newer than the latest version in the reader.
    FutureVersion {
        /// The requested version.
        requested_version: u64,
        /// The latest version in the reader.
        latest_version: Option<u64>,
    },
    #[error("The requested version {requested_version} has been pruned. The oldest available version is {oldest_available_version:?}")]
    /// The requested version is older than the oldest available version.
    PrunedVersion {
        /// The requested version.
        requested_version: u64,
        /// The oldest available version.
        oldest_available_version: Option<u64>,
    },
    /// Any other error
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
