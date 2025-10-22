use std::{borrow::Borrow, collections::HashMap, marker::PhantomData, sync::Arc};

use anyhow::bail;
use rocksdb::ColumnFamilyDescriptor;

use crate::{
    default_cf_descriptor,
    iterator::ScanDirection,
    schema::{ColumnFamilyName, KeyCodec, KeyDecoder, KeyEncoder, ValueCodec},
    CodecError, Schema, SchemaBatch, DB,
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
#[derive(Debug, PartialEq)]
pub enum VersionedTableMetadataKey {
    /// The latest version that has been committed.
    CommittedVersion,
    /// The newest version that has been pruned.
    PrunedVersion,
}

impl VersionedTableMetadataKey {
    /// Encodes the key into a byte vector.
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(match self {
            VersionedTableMetadataKey::CommittedVersion => vec![0],
            VersionedTableMetadataKey::PrunedVersion => vec![1],
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
            0 => VersionedTableMetadataKey::CommittedVersion,
            1 => VersionedTableMetadataKey::PrunedVersion,
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
#[derive(Clone, Debug)]
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
    _schema: S,
}

impl<S: SchemaWithVersion> VersionedDB<S>
// This where clause shouldn't be needed since it's implied by the Schema trait, but Rust intentionally doesn't elaborate these bounds.
where
    VersionedTableMetadataKey: KeyEncoder<S::VersionMetadatacolumn>,
{
    /// Returns the oldest version that is available in the database.
    pub fn get_pruned_version(&self) -> anyhow::Result<Option<u64>> {
        self.archival_db
            .get::<S::VersionMetadatacolumn>(&VersionedTableMetadataKey::PrunedVersion)
    }
    /// Returns the latest committed version in the database.
    pub fn get_committed_version(&self) -> anyhow::Result<Option<u64>> {
        self.live_db
            .get::<S::VersionMetadatacolumn>(&VersionedTableMetadataKey::CommittedVersion)
    }
}

#[derive(Debug)]
/// A key suffixed with a version number
pub struct VersionedKey<S: SchemaWithVersion, T>(pub T, pub u64, PhantomData<S>);
impl<S: SchemaWithVersion, T> VersionedKey<S, T> {
    /// Creates a new versioned key.
    pub fn new(key: T, version: u64) -> Self {
        Self(key, version, PhantomData)
    }
}

impl<S: SchemaWithVersion, T: KeyEncoder<S>> KeyEncoder<S::HistoricalColumnFamily>
    for VersionedKey<S, T>
{
    fn encode_key(&self) -> Result<Vec<u8>, CodecError> {
        let mut key = self.0.encode_key()?;
        key.extend_from_slice(&self.1.to_be_bytes());
        Ok(key)
    }
}

impl<S: SchemaWithVersion> KeyDecoder<S::HistoricalColumnFamily> for VersionedKey<S, S::Key> {
    fn decode_key(data: &[u8]) -> Result<Self, CodecError> {
        let len = data.len();
        let key_len = len.checked_sub(8).ok_or(CodecError::InvalidKeyLength {
            expected: 8,
            got: len,
        })?;
        let key = S::Key::decode_key(&data[..key_len])?;
        let version =
            u64::from_be_bytes(data[key_len..].try_into().expect(
                "key length was just checked to be 8 bytes but no longer is. This is a bug.",
            ));
        Ok(Self::new(key, version))
    }
}

/// A key *prefixed* with a version number for easy iteration by version.
#[derive(Debug)]
pub struct PrunableKey<S: SchemaWithVersion, T>(u64, T, PhantomData<S>);
impl<S: SchemaWithVersion, T> PrunableKey<S, T> {
    /// Creates a new prunable key.
    pub fn new(version: u64, key: T) -> Self {
        Self(version, key, PhantomData)
    }
}

impl<S: SchemaWithVersion, T: KeyEncoder<S>> KeyEncoder<S::PruningColumnFamily>
    for PrunableKey<S, T>
{
    fn encode_key(&self) -> Result<Vec<u8>, CodecError> {
        let mut key = self.0.to_be_bytes().to_vec();
        key.extend_from_slice(&self.1.encode_key()?);
        Ok(key)
    }
}

impl<S: SchemaWithVersion> KeyDecoder<S::PruningColumnFamily> for PrunableKey<S, S::Key> {
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
impl<S: SchemaWithVersion, T> PrunableKey<S, T> {
    /// Converts a prunable key into a versioned key.
    pub fn into_versioned_key(self) -> VersionedKey<S, T> {
        VersionedKey::new(self.1, self.0)
    }
}

#[derive(Debug, Clone, Default)]
/// A batch of writes to a versioned schema.
pub struct VersionedSchemaBatch<S: Schema> {
    versioned_table_writes: HashMap<S::Key, Option<S::Value>>,
}

impl<S: Schema, T: IntoIterator<Item = (S::Key, Option<S::Value>)>> From<T>
    for VersionedSchemaBatch<S>
where
    S::Key: Eq + std::hash::Hash,
{
    fn from(iter: T) -> Self {
        Self {
            versioned_table_writes: iter.into_iter().collect(),
        }
    }
}

impl<S: Schema> VersionedSchemaBatch<S>
where
    S::Key: Eq + std::hash::Hash,
{
    /// Puts a key-value pair into the batch.
    pub fn put_versioned(&mut self, key: S::Key, value: S::Value) {
        self.versioned_table_writes.insert(key, Some(value));
    }

    /// Deletes a key from the batch.
    pub fn delete_versioned(&mut self, key: S::Key) {
        self.versioned_table_writes.insert(key, None);
    }
}

fn live_versioned_column_family_descriptor(name: &str) -> ColumnFamilyDescriptor {
    let mut cf_opts: rocksdb::Options = rocksdb::Options::default();
    cf_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    // Use a 1GB block cache. TODO: Tune this value
    cf_opts.optimize_for_point_lookup(1024);
    rocksdb::ColumnFamilyDescriptor::new(name, cf_opts)
}

/// A specialized schema for values which have one "live" version and wish to automatically store a
/// (possibly truncated) history of all versions over time.
pub trait SchemaWithVersion: Schema {
    /// A column family for storing the historical values of the schema.
    type HistoricalColumnFamily: Schema<Key = VersionedKey<Self, Self::Key>, Value = Self::Value>;
    /// A column family for storing the pruning keys of the schema.
    type PruningColumnFamily: Schema<Key = PrunableKey<Self, Self::Key>, Value = ()>;
    /// A column family for storing the committed version of storage and the latest pruned version.
    /// For backwards compatibility, we make the slightly awkward choice of storing a column family of this name in both the live and archival DBs
    /// where the archival DB only stores the pruned version metadata and the live DB stores only the committed version metadata.
    /// (This way, the behavior of the split DBs is identical to the old behavior of a single DB.)
    type VersionMetadatacolumn: Schema<Key = VersionedTableMetadataKey, Value = u64>;
}

impl<V: SchemaWithVersion> VersionedDB<V>
where
    VersionedTableMetadataKey: KeyCodec<V::VersionMetadatacolumn>,
    (): ValueCodec<V::PruningColumnFamily>,
    V::Value: ValueCodec<V::HistoricalColumnFamily>,
    u64: ValueCodec<V::VersionMetadatacolumn>,
{
    /// Adds the column families for the versioned schema to the existing column families.
    pub fn add_column_families(
        existing_column_families: &mut Vec<ColumnFamilyDescriptor>,
        separate_archival_and_pruning: bool,
    ) -> anyhow::Result<()> {
        let historical_versioned_column_family = V::HistoricalColumnFamily::COLUMN_FAMILY_NAME;
        let pruning_column_family = V::PruningColumnFamily::COLUMN_FAMILY_NAME;
        let live_column_family = V::COLUMN_FAMILY_NAME;
        let metadata_column_family = V::VersionMetadatacolumn::COLUMN_FAMILY_NAME;
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
    pub fn from_dbs(live_db: Arc<DB>, archival_db: Arc<DB>) -> anyhow::Result<Self> {
        Ok(Self {
            live_db,
            archival_db,
            _schema: Default::default(),
        })
    }

    /// Returns an iterator over the pruning keys up to a given version.
    pub fn iter_pruning_keys_up_to_version(
        &self,
        version: u64,
    ) -> anyhow::Result<
        impl Iterator<Item = Result<<V::PruningColumnFamily as Schema>::Key, CodecError>> + use<'_, V>,
    > {
        // Because some keys are longer than 8 bytes, we use an exclusive range
        let first_version_to_keep = version.saturating_add(1);
        let range = ..first_version_to_keep.to_be_bytes().to_vec();
        let iterator = self
            .archival_db
            .raw_iter_range::<V::PruningColumnFamily>(range, ScanDirection::Forward)?;
        Ok(iterator.map(|(key, _value)| <V::PruningColumnFamily as Schema>::Key::decode_key(&key)))
    }

    /// Name of the database that can be used for logging or metrics or tracing.
    #[inline]
    pub fn name(&self) -> &'static str {
        self.live_db.name()
    }

    /// Returns the value of a key in the live column family.
    pub fn get_live_value(&self, key: &impl KeyEncoder<V>) -> anyhow::Result<Option<V::Value>> {
        self.live_db.get::<V>(key)
    }

    /// Returns the latest committed version in the database.
    pub fn load_latest_committed_version(&self) -> anyhow::Result<Option<u64>> {
        self.live_db
            .get::<V::VersionMetadatacolumn>(&VersionedTableMetadataKey::CommittedVersion)
    }

    /// Materializes a batch of writes to the database.
    // TODO: This is a confusing API that modifies the live DB batch in place *and* returns a batch. This is the most efficient way we can do things at the moment, but... oof.
    pub fn materialize(
        &self,
        batch: &VersionedSchemaBatch<V>,
        output_batch: &mut SchemaBatch,
        version: u64,
    ) -> anyhow::Result<SchemaBatch> {
        let mut historical_batch = SchemaBatch::new();
        for (key, value) in batch.versioned_table_writes.iter() {
            // Write to the Live keys table
            if let Some(value) = value {
                output_batch.put::<V>(key, value)?;
                historical_batch.put::<V::HistoricalColumnFamily>(
                    &VersionedKey::<V, &V::Key>::new(key, version),
                    value,
                )?;
            } else {
                output_batch.delete::<V>(key)?;
                historical_batch.delete::<V::HistoricalColumnFamily>(
                    &VersionedKey::<V, &V::Key>::new(key, version),
                )?;
            }
            // Write to the pruning table
            historical_batch.put::<V::PruningColumnFamily>(
                &PrunableKey::<V, &V::Key>::new(version, key),
                &(),
            )?;
        }
        // Write to the historical table
        output_batch.put::<V::VersionMetadatacolumn>(
            &VersionedTableMetadataKey::CommittedVersion,
            &version,
        )?;
        Ok(historical_batch)
    }

    /// Commits the archival state to the archival DB.
    pub fn commit_archival(&self, batch: SchemaBatch) -> anyhow::Result<()> {
        self.archival_db.write_schemas(batch)
    }

    /// Returns the value of a key in the historical column family as of the given version.
    pub fn get_historical_value(
        &self,
        key_to_get: &impl KeyEncoder<V>,
        version: u64,
    ) -> anyhow::Result<Option<V::Value>> {
        let Some((value_bytes, _version)) = self.get_versioned_internal(key_to_get, version)?
        else {
            return Ok(None);
        };
        let value = V::Value::decode_value(&value_bytes)?;
        Ok(Some(value))
    }

    /// Returns the latest version at which the given key was written.
    pub fn get_version_for_key(
        &self,
        key_to_get: &impl KeyEncoder<V>,
        max_version: u64,
    ) -> anyhow::Result<Option<u64>> {
        let Some((_, version)) = self.get_versioned_internal(key_to_get, max_version)? else {
            return Ok(None);
        };
        Ok(Some(version))
    }

    fn get_versioned_internal(
        &self,
        key_to_get: &impl KeyEncoder<V>,
        max_version: u64,
    ) -> anyhow::Result<Option<(Vec<u8>, u64)>> {
        let key_with_version = VersionedKey::<V, _>::new(key_to_get, max_version).encode_key()?;
        let range = ..=&key_with_version;
        let mut iterator = self
            .archival_db
            .raw_iter_range::<V::HistoricalColumnFamily>(range, ScanDirection::Backward)?;
        if let Some((key, value_bytes)) = iterator.next() {
            // Safety: All keys are suffixed with an 8-byte version.
            let (key_bytes, version_bytes) = key.split_at(key.len() - 8);
            if key_bytes.len() != key_with_version.len() - 8
                || key_bytes != &key_with_version[..key_bytes.len()]
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
    db: VersionedDB<V>,
    // The version of the underlying DB at the time this snapshot was taken.
    base_version: Option<u64>,
    snapshots: Vec<Arc<VersionedSchemaBatch<V>>>,
}

impl<V: SchemaWithVersion> VersionedDeltaReader<V>
where
    V::Key: Eq + std::hash::Hash,
    VersionedTableMetadataKey: KeyCodec<V::VersionMetadatacolumn>,
    V::Value: Clone,
    (): ValueCodec<V::PruningColumnFamily>,
    V::Value: ValueCodec<V::HistoricalColumnFamily>,
    u64: ValueCodec<V::VersionMetadatacolumn>,
{
    /// Creates a new versioned delta reader.
    pub fn new(
        db: VersionedDB<V>,
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

impl<V: SchemaWithVersion<Key = Arc<K>>, K> VersionedDeltaReader<V>
where
    K: Eq + std::hash::Hash + KeyEncoder<V>,
    VersionedTableMetadataKey: KeyCodec<V::VersionMetadatacolumn>,
    V::Value: Clone,
    (): ValueCodec<V::PruningColumnFamily>,
    V::Value: ValueCodec<V::HistoricalColumnFamily>,
    u64: ValueCodec<V::VersionMetadatacolumn>,
{
    /// Returns the latest value of a key in the reader.
    pub fn get_latest_borrowed(&self, key: impl Borrow<K>) -> anyhow::Result<Option<V::Value>> {
        for snapshot in self.snapshots.iter().rev() {
            if let Some(value) = snapshot.versioned_table_writes.get(key.borrow()) {
                return Ok(value.clone());
            }
        }
        // The live value for any key is None if the DB is empty.
        let Some(latest_version) = self.latest_version() else {
            return Ok(None);
        };
        // If the DB mutated underneath us such that the live version is now newer than this snapshot's version, we need to fetch from the historical table. This is much slower than fetching from the live table, but it should be a rare case.
        // Note that the data that was committed could come from a different fork even if it's at the same height as our snapshot. This is intentionally allowed for compatibiltiy with pre-existing
        // behavior of the system.
        let loaded_version = self.db.get_committed_version()?;
        if loaded_version.is_some_and(|v| v > latest_version) {
            tracing::trace!(?loaded_version, "DB is out of date, fetching 'live' values from historical table. Using latest version {:?}", latest_version);
            // The data from the base version is guaranteed to match our data - but data from the latest version could be from a different fork that was committed
            return Ok(self.get_historical_borrowed(key, latest_version)?);
        }

        let live_value = self.db.get_live_value(key.borrow())?;
        // If the DB has no committed version or if its latest version is less than the latest version we know about, then it hasn't changed underneath us in a way that would invalidate the read.
        let loaded_version = self.db.get_committed_version()?;
        if loaded_version.is_some_and(|v| v > latest_version) {
            // Coherency - check that the DB is still in date before returning the value. If not, we need to retry from the historical table.
            tracing::trace!(
                ?loaded_version, "DB became out of date during a read. Fetching 'live' values from historical table. Using latest version {:?}",
                latest_version
            );
            Ok(self.get_historical_borrowed(key, latest_version)?)
        } else {
            Ok(live_value)
        }
    }

    /// Returns the global latest value for the given key.
    pub fn get_latest_borrowed_unbound(
        &self,
        key: impl Borrow<K>,
    ) -> anyhow::Result<Option<V::Value>> {
        let live_table_version = self.db.get_committed_version()?;
        let latest_version = self.latest_version();
        // If the "live" table is ahead of this storage, fetch directly from the live table.
        let Some(latest_version) = latest_version else {
            return self.db.get_live_value(key.borrow());
        };
        if live_table_version.is_some_and(|v| v > latest_version) {
            return self.db.get_live_value(key.borrow());
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
            if let Some(value) = snapshot.versioned_table_writes.get(key.borrow()) {
                return Ok(value.clone());
            }
        }
        // If we've looked through all the snapshots and the live table is still ahead, fetch from the live table.
        self.db.get_live_value(key.borrow())
    }

    /// Returns the value of a key in the historical column family as of the given version.
    pub fn get_historical_borrowed(
        &self,
        key: impl Borrow<K>,
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
            if let Some(value) = snapshot.versioned_table_writes.get(key.borrow()) {
                return Ok(value.clone());
            }
            version_of_current_snapshot -= 1;
        }
        let historical_value = self.db.get_historical_value(key.borrow(), version)?;
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
