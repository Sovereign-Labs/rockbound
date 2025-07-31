use std::{
    borrow::Borrow,
    collections::HashMap,
    marker::PhantomData,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use anyhow::bail;
use rocksdb::ColumnFamilyDescriptor;

use crate::{
    default_cf_descriptor,
    iterator::ScanDirection,
    schema::{ColumnFamilyName, KeyCodec, KeyDecoder, KeyEncoder, ValueCodec},
    CodecError, Schema, SchemaBatch, DB,
};
#[derive(Debug, Default)]
pub(crate) struct CommittedVersion;

impl Schema for CommittedVersion {
    const COLUMN_FAMILY_NAME: ColumnFamilyName = "committed_version";
    const SHOULD_CACHE: bool = false;

    type Key = EmptyKey;
    type Value = u64;
}

/// A singleton key. Encodes to the empty vec.
#[derive(Debug, PartialEq)]
pub struct EmptyKey;

impl AsRef<EmptyKey> for EmptyKey {
    fn as_ref(&self) -> &EmptyKey {
        self
    }
}

impl KeyEncoder<CommittedVersion> for EmptyKey {
    fn encode_key(&self) -> Result<Vec<u8>, CodecError> {
        Ok(vec![])
    }
}

impl KeyDecoder<CommittedVersion> for EmptyKey {
    fn decode_key(_data: &[u8]) -> Result<Self, CodecError> {
        if !_data.is_empty() {
            return Err(CodecError::InvalidKeyLength {
                expected: 0,
                got: _data.len(),
            });
        }
        Ok(EmptyKey)
    }
}

impl ValueCodec<CommittedVersion> for u64 {
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
///     Note that: pruning must leave at least one entry for each live key indicating at which version it was written.
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
    db: Arc<DB>,
    oldest_available_version: Arc<AtomicU64>,
    _schema: S,
}

impl<S: SchemaWithVersion> VersionedDB<S>
// This where clause shouldn't be needed since it's implied by the Schema trait, but Rust intentionally doesn't elaborate these bounds.
where
    EmptyKey: KeyEncoder<S::CommittedVersionColumn>,
{
    /// Returns the oldest version that is available in the database.
    pub fn get_oldest_available_version(&self, ordering: Ordering) -> u64 {
        self.oldest_available_version.load(ordering)
    }
    /// Returns the latest committed version in the database.
    pub fn get_committed_version(&self) -> anyhow::Result<Option<u64>> {
        self.db.get::<S::CommittedVersionColumn>(&EmptyKey)
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
        let key_len = len.checked_sub(8).ok_or(CodecError::InvalidKeyLength {
            expected: 8,
            got: len,
        })?;
        let key = S::Key::decode_key(&data[..key_len])?;
        let version =
            u64::from_be_bytes(data[key_len..].try_into().expect(
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
    /// A column family for storing the committed version of the schema.
    type CommittedVersionColumn: Schema<Key = EmptyKey, Value = u64>;
}

impl<V: SchemaWithVersion> VersionedDB<V>
where
    EmptyKey: KeyCodec<V::CommittedVersionColumn>,
    (): ValueCodec<V::PruningColumnFamily>,
    V::Value: ValueCodec<V::HistoricalColumnFamily>,
    u64: ValueCodec<V::CommittedVersionColumn>,
{
    /// Adds the column families for the versioned schema to the existing column families.
    pub fn add_column_families(
        existing_column_families: &mut Vec<ColumnFamilyDescriptor>,
    ) -> anyhow::Result<()> {
        let historical_versioned_column_family = V::HistoricalColumnFamily::COLUMN_FAMILY_NAME;
        let pruning_column_family = V::PruningColumnFamily::COLUMN_FAMILY_NAME;
        let live_column_family = V::COLUMN_FAMILY_NAME;
        let committed_version_column_family = V::CommittedVersionColumn::COLUMN_FAMILY_NAME;
        for column in existing_column_families.iter() {
            if column.name() == committed_version_column_family
                || column.name() == historical_versioned_column_family
                || column.name() == live_column_family
                || column.name() == pruning_column_family
            {
                bail!("{} column name is reserved for internal use", column.name());
            }
        }
        existing_column_families.push(default_cf_descriptor(historical_versioned_column_family));
        existing_column_families.push(live_versioned_column_family_descriptor(live_column_family));
        existing_column_families.push(default_cf_descriptor(pruning_column_family));
        existing_column_families.push(default_cf_descriptor(committed_version_column_family));
        Ok(())
    }

    /// Creates a new versioned DB from an existing DB.
    pub fn from_db(db: Arc<DB>) -> anyhow::Result<Self> {
        let oldest_available_version = Self::get_next_version_to_prune_from_db(db.as_ref())?;
        Ok(Self {
            db,
            _schema: Default::default(),
            oldest_available_version: Arc::new(AtomicU64::new(oldest_available_version)),
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
        let range = 0u64.to_be_bytes().to_vec()..first_version_to_keep.to_be_bytes().to_vec();
        let iterator = self
            .db
            .raw_iter_range::<V::PruningColumnFamily>(range, ScanDirection::Forward)?;
        Ok(iterator.map(|(key, _value)| <V::PruningColumnFamily as Schema>::Key::decode_key(&key)))
    }

    /// Returns the next version to prune from the database.
    pub fn get_next_version_to_prune_from_db(db: &DB) -> anyhow::Result<u64> {
        let mut iterator = db.raw_iter_range::<V::PruningColumnFamily>(
            0u64.to_be_bytes().to_vec()..,
            ScanDirection::Forward,
        )?;
        let next_version_to_prune = iterator
            .next()
            .map(|(key, _value)| {
                u64::from_be_bytes(
                    key[..8]
                        .try_into()
                        .expect("version bytes were 8 bytes but no longer are. This is a bug."),
                )
            })
            .unwrap_or(0);
        Ok(next_version_to_prune)
    }

    /// Name of the database that can be used for logging or metrics or tracing.
    #[inline]
    pub fn name(&self) -> &'static str {
        self.db.name()
    }

    /// Returns the value of a key in the live column family.
    pub fn get_live_value(&self, key: &impl KeyEncoder<V>) -> anyhow::Result<Option<V::Value>> {
        self.db.get::<V>(key)
    }

    /// Returns the latest committed version in the database.
    pub fn load_latest_committed_version(&self) -> anyhow::Result<Option<u64>> {
        self.db.get::<V::CommittedVersionColumn>(&EmptyKey)
    }

    /// Materializes a batch of writes to the database.
    pub fn materialize(
        &self,
        batch: &VersionedSchemaBatch<V>,
        output_batch: &mut SchemaBatch,
        version: u64,
    ) -> anyhow::Result<()> {
        for (key, value) in batch.versioned_table_writes.iter() {
            // Write to the Live keys table
            if let Some(value) = value {
                output_batch.put::<V>(key, value)?;
                output_batch.put::<V::HistoricalColumnFamily>(
                    &VersionedKey::<V, &V::Key>::new(key, version),
                    value,
                )?;
            } else {
                output_batch.delete::<V>(key)?;
                output_batch.delete::<V::HistoricalColumnFamily>(
                    &VersionedKey::<V, &V::Key>::new(key, version),
                )?;
            }
            // Write to the pruning table
            output_batch.put::<V::PruningColumnFamily>(
                &PrunableKey::<V, &V::Key>::new(version, key),
                &(),
            )?;
        }
        // Write to the historical table
        output_batch.put::<V::CommittedVersionColumn>(&EmptyKey, &version)?;
        Ok(())
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
            .db
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
            debug_assert!( version <= max_version, "Unexpected version. Queried for less than or equal to version {} but got version {}", max_version, version);
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
    EmptyKey: KeyCodec<V::CommittedVersionColumn>,
    V::Value: Clone,
    (): ValueCodec<V::PruningColumnFamily>,
    V::Value: ValueCodec<V::HistoricalColumnFamily>,
    u64: ValueCodec<V::CommittedVersionColumn>,
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
    EmptyKey: KeyCodec<V::CommittedVersionColumn>,
    V::Value: Clone,
    (): ValueCodec<V::PruningColumnFamily>,
    V::Value: ValueCodec<V::HistoricalColumnFamily>,
    u64: ValueCodec<V::CommittedVersionColumn>,
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
        // If the DB mutated underneath us such that the live version is now newer than this snapshot's versino, we need to fetch from the historical table. This is much slower than fetching from the live table, but it should be a rare case.
        // Note that the data that was committed could come from a different fork even if it's at the same height as our snapshot. This is intentionally allowed for compatibiltiy with pre-existing
        // behavior of the system.
        let loaded_version = self.db.get_committed_version()?;
        if loaded_version.is_some_and(|v| v > latest_version) {
            tracing::debug!(?loaded_version, "DB is out of date, fetching 'live' values from historical table. Using latest version {:?}", latest_version);
            // The data from the base version is guaranteed to match our data - but data from the latest version could be from a different fork that was committed
            return self.get_historical_borrowed(key, latest_version);
        }

        let live_value = self.db.get_live_value(key.borrow())?;
        // If the DB has no committed version or if its latest version is less than the latest version we know about, then it hasn't changed underneath us in a way that would invalidate the read.
        let loaded_version = self.db.get_committed_version()?;
        if loaded_version.is_some_and(|v| v > latest_version) {
            // Coherency - check that the DB is still in date before returning the value. If not, we need to retry from the historical table.
            tracing::debug!(
                ?loaded_version, "DB became out of date during a read. Fetching 'live' values from historical table. Using latest version {:?}",
                latest_version
            );
            self.get_historical_borrowed(key, latest_version)
        } else {
            Ok(live_value)
        }
    }

    /// Returns the value of a key in the historical column family as of the given version.
    pub fn get_historical_borrowed(
        &self,
        key: impl Borrow<K>,
        version: u64,
    ) -> anyhow::Result<Option<V::Value>> {
        let Some(newest_version) = self.latest_version() else {
            return Err(anyhow::anyhow!(
                "Cannot query for historical values against an empty database"
            ));
        };
        if version > newest_version {
            return Err(anyhow::anyhow!(
                "Requested version {} is greater than the newest version {}",
                version,
                newest_version
            ));
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
        let oldest_available_version = self.db.get_oldest_available_version(Ordering::Acquire);
        if version < oldest_available_version {
            Err(anyhow::anyhow!(
                "Requested version {} is older than the oldest available version {}",
                version,
                oldest_available_version
            ))
        } else {
            Ok(historical_value)
        }
    }
}
