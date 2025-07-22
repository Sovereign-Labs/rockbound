#![allow(missing_docs)]
use std::{borrow::Borrow, collections::HashMap, marker::PhantomData, path::Path, sync::{atomic::{AtomicU64, Ordering}, Arc}};

use anyhow::bail;

use crate::{iterator::ScanDirection, schema::{ColumnFamilyName, KeyCodec, KeyDecoder, KeyEncoder, ValueCodec}, CodecError, Schema, SchemaBatch, DB};

#[derive(Debug)]
struct Cache;

#[derive(Debug, Default)]
pub(crate) struct CommittedVersion;

impl Schema for CommittedVersion {
    const COLUMN_FAMILY_NAME: ColumnFamilyName = "committed_version";

    type Key = EmptyKey;
    type Value = u64;
}


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
		if _data.len() != 0 {
			return Err(CodecError::InvalidKeyLength { expected: 0, got: _data.len() });
		}
		Ok(EmptyKey)
    }
}

impl ValueCodec<CommittedVersion> for u64 {
    fn encode_value(&self) -> Result<Vec<u8>, CodecError> {
		Ok(self.to_le_bytes().to_vec())
    }

	fn decode_value(data: &[u8]) -> Result<Self, CodecError> {
		Ok(u64::from_le_bytes(data.try_into().map_err(|_| CodecError::InvalidKeyLength { expected: 8, got: data.len() })?))
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
	// TODO: Consider using a Mutex instead of an atomic since we need to synchronize this with the DB.
	committed_version: Arc<AtomicU64>,
	cache: Arc<Cache>,
	schema: S,
}

#[derive(Debug )]
pub struct VersionedKey<S: SchemaWithVersion, T>(T, u64, PhantomData<S>);
impl<S: SchemaWithVersion, T> VersionedKey<S, T> {
	pub fn new(key: T, version: u64) -> Self {
		Self(key, version, PhantomData)
	}
}

impl<S: SchemaWithVersion, T: KeyEncoder<S>> KeyEncoder<S::HistoricalColumnFamily> for VersionedKey<S, T> {
	fn encode_key(&self) -> Result<Vec<u8>, CodecError> {
		let mut key = self.0.encode_key()?;
		key.extend_from_slice(&self.1.to_be_bytes());
		Ok(key)
	}
}

impl<S: SchemaWithVersion> KeyDecoder<S::HistoricalColumnFamily> for VersionedKey<S, S::Key> {
	fn decode_key(data: &[u8]) -> Result<Self, CodecError> {
		let len = data.len();
		let key_len = len.checked_sub(8).ok_or(CodecError::InvalidKeyLength { expected: 8, got: len })?;
		let key = S::Key::decode_key(&data[..key_len])?;
		let version = u64::from_be_bytes(data[key_len..].try_into().expect("key length was just checked to be 8 bytes but no longer is. This is a bug."));
		Ok(Self::new(key, version,))
	}
}

#[derive(Debug )]
pub struct PrunableKey<S: SchemaWithVersion, T>(u64, T, PhantomData<S>);
impl<S: SchemaWithVersion, T> PrunableKey<S, T> {
	pub fn new(version: u64, key: T) -> Self {
		Self(version, key, PhantomData)
	}
}

impl<S: SchemaWithVersion, T: KeyEncoder<S>> KeyEncoder<S::PruningColumnFamily> for PrunableKey<S, T> {
	fn encode_key(&self) -> Result<Vec<u8>, CodecError> {
		let mut key = self.0.to_be_bytes().to_vec();
		key.extend_from_slice(&self.1.encode_key()?);
		Ok(key)
	}
}

impl<S: SchemaWithVersion> KeyDecoder<S::PruningColumnFamily> for PrunableKey<S, S::Key> {
	fn decode_key(data: &[u8]) -> Result<Self, CodecError> {
		let len = data.len();
		let key_len = len.checked_sub(8).ok_or(CodecError::InvalidKeyLength { expected: 8, got: len })?;
		let key = S::Key::decode_key(&data[..key_len])?;
		let version = u64::from_be_bytes(data[key_len..].try_into().expect("key length was just checked to be 8 bytes but no longer is. This is a bug."));
		Ok(Self::new(version, key))
	}
}

// #[derive(Debug, Default)]
// pub struct VersionedSchema<S: Schema>(PhantomData<S>);
// impl<S: Schema> Schema for VersionedSchema<S> {
// 	const COLUMN_FAMILY_NAME: ColumnFamilyName = "historical_data";
// 	type Key = VersionedKey<S, S::Key>;
// 	type Value = S::Value;
// }


// impl<S: Schema> ValueCodec<VersionedSchema<S>> for S::Value {
// 	fn encode_value(&self) -> Result<Vec<u8>, CodecError> {
// 		S::Value::encode_value(self)
// 	}
	
// 	fn decode_value(data: &[u8]) -> Result<Self, CodecError> {
// 		S::Value::decode_value(data)
// 	}
// }

#[derive(Debug, Clone, Default)]
pub struct VersionedSchemaBatch<S: Schema> {
	versioned_table_writes: HashMap<S::Key, Option<S::Value>>,
}

impl<S: Schema, T: IntoIterator<Item = (S::Key, Option<S::Value>)>> From<T> for VersionedSchemaBatch<S>
where S::Key: Eq + std::hash::Hash {
	fn from(iter: T) -> Self {
		Self {
			versioned_table_writes: iter.into_iter().collect(),
		}
	}
}

impl<S: Schema> VersionedSchemaBatch<S> 
where S::Key: Eq + std::hash::Hash {
	pub fn put_versioned(&mut self, key: S::Key, value: S::Value) {
		self.versioned_table_writes.insert(key, Some(value));
	}

	pub fn delete_versioned(&mut self, key: S::Key) {
		self.versioned_table_writes.insert(key, None);
	}
}

// impl<S: Schema> VersionedSchemaBatch<S> {
// 	pub fn put_non_versioned<S2: Schema>(&mut self, key: &impl KeyCodec<S2>, value: &impl ValueCodec<S2>) {
// 		assert_ne!(S::COLUMN_FAMILY_NAME, VersionedSchema::<S>::COLUMN_FAMILY_NAME);
// 		assert_ne!(S::COLUMN_FAMILY_NAME, S2::COLUMN_FAMILY_NAME);
// 		self.other_changes.put::<S2>(key, value);
// 	}

// 	pub fn delete_non_versioned<S2: Schema>(&mut self, key: &impl KeyCodec<S2>) {
// 		assert_ne!(S::COLUMN_FAMILY_NAME, VersionedSchema::<S>::COLUMN_FAMILY_NAME);
// 		assert_ne!(S::COLUMN_FAMILY_NAME, S2::COLUMN_FAMILY_NAME);
// 		self.other_changes.delete::<S2>(key);
// 	}
// }

/// A specialized schema for values which have one "live" version and wish to automatically store a 
/// (possibly truncated) history of all versions over time.
pub trait SchemaWithVersion: Schema {
	type HistoricalColumnFamily: Schema<Key = VersionedKey<Self, Self::Key>, Value = Self::Value>;
	type PruningColumnFamily: Schema<Key = PrunableKey<Self, Self::Key>, Value = ()>;
	type CommittedVersionColumn: Schema<Key = EmptyKey, Value = u64>;
}

impl<V: SchemaWithVersion> VersionedDB<V> 
where EmptyKey: KeyCodec<V::CommittedVersionColumn>,
	(): ValueCodec<V::PruningColumnFamily>,
	V::Value: ValueCodec<V::HistoricalColumnFamily>,
	u64: ValueCodec<V::CommittedVersionColumn>
	{
		// TODO: Optimize for point lookup on the live version 
	pub fn add_column_families(
		existing_column_families: &mut Vec<ColumnFamilyName>,
    ) -> anyhow::Result<()> {
		let historical_versioned_column_family = V::HistoricalColumnFamily::COLUMN_FAMILY_NAME;
		let pruning_column_family = V::PruningColumnFamily::COLUMN_FAMILY_NAME;
		let live_column_family = V::COLUMN_FAMILY_NAME;
		let committed_version_column_family = V::CommittedVersionColumn::COLUMN_FAMILY_NAME;
		for column in existing_column_families.iter() {
			if column == &committed_version_column_family || column == &historical_versioned_column_family || column == &live_column_family || column == &pruning_column_family {
				bail!("{} column name is reserved for internal use", column);
			}
		}
		existing_column_families.push(historical_versioned_column_family);
		existing_column_families.push(live_column_family);
		existing_column_families.push(pruning_column_family);
		existing_column_families.push(committed_version_column_family);
		Ok(())
    }

	pub fn from_db(
		db: Arc<DB>,
	) -> anyhow::Result<Self> {
		let committed_version = db.get::<V::CommittedVersionColumn>(&EmptyKey)?.unwrap_or(0);
		Ok(Self { db, committed_version: Arc::new(AtomicU64::new(committed_version)), cache: Arc::new(Cache), schema: Default::default() })
	}

	/// Name of the database that can be used for logging or metrics or tracing.
    #[inline]
    pub fn name(&self) -> &'static str {
        self.db.name()
    }

	pub fn get_live_value(&self, key: &impl KeyEncoder<V>) -> anyhow::Result<Option<V::Value>> {
		self.db.get::<V>(key)
	}

	pub fn materialize(&self, batch: &VersionedSchemaBatch<V>, output_batch: &mut SchemaBatch) -> anyhow::Result<()> {
		let version = self.committed_version.fetch_add(1, Ordering::SeqCst);
		for (key, value) in batch.versioned_table_writes.iter() {
			// TODO: Update cache here;
			// Write to the Live keys table
			if let Some(value) = value {
				output_batch.put::<V>(key, value)?;
				output_batch.put::<V::HistoricalColumnFamily>(&VersionedKey::<V, &V::Key>::new(key, version), value)?;
			} else {
				output_batch.delete::<V>(key)?;
				output_batch.delete::<V::HistoricalColumnFamily>(&VersionedKey::<V, &V::Key>::new(key, version))?;
			}
			// Write to the pruning table
			output_batch.put::<V::PruningColumnFamily>(&PrunableKey::<V, &V::Key>::new(version, key), &())?;
			// Write to the historical table
			output_batch.put::<V::CommittedVersionColumn>(&EmptyKey, &version)?;

		}
		self.db.write_schemas(&output_batch)
	}

	pub fn get_historical_value(&self, key_to_get: &V::Key, version: u64) -> anyhow::Result<Option<V::Value>> {
		let key_with_version = VersionedKey::<V, _>::new(key_to_get, version).encode_key()?;
		let range = &key_with_version..;
		let mut iterator = self.db.raw_iter_range::<V::HistoricalColumnFamily>(range, ScanDirection::Backward)?;
        if let Some((key, value_bytes)) = iterator.next() {
			// Safety: All keys are suffixed with an 8-byte version.
			let (key_bytes, version_bytes) = key.split_at(key.len() - 8);
			if key_bytes.len() != key_with_version.len() - 8 || key_bytes != &key_with_version[..key_bytes.len()] {
				return Ok(None);
			}
			debug_assert!(u64::from_be_bytes(version_bytes.try_into().expect("version bytes were 8 bytes but no longer are. This is a bug.")) <= version, "Unexpected version. Queried for less than or equal to version {} but got version {}", version, u64::from_be_bytes(version_bytes.try_into().unwrap()));
            let value = V::Value::decode_value(&value_bytes)?;
            return Ok(Some(value));
        }
		Ok(None)
	}
	
}

#[derive(Debug, Clone)]
pub struct VersionedDeltaReader<V: SchemaWithVersion> {
	db: VersionedDB<V>,
	base_version: Option<u64>,
	snapshots: Vec<Arc<VersionedSchemaBatch<V>>>,
}

impl<V: SchemaWithVersion> VersionedDeltaReader<V> 
where V::Key: Eq + std::hash::Hash,
EmptyKey: KeyCodec<V::CommittedVersionColumn>,
	V::Value: Clone,
	(): ValueCodec<V::PruningColumnFamily>,
	V::Value: ValueCodec<V::HistoricalColumnFamily>,
	u64: ValueCodec<V::CommittedVersionColumn>
{
	pub fn new(db: VersionedDB<V>, base_version: Option<u64>, snapshots: Vec<Arc<VersionedSchemaBatch<V>>>) -> Self {
        Self { snapshots, db, base_version }
    }

	pub fn get_latest(&self, key: impl Borrow<V::Key>) -> anyhow::Result<Option<V::Value>> {
		for snapshot in self.snapshots.iter().rev() {
			if let Some(value) = snapshot.versioned_table_writes.get(key.borrow()) {
				return Ok(value.as_ref().map(|value| value.clone()));
			}
		}
		self.db.get_live_value(key.borrow())
	}

	pub fn get_historical(&self, key: &V::Key, version: u64) -> anyhow::Result<Option<V::Value>> {
		let Some(newest_version) = self.latest_version() else {
			return Err(anyhow::anyhow!("Cannot query for historical values against an empty database"));
		};
		if version > newest_version {
			return Err(anyhow::anyhow!("Requested version {} is greater than the newest version {}", version, newest_version));
		}
		let mut version_of_current_snapshot = newest_version;
		for snapshot in self.snapshots.iter().rev() {
			if version_of_current_snapshot > version {
				version_of_current_snapshot -= 1;
				continue;
			}
			if let Some(value) = snapshot.versioned_table_writes.get(key) {
				return Ok(value.as_ref().map(|value| value.clone()));
			}
			version_of_current_snapshot -= 1;
		}
		self.db.get_historical_value(key, version)
	}

	pub fn latest_version(&self) -> Option<u64> {
		match self.base_version {
			Some(base_version) => Some(base_version + self.snapshots.len() as u64),
			None => self.snapshots.len().checked_sub(1).map(|len| len as u64),
		}
	}
}


impl<V: SchemaWithVersion<Key = Arc<K>>, K> VersionedDeltaReader<V> 
where K: Eq + std::hash::Hash + KeyEncoder<V>,
EmptyKey: KeyCodec<V::CommittedVersionColumn>,
	V::Value: Clone,
	(): ValueCodec<V::PruningColumnFamily>,
	V::Value: ValueCodec<V::HistoricalColumnFamily>,
	u64: ValueCodec<V::CommittedVersionColumn>
{
	pub fn get_latest_borrowed(&self, key: impl Borrow<K>) -> anyhow::Result<Option<V::Value>> {
		for snapshot in self.snapshots.iter().rev() {
			if let Some(value) = snapshot.versioned_table_writes.get(key.borrow()) {
				return Ok(value.as_ref().map(|value| value.clone()));
			}
		}
		self.db.get_live_value(key.borrow())
	}
}
