use std::{marker::PhantomData, path::Path, sync::{atomic::AtomicU64, Arc}};

use anyhow::bail;

use crate::{iterator::ScanDirection, schema::{ColumnFamilyName, KeyCodec, KeyDecoder, KeyEncoder, ValueCodec}, CodecError, Schema, DB};

struct Cache;

#[derive(Debug, Default)]
pub(crate) struct CommittedVersion;

impl Schema for CommittedVersion {
    const COLUMN_FAMILY_NAME: ColumnFamilyName = "committed_version";

    type Key = EmptyKey;
    type Value = u64;
}


#[derive(Debug, PartialEq)]
struct EmptyKey;

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
struct VersionedDB<S: Schema> {
	db: DB,
	committed_version: Arc<AtomicU64>,
	cache: Arc<Cache>,
	schema: S,
}

#[derive(Debug )]
pub struct VersionedKey<S: Schema, T>(T, u64, PhantomData<S>);
impl<S: Schema, T: KeyEncoder<S>> VersionedKey<S, T> {
	pub fn new(key: T, version: u64) -> Self {
		Self(key, version, PhantomData)
	}
}

impl<S: Schema, T: KeyEncoder<S>> PartialEq for VersionedKey<S, T> {
	fn eq(&self, other: &Self) -> bool {
		self.0.encode_key().map_or(false, |key| key == other.0.encode_key().map_or(false, |other_key| other_key == key)) && self.1 == other.1
	}
}

impl<S: Schema, T: KeyEncoder<S>> KeyEncoder<VersionedSchema<S>> for VersionedKey<S, T> {
	fn encode_key(&self) -> Result<Vec<u8>, CodecError> {
		let mut key = self.0.encode_key()?;
		key.extend_from_slice(&self.1.to_be_bytes());
		Ok(key)
	}
}

impl<S: Schema> KeyDecoder<VersionedSchema<S>> for VersionedKey<S, S::Key> {
	fn decode_key(data: &[u8]) -> Result<Self, CodecError> {
		let len = data.len();
		let key_len = len.checked_sub(8).ok_or(CodecError::InvalidKeyLength { expected: 8, got: len })?;
		let key = S::Key::decode_key(&data[..key_len])?;
		let version = u64::from_be_bytes(data[key_len..].try_into().expect("key length was just checked to be 8 bytes but no longer is. This is a bug."));
		Ok(Self::new(key, version,))
	}
}

#[derive(Debug, Default)]
pub struct VersionedSchema<S: Schema>(PhantomData<S>);
impl<S: Schema> Schema for VersionedSchema<S> {
	const COLUMN_FAMILY_NAME: ColumnFamilyName = "historical_data";
	type Key = VersionedKey<S>;
	type Value = S::Value;
}

// impl<S: Schema, K: AsRef<S::Key> + std::fmt::Debug> KeyEncoder<VersionedSchema<S>> for (K, u64) {
// 	fn encode_key(&self) -> Result<Vec<u8>, CodecError> {
// 		let (key, version) = self;
// 		let mut key = key.as_ref().encode_key()?;
// 		key.extend_from_slice(&version.to_be_bytes());
// 		Ok(key)
// 	}
// }



impl<S: Schema> ValueCodec<VersionedSchema<S>> for S::Value {
	fn encode_value(&self) -> Result<Vec<u8>, CodecError> {
		S::Value::encode_value(self)
	}
	
	fn decode_value(data: &[u8]) -> Result<Self, CodecError> {
		S::Value::decode_value(data)
	}
}

impl<S: Schema> VersionedDB<S> {
	// TODO: Figure out how to get the version from the db
	pub fn open(
        path: impl AsRef<Path>,
        name: &'static str,
		versioned_column_family: impl Into<String>,
        plain_column_families: impl IntoIterator<Item = impl Into<String>>,
        db_opts: &rocksdb::Options,
    ) -> anyhow::Result<Self> {
		let versioned_column_family = versioned_column_family.into();
		let historical_versioned_column_family = VersionedSchema::<S>::COLUMN_FAMILY_NAME;
		let pruning_column_family = format!("{}_pruning", versioned_column_family);
		let live_column_family = format!("{}_live", versioned_column_family);
		let mut columns = plain_column_families.into_iter().map(|cf_name| cf_name.into()).collect::<Vec<_>>();
		for column in columns.iter() {
			if column == "committed_version" || column == &historical_versioned_column_family || column == &live_column_family || column == &pruning_column_family {
				bail!("{} column name is reserved for internal use", column);
			}
		}
		columns.push("committed_version".to_string());
		columns.push(historical_versioned_column_family.to_string());
		columns.push(live_column_family);
		columns.push(pruning_column_family);

		let db = DB::open(path, name, columns, db_opts)?;
		let committed_version = db.get::<CommittedVersion>(&EmptyKey)?.unwrap_or(0);
		let cache = Arc::new(Cache);

		Ok(Self { db, committed_version: Arc::new(AtomicU64::new(committed_version)), cache, schema: Default::default() })
    }

	/// Name of the database that can be used for logging or metrics or tracing.
    #[inline]
    pub fn name(&self) -> &'static str {
        self.db.name()
    }

	pub fn get_live_value(&self, key: &impl KeyCodec<S>) -> anyhow::Result<Option<S::Value>> {
		self.db.get::<S>(key)
	}

	pub fn get_historical_value(&self, key_to_get: &S::Key, version: u64) -> anyhow::Result<Option<S::Value>> {
		let key_with_version = VersionedKey::<S>(key_to_get, version).encode_key()?;
		let range = &key_with_version..;
		let mut iterator = self.db.raw_iter_range::<VersionedSchema<S>>(range, ScanDirection::Backward)?;
        if let Some((key, value_bytes)) = iterator.next() {
			// Safety: All keys are suffixed with an 8-byte version.
			let (key_bytes, version_bytes) = key.split_at(key.len() - 8);
			if key_bytes.len() != key_with_version.len() - 8 || key_bytes != &key_with_version[..key_bytes.len()] {
				return Ok(None);
			}
			debug_assert_eq!(key_to_get, &S::Key::decode_key(&key_bytes).expect("Failed to decode valid key"), "Byte-equal keys must decode to the same type");
            let value = S::Value::decode_value(&value_bytes)?;
            return Ok(Some(value));
        }
		Ok(None)
	}

}
