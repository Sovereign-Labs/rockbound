mod cache;
mod delta_reader;
mod empty_value;
mod iterator;
mod pruning;

use std::sync::Arc;

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use rockbound::schema::{ColumnFamilyName, KeyDecoder, KeyEncoder, Schema, ValueCodec};
use rockbound::versioned_db::{
    CacheForVersionedDB, HasPrefix, SchemaWithVersion, VersionedDB, VersionedSchemaBatch,
    VersionedSchemaKeyMarker,
};
use rockbound::{default_cf_descriptor, new_cache_for_schema, CodecError};
use rockbound::{CacheForSchema, DB};
use rocksdb::DEFAULT_COLUMN_FAMILY_NAME;
use tempfile::TempDir;

#[derive(Debug, Default, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
pub struct TestField([u8; 4]);
impl TestField {
    pub fn new(value: u32) -> Self {
        Self(value.to_be_bytes())
    }

    pub fn value(&self) -> u32 {
        u32::from_be_bytes(self.0)
    }
}

impl<S: Schema> ValueCodec<S> for TestField {
    fn encode_value(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.to_vec())
    }

    fn decode_value(data: &[u8]) -> Result<Self, CodecError> {
        Ok(Self(data.try_into().map_err(|_| {
            CodecError::InvalidKeyLength {
                expected: 4,
                got: data.len(),
            }
        })?))
    }
}

impl std::fmt::Display for TestField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", u32::from_le_bytes(self.0))
    }
}

impl AsRef<[u8]> for TestField {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Hash, Default)]
pub struct TestKey(Arc<Vec<u8>>);

impl From<Vec<u8>> for TestKey {
    fn from(key: Vec<u8>) -> Self {
        Self(Arc::new(key))
    }
}

impl From<Arc<Vec<u8>>> for TestKey {
    fn from(key: Arc<Vec<u8>>) -> Self {
        Self(key)
    }
}

impl AsRef<[u8]> for TestKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl KeyEncoder<LiveKeys> for TestKey {
    fn encode_key(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.as_ref().to_vec())
    }
}

impl KeyDecoder<LiveKeys> for TestKey {
    fn decode_key(data: &[u8]) -> Result<Self, CodecError> {
        Ok(TestKey::from(data.to_vec()))
    }
}

// Create cached and non-cached schemas for testing
#[derive(Debug, Default, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct LiveKeys;

impl Schema for LiveKeys {
    type Key = TestKey;
    type Value = TestField;
    const COLUMN_FAMILY_NAME: ColumnFamilyName = "LiveKeysCF";
}

impl VersionedSchemaKeyMarker for TestKey {}

impl SchemaWithVersion for LiveKeys {
    const HISTORICAL_COLUMN_FAMILY_NAME: ColumnFamilyName = "HistoricalKeysCF";
    const PRUNING_COLUMN_FAMILY_NAME: ColumnFamilyName = "PruningKeysCF";
    const VERSION_METADATA_COLUMN_FAMILY_NAME: ColumnFamilyName = "VersionMetadataCF";
}

impl KeyDecoder<LiveKeys> for Arc<Vec<u8>> {
    fn decode_key(data: &[u8]) -> Result<Self, CodecError> {
        Ok(Arc::new(data.to_vec()))
    }
}

impl KeyEncoder<LiveKeys> for Vec<u8> {
    fn encode_key(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.clone())
    }
}

impl KeyDecoder<LiveKeys> for Vec<u8> {
    fn decode_key(data: &[u8]) -> Result<Self, CodecError> {
        Ok(data.to_vec())
    }
}

fn get_column_families() -> Vec<ColumnFamilyName> {
    vec![
        DEFAULT_COLUMN_FAMILY_NAME,
        LiveKeys::COLUMN_FAMILY_NAME,
        LiveKeys::HISTORICAL_COLUMN_FAMILY_NAME,
        LiveKeys::PRUNING_COLUMN_FAMILY_NAME,
        LiveKeys::VERSION_METADATA_COLUMN_FAMILY_NAME,
        ByteVecSchema::COLUMN_FAMILY_NAME,
        ByteVecSchema::HISTORICAL_COLUMN_FAMILY_NAME,
        ByteVecSchema::PRUNING_COLUMN_FAMILY_NAME,
        ByteVecSchema::VERSION_METADATA_COLUMN_FAMILY_NAME,
    ]
}

fn open_db(dir: impl AsRef<std::path::Path>) -> DB {
    let mut db_opts = rocksdb::Options::default();
    db_opts.create_if_missing(true);
    db_opts.create_missing_column_families(true);
    DB::open_with_cfds(
        &db_opts,
        dir,
        "cache_test",
        get_column_families().into_iter().map(default_cf_descriptor),
    )
    .expect("Failed to open DB.") // Use 1 MB cache for testing
}

struct TestDB {
    tmpdir: TempDir,
    db: DB,
}

impl TestDB {
    fn new() -> Self {
        let tmpdir = tempfile::tempdir().unwrap();
        let db = open_db(&tmpdir);

        TestDB { tmpdir, db }
    }

    fn from_tempdir(tmpdir: TempDir) -> Self {
        let db = open_db(&tmpdir);
        TestDB { tmpdir, db }
    }
}

impl std::ops::Deref for TestDB {
    type Target = DB;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl HasPrefix<TestKey> for TestKey {
    fn has_prefix(&self, prefix: &TestKey) -> bool {
        self.0.as_ref().starts_with(prefix.0.as_ref())
    }
}

fn commit_batch(
    versioned_db: &VersionedDB<LiveKeys, VersionedDbCache<LiveKeys>>,
    batch: &VersionedSchemaBatch<LiveKeys>,
    version: u64,
) {
    versioned_db.commit(batch, version).unwrap();
}

fn put_keys(
    versioned_db: &VersionedDB<LiveKeys, VersionedDbCache<LiveKeys>>,
    keys: &[(&[u8], u32)],
    version: u64,
) {
    let mut batch = VersionedSchemaBatch::<LiveKeys>::default();
    for (key, value) in keys {
        batch.put_versioned(TestKey::from(key.to_vec()), TestField::new(*value));
    }

    commit_batch(versioned_db, &batch, version);

    for (key, value) in keys {
        let key = TestKey::from(key.to_vec());
        assert_eq!(
            versioned_db.get_live_value(&key).unwrap(),
            Some(TestField::new(*value))
        );
        assert_eq!(
            versioned_db.get_historical_value(&key, version).unwrap(),
            Some(TestField::new(*value))
        );
    }
}

fn delete_keys(
    versioned_db: &VersionedDB<LiveKeys, VersionedDbCache<LiveKeys>>,
    keys: &[&[u8]],
    version: u64,
) {
    let mut batch = VersionedSchemaBatch::<LiveKeys>::default();
    for key in keys {
        batch.delete_versioned(TestKey::from(key.to_vec()));
    }

    commit_batch(versioned_db, &batch, version);

    for key in keys {
        let key = TestKey::from(key.to_vec());
        assert_eq!(versioned_db.get_live_value(&key).unwrap(), None,);
        assert_eq!(
            versioned_db.get_historical_value(&key, version).unwrap(),
            None,
        );
    }
}

/// Cache for versioned db;
#[derive(Debug, Clone)]
pub struct VersionedDbCache<S: SchemaWithVersion> {
    cache: Arc<RwLock<CacheForSchema<S>>>,
}

impl<S: SchemaWithVersion> VersionedDbCache<S>
where
    S::Key: Ord + Clone + std::hash::Hash + AsRef<[u8]>,
    S::Value: Clone + AsRef<[u8]>,
    S: Ord,
{
    pub fn new(cache_size: usize) -> Self {
        let cache = RwLock::new(new_cache_for_schema::<S>(cache_size));
        Self {
            cache: cache.into(),
        }
    }
}

impl CacheForVersionedDB<LiveKeys> for VersionedDbCache<LiveKeys> {
    fn write(&self) -> parking_lot::MappedRwLockWriteGuard<'_, CacheForSchema<LiveKeys>> {
        RwLockWriteGuard::map(self.cache.write(), |c| c)
    }

    fn read(&self) -> parking_lot::MappedRwLockReadGuard<'_, CacheForSchema<LiveKeys>> {
        RwLockReadGuard::map(self.cache.read(), |c| c)
    }

    fn try_read(&self) -> Option<parking_lot::MappedRwLockReadGuard<'_, CacheForSchema<LiveKeys>>> {
        let lock = self.cache.try_read()?;
        Some(RwLockReadGuard::map(lock, |c| c))
    }
}

/// Variable-length value type for exercising edge cases in the versioned-db
/// encoding. Unlike `TestField` (fixed 4 bytes), this can round-trip payloads
/// of arbitrary length including zero, which is needed to verify that
/// `put_versioned` rejects empty values.
#[derive(Debug, Default, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct TestByteVec(pub Vec<u8>);

impl AsRef<[u8]> for TestByteVec {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl<S: Schema> ValueCodec<S> for TestByteVec {
    fn encode_value(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.clone())
    }

    fn decode_value(data: &[u8]) -> Result<Self, CodecError> {
        Ok(Self(data.to_vec()))
    }
}

#[derive(Debug, Default, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct ByteVecSchema;

impl Schema for ByteVecSchema {
    type Key = TestKey;
    type Value = TestByteVec;
    const COLUMN_FAMILY_NAME: ColumnFamilyName = "ByteVecLiveCF";
}

impl SchemaWithVersion for ByteVecSchema {
    const HISTORICAL_COLUMN_FAMILY_NAME: ColumnFamilyName = "ByteVecHistoricalCF";
    const PRUNING_COLUMN_FAMILY_NAME: ColumnFamilyName = "ByteVecPruningCF";
    const VERSION_METADATA_COLUMN_FAMILY_NAME: ColumnFamilyName = "ByteVecVersionMetadataCF";
}

impl KeyEncoder<ByteVecSchema> for TestKey {
    fn encode_key(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.as_ref().to_vec())
    }
}

impl KeyDecoder<ByteVecSchema> for TestKey {
    fn decode_key(data: &[u8]) -> Result<Self, CodecError> {
        Ok(TestKey::from(data.to_vec()))
    }
}

impl CacheForVersionedDB<ByteVecSchema> for VersionedDbCache<ByteVecSchema> {
    fn write(&self) -> parking_lot::MappedRwLockWriteGuard<'_, CacheForSchema<ByteVecSchema>> {
        RwLockWriteGuard::map(self.cache.write(), |c| c)
    }

    fn read(&self) -> parking_lot::MappedRwLockReadGuard<'_, CacheForSchema<ByteVecSchema>> {
        RwLockReadGuard::map(self.cache.read(), |c| c)
    }

    fn try_read(
        &self,
    ) -> Option<parking_lot::MappedRwLockReadGuard<'_, CacheForSchema<ByteVecSchema>>> {
        let lock = self.cache.try_read()?;
        Some(RwLockReadGuard::map(lock, |c| c))
    }
}

/// Creates an on-disk `VersionedDB` backed by `ByteVecSchema`. The returned
/// `TempDir` must be kept alive for the lifetime of the returned `VersionedDB`;
/// callers typically bind it to a `_tmpdir` local.
pub fn setup_bytevec_db() -> (
    TempDir,
    Arc<VersionedDB<ByteVecSchema, VersionedDbCache<ByteVecSchema>>>,
) {
    let tmpdir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_db(&tmpdir));
    let cache = VersionedDbCache::<ByteVecSchema>::new(1_000);
    let vdb = Arc::new(
        VersionedDB::<ByteVecSchema, VersionedDbCache<ByteVecSchema>>::from_dbs(
            db.clone(),
            db.clone(),
            cache,
        )
        .unwrap(),
    );
    (tmpdir, vdb)
}
