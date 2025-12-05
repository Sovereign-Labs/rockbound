mod cache;
mod delta_reader;
mod iterator;

use std::sync::Arc;

use rockbound::schema::{ColumnFamilyName, KeyDecoder, KeyEncoder, Schema, ValueCodec};
use rockbound::versioned_db::{
    HasPrefix, SchemaWithVersion, VersionedDB, VersionedSchemaBatch, VersionedSchemaKeyMarker,
};
use rockbound::DB;
use rockbound::{default_cf_descriptor, CodecError};
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
    const SHOULD_CACHE: bool = true;
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

    fn take_tempdir(self) -> TempDir {
        self.tmpdir
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
    versioned_db: &VersionedDB<LiveKeys>,
    batch: &VersionedSchemaBatch<LiveKeys>,
    version: u64,
) {
    versioned_db.commit(batch, version).unwrap();
}

fn put_keys(versioned_db: &VersionedDB<LiveKeys>, keys: &[(&[u8], u32)], version: u64) {
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

fn delete_keys(versioned_db: &VersionedDB<LiveKeys>, keys: &[&[u8]], version: u64) {
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
