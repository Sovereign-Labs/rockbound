// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

//! Tests for cache consistency with the backing RocksDB

use std::sync::Arc;

use rockbound::schema::{ColumnFamilyName, KeyDecoder, KeyEncoder, Schema, ValueCodec};
use rockbound::test::TestField;
use rockbound::versioned_db::{
    PrunableKey, SchemaWithVersion, VersionedDB, VersionedDeltaReader, VersionedKey,
    VersionedSchemaBatch, VersionedTableMetadataKey,
};
use rockbound::{default_cf_descriptor, CodecError};
use rockbound::{SchemaBatch, DB};
use rocksdb::DEFAULT_COLUMN_FAMILY_NAME;
use tempfile::TempDir;

// Create cached and non-cached schemas for testing
#[derive(Debug, Default, Clone)]
pub struct LiveKeys;

impl Schema for LiveKeys {
    type Key = Arc<Vec<u8>>;
    type Value = TestField;
    const COLUMN_FAMILY_NAME: ColumnFamilyName = "LiveKeysCF";
    const SHOULD_CACHE: bool = true;
}

impl SchemaWithVersion for LiveKeys {
    type HistoricalColumnFamily = HistoricalKeys;
    type PruningColumnFamily = PruningKeys;
    type VersionMetadatacolumn = VersionMetadata;
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

#[derive(Debug, Default)]
pub struct PruningKeys;

impl Schema for PruningKeys {
    type Key = PrunableKey<LiveKeys, Arc<Vec<u8>>>;
    type Value = ();
    const COLUMN_FAMILY_NAME: ColumnFamilyName = "PruningKeysCF";
    const SHOULD_CACHE: bool = false;
}

impl ValueCodec<PruningKeys> for () {
    fn encode_value(&self) -> Result<Vec<u8>, CodecError> {
        Ok(vec![])
    }
    fn decode_value(_data: &[u8]) -> Result<Self, CodecError> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct VersionMetadata;

impl Schema for VersionMetadata {
    type Key = VersionedTableMetadataKey;
    type Value = u64;
    const COLUMN_FAMILY_NAME: ColumnFamilyName = "VersionMetadataCF";
    const SHOULD_CACHE: bool = false;
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

#[derive(Debug, Default)]
pub struct HistoricalKeys;

impl Schema for HistoricalKeys {
    type Key = VersionedKey<LiveKeys, Arc<Vec<u8>>>;
    type Value = TestField;
    const COLUMN_FAMILY_NAME: ColumnFamilyName = "HistoricalKeysCF";
    const SHOULD_CACHE: bool = false;
}

fn get_column_families() -> Vec<ColumnFamilyName> {
    vec![
        DEFAULT_COLUMN_FAMILY_NAME,
        LiveKeys::COLUMN_FAMILY_NAME,
        PruningKeys::COLUMN_FAMILY_NAME,
        VersionMetadata::COLUMN_FAMILY_NAME,
        HistoricalKeys::COLUMN_FAMILY_NAME,
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
        vec![LiveKeys::COLUMN_FAMILY_NAME.into()],
        1_000_000,
    )
    .expect("Failed to open DB.") // Use 1 MB cache for testing
}

struct TestDB {
    _tmpdir: TempDir,
    db: DB,
}

impl TestDB {
    fn new() -> Self {
        let tmpdir = tempfile::tempdir().unwrap();
        let db = open_db(&tmpdir);

        TestDB {
            _tmpdir: tmpdir,
            db,
        }
    }
}

impl std::ops::Deref for TestDB {
    type Target = DB;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

fn check_iterator(
    delta_reader: &VersionedDeltaReader<LiveKeys>,
    prefix: &[u8],
    expected_values: &[(&[u8], u32)],
) {
    let mut iter = delta_reader
        .iter_with_prefix(Arc::new(prefix.to_vec()))
        .unwrap();
    for (key, value) in expected_values {
        let next = iter.next();
        let next = next.unwrap_or_else(|| {
            panic!(
                "Expected key {} with value {} in iterator, but got None",
                std::str::from_utf8(*key).unwrap(),
                value
            )
        });
        assert_eq!(
            next.0,
            Arc::new(key.to_vec()),
            "Expected key {} with value {} in iterator, but got key {}",
            std::str::from_utf8(*key).unwrap(),
            value,
            std::str::from_utf8(&next.0).unwrap()
        );
        assert_eq!(
            next.1,
            Some(TestField(*value as u32)),
            "Expected value {} for key {} in iterator, but got {}",
            value,
            std::str::from_utf8(*key).unwrap(),
            next.1.unwrap().0
        );
    }
    assert_eq!(iter.next(), None);
}

fn commit_batch(
    versioned_db: &VersionedDB<LiveKeys>,
    batch: &VersionedSchemaBatch<LiveKeys>,
    version: u64,
) {
    let mut live_keys_batch = SchemaBatch::new();
    let mut historical_keys_batch = SchemaBatch::new();

    versioned_db
        .materialize(
            &batch,
            &mut live_keys_batch,
            &mut historical_keys_batch,
            version,
        )
        .unwrap();
    versioned_db
        .live_db()
        .write_schemas(live_keys_batch)
        .unwrap();
    versioned_db
        .archival_db()
        .write_schemas(historical_keys_batch)
        .unwrap();
}

fn put_keys(versioned_db: &VersionedDB<LiveKeys>, keys: &[(&[u8], u32)], version: u64) {
    let mut batch = VersionedSchemaBatch::<LiveKeys>::default();
    for (key, value) in keys {
        batch.put_versioned(Arc::new(key.to_vec()), TestField(*value as u32));
    }

    commit_batch(versioned_db, &batch, version);

    for (key, value) in keys {
        assert_eq!(
            versioned_db
                .get_live_value(&Arc::new(key.to_vec()).encode_key().unwrap())
                .unwrap(),
            Some(TestField(*value as u32))
        );
        assert_eq!(
            versioned_db
                .get_historical_value(&Arc::new(key.to_vec()), version)
                .unwrap(),
            Some(TestField(*value as u32))
        );
    }
}

#[test]
fn test_iteration() {
    let test_db = TestDB::new();
    let db = Arc::new(test_db.db);
    let versioned_db = VersionedDB::<LiveKeys>::from_dbs(db.clone(), db.clone()).unwrap();

    // Check iteration against an empty DB.
    let version = versioned_db.get_committed_version().unwrap();
    assert_eq!(version, None);
    let delta_reader = VersionedDeltaReader::<LiveKeys>::new(versioned_db.clone(), None, vec![]);
    let mut iter = delta_reader
        .iter_with_prefix(Arc::new(b"key".to_vec()))
        .unwrap();
    assert_eq!(iter.next(), None);
    drop(iter);

    put_keys(
        &versioned_db,
        &[(b"key11", 0), (b"key19", 0), (b"key20", 0)],
        0,
    );

    // Check iteration against prefixes "key". and key1
    let delta_reader = VersionedDeltaReader::<LiveKeys>::new(versioned_db.clone(), Some(0), vec![]);
    check_iterator(
        &delta_reader,
        b"key",
        &[(b"key11", 0), (b"key19", 0), (b"key20", 0)],
    );
    check_iterator(&delta_reader, b"key1", &[(b"key11", 0), (b"key19", 0)]);

    let mut snapshot = VersionedSchemaBatch::<LiveKeys>::default();
    snapshot.put_versioned(Arc::new(b"key12".to_vec()), TestField(1));
    snapshot.put_versioned(Arc::new(b"key19".to_vec()), TestField(1));

    let snapshot_1 = Arc::new(snapshot);
    let delta_reader = VersionedDeltaReader::<LiveKeys>::new(
        versioned_db.clone(),
        Some(0),
        vec![snapshot_1.clone()],
    );
    check_iterator(
        &delta_reader,
        b"key",
        &[(b"key11", 0), (b"key12", 1), (b"key19", 1), (b"key20", 0)],
    );
    check_iterator(
        &delta_reader,
        b"key1",
        &[(b"key11", 0), (b"key12", 1), (b"key19", 1)],
    );

    let mut snapshot = VersionedSchemaBatch::<LiveKeys>::default();
    snapshot.put_versioned(Arc::new(b"key12".to_vec()), TestField(2));
    snapshot.delete_versioned(Arc::new(b"key19".to_vec()));

    let snapshot_2 = Arc::new(snapshot);
    let delta_reader = VersionedDeltaReader::<LiveKeys>::new(
        versioned_db.clone(),
        Some(0),
        vec![snapshot_2.clone(), snapshot_1.clone()],
    );
    check_iterator(
        &delta_reader,
        b"key",
        &[(b"key11", 0), (b"key12", 2), (b"key20", 0)],
    );
    check_iterator(&delta_reader, b"key1", &[(b"key11", 0), (b"key12", 2)]);

    // Committing the oldest snapshot should not change the iterator.
    commit_batch(&versioned_db, &snapshot_1, 1);
    check_iterator(
        &delta_reader,
        b"key",
        &[(b"key11", 0), (b"key12", 2), (b"key20", 0)],
    );
    check_iterator(&delta_reader, b"key1", &[(b"key11", 0), (b"key12", 2)]);

    // Committing the second snapshot should not change the iterator.
    commit_batch(&versioned_db, &snapshot_2, 2);
    check_iterator(
        &delta_reader,
        b"key",
        &[(b"key11", 0), (b"key12", 2), (b"key20", 0)],
    );
    check_iterator(&delta_reader, b"key1", &[(b"key11", 0), (b"key12", 2)]);

    // Commiting another batch should make it impossible to create an iterator
    commit_batch(
        &versioned_db,
        &VersionedSchemaBatch::<LiveKeys>::default(),
        3,
    );
    assert!(delta_reader
        .iter_with_prefix(Arc::new(b"key".to_vec()))
        .is_err());
}

#[test]
fn test_open_iterator_blocks_writes() {
    let test_db = TestDB::new();
    let db = Arc::new(test_db.db);
    let versioned_db = VersionedDB::<LiveKeys>::from_dbs(db.clone(), db.clone()).unwrap();

    let delta_reader = VersionedDeltaReader::<LiveKeys>::new(versioned_db.clone(), None, vec![]);

    let mut iter = delta_reader
        .iter_with_prefix(Arc::new(b"key".to_vec()))
        .unwrap();

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        tx.send("starting").unwrap();
        commit_batch(
            &versioned_db,
            &VersionedSchemaBatch::<LiveKeys>::default(),
            0,
        );
        tx.send("finished").unwrap();
    });

    // Block until the background thread has sent its message, then sleep to ensure it has time to run if it's not blocked
    assert!(rx.recv().unwrap() == "starting");
    std::thread::sleep(std::time::Duration::from_secs(5));
    // Assert that it didn't finish. Then drop the iterator to release the read lock and ensure that it completes.
    assert!(rx.try_recv().is_err());
    assert!(iter.next().is_none());
    drop(iter);
    assert!(rx.recv().unwrap() == "finished");
}
