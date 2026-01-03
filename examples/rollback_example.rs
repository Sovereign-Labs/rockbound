/// Example demonstrating the rollback functionality for VersionedDB
///
/// This example shows how to:
/// 1. Create a versioned database
/// 2. Commit multiple versions
/// 3. Roll back to a previous version
use std::sync::Arc;

use parking_lot::{MappedRwLockReadGuard, MappedRwLockWriteGuard, RwLock};
use rockbound::schema::{ColumnFamilyName, KeyDecoder, KeyEncoder, Schema, ValueCodec};
use rockbound::versioned_db::{
    CacheForVersionedDB, SchemaWithVersion, VersionedDB, VersionedSchemaBatch,
    VersionedSchemaKeyMarker,
};
use rockbound::{default_cf_descriptor, new_cache_for_schema, CacheForSchema, CodecError, DB};

// Define a simple schema for demonstration
#[derive(Debug, Default, Clone, PartialOrd, Ord, PartialEq, Eq)]
struct MySchema;

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
struct MyKey(Arc<Vec<u8>>);

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
struct MyValue(Vec<u8>);

impl AsRef<[u8]> for MyKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for MyValue {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl VersionedSchemaKeyMarker for MyKey {}

impl KeyEncoder<MySchema> for MyKey {
    fn encode_key(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.as_ref().to_vec())
    }
}

impl KeyDecoder<MySchema> for MyKey {
    fn decode_key(data: &[u8]) -> Result<Self, CodecError> {
        Ok(MyKey(Arc::new(data.to_vec())))
    }
}

impl ValueCodec<MySchema> for MyValue {
    fn encode_value(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.clone())
    }

    fn decode_value(data: &[u8]) -> Result<Self, CodecError> {
        Ok(MyValue(data.to_vec()))
    }
}

impl Schema for MySchema {
    type Key = MyKey;
    type Value = MyValue;
    const COLUMN_FAMILY_NAME: ColumnFamilyName = "my_schema";
}

impl SchemaWithVersion for MySchema {
    const HISTORICAL_COLUMN_FAMILY_NAME: ColumnFamilyName = "my_schema_historical";
    const PRUNING_COLUMN_FAMILY_NAME: ColumnFamilyName = "my_schema_pruning";
    const VERSION_METADATA_COLUMN_FAMILY_NAME: ColumnFamilyName = "my_schema_version_metadata";
}

#[derive(Debug, Clone)]
struct MyCache {
    cache: Arc<RwLock<CacheForSchema<MySchema>>>,
}

impl MyCache {
    fn new(cache_size: usize) -> Self {
        Self {
            cache: Arc::new(RwLock::new(new_cache_for_schema::<MySchema>(cache_size))),
        }
    }
}

impl CacheForVersionedDB<MySchema> for MyCache {
    fn write(&self) -> MappedRwLockWriteGuard<'_, CacheForSchema<MySchema>> {
        use parking_lot::RwLockWriteGuard;
        RwLockWriteGuard::map(self.cache.write(), |c| c)
    }

    fn read(&self) -> MappedRwLockReadGuard<'_, CacheForSchema<MySchema>> {
        use parking_lot::RwLockReadGuard;
        RwLockReadGuard::map(self.cache.read(), |c| c)
    }

    fn try_read(&self) -> Option<MappedRwLockReadGuard<'_, CacheForSchema<MySchema>>> {
        use parking_lot::RwLockReadGuard;
        let lock = self.cache.try_read()?;
        Some(RwLockReadGuard::map(lock, |c| c))
    }
}

fn main() -> anyhow::Result<()> {
    // Create a temporary directory for the database
    let tmpdir = tempfile::tempdir()?;

    // Set up column families
    let mut db_opts = rocksdb::Options::default();
    db_opts.create_if_missing(true);
    db_opts.create_missing_column_families(true);

    let column_families = vec![
        rocksdb::DEFAULT_COLUMN_FAMILY_NAME,
        MySchema::COLUMN_FAMILY_NAME,
        MySchema::HISTORICAL_COLUMN_FAMILY_NAME,
        MySchema::PRUNING_COLUMN_FAMILY_NAME,
        MySchema::VERSION_METADATA_COLUMN_FAMILY_NAME,
    ];

    // Open the database
    let db = DB::open_with_cfds(
        &db_opts,
        tmpdir.path(),
        "example_db",
        column_families.into_iter().map(default_cf_descriptor),
    )?;

    let db_arc = Arc::new(db);
    let cache = MyCache::new(1_000_000);

    // Create versioned DB (using same DB for live and archival for simplicity)
    let versioned_db = VersionedDB::from_dbs(db_arc.clone(), db_arc.clone(), cache)?;

    println!("=== Rollback Example ===\n");

    // Version 1: Insert initial data
    let mut batch = VersionedSchemaBatch::<MySchema>::default();
    batch.put_versioned(
        MyKey(Arc::new(b"user:1".to_vec())),
        MyValue(b"Alice".to_vec()),
    );
    batch.put_versioned(
        MyKey(Arc::new(b"user:2".to_vec())),
        MyValue(b"Bob".to_vec()),
    );
    versioned_db.commit(&batch, 1)?;
    println!("Version 1: Added Alice and Bob");

    // Version 2: Update and add data
    let mut batch = VersionedSchemaBatch::<MySchema>::default();
    batch.put_versioned(
        MyKey(Arc::new(b"user:1".to_vec())),
        MyValue(b"Alice Smith".to_vec()),
    );
    batch.put_versioned(
        MyKey(Arc::new(b"user:3".to_vec())),
        MyValue(b"Charlie".to_vec()),
    );
    versioned_db.commit(&batch, 2)?;
    println!("Version 2: Updated Alice, added Charlie");

    // Check current state
    let user1 = versioned_db.get_live_value(&MyKey(Arc::new(b"user:1".to_vec())))?;
    let user3 = versioned_db.get_live_value(&MyKey(Arc::new(b"user:3".to_vec())))?;
    println!("\nCurrent state (version 2):");
    println!(
        "  user:1 = {:?}",
        user1.as_ref().map(|v| String::from_utf8_lossy(&v.0).to_string())
    );
    println!(
        "  user:3 = {:?}",
        user3.as_ref().map(|v| String::from_utf8_lossy(&v.0).to_string())
    );

    // Rollback to version 1
    println!("\nRolling back to version 1...");
    let new_version = versioned_db.rollback_last_version()?;
    println!("Rolled back to version: {}", new_version);

    // Check state after rollback
    let user1 = versioned_db.get_live_value(&MyKey(Arc::new(b"user:1".to_vec())))?;
    let user3 = versioned_db.get_live_value(&MyKey(Arc::new(b"user:3".to_vec())))?;
    println!("\nState after rollback (version 1):");
    println!(
        "  user:1 = {:?}",
        user1.as_ref().map(|v| String::from_utf8_lossy(&v.0).to_string())
    );
    println!(
        "  user:3 = {:?}",
        user3.as_ref().map(|v| String::from_utf8_lossy(&v.0).to_string())
    );

    println!("\n=== Rollback Complete ===");

    Ok(())
}
