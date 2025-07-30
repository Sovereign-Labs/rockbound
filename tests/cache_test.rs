// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

//! Tests for cache consistency with the backing RocksDB

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use rockbound::schema::{ColumnFamilyName, Schema};
use rockbound::test::TestField;
use rockbound::{SchemaBatch, DB};
use rocksdb::DEFAULT_COLUMN_FAMILY_NAME;
use tempfile::TempDir;

// Create cached and non-cached schemas for testing
#[derive(Debug, Default)]
pub struct CachedTestSchema;

impl Schema for CachedTestSchema {
    type Key = TestField;
    type Value = TestField;
    const COLUMN_FAMILY_NAME: ColumnFamilyName = "CachedTestCF";
    const SHOULD_CACHE: bool = true;
}

#[derive(Debug, Default)]
pub struct NonCachedTestSchema;

impl Schema for NonCachedTestSchema {
    type Key = TestField;
    type Value = TestField;
    const COLUMN_FAMILY_NAME: ColumnFamilyName = "NonCachedTestCF";
    const SHOULD_CACHE: bool = false;
}

fn get_column_families() -> Vec<ColumnFamilyName> {
    vec![
        DEFAULT_COLUMN_FAMILY_NAME,
        CachedTestSchema::COLUMN_FAMILY_NAME,
        NonCachedTestSchema::COLUMN_FAMILY_NAME,
    ]
}

fn open_db(dir: impl AsRef<std::path::Path>) -> DB {
    let mut db_opts = rocksdb::Options::default();
    db_opts.create_if_missing(true);
    db_opts.create_missing_column_families(true);
    DB::open(dir, "cache_test", get_column_families(), &db_opts).expect("Failed to open DB.")
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

#[test]
fn test_basic_cache_consistency() {
    let db = TestDB::new();

    // Test put operation - cache and DB should both contain the value
    db.put::<CachedTestSchema>(&TestField(1), &TestField(100))
        .unwrap();

    // Read should get from cache
    assert_eq!(
        db.get::<CachedTestSchema>(&TestField(1)).unwrap(),
        Some(TestField(100))
    );

    // Update the value
    db.put::<CachedTestSchema>(&TestField(1), &TestField(200))
        .unwrap();

    // Read should get updated value from cache
    assert_eq!(
        db.get::<CachedTestSchema>(&TestField(1)).unwrap(),
        Some(TestField(200))
    );

    // Delete the value
    db.delete::<CachedTestSchema>(&TestField(1)).unwrap();

    // Read should get None from cache
    assert_eq!(db.get::<CachedTestSchema>(&TestField(1)).unwrap(), None);
}

#[test]
fn test_cache_vs_non_cache_consistency() {
    let db = TestDB::new();

    // Put same key-value in both cached and non-cached schemas
    db.put::<CachedTestSchema>(&TestField(1), &TestField(100))
        .unwrap();
    db.put::<NonCachedTestSchema>(&TestField(1), &TestField(100))
        .unwrap();

    // Both should return the same value
    assert_eq!(
        db.get::<CachedTestSchema>(&TestField(1)).unwrap(),
        db.get::<NonCachedTestSchema>(&TestField(1)).unwrap()
    );

    // Update both
    db.put::<CachedTestSchema>(&TestField(1), &TestField(200))
        .unwrap();
    db.put::<NonCachedTestSchema>(&TestField(1), &TestField(200))
        .unwrap();

    // Both should still return the same value
    assert_eq!(
        db.get::<CachedTestSchema>(&TestField(1)).unwrap(),
        db.get::<NonCachedTestSchema>(&TestField(1)).unwrap()
    );

    // Delete from both
    db.delete::<CachedTestSchema>(&TestField(1)).unwrap();
    db.delete::<NonCachedTestSchema>(&TestField(1)).unwrap();

    // Both should return None
    assert_eq!(
        db.get::<CachedTestSchema>(&TestField(1)).unwrap(),
        db.get::<NonCachedTestSchema>(&TestField(1)).unwrap()
    );
}

#[test]
fn test_batch_operations_cache_consistency() {
    let db = TestDB::new();

    // Create a batch with multiple operations
    let mut batch = SchemaBatch::new();
    batch
        .put::<CachedTestSchema>(&TestField(1), &TestField(100))
        .unwrap();
    batch
        .put::<CachedTestSchema>(&TestField(2), &TestField(200))
        .unwrap();
    batch
        .put::<CachedTestSchema>(&TestField(3), &TestField(300))
        .unwrap();
    batch.delete::<CachedTestSchema>(&TestField(4)).unwrap(); // Delete non-existent key

    db.write_schemas(batch).unwrap();

    // Verify all operations took effect in cache
    assert_eq!(
        db.get::<CachedTestSchema>(&TestField(1)).unwrap(),
        Some(TestField(100))
    );
    assert_eq!(
        db.get::<CachedTestSchema>(&TestField(2)).unwrap(),
        Some(TestField(200))
    );
    assert_eq!(
        db.get::<CachedTestSchema>(&TestField(3)).unwrap(),
        Some(TestField(300))
    );
    assert_eq!(db.get::<CachedTestSchema>(&TestField(4)).unwrap(), None);

    // Create another batch that modifies existing values
    let mut batch2 = SchemaBatch::new();
    batch2
        .put::<CachedTestSchema>(&TestField(1), &TestField(150))
        .unwrap(); // Update
    batch2.delete::<CachedTestSchema>(&TestField(2)).unwrap(); // Delete existing
    batch2
        .put::<CachedTestSchema>(&TestField(5), &TestField(500))
        .unwrap(); // New key

    db.write_schemas(batch2).unwrap();

    // Verify all operations took effect
    assert_eq!(
        db.get::<CachedTestSchema>(&TestField(1)).unwrap(),
        Some(TestField(150))
    );
    assert_eq!(db.get::<CachedTestSchema>(&TestField(2)).unwrap(), None);
    assert_eq!(
        db.get::<CachedTestSchema>(&TestField(3)).unwrap(),
        Some(TestField(300))
    ); // Unchanged
    assert_eq!(
        db.get::<CachedTestSchema>(&TestField(5)).unwrap(),
        Some(TestField(500))
    );
}

/// This test ensures two things:
/// 1. When a write happens, no stale entry is left behind in the cache.
/// 2. Batches are applied atomically regardless of whether the keys are in the cache or not.
///
/// We test this by setting a very small cache (so that some of the working set is in cache and some is not),
/// and spinning up one writer and several readers. The writer repeatedly performs an atomic increment of all keys, while the readers
/// read from different keys and assert that successive read values are non-decreasing. (Decreasing would indicate a bug in the cache)
#[test]
fn test_cache_behavior_under_concurrency() {
    let db = Arc::new(TestDB::new());
    let num_generations = 20;
    let num_keys = 10000;
    let num_readers = 10;

    let barrier = Arc::new(Barrier::new(num_readers + 1));
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Keys/value pairs have weight between 50 and 80 due to overhead. If we set the weight capacity to 50_000, we'll have some keys
    // in cache but not all.
    db.set_cache_size(1000, 50_000);

    let mut handles = vec![];

    // Spawn threads that perform concurrent reads
    for thread_id in 1..=num_readers {
        let db_clone = Arc::clone(&db);
        let done = Arc::clone(&done);
        let barrier = Arc::clone(&barrier);
        let handle = thread::spawn(move || {
            barrier.wait();
            let mut generation = 0;
            let mut current_generation_is_active = true;
            let mut hit_nones = 0;
            while !done.load(std::sync::atomic::Ordering::Relaxed) {
                for key in (0..num_keys).step_by(thread_id) {
                    let key = TestField(key as u32);
                    let value = db_clone.get::<CachedTestSchema>(&key).unwrap();
                    if let Some(value) = value {
                        // We delete in batches. If we deleted some keys, then all keys should have been deleted - so no
                        // more reads from the current generation are allowed
                        if current_generation_is_active {
                            assert!(value.0 >= generation, "Value {} is less than generation {}. A reader went backwards! This means the cache is buggy!", value.0, generation);
                            generation = value.0;
                        } else {
                            assert!(value.0 > generation, "Value {} is not greater than generation {}. A reader went backwards! This means the cache is buggy!", value.0, generation);
                            current_generation_is_active = true;
                            generation = value.0;
                        }
                    } else {
                        hit_nones += 1;
                        current_generation_is_active = false;
                    }
                }
            }
            assert!(
                hit_nones > 1000,
                "Very deleted keys were encountered. This means the test logic is probably buggy"
            );
        });
        handles.push(handle);
    }

    let db_clone = Arc::clone(&db);
    // Spawn the writer
    let writer = thread::spawn(move || {
        // Pre-populate the db before starting the readers
        let mut batch = SchemaBatch::new();
        for key in 0..num_keys {
            let key = TestField(key as u32);
            let value = TestField(0);
            batch.put::<CachedTestSchema>(&key, &value).unwrap();
        }
        db.write_schemas(batch).unwrap();
        barrier.wait();
        for generation in 1..num_generations {
            let mut batch = SchemaBatch::new();
            for key in 0..num_keys {
                let key = TestField(key as u32);
                let value = TestField(generation);
                batch.put::<CachedTestSchema>(&key, &value).unwrap();
            }
            db.write_schemas(batch).unwrap();
            // Sleep for a bit to let the readers churn. This helps ensure that both cached and non-cached keys are read.
            std::thread::sleep(Duration::from_millis(100));
            // Every few generations, delete all keys. This gives us coverage on the `None` case
            if generation % 3 == 0 {
                let mut batch = SchemaBatch::new();
                for key in 0..num_keys {
                    let key = TestField(key as u32);
                    batch.delete::<CachedTestSchema>(&key).unwrap();
                }
                db.write_schemas(batch).unwrap();
                std::thread::sleep(Duration::from_millis(100));
            }
        }
        done.store(true, std::sync::atomic::Ordering::Relaxed);
    });
    handles.push(writer);

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    //  Sanity check that the cache was used and that we didn't set it so large that misses were very rare
    assert!(
        db_clone.cache_hits() > num_keys,
        "Cache was not used. Hits: {}, Misses: {}",
        db_clone.cache_hits(),
        db_clone.cache_misses()
    );
    assert!(
        db_clone.cache_misses() > db_clone.cache_hits(),
        "Cache misses should be greater than cache hits. Cache size is too large"
    );
}
