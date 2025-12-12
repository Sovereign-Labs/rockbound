use std::{
    sync::{Arc, Barrier},
    time::Duration,
};

use rockbound::versioned_db::{VersionedDB, VersionedSchemaBatch};

#[cfg(feature = "test-utils")]
use crate::versioned_db_inner::TestDB;
use crate::versioned_db_inner::{delete_keys, put_keys, LiveKeys, TestField, TestKey};

#[test]
fn test_basic_cache_consistency() {
    let test_db = TestDB::new();
    let db = Arc::new(test_db.db);
    let versioned_db =
        Arc::new(VersionedDB::<LiveKeys>::from_dbs(db.clone(), db.clone(), 10_000).unwrap());

    // Put key1 at version 0
    put_keys(&versioned_db, &[(b"key1", 100)], 0);
    let key1 = versioned_db
        .get_historical_value(&TestKey::from(b"key1".to_vec()), 0)
        .unwrap();
    assert_eq!(key1, Some(TestField::new(100)));

    // Put key1 at version 1
    put_keys(&versioned_db, &[(b"key1", 200)], 1);
    let key1_at_0 = versioned_db
        .get_historical_value(&TestKey::from(b"key1".to_vec()), 0)
        .unwrap();
    let key1_at_1 = versioned_db
        .get_historical_value(&TestKey::from(b"key1".to_vec()), 1)
        .unwrap();
    assert_eq!(key1_at_0, Some(TestField::new(100)));
    assert_eq!(key1_at_1, Some(TestField::new(200)));

    // put key1 at version 2
    delete_keys(&versioned_db, &[b"key1"], 2);
    let key1_at_0 = versioned_db
        .get_historical_value(&TestKey::from(b"key1".to_vec()), 0)
        .unwrap();
    let key1_at_1 = versioned_db
        .get_historical_value(&TestKey::from(b"key1".to_vec()), 1)
        .unwrap();
    let key1_at_2 = versioned_db
        .get_historical_value(&TestKey::from(b"key1".to_vec()), 2)
        .unwrap();
    assert_eq!(key1_at_0, Some(TestField::new(100)));
    assert_eq!(key1_at_1, Some(TestField::new(200)));
    assert_eq!(key1_at_2, None);
}

fn make_key(key: usize) -> TestKey {
    let length = (key % 1000) + 1; // Offset by 1 to ensure non-zero key length
    let value: u8 = (key / 1000)
        .try_into()
        .expect("Key is too large; update the make_key function to handle larger keys");
    let key: Vec<u8> = std::iter::repeat_n(value, length).collect();
    TestKey::from(key)
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
    let test_db = TestDB::new();
    let db = Arc::new(test_db.db);
    let versioned_db =
        Arc::new(VersionedDB::<LiveKeys>::from_dbs(db.clone(), db.clone(), 10_000).unwrap());
    let num_generations = 20;
    let num_keys = 10000;
    let num_readers = 10;

    let barrier = Arc::new(Barrier::new(num_readers + 1));
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Keys/value pairs have weight between 110 and 140 due to overhead. If we set the weight capacity to 50_000, we'll have some keys
    // in cache but not all.
    versioned_db.set_cache_size(1000, 50_000);

    let mut handles = vec![];

    // Spawn threads that perform concurrent reads
    for thread_id in 1..=num_readers {
        let versioned_db = Arc::clone(&versioned_db);
        let done = Arc::clone(&done);
        let barrier = Arc::clone(&barrier);
        let handle = std::thread::spawn(move || {
            barrier.wait();
            let mut generation = 0;
            let mut current_generation_is_active = true;
            let mut hit_nones = 0;
            while !done.load(std::sync::atomic::Ordering::Relaxed) {
                for key in (0..num_keys).step_by(thread_id) {
                    let key = make_key(key);
                    let value = versioned_db.get_live_value(&key).expect("DbError");
                    if let Some(value) = value {
                        // We delete in batches. If we deleted some keys, then all keys should have been deleted - so no
                        // more reads from the current generation are allowed
                        if current_generation_is_active {
                            assert!(value.value() >= generation, "Value {} is less than generation {}. A reader went backwards! This means the cache is buggy!", value.value(), generation);
                            generation = value.value();
                        } else {
                            assert!(value.value() > generation, "Value {} is not greater than generation {}. A reader went backwards! This means the cache is buggy!", value.value(), generation);
                            current_generation_is_active = true;
                            generation = value.value();
                        }
                    } else {
                        hit_nones += 1;
                        current_generation_is_active = false;
                    }
                }
            }
            assert!(
                hit_nones > 1000,
                "Very few deleted keys were encountered. This means the test logic is probably buggy"
            );
        });
        handles.push(handle);
    }

    let versioned_db_clone = Arc::clone(&versioned_db);
    // Spawn the writer
    let writer = std::thread::spawn(move || {
        let versioned_db = versioned_db_clone;
        // Pre-populate the db before starting the readers
        let mut batch = VersionedSchemaBatch::<LiveKeys>::default();
        for key in 0..num_keys {
            let key = make_key(key);
            let value = TestField::new(0);
            batch.put_versioned(key, value);
        }
        versioned_db.commit(&batch, 0).unwrap();
        barrier.wait();
        for generation in 1..num_generations {
            let mut batch = VersionedSchemaBatch::<LiveKeys>::default();
            for key in 0..num_keys {
                let key = make_key(key);
                let value = TestField::new(generation);
                batch.put_versioned(key, value);
            }
            println!("Committing generation {generation}");
            versioned_db.commit(&batch, generation as u64).unwrap();
            // Sleep for a bit to let the readers churn. This helps ensure that both cached and non-cached keys are read.
            std::thread::sleep(Duration::from_millis(100));
            // Every few generations, delete all keys. This gives us coverage on the `None` case
            if generation % 3 == 0 {
                let mut batch = VersionedSchemaBatch::<LiveKeys>::default();
                for key in 0..num_keys {
                    let key = make_key(key);
                    batch.delete_versioned(key);
                }
                versioned_db.commit(&batch, generation as u64).unwrap();
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
        versioned_db.cache_hits() > num_keys as u64,
        "Cache was not used. Hits: {}, Misses: {}",
        versioned_db.cache_hits(),
        versioned_db.cache_misses()
    );
    assert!(
        versioned_db.cache_misses() > versioned_db.cache_hits(),
        "Cache misses should be greater than cache hits. Cache size is too large"
    );
}
