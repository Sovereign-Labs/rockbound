// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

//! Tests for cache consistency with the backing RocksDB

use std::sync::Arc;

use rockbound::versioned_db::{VersionedDB, VersionedDeltaReader, VersionedSchemaBatch};

use crate::versioned_db_inner::{commit_batch, put_keys, TestDB, VersionedDbCache};

use super::{LiveKeys, TestField, TestKey};

fn check_keys(
    delta_reader: &VersionedDeltaReader<LiveKeys, VersionedDbCache<LiveKeys>>,
    expected_values: &[(&[u8], Option<u32>)],
) {
    for (key, expected_value) in expected_values {
        let key = TestKey::from(key.to_vec());
        let value = delta_reader.get_latest_borrowed(&key).unwrap();

        assert_eq!(
            &value.map(|v| v.value()),
            expected_value,
            "Expected key {} value {:?} but got value {:?}",
            std::str::from_utf8(key.as_ref()).unwrap(),
            expected_value,
            value.map(|v| v.value()),
        );
    }
}

/// Test that the delta reader always returns the correct values even when the underlying DB is being modified.
#[test]
fn test_delta_reader_consistency_single_threaded() {
    let test_db = TestDB::new();
    let db = Arc::new(test_db.db);
    let versioned_db_cache = VersionedDbCache::new(10_000);
    let versioned_db = Arc::new(
        VersionedDB::<LiveKeys, VersionedDbCache<LiveKeys>>::from_dbs(
            db.clone(),
            db.clone(),
            versioned_db_cache,
        )
        .unwrap(),
    );

    // Check iteration against an empty DB.
    let version = versioned_db.get_committed_version().unwrap();
    assert_eq!(version, None);

    put_keys(
        &versioned_db,
        &[(b"key11", 0), (b"key19", 0), (b"key20", 0)],
        0,
    );
    let delta_reader = VersionedDeltaReader::<LiveKeys, VersionedDbCache<LiveKeys>>::new(
        versioned_db.clone(),
        Some(0),
        vec![],
    );
    check_keys(
        &delta_reader,
        &[
            (b"key11", Some(0)),
            (b"key19", Some(0)),
            (b"key20", Some(0)),
        ],
    );

    let mut snapshot = VersionedSchemaBatch::<LiveKeys>::default();
    snapshot.put_versioned(TestKey::from(b"key12".to_vec()), TestField::new(1));
    snapshot.put_versioned(TestKey::from(b"key19".to_vec()), TestField::new(1));

    let snapshot_1 = Arc::new(snapshot);
    let delta_reader = VersionedDeltaReader::<LiveKeys, VersionedDbCache<LiveKeys>>::new(
        versioned_db.clone(),
        Some(0),
        vec![snapshot_1.clone()],
    );
    check_keys(
        &delta_reader,
        &[
            (b"key11", Some(0)),
            (b"key12", Some(1)),
            (b"key19", Some(1)),
            (b"key20", Some(0)),
        ],
    );

    let mut snapshot = VersionedSchemaBatch::<LiveKeys>::default();
    snapshot.put_versioned(TestKey::from(b"key12".to_vec()), TestField::new(2));
    snapshot.delete_versioned(TestKey::from(b"key19".to_vec()));

    let snapshot_2 = Arc::new(snapshot);
    let delta_reader = VersionedDeltaReader::<LiveKeys, VersionedDbCache<LiveKeys>>::new(
        versioned_db.clone(),
        Some(0),
        vec![snapshot_1.clone(), snapshot_2.clone()],
    );
    println!("Checking keys with snapshot 2");
    check_keys(
        &delta_reader,
        &[
            (b"key11", Some(0)),
            (b"key12", Some(2)),
            (b"key19", None),
            (b"key20", Some(0)),
        ],
    );

    // Committing the oldest snapshot should not change the iterator.
    commit_batch(&versioned_db, &snapshot_1, 1);
    check_keys(
        &delta_reader,
        &[
            (b"key11", Some(0)),
            (b"key12", Some(2)),
            (b"key19", None),
            (b"key20", Some(0)),
        ],
    );

    // Committing the second snapshot should not change the iterator.
    commit_batch(&versioned_db, &snapshot_2, 2);
    check_keys(
        &delta_reader,
        &[
            (b"key11", Some(0)),
            (b"key12", Some(2)),
            (b"key19", None),
            (b"key20", Some(0)),
        ],
    );

    // Commiting another batch should not change the the values read from the existing delta reader.
    let mut snapshot = VersionedSchemaBatch::<LiveKeys>::default();
    snapshot.put_versioned(TestKey::from(b"key11".to_vec()), TestField::new(3));
    snapshot.put_versioned(TestKey::from(b"key12".to_vec()), TestField::new(3));
    commit_batch(&versioned_db, &snapshot, 3);
    check_keys(
        &delta_reader,
        &[
            (b"key11", Some(0)),
            (b"key12", Some(2)),
            (b"key19", None),
            (b"key20", Some(0)),
        ],
    );
}

fn make_key(key: usize) -> TestKey {
    let length = (key % 1000) + 1; // Offset by 1 to ensure non-zero key length
    let value: u8 = (key / 1000)
        .try_into()
        .expect("Key is too large; update the make_key function to handle larger keys");
    let key: Vec<u8> = std::iter::repeat_n(value, length).collect();
    TestKey::from(key)
}

/// This test spawns 100 concurrent reads and single writer to check that the delta readers fall back to historical state smoothly under concurrency.
#[test]
fn test_delta_reader_behavior_under_concurrency() {
    let test_db = TestDB::new();
    let db = Arc::new(test_db.db);

    let versioned_db_cache = VersionedDbCache::new(10_000);
    let versioned_db = Arc::new(
        VersionedDB::<LiveKeys, VersionedDbCache<LiveKeys>>::from_dbs(
            db.clone(),
            db.clone(),
            versioned_db_cache,
        )
        .unwrap(),
    );
    let num_keys = 500;
    let num_readers = 100;

    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Keys/value pairs have weight between 110 and 140 due to overhead. If we set the weight capacity to 50_000, we'll have some keys
    // in cache but not all.
    versioned_db.set_cache_size(1000, 50_000);

    let mut handles = vec![];

    let mut batches = vec![];
    // Spawn threads that perform concurrent reads
    // We offset the thread ID by 1 to ensure that there are some keys that are not written by the first thread; this makes
    // sure that we have coverage on falling all the way down to the DB.
    for thread_id in 2..=(num_readers + 1) {
        let mut batch: VersionedSchemaBatch<LiveKeys> = VersionedSchemaBatch::<LiveKeys>::default();
        for key in (0..num_keys).step_by(thread_id) {
            let key = make_key(key);
            let value = TestField::new(thread_id as u32);
            batch.put_versioned(key, value);
        }
        batches.push(Arc::new(batch));

        let versioned_db = Arc::clone(&versioned_db);
        let done = Arc::clone(&done);
        let batches = batches.clone();
        let handle = std::thread::spawn(move || {
            let delta_reader = VersionedDeltaReader::<LiveKeys, VersionedDbCache<LiveKeys>>::new(
                versioned_db.clone(),
                None,
                batches.clone(),
            );
            while !done.load(std::sync::atomic::Ordering::Relaxed) {
                for key in 0..num_keys {
                    let expected_value = 'expected_value: {
                        for i in (2..=thread_id).rev() {
                            if key % i == 0 {
                                break 'expected_value Some(i as u32);
                            }
                        }
                        None
                    };
                    let key = make_key(key);
                    let value = delta_reader.get_latest_borrowed(&key).unwrap();
                    assert_eq!(value.map(|v| v.value()), expected_value);
                }
            }
        });
        handles.push(handle);
    }

    // Spawn the writer
    let writer = std::thread::spawn(move || {
        for (idx, batch) in batches.into_iter().enumerate() {
            println!("Committing generation {idx}");
            versioned_db.commit(&batch, idx as u64).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        done.store(true, std::sync::atomic::Ordering::Relaxed);
    });
    handles.push(writer);

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
}

/// Test that the delta reader always returns the correct values even when the underlying DB is being modified after having opened and closed the db;
#[test]
fn test_delta_reader_after_opening_populated_db() {
    // Create and populate a DB, then close it
    let dir = {
        let TestDB { db, tmpdir } = TestDB::new();
        let db = Arc::new(db);
        let versioned_db_cache = VersionedDbCache::new(10_000);
        let versioned_db = Arc::new(
            VersionedDB::<LiveKeys, VersionedDbCache<LiveKeys>>::from_dbs(
                db.clone(),
                db.clone(),
                versioned_db_cache,
            )
            .unwrap(),
        );

        // Check iteration against an empty DB.
        let version = versioned_db.get_committed_version().unwrap();
        assert_eq!(version, None);

        put_keys(
            &versioned_db,
            &[(b"key11", 0), (b"key19", 0), (b"key20", 0)],
            0,
        );
        tmpdir
    };

    // Reopen the db and check that the delta reader returns the correct values.
    let test_db = TestDB::from_tempdir(dir);
    let db = Arc::new(test_db.db);
    let versioned_db_cache = VersionedDbCache::new(10_000);
    let versioned_db = Arc::new(
        VersionedDB::<LiveKeys, VersionedDbCache<LiveKeys>>::from_dbs(
            db.clone(),
            db.clone(),
            versioned_db_cache,
        )
        .unwrap(),
    );

    let delta_reader = VersionedDeltaReader::<LiveKeys, VersionedDbCache<LiveKeys>>::new(
        versioned_db.clone(),
        Some(0),
        vec![],
    );
    check_keys(
        &delta_reader,
        &[
            (b"key11", Some(0)),
            (b"key19", Some(0)),
            (b"key20", Some(0)),
        ],
    );

    // Check that the behavior of the db continues correctly after the reopen.
    let mut snapshot = VersionedSchemaBatch::<LiveKeys>::default();
    snapshot.put_versioned(TestKey::from(b"key12".to_vec()), TestField::new(1));
    snapshot.put_versioned(TestKey::from(b"key19".to_vec()), TestField::new(1));

    let snapshot_1 = Arc::new(snapshot);
    let delta_reader = VersionedDeltaReader::<LiveKeys, VersionedDbCache<LiveKeys>>::new(
        versioned_db.clone(),
        Some(0),
        vec![snapshot_1.clone()],
    );
    check_keys(
        &delta_reader,
        &[
            (b"key11", Some(0)),
            (b"key12", Some(1)),
            (b"key19", Some(1)),
            (b"key20", Some(0)),
        ],
    );

    let mut snapshot = VersionedSchemaBatch::<LiveKeys>::default();
    snapshot.put_versioned(TestKey::from(b"key12".to_vec()), TestField::new(2));
    snapshot.delete_versioned(TestKey::from(b"key19".to_vec()));

    let snapshot_2 = Arc::new(snapshot);
    let delta_reader = VersionedDeltaReader::<LiveKeys, VersionedDbCache<LiveKeys>>::new(
        versioned_db.clone(),
        Some(0),
        vec![snapshot_1.clone(), snapshot_2.clone()],
    );
    println!("Checking keys with snapshot 2");
    check_keys(
        &delta_reader,
        &[
            (b"key11", Some(0)),
            (b"key12", Some(2)),
            (b"key19", None),
            (b"key20", Some(0)),
        ],
    );

    // Committing the oldest snapshot should not change the iterator.
    commit_batch(&versioned_db, &snapshot_1, 1);
    check_keys(
        &delta_reader,
        &[
            (b"key11", Some(0)),
            (b"key12", Some(2)),
            (b"key19", None),
            (b"key20", Some(0)),
        ],
    );

    // Committing the second snapshot should not change the iterator.
    commit_batch(&versioned_db, &snapshot_2, 2);
    check_keys(
        &delta_reader,
        &[
            (b"key11", Some(0)),
            (b"key12", Some(2)),
            (b"key19", None),
            (b"key20", Some(0)),
        ],
    );

    // Commiting another batch should not change the the values read from the existing delta reader.
    let mut snapshot = VersionedSchemaBatch::<LiveKeys>::default();
    snapshot.put_versioned(TestKey::from(b"key11".to_vec()), TestField::new(3));
    snapshot.put_versioned(TestKey::from(b"key12".to_vec()), TestField::new(3));
    commit_batch(&versioned_db, &snapshot, 3);
    check_keys(
        &delta_reader,
        &[
            (b"key11", Some(0)),
            (b"key12", Some(2)),
            (b"key19", None),
            (b"key20", Some(0)),
        ],
    );
}
