// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

//! Tests for cache consistency with the backing RocksDB

use std::sync::Arc;

use rockbound::versioned_db::{VersionedDB, VersionedDeltaReader, VersionedSchemaBatch};

use crate::versioned_db_inner::{commit_batch, put_keys, TestDB, VersionedDbCache};

use super::{LiveKeys, TestField, TestKey};

fn check_iterator(
    delta_reader: &VersionedDeltaReader<LiveKeys, VersionedDbCache<LiveKeys>>,
    prefix: &[u8],
    expected_values: &[(&[u8], u32)],
) {
    let mut iter = delta_reader
        .iter_with_prefix(TestKey::from(prefix.to_vec()))
        .unwrap();
    for (key, value) in expected_values {
        let next = iter.next();
        let next = next.unwrap_or_else(|| {
            panic!(
                "Expected key {} with value {} in iterator, but got None",
                std::str::from_utf8(key).unwrap(),
                value
            )
        });
        assert_eq!(
            next.0,
            TestKey::from(key.to_vec()),
            "Expected key {} with value {} in iterator, but got key {}",
            std::str::from_utf8(key).unwrap(),
            value,
            std::str::from_utf8(next.0.as_ref()).unwrap()
        );
        assert_eq!(
            next.1,
            Some(TestField::new(*value)),
            "Expected value {} for key {} in iterator, but got {}",
            value,
            std::str::from_utf8(key).unwrap(),
            next.1.unwrap()
        );
    }
    assert_eq!(iter.next(), None);
}

#[test]
fn test_iteration() {
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
    let delta_reader = VersionedDeltaReader::<LiveKeys, VersionedDbCache<LiveKeys>>::new(
        versioned_db.clone(),
        None,
        vec![],
    );
    let mut iter = delta_reader
        .iter_with_prefix(TestKey::from(b"key".to_vec()))
        .unwrap();
    assert_eq!(iter.next(), None);
    drop(iter);

    put_keys(
        &versioned_db,
        &[(b"key11", 0), (b"key19", 0), (b"key20", 0)],
        0,
    );

    // Check iteration against prefixes "key". and key1
    let delta_reader = VersionedDeltaReader::<LiveKeys, VersionedDbCache<LiveKeys>>::new(
        versioned_db.clone(),
        Some(0),
        vec![],
    );
    check_iterator(
        &delta_reader,
        b"key",
        &[(b"key11", 0), (b"key19", 0), (b"key20", 0)],
    );
    check_iterator(&delta_reader, b"key1", &[(b"key11", 0), (b"key19", 0)]);

    let mut snapshot = VersionedSchemaBatch::<LiveKeys>::default();
    snapshot.put_versioned(TestKey::from(b"key12".to_vec()), TestField::new(1));
    snapshot.put_versioned(TestKey::from(b"key19".to_vec()), TestField::new(1));

    let snapshot_1 = Arc::new(snapshot);
    let mut snapshots = vec![snapshot_1.clone()];
    let delta_reader = VersionedDeltaReader::<LiveKeys, VersionedDbCache<LiveKeys>>::new(
        versioned_db.clone(),
        Some(0),
        snapshots.clone(),
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

    let mut snapshot_2 = VersionedSchemaBatch::<LiveKeys>::default();
    snapshot_2.put_versioned(TestKey::from(b"key12".to_vec()), TestField::new(2));
    snapshot_2.delete_versioned(TestKey::from(b"key19".to_vec()));
    let snapshot_2 = Arc::new(snapshot_2);
    snapshots.push(snapshot_2.clone());

    let delta_reader = VersionedDeltaReader::<LiveKeys, VersionedDbCache<LiveKeys>>::new(
        versioned_db.clone(),
        Some(0),
        snapshots.clone(),
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
        .iter_with_prefix(TestKey::from(b"key".to_vec()))
        .is_err());
}

#[test]
fn test_open_iterator_blocks_writes() {
    let test_db = TestDB::new();
    let db = Arc::new(test_db.db);
    let versioned_db_cache = VersionedDbCache::new(0);
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
        None,
        vec![],
    );

    let mut iter = delta_reader
        .iter_with_prefix(TestKey::from(b"key".to_vec()))
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
