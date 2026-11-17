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
    check_iterator_with_cursor(delta_reader, prefix, expected_values, None);
}

fn check_iterator_with_cursor(
    delta_reader: &VersionedDeltaReader<LiveKeys, VersionedDbCache<LiveKeys>>,
    prefix: &[u8],
    expected_values: &[(&[u8], u32)],
    cursor: Option<TestKey>,
) {
    let mut iter = delta_reader
        .iter_with_prefix_and_cursor(TestKey::from(prefix.to_vec()), cursor)
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
    let version = versioned_db.get_committed_version_live_db().unwrap();
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
fn test_iteration_with_cursor() {
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
    let version = versioned_db.get_committed_version_live_db().unwrap();
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
    let mut iter = delta_reader
        .iter_with_prefix_and_cursor(
            TestKey::from(b"key".to_vec()),
            Some(TestKey::from(b"key11".to_vec())),
        )
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
    check_iterator_with_cursor(
        &delta_reader,
        b"key",
        &[(b"key11", 0), (b"key19", 0), (b"key20", 0)],
        Some(TestKey::from(b"key11".to_vec())),
    );
    check_iterator_with_cursor(
        &delta_reader,
        b"key",
        &[(b"key11", 0), (b"key19", 0), (b"key20", 0)],
        Some(TestKey::from(b"key10".to_vec())),
    );
    check_iterator_with_cursor(
        &delta_reader,
        b"key",
        &[(b"key19", 0), (b"key20", 0)],
        Some(TestKey::from(b"key12".to_vec())),
    );
    check_iterator_with_cursor(
        &delta_reader,
        b"key1",
        &[(b"key11", 0), (b"key19", 0)],
        Some(TestKey::from(b"key10".to_vec())),
    );

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
    check_iterator_with_cursor(
        &delta_reader,
        b"key",
        &[(b"key11", 0), (b"key12", 1), (b"key19", 1), (b"key20", 0)],
        Some(TestKey::from(b"key11".to_vec())),
    );
    check_iterator_with_cursor(
        &delta_reader,
        b"key1",
        &[(b"key11", 0), (b"key12", 1), (b"key19", 1)],
        Some(TestKey::from(b"key10".to_vec())),
    );

    check_iterator_with_cursor(
        &delta_reader,
        b"key",
        &[(b"key12", 1), (b"key19", 1), (b"key20", 0)],
        Some(TestKey::from(b"key12".to_vec())),
    );
    check_iterator_with_cursor(
        &delta_reader,
        b"key1",
        &[(b"key12", 1), (b"key19", 1)],
        Some(TestKey::from(b"key12".to_vec())),
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
    check_iterator_with_cursor(
        &delta_reader,
        b"key",
        &[(b"key11", 0), (b"key12", 2), (b"key20", 0)],
        Some(TestKey::from(b"key11".to_vec())),
    );
    check_iterator_with_cursor(
        &delta_reader,
        b"key1",
        &[(b"key11", 0), (b"key12", 2)],
        Some(TestKey::from(b"key11".to_vec())),
    );
    check_iterator_with_cursor(
        &delta_reader,
        b"key1",
        &[(b"key12", 2)],
        Some(TestKey::from(b"key12".to_vec())),
    );
    check_iterator_with_cursor(
        &delta_reader,
        b"key",
        &[(b"key11", 0), (b"key12", 2), (b"key20", 0)],
        Some(TestKey::from(b"key11".to_vec())),
    );

    // Committing the oldest snapshot should not change the iterator.
    commit_batch(&versioned_db, &snapshot_1, 1);
    check_iterator_with_cursor(
        &delta_reader,
        b"key",
        &[(b"key11", 0), (b"key12", 2), (b"key20", 0)],
        Some(TestKey::from(b"key11".to_vec())),
    );
    check_iterator_with_cursor(
        &delta_reader,
        b"key1",
        &[(b"key11", 0), (b"key12", 2)],
        Some(TestKey::from(b"key11".to_vec())),
    );
    check_iterator_with_cursor(
        &delta_reader,
        b"key1",
        &[(b"key12", 2)],
        Some(TestKey::from(b"key12".to_vec())),
    );
    check_iterator_with_cursor(
        &delta_reader,
        b"key",
        &[(b"key11", 0), (b"key12", 2), (b"key20", 0)],
        Some(TestKey::from(b"key11".to_vec())),
    );

    // Committing the second snapshot should not change the iterator.
    commit_batch(&versioned_db, &snapshot_2, 2);
    check_iterator_with_cursor(
        &delta_reader,
        b"key",
        &[(b"key11", 0), (b"key12", 2), (b"key20", 0)],
        Some(TestKey::from(b"key11".to_vec())),
    );
    check_iterator_with_cursor(
        &delta_reader,
        b"key1",
        &[(b"key11", 0), (b"key12", 2)],
        Some(TestKey::from(b"key11".to_vec())),
    );
    check_iterator_with_cursor(
        &delta_reader,
        b"key1",
        &[(b"key12", 2)],
        Some(TestKey::from(b"key12".to_vec())),
    );
    check_iterator_with_cursor(
        &delta_reader,
        b"key",
        &[(b"key11", 0), (b"key12", 2), (b"key20", 0)],
        Some(TestKey::from(b"key11".to_vec())),
    );

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

/// An open iterator holds the cache read lock, so a commit must not be able to change the
/// live DB underneath it. The commit may write the archival DB, which the iterator never
/// reads, but it has to wait for the iterator before it writes the live DB.
#[test]
fn test_open_iterator_blocks_live_write() {
    let test_db = TestDB::new();
    let db = Arc::new(test_db.db);
    let versioned_db_cache = VersionedDbCache::new(0);
    let versioned_db = Arc::new(
        VersionedDB::<LiveKeys, VersionedDbCache<LiveKeys>>::from_dbs(
            db.clone(),
            db.clone(),
            versioned_db_cache.clone(),
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

    let key = TestKey::from(b"key1".to_vec());
    let mut batch = VersionedSchemaBatch::<LiveKeys>::default();
    batch.put_versioned(key.clone(), TestField::new(7));

    std::thread::scope(|s| {
        let writer = s.spawn(|| commit_batch(&versioned_db, &batch, 0));

        // Wait until the writer is queued for the write lock, instead of sleeping and hoping.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !versioned_db_cache.writer_holds_or_waits() {
            assert!(
                !writer.is_finished(),
                "The commit completed while an iterator was open"
            );
            assert!(
                std::time::Instant::now() < deadline,
                "The writer never reached the write lock"
            );
            std::thread::yield_now();
        }

        // The writer is now stuck behind the iterator: the archival DB has the new version,
        // the live DB does not.
        assert_eq!(
            versioned_db.get_historical_value(&key, 0).unwrap(),
            Some(TestField::new(7))
        );
        assert_eq!(versioned_db.get_committed_version_live_db().unwrap(), None);
        assert!(iter.next().is_none());
        assert!(!writer.is_finished());

        // Dropping the iterator releases the read lock and lets the commit complete.
        drop(iter);
        writer.join().unwrap();
    });

    assert_eq!(
        versioned_db.get_committed_version_live_db().unwrap(),
        Some(0)
    );
    assert_eq!(
        versioned_db.get_live_value(&key).unwrap(),
        Some(TestField::new(7))
    );
}
