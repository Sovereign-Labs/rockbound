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

/// Runs `body` on its own thread and fails the test if it does not finish, so that a
/// deadlock shows up as a test failure instead of a hanging test run.
fn fail_on_deadlock(body: impl FnOnce() + Send + 'static) {
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        body();
        let _ = done_tx.send(());
    });
    match done_rx.recv_timeout(std::time::Duration::from_secs(30)) {
        Ok(()) => handle.join().unwrap(),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("The test body did not finish: it is most likely deadlocked")
        }
        // The body panicked: report its panic.
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            std::panic::resume_unwind(handle.join().unwrap_err())
        }
    }
}

/// An iterator reads from a snapshot of the live DB that is taken when it is created. It
/// does not hold the cache lock afterwards, so the thread that holds it can commit and can
/// open further iterators, and it keeps yielding the state it was created at.
#[test]
fn test_open_iterator_keeps_its_snapshot_and_does_not_block_commits() {
    fail_on_deadlock(|| {
        let test_db = TestDB::new();
        let db = Arc::new(test_db.db);
        let versioned_db = Arc::new(
            VersionedDB::<LiveKeys, VersionedDbCache<LiveKeys>>::from_dbs(
                db.clone(),
                db.clone(),
                VersionedDbCache::new(10_000),
            )
            .unwrap(),
        );
        let key = |name: &[u8]| TestKey::from(name.to_vec());
        let prefix = key(b"key");

        put_keys(
            &versioned_db,
            &[(b"key1", 0), (b"key2", 0), (b"key3", 0)],
            0,
        );
        let reader_at_v0 = VersionedDeltaReader::<LiveKeys, VersionedDbCache<LiveKeys>>::new(
            versioned_db.clone(),
            Some(0),
            vec![],
        );
        let mut iter_at_v0 = reader_at_v0.iter_with_prefix(prefix.clone()).unwrap();
        assert_eq!(
            iter_at_v0.next(),
            Some((key(b"key1"), Some(TestField::new(0))))
        );

        // Commit version 1 while the iterator is open. It changes, deletes and adds keys
        // that the iterator has not reached yet.
        let mut batch = VersionedSchemaBatch::<LiveKeys>::default();
        batch.put_versioned(key(b"key2"), TestField::new(1));
        batch.delete_versioned(key(b"key3"));
        batch.put_versioned(key(b"key4"), TestField::new(1));
        commit_batch(&versioned_db, &batch, 1);

        // A second iterator can be opened while the first one is still alive, also from a
        // second thread while this one keeps its iterator.
        let reader_at_v1 = VersionedDeltaReader::<LiveKeys, VersionedDbCache<LiveKeys>>::new(
            versioned_db.clone(),
            Some(1),
            vec![],
        );
        let expected_at_v1: &[(&[u8], u32)] = &[(b"key1", 0), (b"key2", 1), (b"key4", 1)];
        check_iterator(&reader_at_v1, b"key", expected_at_v1);
        std::thread::scope(|s| {
            s.spawn(|| check_iterator(&reader_at_v1, b"key", expected_at_v1));
        });

        // The first iterator still yields version 0.
        assert_eq!(
            iter_at_v0.collect::<Vec<_>>(),
            [
                (key(b"key2"), Some(TestField::new(0))),
                (key(b"key3"), Some(TestField::new(0)))
            ]
        );
    });
}
