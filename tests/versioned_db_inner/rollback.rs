//! Tests for [`VersionedDB::update_archival_db_batch_for_rollback`], which undoes the
//! archival half of a commit.

use rockbound::versioned_db::CommitStep;

use super::commit_protocol::{
    assert_live_view_is_v0, assert_live_view_is_v1, assert_reader_at_v0_is_stable,
    commit_v0_and_build_v1, key, open_versioned, pinned_reader, value, V,
};
use super::{LiveKeys, TestField, TestKey};

fn rollback(db: &V, version: u64) {
    let mut batch = rocksdb::WriteBatch::default();
    db.update_archival_db_batch_for_rollback(&mut batch, version)
        .unwrap();
    db.archival_db().write_db_batch(batch).unwrap();
}

fn historical(db: &V, name: &[u8], version: u64) -> Option<TestField> {
    db.get_historical_value(&key(name), version).unwrap()
}

fn pruning_keys(db: &V, version: u64) -> Vec<TestKey> {
    db.iter_pruning_keys_at_version(version)
        .unwrap()
        .map(|prunable_key| prunable_key.version_and_key().1)
        .collect()
}

/// Version 1 updates `updated`, deletes `deleted` and creates `created`. Once it is rolled
/// back, the archival DB must look as if only version 0 was ever committed.
fn assert_archival_db_is_at_v0(db: &V) {
    for version in [0, 1, 99] {
        assert_eq!(historical(db, b"updated", version), value(1));
        assert_eq!(historical(db, b"deleted", version), value(1));
        assert_eq!(historical(db, b"created", version), None);
    }
    assert_eq!(
        db.get_version_for_key(&key(b"updated"), 99).unwrap(),
        Some(0)
    );
    assert_eq!(
        db.get_version_for_key(&key(b"deleted"), 99).unwrap(),
        Some(0)
    );
    assert_eq!(db.get_version_for_key(&key(b"created"), 99).unwrap(), None);
    assert_eq!(pruning_keys(db, 0), [key(b"deleted"), key(b"updated")]);
    assert_eq!(pruning_keys(db, 1), []);
}

fn assert_archival_db_is_at_v1(db: &V) {
    assert_eq!(historical(db, b"updated", 1), value(2));
    assert_eq!(historical(db, b"deleted", 1), None);
    assert_eq!(historical(db, b"created", 1), value(2));
    assert_eq!(
        pruning_keys(db, 1),
        [key(b"created"), key(b"deleted"), key(b"updated")]
    );
}

/// A rollback removes the historical rows and the pruning entries of the version, and
/// nothing else: the live DB, the cache and the committed version stay as they are.
#[test]
fn rollback_removes_what_a_version_wrote_to_the_archival_db() {
    let (_tmpdir, db, cache) = open_versioned();
    let batch = commit_v0_and_build_v1(&db);
    db.commit(&batch, 1).unwrap();
    assert_archival_db_is_at_v1(&db);

    rollback(&db, 1);

    assert_archival_db_is_at_v0(&db);
    assert_eq!(db.fetch_latest_committed_archival_version(), Some(1));
    assert_live_view_is_v1(&db, &cache);
    for (name, expected) in [
        (&b"updated"[..], value(2)),
        (&b"deleted"[..], None),
        (&b"created"[..], value(2)),
    ] {
        assert_eq!(
            db.live_db().get_raw::<LiveKeys>(name).unwrap(),
            expected,
            "A rollback must not touch the live DB"
        );
    }
}

/// The case a rollback exists for: the archival batch of version 1 was written, the live
/// batch was not. After the rollback the DB must behave as if version 1 was never started,
/// and in particular readers at version 0 must keep using the live DB and its cache.
#[test]
fn rollback_after_interrupted_commit_restores_the_previous_version() {
    let (_tmpdir, db, cache) = open_versioned();
    let batch = commit_v0_and_build_v1(&db);
    let result = db.commit_with_hook(&batch, 1, |step| match step {
        CommitStep::ArchivalWritten => anyhow::bail!("injected crash"),
        _ => Ok(()),
    });
    assert!(result.is_err());
    assert_archival_db_is_at_v1(&db);

    rollback(&db, 1);

    assert_archival_db_is_at_v0(&db);
    assert_live_view_is_v0(&db, &cache);
    assert_eq!(
        db.fetch_latest_committed_archival_version(),
        Some(0),
        "A rollback must not move the committed version: readers at version 0 would \
         stop using the live DB"
    );
    let reader_at_v0 = pinned_reader(&db, 0);
    let hits_before = db.cache_hits();
    assert_reader_at_v0_is_stable(&reader_at_v0);
    assert_eq!(
        db.cache_hits(),
        hits_before + 3,
        "A reader at the live version must be served from the live cache after a rollback"
    );

    // The rolled back version can be committed again.
    db.commit(&batch, 1).unwrap();
    assert_archival_db_is_at_v1(&db);
    assert_live_view_is_v1(&db, &cache);
    assert_reader_at_v0_is_stable(&reader_at_v0);
}

#[test]
fn rollback_of_a_version_without_writes_is_empty() {
    let (_tmpdir, db, _cache) = open_versioned();
    let batch = commit_v0_and_build_v1(&db);
    db.commit(&batch, 1).unwrap();

    let mut rollback_batch = rocksdb::WriteBatch::default();
    db.update_archival_db_batch_for_rollback(&mut rollback_batch, 2)
        .unwrap();
    assert!(rollback_batch.is_empty());

    // Each key of the version contributes one historical row and one pruning entry.
    db.update_archival_db_batch_for_rollback(&mut rollback_batch, 1)
        .unwrap();
    assert_eq!(rollback_batch.len(), 6);
}
