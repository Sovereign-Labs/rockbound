use std::sync::Arc;

use rockbound::versioned_db::{
    HistoricalValueError, VersionedDB, VersionedDeltaReader, VersionedSchemaBatch,
};
use rockbound::SchemaBatch;
use tempfile::TempDir;

use super::{commit_batch, LiveKeys, TestDB, TestField, TestKey, VersionedDbCache};

type V = VersionedDB<LiveKeys, VersionedDbCache<LiveKeys>>;

fn open_versioned() -> (TempDir, Arc<V>) {
    let TestDB { tmpdir, db } = TestDB::new();
    let db = Arc::new(db);
    let cache = VersionedDbCache::new(10_000);
    let versioned_db = Arc::new(V::from_dbs(db.clone(), db, cache).unwrap());
    (tmpdir, versioned_db)
}

fn put_at(versioned_db: &V, keys: &[(&[u8], u32)], version: u64) {
    let mut batch = VersionedSchemaBatch::<LiveKeys>::default();
    for (k, v) in keys {
        batch.put_versioned(TestKey::from(k.to_vec()), TestField::new(*v));
    }
    commit_batch(versioned_db, &batch, version);
}

fn delete_at(versioned_db: &V, keys: &[&[u8]], version: u64) {
    let mut batch = VersionedSchemaBatch::<LiveKeys>::default();
    for k in keys {
        batch.delete_versioned(TestKey::from(k.to_vec()));
    }
    commit_batch(versioned_db, &batch, version);
}

fn commit_pruning_batch(versioned_db: &V, batch: &SchemaBatch) {
    versioned_db.archival_db().write_schemas(batch).unwrap();
}

fn hist(versioned_db: &V, key: &[u8], version: u64) -> Option<u32> {
    versioned_db
        .get_historical_value(&TestKey::from(key.to_vec()), version)
        .unwrap()
        .map(|f| f.value())
}

fn live(versioned_db: &V, key: &[u8]) -> Option<u32> {
    versioned_db
        .get_live_value(&TestKey::from(key.to_vec()))
        .unwrap()
        .map(|f| f.value())
}

fn historical(
    delta_reader: &VersionedDeltaReader<LiveKeys, VersionedDbCache<LiveKeys>>,
    key: &[u8],
    version: u64,
) -> Result<Option<u32>, HistoricalValueError> {
    delta_reader
        .get_historical_borrowed(&TestKey::from(key.to_vec()), version)
        .map(|value| value.map(|field| field.value()))
}

#[test]
fn basic_prune() {
    let (_dir, db) = open_versioned();
    for v in 0..=9u64 {
        put_at(&db, &[(b"k", v as u32)], v);
    }

    let out = db.collect_pruning_batch(3, None).unwrap();
    assert!(!out.hit_size_limit);
    assert_eq!(out.last_pruned_version, Some(5));
    // V=0 has no prev write, so no historical delete; V=1..=6 each produce one historical delete.
    assert_eq!(out.keys_to_prune, 6);
    assert_eq!(out.keys_inspected, 7);

    commit_pruning_batch(&db, &out.batch);

    for v in 0..=5u64 {
        assert_eq!(hist(&db, b"k", v), None, "k @ v={v} should be pruned");
    }
    for v in 6..=9u64 {
        assert_eq!(
            hist(&db, b"k", v),
            Some(v as u32),
            "k @ v={v} should survive"
        );
    }
    assert_eq!(db.get_pruned_version().unwrap(), Some(5));
    assert_eq!(live(&db, b"k"), Some(9));

    let delta_reader = VersionedDeltaReader::<LiveKeys, VersionedDbCache<LiveKeys>>::new(
        db.clone(),
        Some(9),
        vec![],
    );
    assert!(matches!(
        historical(&delta_reader, b"k", 5),
        Err(HistoricalValueError::PrunedVersion {
            requested_version: 5,
            oldest_available_version: Some(6),
        })
    ));
    assert_eq!(historical(&delta_reader, b"k", 6).unwrap(), Some(6));
}

/// Mirrors the accessory Pruner A..G layout. cutoff = 9 - 3 = 6.
#[test]
fn multi_key_layout() {
    let (_dir, db) = open_versioned();

    put_at(
        &db,
        &[(b"C", 0), (b"D", 0), (b"E", 0), (b"F", 0), (b"G", 0)],
        0,
    );
    put_at(&db, &[(b"C", 1), (b"E", 1), (b"F", 1)], 1);
    put_at(&db, &[(b"C", 2), (b"E", 2), (b"F", 2)], 2);
    put_at(&db, &[(b"C", 3), (b"D", 3), (b"F", 3)], 3);
    put_at(&db, &[(b"C", 4)], 4);
    put_at(&db, &[(b"C", 5)], 5);
    put_at(&db, &[(b"A", 6), (b"C", 6), (b"D", 6)], 6);
    put_at(&db, &[(b"A", 7), (b"B", 7), (b"C", 7), (b"G", 7)], 7);
    put_at(&db, &[(b"A", 8), (b"B", 8), (b"C", 8), (b"G", 8)], 8);
    put_at(
        &db,
        &[(b"A", 9), (b"B", 9), (b"C", 9), (b"D", 9), (b"G", 9)],
        9,
    );

    let out = db.collect_pruning_batch(3, None).unwrap();
    assert!(!out.hit_size_limit);
    assert_eq!(out.last_pruned_version, Some(5));
    commit_pruning_batch(&db, &out.batch);

    type HistAt = (u64, Option<u32>);
    type HistCase<'a> = (&'a [u8], &'a [HistAt]);
    let cases: &[HistCase] = &[
        // A written at [6,7,8,9]; V=6 is first → no historical delete → all 4 entries survive.
        (
            b"A",
            &[
                (0, None),
                (1, None),
                (2, None),
                (3, None),
                (4, None),
                (5, None),
                (6, Some(6)),
                (7, Some(7)),
                (8, Some(8)),
                (9, Some(9)),
            ],
        ),
        // B written at [7,8,9]; no pruning entries ≤ 6.
        (
            b"B",
            &[
                (0, None),
                (1, None),
                (2, None),
                (3, None),
                (4, None),
                (5, None),
                (6, None),
                (7, Some(7)),
                (8, Some(8)),
                (9, Some(9)),
            ],
        ),
        // C written at [0..=9]; V=1..=6 each delete the prev. Survivors: C@6,7,8,9.
        (
            b"C",
            &[
                (0, None),
                (1, None),
                (2, None),
                (3, None),
                (4, None),
                (5, None),
                (6, Some(6)),
                (7, Some(7)),
                (8, Some(8)),
                (9, Some(9)),
            ],
        ),
        // D written at [0,3,6,9]; survivors: D@6, D@9.
        (
            b"D",
            &[
                (0, None),
                (1, None),
                (2, None),
                (3, None),
                (4, None),
                (5, None),
                (6, Some(6)),
                (7, Some(6)),
                (8, Some(6)),
                (9, Some(9)),
            ],
        ),
        // E written at [0,1,2]; survivor: E@2.
        (
            b"E",
            &[
                (0, None),
                (1, None),
                (2, Some(2)),
                (3, Some(2)),
                (4, Some(2)),
                (5, Some(2)),
                (6, Some(2)),
                (7, Some(2)),
                (8, Some(2)),
                (9, Some(2)),
            ],
        ),
        // F written at [0,1,2,3]; survivor: F@3.
        (
            b"F",
            &[
                (0, None),
                (1, None),
                (2, None),
                (3, Some(3)),
                (4, Some(3)),
                (5, Some(3)),
                (6, Some(3)),
                (7, Some(3)),
                (8, Some(3)),
                (9, Some(3)),
            ],
        ),
        // G written at [0,7,8,9]; V=0 is first → kept. Survivors: G@0,7,8,9.
        (
            b"G",
            &[
                (0, Some(0)),
                (1, Some(0)),
                (2, Some(0)),
                (3, Some(0)),
                (4, Some(0)),
                (5, Some(0)),
                (6, Some(0)),
                (7, Some(7)),
                (8, Some(8)),
                (9, Some(9)),
            ],
        ),
    ];

    for (key, queries) in cases {
        for (qv, expected) in *queries {
            assert_eq!(
                hist(&db, key, *qv),
                *expected,
                "historical({}, {}) mismatch",
                std::str::from_utf8(key).unwrap(),
                qv,
            );
        }
    }

    // Live values are unaffected by pruning.
    for (key, latest) in [
        (&b"A"[..], 9u32),
        (b"B", 9),
        (b"C", 9),
        (b"D", 9),
        // E, F have no writes after pruning boundary but live still reflects their last write
        (b"E", 2),
        (b"F", 3),
        (b"G", 9),
    ] {
        assert_eq!(
            live(&db, key),
            Some(latest),
            "live({}) mismatch",
            std::str::from_utf8(key).unwrap()
        );
    }

    assert_eq!(db.get_pruned_version().unwrap(), Some(5));
}

#[test]
fn max_batch_size_split() {
    let (_dir, db) = open_versioned();
    for v in 0..=9u64 {
        put_at(&db, &[(b"k", v as u32)], v);
    }

    // 7 pruning entries ≤ cutoff=6 (V=0..=6); V=0 produces no historical delete.
    // With max_batch_size=3, the first run breaks after the 3rd historical delete (V=3).
    let out1 = db.collect_pruning_batch(3, Some(3)).unwrap();
    assert!(out1.hit_size_limit);
    assert_eq!(out1.last_pruned_version, Some(2));
    assert_eq!(out1.keys_to_prune, 3);
    assert_eq!(out1.keys_inspected, 4); // V=0,1,2,3
    commit_pruning_batch(&db, &out1.batch);
    assert_eq!(db.get_pruned_version().unwrap(), Some(2));

    let delta_reader = VersionedDeltaReader::<LiveKeys, VersionedDbCache<LiveKeys>>::new(
        db.clone(),
        Some(9),
        vec![],
    );
    assert!(matches!(
        historical(&delta_reader, b"k", 2),
        Err(HistoricalValueError::PrunedVersion {
            requested_version: 2,
            oldest_available_version: Some(3),
        })
    ));
    assert_eq!(historical(&delta_reader, b"k", 3).unwrap(), Some(3));

    // After commit: pruning entries [0..=3|k] gone, historical k|0,1,2 gone.
    // Remaining pruning entries ≤ cutoff: [4|k], [5|k], [6|k]. Each produces one historical delete.
    let out2 = db.collect_pruning_batch(3, Some(3)).unwrap();
    assert!(out2.hit_size_limit);
    assert_eq!(out2.last_pruned_version, Some(5));
    assert_eq!(out2.keys_to_prune, 3);
    assert_eq!(out2.keys_inspected, 3);
    commit_pruning_batch(&db, &out2.batch);

    // No remaining pruning entries ≤ cutoff.
    let out3 = db.collect_pruning_batch(3, Some(3)).unwrap();
    assert!(!out3.hit_size_limit);
    assert_eq!(out3.keys_to_prune, 0);
    assert_eq!(out3.keys_inspected, 0);
    assert_eq!(out3.last_pruned_version, None);
    commit_pruning_batch(&db, &out3.batch);

    // Final state should match the basic_prune outcome.
    for v in 0..=5u64 {
        assert_eq!(hist(&db, b"k", v), None);
    }
    for v in 6..=9u64 {
        assert_eq!(hist(&db, b"k", v), Some(v as u32));
    }
    assert_eq!(db.get_pruned_version().unwrap(), Some(5));
}

#[test]
fn empty_db() {
    let (_dir, db) = open_versioned();

    let out = db.collect_pruning_batch(3, None).unwrap();
    assert!(!out.hit_size_limit);
    assert_eq!(out.last_pruned_version, None);
    assert_eq!(out.keys_inspected, 0);
    assert_eq!(out.keys_to_prune, 0);

    // Committing the empty batch is harmless and doesn't introduce a pruned version.
    commit_pruning_batch(&db, &out.batch);
    assert_eq!(db.get_pruned_version().unwrap(), None);
}

#[test]
fn keep_versions_zero_errors() {
    let (_dir, db) = open_versioned();
    put_at(&db, &[(b"k", 0)], 0);

    let err = db.collect_pruning_batch(0, None).unwrap_err();
    assert!(
        err.to_string().contains("keep_versions must be >= 1"),
        "unexpected error: {err}",
    );
}

#[test]
fn cutoff_underflow_returns_empty() {
    // last_committed < keep_versions → checked_sub underflows → empty batch.
    let (_dir, db) = open_versioned();
    put_at(&db, &[(b"k", 0)], 0);
    put_at(&db, &[(b"k", 1)], 1);

    let out = db.collect_pruning_batch(5, None).unwrap();
    assert!(!out.hit_size_limit);
    assert_eq!(out.last_pruned_version, None);
    assert_eq!(out.keys_inspected, 0);
    assert_eq!(out.keys_to_prune, 0);
}

#[test]
fn pruned_version_persists_across_reopen() {
    let dir = {
        let TestDB { tmpdir, db } = TestDB::new();
        let db = Arc::new(db);
        let cache = VersionedDbCache::new(10_000);
        let versioned_db = Arc::new(V::from_dbs(db.clone(), db, cache).unwrap());

        for v in 0..=9u64 {
            put_at(&versioned_db, &[(b"k", v as u32)], v);
        }
        let out = versioned_db.collect_pruning_batch(3, None).unwrap();
        commit_pruning_batch(&versioned_db, &out.batch);
        assert_eq!(versioned_db.get_pruned_version().unwrap(), Some(5));

        tmpdir
    };

    let test_db = TestDB::from_tempdir(dir);
    let db = Arc::new(test_db.db);
    let cache = VersionedDbCache::new(10_000);
    let reopened = V::from_dbs(db.clone(), db, cache).unwrap();
    assert_eq!(reopened.get_pruned_version().unwrap(), Some(5));
}

#[test]
fn partial_pruned_version_persists_across_reopen() {
    let dir = {
        let TestDB { tmpdir, db } = TestDB::new();
        let db = Arc::new(db);
        let cache = VersionedDbCache::new(10_000);
        let versioned_db = Arc::new(V::from_dbs(db.clone(), db, cache).unwrap());

        for v in 0..=9u64 {
            put_at(&versioned_db, &[(b"k", v as u32)], v);
        }
        let out = versioned_db.collect_pruning_batch(3, Some(3)).unwrap();
        assert_eq!(out.last_pruned_version, Some(2));
        commit_pruning_batch(&versioned_db, &out.batch);
        assert_eq!(versioned_db.get_pruned_version().unwrap(), Some(2));

        let delta_reader = VersionedDeltaReader::<LiveKeys, VersionedDbCache<LiveKeys>>::new(
            versioned_db.clone(),
            Some(9),
            vec![],
        );
        assert!(matches!(
            historical(&delta_reader, b"k", 2),
            Err(HistoricalValueError::PrunedVersion {
                requested_version: 2,
                oldest_available_version: Some(3),
            })
        ));
        assert_eq!(historical(&delta_reader, b"k", 3).unwrap(), Some(3));

        tmpdir
    };

    let test_db = TestDB::from_tempdir(dir);
    let db = Arc::new(test_db.db);
    let cache = VersionedDbCache::new(10_000);
    let reopened = Arc::new(V::from_dbs(db.clone(), db, cache).unwrap());
    assert_eq!(reopened.get_pruned_version().unwrap(), Some(2));

    let delta_reader = VersionedDeltaReader::<LiveKeys, VersionedDbCache<LiveKeys>>::new(
        reopened,
        Some(9),
        vec![],
    );
    assert!(matches!(
        historical(&delta_reader, b"k", 2),
        Err(HistoricalValueError::PrunedVersion {
            requested_version: 2,
            oldest_available_version: Some(3),
        })
    ));
    assert_eq!(historical(&delta_reader, b"k", 3).unwrap(), Some(3));
}

#[test]
fn no_historical_deletes_do_not_advance_pruned_version() {
    let (_dir, db) = open_versioned();

    for v in 0..=9u64 {
        let key = format!("k{v}").into_bytes();
        put_at(&db, &[(key.as_slice(), v as u32)], v);
    }

    let out = db.collect_pruning_batch(3, None).unwrap();
    assert!(!out.hit_size_limit);
    assert_eq!(out.last_pruned_version, None);
    assert_eq!(out.keys_inspected, 7);
    assert_eq!(out.keys_to_prune, 0);

    commit_pruning_batch(&db, &out.batch);
    assert_eq!(db.get_pruned_version().unwrap(), None);
}

#[test]
fn live_cf_unaffected() {
    let (_dir, db) = open_versioned();

    // K1 written at every version 0..=9, K2 only at v=0..=5, K3 only at v=7..=9.
    for v in 0..=9u64 {
        let mut entries: Vec<(&[u8], u32)> = vec![(b"K1", v as u32)];
        if v <= 5 {
            entries.push((b"K2", v as u32));
        }
        if v >= 7 {
            entries.push((b"K3", v as u32));
        }
        put_at(&db, &entries, v);
    }
    // Sanity: K2 was deleted at v=9 to exercise the live-CF delete-tombstone path.
    delete_at(&db, &[b"K2"], 9);

    // Snapshot live values pre-prune.
    let pre_k1 = live(&db, b"K1");
    let pre_k2 = live(&db, b"K2");
    let pre_k3 = live(&db, b"K3");

    let out = db.collect_pruning_batch(3, None).unwrap();
    commit_pruning_batch(&db, &out.batch);

    // Live values must be identical to pre-prune.
    assert_eq!(live(&db, b"K1"), pre_k1);
    assert_eq!(live(&db, b"K2"), pre_k2);
    assert_eq!(live(&db, b"K3"), pre_k3);
}
