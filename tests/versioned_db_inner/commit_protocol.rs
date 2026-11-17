//! Tests for the commit protocol documented on [`VersionedDB`]: the archival DB is written
//! without the cache lock, and the live write and the cache update share one write lock.
//!
//! The deterministic tests use [`VersionedDB::commit_with_hook`] to stop a commit at a known
//! point and look at what readers observe there. The stress test checks every read against a
//! model of the committed state while commits run without pause.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use rockbound::versioned_db::{
    CommitStep, VersionedDB, VersionedDeltaReader, VersionedSchemaBatch,
};
use tempfile::TempDir;

use super::{put_keys, LiveKeys, TestDB, TestField, TestKey, VersionedDbCache};

type V = VersionedDB<LiveKeys, VersionedDbCache<LiveKeys>>;
type Reader = VersionedDeltaReader<LiveKeys, VersionedDbCache<LiveKeys>>;

fn open_versioned() -> (TempDir, Arc<V>, VersionedDbCache<LiveKeys>) {
    let TestDB { tmpdir, db } = TestDB::new();
    let db = Arc::new(db);
    let cache = VersionedDbCache::new(1_000_000);
    let versioned_db = Arc::new(V::from_dbs(db.clone(), db, cache.clone()).unwrap());
    (tmpdir, versioned_db, cache)
}

fn key(name: &[u8]) -> TestKey {
    TestKey::from(name.to_vec())
}

fn value(value: u32) -> Option<TestField> {
    Some(TestField::new(value))
}

fn pinned_reader(db: &Arc<V>, version: u64) -> Reader {
    Reader::new(db.clone(), Some(version), vec![])
}

/// Commits version 0 with `updated` and `deleted` present and `created` absent, reads all
/// three so that the cache holds their version 0 state, and returns the batch for version 1.
fn commit_v0_and_build_v1(db: &Arc<V>) -> VersionedSchemaBatch<LiveKeys> {
    put_keys(db, &[(b"updated", 1), (b"deleted", 1)], 0);
    assert_eq!(db.get_live_value(&key(b"created")).unwrap(), None);

    let mut batch = VersionedSchemaBatch::<LiveKeys>::default();
    batch.put_versioned(key(b"updated"), TestField::new(2));
    batch.delete_versioned(key(b"deleted"));
    batch.put_versioned(key(b"created"), TestField::new(2));
    batch
}

fn assert_live_view_is_v0(db: &Arc<V>, cache: &VersionedDbCache<LiveKeys>) {
    assert_eq!(db.get_committed_version_live_db().unwrap(), Some(0));
    for (name, expected) in [
        (&b"updated"[..], value(1)),
        (&b"deleted"[..], value(1)),
        (&b"created"[..], None),
    ] {
        assert_eq!(cache.peek(&key(name)), Some(expected));
        assert_eq!(db.get_live_value(&key(name)).unwrap(), expected);
    }
}

fn assert_live_view_is_v1(db: &Arc<V>, cache: &VersionedDbCache<LiveKeys>) {
    assert_eq!(db.get_committed_version_live_db().unwrap(), Some(1));
    for (name, expected) in [
        (&b"updated"[..], value(2)),
        (&b"deleted"[..], None),
        (&b"created"[..], value(2)),
    ] {
        // The cache itself must hold the new state: a version 0 entry left behind here
        // would be served forever.
        assert_eq!(cache.peek(&key(name)), Some(expected));
        assert_eq!(db.get_live_value(&key(name)).unwrap(), expected);
    }
}

fn assert_reader_at_v0_is_stable(reader: &Reader) {
    assert_eq!(
        reader.get_latest_borrowed(&key(b"updated")).unwrap(),
        value(1)
    );
    assert_eq!(
        reader.get_latest_borrowed(&key(b"deleted")).unwrap(),
        value(1)
    );
    assert_eq!(reader.get_latest_borrowed(&key(b"created")).unwrap(), None);
}

/// Between the archival write and the live write the cache lock is free, so readers keep
/// reading, and caching, the previous version. The commit must then replace those entries.
#[test]
fn readers_see_previous_version_until_live_write() {
    let (_tmpdir, db, cache) = open_versioned();
    let batch = commit_v0_and_build_v1(&db);
    let reader_at_v0 = pinned_reader(&db, 0);

    let mut hook_ran = false;
    db.commit_with_hook(&batch, 1, |step| {
        if step == CommitStep::ArchivalWritten {
            hook_ran = true;
            assert!(
                !cache.writer_holds_or_waits(),
                "The cache lock must not be held while only the archival DB is written"
            );
            // The archival DB is already at version 1...
            assert_eq!(
                db.get_historical_value(&key(b"updated"), 1).unwrap(),
                value(2)
            );
            // ...while everything a live reader looks at is still at version 0.
            assert_eq!(db.fetch_latest_committed_archival_version(), Some(0));
            assert_live_view_is_v0(&db, &cache);
            assert_reader_at_v0_is_stable(&reader_at_v0);
        }
        Ok(())
    })
    .unwrap();
    assert!(hook_ran);

    assert_live_view_is_v1(&db, &cache);
    assert_reader_at_v0_is_stable(&reader_at_v0);
    let reader_at_v1 = pinned_reader(&db, 1);
    assert_eq!(
        reader_at_v1.get_latest_borrowed(&key(b"updated")).unwrap(),
        value(2)
    );
    assert_eq!(
        reader_at_v1.get_latest_borrowed(&key(b"deleted")).unwrap(),
        None
    );
    assert_eq!(
        reader_at_v1.get_latest_borrowed(&key(b"created")).unwrap(),
        value(2)
    );
}

/// A commit that is abandoned after the archival write must leave everything a live reader
/// looks at untouched, and must be retryable.
#[test]
fn commit_aborted_after_archival_write_leaves_live_view_untouched() {
    let (_tmpdir, db, cache) = open_versioned();
    let batch = commit_v0_and_build_v1(&db);
    let reader_at_v0 = pinned_reader(&db, 0);

    let result = db.commit_with_hook(&batch, 1, |step| match step {
        CommitStep::ArchivalWritten => anyhow::bail!("injected failure"),
        _ => Ok(()),
    });
    assert!(result.is_err());

    assert!(!cache.writer_holds_or_waits());
    assert_eq!(db.fetch_latest_committed_archival_version(), Some(0));
    assert_live_view_is_v0(&db, &cache);
    assert_reader_at_v0_is_stable(&reader_at_v0);

    db.commit(&batch, 1).unwrap();
    assert_live_view_is_v1(&db, &cache);
    assert_reader_at_v0_is_stable(&reader_at_v0);
}

/// A failed live write must not leave values in the cache that are not in the live DB.
#[test]
fn failed_live_write_leaves_cache_equal_to_live_db() {
    let (_tmpdir, db, cache) = open_versioned();
    let batch = commit_v0_and_build_v1(&db);
    let reader_at_v0 = pinned_reader(&db, 0);

    let result = db.commit_with_hook(&batch, 1, |step| match step {
        CommitStep::BeforeLiveWrite => anyhow::bail!("injected live write failure"),
        _ => Ok(()),
    });
    assert!(result.is_err());

    assert!(!cache.writer_holds_or_waits());
    assert_live_view_is_v0(&db, &cache);
    // The committed version has already moved on. That is harmless: it only sends the
    // reader at version 0 to the archival DB, which holds the same values.
    assert_eq!(db.fetch_latest_committed_archival_version(), Some(1));
    assert_reader_at_v0_is_stable(&reader_at_v0);

    db.commit(&batch, 1).unwrap();
    assert_live_view_is_v1(&db, &cache);
    assert_reader_at_v0_is_stable(&reader_at_v0);
}

/// Checks the part of a commit that runs under the write lock, from the point of view of
/// readers on another thread.
#[test]
fn readers_are_correct_while_live_write_is_in_progress() {
    let (_tmpdir, db, cache) = open_versioned();
    let batch = commit_v0_and_build_v1(&db);
    let reader_at_v0 = pinned_reader(&db, 0);

    let mut steps = vec![];
    db.commit_with_hook(&batch, 1, |step| {
        steps.push(step);
        match step {
            CommitStep::ArchivalWritten => {}
            CommitStep::BeforeLiveWrite => {
                assert!(
                    cache.writer_holds_or_waits(),
                    "The live DB must only be written under the cache write lock"
                );
                assert_eq!(
                    db.fetch_latest_committed_archival_version(),
                    Some(1),
                    "The committed version must be stored before the live DB is written"
                );
                assert_eq!(db.get_committed_version_live_db().unwrap(), Some(0));
            }
            CommitStep::LiveWritten => {
                assert!(
                    cache.writer_holds_or_waits(),
                    "The cache write lock must be held until the cache is updated"
                );
                assert_eq!(db.get_committed_version_live_db().unwrap(), Some(1));
                // The live DB is at version 1 and the cache still holds version 0 entries.
                // Readers on other threads cannot take the read lock, so they must neither
                // block nor use the cache.
                std::thread::scope(|s| {
                    s.spawn(|| {
                        assert_eq!(db.get_live_value(&key(b"updated")).unwrap(), value(2));
                        assert_eq!(db.get_live_value(&key(b"deleted")).unwrap(), None);
                        assert_eq!(db.get_live_value(&key(b"created")).unwrap(), value(2));
                        // A reader pinned at version 0 must not be handed version 1 values.
                        assert_reader_at_v0_is_stable(&reader_at_v0);
                    })
                    .join()
                    .unwrap();
                });
            }
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(
        steps,
        [
            CommitStep::ArchivalWritten,
            CommitStep::BeforeLiveWrite,
            CommitStep::LiveWritten
        ]
    );

    assert_live_view_is_v1(&db, &cache);
    assert_reader_at_v0_is_stable(&reader_at_v0);
}

/// A reader that cannot take the read lock must not write to the cache: it has no way to
/// know that the value it read from the DB is still current.
#[test]
fn read_without_the_read_lock_does_not_populate_the_cache() {
    let (_tmpdir, db, cache) = open_versioned();
    let mut batch = VersionedSchemaBatch::<LiveKeys>::default();
    batch.put_versioned(key(b"key"), TestField::new(1));
    db.commit(&batch, 0).unwrap();
    // `set_cache_size` installs a new, empty cache.
    db.set_cache_size(1_000, 1_000_000);
    assert_eq!(cache.peek(&key(b"key")), None);

    {
        let _write_guard = rockbound::versioned_db::CacheForVersionedDB::write(&cache);
        assert_eq!(db.get_live_value(&key(b"key")).unwrap(), value(1));
    }
    assert_eq!(cache.peek(&key(b"key")), None);

    assert_eq!(db.get_live_value(&key(b"key")).unwrap(), value(1));
    assert_eq!(cache.peek(&key(b"key")), Some(value(1)));
}

// ---------------------------------------------------------------------------------------
// Stress test against a model.
//
// Version `v` writes every key `i` with `(i + v) % 4 == 0`. The write is a delete if
// `v % 5 == 4` and a put of the value `v` otherwise. That makes the state at any version a
// pure function of the key and the version, so every read can be checked exactly.
// ---------------------------------------------------------------------------------------

const NUM_KEYS: usize = 400;
// Every commit does two synchronous writes, which is what bounds the runtime of this test.
const LAST_VERSION: u64 = 150;
const KEY_PREFIX: &[u8] = b"k";

fn model_key(i: usize) -> TestKey {
    TestKey::from(format!("k{i:03}").into_bytes())
}

/// The value of key `i` once `version` is committed.
fn model_value(i: usize, version: u64) -> Option<TestField> {
    let versions_since_last_write = (i as u64 + version) % 4;
    let written_at = version.checked_sub(versions_since_last_write)?;
    (written_at % 5 != 4).then(|| TestField::new(written_at as u32))
}

fn model_batch(version: u64) -> VersionedSchemaBatch<LiveKeys> {
    let mut batch = VersionedSchemaBatch::<LiveKeys>::default();
    for i in (0..NUM_KEYS).filter(|i| (*i as u64 + version).is_multiple_of(4)) {
        match model_value(i, version) {
            Some(value) => batch.put_versioned(model_key(i), value),
            None => batch.delete_versioned(model_key(i)),
        }
    }
    batch
}

fn committed_version(db: &V) -> u64 {
    db.get_committed_version_live_db()
        .unwrap()
        .expect("Version 0 is committed before the readers start")
}

/// Stops the readers when the writer is done, also if the writer panics.
struct StopOnDrop<'a>(&'a AtomicBool);

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// One pass of a reader of the live value: the value returned for a key must be the value
/// of that key at one of the versions that were committed while the read ran. Anything older
/// is a stale cache entry, anything newer was visible before its version was committed.
fn check_live_values(db: &V) {
    for i in 0..NUM_KEYS {
        let key = model_key(i);
        let version_before = committed_version(db);
        let value = db.get_live_value(&key).unwrap();
        let version_after = committed_version(db);
        assert!(
            (version_before..=version_after).any(|version| model_value(i, version) == value),
            "Key {i} read {value:?}, which is not its value at any version in {version_before}..={version_after}"
        );
    }
}

/// One pass of a reader pinned at a committed version: it must read the state as of exactly
/// that version, however many versions are committed in the meantime.
fn check_pinned_reader(db: &Arc<V>) {
    let version = committed_version(db);
    let reader = pinned_reader(db, version);
    for i in 0..NUM_KEYS {
        assert_eq!(
            reader.get_latest_borrowed(&model_key(i)).unwrap(),
            model_value(i, version),
            "Key {i} read through a reader pinned at version {version}"
        );
    }
}

/// One attempt to iterate at a committed version. Returns false if a newer version was
/// committed before the iterator took the read lock, which the iterator refuses.
fn check_iterator(db: &Arc<V>) -> bool {
    let version = committed_version(db);
    let reader = pinned_reader(db, version);
    let iter = match reader.iter_with_prefix(TestKey::from(KEY_PREFIX.to_vec())) {
        Ok(iter) => iter,
        Err(err) => {
            assert!(
                err.to_string().contains("Version on disk is newer"),
                "Unexpected iterator error: {err}"
            );
            return false;
        }
    };
    let seen: Vec<_> = iter.collect();
    let expected: Vec<_> = (0..NUM_KEYS)
        .filter_map(|i| model_value(i, version).map(|value| (model_key(i), Some(value))))
        .collect();
    assert_eq!(seen, expected, "Iteration at version {version}");
    true
}

#[test]
fn stress_readers_match_model_while_commits_run() {
    let (_tmpdir, db, cache) = open_versioned();
    // Entries weigh about 120, so this holds roughly half of the keys: both the cached and
    // the uncached read paths stay in use.
    db.set_cache_size(200, 25_000);
    db.commit(&model_batch(0), 0).unwrap();

    let done = AtomicBool::new(false);
    let live_passes = AtomicU64::new(0);
    let pinned_passes = AtomicU64::new(0);
    let iterations = AtomicU64::new(0);

    std::thread::scope(|s| {
        for _ in 0..3 {
            s.spawn(|| {
                while !done.load(Ordering::Relaxed) {
                    check_live_values(&db);
                    live_passes.fetch_add(1, Ordering::Relaxed);
                }
            });
            s.spawn(|| {
                while !done.load(Ordering::Relaxed) {
                    check_pinned_reader(&db);
                    pinned_passes.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        for _ in 0..2 {
            s.spawn(|| {
                while !done.load(Ordering::Relaxed) {
                    if check_iterator(&db) {
                        iterations.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }

        let _stop = StopOnDrop(&done);
        for version in 1..=LAST_VERSION {
            db.commit(&model_batch(version), version).unwrap();
        }
    });

    // Make sure that the test did what it claims to do.
    let (hits, misses) = (db.cache_hits(), db.cache_misses());
    println!(
        "live passes: {live_passes:?}, pinned passes: {pinned_passes:?}, iterations: {iterations:?}, cache hits: {hits}, cache misses: {misses}"
    );
    assert!(live_passes.into_inner() > 0);
    assert!(pinned_passes.into_inner() > 0);
    assert!(iterations.into_inner() > 0);
    assert!(hits > 0, "The cache was never hit");
    assert!(misses > 0, "The cache was never missed");

    // With all threads gone, whatever is left in the cache must agree with the live DB.
    let mut cached = 0;
    for i in 0..NUM_KEYS {
        let key = model_key(i);
        let expected = model_value(i, LAST_VERSION);
        let on_disk = db.live_db().get_raw::<LiveKeys>(key.as_ref()).unwrap();
        assert_eq!(on_disk, expected, "Key {i} in the live DB");
        if let Some(in_cache) = cache.peek(&key) {
            cached += 1;
            assert_eq!(in_cache, expected, "Key {i} is stale in the cache");
        }
        assert_eq!(db.get_live_value(&key).unwrap(), expected, "Key {i}");
    }
    assert!(cached > 0, "Nothing was left in the cache to check");
    assert!(cached < NUM_KEYS, "The cache held every key");
}
