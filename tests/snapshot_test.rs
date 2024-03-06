use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

use rockbound::cache::cache_container::CacheContainer;
use rockbound::cache::cache_db::CacheDb;
use rockbound::schema::ColumnFamilyName;
use rockbound::test::TestField;
use rockbound::{define_schema, ReadOnlyLock, Schema, DB};
use rocksdb::DEFAULT_COLUMN_FAMILY_NAME;

define_schema!(TestSchema1, TestField, TestField, "TestCF1");

type S = TestSchema1;

fn get_column_families() -> Vec<ColumnFamilyName> {
    vec![DEFAULT_COLUMN_FAMILY_NAME, S::COLUMN_FAMILY_NAME]
}

fn open_db(dir: impl AsRef<Path>) -> DB {
    let mut db_opts = rocksdb::Options::default();
    db_opts.create_if_missing(true);
    db_opts.create_missing_column_families(true);
    DB::open(dir, "test", get_column_families(), &db_opts).expect("Failed to open DB.")
}

#[test]
fn snapshot_lifecycle() {
    let tmpdir = tempfile::tempdir().unwrap();
    let db = open_db(&tmpdir);

    let to_parent = Arc::new(RwLock::new(HashMap::new()));
    {
        let mut to_parent = to_parent.write().unwrap();
        to_parent.insert(1, 0);
        to_parent.insert(2, 1);
    }
    let manager = Arc::new(RwLock::new(CacheContainer::new(
        db,
        to_parent.clone().into(),
    )));

    let key = TestField(1);
    let value = TestField(1);

    let snapshot_1 = CacheDb::new(0, ReadOnlyLock::new(manager.clone()));
    assert_eq!(
        None,
        snapshot_1.read::<S>(&key).unwrap(),
        "Incorrect value, should find nothing"
    );

    snapshot_1.put::<S>(&key, &value).unwrap();
    assert_eq!(
        Some(value),
        snapshot_1.read::<S>(&key).unwrap(),
        "Incorrect value, should be fetched from local cache"
    );
    {
        let mut manager = manager.write().unwrap();
        manager.add_snapshot(snapshot_1.into()).unwrap();
    }

    // Snapshot 2: reads value from snapshot 1, then deletes it
    let snapshot_2 = CacheDb::new(1, ReadOnlyLock::new(manager.clone()));
    assert_eq!(Some(value), snapshot_2.read::<S>(&key).unwrap());
    snapshot_2.delete::<S>(&key).unwrap();
    assert_eq!(None, snapshot_2.read::<S>(&key).unwrap());
    {
        let mut manager = manager.write().unwrap();
        manager.add_snapshot(snapshot_2.into()).unwrap();
        let mut to_parent = to_parent.write().unwrap();
        to_parent.insert(1, 0);
    }

    // Snapshot 3: gets empty result, event value is in some previous snapshots
    let snapshot_3 = CacheDb::new(2, ReadOnlyLock::new(manager));
    {
        let mut to_parent = to_parent.write().unwrap();
        to_parent.insert(2, 1);
    }
    assert_eq!(None, snapshot_3.read::<S>(&key).unwrap());
}
