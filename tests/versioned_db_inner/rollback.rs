use super::*;

#[test]
fn test_rollback_simple_put() {
    let test_db = TestDB::new();
    let db = Arc::new(test_db.db);
    let cache = VersionedDbCache::new(1_000_000);

    let versioned_db = VersionedDB::from_dbs(db.clone(), db.clone(), cache).unwrap();

    // Version 1: Insert key "a" with value 1
    put_keys(&versioned_db, &[(b"a", 1)], 1);

    // Version 2: Update key "a" with value 2
    put_keys(&versioned_db, &[(b"a", 2)], 2);

    // Verify current state
    assert_eq!(versioned_db.get_committed_version().unwrap(), Some(2));
    assert_eq!(
        versioned_db
            .get_live_value(&TestKey::from(b"a".to_vec()))
            .unwrap(),
        Some(TestField::new(2))
    );

    // Rollback to version 1
    let new_version = versioned_db.rollback_last_version().unwrap();
    assert_eq!(new_version, 1);

    // Verify rolled back state
    assert_eq!(versioned_db.get_committed_version().unwrap(), Some(1));
    assert_eq!(
        versioned_db
            .get_live_value(&TestKey::from(b"a".to_vec()))
            .unwrap(),
        Some(TestField::new(1))
    );
}

#[test]
fn test_rollback_with_delete() {
    let test_db = TestDB::new();
    let db = Arc::new(test_db.db);
    let cache = VersionedDbCache::new(1_000_000);

    let versioned_db = VersionedDB::from_dbs(db.clone(), db.clone(), cache).unwrap();

    // Version 1: Insert key "a" with value 1
    put_keys(&versioned_db, &[(b"a", 1)], 1);

    // Version 2: Delete key "a"
    delete_keys(&versioned_db, &[b"a"], 2);

    // Verify current state
    assert_eq!(versioned_db.get_committed_version().unwrap(), Some(2));
    assert_eq!(
        versioned_db
            .get_live_value(&TestKey::from(b"a".to_vec()))
            .unwrap(),
        None
    );

    // Rollback to version 1
    let new_version = versioned_db.rollback_last_version().unwrap();
    assert_eq!(new_version, 1);

    // Verify rolled back state - key should be restored
    assert_eq!(versioned_db.get_committed_version().unwrap(), Some(1));
    assert_eq!(
        versioned_db
            .get_live_value(&TestKey::from(b"a".to_vec()))
            .unwrap(),
        Some(TestField::new(1))
    );
}

#[test]
fn test_rollback_multiple_keys() {
    let test_db = TestDB::new();
    let db = Arc::new(test_db.db);
    let cache = VersionedDbCache::new(1_000_000);

    let versioned_db = VersionedDB::from_dbs(db.clone(), db.clone(), cache).unwrap();

    // Version 1: Insert keys a=1, b=2, c=3
    put_keys(&versioned_db, &[(b"a", 1), (b"b", 2), (b"c", 3)], 1);

    // Version 2: Update all keys
    put_keys(&versioned_db, &[(b"a", 10), (b"b", 20), (b"c", 30)], 2);

    // Verify current state
    assert_eq!(versioned_db.get_committed_version().unwrap(), Some(2));

    // Rollback to version 1
    let new_version = versioned_db.rollback_last_version().unwrap();
    assert_eq!(new_version, 1);

    // Verify all keys are restored
    assert_eq!(versioned_db.get_committed_version().unwrap(), Some(1));
    assert_eq!(
        versioned_db
            .get_live_value(&TestKey::from(b"a".to_vec()))
            .unwrap(),
        Some(TestField::new(1))
    );
    assert_eq!(
        versioned_db
            .get_live_value(&TestKey::from(b"b".to_vec()))
            .unwrap(),
        Some(TestField::new(2))
    );
    assert_eq!(
        versioned_db
            .get_live_value(&TestKey::from(b"c".to_vec()))
            .unwrap(),
        Some(TestField::new(3))
    );
}

#[test]
fn test_rollback_new_key_insertion() {
    let test_db = TestDB::new();
    let db = Arc::new(test_db.db);
    let cache = VersionedDbCache::new(1_000_000);

    let versioned_db = VersionedDB::from_dbs(db.clone(), db.clone(), cache).unwrap();

    // Version 1: Insert key "a" with value 1
    put_keys(&versioned_db, &[(b"a", 1)], 1);

    // Version 2: Insert new key "b" with value 2
    put_keys(&versioned_db, &[(b"b", 2)], 2);

    // Verify current state
    assert_eq!(versioned_db.get_committed_version().unwrap(), Some(2));
    assert_eq!(
        versioned_db
            .get_live_value(&TestKey::from(b"b".to_vec()))
            .unwrap(),
        Some(TestField::new(2))
    );

    // Rollback to version 1
    let new_version = versioned_db.rollback_last_version().unwrap();
    assert_eq!(new_version, 1);

    // Verify key "b" is deleted and key "a" is unchanged
    assert_eq!(versioned_db.get_committed_version().unwrap(), Some(1));
    assert_eq!(
        versioned_db
            .get_live_value(&TestKey::from(b"a".to_vec()))
            .unwrap(),
        Some(TestField::new(1))
    );
    assert_eq!(
        versioned_db
            .get_live_value(&TestKey::from(b"b".to_vec()))
            .unwrap(),
        None
    );
}

#[test]
fn test_rollback_version_zero_fails() {
    let test_db = TestDB::new();
    let db = Arc::new(test_db.db);
    let cache = VersionedDbCache::new(1_000_000);

    let versioned_db = VersionedDB::from_dbs(db.clone(), db.clone(), cache).unwrap();

    // Version 0: Insert key "a" with value 1
    put_keys(&versioned_db, &[(b"a", 1)], 0);

    // Attempt to rollback should fail
    let result = versioned_db.rollback_last_version();
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Cannot rollback version 0"));
}

#[test]
fn test_rollback_empty_db_fails() {
    let test_db = TestDB::new();
    let db = Arc::new(test_db.db);
    let cache = VersionedDbCache::new(1_000_000);

    let versioned_db = VersionedDB::from_dbs(db.clone(), db.clone(), cache).unwrap();

    // Attempt to rollback empty DB should fail
    let result = versioned_db.rollback_last_version();
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("no versions committed"));
}
