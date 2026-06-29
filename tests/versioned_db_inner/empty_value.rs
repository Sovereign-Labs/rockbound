use rockbound::versioned_db::VersionedSchemaBatch;

use crate::versioned_db_inner::{setup_bytevec_db, ByteVecSchema, TestByteVec, TestKey};

#[test]
#[should_panic(expected = "Versioned values may not be zero-length")]
fn test_put_versioned_empty_value_panics() {
    let mut batch = VersionedSchemaBatch::<ByteVecSchema>::default();
    batch.put_versioned(TestKey::from(b"k".to_vec()), TestByteVec(Vec::new()));
}

#[test]
fn test_put_versioned_non_empty_round_trip() {
    let (_tmpdir, vdb) = setup_bytevec_db();
    let key = TestKey::from(b"k".to_vec());
    let value = TestByteVec(vec![0x42]);

    let mut batch = VersionedSchemaBatch::<ByteVecSchema>::default();
    batch.put_versioned(key.clone(), value.clone());
    vdb.commit(&batch, 0).unwrap();

    assert_eq!(vdb.get_live_value(&key).unwrap(), Some(value.clone()));
    assert_eq!(vdb.get_historical_value(&key, 0).unwrap(), Some(value));
}

#[test]
fn test_delete_versioned_still_hides_key() {
    let (_tmpdir, vdb) = setup_bytevec_db();
    let key = TestKey::from(b"k".to_vec());
    let value_v0 = TestByteVec(vec![0x01]);

    let mut b0 = VersionedSchemaBatch::<ByteVecSchema>::default();
    b0.put_versioned(key.clone(), value_v0.clone());
    vdb.commit(&b0, 0).unwrap();

    let mut b1 = VersionedSchemaBatch::<ByteVecSchema>::default();
    b1.delete_versioned(key.clone());
    vdb.commit(&b1, 1).unwrap();

    assert_eq!(vdb.get_historical_value(&key, 0).unwrap(), Some(value_v0));
    assert_eq!(vdb.get_historical_value(&key, 1).unwrap(), None);
    assert_eq!(vdb.get_live_value(&key).unwrap(), None);
}

#[test]
#[should_panic(expected = "Versioned values may not have zero-length")]
fn test_commit_loop_empty_value_panics_if_bypassed() {
    let (_tmpdir, vdb) = setup_bytevec_db();
    let batch = VersionedSchemaBatch::<ByteVecSchema>::from([(
        TestKey::from(b"k".to_vec()),
        Some(TestByteVec(Vec::new())),
    )]);
    let _ = vdb.commit(&batch, 0);
}
