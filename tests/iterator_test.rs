// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use rockbound::schema::{KeyDecoder, KeyEncoder, ValueCodec};
use rockbound::test::{KeyPrefix1, KeyPrefix2, TestCompositeField, TestField};
use rockbound::{
    define_schema, Operation, Schema, SchemaBatch, SchemaIterator, SeekKeyEncoder, DB,
};
use rocksdb::DEFAULT_COLUMN_FAMILY_NAME;
use tempfile::TempDir;

define_schema!(TestSchema, TestCompositeField, TestField, "TestCF");

type S = TestSchema;

fn collect_values(iter: SchemaIterator<S>) -> Vec<u32> {
    iter.map(|row| row.unwrap().value.0).collect()
}

fn decode_key(key: &[u8]) -> TestCompositeField {
    <TestCompositeField as KeyDecoder<S>>::decode_key(key).unwrap()
}

fn encode_key(key: &TestCompositeField) -> Vec<u8> {
    <TestCompositeField as KeyEncoder<S>>::encode_key(key).unwrap()
}

fn encode_value(value: &TestField) -> Vec<u8> {
    <TestField as ValueCodec<S>>::encode_value(value).unwrap()
}

struct TestDB {
    _tmpdir: TempDir,
    db: DB,
}

fn open_inner_db(path: &std::path::Path) -> DB {
    let column_families = vec![DEFAULT_COLUMN_FAMILY_NAME, S::COLUMN_FAMILY_NAME];
    let mut db_opts = rocksdb::Options::default();
    db_opts.create_if_missing(true);
    db_opts.create_missing_column_families(true);
    DB::open(
        path,
        "test-iterator-db",
        column_families,
        &db_opts,
        1_000_000,
    )
    .unwrap()
}

impl TestDB {
    fn new() -> Self {
        let tmpdir = tempfile::tempdir().unwrap();
        let db = open_inner_db(tmpdir.path());

        db.put::<S>(&TestCompositeField(1, 0, 0), &TestField(100))
            .unwrap();
        db.put::<S>(&TestCompositeField(1, 0, 2), &TestField(102))
            .unwrap();
        db.put::<S>(&TestCompositeField(1, 0, 4), &TestField(104))
            .unwrap();
        db.put::<S>(&TestCompositeField(1, 1, 0), &TestField(110))
            .unwrap();
        db.put::<S>(&TestCompositeField(1, 1, 2), &TestField(112))
            .unwrap();
        db.put::<S>(&TestCompositeField(1, 1, 4), &TestField(114))
            .unwrap();
        db.put::<S>(&TestCompositeField(2, 0, 0), &TestField(200))
            .unwrap();
        db.put::<S>(&TestCompositeField(2, 0, 2), &TestField(202))
            .unwrap();

        TestDB {
            _tmpdir: tmpdir,
            db,
        }
    }
}

impl TestDB {
    fn iter(&self) -> SchemaIterator<'_, S> {
        self.db.iter().expect("Failed to create iterator.")
    }

    fn rev_iter(&self) -> SchemaIterator<'_, S> {
        self.db.iter().expect("Failed to create iterator.").rev()
    }
}

impl std::ops::Deref for TestDB {
    type Target = DB;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

#[test]
fn test_seek_to_first() {
    let db = TestDB::new();
    let mut iter;

    // Forward iterator from the beginning should return all values.
    iter = db.iter();
    iter.seek_to_first();
    assert_eq!(
        collect_values(iter),
        [100, 102, 104, 110, 112, 114, 200, 202]
    );

    // Similarly, forward iterator *without an explicit seek call* should return
    // all values.
    iter = db.iter();
    assert_eq!(
        collect_values(iter),
        [100, 102, 104, 110, 112, 114, 200, 202]
    );

    // Reverse iterator from the beginning should only return the first value.
    iter = db.rev_iter();
    iter.seek_to_first();
    assert_eq!(collect_values(iter), [100]);
}

#[test]
fn test_seek_to_last() {
    let db = TestDB::new();

    let mut iter = db.iter();
    iter.seek_to_last();
    assert_eq!(collect_values(iter), [202]);

    let mut iter = db.rev_iter();
    iter.seek_to_last();
    assert_eq!(
        collect_values(iter),
        [202, 200, 114, 112, 110, 104, 102, 100]
    );
}

#[test]
fn test_seek_by_existing_key() {
    let db = TestDB::new();

    let mut iter = db.iter();
    iter.seek(&TestCompositeField(1, 1, 0)).unwrap();
    assert_eq!(collect_values(iter), [110, 112, 114, 200, 202]);

    let mut iter = db.rev_iter();
    iter.seek(&TestCompositeField(1, 1, 0)).unwrap();
    assert_eq!(collect_values(iter), [110, 104, 102, 100]);
}

#[test]
fn test_seek_by_nonexistent_key() {
    let db = TestDB::new();

    let mut iter = db.iter();
    iter.seek(&TestCompositeField(1, 1, 1)).unwrap();
    assert_eq!(collect_values(iter), [112, 114, 200, 202]);

    let mut iter = db.rev_iter();
    iter.seek(&TestCompositeField(1, 1, 1)).unwrap();
    assert_eq!(collect_values(iter), [112, 110, 104, 102, 100]);
}

#[test]
fn test_seek_for_prev_by_existing_key() {
    let db = TestDB::new();

    let mut iter = db.iter();
    iter.seek_for_prev(&TestCompositeField(1, 1, 0)).unwrap();
    assert_eq!(collect_values(iter), [110, 112, 114, 200, 202]);

    let mut iter = db.rev_iter();
    iter.seek_for_prev(&TestCompositeField(1, 1, 0)).unwrap();
    assert_eq!(collect_values(iter), [110, 104, 102, 100]);
}

#[test]
fn test_seek_for_prev_by_nonexistent_key() {
    let db = TestDB::new();

    let mut iter = db.iter();
    iter.seek_for_prev(&TestCompositeField(1, 1, 1)).unwrap();
    assert_eq!(collect_values(iter), [110, 112, 114, 200, 202]);

    let mut iter = db.rev_iter();
    iter.seek_for_prev(&TestCompositeField(1, 1, 1)).unwrap();
    assert_eq!(collect_values(iter), [110, 104, 102, 100]);
}

#[test]
fn test_seek_by_1prefix() {
    let db = TestDB::new();

    let mut iter = db.iter();
    iter.seek(&KeyPrefix1(2)).unwrap();
    assert_eq!(collect_values(iter), [200, 202]);

    let mut iter = db.rev_iter();
    iter.seek(&KeyPrefix1(2)).unwrap();
    assert_eq!(collect_values(iter), [200, 114, 112, 110, 104, 102, 100]);
}

#[test]
fn test_seek_for_prev_by_1prefix() {
    let db = TestDB::new();

    let mut iter = db.iter();
    iter.seek_for_prev(&KeyPrefix1(2)).unwrap();
    assert_eq!(collect_values(iter), [114, 200, 202]);

    let mut iter = db.rev_iter();
    iter.seek_for_prev(&KeyPrefix1(2)).unwrap();
    assert_eq!(collect_values(iter), [114, 112, 110, 104, 102, 100]);
}

#[test]
fn test_seek_by_2prefix() {
    let db = TestDB::new();

    let mut iter = db.iter();
    iter.seek(&KeyPrefix2(2, 0)).unwrap();
    assert_eq!(collect_values(iter), [200, 202]);

    let mut iter = db.rev_iter();
    iter.seek(&KeyPrefix2(2, 0)).unwrap();
    assert_eq!(collect_values(iter), [200, 114, 112, 110, 104, 102, 100]);
}

#[test]
fn test_seek_for_prev_by_2prefix() {
    let db = TestDB::new();

    let mut iter = db.iter();
    iter.seek_for_prev(&KeyPrefix2(2, 0)).unwrap();
    assert_eq!(collect_values(iter), [114, 200, 202]);

    let mut iter = db.rev_iter();
    iter.seek_for_prev(&KeyPrefix2(2, 0)).unwrap();
    assert_eq!(collect_values(iter), [114, 112, 110, 104, 102, 100]);
}

#[test]
fn test_schema_batch_iteration_order() {
    let mut batch = SchemaBatch::new();

    // Operations in expected order
    let operations = vec![
        (TestCompositeField(2, 0, 0), TestField(600)),
        (TestCompositeField(1, 3, 4), TestField(500)),
        (TestCompositeField(1, 3, 3), TestField(400)),
        (TestCompositeField(1, 3, 2), TestField(300)),
        (TestCompositeField(1, 3, 0), TestField(200)),
        (TestCompositeField(1, 2, 0), TestField(100)),
    ];

    // Insert them out of order
    for i in [4, 2, 0, 1, 3, 5] {
        let (key, value) = &operations[i];
        batch.put::<S>(key, value).unwrap();
    }

    let iter = batch.iter::<S>().rev();
    let collected: Vec<_> = iter
        .filter_map(|(key, value)| match value {
            Operation::Put { value } => Some((
                decode_key(key),
                <TestField as ValueCodec<S>>::decode_value(value).unwrap(),
            )),
            Operation::Delete | Operation::DeleteRange { .. } => None,
        })
        .collect();
    assert_eq!(operations, collected);
}

#[test]
fn test_schema_batch_iteration_with_deletions() {
    let mut batch = SchemaBatch::new();

    batch
        .put::<S>(&TestCompositeField(8, 0, 0), &TestField(6))
        .unwrap();
    batch.delete::<S>(&TestCompositeField(9, 0, 0)).unwrap();
    batch
        .put::<S>(&TestCompositeField(12, 0, 0), &TestField(1))
        .unwrap();
    batch
        .put::<S>(&TestCompositeField(1, 0, 0), &TestField(2))
        .unwrap();
    let mut iter = batch.iter::<S>().rev().peekable();
    let first1 = iter.peek().unwrap();
    assert_eq!(first1.0, &encode_key(&TestCompositeField(12, 0, 0)));
    assert_eq!(
        first1.1,
        &Operation::Put {
            value: encode_value(&TestField(1)),
        }
    );
    let collected: Vec<_> = iter.collect();
    assert_eq!(4, collected.len());
}

#[test]
fn test_schema_batch_iter_range() {
    let mut batch = SchemaBatch::new();

    batch
        .put::<S>(&TestCompositeField(8, 0, 0), &TestField(5))
        .unwrap();
    batch.delete::<S>(&TestCompositeField(9, 0, 0)).unwrap();
    batch
        .put::<S>(&TestCompositeField(8, 1, 0), &TestField(3))
        .unwrap();

    batch
        .put::<S>(&TestCompositeField(11, 0, 0), &TestField(6))
        .unwrap();
    batch
        .put::<S>(&TestCompositeField(13, 0, 0), &TestField(2))
        .unwrap();
    batch
        .put::<S>(&TestCompositeField(12, 0, 0), &TestField(1))
        .unwrap();

    let seek_key =
        <TestCompositeField as SeekKeyEncoder<S>>::encode_seek_key(&TestCompositeField(11, 0, 0))
            .unwrap();

    let mut iter = batch.iter_range::<S>(..=seek_key).rev();

    assert_eq!(
        Some((
            &encode_key(&TestCompositeField(11, 0, 0)),
            &Operation::Put {
                value: encode_value(&TestField(6))
            }
        )),
        iter.next()
    );
    assert_eq!(
        Some((
            &encode_key(&TestCompositeField(9, 0, 0)),
            &Operation::Delete
        )),
        iter.next()
    );
    assert_eq!(
        Some((
            &encode_key(&TestCompositeField(8, 1, 0)),
            &Operation::Put {
                value: encode_value(&TestField(3))
            }
        )),
        iter.next()
    );
    assert_eq!(
        Some((
            &encode_key(&TestCompositeField(8, 0, 0)),
            &Operation::Put {
                value: encode_value(&TestField(5))
            }
        )),
        iter.next()
    );
    assert_eq!(None, iter.next());
}
