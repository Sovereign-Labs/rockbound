use std::collections::{btree_map, BTreeMap, HashMap};

use crate::metrics::SCHEMADB_BATCH_PUT_LATENCY_SECONDS;
use crate::schema::{ColumnFamilyName, KeyCodec, KeyEncoder, ValueCodec};
use crate::{Operation, Schema, SchemaKey, SchemaValue, SeekKeyEncoder};

// [`SchemaBatch`] holds a collection of updates that can be applied to a DB
/// ([`Schema`]) atomically. The updates will be applied in the order in which
/// they are added to the [`SchemaBatch`].
#[derive(Debug, Clone)]
pub struct SchemaBatch<K = SchemaKey, V = SchemaValue> {
    pub(crate) last_writes: HashMap<ColumnFamilyName, BTreeMap<K, Operation<K, V>>>,
    pub(crate) range_ops: HashMap<ColumnFamilyName, Vec<Operation<K, V>>>,
}

impl<K, V> Default for SchemaBatch<K, V> {
    fn default() -> Self {
        Self {
            last_writes: HashMap::new(),
            range_ops: HashMap::new(),
        }
    }
}

impl SchemaBatch {
    /// Adds an insert/update operation to the batch.
    pub fn put<S: Schema>(
        &mut self,
        key: &impl KeyEncoder<S>,
        value: &impl ValueCodec<S>,
    ) -> anyhow::Result<()> {
        let _timer = SCHEMADB_BATCH_PUT_LATENCY_SECONDS
            .with_label_values(&["unknown"])
            .start_timer();

        let key = key.encode_key()?;
        self.put_raw::<S>(key, value.encode_value()?)?;

        Ok(())
    }

    /// Adds a delete operation to the batch.
    pub fn delete<S: Schema>(&mut self, key: &impl KeyEncoder<S>) -> anyhow::Result<()> {
        let key = key.encode_key()?;
        self.delete_raw::<S>(key)?;

        Ok(())
    }

    /// Adds a delete range operation to the batch.
    ///
    /// Note: Range based operations aren't reflected by the batch iterators
    /// ([`SchemaBatch::iter`] / [`SchemaBatch::iter_range`]), consistent with rocksdb
    /// `WriteBatch`. They are preserved by [`SchemaBatch::merge`].
    pub fn delete_range<S: Schema>(
        &mut self,
        from: &impl SeekKeyEncoder<S>,
        to: &impl SeekKeyEncoder<S>,
    ) -> anyhow::Result<()> {
        self.push_range_op_cf(
            S::COLUMN_FAMILY_NAME,
            from.encode_seek_key()?,
            to.encode_seek_key()?,
        );
        Ok(())
    }

    /// Getting the operation from current schema batch if present
    pub(crate) fn get_operation<S: Schema>(
        &self,
        key: &impl KeyCodec<S>,
    ) -> anyhow::Result<Option<&Operation>> {
        let key = key.encode_key()?;
        self.get_operation_raw::<S>(&key)
    }

    /// Getting value by key if it was written in this batch.
    /// Deleted operation will return None as well as missing key
    pub fn get_value<S: Schema>(&self, key: &impl KeyCodec<S>) -> anyhow::Result<Option<S::Value>> {
        let operation = self.get_operation(key)?;
        if let Some(operation) = operation {
            let value = operation.decode_value::<S>()?;
            return Ok(value);
        }
        Ok(None)
    }
}

impl<K: Ord, V> SchemaBatch<K, V> {
    /// Creates an empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if this batch has no point writes and no range operations.
    pub fn is_empty(&self) -> bool {
        self.last_writes.values().all(|writes| writes.is_empty())
            && self.range_ops.values().all(|ops| ops.is_empty())
    }

    /// Put a pre-encoded key and its value into the batch.
    pub fn put_raw<S: Schema>(&mut self, key: K, value: V) -> anyhow::Result<()> {
        let put_operation = Operation::Put { value };
        self.insert_operation::<S>(key, put_operation);
        Ok(())
    }

    /// Delete a pre-encoded key from the batch. A schema must be provided to ensure the key is deleted from the correct column family.
    pub fn delete_raw<S: Schema>(&mut self, key: K) -> anyhow::Result<()> {
        self.insert_operation::<S>(key, Operation::Delete);
        Ok(())
    }

    /// Add a delete op against a column family known only at runtime.
    ///
    /// Use this when the CF name comes from a trait constant or other runtime source
    /// rather than from a `Schema` impl in scope. Mirrors [`Self::delete_raw`] but takes
    /// the CF name as an argument instead of inferring it from `S::COLUMN_FAMILY_NAME`.
    pub(crate) fn delete_cf_raw(&mut self, cf_name: ColumnFamilyName, key: K) {
        self.insert_operation_cf(cf_name, key, Operation::Delete);
    }

    /// Add a put op against a column family known only at runtime.
    ///
    /// Use this when the CF name comes from a trait constant or other runtime source
    /// rather than from a `Schema` impl in scope. Mirrors [`Self::put_raw`] but takes
    /// the CF name as an argument instead of inferring it from `S::COLUMN_FAMILY_NAME`.
    pub(crate) fn put_cf_raw(&mut self, cf_name: ColumnFamilyName, key: K, value: V) {
        self.insert_operation_cf(cf_name, key, Operation::Put { value });
    }

    /// Add a delete-range op against a column family known only at runtime.
    ///
    /// Mirrors [`Self::delete_range`] but takes the CF name as an argument and raw,
    /// already-encoded bounds instead of inferring the CF from a `Schema` in scope.
    /// The range is `[from, to)` (inclusive `from`, exclusive `to`), matching RocksDB's
    /// `delete_range_cf`.
    pub(crate) fn delete_range_cf_raw(&mut self, cf_name: ColumnFamilyName, from: K, to: K) {
        self.push_range_op_cf(cf_name, from, to);
    }

    fn insert_operation<S: Schema>(&mut self, key: K, operation: Operation<K, V>) {
        self.insert_operation_cf(S::COLUMN_FAMILY_NAME, key, operation);
    }

    fn insert_operation_cf(
        &mut self,
        cf_name: ColumnFamilyName,
        key: K,
        operation: Operation<K, V>,
    ) {
        let column_writes = self.last_writes.entry(cf_name).or_default();
        column_writes.insert(key, operation);
    }

    fn push_range_op_cf(&mut self, cf_name: ColumnFamilyName, from: K, to: K) {
        self.range_ops
            .entry(cf_name)
            .or_default()
            .push(Operation::DeleteRange { from, to });
    }

    /// Getting the operation from current schema batch if present
    pub(crate) fn get_operation_raw<S: Schema>(
        &self,
        key: &K,
    ) -> anyhow::Result<Option<&Operation<K, V>>> {
        if let Some(column_writes) = self.last_writes.get(&S::COLUMN_FAMILY_NAME) {
            Ok(column_writes.get(key))
        } else {
            Ok(None)
        }
    }

    /// Iterator over all values in lexicographic order.
    pub fn iter<S: Schema>(&self) -> btree_map::Iter<'_, K, Operation<K, V>> {
        self.last_writes
            .get(&S::COLUMN_FAMILY_NAME)
            .map(BTreeMap::iter)
            .unwrap_or_default()
    }

    /// Iterator in given range in lexicographic order.
    pub fn iter_range<S: Schema>(
        &self,
        range: impl std::ops::RangeBounds<K>,
    ) -> btree_map::Range<'_, K, Operation<K, V>> {
        self.last_writes
            .get(&S::COLUMN_FAMILY_NAME)
            .map(|column_writes| column_writes.range(range))
            .unwrap_or_default()
    }

    /// Merge other [`SchemaBatch`] on top of this one.
    ///
    /// Point writes are combined per key with last-write-wins (keys from `other` overwrite
    /// keys in `self`). Range deletes have no key to overwrite, so both batches' range
    /// tombstones are retained (appended per column family). Earlier range tombstones are
    /// split around later puts from `other` so those puts survive the merged write batch.
    ///
    /// The split is necessary because a written batch applies all point ops before all
    /// range ops per column family (see [`crate::DB::update_db_batch_with_schema_data`]),
    /// so within one batch a range tombstone always wins over an overlapping put. Splitting
    /// `self`'s earlier ranges around `other`'s later puts restores last-write-wins across
    /// the merge.
    ///
    /// The `K: Clone + Extend<u8>` bound exists only for that split: it lets us compute
    /// the next key after a preserved put (see `range_delete_key_after`) so the re-emitted
    /// ranges exclude exactly that key. In practice `K` is always [`SchemaKey`]
    /// (`Vec<u8>`), which satisfies the bound; it is stated generically only because
    /// `SchemaBatch` is generic over `K`.
    pub fn merge(&mut self, other: SchemaBatch<K, V>)
    where
        K: Clone + Extend<u8>,
    {
        self.preserve_later_puts_from_earlier_range_ops(&other.last_writes);

        for (cf_name, other_cf_map) in other.last_writes {
            let cf_map = self.last_writes.entry(cf_name).or_default();
            cf_map.extend(other_cf_map);
        }
        for (cf_name, other_cf_ops) in other.range_ops {
            let cf_ops = self.range_ops.entry(cf_name).or_default();
            cf_ops.extend(other_cf_ops);
        }
    }

    fn preserve_later_puts_from_earlier_range_ops(
        &mut self,
        later_writes: &HashMap<ColumnFamilyName, BTreeMap<K, Operation<K, V>>>,
    ) where
        K: Clone + Extend<u8>,
    {
        for (cf_name, later_writes_for_cf) in later_writes {
            if !later_writes_for_cf
                .values()
                .any(|operation| matches!(operation, Operation::Put { .. }))
            {
                continue;
            }

            let Some(range_ops) = self.range_ops.get_mut(cf_name) else {
                continue;
            };

            let mut preserved_range_ops = Vec::with_capacity(range_ops.len());
            for operation in std::mem::take(range_ops) {
                match operation {
                    Operation::DeleteRange { from, to } => {
                        preserved_range_ops.extend(split_delete_range_around_later_puts(
                            from,
                            to,
                            later_writes_for_cf,
                        ));
                    }
                    operation => preserved_range_ops.push(operation),
                }
            }
            *range_ops = preserved_range_ops;
        }
    }
}

fn split_delete_range_around_later_puts<K, V>(
    from: K,
    to: K,
    later_writes: &BTreeMap<K, Operation<K, V>>,
) -> Vec<Operation<K, V>>
where
    K: Ord + Clone + Extend<u8>,
{
    let mut ranges = vec![(from, to)];
    for (key, operation) in later_writes {
        if !matches!(operation, Operation::Put { .. }) {
            continue;
        }

        let mut next_ranges = Vec::with_capacity(ranges.len() + 1);
        for (from, to) in ranges {
            if key < &from || key >= &to {
                next_ranges.push((from, to));
                continue;
            }

            if &from < key {
                next_ranges.push((from, key.clone()));
            }

            let key_after = range_delete_key_after(key);
            if key_after < to {
                next_ranges.push((key_after, to));
            }
        }
        ranges = next_ranges;
    }

    ranges
        .into_iter()
        .map(|(from, to)| Operation::DeleteRange { from, to })
        .collect()
}

/// Returns the smallest key strictly greater than `key` under RocksDB's bytewise key
/// ordering, computed as `key ++ 0x00`. Used by [`SchemaBatch::merge`] to re-emit a range
/// delete that resumes just past a later put it must preserve. Requires `K: Extend<u8>`,
/// which is the substantive reason `merge` carries that bound.
fn range_delete_key_after<K>(key: &K) -> K
where
    K: Clone + Extend<u8>,
{
    let mut key_after = key.clone();
    key_after.extend(std::iter::once(0));
    key_after
}

#[cfg(feature = "arbitrary")]
impl proptest::arbitrary::Arbitrary for SchemaBatch {
    type Parameters = &'static [ColumnFamilyName];
    fn arbitrary_with(columns: Self::Parameters) -> Self::Strategy {
        use proptest::prelude::*;
        use proptest::strategy::Strategy;

        let point_ops_strategy = proptest::collection::vec(
            proptest::collection::btree_map(
                any::<SchemaKey>(),
                prop_oneof![
                    any::<crate::SchemaValue>().prop_map(|v| Operation::Put { value: v }),
                    Just(Operation::Delete)
                ],
                0..10,
            ),
            columns.len(),
        );

        let range_ops_strategy = proptest::collection::vec(
            proptest::collection::vec(
                (any::<SchemaKey>(), any::<SchemaKey>()).prop_map(|(start, end)| {
                    Operation::DeleteRange {
                        from: start,
                        to: end,
                    }
                }),
                0..5,
            ),
            columns.len(),
        );

        (point_ops_strategy, range_ops_strategy)
            .prop_map(|(point_ops, range_ops)| {
                let mut last_writes = HashMap::new();
                let mut range_operations = HashMap::new();

                for (col, point_op) in columns.iter().zip(point_ops.into_iter()) {
                    last_writes.insert(*col, point_op);
                }

                for (col, range_op) in columns.iter().zip(range_ops.into_iter()) {
                    range_operations.insert(*col, range_op);
                }

                SchemaBatch {
                    last_writes,
                    range_ops: range_operations,
                }
            })
            .boxed()
    }

    type Strategy = proptest::strategy::BoxedStrategy<Self>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::define_schema;
    use crate::schema::{KeyEncoder, ValueCodec};
    use crate::test::TestField;

    define_schema!(TestSchema1, TestField, TestField, "TestCF1");

    #[test]
    fn is_empty_tracks_point_and_range_ops() {
        let mut batch = SchemaBatch::new();
        assert!(batch.is_empty());

        batch
            .put::<TestSchema1>(&TestField(1), &TestField(10))
            .unwrap();
        assert!(!batch.is_empty());

        let mut batch = SchemaBatch::new();
        batch
            .delete_range::<TestSchema1>(&TestField(1), &TestField(2))
            .unwrap();
        assert!(!batch.is_empty());
    }

    mod range {
        use super::*;
        use crate::schema::KeyDecoder;

        #[test]
        fn test_delete_range_inserts_expected_operation() {
            let mut batch = SchemaBatch::new();
            let field_1 = TestField(1);
            let field_2 = TestField(5);
            batch
                .delete_range::<TestSchema1>(&field_1, &field_2)
                .unwrap();

            let op = &batch
                .range_ops
                .get(TestSchema1::COLUMN_FAMILY_NAME)
                .unwrap()[0];

            match op {
                Operation::DeleteRange { from, to } => {
                    let from_key =
                        <<TestSchema1 as Schema>::Key as KeyDecoder<TestSchema1>>::decode_key(from)
                            .unwrap()
                            .0;
                    assert_eq!(from_key, 1);
                    let to_key =
                        <<TestSchema1 as Schema>::Key as KeyDecoder<TestSchema1>>::decode_key(to)
                            .unwrap()
                            .0;
                    assert_eq!(to_key, 5);
                }
                _ => panic!("Incorrect Operation disctriminate"),
            };
        }
    }

    mod iter {
        use super::*;
        use crate::schema::KeyDecoder;

        #[test]
        fn empty_schema_batch_iterator() {
            let batch = SchemaBatch::<SchemaKey, SchemaValue>::new();
            let mut iter_forward = batch.iter::<TestSchema1>();
            assert_eq!(None, iter_forward.next());
            let mut iter_backward = batch.iter::<TestSchema1>().rev();
            assert_eq!(None, iter_backward.next());
        }

        fn collect_actual_values<'a, I: Iterator<Item = (&'a SchemaKey, &'a Operation)>>(
            iter: I,
        ) -> Vec<(u32, Option<u32>)> {
            iter.map(|(key, operation)| {
                let key =
                    <<TestSchema1 as Schema>::Key as KeyDecoder<TestSchema1>>::decode_key(key)
                        .unwrap()
                        .0;
                let value = match operation {
                    Operation::Put { value } => Some(
                        <<TestSchema1 as Schema>::Value as ValueCodec<TestSchema1>>::decode_value(
                            value,
                        )
                        .unwrap()
                        .0,
                    ),
                    Operation::Delete | Operation::DeleteRange { .. } => None,
                };
                (key, value)
            })
            .collect()
        }

        #[test]
        fn iterator() {
            let mut batch = SchemaBatch::new();

            let field_1 = TestField(1);
            let field_2 = TestField(2);
            let field_3 = TestField(3);
            let field_4 = TestField(4);

            batch.put::<TestSchema1>(&field_2, &field_1).unwrap();
            batch.put::<TestSchema1>(&field_1, &field_3).unwrap();
            batch.delete::<TestSchema1>(&field_4).unwrap();
            batch.put::<TestSchema1>(&field_3, &field_4).unwrap();

            let iter_forward = batch.iter::<TestSchema1>();
            let actual_values: Vec<(u32, Option<u32>)> = collect_actual_values(iter_forward);
            let expected_values = vec![(1, Some(3)), (2, Some(1)), (3, Some(4)), (4, None)];
            assert_eq!(expected_values, actual_values);

            let iter_backward = batch.iter::<TestSchema1>().rev();
            let actual_values: Vec<(u32, Option<u32>)> = collect_actual_values(iter_backward);
            let expected_values = vec![(4, None), (3, Some(4)), (2, Some(1)), (1, Some(3))];
            assert_eq!(expected_values, actual_values);
        }

        fn encode_key(field: &TestField) -> SchemaKey {
            <TestField as KeyEncoder<TestSchema1>>::encode_key(field).unwrap()
        }

        #[test]
        fn range_iterator() {
            let mut batch = SchemaBatch::new();

            let field_1 = TestField(1);
            let field_2 = TestField(2);
            let field_3 = TestField(3);
            let field_4 = TestField(4);
            let field_5 = TestField(5);

            batch.put::<TestSchema1>(&field_2, &field_1).unwrap();
            batch.put::<TestSchema1>(&field_1, &field_3).unwrap();
            batch.delete::<TestSchema1>(&field_4).unwrap();
            batch.put::<TestSchema1>(&field_5, &field_2).unwrap();
            batch.put::<TestSchema1>(&field_3, &field_4).unwrap();

            // 2..4
            let iter_range =
                batch.iter_range::<TestSchema1>(encode_key(&field_2)..encode_key(&field_4));
            let actual_values: Vec<(u32, Option<u32>)> = collect_actual_values(iter_range);
            let mut expected_values = vec![(2, Some(1)), (3, Some(4))];
            assert_eq!(expected_values, actual_values, "2..4");
            let rev_iter_range = batch
                .iter_range::<TestSchema1>(encode_key(&field_2)..encode_key(&field_4))
                .rev();
            let actual_values: Vec<(u32, Option<u32>)> = collect_actual_values(rev_iter_range);
            expected_values.reverse();
            assert_eq!(expected_values, actual_values, "rev:2..4");

            // 2..
            let iter_range = batch.iter_range::<TestSchema1>(encode_key(&field_2)..);
            let actual_values: Vec<(u32, Option<u32>)> = collect_actual_values(iter_range);
            let mut expected_values = vec![(2, Some(1)), (3, Some(4)), (4, None), (5, Some(2))];
            assert_eq!(expected_values, actual_values, "2..");
            let rev_iter_range = batch
                .iter_range::<TestSchema1>(encode_key(&field_2)..)
                .rev();
            let actual_values: Vec<(u32, Option<u32>)> = collect_actual_values(rev_iter_range);
            expected_values.reverse();
            assert_eq!(expected_values, actual_values, "rev:2..");

            // ..4
            let iter_range = batch.iter_range::<TestSchema1>(..encode_key(&field_4));
            let actual_values: Vec<(u32, Option<u32>)> = collect_actual_values(iter_range);
            let mut expected_values = vec![(1, Some(3)), (2, Some(1)), (3, Some(4))];
            assert_eq!(expected_values, actual_values, "..4");
            let rev_iter_range = batch
                .iter_range::<TestSchema1>(..encode_key(&field_4))
                .rev();
            let actual_values: Vec<(u32, Option<u32>)> = collect_actual_values(rev_iter_range);
            expected_values.reverse();
            assert_eq!(expected_values, actual_values, "rev:..4");
            // ..
            let iter_range = batch.iter_range::<TestSchema1>(..);
            let actual_values: Vec<(u32, Option<u32>)> = collect_actual_values(iter_range);
            let mut expected_values = vec![
                (1, Some(3)),
                (2, Some(1)),
                (3, Some(4)),
                (4, None),
                (5, Some(2)),
            ];
            assert_eq!(expected_values, actual_values, "..");
            let rev_iter_range = batch.iter_range::<TestSchema1>(..).rev();
            let actual_values: Vec<(u32, Option<u32>)> = collect_actual_values(rev_iter_range);
            expected_values.reverse();
            assert_eq!(expected_values, actual_values, "rev:..");
        }

        #[test]
        #[should_panic(expected = "range start is greater than range end in BTreeMap")]
        fn inverse_range_iterator() {
            let mut batch = SchemaBatch::new();

            let field_1 = TestField(1);
            let field_2 = TestField(2);
            let field_3 = TestField(3);
            let field_4 = TestField(4);

            batch.put::<TestSchema1>(&field_2, &field_1).unwrap();
            batch.put::<TestSchema1>(&field_1, &field_3).unwrap();
            batch.delete::<TestSchema1>(&field_4).unwrap();
            batch
                .iter_range::<TestSchema1>(encode_key(&field_4)..encode_key(&field_2))
                .for_each(drop);
        }

        #[test]
        #[should_panic(expected = "range start is greater than range end in BTreeMap")]
        fn inverse_range_rev_iterator() {
            let mut batch = SchemaBatch::new();

            let field_1 = TestField(1);
            let field_2 = TestField(2);
            let field_3 = TestField(3);
            let field_4 = TestField(4);

            batch.put::<TestSchema1>(&field_2, &field_1).unwrap();
            batch.put::<TestSchema1>(&field_1, &field_3).unwrap();
            batch.delete::<TestSchema1>(&field_4).unwrap();
            batch
                .iter_range::<TestSchema1>(encode_key(&field_4)..encode_key(&field_2))
                .rev()
                .for_each(drop);
        }
    }

    mod merge {
        use super::*;

        define_schema!(TestSchema2, TestField, TestField, "TestCF2");

        fn encode_key(field: &TestField) -> SchemaKey {
            <TestField as KeyEncoder<TestSchema1>>::encode_key(field).unwrap()
        }

        #[test]
        fn test_simple_merge() {
            let field_1 = TestField(1);
            let field_2 = TestField(2);
            let field_3 = TestField(3);
            let field_4 = TestField(4);
            let field_5 = TestField(5);

            let mut batch1 = SchemaBatch::new();
            batch1.put::<TestSchema1>(&field_1, &field_2).unwrap();
            batch1.put::<TestSchema1>(&field_3, &field_1).unwrap();
            batch1.put::<TestSchema1>(&field_5, &field_4).unwrap();

            let mut batch2 = SchemaBatch::new();
            batch2.put::<TestSchema1>(&field_1, &field_3).unwrap();
            batch2.put::<TestSchema1>(&field_4, &field_2).unwrap();
            batch2.delete::<TestSchema1>(&field_5).unwrap();

            batch1.merge(batch2);

            let get_value = |field: &TestField| -> Option<TestField> {
                batch1
                    .get_operation::<TestSchema1>(field)
                    .unwrap()
                    .unwrap()
                    .decode_value::<TestSchema1>()
                    .unwrap()
            };

            assert_eq!(Some(field_3), get_value(&field_1), "key (1) wasn't updated");
            assert_eq!(
                Some(field_1),
                get_value(&field_3),
                "key (3) has been be changed, when it shouldn't"
            );
            assert_eq!(Some(field_2), get_value(&field_4), "key (4) wasn't added");
            assert_eq!(None, get_value(&field_5), "key (5) wasn't deleted");
        }

        #[test]
        fn merge_preserves_range_ops() {
            let (f1, f2, f3) = (TestField(1), TestField(2), TestField(3));

            // batch1: a range delete on CF1 (e.g. the user namespace's pruning CF).
            let mut batch1 = SchemaBatch::new();
            batch1.delete_range::<TestSchema1>(&f1, &f2).unwrap();

            // batch2: a range delete on CF1, one on CF2 (a CF absent from batch1 — a second
            // namespace's pruning CF), plus a point write.
            let mut batch2 = SchemaBatch::new();
            batch2.delete_range::<TestSchema1>(&f2, &f3).unwrap();
            batch2.delete_range::<TestSchema2>(&f1, &f3).unwrap();
            batch2.put::<TestSchema2>(&f1, &f2).unwrap();

            batch1.merge(batch2);

            // Same-CF range ops are concatenated (batch1's + batch2's).
            assert_eq!(
                batch1
                    .range_ops
                    .get(TestSchema1::COLUMN_FAMILY_NAME)
                    .unwrap()
                    .len(),
                2
            );
            // The CF2 range op from `other` must NOT be dropped (the regression this guards).
            assert_eq!(
                batch1
                    .range_ops
                    .get(TestSchema2::COLUMN_FAMILY_NAME)
                    .unwrap()
                    .len(),
                1
            );
            // Point ops still merge as before.
            assert_eq!(
                Some(f2),
                batch1
                    .get_operation::<TestSchema2>(&f1)
                    .unwrap()
                    .unwrap()
                    .decode_value::<TestSchema2>()
                    .unwrap()
            );
        }

        #[test]
        fn merge_splits_earlier_range_ops_around_later_puts() {
            let (f1, f3, f5, f30) = (TestField(1), TestField(3), TestField(5), TestField(30));

            let mut batch1 = SchemaBatch::new();
            batch1.delete_range::<TestSchema1>(&f1, &f5).unwrap();

            let mut batch2 = SchemaBatch::new();
            batch2.put::<TestSchema1>(&f3, &f30).unwrap();

            batch1.merge(batch2);

            let f1_key = encode_key(&f1);
            let f3_key = encode_key(&f3);
            let f5_key = encode_key(&f5);
            let expected = vec![
                Operation::DeleteRange {
                    from: f1_key,
                    to: f3_key.clone(),
                },
                Operation::DeleteRange {
                    from: range_delete_key_after(&f3_key),
                    to: f5_key,
                },
            ];
            assert_eq!(
                Some(&expected),
                batch1.range_ops.get(TestSchema1::COLUMN_FAMILY_NAME)
            );
            assert_eq!(
                Some(f30),
                batch1
                    .get_operation::<TestSchema1>(&f3)
                    .unwrap()
                    .unwrap()
                    .decode_value::<TestSchema1>()
                    .unwrap()
            );
        }
    }
}
