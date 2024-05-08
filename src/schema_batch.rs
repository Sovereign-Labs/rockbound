use std::collections::{btree_map, BTreeMap, HashMap};

use crate::metrics::SCHEMADB_BATCH_PUT_LATENCY_SECONDS;
use crate::schema::{ColumnFamilyName, KeyCodec, ValueCodec};
use crate::{Operation, Schema, SchemaKey};

// [`SchemaBatch`] holds a collection of updates that can be applied to a DB
/// ([`Schema`]) atomically. The updates will be applied in the order in which
/// they are added to the [`SchemaBatch`].
#[derive(Debug, Default, Clone)]
pub struct SchemaBatch {
    pub(crate) last_writes: HashMap<ColumnFamilyName, BTreeMap<SchemaKey, Operation>>,
}

impl SchemaBatch {
    /// Creates an empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an insert/update operation to the batch.
    // TODO: made this pub(crate) ???
    pub fn put<S: Schema>(
        &mut self,
        key: &impl KeyCodec<S>,
        value: &impl ValueCodec<S>,
    ) -> anyhow::Result<()> {
        let _timer = SCHEMADB_BATCH_PUT_LATENCY_SECONDS
            .with_label_values(&["unknown"])
            .start_timer();

        let key = key.encode_key()?;
        let put_operation = Operation::Put {
            value: value.encode_value()?,
        };
        self.insert_operation::<S>(key, put_operation);

        Ok(())
    }

    /// Adds a delete operation to the batch.
    // TODO: Make this pub(crate)
    pub fn delete<S: Schema>(&mut self, key: &impl KeyCodec<S>) -> anyhow::Result<()> {
        let key = key.encode_key()?;
        self.insert_operation::<S>(key, Operation::Delete);

        Ok(())
    }

    fn insert_operation<S: Schema>(&mut self, key: SchemaKey, operation: Operation) {
        let column_writes = self.last_writes.entry(S::COLUMN_FAMILY_NAME).or_default();
        column_writes.insert(key, operation);
    }

    /// Getting the operation from current schema batch if present
    pub(crate) fn get_operation<S: Schema>(
        &self,
        key: &impl KeyCodec<S>,
    ) -> anyhow::Result<Option<&Operation>> {
        let key = key.encode_key()?;

        if let Some(column_writes) = self.last_writes.get(&S::COLUMN_FAMILY_NAME) {
            Ok(column_writes.get(&key))
        } else {
            Ok(None)
        }
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

    /// Iterator over all values in lexicographic order.
    pub fn iter<S: Schema>(&self) -> btree_map::Iter<SchemaKey, Operation> {
        self.last_writes
            .get(&S::COLUMN_FAMILY_NAME)
            .map(BTreeMap::iter)
            .unwrap_or_default()
    }

    /// Iterator in given range in lexicographic order.
    pub fn iter_range<S: Schema>(
        &self,
        range: impl std::ops::RangeBounds<SchemaKey>,
    ) -> btree_map::Range<SchemaKey, Operation> {
        self.last_writes
            .get(&S::COLUMN_FAMILY_NAME)
            .map(|column_writes| column_writes.range(range))
            .unwrap_or_default()
    }

    /// Merge other [`SchemaBatch`] on top of this one.
    /// Keys from other will overwrite keys in self.
    pub fn merge(&mut self, other: SchemaBatch) {
        for (cf_name, other_cf_map) in other.last_writes {
            let cf_map = self.last_writes.entry(cf_name).or_default();
            cf_map.extend(other_cf_map);
        }
    }
}

#[cfg(feature = "arbitrary")]
impl proptest::arbitrary::Arbitrary for SchemaBatch {
    type Parameters = &'static [ColumnFamilyName];
    fn arbitrary_with(columns: Self::Parameters) -> Self::Strategy {
        use proptest::prelude::any;
        use proptest::strategy::Strategy;

        proptest::collection::vec(any::<BTreeMap<SchemaKey, Operation>>(), columns.len())
            .prop_map::<SchemaBatch, _>(|vec_vec_write_ops| {
                let mut rows = HashMap::new();
                for (col, write_op) in columns.iter().zip(vec_vec_write_ops.into_iter()) {
                    rows.insert(*col, write_op);
                }
                SchemaBatch { last_writes: rows }
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

    mod iter {
        use super::*;
        use crate::schema::KeyDecoder;

        #[test]
        fn empty_schema_batch_iterator() {
            let batch = SchemaBatch::new();
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
                    Operation::Delete => None,
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
    }
}
