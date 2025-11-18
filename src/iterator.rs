use std::iter::FusedIterator;
use std::marker::PhantomData;

use anyhow::Result;
use rocksdb::{ColumnFamily, ReadOptions};

use crate::metrics::{SCHEMADB_ITER_BYTES, SCHEMADB_ITER_LATENCY_SECONDS};
use crate::schema::{KeyDecoder, Schema, ValueCodec};
use crate::{SchemaKey, SchemaValue};

/// This defines a type that can be used to seek a [`SchemaIterator`], via
/// interfaces like [`SchemaIterator::seek`]. Mind you, not all
/// [`KeyEncoder`](crate::schema::KeyEncoder)s shall be [`SeekKeyEncoder`]s, and
/// vice versa. E.g.:
///
/// - Some key types don't use an encoding that results in sensible
///   seeking behavior under lexicographic ordering (what RocksDB uses by
///   default), which means you shouldn't implement [`SeekKeyEncoder`] at all.
/// - Other key types might maintain full lexicographic order, which means the
///   original key type can also be [`SeekKeyEncoder`].
/// - Other key types may be composite, and the first field alone may be
///   a good candidate for [`SeekKeyEncoder`].
pub trait SeekKeyEncoder<S: Schema>: Sized {
    /// Converts `self` to bytes which is used to seek the underlying raw
    /// iterator.
    ///
    /// If `self` is also a [`KeyEncoder`](crate::schema::KeyEncoder), then
    /// [`SeekKeyEncoder::encode_seek_key`] MUST return the same bytes as
    /// [`KeyEncoder::encode_key`](crate::schema::KeyEncoder::encode_key).
    fn encode_seek_key(&self) -> crate::schema::Result<Vec<u8>>;
}

/// Indicates in which direction iterator should be scanned.
#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) enum ScanDirection {
    Forward,
    Backward,
}

/// DB Iterator parameterized on [`Schema`] that seeks with [`Schema::Key`] and yields
/// [`Schema::Key`] and [`Schema::Value`] pairs.
pub struct SchemaIterator<'a, S> {
    db_iter: rocksdb::DBRawIterator<'a>,
    direction: ScanDirection,
    phantom: PhantomData<S>,
}

impl<'a, S> SchemaIterator<'a, S>
where
    S: Schema,
{
    pub(crate) fn new(db_iter: rocksdb::DBRawIterator<'a>, direction: ScanDirection) -> Self {
        let mut iter = SchemaIterator {
            db_iter,
            direction,
            phantom: PhantomData,
        };

        // We need an explicit `seek` call before we can start
        // iterating.
        match direction {
            ScanDirection::Forward => iter.seek_to_first(),
            ScanDirection::Backward => iter.seek_to_last(),
        };

        iter
    }

    /// Seeks to the first key.
    pub fn seek_to_first(&mut self) {
        self.db_iter.seek_to_first();
    }

    /// Seeks to the last key.
    pub fn seek_to_last(&mut self) {
        self.db_iter.seek_to_last();
    }

    /// Seeks to the first key whose binary representation is equal to or greater than that of the
    /// `seek_key`.
    pub fn seek(&mut self, seek_key: &impl SeekKeyEncoder<S>) -> Result<()> {
        let key = seek_key.encode_seek_key()?;
        self.db_iter.seek(&key);
        Ok(())
    }

    /// Seeks to the last key whose binary representation is less than or equal to that of the
    /// `seek_key`.
    ///
    /// See example in [`RocksDB doc`](https://github.com/facebook/rocksdb/wiki/SeekForPrev).
    pub fn seek_for_prev(&mut self, seek_key: &impl SeekKeyEncoder<S>) -> Result<()> {
        let key = seek_key.encode_seek_key()?;
        self.db_iter.seek_for_prev(&key);
        Ok(())
    }

    /// Reverses iterator direction.
    pub fn rev(self) -> Self {
        let new_direction = match self.direction {
            ScanDirection::Forward => ScanDirection::Backward,
            ScanDirection::Backward => ScanDirection::Forward,
        };
        Self::new(self.db_iter, new_direction)
    }

    fn next_impl(&mut self) -> Result<Option<IteratorOutput<S::Key, S::Value>>> {
        let _timer = SCHEMADB_ITER_LATENCY_SECONDS
            .with_label_values(&[S::COLUMN_FAMILY_NAME])
            .start_timer();

        // SAFETY: Calling `next` or `prev` requires to check `valid` first.
        // Not doing so may result in UB because of a nasty `rust-rocksdb` bug:
        // <https://github.com/rust-rocksdb/rust-rocksdb/issues/824>.
        if !self.db_iter.valid() {
            self.db_iter.status()?;
            return Ok(None);
        }

        let raw_key = self.db_iter.key().expect("db_iter.key() failed.");
        let raw_value = self.db_iter.value().expect("db_iter.value() failed.");
        let value_size_bytes = raw_value.len();
        SCHEMADB_ITER_BYTES
            .with_label_values(&[S::COLUMN_FAMILY_NAME])
            .observe((raw_key.len() + raw_value.len()) as f64);

        let key = <S::Key as KeyDecoder<S>>::decode_key(raw_key)?;
        let value = <S::Value as ValueCodec<S>>::decode_value(raw_value)?;

        match self.direction {
            ScanDirection::Forward => self.db_iter.next(),
            ScanDirection::Backward => self.db_iter.prev(),
        }

        Ok(Some(IteratorOutput {
            key,
            value,
            value_size_bytes,
        }))
    }
}

/// The output of [`SchemaIterator`]'s next_impl
pub struct IteratorOutput<K, V> {
    pub key: K,
    pub value: V,
    pub value_size_bytes: usize,
}

impl<K, V> IteratorOutput<K, V> {
    pub fn into_tuple(self) -> (K, V) {
        (self.key, self.value)
    }
}

impl<S> Iterator for SchemaIterator<'_, S>
where
    S: Schema,
{
    type Item = Result<IteratorOutput<S::Key, S::Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_impl().transpose()
    }
}

impl<S> FusedIterator for SchemaIterator<'_, S> where S: Schema {}

/// Iterates over given column in [`rocksdb::DB`] using raw types.
pub(crate) struct RawDbIter<'a> {
    db_iter: rocksdb::DBRawIterator<'a>,
    direction: ScanDirection,
    upper_bound: std::ops::Bound<SchemaKey>,
}

impl<'a> RawDbIter<'a> {
    pub(crate) fn new(
        inner: &'a rocksdb::DB,
        cf_handle: &ColumnFamily,
        range: impl std::ops::RangeBounds<SchemaKey>,
        direction: ScanDirection,
    ) -> Self {
        if let std::ops::Bound::Excluded(_) = range.start_bound() {
            panic!("Excluded start_bound is not supported");
        }

        // |                  | ScanDirection::Forward                        | ScanDirection::Backward                      |
        // |------------------|-----------------------------------------------|----------------------------------------------|
        // |                  | iterator init         | bounds check          | iterator init         | bounds check         |
        // |------------------|-----------------------|-----------------------|-----------------------|----------------------|
        // | start::Excluded  | panic                 |                       | panic                 |                      |
        // | start::Included  | db_iter.seek_to_first | opts.set_lower_bound  |                       | opts.set_lower_bound |
        // | start::Unbounded | db_iter.seek_to_first | N/A                   |                       |                      |
        // | end::Excluded    |                       | opts.set_upper_bound  | db_iter.seek_to_last  | opts.set_upper_bound |
        // | end::Included    |                       | check inside `next()` | db_iter.seek_for_prev | via db_iter.seek*    |
        // | end::Unbounded   |                       | N/A                   | db_iter.seek_to_last  | via db_iter.seek*    |

        // Configure options
        let mut opts: ReadOptions = Default::default();
        // It is safe to always start_bound, because `Bound::Excluded` was excluded before
        if let std::ops::Bound::Included(lower) = range.start_bound() {
            opts.set_iterate_lower_bound(lower.clone());
        }
        // end_bound explicitly set only if it's excluded, to match rocksdb API
        if let std::ops::Bound::Excluded(upper) = range.end_bound() {
            opts.set_iterate_upper_bound(upper.clone());
        }

        //
        // Configure iterator
        let mut db_iter = inner.raw_iterator_cf_opt(cf_handle, opts);

        // Now need to navigate
        match direction {
            ScanDirection::Forward => {
                // Always seek to first for the forward iterator.
                db_iter.seek_to_first();
            }
            ScanDirection::Backward => match range.end_bound() {
                // Seek to last only if it matches rocksdb API
                std::ops::Bound::Excluded(_) | std::ops::Bound::Unbounded => {
                    db_iter.seek_to_last();
                }
                // In case of Included upper bound, we have to seek to it manually, to move backwards from there
                std::ops::Bound::Included(upper) => {
                    db_iter.seek_for_prev(upper);
                }
            },
        };
        RawDbIter {
            db_iter,
            direction,
            upper_bound: range.end_bound().cloned(),
        }
    }
}

impl Iterator for RawDbIter<'_> {
    type Item = (SchemaKey, SchemaValue);

    fn next(&mut self) -> Option<Self::Item> {
        if !self.db_iter.valid() {
            self.db_iter.status().ok()?;
            return None;
        }

        let next_item = self.db_iter.item().expect("db_iter.key() failed.");
        // Have to allocate to fix lifetime issue
        let next_item = (next_item.0.to_vec(), next_item.1.to_vec());

        // If next item is larger than upper bound, we're done
        if let std::ops::Bound::Included(upper) = self.upper_bound.as_ref() {
            if &next_item.0 > upper {
                // That we're moving forward!!!
                assert_eq!(
                    &ScanDirection::Forward,
                    &self.direction,
                    "Upper bound exceeded, while moving backward: {next_item:?} {upper:?}",
                );
                return None;
            }
        }

        match self.direction {
            ScanDirection::Forward => self.db_iter.next(),
            ScanDirection::Backward => self.db_iter.prev(),
        };

        Some(next_item)
    }
}

#[cfg(test)]
mod tests {
    use rocksdb::DEFAULT_COLUMN_FAMILY_NAME;
    use std::path::Path;

    use super::*;
    use crate::schema::ColumnFamilyName;
    use crate::schema::KeyEncoder;
    use crate::test::TestField;
    use crate::{define_schema, DB};
    use proptest::prelude::*;

    define_schema!(TestSchema1, TestField, TestField, "TestCF1");

    type S = TestSchema1;

    fn get_column_families() -> Vec<ColumnFamilyName> {
        vec![DEFAULT_COLUMN_FAMILY_NAME, S::COLUMN_FAMILY_NAME]
    }

    fn open_db(dir: impl AsRef<Path>) -> DB {
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        DB::open(
            dir,
            "test-iterator",
            get_column_families(),
            &db_opts,
        )
        .expect("Failed to open DB.")
    }

    #[test]
    fn test_empty_raw_iterator() {
        let tmpdir = tempfile::tempdir().unwrap();
        let db = open_db(tmpdir.path());

        let iter_forward = db.raw_iter::<S>(ScanDirection::Forward).unwrap();
        let count_forward = iter_forward.count();
        assert_eq!(0, count_forward);

        let iter_backward = db.raw_iter::<S>(ScanDirection::Backward).unwrap();
        let count_backward = iter_backward.count();
        assert_eq!(0, count_backward);
    }

    fn collect_actual_values(iter: RawDbIter) -> Vec<(u32, u32)> {
        iter.map(|(key, value)| {
            let key = <<S as Schema>::Key as KeyDecoder<S>>::decode_key(&key).unwrap();
            let value = <<S as Schema>::Value as ValueCodec<S>>::decode_value(&value).unwrap();
            (key.0, value.0)
        })
        .collect::<Vec<_>>()
    }

    fn encode_key(field: &TestField) -> SchemaKey {
        <TestField as KeyEncoder<S>>::encode_key(field).unwrap()
    }

    fn test_iterator(
        db: &DB,
        prefix: &'static str,
        range: impl std::ops::RangeBounds<SchemaKey> + Clone,
        mut expected_values: Vec<(u32, u32)>,
    ) {
        let iter_range_forward = db
            .raw_iter_range::<S>(range.clone(), ScanDirection::Forward)
            .unwrap();
        let actual_values = collect_actual_values(iter_range_forward);

        assert_eq!(expected_values, actual_values, "{prefix} forward",);
        let iter_range_backward = db
            .raw_iter_range::<S>(range, ScanDirection::Backward)
            .unwrap();
        let actual_values = collect_actual_values(iter_range_backward);
        expected_values.reverse();
        assert_eq!(expected_values, actual_values, "{prefix} backward");
    }

    #[test]
    fn iterator_tests() {
        let tmpdir = tempfile::tempdir().unwrap();
        let db = open_db(tmpdir.path());

        let field_1 = TestField(10);
        let field_2 = TestField(20);
        let field_3 = TestField(30);
        let field_4 = TestField(40);
        let field_5 = TestField(50);
        let field_6 = TestField(60);

        db.put::<S>(&field_3, &field_2).unwrap();
        db.put::<S>(&field_2, &field_1).unwrap();
        db.put::<S>(&field_4, &field_5).unwrap();
        db.put::<S>(&field_1, &field_4).unwrap();
        db.put::<S>(&field_5, &field_6).unwrap();
        db.put::<S>(&field_6, &field_6).unwrap();

        // All values, forward
        let iter_forward = db.raw_iter::<S>(ScanDirection::Forward).unwrap();

        let actual_values = collect_actual_values(iter_forward);
        let expected_values = vec![(10, 40), (20, 10), (30, 20), (40, 50), (50, 60), (60, 60)];
        assert_eq!(expected_values, actual_values, "all values, forward");

        // All values, backward
        let iter_backward = db.raw_iter::<S>(ScanDirection::Backward).unwrap();
        let actual_values = collect_actual_values(iter_backward);
        let expected_values = vec![(60, 60), (50, 60), (40, 50), (30, 20), (20, 10), (10, 40)];
        assert_eq!(expected_values, actual_values, "all values, backward");

        // All values, initialize with forward and then .rev()
        let iter_rev = db.iter::<S>().unwrap().rev();
        let actual_values: Vec<(u32, u32)> = iter_rev
            .map(|res| {
                let item = res.unwrap();
                (item.key.0, item.value.0)
            })
            .collect();
        let expected_values = vec![(60, 60), (50, 60), (40, 50), (30, 20), (20, 10), (10, 40)];
        assert_eq!(expected_values, actual_values, "all values, .rev()");

        // Bounds: Ranges
        let lower_bound = encode_key(&field_2);
        let upper_bound = encode_key(&field_5);

        test_iterator(
            &db,
            "20..50",
            lower_bound.clone()..upper_bound.clone(),
            vec![(20, 10), (30, 20), (40, 50)],
        );
        test_iterator(
            &db,
            "20..=50",
            lower_bound.clone()..=upper_bound.clone(),
            vec![(20, 10), (30, 20), (40, 50), (50, 60)],
        );
        test_iterator(
            &db,
            "15..45",
            encode_key(&TestField(15))..encode_key(&TestField(45)),
            vec![(20, 10), (30, 20), (40, 50)],
        );
        test_iterator(
            &db,
            "15..=45",
            encode_key(&TestField(15))..=encode_key(&TestField(45)),
            vec![(20, 10), (30, 20), (40, 50)],
        );
        test_iterator(
            &db,
            "20..",
            lower_bound.clone()..,
            vec![(20, 10), (30, 20), (40, 50), (50, 60), (60, 60)],
        );
        test_iterator(
            &db,
            "..50",
            ..upper_bound.clone(),
            vec![(10, 40), (20, 10), (30, 20), (40, 50)],
        );
        test_iterator(
            &db,
            "..=50",
            ..=upper_bound.clone(),
            vec![(10, 40), (20, 10), (30, 20), (40, 50), (50, 60)],
        );
        test_iterator(
            &db,
            "..=59",
            ..encode_key(&TestField(59)),
            vec![(10, 40), (20, 10), (30, 20), (40, 50), (50, 60)],
        );
        test_iterator(
            &db,
            "..",
            ..,
            vec![(10, 40), (20, 10), (30, 20), (40, 50), (50, 60), (60, 60)],
        );
        test_iterator(
            &db,
            "50..=50",
            upper_bound.clone()..=upper_bound.clone(),
            vec![(50, 60)],
        );
        test_iterator(
            &db,
            "outside upper 0..100",
            encode_key(&TestField(100))..encode_key(&TestField(102)),
            vec![],
        );

        test_iterator(
            &db,
            "outside lower 0..10",
            encode_key(&TestField(0))..encode_key(&TestField(1)),
            vec![],
        );

        {
            for direction in [ScanDirection::Forward, ScanDirection::Backward] {
                // Inverse
                let err = db
                    .raw_iter_range::<S>(upper_bound.clone()..lower_bound.clone(), direction)
                    .err()
                    .unwrap();
                assert_eq!(
                    "[Rockbound]: error in raw_iter_range: lower_bound > upper_bound",
                    err.to_string()
                );

                // Empty
                let iter = db
                    .raw_iter_range::<S>(upper_bound.clone()..upper_bound.clone(), direction)
                    .unwrap();
                let actual_values = collect_actual_values(iter);
                assert_eq!(Vec::<(u32, u32)>::new(), actual_values);
            }
        }
    }

    fn check_iterator_proptest(numbers: Vec<u32>) {
        assert!(numbers.len() >= 10);
        let tmpdir = tempfile::tempdir().unwrap();
        let db = open_db(tmpdir.path());

        // Numbers
        let existing_lower = numbers[2];
        let existing_upper = numbers[8];

        let min_value = *numbers.iter().min().unwrap();
        let max_value = *numbers.iter().max().unwrap();
        let maybe_non_existing_lower_1 = existing_lower.saturating_sub(1);
        let maybe_non_existing_lower_2 = existing_lower.checked_add(1).unwrap_or(u32::MAX - 2);
        let maybe_non_existing_upper_1 = existing_upper.saturating_sub(1);
        let maybe_non_existing_upper_2 = existing_upper.checked_add(1).unwrap_or(u32::MAX - 2);

        // Ranges will be constructed from these pairs
        let range_pairs = vec![
            (min_value, max_value),
            (existing_lower, existing_upper),
            (min_value, existing_lower),
            (min_value, existing_upper),
            (existing_lower, max_value),
            (existing_upper, max_value),
            (existing_lower, maybe_non_existing_upper_1),
            (existing_lower, maybe_non_existing_upper_2),
            (maybe_non_existing_lower_1, existing_upper),
            (maybe_non_existing_lower_2, existing_upper),
            (maybe_non_existing_lower_1, maybe_non_existing_upper_1),
            (maybe_non_existing_lower_1, maybe_non_existing_upper_2),
            (maybe_non_existing_lower_2, maybe_non_existing_upper_1),
            (maybe_non_existing_lower_1, maybe_non_existing_upper_2),
            (maybe_non_existing_lower_1, max_value),
            (maybe_non_existing_lower_2, max_value),
            (min_value, maybe_non_existing_upper_1),
            (min_value, maybe_non_existing_upper_2),
        ];

        for number in numbers {
            db.put::<S>(&TestField(number), &TestField(number)).unwrap();
        }
        for (low, high) in range_pairs {
            if low > high {
                continue;
            }
            let low = TestField(low);
            let high = TestField(high);

            let range = encode_key(&low)..encode_key(&high);
            let range_inclusive = encode_key(&low)..=encode_key(&high);
            let range_to = ..encode_key(&high);
            let range_to_inclusive = ..=encode_key(&high);
            let range_from = encode_key(&low)..;
            let range_full = ..;

            check_range(&db, range);
            check_range(&db, range_inclusive);
            check_range(&db, range_to);
            check_range(&db, range_to_inclusive);
            check_range(&db, range_from);
            check_range(&db, range_full);
        }
    }

    fn check_range<R: std::ops::RangeBounds<SchemaKey> + Clone + std::fmt::Debug>(
        db: &DB,
        range: R,
    ) {
        for direction in [ScanDirection::Forward, ScanDirection::Backward] {
            let iter_range = db.raw_iter_range::<S>(range.clone(), direction).unwrap();
            validate_iterator(iter_range, range.clone(), direction);
        }
    }

    fn validate_iterator<I, R>(iterator: I, range: R, direction: ScanDirection)
    where
        I: Iterator<Item = (SchemaKey, SchemaValue)>,
        R: std::ops::RangeBounds<SchemaKey> + std::fmt::Debug,
    {
        let mut prev_key: Option<TestField> = None;
        for (key, _) in iterator {
            assert!(range.contains(&key));
            let key = <<S as Schema>::Key as KeyDecoder<S>>::decode_key(&key).unwrap();
            if let Some(prev_key) = prev_key {
                match direction {
                    ScanDirection::Forward => assert!(
                        key.0 >= prev_key.0,
                        "key={}, prev_key={} range={:?}",
                        key.0,
                        prev_key.0,
                        range
                    ),
                    ScanDirection::Backward => assert!(
                        key.0 <= prev_key.0,
                        "key={}, prev_key={} range={:?}",
                        key.0,
                        prev_key.0,
                        range
                    ),
                };
            }
            prev_key = Some(key);
        }
    }

    proptest! {
        #[test]
        fn raw_db_iterator_iterate_proptest_any_number(input in prop::collection::vec(any::<u32>(), 10..80)) {
            check_iterator_proptest(input);
        }

        #[test]
        fn raw_db_iterator_iterate_proptest_uniq_numbers(input in prop::collection::hash_set(any::<u32>(), 10..80)) {
            let input: Vec<u32> = input.into_iter().collect();
            check_iterator_proptest(input);
        }
    }
}
