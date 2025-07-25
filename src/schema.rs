// Adapted from Aptos::storage::schemadb;
// While most of the Sovereign SDK will be available under both
// MIT and APACHE 2.0 licenses, this file is
// licensed under APACHE 2.0 only.

//! A type-safe interface over [`DB`](crate::DB) column families.

use std::{fmt::Debug, sync::Arc};

use crate::CodecError;

/// Crate users are expected to know [column
/// family](https://github.com/EighteenZi/rocksdb_wiki/blob/master/Column-Families.md)
/// names beforehand, so they can have `static` lifetimes.
pub type ColumnFamilyName = &'static str;

/// A [`Schema`] is a type-safe interface over a specific column family in a
/// [`DB`](crate::DB). It is always a key type ([`KeyCodec`]) and a value type ([`ValueCodec`]).
pub trait Schema: Debug + Send + Sync + 'static + Sized + Default {
    /// The column family name associated with this struct.
    /// Note: all schemas within the same SchemaDB must have distinct column family names.
    const COLUMN_FAMILY_NAME: ColumnFamilyName;
    /// Whether this table should be cached at the application layer (i.e. above RocksDB).
    const SHOULD_CACHE: bool;

    /// Type of the key.
    type Key: KeyCodec<Self>;

    /// Type of the value.
    type Value: ValueCodec<Self>;
}

/// A [`core::result::Result`] alias with [`CodecError`] as the error type.
pub type Result<T, E = CodecError> = core::result::Result<T, E>;

/// This trait defines a type that can serve as a [`Schema::Key`].
///
/// [`KeyCodec`] is a marker trait with a blanket implementation for all types
/// that are both [`KeyEncoder`] and [`KeyDecoder`]. Having [`KeyEncoder`] and
/// [`KeyDecoder`] as two standalone traits on top of [`KeyCodec`] may seem
/// superfluous, but it allows for zero-copy key encoding under specific
/// circumstances. E.g.:
///
/// ```rust
/// use anyhow::Context;
///
/// use rockbound::define_schema;
/// use rockbound::schema::{
///     Schema, KeyEncoder, KeyDecoder, ValueCodec, Result,
/// };
///
/// define_schema!(PersonAgeByName, String, u32, "person_age_by_name");
///
/// impl KeyEncoder<PersonAgeByName> for String {
///     fn encode_key(&self) -> Result<Vec<u8>> {
///         Ok(self.as_bytes().to_vec())
///     }
/// }
///
/// /// What about encoding a `&str`, though? We'd have to copy it into a
/// /// `String` first, which is not ideal. But we can do better:
/// impl<'a> KeyEncoder<PersonAgeByName> for &'a str {
///     fn encode_key(&self) -> Result<Vec<u8>> {
///         Ok(self.as_bytes().to_vec())
///     }
/// }
///
/// impl KeyDecoder<PersonAgeByName> for String {
///     fn decode_key(data: &[u8]) -> Result<Self> {
///         Ok(String::from_utf8(data.to_vec()).context("Can't read key")?)
///     }
/// }
///
/// impl ValueCodec<PersonAgeByName> for u32 {
///     fn encode_value(&self) -> Result<Vec<u8>> {
///         Ok(self.to_le_bytes().to_vec())
///     }
///
///     fn decode_value(data: &[u8]) -> Result<Self> {
///         let mut buf = [0u8; 4];
///         buf.copy_from_slice(data);
///         Ok(u32::from_le_bytes(buf))
///     }
/// }
/// ```
pub trait KeyCodec<S: Schema + ?Sized>: KeyEncoder<S> + KeyDecoder<S> {}

impl<T, S: Schema + ?Sized> KeyCodec<S> for T where T: KeyEncoder<S> + KeyDecoder<S> {}

/// Implementors of this trait can be used to encode keys in the given [`Schema`].
pub trait KeyEncoder<S: Schema + ?Sized>: Sized + Debug {
    /// Converts `self` to bytes to be stored in RocksDB.
    fn encode_key(&self) -> Result<Vec<u8>>;
}

impl<S: Schema, T: KeyEncoder<S>> KeyEncoder<S> for &T {
    fn encode_key(&self) -> Result<Vec<u8>> {
        (*self).encode_key()
    }
}

impl<S: Schema, T: KeyEncoder<S>> KeyEncoder<S> for Arc<T> {
    fn encode_key(&self) -> Result<Vec<u8>> {
        self.as_ref().encode_key()
    }
}

/// Implementors of this trait can be used to decode keys in the given [`Schema`].
pub trait KeyDecoder<S: Schema + ?Sized>: Sized + Debug {
    /// Converts bytes fetched from RocksDB to `Self`.
    fn decode_key(data: &[u8]) -> Result<Self>;
}

/// This trait defines a type that can serve as a [`Schema::Value`].
pub trait ValueCodec<S: Schema + ?Sized>: Sized + Debug {
    /// Converts `self` to bytes to be stored in DB.
    fn encode_value(&self) -> Result<Vec<u8>>;
    /// Converts bytes fetched from DB to `Self`.
    fn decode_value(data: &[u8]) -> Result<Self>;
}

/// A utility macro to define [`Schema`] implementors. You must specify the
/// [`Schema`] implementor's name, the key type, the value type, and the column
/// family name.
///
/// # Example
///
/// ```rust
/// use anyhow::Context;
///
/// use rockbound::define_schema;
/// use rockbound::schema::{
///     Schema, KeyEncoder, KeyDecoder, ValueCodec, Result,
/// };
///
/// define_schema!(PersonAgeByName, String, u32, "person_age_by_name");
///
/// impl KeyEncoder<PersonAgeByName> for String {
///     fn encode_key(&self) -> Result<Vec<u8>> {
///         Ok(self.as_bytes().to_vec())
///     }
/// }
///
/// impl KeyDecoder<PersonAgeByName> for String {
///     fn decode_key(data: &[u8]) -> Result<Self> {
///         Ok(String::from_utf8(data.to_vec()).context("Can't read key")?)
///     }
/// }
///
/// impl ValueCodec<PersonAgeByName> for u32 {
///     fn encode_value(&self) -> Result<Vec<u8>> {
///         Ok(self.to_le_bytes().to_vec())
///     }
///
///     fn decode_value(data: &[u8]) -> Result<Self> {
///         let mut buf = [0u8; 4];
///         buf.copy_from_slice(data);
///         Ok(u32::from_le_bytes(buf))
///     }
/// }
/// ```
#[macro_export]
macro_rules! define_schema {
    ($schema_type:ident, $key_type:ty, $value_type:ty, $cf_name:expr) => {
        #[derive(Debug, Default)]
        pub(crate) struct $schema_type;

        impl $crate::schema::Schema for $schema_type {
            type Key = $key_type;
            type Value = $value_type;

            const COLUMN_FAMILY_NAME: $crate::schema::ColumnFamilyName = $cf_name;
            const SHOULD_CACHE: bool = false;
        }
    };
}
