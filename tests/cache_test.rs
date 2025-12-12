// // Copyright (c) Aptos
// // SPDX-License-Identifier: Apache-2.0

// //! Tests for cache consistency with the backing RocksDB

// use std::sync::{Arc, Barrier};
// use std::thread;
// use std::time::Duration;

// use rockbound::default_cf_descriptor;
// use rockbound::schema::{ColumnFamilyName, Schema};
// use rockbound::test::TestField;
// use rockbound::{SchemaBatch, DB};
// use rocksdb::DEFAULT_COLUMN_FAMILY_NAME;
// use tempfile::TempDir;

// // // Create cached and non-cached schemas for testing
// // #[derive(Debug, Default)]
// // pub struct TestKey;

// // impl Schema for CachedTestSchema {
// //     type Key = TestField;
// //     type Value = TestField;
// //     const COLUMN_FAMILY_NAME: ColumnFamilyName = "CachedTestCF";
// //     const SHOULD_CACHE: bool = true;
// // }

// // #[derive(Debug, Default)]
// // pub struct NonCachedTestSchema;

// // impl Schema for NonCachedTestSchema {
// //     type Key = TestField;
// //     type Value = TestField;
// //     const COLUMN_FAMILY_NAME: ColumnFamilyName = "NonCachedTestCF";
// //     const SHOULD_CACHE: bool = false;
// // }

// // fn get_column_families() -> Vec<ColumnFamilyName> {
// //     vec![
// //         DEFAULT_COLUMN_FAMILY_NAME,
// //         CachedTestSchema::COLUMN_FAMILY_NAME,
// //         NonCachedTestSchema::COLUMN_FAMILY_NAME,
// //     ]
// // }

// // fn open_db(dir: impl AsRef<std::path::Path>) -> DB {
// //     let mut db_opts = rocksdb::Options::default();
// //     db_opts.create_if_missing(true);
// //     db_opts.create_missing_column_families(true);
// //     DB::open_with_cfds(
// //         &db_opts,
// //         dir,
// //         "cache_test",
// //         get_column_families().into_iter().map(default_cf_descriptor),
// //     )
// //     .expect("Failed to open DB.") // Use 1 MB cache for testing
// // }

// struct TestDB {
//     _tmpdir: TempDir,
//     db: DB,
// }

// impl TestDB {
//     fn new() -> Self {
//         let tmpdir = tempfile::tempdir().unwrap();
//         let db = open_db(&tmpdir);

//         TestDB {
//             _tmpdir: tmpdir,
//             db,
//         }
//     }
// }

// impl std::ops::Deref for TestDB {
//     type Target = DB;

//     fn deref(&self) -> &Self::Target {
//         &self.db
//     }
// }
