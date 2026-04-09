/// Alias for the full RocksDB options surface exposed by the upstream crate.
///
/// `rocksdb::Options` contains both DB-wide and column-family-specific settings, so callers can
/// use any upstream setter before passing the config to [`gen_rocksdb_options`].
pub type RocksdbConfig = rocksdb::Options;

/// Generate [`rocksdb::Options`] corresponding to the given [`RocksdbConfig`].
pub fn gen_rocksdb_options(config: &RocksdbConfig, readonly: bool) -> rocksdb::Options {
    let mut db_opts = config.clone();
    if !readonly {
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        // Do not enable db_opts.set_atomic_flush(true)! We use the WAL, so it provides no benefit and can prevent
        // tables from ever flushing. According to o3 (weakly supported by this source: https://github.com/facebook/rocksdb/issues/13487#issuecomment-2757182047),
        // the mechanism for the issue is that RocksDB will never *automatically* flush a MemTable until it is full. Since some of our DB columns
        // are written very rarely and this option requires that we flush all tables at once, these slow tables prevent *any* columns from being flushed automatically.
        // This causes the memtables of the busy columns to grow until they consume all available memory, leading to a crash.
        //
        // "Note that this is only useful when the WAL is disabled. When using the WAL, writes are always consistent across column families.""
        // <https://docs.rs/rocksdb/latest/rocksdb/struct.Options.html#method.set_atomic_flush>
        //
        // See [`crate::default_write_options()`]. We do not explicitly set `disable_wal` and it defaults to false:
        // Quoting from <https://docs.rs/rocksdb/latest/rocksdb/struct.WriteOptions.html#method.disable_wal>
        // Sets whether WAL should be active or not. If true, writes will not first go to the write ahead log, and the write may got lost after a crash.
        // Default: false
    }

    db_opts
}

#[cfg(test)]
mod tests {
    use crate::{DB, DEFAULT_COLUMN_FAMILY_NAME};

    use super::*;

    #[test]
    fn gen_rocksdb_options_preserves_full_upstream_options_surface() {
        let mut config = RocksdbConfig::default();
        config.set_max_open_files(42);

        let tmpdir = tempfile::tempdir().unwrap();
        let missing_db_path = tmpdir.path().join("missing-db");
        let writable_db_path = tmpdir.path().join("writable-db");
        let column_families = vec![DEFAULT_COLUMN_FAMILY_NAME];

        assert!(DB::open(
            &missing_db_path,
            "missing-db",
            column_families.clone(),
            &config,
        )
        .is_err());

        let writable_config = gen_rocksdb_options(&config, false);
        DB::open(
            &writable_db_path,
            "writable-db",
            column_families,
            &writable_config,
        )
        .unwrap();
    }
}
