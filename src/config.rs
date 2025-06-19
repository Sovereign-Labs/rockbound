/// Port selected RocksDB options for tuning underlying rocksdb instance of our state db.
/// The current default values are taken from Aptos. TODO: tune rocksdb for our workload.
/// see <https://github.com/facebook/rocksdb/blob/master/include/rocksdb/options.h>
/// for detailed explanations.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct RocksdbConfig {
    /// The maximum number of files that can be open concurrently. Defaults to 5000
    pub max_open_files: i32,
    /// Once write-ahead logs exceed this size, RocksDB will start forcing the flush of column
    /// families whose memtables are backed by the oldest live WAL file. Defaults to 1GB
    pub max_total_wal_size: u64,
    /// The maximum number of background threads, including threads for flushing and compaction. Defaults to 16.
    pub max_background_jobs: i32,
}

impl Default for RocksdbConfig {
    fn default() -> Self {
        Self {
            // Allow db to close old sst files, saving memory.
            max_open_files: 5000,
            // For now we set the max total WAL size to be 1G. This config can be useful when column
            // families are updated at non-uniform frequencies.
            max_total_wal_size: 1u64 << 30,
            // This includes threads for flushing and compaction. Rocksdb will decide the # of
            // threads to use internally.
            max_background_jobs: 16,
        }
    }
}

/// Generate [`rocksdb::Options`] corresponding to the given [`RocksdbConfig`].
pub fn gen_rocksdb_options(config: &RocksdbConfig, readonly: bool) -> rocksdb::Options {
    let mut db_opts = rocksdb::Options::default();
    db_opts.set_max_open_files(config.max_open_files);
    db_opts.set_max_total_wal_size(config.max_total_wal_size);
    db_opts.set_max_background_jobs(config.max_background_jobs);
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
