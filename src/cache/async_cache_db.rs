use std::sync::Arc;

use super::cache_db::{CacheDb, PaginatedResponse};
use crate::{KeyCodec, Schema, SeekKeyEncoder};

/// Async version of [`CacheDb`].
#[derive(Debug, Clone)]
pub struct AsyncCacheDb {
    db: Arc<CacheDb>,
}

impl AsyncCacheDb {
    /// Create new [`AsyncCacheDb`] pointing to given [`CacheDb`].
    pub fn new(db: Arc<CacheDb>) -> Self {
        Self { db }
    }

    /// See [`CacheDb::put`].
    pub async fn read<S: Schema>(
        &self,
        key: impl KeyCodec<S> + Send + 'static,
    ) -> anyhow::Result<Option<S::Value>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.read(&key)).await?
    }

    /// See [`CacheDb::get_largest`].
    pub async fn get_largest<S: Schema>(&self) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.get_largest::<S>()).await?
    }

    /// See [`CacheDb::get_prev`].
    pub async fn get_prev<S: Schema>(
        &self,
        seek_key: impl SeekKeyEncoder<S> + Send + 'static,
    ) -> anyhow::Result<Option<(S::Key, S::Value)>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.get_prev(&seek_key)).await?
    }

    /// See [`CacheDb::get_n_from_first_match`].
    pub async fn get_n_from_first_match<S: Schema>(
        &self,
        seek_key: impl SeekKeyEncoder<S> + Send + 'static,
        n: usize,
    ) -> anyhow::Result<PaginatedResponse<S>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.get_n_from_first_match(&seek_key, n)).await?
    }

    /// See [`CacheDb::collect_in_range`].
    pub async fn collect_in_range<S: Schema, Sk: SeekKeyEncoder<S> + Send + 'static>(
        &self,
        range: std::ops::Range<Sk>,
    ) -> anyhow::Result<Vec<(S::Key, S::Value)>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.collect_in_range(range)).await?
    }
}
