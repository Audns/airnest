//! Backend abstraction layer — trait + shared types.

use std::collections::HashMap;

use crate::{Codec, Persistent, StoreError};

#[cfg(feature = "redb")]
pub mod redb;
pub mod sqlite;

/// Filter condition for backend-agnostic queries.
#[derive(Debug, Clone)]
pub enum Filter {
    Eq(String, String),
    In(String, Vec<String>),
}

/// Sort direction.
#[derive(Debug, Clone, Copy)]
pub enum Order {
    Asc,
    Desc,
}

/// A structured query request dispatched to a [`Backend`].
#[derive(Debug, Clone)]
pub struct QueryRequest {
    pub table: &'static str,
    pub filters: Vec<Filter>,
    pub order_by: Vec<(String, Order)>,
    pub limit: Option<usize>,
}

/// Single entry in a backend-agnostic batch.
#[derive(Debug, Clone)]
pub struct BatchEntry {
    pub table: &'static str,
    pub id_bytes: Vec<u8>,
    pub value_bytes: Vec<u8>,
    pub index_columns: &'static [&'static str],
    pub index_values: Vec<String>,
}

/// Backend-agnostic batch container.
#[derive(Debug, Default, Clone)]
pub struct BackendBatch {
    pub entries: Vec<BatchEntry>,
}

/// Storage-engine abstraction.
///
/// This trait uses native `async fn` and is **not object-safe**.
/// `Store` uses enum dispatch (`BackendImpl`) to avoid dynamic allocation.
#[allow(async_fn_in_trait)]
pub trait Backend: Send + Sync + 'static {
    async fn ensure_table<T: Persistent>(&self) -> Result<(), StoreError>;

    async fn save<T: Persistent>(&self, value: &T, codec: Codec) -> Result<(), StoreError>;

    async fn load<T: Persistent>(
        &self,
        id_bytes: &[u8],
        codec: Codec,
    ) -> Result<Option<T>, StoreError>;

    async fn load_many<T: Persistent>(
        &self,
        ids: &[Vec<u8>],
        codec: Codec,
    ) -> Result<Vec<T>, StoreError>;

    async fn exists<T: Persistent>(&self, id_bytes: &[u8]) -> Result<bool, StoreError>;

    async fn delete<T: Persistent>(&self, id_bytes: &[u8]) -> Result<(), StoreError>;

    async fn delete_all<T: Persistent>(&self) -> Result<u64, StoreError>;

    async fn scan<T: Persistent>(&self, codec: Codec) -> Result<Vec<T>, StoreError>;

    async fn count<T: Persistent>(&self) -> Result<i64, StoreError>;

    async fn query<T: Persistent>(
        &self,
        request: QueryRequest,
        codec: Codec,
    ) -> Result<Vec<T>, StoreError>;

    async fn query_count(&self, request: QueryRequest) -> Result<i64, StoreError>;

    async fn count_grouped_by<T: Persistent>(
        &self,
        column: &str,
    ) -> Result<HashMap<String, i64>, StoreError>;

    async fn replace_where<T: Persistent>(
        &self,
        filters: &[(String, String)],
        items: &[(Vec<u8>, Vec<u8>, Vec<String>)],
        codec: Codec,
    ) -> Result<(), StoreError>;

    async fn save_batch(&self, batch: &BackendBatch, codec: Codec) -> Result<(), StoreError>;

    fn as_sqlite_pool(&self) -> Option<&sqlx::SqlitePool>;

    async fn query_raw<T: Persistent>(&self, sql: &str, codec: Codec)
    -> Result<Vec<T>, StoreError>;
}
