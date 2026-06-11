//! `Store` — one handle, one file, all types.
//!
//! Backed by either SQLite or redb via the [`Backend`] trait.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::Serialize;

use crate::{
    backend::{Backend, BackendBatch, Filter, Order, QueryRequest},
    codec::Codec,
    error::StoreError,
    index::ToIndexValue,
    into_air_id::IntoAirId,
    persistent::Persistent,
};

#[cfg(feature = "redb")]
use crate::backend::redb::RedbBackend;
use crate::backend::sqlite::SqliteBackend;

// ── BackendKind ───────────────────────────────────────────────────────────────

/// Backend selector for [`StoreBuilder`].
#[derive(Clone, Copy, Debug)]
pub enum BackendKind {
    Sqlite,
    #[cfg(feature = "redb")]
    Redb,
}

// ── BackendImpl ───────────────────────────────────────────────────────────────

#[derive(Clone)]
enum BackendImpl {
    Sqlite(Arc<SqliteBackend>),
    #[cfg(feature = "redb")]
    Redb(Arc<RedbBackend>),
}

impl BackendImpl {
    async fn ensure_table<T: Persistent>(&self) -> Result<(), StoreError> {
        match self {
            Self::Sqlite(b) => b.ensure_table::<T>().await,
            #[cfg(feature = "redb")]
            Self::Redb(b) => b.ensure_table::<T>().await,
        }
    }

    async fn save<T: Persistent>(&self, value: &T, codec: Codec) -> Result<(), StoreError> {
        match self {
            Self::Sqlite(b) => b.save(value, codec).await,
            #[cfg(feature = "redb")]
            Self::Redb(b) => b.save(value, codec).await,
        }
    }

    async fn load<T: Persistent>(
        &self,
        id_bytes: &[u8],
        codec: Codec,
    ) -> Result<Option<T>, StoreError> {
        match self {
            Self::Sqlite(b) => b.load::<T>(id_bytes, codec).await,
            #[cfg(feature = "redb")]
            Self::Redb(b) => b.load::<T>(id_bytes, codec).await,
        }
    }

    async fn load_many<T: Persistent>(
        &self,
        ids: &[Vec<u8>],
        codec: Codec,
    ) -> Result<Vec<T>, StoreError> {
        match self {
            Self::Sqlite(b) => b.load_many::<T>(ids, codec).await,
            #[cfg(feature = "redb")]
            Self::Redb(b) => b.load_many::<T>(ids, codec).await,
        }
    }

    async fn exists<T: Persistent>(&self, id_bytes: &[u8]) -> Result<bool, StoreError> {
        match self {
            Self::Sqlite(b) => b.exists::<T>(id_bytes).await,
            #[cfg(feature = "redb")]
            Self::Redb(b) => b.exists::<T>(id_bytes).await,
        }
    }

    async fn delete<T: Persistent>(&self, id_bytes: &[u8]) -> Result<(), StoreError> {
        match self {
            Self::Sqlite(b) => b.delete::<T>(id_bytes).await,
            #[cfg(feature = "redb")]
            Self::Redb(b) => b.delete::<T>(id_bytes).await,
        }
    }

    async fn delete_all<T: Persistent>(&self) -> Result<u64, StoreError> {
        match self {
            Self::Sqlite(b) => b.delete_all::<T>().await,
            #[cfg(feature = "redb")]
            Self::Redb(b) => b.delete_all::<T>().await,
        }
    }

    async fn scan<T: Persistent>(&self, codec: Codec) -> Result<Vec<T>, StoreError> {
        match self {
            Self::Sqlite(b) => b.scan::<T>(codec).await,
            #[cfg(feature = "redb")]
            Self::Redb(b) => b.scan::<T>(codec).await,
        }
    }

    async fn count<T: Persistent>(&self) -> Result<i64, StoreError> {
        match self {
            Self::Sqlite(b) => b.count::<T>().await,
            #[cfg(feature = "redb")]
            Self::Redb(b) => b.count::<T>().await,
        }
    }

    async fn query<T: Persistent>(
        &self,
        request: QueryRequest,
        codec: Codec,
    ) -> Result<Vec<T>, StoreError> {
        match self {
            Self::Sqlite(b) => b.query::<T>(request, codec).await,
            #[cfg(feature = "redb")]
            Self::Redb(b) => b.query::<T>(request, codec).await,
        }
    }

    async fn query_count(&self, request: QueryRequest) -> Result<i64, StoreError> {
        match self {
            Self::Sqlite(b) => b.query_count(request).await,
            #[cfg(feature = "redb")]
            Self::Redb(b) => b.query_count(request).await,
        }
    }

    async fn count_grouped_by<T: Persistent>(
        &self,
        column: &str,
    ) -> Result<HashMap<String, i64>, StoreError> {
        match self {
            Self::Sqlite(b) => b.count_grouped_by::<T>(column).await,
            #[cfg(feature = "redb")]
            Self::Redb(b) => b.count_grouped_by::<T>(column).await,
        }
    }

    async fn replace_where<T: Persistent>(
        &self,
        filters: &[(String, String)],
        items: &[(Vec<u8>, Vec<u8>, Vec<String>)],
        codec: Codec,
    ) -> Result<(), StoreError> {
        match self {
            Self::Sqlite(b) => b.replace_where::<T>(filters, items, codec).await,
            #[cfg(feature = "redb")]
            Self::Redb(b) => b.replace_where::<T>(filters, items, codec).await,
        }
    }

    async fn save_batch(
        &self,
        batch: &crate::backend::BackendBatch,
        codec: Codec,
    ) -> Result<(), StoreError> {
        match self {
            Self::Sqlite(b) => b.save_batch(batch, codec).await,
            #[cfg(feature = "redb")]
            Self::Redb(b) => b.save_batch(batch, codec).await,
        }
    }

    fn as_sqlite_pool(&self) -> Option<&sqlx::SqlitePool> {
        match self {
            Self::Sqlite(b) => b.as_sqlite_pool(),
            #[cfg(feature = "redb")]
            Self::Redb(b) => b.as_sqlite_pool(),
        }
    }

    async fn query_raw<T: Persistent>(
        &self,
        sql: &str,
        codec: Codec,
    ) -> Result<Vec<T>, StoreError> {
        match self {
            Self::Sqlite(b) => b.query_raw::<T>(sql, codec).await,
            #[cfg(feature = "redb")]
            Self::Redb(b) => b.query_raw::<T>(sql, codec).await,
        }
    }
}

// ── StoreBuilder ──────────────────────────────────────────────────────────────

/// Builder for configuring a [`Store`] before opening.
pub struct StoreBuilder {
    path: String,
    codec: Option<Codec>,
    backend: BackendKind,
    pool_size: Option<u32>,
}

impl StoreBuilder {
    fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            codec: None,
            backend: BackendKind::Sqlite,
            pool_size: None,
        }
    }

    /// Set a custom serialization codec.
    pub fn codec(mut self, codec: Codec) -> Self {
        self.codec = Some(codec);
        self
    }

    /// Set the backend kind (default: SQLite).
    pub fn backend(mut self, kind: BackendKind) -> Self {
        self.backend = kind;
        self
    }

    /// Set the connection pool size (SQLite only — reserved for future use).
    pub fn pool_size(mut self, n: u32) -> Self {
        self.pool_size = Some(n);
        self
    }

    /// Open the store with the configured options.
    pub async fn open(self) -> Result<Store, StoreError> {
        let inner = match self.backend {
            BackendKind::Sqlite => {
                BackendImpl::Sqlite(Arc::new(SqliteBackend::open(&self.path).await?))
            }
            #[cfg(feature = "redb")]
            BackendKind::Redb => BackendImpl::Redb(Arc::new(RedbBackend::open(&self.path).await?)),
        };
        Ok(Store {
            inner,
            codec: self.codec.unwrap_or_default(),
        })
    }
}

// ── Store ─────────────────────────────────────────────────────────────────────

/// An async store backed by SQLite or redb. Cheap to clone — `Arc`-wrapped internally.
#[derive(Clone)]
pub struct Store {
    inner: BackendImpl,
    codec: Codec,
}

impl Store {
    // ── constructors ─────────────────────────────────────────────────────────

    /// Open or create a persistent SQLite database at `path`.
    pub async fn open(path: &str) -> Result<Self, StoreError> {
        StoreBuilder::new(path).open().await
    }

    /// Open a transient in-memory SQLite database. All data is lost when dropped.
    pub async fn in_memory() -> Result<Self, StoreError> {
        StoreBuilder::new(":memory:").open().await
    }

    /// Open or create a redb database at `path`.
    #[cfg(feature = "redb")]
    pub async fn open_redb(path: &str) -> Result<Self, StoreError> {
        StoreBuilder::new(path)
            .backend(BackendKind::Redb)
            .open()
            .await
    }

    /// Create a [`StoreBuilder`] for advanced configuration.
    pub fn builder(path: impl Into<String>) -> StoreBuilder {
        StoreBuilder::new(path)
    }

    // ── schema ────────────────────────────────────────────────────────────────

    pub(crate) async fn ensure_table<T: Persistent>(&self) -> Result<(), StoreError> {
        self.inner.ensure_table::<T>().await
    }

    // ── core CRUD ─────────────────────────────────────────────────────────────

    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, StoreError> {
        self.codec.encode(value)
    }

    /// Persist a value. **Upsert** semantics: inserts or overwrites by the
    /// struct's embedded [`AirId`](crate::AirId).
    pub async fn save<T: Persistent>(&self, value: &T) -> Result<(), StoreError> {
        self.ensure_table::<T>().await?;
        self.inner.save(value, self.codec).await
    }

    /// Load a value by id. Accepts an [`AirId`](crate::AirId) or a reference to the value itself.
    pub async fn load<T, I>(&self, input: I) -> Result<Option<T>, StoreError>
    where
        T: Persistent,
        I: IntoAirId<T>,
    {
        self.ensure_table::<T>().await?;
        let id_bytes = input.into_air_id().to_bytes();
        self.inner.load::<T>(&id_bytes, self.codec).await
    }

    /// Load many values by id in a single query.
    pub async fn load_many<T, I>(&self, ids: &[I]) -> Result<Vec<T>, StoreError>
    where
        T: Persistent,
        I: Clone + IntoAirId<T>,
    {
        self.ensure_table::<T>().await?;
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let id_bytes: Vec<Vec<u8>> = ids
            .iter()
            .map(|id| id.clone().into_air_id().to_bytes())
            .collect();
        self.inner.load_many::<T>(&id_bytes, self.codec).await
    }

    /// Check whether an id exists in the store.
    pub async fn exists<T, I>(&self, input: I) -> Result<bool, StoreError>
    where
        T: Persistent,
        I: IntoAirId<T>,
    {
        self.ensure_table::<T>().await?;
        let id_bytes = input.into_air_id().to_bytes();
        self.inner.exists::<T>(&id_bytes).await
    }

    /// Delete a value by id. No-op if the id doesn't exist.
    pub async fn delete<T, I>(&self, input: I) -> Result<(), StoreError>
    where
        T: Persistent,
        I: IntoAirId<T>,
    {
        self.ensure_table::<T>().await?;
        let id_bytes = input.into_air_id().to_bytes();
        self.inner.delete::<T>(&id_bytes).await
    }

    /// Delete **all** rows of type `T`. Returns the number of rows deleted.
    pub async fn delete_all<T: Persistent>(&self) -> Result<u64, StoreError> {
        self.ensure_table::<T>().await?;
        self.inner.delete_all::<T>().await
    }

    /// Scan all values of type `T`, ordered by save time (SQLite) or
    /// arbitrary order (redb).
    pub async fn scan<T: Persistent>(&self) -> Result<Vec<T>, StoreError> {
        self.ensure_table::<T>().await?;
        self.inner.scan::<T>(self.codec).await
    }

    /// Alias for [`Store::scan`] — load every record of type `T` into memory.
    pub async fn load_all<T: Persistent>(&self) -> Result<Vec<T>, StoreError> {
        self.scan::<T>().await
    }

    /// Count all stored values of type `T`.
    pub async fn count<T: Persistent>(&self) -> Result<i64, StoreError> {
        self.ensure_table::<T>().await?;
        self.inner.count::<T>().await
    }

    // ── convenience helpers ───────────────────────────────────────────────────

    /// Load → mutate → save. Returns `None` if the id doesn't exist.
    pub async fn update<T, I, F>(&self, input: I, f: F) -> Result<Option<T>, StoreError>
    where
        T: Persistent + Clone,
        I: IntoAirId<T>,
        F: FnOnce(&mut T),
    {
        match self.load(input).await? {
            Some(mut value) => {
                f(&mut value);
                self.save(&value).await?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Atomically save multiple values (possibly of different types) in one transaction.
    pub async fn save_batch(&self, batch: StoreBatch) -> Result<(), StoreError> {
        self.inner.save_batch(&batch.inner, self.codec).await
    }

    /// Start a typed query for `T`.
    pub fn find<T: Persistent>(&self) -> Query<'_, T> {
        Query::new(self)
    }

    /// Execute a raw SQL query and decode the `v` blob column as `T`.
    ///
    /// Only available when using the SQLite backend.
    pub async fn query_raw<T: Persistent>(&self, sql: &str) -> Result<Vec<T>, StoreError> {
        self.inner.query_raw::<T>(sql, self.codec).await
    }

    /// Access the underlying sqlx pool for custom queries (SQLite only).
    pub fn pool(&self) -> Option<&sqlx::SqlitePool> {
        self.inner.as_sqlite_pool()
    }

    // ── bulk / set helpers ────────────────────────────────────────────────────

    /// Count rows grouped by an indexed column. Returns a map of column value → count.
    pub async fn count_grouped_by<T: Persistent>(
        &self,
        column: &str,
    ) -> Result<HashMap<String, i64>, StoreError> {
        self.ensure_table::<T>().await?;
        self.inner.count_grouped_by::<T>(column).await
    }

    /// Delete rows matching filters, then insert the given items.
    ///
    /// Deletes first (not transactional with inserts — see `save_batch` for
    /// atomic multi-item writes). The ergonomic wrapper is the typed
    /// `replace_for` builder generated by `#[persistent]`.
    pub async fn replace_where<T: Persistent>(
        &self,
        filters: &[(String, String)],
        items: &[T],
    ) -> Result<(), StoreError> {
        self.ensure_table::<T>().await?;
        let mut prepared = Vec::with_capacity(items.len());
        for item in items {
            let id = item.id().to_bytes();
            let v = self.encode(item)?;
            let index_vals = item.index_values();
            prepared.push((id, v, index_vals));
        }
        self.inner
            .replace_where::<T>(filters, &prepared, self.codec)
            .await
    }

    // ── init registration ─────────────────────────────────────────────────────

    /// Ensure tables exist for a single type or a tuple of types.
    pub async fn init<T: InitMany>(&self) -> Result<(), StoreError> {
        T::init(self).await
    }

    /// Create a [`StoreBatch`] pre-configured with this store's codec.
    pub fn batch(&self) -> StoreBatch {
        StoreBatch::with_codec(self.codec)
    }
}

// ── Query builder ─────────────────────────────────────────────────────────────

/// Typed query builder for indexed columns.
///
/// Created via [`Store::find`] or the macro-generated `T::find`.
pub struct Query<'a, T: Persistent> {
    store: &'a Store,
    filters: Vec<Filter>,
    order_by: Vec<(String, Order)>,
    limit: Option<usize>,
    _phantom: PhantomData<T>,
}

impl<'a, T: Persistent> Query<'a, T> {
    fn new(store: &'a Store) -> Self {
        Self {
            store,
            filters: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            _phantom: PhantomData,
        }
    }

    /// Filter where `column` equals `value`.
    pub fn eq(mut self, column: &str, value: impl ToIndexValue) -> Self {
        self.filters
            .push(Filter::Eq(column.to_string(), value.to_index_value()));
        self
    }

    /// Filter where `column` is in `values`.
    pub fn in_(mut self, column: &str, values: &[impl ToIndexValue]) -> Self {
        let vals: Vec<String> = values.iter().map(|v| v.to_index_value()).collect();
        self.filters.push(Filter::In(column.to_string(), vals));
        self
    }

    /// Add an ORDER BY clause.
    pub fn order_by(mut self, column: &str, order: Order) -> Self {
        self.order_by.push((column.to_string(), order));
        self
    }

    /// Limit the number of results.
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    fn to_request(&self) -> QueryRequest {
        QueryRequest {
            table: T::TABLE,
            filters: self.filters.clone(),
            order_by: self.order_by.clone(),
            limit: self.limit,
        }
    }

    /// Execute the query and return all matching rows.
    pub async fn all(self) -> Result<Vec<T>, StoreError> {
        self.store.ensure_table::<T>().await?;
        for f in &self.filters {
            if let Filter::In(_, vals) = f
                && vals.is_empty()
            {
                return Ok(vec![]);
            }
        }
        let request = self.to_request();
        self.store.inner.query::<T>(request, self.store.codec).await
    }

    /// Execute the query and return at most one row.
    pub async fn first(self) -> Result<Option<T>, StoreError> {
        let mut rows = self.limit(1).all().await?;
        Ok(rows.pop())
    }

    /// Count matching rows without decoding blobs.
    pub async fn count(self) -> Result<i64, StoreError> {
        self.store.ensure_table::<T>().await?;
        for f in &self.filters {
            if let Filter::In(_, vals) = f
                && vals.is_empty()
            {
                return Ok(0);
            }
        }
        let request = self.to_request();
        self.store.inner.query_count(request).await
    }
}

// ── ReplaceBuilder ────────────────────────────────────────────────────────────

/// Generic replace builder. Use the macro-generated typed wrapper instead.
pub struct ReplaceBuilder<'a, T: Persistent> {
    store: &'a Store,
    filters: Vec<(String, String)>,
    _phantom: PhantomData<T>,
}

impl<'a, T: Persistent> ReplaceBuilder<'a, T> {
    pub fn new(store: &'a Store) -> Self {
        Self {
            store,
            filters: Vec::new(),
            _phantom: PhantomData,
        }
    }

    /// Add an equality filter.
    pub fn eq(mut self, column: &str, value: impl ToIndexValue) -> Self {
        self.filters
            .push((column.to_string(), value.to_index_value()));
        self
    }

    /// Execute the replace: delete matching rows, then insert items.
    pub async fn items(self, items: Vec<T>) -> Result<(), StoreError> {
        self.store.replace_where(&self.filters, &items).await
    }
}

// ── UpsertBuilder / UpsertModifyBuilder ───────────────────────────────────────

/// Generic upsert builder. Use the macro-generated typed wrapper instead.
pub struct UpsertBuilder<'a, T: Persistent> {
    store: &'a Store,
    column: Option<String>,
    value: Option<String>,
    _phantom: PhantomData<T>,
}

impl<'a, T: Persistent> UpsertBuilder<'a, T> {
    pub fn new(store: &'a Store) -> Self {
        Self {
            store,
            column: None,
            value: None,
            _phantom: PhantomData,
        }
    }

    /// Add an equality filter.
    pub fn eq(mut self, column: &str, value: impl ToIndexValue) -> Self {
        self.column = Some(column.to_string());
        self.value = Some(value.to_index_value());
        self
    }

    /// Provide the modify closure. Returns a builder that requires `or_insert`.
    pub fn modify<F: FnOnce(&mut T)>(self, f: F) -> UpsertModifyBuilder<'a, T, F> {
        UpsertModifyBuilder {
            store: self.store,
            column: self.column.unwrap_or_default(),
            value: self.value.unwrap_or_default(),
            modify: f,
            _phantom: PhantomData,
        }
    }
}

/// Second stage of upsert: requires `or_insert` before awaiting.
pub struct UpsertModifyBuilder<'a, T: Persistent, F> {
    store: &'a Store,
    column: String,
    value: String,
    modify: F,
    _phantom: PhantomData<T>,
}

impl<'a, T: Persistent + Clone, F: FnOnce(&mut T)> UpsertModifyBuilder<'a, T, F> {
    /// Finish the upsert: modify existing row or insert a new one.
    pub async fn or_insert<G: FnOnce() -> T>(self, g: G) -> Result<T, StoreError> {
        match self
            .store
            .find::<T>()
            .eq(&self.column, &self.value)
            .first()
            .await?
        {
            Some(mut item) => {
                (self.modify)(&mut item);
                self.store.save(&item).await?;
                Ok(item)
            }
            None => {
                let item = g();
                self.store.save(&item).await?;
                Ok(item)
            }
        }
    }
}

// ── StoreBatch ────────────────────────────────────────────────────────────────

/// Accumulate heterogeneous saves for one atomic write.
#[derive(Default)]
pub struct StoreBatch {
    pub(crate) inner: BackendBatch,
    codec: Option<Codec>,
}

impl StoreBatch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a batch that uses a specific codec for encoding values.
    pub fn with_codec(codec: Codec) -> Self {
        Self {
            codec: Some(codec),
            ..Default::default()
        }
    }

    /// Stage a value for saving. Uses the embedded [`AirId`](crate::AirId) inside `value`.
    pub fn push<T: Persistent>(&mut self, value: &T) -> Result<(), StoreError> {
        let id_bytes = value.id().to_bytes();
        let v = match &self.codec {
            Some(codec) => codec.encode(value)?,
            None => bitcode::serialize(value).map_err(StoreError::Encode)?,
        };
        self.inner.entries.push(crate::backend::BatchEntry {
            table: T::TABLE,
            id_bytes,
            value_bytes: v,
            index_columns: T::index_columns(),
            index_values: value.index_values(),
        });
        Ok(())
    }
}

// ── InitMany trait ────────────────────────────────────────────────────────────

/// Trait for initializing multiple tables at once via [`Store::init`].
pub trait InitMany {
    fn init(store: &Store) -> impl std::future::Future<Output = Result<(), StoreError>> + Send;
}

impl InitMany for () {
    async fn init(_store: &Store) -> Result<(), StoreError> {
        Ok(())
    }
}

impl<T: Persistent> InitMany for T {
    async fn init(store: &Store) -> Result<(), StoreError> {
        store.ensure_table::<T>().await
    }
}

macro_rules! impl_init_many {
    ($($T:ident),+) => {
        impl<$($T: Persistent),+> InitMany for ($($T,)+) {
            async fn init(store: &Store) -> Result<(), StoreError> {
                $(store.ensure_table::<$T>().await?;)+
                Ok(())
            }
        }
    };
}

impl_init_many!(A);
impl_init_many!(A, B);
impl_init_many!(A, B, C);
impl_init_many!(A, B, C, D);
impl_init_many!(A, B, C, D, E);
impl_init_many!(A, B, C, D, E, F);
impl_init_many!(A, B, C, D, E, F, G);
impl_init_many!(A, B, C, D, E, F, G, H);
impl_init_many!(A, B, C, D, E, F, G, H, I);
impl_init_many!(A, B, C, D, E, F, G, H, I, J);
impl_init_many!(A, B, C, D, E, F, G, H, I, J, K);
impl_init_many!(A, B, C, D, E, F, G, H, I, J, K, L);
