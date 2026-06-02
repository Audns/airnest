//! `Store` — one handle, one file, all types.
//!
//! Every `save`/`load`/`delete`/`scan` call is async-friendly via sqlx.

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use tokio::sync::Mutex;

use crate::{
    codec::Codec, error::StoreError, index::ToIndexValue, into_air_id::IntoAirId,
    persistent::Persistent,
};

// ── inner connection ──────────────────────────────────────────────────────────

#[derive(Clone)]
struct Inner {
    pool: SqlitePool,
    tables: Arc<Mutex<HashSet<&'static str>>>,
    codec: Codec,
}

impl Inner {
    async fn open(path: &str) -> Result<Self, StoreError> {
        let pool = if path == ":memory:" {
            SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await?
        } else {
            if let Some(parent) = std::path::Path::new(path).parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let options = SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(true);
            SqlitePool::connect_with(options).await?
        };

        sqlx::query(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA foreign_keys=OFF;
            ",
        )
        .execute(&pool)
        .await?;

        Ok(Self {
            pool,
            tables: Arc::new(Mutex::new(HashSet::new())),
            codec: Codec::Bitcode,
        })
    }
}

// ── StoreBuilder ──────────────────────────────────────────────────────────────

/// Builder for configuring a [`Store`] before opening.
pub struct StoreBuilder {
    path: String,
    codec: Option<Codec>,
    pool_size: Option<u32>,
}

impl StoreBuilder {
    fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            codec: None,
            pool_size: None,
        }
    }

    /// Set a custom serialization codec.
    pub fn codec(mut self, codec: Codec) -> Self {
        self.codec = Some(codec);
        self
    }

    /// Set the connection pool size (currently ignored — reserved for future use).
    pub fn pool_size(mut self, n: u32) -> Self {
        self.pool_size = Some(n);
        self
    }

    /// Open the store with the configured options.
    pub async fn open(self) -> Result<Store, StoreError> {
        let mut inner = Inner::open(&self.path).await?;
        if let Some(codec) = self.codec {
            inner.codec = codec;
        }
        Ok(Store { inner })
    }
}

// ── Store ─────────────────────────────────────────────────────────────────────

/// An async SQLite-backed store. Cheap to clone — `Arc`-wrapped internally.
#[derive(Clone)]
pub struct Store {
    inner: Inner,
}

impl Store {
    // ── constructors ─────────────────────────────────────────────────────────

    /// Open or create a persistent SQLite database at `path`.
    pub async fn open(path: &str) -> Result<Self, StoreError> {
        StoreBuilder::new(path).open().await
    }

    /// Open a transient in-memory database. All data is lost when dropped.
    pub async fn in_memory() -> Result<Self, StoreError> {
        StoreBuilder::new(":memory:").open().await
    }

    /// Create a [`StoreBuilder`] for advanced configuration.
    pub fn builder(path: impl Into<String>) -> StoreBuilder {
        StoreBuilder::new(path)
    }

    // ── schema ────────────────────────────────────────────────────────────────

    pub(crate) async fn ensure_table<T: Persistent>(&self) -> Result<(), StoreError> {
        let table = T::TABLE;
        {
            let guard = self.inner.tables.lock().await;
            if guard.contains(table) {
                return Ok(());
            }
        }

        let index_cols = T::index_columns().to_vec();
        let extra_cols: String = index_cols
            .iter()
            .map(|c| format!(",\n                     \"{c}\" TEXT"))
            .collect();

        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS \"{table}\" (\n                         id       BLOB    NOT NULL PRIMARY KEY,\n                         v        BLOB    NOT NULL,\n                         saved_at INTEGER NOT NULL DEFAULT (unixepoch()){extra_cols}\n                     ) STRICT"
        );

        sqlx::query(sqlx::AssertSqlSafe(&*create_sql))
            .execute(&self.inner.pool)
            .await?;

        for col in &index_cols {
            let _ = sqlx::query(sqlx::AssertSqlSafe(&*format!(
                "ALTER TABLE \"{table}\" ADD COLUMN \"{col}\" TEXT"
            )))
            .execute(&self.inner.pool)
            .await;
        }

        sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "CREATE INDEX IF NOT EXISTS \"{table}_saved_at\"\n                     ON \"{table}\" (saved_at)"
        )))
        .execute(&self.inner.pool)
        .await?;

        for col in &index_cols {
            sqlx::query(sqlx::AssertSqlSafe(&*format!(
                "CREATE INDEX IF NOT EXISTS \"{table}_{col}_idx\"\n                         ON \"{table}\" (\"{col}\")"
            )))
            .execute(&self.inner.pool)
            .await?;
        }

        let mut guard = self.inner.tables.lock().await;
        guard.insert(table);
        Ok(())
    }

    // ── core CRUD ─────────────────────────────────────────────────────────────

    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, StoreError> {
        self.inner.codec.encode(value)
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, StoreError> {
        self.inner.codec.decode(bytes)
    }

    /// Persist a value. **Upsert** semantics: inserts or overwrites by the
    /// struct's embedded [`AirId`].
    pub async fn save<T: Persistent>(&self, value: &T) -> Result<(), StoreError> {
        self.ensure_table::<T>().await?;

        let id_bytes = value.id().to_bytes();
        let v = self.encode(value)?;
        let table = T::TABLE;
        let index_cols = T::index_columns().to_vec();
        let index_vals = value.index_values();

        if index_cols.is_empty() {
            sqlx::query(sqlx::AssertSqlSafe(&*format!(
                "INSERT INTO \"{table}\" (id, v, saved_at)\n                             VALUES (?1, ?2, unixepoch())\n                             ON CONFLICT(id) DO UPDATE SET\n                                 v        = excluded.v,\n                                 saved_at = excluded.saved_at"
            )))
            .bind(&id_bytes)
            .bind(&v)
            .execute(&self.inner.pool)
            .await?;
        } else {
            let col_list: String = index_cols.iter().map(|c| format!(", \"{c}\"")).collect();
            let placeholders: String = (3..=2 + index_cols.len())
                .map(|i| format!(", ?{i}"))
                .collect();
            let updates: String = index_cols
                .iter()
                .map(|c| format!(", \"{c}\" = excluded.\"{c}\""))
                .collect();
            let sql = format!(
                "INSERT INTO \"{table}\" (id, v, saved_at{col_list})\n                         VALUES (?1, ?2, unixepoch(){placeholders})\n                         ON CONFLICT(id) DO UPDATE SET\n                             v        = excluded.v,\n                             saved_at = excluded.saved_at{updates}"
            );
            let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
            query = query.bind(&id_bytes).bind(&v);
            for val in index_vals {
                query = query.bind(val);
            }
            query.execute(&self.inner.pool).await?;
        }
        Ok(())
    }

    /// Load a value by id. Accepts an [`AirId`] or a reference to the value itself.
    pub async fn load<T, I>(&self, input: I) -> Result<Option<T>, StoreError>
    where
        T: Persistent,
        I: IntoAirId<T>,
    {
        self.ensure_table::<T>().await?;
        let id_bytes = input.into_air_id().to_bytes();
        let table = T::TABLE;

        let row = sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "SELECT v FROM \"{table}\" WHERE id = ?1"
        )))
        .bind(&id_bytes)
        .fetch_optional(&self.inner.pool)
        .await?;

        match row {
            Some(r) => {
                let bytes: Vec<u8> = r.get(0);
                Ok(Some(self.decode(&bytes)?))
            }
            None => Ok(None),
        }
    }

    /// Load many values by id in a single query.
    pub async fn load_many<T, I>(&self, ids: &[I]) -> Result<Vec<T>, StoreError>
    where
        T: Persistent,
        I: Clone + IntoAirId<T>,
    {
        self.ensure_table::<T>().await?;
        let table = T::TABLE;
        if ids.is_empty() {
            return Ok(vec![]);
        }

        let placeholders: Vec<String> = (1..=ids.len()).map(|i| format!("?{i}")).collect();
        let sql = format!(
            r#"SELECT v FROM "{table}" WHERE id IN ({})"#,
            placeholders.join(", ")
        );
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        for id in ids {
            query = query.bind(id.clone().into_air_id().to_bytes());
        }

        let rows = query.fetch_all(&self.inner.pool).await?;
        rows.into_iter()
            .map(|r| {
                let bytes: Vec<u8> = r.get(0);
                self.decode(&bytes)
            })
            .collect::<Result<Vec<T>, _>>()
    }

    /// Check whether an id exists in the store.
    pub async fn exists<T, I>(&self, input: I) -> Result<bool, StoreError>
    where
        T: Persistent,
        I: IntoAirId<T>,
    {
        self.ensure_table::<T>().await?;
        let id_bytes = input.into_air_id().to_bytes();
        let table = T::TABLE;

        let row = sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "SELECT COUNT(*) FROM \"{table}\" WHERE id = ?1"
        )))
        .bind(&id_bytes)
        .fetch_one(&self.inner.pool)
        .await?;

        let n: i64 = row.get(0);
        Ok(n > 0)
    }

    /// Delete a value by id. No-op if the id doesn't exist.
    pub async fn delete<T, I>(&self, input: I) -> Result<(), StoreError>
    where
        T: Persistent,
        I: IntoAirId<T>,
    {
        self.ensure_table::<T>().await?;
        let id_bytes = input.into_air_id().to_bytes();
        let table = T::TABLE;

        sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "DELETE FROM \"{table}\" WHERE id = ?1"
        )))
        .bind(&id_bytes)
        .execute(&self.inner.pool)
        .await?;

        Ok(())
    }

    /// Delete **all** rows of type `T`. Returns the number of rows deleted.
    pub async fn delete_all<T: Persistent>(&self) -> Result<u64, StoreError> {
        self.ensure_table::<T>().await?;
        let table = T::TABLE;

        let result = sqlx::query(sqlx::AssertSqlSafe(&*format!("DELETE FROM \"{table}\"")))
            .execute(&self.inner.pool)
            .await?;

        Ok(result.rows_affected())
    }

    /// Scan all values of type `T`, ordered by save time.
    pub async fn scan<T: Persistent>(&self) -> Result<Vec<T>, StoreError> {
        self.ensure_table::<T>().await?;
        let table = T::TABLE;

        let rows = sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "SELECT v FROM \"{table}\" ORDER BY saved_at ASC"
        )))
        .fetch_all(&self.inner.pool)
        .await?;

        rows.into_iter()
            .map(|r| {
                let bytes: Vec<u8> = r.get(0);
                self.decode(&bytes)
            })
            .collect::<Result<Vec<T>, _>>()
    }

    /// Alias for [`Store::scan`] — load every record of type `T` into memory.
    pub async fn load_all<T: Persistent>(&self) -> Result<Vec<T>, StoreError> {
        self.scan::<T>().await
    }

    /// Count all stored values of type `T`.
    pub async fn count<T: Persistent>(&self) -> Result<i64, StoreError> {
        self.ensure_table::<T>().await?;
        let table = T::TABLE;

        let row = sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "SELECT COUNT(*) FROM \"{table}\""
        )))
        .fetch_one(&self.inner.pool)
        .await?;

        let n: i64 = row.get(0);
        Ok(n)
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
        let setup_sqls = batch.table_setup_sqls;
        let tables = batch.tables;
        let entries = batch.entries;

        for sql in &setup_sqls {
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .execute(&self.inner.pool)
                .await?;
        }

        {
            let mut guard = self.inner.tables.lock().await;
            for table in &tables {
                guard.insert(table);
            }
        }

        let mut tx = self.inner.pool.begin().await?;

        for (sql, id_bytes, v, index_vals) in &entries {
            if index_vals.is_empty() {
                sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(id_bytes)
                    .bind(v)
                    .execute(&mut *tx)
                    .await?;
            } else {
                let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
                query = query.bind(id_bytes).bind(v);
                for val in index_vals {
                    query = query.bind(val);
                }
                query.execute(&mut *tx).await?;
            }
        }

        tx.commit().await?;
        Ok(())
    }

    /// Start a typed query for `T`.
    pub fn find<T: Persistent>(&self) -> Query<'_, T> {
        Query::new(self)
    }

    /// Execute a raw SQL query and decode the `v` blob column as `T`.
    pub async fn query_raw<T: Persistent>(&self, sql: &str) -> Result<Vec<T>, StoreError> {
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .fetch_all(&self.inner.pool)
            .await?;

        rows.into_iter()
            .map(|r| {
                let bytes: Vec<u8> = r.get(0);
                self.decode(&bytes)
            })
            .collect()
    }

    /// Access the underlying sqlx pool for custom queries.
    pub fn pool(&self) -> &SqlitePool {
        &self.inner.pool
    }

    // ── bulk / set helpers ────────────────────────────────────────────────────

    /// Count rows grouped by an indexed column. Returns a map of column value → count.
    pub async fn count_grouped_by<T: Persistent>(
        &self,
        column: &str,
    ) -> Result<HashMap<String, i64>, StoreError> {
        self.ensure_table::<T>().await?;
        let table = T::TABLE;
        let sql = format!(r#"SELECT "{column}", COUNT(*) FROM "{table}" GROUP BY "{column}""#);
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .fetch_all(&self.inner.pool)
            .await?;
        let mut map = HashMap::new();
        for row in rows {
            if let Ok(Some(key)) = row.try_get::<Option<String>, _>(0) {
                let count: i64 = row.get(1);
                map.insert(key, count);
            }
        }
        Ok(map)
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
        let table = T::TABLE;

        if !filters.is_empty() {
            let conditions: Vec<String> = filters
                .iter()
                .enumerate()
                .map(|(i, (col, _))| format!(r#""{col}" = ?{}"#, i + 1))
                .collect();
            let sql = format!(
                r#"DELETE FROM "{table}" WHERE {}"#,
                conditions.join(" AND ")
            );
            let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
            for (_, val) in filters {
                query = query.bind(val);
            }
            query.execute(&self.inner.pool).await?;
        } else {
            sqlx::query(sqlx::AssertSqlSafe(
                format!(r#"DELETE FROM "{table}""#).as_str(),
            ))
            .execute(&self.inner.pool)
            .await?;
        }

        let mut batch = StoreBatch::with_codec(self.inner.codec);
        for item in items {
            batch.push(item)?;
        }
        self.save_batch(batch).await
    }

    // ── init registration ─────────────────────────────────────────────────────

    /// Ensure tables exist for a single type or a tuple of types.
    pub async fn init<T: InitMany>(&self) -> Result<(), StoreError> {
        T::init(self).await
    }

    /// Create a [`StoreBatch`] pre-configured with this store's codec.
    pub fn batch(&self) -> StoreBatch {
        StoreBatch::with_codec(self.inner.codec)
    }
}

// ── Query builder ─────────────────────────────────────────────────────────────

/// Filter condition for [`Query`].
enum Filter {
    Eq(String, String),
    In(String, Vec<String>),
}

/// Sort direction for [`Query::order_by`].
pub enum Order {
    Asc,
    Desc,
}

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

    fn build_sql(&self, table: &str, select: &str) -> (String, Vec<String>) {
        let mut sql = format!(r#"{select} FROM "{table}""#);
        let mut params: Vec<String> = Vec::new();
        let mut param_idx = 1;

        if !self.filters.is_empty() {
            let conditions: Vec<String> = self
                .filters
                .iter()
                .map(|f| match f {
                    Filter::Eq(col, _) => {
                        let p = param_idx;
                        param_idx += 1;
                        format!(r#""{col}" = ?{p}"#)
                    }
                    Filter::In(col, vals) => {
                        let start = param_idx;
                        param_idx += vals.len();
                        let placeholders: Vec<String> =
                            (start..param_idx).map(|j| format!("?{j}")).collect();
                        format!(r#""{col}" IN ({})"#, placeholders.join(", "))
                    }
                })
                .collect();
            sql.push_str(&format!(" WHERE {}", conditions.join(" AND ")));
            for f in &self.filters {
                match f {
                    Filter::Eq(_, v) => params.push(v.clone()),
                    Filter::In(_, vals) => params.extend(vals.clone()),
                }
            }
        }

        for (col, order) in &self.order_by {
            let dir = match order {
                Order::Asc => "ASC",
                Order::Desc => "DESC",
            };
            sql.push_str(&format!(r#" ORDER BY "{col}" {dir}"#));
        }

        if let Some(n) = self.limit {
            sql.push_str(&format!(" LIMIT {n}"));
        }

        (sql, params)
    }

    /// Execute the query and return all matching rows.
    pub async fn all(self) -> Result<Vec<T>, StoreError> {
        self.store.ensure_table::<T>().await?;

        // Early exit: any empty IN filter means no results.
        for f in &self.filters {
            if let Filter::In(_, vals) = f
                && vals.is_empty()
            {
                return Ok(vec![]);
            }
        }

        let table = T::TABLE;
        let (sql, params) = self.build_sql(table, r#"SELECT v"#);
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        for p in params {
            query = query.bind(p);
        }

        let rows = query.fetch_all(&self.store.inner.pool).await?;

        rows.into_iter()
            .map(|r| {
                let bytes: Vec<u8> = r.get(0);
                self.store.decode(&bytes)
            })
            .collect::<Result<Vec<T>, _>>()
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

        let table = T::TABLE;
        let (sql, params) = self.build_sql(table, "SELECT COUNT(*)");
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        for p in params {
            query = query.bind(p);
        }

        let row = query.fetch_one(&self.store.inner.pool).await?;
        let n: i64 = row.get(0);
        Ok(n)
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

/// Single entry in a [`StoreBatch`]: (upsert_sql, id_bytes, v_bytes, index_vals).
pub type BatchEntry = (String, Vec<u8>, Vec<u8>, Vec<String>);

/// Accumulate heterogeneous saves for one atomic write.
#[derive(Default)]
pub struct StoreBatch {
    pub(crate) table_setup_sqls: Vec<String>,
    pub(crate) tables: Vec<&'static str>,
    pub(crate) entries: Vec<BatchEntry>,
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

    /// Stage a value for saving. Uses the embedded [`AirId`] inside `value`.
    pub fn push<T: Persistent>(&mut self, value: &T) -> Result<(), StoreError> {
        let id_bytes = value.id().to_bytes();
        let index_cols = T::index_columns();
        let index_vals = value.index_values();

        let v = match &self.codec {
            Some(codec) => codec.encode(value)?,
            None => bitcode::serialize(value).map_err(StoreError::Encode)?,
        };

        let extra_cols: String = index_cols
            .iter()
            .map(|c| format!(",\n                 \"{c}\" TEXT"))
            .collect();
        self.table_setup_sqls.push(format!(
            "CREATE TABLE IF NOT EXISTS \"{table}\" (\n                 id       BLOB    NOT NULL PRIMARY KEY,\n                 v        BLOB    NOT NULL,\n                 saved_at INTEGER NOT NULL DEFAULT (unixepoch()){extra_cols}\n             ) STRICT",
            table = T::TABLE,
        ));
        self.tables.push(T::TABLE);

        let upsert_sql = if index_cols.is_empty() {
            format!(
                "INSERT INTO \"{table}\" (id, v, saved_at)\n                 VALUES (?1, ?2, unixepoch())\n                 ON CONFLICT(id) DO UPDATE SET\n                     v        = excluded.v,\n                     saved_at = excluded.saved_at",
                table = T::TABLE,
            )
        } else {
            let col_list: String = index_cols.iter().map(|c| format!(", \"{c}\"")).collect();
            let placeholders: String = (3..=2 + index_cols.len())
                .map(|i| format!(", ?{i}"))
                .collect();
            let updates: String = index_cols
                .iter()
                .map(|c| format!(", \"{c}\" = excluded.\"{c}\""))
                .collect();
            format!(
                "INSERT INTO \"{table}\" (id, v, saved_at{col_list})\n                 VALUES (?1, ?2, unixepoch(){placeholders})\n                 ON CONFLICT(id) DO UPDATE SET\n                     v        = excluded.v,\n                     saved_at = excluded.saved_at{updates}",
                table = T::TABLE,
            )
        };

        self.entries.push((upsert_sql, id_bytes, v, index_vals));
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
