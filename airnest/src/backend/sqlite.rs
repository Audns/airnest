//! `SQLite` backend implementation.
//!
//! All DDL/DML string generation is delegated to the internal `SqlDialect`
//! trait. This backend owns the connection pool, table-cache, and bind logic;
//! the dialect owns the SQL surface.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};

use crate::{
    backend::{
        Backend, BackendBatch, Filter, GuardOutcome, QueryRequest, SequenceGuard,
        dialect::{SqlDialect, TableSchema},
        sqlite_dialect::SqliteDialect,
    },
    codec::Codec,
    error::StoreError,
    persistent::Persistent,
};

/// Execution target for one statement: the shared pool or a
/// transaction connection. Schema DDL and CRUD run either outside or
/// inside a transaction without duplicating SQL construction.
///
/// DDL must run on the transaction's own connection: in-memory
/// databases use a single pooled connection, so touching the pool
/// while a transaction holds it would deadlock.
pub(crate) enum Db<'a> {
    Pool(&'a sqlx::SqlitePool),
    Tx(&'a mut sqlx::SqliteConnection),
}

impl Db<'_> {
    async fn execute(
        &mut self,
        sql: String,
    ) -> Result<sqlx::sqlite::SqliteQueryResult, StoreError> {
        match self {
            Db::Pool(pool) => sqlx::query(sqlx::AssertSqlSafe(sql))
                .execute(*pool)
                .await
                .map_err(Into::into),
            Db::Tx(tx) => sqlx::query(sqlx::AssertSqlSafe(sql))
                .execute(&mut **tx)
                .await
                .map_err(Into::into),
        }
    }
}

#[derive(Clone)]
pub struct SqliteBackend {
    pool: SqlitePool,
    tables: Arc<RwLock<HashSet<&'static str>>>,
    upsert_cache: Arc<RwLock<HashMap<String, String>>>,
    dialect: Arc<dyn SqlDialect>,
}

impl SqliteBackend {
    pub async fn open(path: &str) -> Result<Self, StoreError> {
        Self::open_with_pool(path, None).await
    }

    pub async fn open_with_pool(path: &str, pool_size: Option<u32>) -> Result<Self, StoreError> {
        let pool = if path == ":memory:" {
            SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await?
        } else {
            if let Some(parent) = std::path::Path::new(path).parent() {
                if !parent.as_os_str().is_empty() {
                    tokio::fs::create_dir_all(parent).await?;
                }
            }
            let mut options = SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(true)
                .busy_timeout(Duration::from_secs(5));
            // Statement cache is per-connection; 100 is a good default.
            options = options.statement_cache_capacity(100);
            let max_conns = pool_size.unwrap_or(8);
            SqlitePoolOptions::new()
                .max_connections(max_conns)
                .min_connections(1)
                .acquire_timeout(Duration::from_secs(5))
                .idle_timeout(Duration::from_secs(60))
                .connect_with(options)
                .await?
        };

        let dialect: Arc<dyn SqlDialect> = Arc::new(SqliteDialect);
        for stmt in dialect.session_init_sql() {
            // PRAGMAs are constants from the dialect; AssertSqlSafe documents
            // the audit.
            sqlx::query(sqlx::AssertSqlSafe(*stmt))
                .execute(&pool)
                .await?;
        }

        Ok(Self {
            pool,
            tables: Arc::new(RwLock::new(HashSet::new())),
            upsert_cache: Arc::new(RwLock::new(HashMap::new())),
            dialect,
        })
    }

    fn cached_upsert(&self, schema: &TableSchema) -> String {
        // Fast-path: read lock
        if let Ok(cache) = self.upsert_cache.read() {
            if let Some(sql) = cache.get(schema.table) {
                return sql.clone();
            }
        }
        // Miss: render and populate
        let sql = self.dialect.render_upsert(schema);
        if let Ok(mut cache) = self.upsert_cache.write() {
            cache.insert(schema.table.to_string(), sql.clone());
        }
        sql
    }

    async fn ensure_table_raw(
        &self,
        table: &'static str,
        index_cols: &[&'static str],
        unique: &[&[&str]],
    ) -> Result<(), StoreError> {
        // One DDL implementation for both targets: the pool here and a
        // transaction connection in `ensure_table_in`.
        let mut db = Db::Pool(&self.pool);
        self.ensure_table_in(&mut db, table, index_cols, unique, true)
            .await
    }

    /// The idempotent DDL behind [`SqliteBackend::ensure_table_raw`], on
    /// either target.
    ///
    /// `remember` records the table as created. A transaction passes
    /// `false`: its DDL rolls back with it, and a cache entry that
    /// outlived the rollback would skip the DDL forever after
    /// ("no such table"). Uncached tables in a transaction rerun the
    /// `IF NOT EXISTS` statements instead.
    async fn ensure_table_in(
        &self,
        db: &mut Db<'_>,
        table: &'static str,
        index_cols: &[&'static str],
        unique: &[&[&str]],
        remember: bool,
    ) -> Result<(), StoreError> {
        {
            let guard = self.tables.read().map_err(|_| StoreError::Poisoned)?;
            if guard.contains(table) {
                return Ok(());
            }
        }
        let schema = TableSchema {
            table,
            index_columns: index_cols,
        };
        db.execute(self.dialect.render_create_table(&schema)).await?;
        for col in index_cols {
            // ADD COLUMN is not idempotent in SQLite; the duplicate-column
            // error is the expected no-op.
            let _ = db.execute(self.dialect.render_add_column(table, col)).await;
        }
        db.execute(self.dialect.render_create_index(table, "saved_at"))
            .await?;
        for col in index_cols {
            db.execute(self.dialect.render_create_index(table, col)).await?;
        }
        for group in unique {
            db.execute(self.dialect.render_create_unique_index(table, group))
                .await?;
        }
        let upsert_sql = self.dialect.render_upsert(&schema);
        if let Ok(mut cache) = self.upsert_cache.write() {
            cache.insert(table.to_string(), upsert_sql);
        }
        if remember {
            let mut guard = self.tables.write().map_err(|_| StoreError::Poisoned)?;
            guard.insert(table);
        }
        Ok(())
    }

    async fn ensure_table_tx(
        &self,
        tx: &mut sqlx::Transaction<'static, sqlx::Sqlite>,
        table: &'static str,
        index_cols: &[&'static str],
        unique: &[&[&str]],
    ) -> Result<(), StoreError> {
        let mut db = Db::Tx(tx);
        self.ensure_table_in(&mut db, table, index_cols, unique, false)
            .await
    }

    /// Typed upsert on a transaction connection.
    pub(crate) async fn save_in<T: Persistent>(
        &self,
        tx: &mut sqlx::Transaction<'static, sqlx::Sqlite>,
        value: &T,
        codec: Codec,
    ) -> Result<(), StoreError> {
        self.ensure_table_tx(tx, T::TABLE, T::index_columns(), T::unique_constraints())
            .await?;
        let id_bytes = value.id().to_bytes();
        let v = codec.encode(value)?;
        let schema = TableSchema {
            table: T::TABLE,
            index_columns: T::index_columns(),
        };
        let sql = self.cached_upsert(&schema);
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        query = query.bind(&id_bytes[..]).bind(&v);
        for val in value.index_values() {
            query = query.bind(val);
        }
        query.execute(&mut **tx).await?;
        Ok(())
    }

    /// Typed point-read on a transaction connection.
    pub(crate) async fn load_in<T: Persistent>(
        &self,
        tx: &mut sqlx::Transaction<'static, sqlx::Sqlite>,
        id_bytes: &[u8],
        codec: Codec,
    ) -> Result<Option<T>, StoreError> {
        let sql = format!(
            "SELECT v FROM {} WHERE {} = {}",
            self.quote(T::TABLE),
            self.quote("id"),
            self.ph(1),
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(id_bytes)
            .fetch_optional(&mut **tx)
            .await?;
        match row {
            Some(r) => {
                let bytes: Vec<u8> = r.get(0);
                Ok(Some(crate::codec::decode_row::<T>(codec, &bytes)?))
            }
            None => Ok(None),
        }
    }

    /// Typed delete on a transaction connection.
    pub(crate) async fn delete_in<T: Persistent>(
        &self,
        tx: &mut sqlx::Transaction<'static, sqlx::Sqlite>,
        id_bytes: &[u8],
    ) -> Result<(), StoreError> {
        let sql = format!(
            "DELETE FROM {} WHERE {} = {}",
            self.quote(T::TABLE),
            self.quote("id"),
            self.ph(1),
        );
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(id_bytes)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    /// Typed indexed query on a transaction connection.
    pub(crate) async fn query_in<T: Persistent>(
        &self,
        tx: &mut sqlx::Transaction<'static, sqlx::Sqlite>,
        request: crate::backend::QueryRequest,
        codec: Codec,
    ) -> Result<Vec<T>, StoreError> {
        self.ensure_table_tx(tx, T::TABLE, T::index_columns(), T::unique_constraints())
            .await?;
        let rendered = self.dialect.render_select(&request, "SELECT v");
        let mut query = sqlx::query(sqlx::AssertSqlSafe(rendered.sql.as_str()));
        for p in rendered.binds {
            query = query.bind(p);
        }
        let rows = query.fetch_all(&mut **tx).await?;
        rows.into_iter()
            .map(|r| {
                let bytes: Vec<u8> = r.get(0);
                crate::codec::decode_row::<T>(codec, &bytes)
            })
            .collect::<Result<Vec<T>, _>>()
    }

    /// Plain insert on a transaction connection (see [`Backend::insert`]).
    pub(crate) async fn insert_in<T: Persistent>(
        &self,
        tx: &mut sqlx::Transaction<'static, sqlx::Sqlite>,
        value: &T,
        codec: Codec,
    ) -> Result<(), StoreError> {
        self.ensure_table_tx(tx, T::TABLE, T::index_columns(), T::unique_constraints())
            .await?;
        let schema = TableSchema {
            table: T::TABLE,
            index_columns: T::index_columns(),
        };
        let sql = self.dialect.render_insert(&schema);
        let id_bytes = value.id().to_bytes();
        let v = codec.encode(value)?;
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        query = query.bind(&id_bytes[..]).bind(&v);
        for val in value.index_values() {
            query = query.bind(val);
        }
        match query.execute(&mut **tx).await {
            Ok(_) => Ok(()),
            Err(e) if crate::backend::is_unique_violation(&e) => Err(StoreError::Conflict(
                format!("insert into `{}`: row exists", T::TABLE),
            )),
            Err(e) => Err(StoreError::Sqlite(e)),
        }
    }

    /// Filtered delete on a transaction connection.
    pub(crate) async fn delete_where_in<T: Persistent>(
        &self,
        tx: &mut sqlx::Transaction<'static, sqlx::Sqlite>,
        filters: &[Filter],
    ) -> Result<u64, StoreError> {
        self.ensure_table_tx(tx, T::TABLE, T::index_columns(), T::unique_constraints())
            .await?;
        let rendered = self.dialect.render_delete_where(T::TABLE, filters);
        let mut query = sqlx::query(sqlx::AssertSqlSafe(rendered.sql.as_str()));
        for p in rendered.binds {
            query = query.bind(p);
        }
        Ok(query.execute(&mut **tx).await?.rows_affected())
    }

    /// Undecoded blobs on a transaction connection (lenient reads).
    pub(crate) async fn query_blobs_in<T: Persistent>(
        &self,
        tx: &mut sqlx::Transaction<'static, sqlx::Sqlite>,
        request: QueryRequest,
    ) -> Result<Vec<Vec<u8>>, StoreError> {
        self.ensure_table_tx(tx, T::TABLE, T::index_columns(), T::unique_constraints())
            .await?;
        let rendered = self.dialect.render_select(&request, "SELECT v");
        let mut query = sqlx::query(sqlx::AssertSqlSafe(rendered.sql.as_str()));
        for p in rendered.binds {
            query = query.bind(p);
        }
        let rows = query.fetch_all(&mut **tx).await?;
        Ok(rows.into_iter().map(|r| r.get::<Vec<u8>, _>(0)).collect())
    }

    /// Composes a `SELECT v FROM <table> WHERE <column> IN (?, ?, ...)` SQL
    /// using the dialect's identifier quoting and placeholder formatting.
    fn select_in_sql(&self, table: &str, column: &str, count: usize) -> String {
        let placeholders: Vec<String> = (1..=count).map(|i| self.dialect.placeholder(i)).collect();
        format!(
            "SELECT v FROM {} WHERE {} IN ({})",
            self.dialect.quote_ident(table),
            self.dialect.quote_ident(column),
            placeholders.join(", "),
        )
    }

    /// Renders a simple CRUD statement using dialect primitives.
    fn quote(&self, name: &str) -> String {
        self.dialect.quote_ident(name)
    }

    fn ph(&self, n: usize) -> String {
        self.dialect.placeholder(n)
    }
}

impl Backend for SqliteBackend {
    async fn ensure_table<T: Persistent>(&self) -> Result<(), StoreError> {
        self.ensure_table_raw(T::TABLE, T::index_columns(), T::unique_constraints())
            .await
    }

    async fn save<T: Persistent>(&self, value: &T, codec: Codec) -> Result<(), StoreError> {
        let table = T::TABLE;
        self.ensure_table_raw(table, T::index_columns(), T::unique_constraints())
            .await?;
        let id_bytes = value.id().to_bytes();
        let v = codec.encode(value)?;
        let index_cols = T::index_columns();
        let index_vals = value.index_values();

        let schema = TableSchema {
            table,
            index_columns: index_cols,
        };
        let sql = self.cached_upsert(&schema);

        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        query = query.bind(&id_bytes[..]).bind(&v);
        for val in index_vals {
            query = query.bind(val);
        }
        query.execute(&self.pool).await?;
        Ok(())
    }

    async fn load<T: Persistent>(
        &self,
        id_bytes: &[u8],
        codec: Codec,
    ) -> Result<Option<T>, StoreError> {
        let table = T::TABLE;
        let sql = format!(
            "SELECT v FROM {} WHERE {} = {}",
            self.quote(table),
            self.quote("id"),
            self.ph(1),
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(id_bytes)
            .fetch_optional(&self.pool)
            .await?;

        match row {
            Some(r) => {
                let bytes: Vec<u8> = r.get(0);
                Ok(Some(crate::codec::decode_row::<T>(codec, &bytes)?))
            }
            None => Ok(None),
        }
    }

    async fn load_many<T: Persistent>(
        &self,
        ids: &[[u8; 16]],
        codec: Codec,
    ) -> Result<Vec<T>, StoreError> {
        let table = T::TABLE;
        if ids.is_empty() {
            return Ok(vec![]);
        }

        // Chunk to stay under SQLite's SQLITE_MAX_VARIABLE_NUMBER (default 999).
        const CHUNK: usize = 500;
        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(CHUNK) {
            let sql = self.select_in_sql(table, "id", chunk.len());
            let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
            for id in chunk {
                query = query.bind(&id[..]);
            }

            let rows = query.fetch_all(&self.pool).await?;
            for r in rows {
                let bytes: Vec<u8> = r.get(0);
                out.push(crate::codec::decode_row::<T>(codec, &bytes)?);
            }
        }
        Ok(out)
    }

    async fn exists<T: Persistent>(&self, id_bytes: &[u8]) -> Result<bool, StoreError> {
        let table = T::TABLE;
        let sql = format!(
            "SELECT EXISTS(SELECT 1 FROM {} WHERE {} = {})",
            self.quote(table),
            self.quote("id"),
            self.ph(1),
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(id_bytes)
            .fetch_one(&self.pool)
            .await?;

        let exists: i64 = row.get(0);
        Ok(exists != 0)
    }

    async fn delete<T: Persistent>(&self, id_bytes: &[u8]) -> Result<(), StoreError> {
        let table = T::TABLE;
        let sql = format!(
            "DELETE FROM {} WHERE {} = {}",
            self.quote(table),
            self.quote("id"),
            self.ph(1),
        );
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(id_bytes)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_all<T: Persistent>(&self) -> Result<u64, StoreError> {
        let table = T::TABLE;
        let sql = format!("DELETE FROM {}", self.quote(table));
        let result = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    async fn scan<T: Persistent>(&self, codec: Codec) -> Result<Vec<T>, StoreError> {
        let table = T::TABLE;
        let sql = format!(
            "SELECT v FROM {} ORDER BY {} ASC",
            self.quote(table),
            self.quote("saved_at"),
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter()
            .map(|r| {
                let bytes: Vec<u8> = r.get(0);
                crate::codec::decode_row::<T>(codec, &bytes)
            })
            .collect::<Result<Vec<T>, _>>()
    }

    async fn count<T: Persistent>(&self) -> Result<i64, StoreError> {
        let table = T::TABLE;
        let sql = format!("SELECT COUNT(*) FROM {}", self.quote(table));
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .fetch_one(&self.pool)
            .await?;

        let n: i64 = row.get(0);
        Ok(n)
    }

    async fn query<T: Persistent>(
        &self,
        request: QueryRequest,
        codec: Codec,
    ) -> Result<Vec<T>, StoreError> {
        let rendered = self.dialect.render_select(&request, "SELECT v");
        let mut query = sqlx::query(sqlx::AssertSqlSafe(rendered.sql.as_str()));
        for p in rendered.binds {
            query = query.bind(p);
        }

        let rows = query.fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|r| {
                let bytes: Vec<u8> = r.get(0);
                crate::codec::decode_row::<T>(codec, &bytes)
            })
            .collect::<Result<Vec<T>, _>>()
    }

    async fn query_blobs(&self, request: QueryRequest) -> Result<Vec<Vec<u8>>, StoreError> {
        let rendered = self.dialect.render_select(&request, "SELECT v");
        let mut query = sqlx::query(sqlx::AssertSqlSafe(rendered.sql.as_str()));
        for p in rendered.binds {
            query = query.bind(p);
        }
        let rows = query.fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(|r| r.get::<Vec<u8>, _>(0)).collect())
    }

    async fn delete_where<T: Persistent>(&self, filters: &[Filter]) -> Result<u64, StoreError> {
        self.ensure_table::<T>().await?;
        let rendered = self.dialect.render_delete_where(T::TABLE, filters);
        let mut query = sqlx::query(sqlx::AssertSqlSafe(rendered.sql.as_str()));
        for p in rendered.binds {
            query = query.bind(p);
        }
        Ok(query.execute(&self.pool).await?.rows_affected())
    }

    async fn insert<T: Persistent>(&self, value: &T, codec: Codec) -> Result<(), StoreError> {
        self.ensure_table::<T>().await?;
        let schema = TableSchema {
            table: T::TABLE,
            index_columns: T::index_columns(),
        };
        let sql = self.dialect.render_insert(&schema);
        let id_bytes = value.id().to_bytes();
        let v = codec.encode(value)?;
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        query = query.bind(&id_bytes[..]).bind(&v);
        for val in value.index_values() {
            query = query.bind(val);
        }
        match query.execute(&self.pool).await {
            Ok(_) => Ok(()),
            Err(e) if crate::backend::is_unique_violation(&e) => Err(StoreError::Conflict(
                format!("insert into `{}`: row exists", T::TABLE),
            )),
            Err(e) => Err(StoreError::Sqlite(e)),
        }
    }

    async fn query_count(&self, request: QueryRequest) -> Result<i64, StoreError> {
        let rendered = self.dialect.render_select(&request, "SELECT COUNT(*)");
        let mut query = sqlx::query(sqlx::AssertSqlSafe(rendered.sql.as_str()));
        for p in rendered.binds {
            query = query.bind(p);
        }

        let row = query.fetch_one(&self.pool).await?;
        let n: i64 = row.get(0);
        Ok(n)
    }

    async fn query_projected(
        &self,
        request: QueryRequest,
        columns: &[String],
    ) -> Result<Vec<crate::backend::ProjectedRow>, StoreError> {
        let clause = format!(
            "SELECT {}",
            columns
                .iter()
                .map(|c| self.quote(c))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let rendered = self.dialect.render_select(&request, &clause);
        let mut query = sqlx::query(sqlx::AssertSqlSafe(rendered.sql.as_str()));
        for p in rendered.binds {
            query = query.bind(p);
        }

        let rows = query.fetch_all(&self.pool).await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let mut values = Vec::with_capacity(columns.len());
            for (i, col) in columns.iter().enumerate() {
                if col == "id" {
                    // Row id as hex: lets metadata-only callers address rows
                    // (GC deletes) without fetching blobs.
                    let raw: Vec<u8> = r.try_get(i).map_err(StoreError::Sqlite)?;
                    let mut id = [0u8; 16];
                    let len = raw.len().min(16);
                    id[..len].copy_from_slice(&raw[..len]);
                    values.push(Some(uuid::Uuid::from_bytes(id).as_simple().to_string()));
                } else {
                    // Index columns are TEXT; a row predating the column reads NULL.
                    let v: Option<String> = r.try_get(i).map_err(StoreError::Sqlite)?;
                    values.push(v);
                }
            }
            out.push(crate::backend::ProjectedRow {
                columns: columns.to_vec(),
                values,
            });
        }
        Ok(out)
    }

    async fn count_grouped_by<T: Persistent>(
        &self,
        column: &str,
    ) -> Result<HashMap<String, i64>, StoreError> {
        let table = T::TABLE;
        let sql = format!(
            "SELECT {}, COUNT(*) FROM {} GROUP BY {}",
            self.quote(column),
            self.quote(table),
            self.quote(column),
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .fetch_all(&self.pool)
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

    async fn replace_where<T: Persistent>(
        &self,
        filters: &[(String, String)],
        items: &[([u8; 16], Vec<u8>, Vec<String>)],
        codec: Codec,
    ) -> Result<(), StoreError> {
        let table = T::TABLE;
        let dialect_filters: Vec<Filter> = filters
            .iter()
            .map(|(c, v)| Filter::Eq(c.clone(), v.clone()))
            .collect();

        if dialect_filters.is_empty() {
            let sql = format!("DELETE FROM {}", self.quote(table));
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .execute(&self.pool)
                .await?;
        } else {
            let (where_clause, binds) = self.dialect.render_where_clause(&dialect_filters);
            let sql = format!("DELETE FROM {} WHERE {where_clause}", self.quote(table));
            let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
            for b in binds {
                query = query.bind(b);
            }
            query.execute(&self.pool).await?;
        }

        let mut batch = BackendBatch::default();
        for (id_bytes, value_bytes, index_values) in items {
            batch.entries.push(crate::backend::BatchEntry {
                table,
                id_bytes: *id_bytes,
                value_bytes: value_bytes.clone(),
                index_columns: T::index_columns(),
                index_values: index_values.clone(),
                unique_groups: T::unique_constraints(),
            });
        }
        self.save_batch(&batch, codec).await
    }

    async fn save_batch(&self, batch: &BackendBatch, _codec: Codec) -> Result<(), StoreError> {
        let mut seen = HashSet::new();
        for entry in &batch.entries {
            if seen.insert(entry.table) {
                self.ensure_table_raw(entry.table, entry.index_columns, entry.unique_groups)
                    .await?;
            }
        }

        // Pre-render upsert SQL per table to avoid re-rendering inside loop.
        let mut sql_cache: HashMap<&'static str, String> = HashMap::new();
        for entry in &batch.entries {
            if !sql_cache.contains_key(entry.table) {
                // Try global cache first
                let cached = if let Ok(c) = self.upsert_cache.read() {
                    c.get(entry.table).cloned()
                } else {
                    None
                };
                let sql = if let Some(s) = cached {
                    s
                } else {
                    let schema = TableSchema {
                        table: entry.table,
                        index_columns: entry.index_columns,
                    };
                    let s = self.dialect.render_upsert(&schema);
                    if let Ok(mut c) = self.upsert_cache.write() {
                        c.insert(entry.table.to_string(), s.clone());
                    }
                    s
                };
                sql_cache.insert(entry.table, sql);
            }
        }

        let mut tx = self.pool.begin().await?;

        for entry in &batch.entries {
            let sql = sql_cache.get(entry.table).expect("sql cached");
            let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
            query = query.bind(&entry.id_bytes[..]).bind(&entry.value_bytes);
            for val in &entry.index_values {
                query = query.bind(val);
            }
            query.execute(&mut *tx).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    fn as_sqlite_pool(&self) -> Option<&sqlx::SqlitePool> {
        Some(&self.pool)
    }

    async fn insert_guarded<T: Persistent>(
        &self,
        value: &T,
        guard: &SequenceGuard,
        codec: Codec,
    ) -> Result<GuardOutcome, StoreError> {
        self.ensure_table::<T>().await?;
        let id_bytes = value.id().to_bytes();
        let v = codec.encode(value)?;
        let index_vals = value.index_values();
        let schema = TableSchema {
            table: T::TABLE,
            index_columns: T::index_columns(),
        };
        let partition: Vec<&str> = guard.partition.iter().map(|(c, _)| c.as_str()).collect();
        let sql = self
            .dialect
            .render_insert_guarded(&schema, &partition, &guard.sequence_column);
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        query = query.bind(&id_bytes[..]).bind(&v);
        for val in index_vals {
            query = query.bind(val);
        }
        for (_, val) in &guard.partition {
            query = query.bind(val);
        }
        query = query.bind(guard.sequence_value);
        match query.execute(&self.pool).await {
            Ok(done) => Ok(if done.rows_affected() == 1 {
                GuardOutcome::Landed
            } else {
                GuardOutcome::Rejected
            }),
            // Lost race against the matching UNIQUE index: the other
            // writer's row stands, which is exactly Rejected.
            Err(e) if crate::backend::is_unique_violation(&e) => Ok(GuardOutcome::Rejected),
            Err(e) => Err(StoreError::Sqlite(e)),
        }
    }

    async fn query_raw<T: Persistent>(
        &self,
        sql: &str,
        codec: Codec,
    ) -> Result<Vec<T>, StoreError> {
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter()
            .map(|r| {
                let bytes: Vec<u8> = r.get(0);
                crate::codec::decode_row::<T>(codec, &bytes)
            })
            .collect()
    }
}
