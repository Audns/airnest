//! `SQLite` backend implementation.
//!
//! All DDL/DML string generation is delegated to the internal `SqlDialect`
//! trait. This backend owns the connection pool, table-cache, and bind logic;
//! the dialect owns the SQL surface.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use tokio::sync::Mutex;

use crate::{
    backend::{
        Backend, BackendBatch, Filter, QueryRequest,
        dialect::{SqlDialect, TableSchema},
        sqlite_dialect::SqliteDialect,
    },
    codec::Codec,
    error::StoreError,
    persistent::Persistent,
};

#[derive(Clone)]
pub struct SqliteBackend {
    pool: SqlitePool,
    tables: Arc<Mutex<HashSet<&'static str>>>,
    dialect: Arc<dyn SqlDialect>,
}

impl SqliteBackend {
    pub async fn open(path: &str) -> Result<Self, StoreError> {
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
            tables: Arc::new(Mutex::new(HashSet::new())),
            dialect,
        })
    }

    async fn ensure_table_raw(
        &self,
        table: &'static str,
        index_cols: &[&'static str],
    ) -> Result<(), StoreError> {
        {
            let guard = self.tables.lock().await;
            if guard.contains(table) {
                return Ok(());
            }
        }

        let schema = TableSchema {
            table,
            index_columns: index_cols,
        };
        let create_sql = self.dialect.render_create_table(&schema);
        sqlx::query(sqlx::AssertSqlSafe(&*create_sql))
            .execute(&self.pool)
            .await?;

        // SQLite cannot add columns inside a CREATE TABLE for already-existing
        // tables, so an idempotent ALTER is issued after the CREATE. CREATE
        // TABLE IF NOT EXISTS is a no-op when the table already exists with
        // the expected schema; the ALTER catches the case where it exists but
        // is missing the column.
        for col in index_cols {
            let add_sql = self.dialect.render_add_column(table, col);
            // ALTER TABLE ADD COLUMN is not idempotent in SQLite, but ignoring
            // the "duplicate column" error preserves the existing semantics.
            let _ = sqlx::query(sqlx::AssertSqlSafe(&*add_sql))
                .execute(&self.pool)
                .await;
        }

        let saved_at_idx_sql = self.dialect.render_create_index(table, "saved_at");
        sqlx::query(sqlx::AssertSqlSafe(&*saved_at_idx_sql))
            .execute(&self.pool)
            .await?;

        for col in index_cols {
            let col_idx_sql = self.dialect.render_create_index(table, col);
            sqlx::query(sqlx::AssertSqlSafe(&*col_idx_sql))
                .execute(&self.pool)
                .await?;
        }

        let mut guard = self.tables.lock().await;
        guard.insert(table);
        Ok(())
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
        self.ensure_table_raw(T::TABLE, T::index_columns()).await
    }

    async fn save<T: Persistent>(&self, value: &T, codec: Codec) -> Result<(), StoreError> {
        let table = T::TABLE;
        self.ensure_table_raw(table, T::index_columns()).await?;
        let id_bytes = value.id().to_bytes();
        let v = codec.encode(value)?;
        let index_cols = T::index_columns();
        let index_vals = value.index_values();

        let schema = TableSchema {
            table,
            index_columns: index_cols,
        };
        let sql = self.dialect.render_upsert(&schema);

        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        query = query.bind(&id_bytes).bind(&v);
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
                Ok(Some(codec.decode(&bytes)?))
            }
            None => Ok(None),
        }
    }

    async fn load_many<T: Persistent>(
        &self,
        ids: &[Vec<u8>],
        codec: Codec,
    ) -> Result<Vec<T>, StoreError> {
        let table = T::TABLE;
        if ids.is_empty() {
            return Ok(vec![]);
        }

        let sql = self.select_in_sql(table, "id", ids.len());
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        for id in ids {
            query = query.bind(id);
        }

        let rows = query.fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|r| {
                let bytes: Vec<u8> = r.get(0);
                codec.decode(&bytes)
            })
            .collect::<Result<Vec<T>, _>>()
    }

    async fn exists<T: Persistent>(&self, id_bytes: &[u8]) -> Result<bool, StoreError> {
        let table = T::TABLE;
        let sql = format!(
            "SELECT COUNT(*) FROM {} WHERE {} = {}",
            self.quote(table),
            self.quote("id"),
            self.ph(1),
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(id_bytes)
            .fetch_one(&self.pool)
            .await?;

        let n: i64 = row.get(0);
        Ok(n > 0)
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
                codec.decode(&bytes)
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
                codec.decode(&bytes)
            })
            .collect::<Result<Vec<T>, _>>()
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
        items: &[(Vec<u8>, Vec<u8>, Vec<String>)],
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
                id_bytes: id_bytes.clone(),
                value_bytes: value_bytes.clone(),
                index_columns: T::index_columns(),
                index_values: index_values.clone(),
            });
        }
        self.save_batch(&batch, codec).await
    }

    async fn save_batch(&self, batch: &BackendBatch, _codec: Codec) -> Result<(), StoreError> {
        let mut seen = HashSet::new();
        for entry in &batch.entries {
            if seen.insert(entry.table) {
                self.ensure_table_raw(entry.table, entry.index_columns)
                    .await?;
            }
        }

        let mut tx = self.pool.begin().await?;

        for entry in &batch.entries {
            let schema = TableSchema {
                table: entry.table,
                index_columns: entry.index_columns,
            };
            let sql = self.dialect.render_upsert(&schema);
            let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
            query = query.bind(&entry.id_bytes).bind(&entry.value_bytes);
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
                codec.decode(&bytes)
            })
            .collect()
    }
}
