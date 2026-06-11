//! SQLite backend implementation.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// serde used via Codec
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use tokio::sync::Mutex;

use crate::{
    backend::{Backend, BackendBatch, Filter, QueryRequest},
    codec::Codec,
    error::StoreError,
    persistent::Persistent,
};

#[derive(Clone)]
pub struct SqliteBackend {
    pool: SqlitePool,
    tables: Arc<Mutex<HashSet<&'static str>>>,
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

        let extra_cols: String = index_cols
            .iter()
            .map(|c| format!(",\n                 \"{c}\" TEXT"))
            .collect();

        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS \"{table}\" (\n                 id       BLOB    NOT NULL PRIMARY KEY,\n                 v        BLOB    NOT NULL,\n                 saved_at INTEGER NOT NULL DEFAULT (unixepoch()){extra_cols}\n             ) STRICT"
        );

        sqlx::query(sqlx::AssertSqlSafe(&*create_sql))
            .execute(&self.pool)
            .await?;

        for col in index_cols {
            let _ = sqlx::query(sqlx::AssertSqlSafe(&*format!(
                "ALTER TABLE \"{table}\" ADD COLUMN \"{col}\" TEXT"
            )))
            .execute(&self.pool)
            .await;
        }

        sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "CREATE INDEX IF NOT EXISTS \"{table}_saved_at\"\n             ON \"{table}\" (saved_at)"
        )))
        .execute(&self.pool)
        .await?;

        for col in index_cols {
            sqlx::query(sqlx::AssertSqlSafe(&*format!(
                "CREATE INDEX IF NOT EXISTS \"{table}_{col}_idx\"\n                 ON \"{table}\" (\"{col}\")"
            )))
            .execute(&self.pool)
            .await?;
        }

        let mut guard = self.tables.lock().await;
        guard.insert(table);
        Ok(())
    }

    fn build_upsert_sql(table: &str, index_cols: &[&str]) -> String {
        if index_cols.is_empty() {
            format!(
                "INSERT INTO \"{table}\" (id, v, saved_at)\n                 VALUES (?1, ?2, unixepoch())\n                 ON CONFLICT(id) DO UPDATE SET\n                     v        = excluded.v,\n                     saved_at = excluded.saved_at"
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
                "INSERT INTO \"{table}\" (id, v, saved_at{col_list})\n                 VALUES (?1, ?2, unixepoch(){placeholders})\n                 ON CONFLICT(id) DO UPDATE SET\n                     v        = excluded.v,\n                     saved_at = excluded.saved_at{updates}"
            )
        }
    }

    fn build_sql(request: &QueryRequest, select: &str) -> (String, Vec<String>) {
        let mut sql = format!(r#"{select} FROM "{}""#, request.table);
        let mut params: Vec<String> = Vec::new();
        let mut param_idx = 1;

        if !request.filters.is_empty() {
            let conditions: Vec<String> = request
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
            for f in &request.filters {
                match f {
                    Filter::Eq(_, v) => params.push(v.clone()),
                    Filter::In(_, vals) => params.extend(vals.clone()),
                }
            }
        }

        for (col, order) in &request.order_by {
            let dir = match order {
                crate::backend::Order::Asc => "ASC",
                crate::backend::Order::Desc => "DESC",
            };
            sql.push_str(&format!(r#" ORDER BY "{col}" {dir}"#));
        }

        if let Some(n) = request.limit {
            sql.push_str(&format!(" LIMIT {n}"));
        }

        (sql, params)
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

        if index_cols.is_empty() {
            let sql = format!(
                "INSERT INTO \"{table}\" (id, v, saved_at)\n                 VALUES (?1, ?2, unixepoch())\n                 ON CONFLICT(id) DO UPDATE SET\n                     v        = excluded.v,\n                     saved_at = excluded.saved_at"
            );
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .bind(&id_bytes)
                .bind(&v)
                .execute(&self.pool)
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
                "INSERT INTO \"{table}\" (id, v, saved_at{col_list})\n                 VALUES (?1, ?2, unixepoch(){placeholders})\n                 ON CONFLICT(id) DO UPDATE SET\n                     v        = excluded.v,\n                     saved_at = excluded.saved_at{updates}"
            );
            let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
            query = query.bind(&id_bytes).bind(&v);
            for val in index_vals {
                query = query.bind(val);
            }
            query.execute(&self.pool).await?;
        }
        Ok(())
    }

    async fn load<T: Persistent>(
        &self,
        id_bytes: &[u8],
        codec: Codec,
    ) -> Result<Option<T>, StoreError> {
        let table = T::TABLE;
        let row = sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "SELECT v FROM \"{table}\" WHERE id = ?1"
        )))
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

        let placeholders: Vec<String> = (1..=ids.len()).map(|i| format!("?{i}")).collect();
        let sql = format!(
            r#"SELECT v FROM "{table}" WHERE id IN ({})"#,
            placeholders.join(", ")
        );
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
        let row = sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "SELECT COUNT(*) FROM \"{table}\" WHERE id = ?1"
        )))
        .bind(id_bytes)
        .fetch_one(&self.pool)
        .await?;

        let n: i64 = row.get(0);
        Ok(n > 0)
    }

    async fn delete<T: Persistent>(&self, id_bytes: &[u8]) -> Result<(), StoreError> {
        let table = T::TABLE;
        sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "DELETE FROM \"{table}\" WHERE id = ?1"
        )))
        .bind(id_bytes)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete_all<T: Persistent>(&self) -> Result<u64, StoreError> {
        let table = T::TABLE;
        let result = sqlx::query(sqlx::AssertSqlSafe(&*format!("DELETE FROM \"{table}\"")))
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    async fn scan<T: Persistent>(&self, codec: Codec) -> Result<Vec<T>, StoreError> {
        let table = T::TABLE;
        let rows = sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "SELECT v FROM \"{table}\" ORDER BY saved_at ASC"
        )))
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
        let row = sqlx::query(sqlx::AssertSqlSafe(&*format!(
            "SELECT COUNT(*) FROM \"{table}\""
        )))
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
        let (sql, params) = Self::build_sql(&request, r#"SELECT v"#);
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        for p in params {
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
        let (sql, params) = Self::build_sql(&request, "SELECT COUNT(*)");
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        for p in params {
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
        let sql = format!(r#"SELECT "{column}", COUNT(*) FROM "{table}" GROUP BY "{column}""#);
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
            query.execute(&self.pool).await?;
        } else {
            sqlx::query(sqlx::AssertSqlSafe(
                format!(r#"DELETE FROM "{table}""#).as_str(),
            ))
            .execute(&self.pool)
            .await?;
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
            let sql = Self::build_upsert_sql(entry.table, entry.index_columns);
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
