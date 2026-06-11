//! Redb backend implementation.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use redb::{Database, ReadableTable, TableDefinition};
use tokio::sync::Mutex;

use crate::{
    backend::{Backend, BackendBatch, BatchEntry, Filter, Order, QueryRequest},
    codec::Codec,
    error::StoreError,
    persistent::Persistent,
};

const KV_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("airnest_kv");

/// Wrapper stored in redb to keep metadata alongside the user blob.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Record {
    id: Vec<u8>,
    bytes: Vec<u8>,
    saved_at: u64,
    index_values: HashMap<String, String>,
}

#[derive(Clone)]
pub struct RedbBackend {
    db: Arc<Database>,
    tables: Arc<Mutex<HashSet<&'static str>>>,
}

impl RedbBackend {
    pub async fn open(path: &str) -> Result<Self, StoreError> {
        let path = path.to_string();
        let db = tokio::task::spawn_blocking(move || {
            Database::create(path).map_err(|e| StoreError::Redb(e.to_string()))
        })
        .await
        .map_err(StoreError::Join)??;

        let backend = Self {
            db: Arc::new(db),
            tables: Arc::new(Mutex::new(HashSet::new())),
        };

        // Ensure the main table exists.
        let db = backend.db.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db
                .begin_write()
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            let _ = txn
                .open_table(KV_TABLE)
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            txn.commit().map_err(|e| StoreError::Redb(e.to_string()))?;
            Ok::<_, StoreError>(())
        })
        .await
        .map_err(StoreError::Join)??;

        Ok(backend)
    }

    fn make_key(table: &str, id: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(table.len() + 1 + id.len());
        key.extend_from_slice(table.as_bytes());
        key.push(0);
        key.extend_from_slice(id);
        key
    }

    fn table_prefix(table: &str) -> Vec<u8> {
        let mut p = table.as_bytes().to_vec();
        p.push(0);
        p
    }

    fn table_range(table: &str) -> (Vec<u8>, Vec<u8>) {
        let start = Self::table_prefix(table);
        let mut end = start.clone();
        end.push(0xff);
        (start, end)
    }

    async fn scan_pairs(&self, table: &str) -> Result<Vec<(Vec<u8>, Record)>, StoreError> {
        let (start, end) = Self::table_range(table);
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db
                .begin_read()
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            let tbl = txn
                .open_table(KV_TABLE)
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            let range = tbl
                .range(start.as_slice()..=end.as_slice())
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            let mut out = Vec::new();
            for item in range {
                let (k, v) = item.map_err(|e| StoreError::Redb(e.to_string()))?;
                let key = k.value().to_vec();
                let rec: Record = bitcode::deserialize(v.value()).map_err(StoreError::Encode)?;
                out.push((key, rec));
            }
            Ok::<_, StoreError>(out)
        })
        .await
        .map_err(StoreError::Join)?
    }

    async fn scan_records(&self, table: &str) -> Result<Vec<Record>, StoreError> {
        let pairs = self.scan_pairs(table).await?;
        Ok(pairs.into_iter().map(|(_k, v)| v).collect())
    }
}

impl Backend for RedbBackend {
    async fn ensure_table<T: Persistent>(&self) -> Result<(), StoreError> {
        let mut guard = self.tables.lock().await;
        guard.insert(T::TABLE);
        Ok(())
    }

    async fn save<T: Persistent>(&self, value: &T, codec: Codec) -> Result<(), StoreError> {
        let table = T::TABLE;
        let id_bytes = value.id().to_bytes();
        let key = Self::make_key(table, &id_bytes);
        let value_bytes = codec.encode(value)?;
        let index_values: HashMap<String, String> = T::index_columns()
            .iter()
            .zip(value.index_values().iter())
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        let record = Record {
            id: id_bytes,
            bytes: value_bytes,
            saved_at: 0,
            index_values,
        };
        let record_bytes = bitcode::serialize(&record).map_err(StoreError::Encode)?;

        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db
                .begin_write()
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            {
                let mut tbl = txn
                    .open_table(KV_TABLE)
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                tbl.insert(key.as_slice(), record_bytes.as_slice())
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
            }
            txn.commit().map_err(|e| StoreError::Redb(e.to_string()))?;
            Ok::<_, StoreError>(())
        })
        .await
        .map_err(StoreError::Join)?
    }

    async fn load<T: Persistent>(
        &self,
        id_bytes: &[u8],
        codec: Codec,
    ) -> Result<Option<T>, StoreError> {
        let table = T::TABLE;
        let key = Self::make_key(table, id_bytes);
        let db = self.db.clone();
        let record_bytes = tokio::task::spawn_blocking(move || {
            let txn = db
                .begin_read()
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            let tbl = txn
                .open_table(KV_TABLE)
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            let result = tbl
                .get(key.as_slice())
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            Ok::<_, StoreError>(result.map(|v| v.value().to_vec()))
        })
        .await
        .map_err(StoreError::Join)??;

        match record_bytes {
            Some(b) => {
                let rec: Record = bitcode::deserialize(&b).map_err(StoreError::Encode)?;
                Ok(Some(codec.decode(&rec.bytes)?))
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
        let keys: Vec<Vec<u8>> = ids.iter().map(|id| Self::make_key(table, id)).collect();
        let db = self.db.clone();
        let records = tokio::task::spawn_blocking(move || {
            let txn = db
                .begin_read()
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            let tbl = txn
                .open_table(KV_TABLE)
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            let mut out = Vec::new();
            for key in keys {
                if let Some(result) = tbl
                    .get(key.as_slice())
                    .map_err(|e| StoreError::Redb(e.to_string()))?
                {
                    out.push(result.value().to_vec());
                }
            }
            Ok::<_, StoreError>(out)
        })
        .await
        .map_err(StoreError::Join)??;

        records
            .into_iter()
            .map(|b| {
                let rec: Record = bitcode::deserialize(&b).map_err(StoreError::Encode)?;
                codec.decode(&rec.bytes)
            })
            .collect::<Result<Vec<T>, _>>()
    }

    async fn exists<T: Persistent>(&self, id_bytes: &[u8]) -> Result<bool, StoreError> {
        let table = T::TABLE;
        let key = Self::make_key(table, id_bytes);
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db
                .begin_read()
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            let tbl = txn
                .open_table(KV_TABLE)
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            let result = tbl
                .get(key.as_slice())
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            Ok::<_, StoreError>(result.is_some())
        })
        .await
        .map_err(StoreError::Join)?
    }

    async fn delete<T: Persistent>(&self, id_bytes: &[u8]) -> Result<(), StoreError> {
        let table = T::TABLE;
        let key = Self::make_key(table, id_bytes);
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db
                .begin_write()
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            {
                let mut tbl = txn
                    .open_table(KV_TABLE)
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                tbl.remove(key.as_slice())
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
            }
            txn.commit().map_err(|e| StoreError::Redb(e.to_string()))?;
            Ok::<_, StoreError>(())
        })
        .await
        .map_err(StoreError::Join)?
    }

    async fn delete_all<T: Persistent>(&self) -> Result<u64, StoreError> {
        let table = T::TABLE;
        let (start, end) = Self::table_range(table);
        let db = self.db.clone();
        let count = tokio::task::spawn_blocking(move || {
            let txn = db
                .begin_write()
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            let mut removed = 0u64;
            {
                let mut tbl = txn
                    .open_table(KV_TABLE)
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                let range = tbl
                    .range(start.as_slice()..=end.as_slice())
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                let keys: Vec<Vec<u8>> = range
                    .map(|item| item.map(|(k, _v)| k.value().to_vec()))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                for key in keys {
                    tbl.remove(key.as_slice())
                        .map_err(|e| StoreError::Redb(e.to_string()))?;
                    removed += 1;
                }
            }
            txn.commit().map_err(|e| StoreError::Redb(e.to_string()))?;
            Ok::<_, StoreError>(removed)
        })
        .await
        .map_err(StoreError::Join)??;
        Ok(count)
    }

    async fn scan<T: Persistent>(&self, codec: Codec) -> Result<Vec<T>, StoreError> {
        let table = T::TABLE;
        let records = self.scan_records(table).await?;
        records
            .into_iter()
            .map(|rec| codec.decode(&rec.bytes))
            .collect::<Result<Vec<T>, _>>()
    }

    async fn count<T: Persistent>(&self) -> Result<i64, StoreError> {
        let table = T::TABLE;
        let (start, end) = Self::table_range(table);
        let db = self.db.clone();
        let count = tokio::task::spawn_blocking(move || {
            let txn = db
                .begin_read()
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            let tbl = txn
                .open_table(KV_TABLE)
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            let range = tbl
                .range(start.as_slice()..=end.as_slice())
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            let mut n = 0i64;
            for _ in range {
                n += 1;
            }
            Ok::<_, StoreError>(n)
        })
        .await
        .map_err(StoreError::Join)??;
        Ok(count)
    }

    async fn query<T: Persistent>(
        &self,
        request: QueryRequest,
        codec: Codec,
    ) -> Result<Vec<T>, StoreError> {
        let mut recs = self.scan_records(request.table).await?;

        for filter in &request.filters {
            match filter {
                Filter::Eq(col, val) => {
                    recs.retain(|r| r.index_values.get(col) == Some(val));
                }
                Filter::In(col, vals) => {
                    recs.retain(|r| {
                        r.index_values
                            .get(col)
                            .map(|v| vals.contains(v))
                            .unwrap_or(false)
                    });
                }
            }
        }

        for (col, order) in request.order_by.iter().rev() {
            recs.sort_by(|a, b| {
                let av = a.index_values.get(col);
                let bv = b.index_values.get(col);
                match order {
                    Order::Asc => av.cmp(&bv),
                    Order::Desc => bv.cmp(&av),
                }
            });
        }

        if let Some(n) = request.limit {
            recs.truncate(n);
        }

        recs.into_iter()
            .map(|rec| codec.decode(&rec.bytes))
            .collect::<Result<Vec<T>, _>>()
    }

    async fn query_count(&self, request: QueryRequest) -> Result<i64, StoreError> {
        let recs = self.scan_records(request.table).await?;
        let mut count = 0i64;
        for rec in &recs {
            let mut keep = true;
            for filter in &request.filters {
                match filter {
                    Filter::Eq(col, val) => {
                        if rec.index_values.get(col) != Some(val) {
                            keep = false;
                        }
                    }
                    Filter::In(col, vals) => {
                        if !vals.contains(rec.index_values.get(col).unwrap_or(&String::new())) {
                            keep = false;
                        }
                    }
                }
            }
            if keep {
                count += 1;
            }
        }
        Ok(count)
    }

    async fn count_grouped_by<T: Persistent>(
        &self,
        column: &str,
    ) -> Result<HashMap<String, i64>, StoreError> {
        let recs = self.scan_records(T::TABLE).await?;
        let mut map = HashMap::new();
        for rec in recs {
            if let Some(v) = rec.index_values.get(column) {
                *map.entry(v.clone()).or_insert(0) += 1;
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
        let pairs = self.scan_pairs(table).await?;
        let mut to_delete = Vec::new();
        for (key, rec) in pairs {
            let mut matches = true;
            for (col, val) in filters {
                if rec.index_values.get(col) != Some(val) {
                    matches = false;
                    break;
                }
            }
            if matches {
                to_delete.push(key);
            }
        }

        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db
                .begin_write()
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            {
                let mut tbl = txn
                    .open_table(KV_TABLE)
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                for key in to_delete {
                    tbl.remove(key.as_slice())
                        .map_err(|e| StoreError::Redb(e.to_string()))?;
                }
            }
            txn.commit().map_err(|e| StoreError::Redb(e.to_string()))?;
            Ok::<_, StoreError>(())
        })
        .await
        .map_err(StoreError::Join)??;

        let mut batch = BackendBatch::default();
        for (id_bytes, value_bytes, index_values) in items {
            batch.entries.push(BatchEntry {
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
        let db = self.db.clone();
        let entries: Vec<_> = batch
            .entries
            .iter()
            .map(|e| {
                let index_map: HashMap<String, String> = e
                    .index_columns
                    .iter()
                    .zip(e.index_values.iter())
                    .map(|(k, v)| (k.to_string(), v.clone()))
                    .collect();
                let rec = Record {
                    id: e.id_bytes.clone(),
                    bytes: e.value_bytes.clone(),
                    saved_at: 0,
                    index_values: index_map,
                };
                let rec_bytes = bitcode::serialize(&rec).unwrap();
                let key = Self::make_key(e.table, &e.id_bytes);
                (key, rec_bytes)
            })
            .collect();

        tokio::task::spawn_blocking(move || {
            let txn = db
                .begin_write()
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            {
                let mut tbl = txn
                    .open_table(KV_TABLE)
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                for (key, rec_bytes) in entries {
                    tbl.insert(key.as_slice(), rec_bytes.as_slice())
                        .map_err(|e| StoreError::Redb(e.to_string()))?;
                }
            }
            txn.commit().map_err(|e| StoreError::Redb(e.to_string()))?;
            Ok::<_, StoreError>(())
        })
        .await
        .map_err(StoreError::Join)?
    }

    fn as_sqlite_pool(&self) -> Option<&sqlx::SqlitePool> {
        None
    }

    async fn query_raw<T: Persistent>(
        &self,
        _sql: &str,
        _codec: Codec,
    ) -> Result<Vec<T>, StoreError> {
        Err(StoreError::Codec(
            "query_raw requires SQLite backend".into(),
        ))
    }
}
