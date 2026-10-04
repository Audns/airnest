//! Redb backend implementation.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use redb::{Database, ReadableDatabase, ReadableTable, Table, TableDefinition};

use crate::{
    backend::{
        Backend, BackendBatch, BatchEntry, Filter, GuardOutcome, QueryRequest, SequenceGuard,
    },
    codec::Codec,
    error::StoreError,
    persistent::Persistent,
};

const KV_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("airnest_kv");

/// Secondary index for uniqueness groups: key → owning record id.
///
/// Key layout is `table_bytes + 0x00 + bitcode((group_columns, values))`,
/// so per-table cleanup is a byte-prefix scan and arbitrary strings
/// round-trip exactly. All reads and writes happen inside the caller's
/// write transaction, keeping check-and-insert atomic.
const UQ_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("airnest_uq");

/// Encode one uniqueness-group key for `record`'s index map.
fn unique_key(
    table: &str,
    cols: &[&str],
    index_map: &HashMap<String, String>,
) -> Result<Vec<u8>, StoreError> {
    let vals: Vec<String> = cols
        .iter()
        .map(|c| index_map.get(*c).cloned().unwrap_or_default())
        .collect();
    let mut key = Vec::with_capacity(table.len() + 1 + 64);
    key.extend_from_slice(table.as_bytes());
    key.push(0);
    let tail = bitcode::serialize(&(cols, vals)).map_err(StoreError::Encode)?;
    key.extend_from_slice(&tail);
    Ok(key)
}

fn unique_table_prefix(table: &str) -> (Vec<u8>, Vec<u8>) {
    let mut start = table.as_bytes().to_vec();
    start.push(0);
    let mut end = start.clone();
    end.push(0xff);
    (start, end)
}

/// Check-and-insert every uniqueness group for one record, and drop stale
/// keys the record no longer claims (upserts that change indexed values).
/// Must run inside the caller's write transaction: the check and the
/// insert are then atomic, and a lost race surfaces as [`StoreError::Conflict`].
fn sync_unique_keys(
    uq: &mut Table<'_, &[u8], &[u8]>,
    table: &'static str,
    groups: &[&[&str]],
    old: Option<&HashMap<String, String>>,
    new: &HashMap<String, String>,
    id: &[u8; 16],
) -> Result<(), StoreError> {
    let mut fresh = Vec::with_capacity(groups.len());
    for cols in groups {
        let key = unique_key(table, cols, new)?;
        match uq
            .get(key.as_slice())
            .map_err(|e| StoreError::Redb(e.to_string()))?
        {
            Some(owner) if owner.value() != id.as_slice() => {
                return Err(StoreError::Conflict(format!(
                    "duplicate value for unique({}) on `{table}`",
                    cols.join(", "),
                )));
            }
            _ => {}
        }
        fresh.push(key);
    }
    if let Some(old_map) = old {
        for cols in groups {
            let stale = unique_key(table, cols, old_map)?;
            if !fresh.iter().any(|k| k == &stale) {
                uq.remove(stale.as_slice())
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
            }
        }
    }
    for key in fresh {
        uq.insert(key.as_slice(), id.as_slice())
            .map_err(|e| StoreError::Redb(e.to_string()))?;
    }
    Ok(())
}

/// Drop every uniqueness key a record claims (delete path).
fn remove_unique_keys(
    uq: &mut Table<'_, &[u8], &[u8]>,
    table: &'static str,
    groups: &[&[&str]],
    index_map: &HashMap<String, String>,
) -> Result<(), StoreError> {
    for cols in groups {
        let key = unique_key(table, cols, index_map)?;
        uq.remove(key.as_slice())
            .map_err(|e| StoreError::Redb(e.to_string()))?;
    }
    Ok(())
}

/// Wrapper stored in redb to keep metadata alongside the user blob.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Record {
    id: [u8; 16],
    bytes: Vec<u8>,
    saved_at: u64,
    index_values: HashMap<String, String>,
}

/// Whether `rec` satisfies every filter (the in-memory WHERE).
fn record_matches(rec: &Record, filters: &[Filter]) -> bool {
    filters.iter().all(|filter| match filter {
        Filter::Eq(col, val) => rec.index_values.get(col) == Some(val),
        Filter::In(col, vals) => rec.index_values.get(col).is_some_and(|v| vals.contains(v)),
    })
}

#[derive(Clone)]
pub struct RedbBackend {
    db: Arc<Database>,
    tables: Arc<RwLock<HashSet<&'static str>>>,
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
            tables: Arc::new(RwLock::new(HashSet::new())),
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
    #[allow(clippy::unused_async_trait_impl)]
    async fn ensure_table<T: Persistent>(&self) -> Result<(), StoreError> {
        let mut guard = self.tables.write().map_err(|_| StoreError::Poisoned)?;
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
            index_values: index_values.clone(),
        };
        let record_bytes = bitcode::serialize(&record).map_err(StoreError::Encode)?;
        let groups = T::unique_constraints();

        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db
                .begin_write()
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            {
                let mut tbl = txn
                    .open_table(KV_TABLE)
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                let mut uq = txn
                    .open_table(UQ_TABLE)
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                let old: Option<HashMap<String, String>> = tbl
                    .get(key.as_slice())
                    .map_err(|e| StoreError::Redb(e.to_string()))?
                    .map(|v| {
                        bitcode::deserialize::<Record>(v.value())
                            .map(|r| r.index_values)
                            .unwrap_or_default()
                    });
                sync_unique_keys(
                    &mut uq,
                    table,
                    groups,
                    old.as_ref(),
                    &index_values,
                    &id_bytes,
                )?;
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
                Ok(Some(crate::codec::decode_row::<T>(codec, &rec.bytes)?))
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
                crate::codec::decode_row::<T>(codec, &rec.bytes)
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
        let groups = T::unique_constraints();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db
                .begin_write()
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            {
                let mut tbl = txn
                    .open_table(KV_TABLE)
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                let mut uq = txn
                    .open_table(UQ_TABLE)
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                let previous: Option<Vec<u8>> = tbl
                    .get(key.as_slice())
                    .map_err(|e| StoreError::Redb(e.to_string()))?
                    .map(|found| found.value().to_vec());
                if let Some(bytes) = previous {
                    if let Ok(rec) = bitcode::deserialize::<Record>(&bytes) {
                        remove_unique_keys(&mut uq, table, groups, &rec.index_values)?;
                    }
                    tbl.remove(key.as_slice())
                        .map_err(|e| StoreError::Redb(e.to_string()))?;
                }
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
        let (uq_start, uq_end) = unique_table_prefix(table);
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
            {
                let mut uq = txn
                    .open_table(UQ_TABLE)
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                let range = uq
                    .range(uq_start.as_slice()..=uq_end.as_slice())
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                let keys: Vec<Vec<u8>> = range
                    .map(|item| item.map(|(k, _v)| k.value().to_vec()))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                for key in keys {
                    uq.remove(key.as_slice())
                        .map_err(|e| StoreError::Redb(e.to_string()))?;
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
            .map(|rec| crate::codec::decode_row::<T>(codec, &rec.bytes))
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
                    // Use HashSet for O(1) lookups when vals is large.
                    if vals.len() > 8 {
                        let set: HashSet<&String> = vals.iter().collect();
                        recs.retain(|r| r.index_values.get(col).is_some_and(|v| set.contains(v)));
                    } else {
                        recs.retain(|r| r.index_values.get(col).is_some_and(|v| vals.contains(v)));
                    }
                }
            }
        }

        for (col, order) in request.order_by.iter().rev() {
            recs.sort_by(|a, b| order.compare(a.index_values.get(col), b.index_values.get(col)));
        }

        if let Some(n) = request.limit {
            recs.truncate(n);
        }

        recs.into_iter()
            .map(|rec| crate::codec::decode_row::<T>(codec, &rec.bytes))
            .collect::<Result<Vec<T>, _>>()
    }

    async fn query_blobs(&self, request: QueryRequest) -> Result<Vec<Vec<u8>>, StoreError> {
        let mut recs = self.scan_records(request.table).await?;
        recs.retain(|rec| record_matches(rec, &request.filters));
        for (col, order) in request.order_by.iter().rev() {
            recs.sort_by(|a, b| order.compare(a.index_values.get(col), b.index_values.get(col)));
        }
        if let Some(n) = request.limit {
            recs.truncate(n);
        }
        Ok(recs.into_iter().map(|rec| rec.bytes).collect())
    }

    async fn delete_where<T: Persistent>(&self, filters: &[Filter]) -> Result<u64, StoreError> {
        let table = T::TABLE;
        let (start, end) = Self::table_range(table);
        let groups = T::unique_constraints();
        let filters = filters.to_vec();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db
                .begin_write()
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            let mut removed = 0u64;
            {
                let mut tbl = txn
                    .open_table(KV_TABLE)
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                let mut uq = txn
                    .open_table(UQ_TABLE)
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                let mut doomed = Vec::new();
                for item in tbl
                    .range(start.as_slice()..=end.as_slice())
                    .map_err(|e| StoreError::Redb(e.to_string()))?
                {
                    let (k, v) = item.map_err(|e| StoreError::Redb(e.to_string()))?;
                    let rec: Record =
                        bitcode::deserialize(v.value()).map_err(StoreError::Encode)?;
                    if record_matches(&rec, &filters) {
                        doomed.push((k.value().to_vec(), rec.index_values));
                    }
                }
                for (key, index_values) in doomed {
                    remove_unique_keys(&mut uq, table, groups, &index_values)?;
                    tbl.remove(key.as_slice())
                        .map_err(|e| StoreError::Redb(e.to_string()))?;
                    removed += 1;
                }
            }
            txn.commit().map_err(|e| StoreError::Redb(e.to_string()))?;
            Ok::<_, StoreError>(removed)
        })
        .await
        .map_err(StoreError::Join)?
    }

    async fn insert<T: Persistent>(&self, value: &T, codec: Codec) -> Result<(), StoreError> {
        let table = T::TABLE;
        let id_bytes = value.id().to_bytes();
        let key = Self::make_key(table, &id_bytes);
        let index_values: HashMap<String, String> = T::index_columns()
            .iter()
            .zip(value.index_values().iter())
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        let record = Record {
            id: id_bytes,
            bytes: codec.encode(value)?,
            saved_at: 0,
            index_values: index_values.clone(),
        };
        let record_bytes = bitcode::serialize(&record).map_err(StoreError::Encode)?;
        let groups = T::unique_constraints();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db
                .begin_write()
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            {
                let mut tbl = txn
                    .open_table(KV_TABLE)
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                if tbl
                    .get(key.as_slice())
                    .map_err(|e| StoreError::Redb(e.to_string()))?
                    .is_some()
                {
                    return Err(StoreError::Conflict(format!(
                        "insert into `{table}`: row exists"
                    )));
                }
                let mut uq = txn
                    .open_table(UQ_TABLE)
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                sync_unique_keys(&mut uq, table, groups, None, &index_values, &id_bytes)?;
                tbl.insert(key.as_slice(), record_bytes.as_slice())
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
            }
            txn.commit().map_err(|e| StoreError::Redb(e.to_string()))?;
            Ok::<_, StoreError>(())
        })
        .await
        .map_err(StoreError::Join)?
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
                            break;
                        }
                    }
                    Filter::In(col, vals) => {
                        if !vals.contains(rec.index_values.get(col).unwrap_or(&String::new())) {
                            keep = false;
                            break;
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

    async fn query_projected(
        &self,
        request: QueryRequest,
        columns: &[String],
    ) -> Result<Vec<crate::backend::ProjectedRow>, StoreError> {
        let mut recs = self.scan_records(request.table).await?;

        for filter in &request.filters {
            match filter {
                Filter::Eq(col, val) => {
                    recs.retain(|r| r.index_values.get(col) == Some(val));
                }
                Filter::In(col, vals) => {
                    recs.retain(|r| r.index_values.get(col).is_some_and(|v| vals.contains(v)));
                }
            }
        }

        for (col, order) in request.order_by.iter().rev() {
            recs.sort_by(|a, b| order.compare(a.index_values.get(col), b.index_values.get(col)));
        }

        if let Some(n) = request.limit {
            recs.truncate(n);
        }

        // Index values only: the blob is never touched.
        Ok(recs
            .into_iter()
            .map(|rec| {
                let values = columns
                    .iter()
                    .map(|c| {
                        if c == "id" {
                            Some(uuid::Uuid::from_bytes(rec.id).as_simple().to_string())
                        } else {
                            rec.index_values.get(c).cloned()
                        }
                    })
                    .collect();
                crate::backend::ProjectedRow {
                    columns: columns.to_vec(),
                    values,
                }
            })
            .collect())
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
        items: &[([u8; 16], Vec<u8>, Vec<String>)],
        codec: Codec,
    ) -> Result<(), StoreError> {
        let table = T::TABLE;
        let groups = T::unique_constraints();
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
                to_delete.push((key, rec.index_values));
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
                let mut uq = txn
                    .open_table(UQ_TABLE)
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                for (key, index_values) in to_delete {
                    remove_unique_keys(&mut uq, table, groups, &index_values)?;
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
        let db = self.db.clone();
        let mut entries = Vec::with_capacity(batch.entries.len());
        for e in &batch.entries {
            let index_map: HashMap<String, String> = e
                .index_columns
                .iter()
                .zip(e.index_values.iter())
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect();
            let rec = Record {
                id: e.id_bytes,
                bytes: e.value_bytes.clone(),
                saved_at: 0,
                index_values: index_map,
            };
            let rec_bytes = bitcode::serialize(&rec).map_err(StoreError::Encode)?;
            let key = Self::make_key(e.table, &e.id_bytes);
            entries.push((
                e.table,
                e.unique_groups,
                e.id_bytes,
                key,
                rec_bytes,
                rec.index_values,
            ));
        }

        tokio::task::spawn_blocking(move || {
            let txn = db
                .begin_write()
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            {
                let mut tbl = txn
                    .open_table(KV_TABLE)
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                let mut uq = txn
                    .open_table(UQ_TABLE)
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                for (table, groups, id_bytes, key, rec_bytes, index_map) in entries {
                    let old: Option<HashMap<String, String>> = tbl
                        .get(key.as_slice())
                        .map_err(|e| StoreError::Redb(e.to_string()))?
                        .map(|v| {
                            bitcode::deserialize::<Record>(v.value())
                                .map(|r| r.index_values)
                                .unwrap_or_default()
                        });
                    sync_unique_keys(&mut uq, table, groups, old.as_ref(), &index_map, &id_bytes)?;
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

    async fn insert_guarded<T: Persistent>(
        &self,
        value: &T,
        guard: &SequenceGuard,
        codec: Codec,
    ) -> Result<GuardOutcome, StoreError> {
        let table = T::TABLE;
        let id_bytes = value.id().to_bytes();
        let key = Self::make_key(table, &id_bytes);
        let value_bytes = codec.encode(value)?;
        let index_map: HashMap<String, String> = T::index_columns()
            .iter()
            .zip(value.index_values().iter())
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        let record = Record {
            id: id_bytes,
            bytes: value_bytes,
            saved_at: 0,
            index_values: index_map,
        };
        let record_bytes = bitcode::serialize(&record).map_err(StoreError::Encode)?;
        let groups = T::unique_constraints();
        let partition = guard.partition.clone();
        let seq_col = guard.sequence_column.clone();
        let seq_val = guard.sequence_value;
        let (start, end) = Self::table_range(table);

        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db
                .begin_write()
                .map_err(|e| StoreError::Redb(e.to_string()))?;
            // Check and insert share one write transaction: redb serializes
            // writers, so no interleaving can slip a stale write through.
            let blocked = {
                let tbl = txn
                    .open_table(KV_TABLE)
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                let range = tbl
                    .range(start.as_slice()..=end.as_slice())
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                let mut blocked = false;
                for item in range {
                    let (_k, v) = item.map_err(|e| StoreError::Redb(e.to_string()))?;
                    let rec: Record =
                        bitcode::deserialize(v.value()).map_err(StoreError::Encode)?;
                    if partition
                        .iter()
                        .all(|(c, want)| rec.index_values.get(c) == Some(want))
                        && let Some(have) = rec.index_values.get(&seq_col)
                        && have.parse::<i64>().unwrap_or(0) >= seq_val
                    {
                        blocked = true;
                        break;
                    }
                }
                blocked
            };
            if blocked {
                return Ok::<_, StoreError>(GuardOutcome::Rejected);
            }
            {
                let mut tbl = txn
                    .open_table(KV_TABLE)
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                let mut uq = txn
                    .open_table(UQ_TABLE)
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
                let old: Option<HashMap<String, String>> = tbl
                    .get(key.as_slice())
                    .map_err(|e| StoreError::Redb(e.to_string()))?
                    .map(|v| {
                        bitcode::deserialize::<Record>(v.value())
                            .map(|r| r.index_values)
                            .unwrap_or_default()
                    });
                // A lost race on a UNIQUE group surfaces as Conflict, which
                // the Store layer reports as Rejected.
                sync_unique_keys(
                    &mut uq,
                    table,
                    groups,
                    old.as_ref(),
                    &record.index_values,
                    &id_bytes,
                )?;
                tbl.insert(key.as_slice(), record_bytes.as_slice())
                    .map_err(|e| StoreError::Redb(e.to_string()))?;
            }
            txn.commit().map_err(|e| StoreError::Redb(e.to_string()))?;
            Ok::<_, StoreError>(GuardOutcome::Landed)
        })
        .await
        .map_err(StoreError::Join)?
    }

    #[allow(clippy::unused_async_trait_impl)]
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
