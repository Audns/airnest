//! Backend abstraction layer — trait + shared types.

use std::collections::HashMap;

use crate::{Codec, Persistent, StoreError};

#[cfg(feature = "redb")]
pub mod redb;
pub mod sqlite;

#[cfg(feature = "postgres")]
pub mod postgres;

// Internal SQL dialect abstraction. Crate-internal so it does not appear in
// the public API; consumed by relational backends to render SQL.
pub(crate) mod dialect;

pub(crate) mod sqlite_dialect;

// Compile-only stub. Validates that the SqlDialect trait is sufficient for
// a non-SQLite engine. Used by `postgres` feature; also useful as a
// reference for future dialect implementors.
pub(crate) mod postgres_dialect;

/// Filter condition for backend-agnostic queries.
#[derive(Debug, Clone)]
pub enum Filter {
    Eq(String, String),
    In(String, Vec<String>),
}

/// Whether a sqlx error is a uniqueness violation on any SQL backend:
/// SQLite extended code 2067, Postgres SQLSTATE 23505. Used by
/// [`Store`](crate::Store) to translate deep driver errors into
/// [`StoreError::Conflict`](crate::StoreError::Conflict).
pub(crate) fn is_unique_violation(e: &sqlx::Error) -> bool {
    let Some(db) = e.as_database_error() else {
        return false;
    };
    matches!(db.code().as_deref(), Some("2067") | Some("23505"))
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

/// One metadata-only row: index-column values without the blob.
///
/// Returned by projection reads ([`Backend::query_projected`]). `columns`
/// echoes the requested column order; `values[i]` is the value of
/// `columns[i]` (`None` when the row predates the column or the backend
/// has no value for it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedRow {
    pub columns: Vec<String>,
    pub values: Vec<Option<String>>,
}

impl ProjectedRow {
    /// The value of `column`, if it was requested and the row has one.
    #[must_use]
    pub fn get(&self, column: &str) -> Option<&str> {
        self.columns
            .iter()
            .position(|c| c == column)
            .and_then(|i| self.values.get(i).and_then(Option::as_deref))
    }
}

/// Monotonic-sequence guard for an atomic conditional insert
/// ([`Store::insert_guarded`](crate::Store::insert_guarded)).
///
/// The guarded column is numeric-by-contract (stored as TEXT, compared
/// numerically): versions, generations, sequence numbers. Let `M` be the
/// maximum value of `sequence_column` over rows matching `partition`
/// (`NULL` when no row matches). The insert lands if and only if `M` is
/// `NULL` or `M < sequence_value` — otherwise nothing is written and the
/// outcome is [`GuardOutcome::Rejected`].
#[derive(Debug, Clone)]
pub struct SequenceGuard {
    /// Equality filters scoping the sequence, e.g.
    /// `[("session_id".into(), "chat/abc".into())]`. Empty means the whole
    /// table shares one sequence.
    pub partition: Vec<(String, String)>,
    /// Numeric-by-contract index column holding the sequence.
    pub sequence_column: String,
    /// The sequence value the inserted row carries.
    pub sequence_value: i64,
}

/// Outcome of [`Store::insert_guarded`](crate::Store::insert_guarded).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardOutcome {
    /// The guard held; the row was inserted.
    Landed,
    /// A row at or beyond the guarded sequence already stands (or a lost
    /// race hit the matching `UNIQUE` index); nothing was written.
    Rejected,
}

/// Single entry in a backend-agnostic batch.
#[derive(Debug, Clone)]
pub struct BatchEntry {
    pub table: &'static str,
    pub id_bytes: [u8; 16],
    pub value_bytes: Vec<u8>,
    pub index_columns: &'static [&'static str],
    pub index_values: Vec<String>,
    /// Uniqueness groups for this entry's type (see
    /// [`Persistent::unique_constraints`](crate::Persistent::unique_constraints)).
    /// The batch path creates them alongside the table, like `ensure_table` does.
    pub unique_groups: &'static [&'static [&'static str]],
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
        ids: &[[u8; 16]],
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

    /// Metadata-only query: return the requested index columns for matching
    /// rows without fetching or decoding blobs. Filters, ordering, and
    /// limit behave exactly like [`query`](Backend::query).
    async fn query_projected(
        &self,
        request: QueryRequest,
        columns: &[String],
    ) -> Result<Vec<ProjectedRow>, StoreError>;

    /// Atomic conditional insert for monotonic sequences.
    ///
    /// Inserts `value` if and only if no row matching `guard.partition`
    /// carries `guard.sequence_column >= guard.sequence_value` (numeric
    /// comparison; missing cells don't block, unparseable cells count as
    /// 0). The check and the insert are one atomic unit: a single
    /// statement on SQL backends, one write transaction on redb. A lost
    /// race against a matching `UNIQUE` index is reported as
    /// [`GuardOutcome::Rejected`], never an error.
    async fn insert_guarded<T: Persistent>(
        &self,
        value: &T,
        guard: &SequenceGuard,
        codec: Codec,
    ) -> Result<GuardOutcome, StoreError>;

    async fn query_count(&self, request: QueryRequest) -> Result<i64, StoreError>;

    async fn count_grouped_by<T: Persistent>(
        &self,
        column: &str,
    ) -> Result<HashMap<String, i64>, StoreError>;

    async fn replace_where<T: Persistent>(
        &self,
        filters: &[(String, String)],
        items: &[([u8; 16], Vec<u8>, Vec<String>)],
        codec: Codec,
    ) -> Result<(), StoreError>;

    async fn save_batch(&self, batch: &BackendBatch, codec: Codec) -> Result<(), StoreError>;

    fn as_sqlite_pool(&self) -> Option<&sqlx::SqlitePool>;

    async fn query_raw<T: Persistent>(&self, sql: &str, codec: Codec)
    -> Result<Vec<T>, StoreError>;
}
