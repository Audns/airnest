//! SQL dialect abstraction.
//!
//! Each relational backend (`SQLite`, `PostgreSQL`, ...) implements [`SqlDialect`]
//! to render DDL, DML, and identifiers in its native syntax. The dialect trait
//! is **internal** — it does not appear in the public API. Backends consume it
//! to generate SQL strings and bind layouts.
//!
//! # Design rationale
//!
//! The dialect interface describes **logical operations**, not surface syntax.
//! For example:
//!
//! - [`SqlDialect::current_epoch_seconds_expr`] returns the dialect's native
//!   way to express "now as epoch seconds" (`unixepoch()` in `SQLite`,
//!   `EXTRACT(EPOCH FROM now())::bigint` in Postgres).
//! - [`SqlDialect::placeholder`] returns the dialect's positional bind token
//!   (`?1` vs `$1` vs `?`).
//! - [`SqlDialect::quote_ident`] handles identifier quoting (`"name"` vs
//!   `` `name` `` vs `"name"`).
//!
//! This keeps the trait stable as engines diverge and prevents accidentally
//! encoding one engine's choices as the contract for others.
//!
//! # Conformance testing
//!
//! Unit tests in [`sqlite_dialect`](super::sqlite_dialect) pin `SQLite`'s
//! output. A future Postgres dialect will run the same conformance checks.

use crate::backend::{Filter, QueryRequest};

/// Description of a persistent table to be created.
///
/// `index_columns` lists the extra TEXT columns (and matching indexes)
/// that the engine should add beyond the canonical `id`, `v`, `saved_at`
/// triple.
#[derive(Debug, Clone, Copy)]
pub struct TableSchema<'a> {
    pub table: &'a str,
    pub index_columns: &'a [&'a str],
}

/// Result of rendering a dynamic SELECT statement with binds.
///
/// `sql` is the complete statement; `binds` is the ordered list of values
/// the caller should bind positionally.
#[derive(Debug, Clone)]
pub struct RenderedSelect {
    pub sql: String,
    pub binds: Vec<String>,
}

/// SQL rendering for a relational engine.
///
/// Implementors describe the logical operation (e.g. "current epoch seconds"),
/// not the syntax. The backend then composes these primitives into full
/// statements.
pub trait SqlDialect: Send + Sync + 'static {
    // ── Type system ──────────────────────────────────────────────

    /// SQL type used to store binary columns (`id`, `v`).
    /// `SQLite`: `"BLOB"`. Postgres: `"BYTEA"`.
    fn blob_type(&self) -> &'static str;

    /// SQL type used to store the `saved_at` epoch-seconds column.
    /// `SQLite`: `"INTEGER"`. Postgres: `"BIGINT"`.
    fn epoch_seconds_type(&self) -> &'static str;

    /// SQL expression that yields the current epoch seconds at write time.
    /// `SQLite`: `"unixepoch()"`. Postgres: `"EXTRACT(EPOCH FROM now())::bigint"`.
    fn current_epoch_seconds_expr(&self) -> &'static str;

    /// Whether tables should be declared with `STRICT` type affinity.
    /// `SQLite`: `true` (since 3.37). Postgres: always strict by default.
    fn strict_tables(&self) -> bool;

    // ── Session setup ────────────────────────────────────────────

    /// Statements to run on a fresh connection (e.g. `SQLite` `PRAGMA`s).
    fn session_init_sql(&self) -> &'static [&'static str];

    // ── Identifier and placeholder formatting ────────────────────

    /// Renders an identifier (table or column name) with dialect-appropriate
    /// quoting.
    fn quote_ident(&self, name: &str) -> String;

    /// Renders a positional bind placeholder for bind position `n` (1-based).
    /// `SQLite`: `"?1"`. Postgres: `"$1"`. `MySQL`: `"?"`.
    fn placeholder(&self, n: usize) -> String;

    // ── DDL ──────────────────────────────────────────────────────

    /// Renders a `CREATE TABLE IF NOT EXISTS` statement.
    fn render_create_table(&self, schema: &TableSchema) -> String;

    /// Renders an `ALTER TABLE ... ADD COLUMN` statement.
    fn render_add_column(&self, table: &str, column: &str) -> String;

    /// Renders a `CREATE INDEX IF NOT EXISTS` statement.
    /// Index naming convention is the dialect's choice.
    fn render_create_index(&self, table: &str, column: &str) -> String;

    /// Renders a `CREATE UNIQUE INDEX IF NOT EXISTS` statement for one
    /// uniqueness group. Additive: runs after table creation, so it also
    /// applies to tables created before the constraint was declared.
    /// Index naming convention is the dialect's choice, but must be
    /// deterministic in (table, columns).
    fn render_create_unique_index(&self, table: &str, columns: &[&str]) -> String;

    // ── DML ──────────────────────────────────────────────────────

    /// Renders an `INSERT ... ON CONFLICT DO UPDATE` statement for a single row.
    /// The dialect assumes bind order is `id, v, [index_values...]`.
    fn render_upsert(&self, schema: &TableSchema) -> String;

    /// Renders a plain `INSERT` for a single row: no conflict clause, so an
    /// existing id or unique group fails the statement. Bind order is
    /// `id, v, [index_values...]`, the same as [`render_upsert`](Self::render_upsert).
    fn render_insert(&self, schema: &TableSchema) -> String {
        let quoted_table = self.quote_ident(schema.table);
        let now = self.current_epoch_seconds_expr();
        let mut cols = String::new();
        let mut placeholders = String::new();
        for (i, col) in schema.index_columns.iter().enumerate() {
            cols.push_str(", ");
            cols.push_str(&self.quote_ident(col));
            placeholders.push_str(", ");
            placeholders.push_str(&self.placeholder(i + 3));
        }
        format!(
            "INSERT INTO {quoted_table} (id, v, saved_at{cols}) VALUES ({}, {}, {now}{placeholders})",
            self.placeholder(1),
            self.placeholder(2),
        )
    }

    /// Renders `DELETE FROM <table> [WHERE ...]` with its binds.
    fn render_delete_where(&self, table: &str, filters: &[Filter]) -> RenderedSelect {
        let (where_clause, binds) = self.render_where_clause(filters);
        let mut sql = format!("DELETE FROM {}", self.quote_ident(table));
        if !where_clause.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clause);
        }
        RenderedSelect { sql, binds }
    }

    /// Renders an atomic guarded insert for a monotonic sequence.
    ///
    /// Shape: `INSERT ... SELECT id, v, now, index_values...
    /// WHERE NOT EXISTS (a row in `partition` at or beyond the sequence
    /// value)`. The check and the insert are one statement, so concurrent
    /// writers cannot interleave between them; pair with a `UNIQUE` index
    /// on `(partition..., sequence)` so even a lost race resolves to zero
    /// affected rows (or a mappable conflict) rather than duplicates.
    ///
    /// Bind order is fixed: `id, v, [index_values...],
    /// [partition values...], sequence_value`.
    fn render_insert_guarded(
        &self,
        schema: &TableSchema,
        partition: &[&str],
        sequence_column: &str,
    ) -> String;

    /// Renders a dynamic SELECT statement for a query request.
    /// `select_clause` is the leading projection (e.g. `"SELECT v"` or
    /// `"SELECT COUNT(*)"`).
    fn render_select(&self, request: &QueryRequest, select_clause: &str) -> RenderedSelect;

    /// SQL expression sorting `column` numerically (a TEXT cell that is
    /// not an integer sorts as 0). `SQLite`: `CAST(col AS INTEGER)`.
    fn numeric_sort_expr(&self, quoted_column: &str) -> String;

    /// Renders the ` ORDER BY a ASC, b DESC` suffix (leading space), or an
    /// empty string for no ordering. One clause for every key: repeating
    /// `ORDER BY` per key is a syntax error.
    fn render_order_by(&self, order_by: &[(String, crate::backend::Order)]) -> String {
        if order_by.is_empty() {
            return String::new();
        }
        let keys: Vec<String> = order_by
            .iter()
            .map(|(col, order)| {
                let quoted = self.quote_ident(col);
                let expr = if order.is_numeric() {
                    self.numeric_sort_expr(&quoted)
                } else {
                    quoted
                };
                let dir = if order.is_descending() { "DESC" } else { "ASC" };
                format!("{expr} {dir}")
            })
            .collect();
        format!(" ORDER BY {}", keys.join(", "))
    }

    /// Renders a `WHERE` clause (without the leading keyword) for the given
    /// filters. Returns the rendered fragment and the bind values in order.
    /// Used by both [`render_select`](Self::render_select) and external
    /// DELETE flows (e.g. `replace_where`).
    fn render_where_clause(&self, filters: &[Filter]) -> (String, Vec<String>);
}
