//! `PostgreSQL` implementation of [`SqlDialect`].
//!
//! **Compile-only stub.** No `PostgresBackend` exists yet; this module exists
//! purely to validate that the [`SqlDialect`] trait is sufficient for an
//! engine other than `SQLite`.
//!
//! # Validating the trait
//!
//! The conformance tests in this module mirror those in
//! [`sqlite_dialect`](super::sqlite_dialect). Where `SQLite` uses one syntax,
//! Postgres uses another. If a test fails to compile, the trait is missing
//! a method. If a test fails at runtime, the trait's *semantics* need
//! adjustment (e.g. a method that returns the wrong kind of value).
//!
//! # Divergences from `SQLite`
//!
//! - Placeholders: `?1` → `$1`
//! - Binary type: `BLOB` → `BYTEA`
//! - Integer type: `INTEGER` → `BIGINT`
//! - Epoch expression: `unixepoch()` → `EXTRACT(EPOCH FROM now())::bigint`
//! - `STRICT` keyword not used (PG is strictly typed by default)
//! - `ON CONFLICT (id)` requires parentheses around the conflict target
//! - No PRAGMAs at session init
//!
//! # What is *not* in the trait (yet)
//!
//! Several Postgres-specific capabilities are intentionally absent:
//!
//! - Multi-row `INSERT ... VALUES (...), (...)` (perf optimization only)
//! - `RETURNING` clause for round-tripping `saved_at`
//! - Array types for `IN (...)` (`= ANY($1)` strategy)
//! - Schema-qualified names (`public."Job"`)
//! - `BIGSERIAL` / `GENERATED AS IDENTITY` for primary keys (we use UUIDs)
//!
//! These can be added as new trait methods if and when they become necessary.

use std::fmt::Write;

use crate::backend::{
    Filter, Order, QueryRequest,
    dialect::{RenderedSelect, SqlDialect, TableSchema},
};

/// `PostgreSQL` dialect.
///
/// Stateless; can be wrapped in `Arc<dyn SqlDialect>` and shared across
/// clones of a backend (once one exists).
#[derive(Debug, Clone, Copy, Default)]
#[allow(dead_code)] // not constructed unless the `postgres` feature is on
pub struct PostgresDialect;

impl SqlDialect for PostgresDialect {
    fn blob_type(&self) -> &'static str {
        "BYTEA"
    }

    fn epoch_seconds_type(&self) -> &'static str {
        "BIGINT"
    }

    fn current_epoch_seconds_expr(&self) -> &'static str {
        "EXTRACT(EPOCH FROM now())::bigint"
    }

    fn strict_tables(&self) -> bool {
        // Postgres is strictly typed by default; no STRICT keyword.
        false
    }

    fn session_init_sql(&self) -> &'static [&'static str] {
        // PG has no PRAGMAs. A real backend may want `SET synchronous_commit`
        // here for parity with SQLite's `synchronous=NORMAL`, but that is a
        // deployment choice, not a dialect requirement.
        &[]
    }

    fn quote_ident(&self, name: &str) -> String {
        // Postgres accepts standard SQL double-quote delimiters.
        format!("\"{name}\"")
    }

    fn placeholder(&self, n: usize) -> String {
        format!("${n}")
    }

    fn render_create_table(&self, schema: &TableSchema) -> String {
        let TableSchema {
            table,
            index_columns,
        } = *schema;
        let quoted_table = self.quote_ident(table);

        let mut body = format!(
            "    id        {} NOT NULL PRIMARY KEY,\n\
             \x20\x20\x20\x20v         {} NOT NULL,\n\
             \x20\x20\x20\x20saved_at  {} NOT NULL DEFAULT ({})",
            self.blob_type(),
            self.blob_type(),
            self.epoch_seconds_type(),
            self.current_epoch_seconds_expr(),
        );
        for col in index_columns {
            let _ = write!(body, ",\n    {} TEXT", self.quote_ident(col));
        }

        // Postgres does not use a STRICT keyword.
        format!("CREATE TABLE IF NOT EXISTS {quoted_table} (\n{body}\n)")
    }

    fn render_add_column(&self, table: &str, column: &str) -> String {
        format!(
            "ALTER TABLE {} ADD COLUMN {} TEXT",
            self.quote_ident(table),
            self.quote_ident(column),
        )
    }

    fn render_create_index(&self, table: &str, column: &str) -> String {
        let idx_name = format!("{table}_{column}_idx");
        format!(
            "CREATE INDEX IF NOT EXISTS {}\n    ON {} ({})",
            self.quote_ident(&idx_name),
            self.quote_ident(table),
            self.quote_ident(column),
        )
    }

    fn render_upsert(&self, schema: &TableSchema) -> String {
        let TableSchema {
            table,
            index_columns,
        } = *schema;
        let quoted_table = self.quote_ident(table);
        let id_ph = self.placeholder(1);
        let v_ph = self.placeholder(2);
        let now = self.current_epoch_seconds_expr();

        if index_columns.is_empty() {
            return format!(
                "INSERT INTO {quoted_table} (id, v, saved_at)\n\
                 VALUES ({id_ph}, {v_ph}, {now})\n\
                 ON CONFLICT (id) DO UPDATE SET\n\
                 \x20\x20\x20\x20v = excluded.v,\n\
                 \x20\x20\x20\x20saved_at = excluded.saved_at",
            );
        }

        let mut col_list = String::new();
        let mut placeholders = String::new();
        let mut updates = String::new();
        for (i, col) in index_columns.iter().enumerate() {
            let idx = i + 3;
            let quoted = self.quote_ident(col);
            let _ = write!(col_list, ", {quoted}");
            let _ = write!(placeholders, ", {}", self.placeholder(idx));
            let _ = write!(updates, ", {quoted} = excluded.{quoted}");
        }

        format!(
            "INSERT INTO {quoted_table} (id, v, saved_at{col_list})\n\
             VALUES ({id_ph}, {v_ph}, {now}{placeholders})\n\
             ON CONFLICT (id) DO UPDATE SET\n\
             \x20\x20\x20\x20v = excluded.v,\n\
             \x20\x20\x20\x20saved_at = excluded.saved_at{updates}",
        )
    }

    fn render_select(&self, request: &QueryRequest, select_clause: &str) -> RenderedSelect {
        let mut sql = format!("{select_clause} FROM {}", self.quote_ident(request.table));

        let (where_clause, binds) = self.render_where_clause(&request.filters);
        if !where_clause.is_empty() {
            let _ = write!(sql, " WHERE {where_clause}");
        }

        for (col, order) in &request.order_by {
            let dir = match order {
                Order::Asc => "ASC",
                Order::Desc => "DESC",
            };
            let _ = write!(sql, " ORDER BY {} {dir}", self.quote_ident(col));
        }

        if let Some(n) = request.limit {
            let _ = write!(sql, " LIMIT {n}");
        }

        RenderedSelect { sql, binds }
    }

    fn render_where_clause(&self, filters: &[Filter]) -> (String, Vec<String>) {
        let mut sql = String::new();
        let mut binds = Vec::new();
        let mut param_idx = 1usize;

        for filter in filters {
            if !sql.is_empty() {
                sql.push_str(" AND ");
            }
            match filter {
                Filter::Eq(col, val) => {
                    let _ = write!(
                        sql,
                        "{} = {}",
                        self.quote_ident(col),
                        self.placeholder(param_idx)
                    );
                    binds.push(val.clone());
                    param_idx += 1;
                }
                Filter::In(col, vals) => {
                    let placeholders: Vec<String> = (0..vals.len())
                        .map(|i| self.placeholder(param_idx + i))
                        .collect();
                    let _ = write!(
                        sql,
                        "{} IN ({})",
                        self.quote_ident(col),
                        placeholders.join(", ")
                    );
                    binds.extend(vals.iter().cloned());
                    param_idx += vals.len();
                }
            }
        }

        (sql, binds)
    }
}

// ── Conformance tests ────────────────────────────────────────────────────────
//
// Mirror the SQLite conformance tests. Each test pins one aspect of the
// Postgres dialect's output. Together with the SQLite tests they define the
// cross-engine contract.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Order;

    fn d() -> PostgresDialect {
        PostgresDialect
    }

    // ── type system ──────────────────────────────────────────────────────

    #[test]
    fn blob_type_is_bytea() {
        assert_eq!(d().blob_type(), "BYTEA");
    }

    #[test]
    fn epoch_seconds_type_is_bigint() {
        assert_eq!(d().epoch_seconds_type(), "BIGINT");
    }

    #[test]
    fn current_epoch_seconds_expr_is_postgres_native() {
        assert_eq!(
            d().current_epoch_seconds_expr(),
            "EXTRACT(EPOCH FROM now())::bigint"
        );
    }

    #[test]
    fn strict_tables_is_false() {
        // Postgres is strictly typed by default; no STRICT keyword needed.
        assert!(!d().strict_tables());
    }

    // ── session init ─────────────────────────────────────────────────────

    #[test]
    fn session_init_sql_is_empty() {
        // PG has no PRAGMAs. A real backend may add session GUCs here.
        assert!(d().session_init_sql().is_empty());
    }

    // ── identifier / placeholder ─────────────────────────────────────────

    #[test]
    fn quote_ident_uses_double_quotes() {
        assert_eq!(d().quote_ident("User"), "\"User\"");
        assert_eq!(d().quote_ident("status"), "\"status\"");
    }

    #[test]
    fn placeholder_uses_dollar_sign() {
        assert_eq!(d().placeholder(1), "$1");
        assert_eq!(d().placeholder(2), "$2");
        assert_eq!(d().placeholder(42), "$42");
    }

    // ── DDL: create table ────────────────────────────────────────────────

    #[test]
    fn create_table_no_indexes_uses_pg_types_and_no_strict() {
        let schema = TableSchema {
            table: "User",
            index_columns: &[],
        };
        let sql = d().render_create_table(&schema);
        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS \"User\" ("));
        assert!(sql.contains("id        BYTEA NOT NULL PRIMARY KEY"));
        assert!(sql.contains("v         BYTEA NOT NULL"));
        assert!(
            sql.contains("saved_at  BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())::bigint)")
        );
        // No STRICT keyword.
        assert!(
            !sql.contains("STRICT"),
            "PG must not emit STRICT; got:\n{sql}"
        );
        assert!(sql.ends_with(')'));
    }

    #[test]
    fn create_table_with_indexes_separates_columns_correctly() {
        // Same regression shape as the SQLite test: the first index column
        // must be on its own line, separated from saved_at.
        let schema = TableSchema {
            table: "Job",
            index_columns: &["status", "priority"],
        };
        let sql = d().render_create_table(&schema);
        assert!(
            sql.contains("saved_at  BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())::bigint),\n    \"status\" TEXT"),
            "missing separator between saved_at and first index column; got:\n{sql}"
        );
        assert!(sql.contains(",\n    \"priority\" TEXT"));
        assert!(!sql.contains("STRICT"));
    }

    // ── DDL: add column ──────────────────────────────────────────────────

    #[test]
    fn add_column_uses_alter_table_with_text() {
        // PG accepts the same minimal ALTER TABLE ADD COLUMN syntax as SQLite.
        let sql = d().render_add_column("Job", "status");
        assert_eq!(sql, "ALTER TABLE \"Job\" ADD COLUMN \"status\" TEXT");
    }

    // ── DDL: create index ────────────────────────────────────────────────

    #[test]
    fn create_index_uses_dialect_naming_convention() {
        let sql = d().render_create_index("Job", "status");
        assert!(sql.starts_with("CREATE INDEX IF NOT EXISTS \"Job_status_idx\""));
        assert!(sql.contains("ON \"Job\" (\"status\")"));
    }

    // ── DML: upsert ──────────────────────────────────────────────────────

    #[test]
    fn upsert_no_indexes_uses_dollar_placeholders_and_paren_conflict() {
        let schema = TableSchema {
            table: "User",
            index_columns: &[],
        };
        let sql = d().render_upsert(&schema);
        assert!(sql.contains("VALUES ($1, $2, EXTRACT(EPOCH FROM now())::bigint)"));
        // Postgres requires parens around the conflict target.
        assert!(
            sql.contains("ON CONFLICT (id) DO UPDATE SET"),
            "PG requires ON CONFLICT (id) with parens; got:\n{sql}"
        );
    }

    #[test]
    fn upsert_with_indexes_uses_sequential_dollar_placeholders() {
        let schema = TableSchema {
            table: "Job",
            index_columns: &["status", "priority"],
        };
        let sql = d().render_upsert(&schema);
        assert!(sql.contains("(id, v, saved_at, \"status\", \"priority\")"));
        assert!(sql.contains("VALUES ($1, $2, EXTRACT(EPOCH FROM now())::bigint, $3, $4)"));
        assert!(sql.contains("\"status\" = excluded.\"status\""));
        assert!(sql.contains("\"priority\" = excluded.\"priority\""));
    }

    // ── DML: where clause ────────────────────────────────────────────────

    #[test]
    fn where_clause_single_eq_uses_dollar_placeholder() {
        let (sql, binds) =
            d().render_where_clause(&[Filter::Eq("status".into(), "pending".into())]);
        assert_eq!(sql, "\"status\" = $1");
        assert_eq!(binds, vec!["pending".to_string()]);
    }

    #[test]
    fn where_clause_in_expands_with_dollar_placeholders() {
        let (sql, binds) = d().render_where_clause(&[Filter::In(
            "status".into(),
            vec!["a".into(), "b".into(), "c".into()],
        )]);
        assert_eq!(sql, "\"status\" IN ($1, $2, $3)");
        assert_eq!(binds, vec!["a", "b", "c"]);
    }

    #[test]
    fn where_clause_combines_eq_and_in_with_sequential_placeholders() {
        let (sql, binds) = d().render_where_clause(&[
            Filter::Eq("kind".into(), "task".into()),
            Filter::In("status".into(), vec!["pending".into(), "running".into()]),
        ]);
        assert_eq!(sql, "\"kind\" = $1 AND \"status\" IN ($2, $3)");
        assert_eq!(binds, vec!["task", "pending", "running"]);
    }

    // ── DML: select ──────────────────────────────────────────────────────

    #[test]
    fn select_no_filters_or_ordering_is_minimal() {
        let req = QueryRequest {
            table: "Job",
            filters: vec![],
            order_by: vec![],
            limit: None,
        };
        let r = d().render_select(&req, "SELECT v");
        assert_eq!(r.sql, "SELECT v FROM \"Job\"");
        assert!(r.binds.is_empty());
    }

    #[test]
    fn select_with_filters_uses_dollar_placeholders() {
        let req = QueryRequest {
            table: "Job",
            filters: vec![Filter::Eq("status".into(), "pending".into())],
            order_by: vec![],
            limit: None,
        };
        let r = d().render_select(&req, "SELECT v");
        assert_eq!(r.sql, "SELECT v FROM \"Job\" WHERE \"status\" = $1");
        assert_eq!(r.binds, vec!["pending"]);
    }

    #[test]
    fn select_with_order_by_and_limit() {
        let req = QueryRequest {
            table: "Job",
            filters: vec![],
            order_by: vec![("priority".into(), Order::Asc)],
            limit: Some(10),
        };
        let r = d().render_select(&req, "SELECT v");
        assert_eq!(
            r.sql,
            "SELECT v FROM \"Job\" ORDER BY \"priority\" ASC LIMIT 10"
        );
    }

    // ── contract ─────────────────────────────────────────────────────────

    #[test]
    fn contract_every_renderer_produces_non_empty_output() {
        let schema = TableSchema {
            table: "Job",
            index_columns: &["status"],
        };
        let req = QueryRequest {
            table: "Job",
            filters: vec![Filter::Eq("status".into(), "pending".into())],
            order_by: vec![("status".into(), Order::Asc)],
            limit: Some(5),
        };
        let dd = d();

        assert!(!dd.render_create_table(&schema).is_empty());
        assert!(!dd.render_add_column("Job", "status").is_empty());
        assert!(!dd.render_create_index("Job", "status").is_empty());
        assert!(!dd.render_upsert(&schema).is_empty());
        let r = dd.render_select(&req, "SELECT v");
        assert!(!r.sql.is_empty());
        assert!(!r.binds.is_empty());
    }
}
