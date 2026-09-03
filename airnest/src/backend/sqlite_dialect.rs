//! SQLite-specific implementation of [`SqlDialect`].
//!
//! This module also hosts the **dialect conformance test suite** for `SQLite`.
//! Each test pins one aspect of the dialect contract so that future engines
//! (Postgres, `MySQL`, ...) can be measured against the same expectations.

use std::fmt::Write;

use crate::backend::{
    Filter, Order, QueryRequest,
    dialect::{RenderedSelect, SqlDialect, TableSchema},
};

/// `SQLite` dialect.
///
/// Stateless; can be wrapped in `Arc<dyn SqlDialect>` and shared across
/// clones of a backend.
#[derive(Debug, Clone, Copy, Default)]
pub struct SqliteDialect;

impl SqlDialect for SqliteDialect {
    fn blob_type(&self) -> &'static str {
        "BLOB"
    }

    fn epoch_seconds_type(&self) -> &'static str {
        "INTEGER"
    }

    fn current_epoch_seconds_expr(&self) -> &'static str {
        "unixepoch()"
    }

    fn strict_tables(&self) -> bool {
        true
    }

    fn session_init_sql(&self) -> &'static [&'static str] {
        &[
            "PRAGMA journal_mode=WAL",
            "PRAGMA synchronous=NORMAL",
            "PRAGMA foreign_keys=OFF",
            "PRAGMA cache_size=-64000",
            "PRAGMA temp_store=MEMORY",
            "PRAGMA busy_timeout=5000",
            "PRAGMA wal_autocheckpoint=1000",
            "PRAGMA mmap_size=268435456",
        ]
    }

    fn quote_ident(&self, name: &str) -> String {
        format!("\"{name}\"")
    }

    fn placeholder(&self, n: usize) -> String {
        format!("?{n}")
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

        let strict = if self.strict_tables() { " STRICT" } else { "" };
        format!("CREATE TABLE IF NOT EXISTS {quoted_table} (\n{body}\n){strict}")
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

    fn render_create_unique_index(&self, table: &str, columns: &[&str]) -> String {
        let idx_name = format!("{table}_{}_uniq", columns.join("_"));
        let cols = columns
            .iter()
            .map(|c| self.quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {}\n    ON {} ({})",
            self.quote_ident(&idx_name),
            self.quote_ident(table),
            cols,
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
                 ON CONFLICT(id) DO UPDATE SET\n\
                 \x20\x20\x20\x20v = excluded.v,\n\
                 \x20\x20\x20\x20saved_at = excluded.saved_at",
            );
        }

        let mut col_list = String::new();
        let mut placeholders = String::new();
        let mut updates = String::new();
        for (i, col) in index_columns.iter().enumerate() {
            let idx = i + 3; // placeholder positions start at 3 (after id, v)
            let quoted = self.quote_ident(col);
            let _ = write!(col_list, ", {quoted}");
            let _ = write!(placeholders, ", {}", self.placeholder(idx));
            let _ = write!(updates, ", {quoted} = excluded.{quoted}");
        }

        format!(
            "INSERT INTO {quoted_table} (id, v, saved_at{col_list})\n\
             VALUES ({id_ph}, {v_ph}, {now}{placeholders})\n\
             ON CONFLICT(id) DO UPDATE SET\n\
             \x20\x20\x20\x20v = excluded.v,\n\
             \x20\x20\x20\x20saved_at = excluded.saved_at{updates}",
        )
    }

    fn render_insert_guarded(
        &self,
        schema: &TableSchema,
        partition: &[&str],
        sequence_column: &str,
    ) -> String {
        let TableSchema {
            table,
            index_columns,
        } = *schema;
        let quoted_table = self.quote_ident(table);
        let now = self.current_epoch_seconds_expr();

        // Bind order: id ?1, v ?2, index values ?3.., partition values..,
        // sequence value last. Non-numeric sequence cells CAST to 0, matching
        // the application's unwrap_or(0) convention.
        let mut next = 3;
        let mut placeholders = String::new();
        for _ in index_columns.iter() {
            let _ = write!(placeholders, ", {}", self.placeholder(next));
            next += 1;
        }
        let mut guards = Vec::with_capacity(partition.len() + 1);
        for col in partition {
            guards.push(format!(
                "{} = {}",
                self.quote_ident(col),
                self.placeholder(next)
            ));
            next += 1;
        }
        guards.push(format!(
            "CAST({} AS INTEGER) >= {}",
            self.quote_ident(sequence_column),
            self.placeholder(next)
        ));
        let where_clause = guards.join(" AND ");

        let mut col_list = String::new();
        for col in index_columns.iter() {
            let _ = write!(col_list, ", {}", self.quote_ident(col));
        }
        format!(
            "INSERT INTO {quoted_table} (id, v, saved_at{col_list})\n\
             SELECT ?1, ?2, {now}{placeholders}\n\
             WHERE NOT EXISTS (\n\
             \x20\x20\x20\x20SELECT 1 FROM {quoted_table} WHERE {where_clause}\n\
             )",
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
// These tests pin the SQLite dialect's output. A future `PostgresDialect`
// should pass equivalent tests (renamed `postgres_*`) so the contract is
// verified across engines.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Order;

    fn d() -> SqliteDialect {
        SqliteDialect
    }

    // ── type system ──────────────────────────────────────────────────────

    #[test]
    fn blob_type_is_blob() {
        assert_eq!(d().blob_type(), "BLOB");
    }

    #[test]
    fn epoch_seconds_type_is_integer() {
        assert_eq!(d().epoch_seconds_type(), "INTEGER");
    }

    #[test]
    fn current_epoch_seconds_expr_is_unixepoch() {
        assert_eq!(d().current_epoch_seconds_expr(), "unixepoch()");
    }

    #[test]
    fn strict_tables_is_true() {
        assert!(d().strict_tables());
    }

    // ── session init ─────────────────────────────────────────────────────

    #[test]
    fn session_init_sql_has_three_pragmas() {
        let stmts = d().session_init_sql();
        assert!(stmts.len() >= 3);
        assert!(stmts.iter().any(|s| s.contains("journal_mode")));
        assert!(stmts.iter().any(|s| s.contains("synchronous")));
        assert!(stmts.iter().any(|s| s.contains("foreign_keys")));
        // Tuned pragmas for throughput
        assert!(stmts.iter().any(|s| s.contains("cache_size")));
        assert!(stmts.iter().any(|s| s.contains("busy_timeout")));
    }

    // ── identifier / placeholder ─────────────────────────────────────────

    #[test]
    fn quote_ident_wraps_in_double_quotes() {
        assert_eq!(d().quote_ident("User"), "\"User\"");
        assert_eq!(d().quote_ident("status"), "\"status\"");
    }

    #[test]
    fn placeholder_uses_question_mark_index() {
        assert_eq!(d().placeholder(1), "?1");
        assert_eq!(d().placeholder(2), "?2");
        assert_eq!(d().placeholder(42), "?42");
    }

    // ── DDL: create table ────────────────────────────────────────────────

    #[test]
    fn create_table_no_indexes_is_strict_and_has_canonical_columns() {
        let schema = TableSchema {
            table: "User",
            index_columns: &[],
        };
        let sql = d().render_create_table(&schema);
        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS \"User\" ("));
        // The dialect uses canonical column names unquoted in DDL.
        assert!(sql.contains("id        BLOB NOT NULL PRIMARY KEY"));
        assert!(sql.contains("v         BLOB NOT NULL"));
        assert!(sql.contains("saved_at  INTEGER NOT NULL DEFAULT (unixepoch())"));
        assert!(sql.ends_with(") STRICT"));
    }

    #[test]
    fn create_table_with_indexes_appends_text_columns_and_indents_correctly() {
        let schema = TableSchema {
            table: "Job",
            index_columns: &["status", "priority"],
        };
        let sql = d().render_create_table(&schema);
        // Regression test for the historical bug where the first index column
        // was glued onto the `saved_at` line without a leading separator.
        assert!(
            sql.contains("saved_at  INTEGER NOT NULL DEFAULT (unixepoch()),\n    \"status\" TEXT"),
            "missing separator between saved_at and first index column; got:\n{sql}"
        );
        assert!(sql.contains(",\n    \"priority\" TEXT"));
        assert!(sql.ends_with(") STRICT"));
    }

    // ── DDL: add column ──────────────────────────────────────────────────

    #[test]
    fn add_column_uses_alter_table_with_text() {
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

    #[test]
    fn create_unique_index_names_groups_deterministically() {
        let sql = d().render_create_unique_index("Revision", &["session_id", "generation"]);
        assert_eq!(
            sql,
            "CREATE UNIQUE INDEX IF NOT EXISTS \"Revision_session_id_generation_uniq\"\n    ON \"Revision\" (\"session_id\", \"generation\")"
        );
        let single = d().render_create_unique_index("Job", &["name"]);
        assert!(single.starts_with("CREATE UNIQUE INDEX IF NOT EXISTS \"Job_name_uniq\""));
    }

    #[test]
    fn insert_guarded_binds_id_v_indexes_partition_then_sequence() {
        let schema = TableSchema {
            table: "Revision",
            index_columns: &["session_id", "generation"],
        };
        let sql = d().render_insert_guarded(&schema, &["session_id"], "generation");
        // id ?1, v ?2, two index values ?3 ?4, one partition value ?5,
        // sequence value ?6 — in exactly this order.
        assert!(sql.contains("SELECT ?1, ?2, unixepoch(), ?3, ?4"));
        assert!(sql.contains("\"session_id\" = ?5"));
        assert!(sql.contains("CAST(\"generation\" AS INTEGER) >= ?6"));
        assert!(sql.contains("WHERE NOT EXISTS"));
    }

    // ── DML: upsert ──────────────────────────────────────────────────────

    #[test]
    fn upsert_no_indexes_uses_canonical_bind_order() {
        let schema = TableSchema {
            table: "User",
            index_columns: &[],
        };
        let sql = d().render_upsert(&schema);
        assert_eq!(
            sql,
            "INSERT INTO \"User\" (id, v, saved_at)\n\
             VALUES (?1, ?2, unixepoch())\n\
             ON CONFLICT(id) DO UPDATE SET\n\
             \x20\x20\x20\x20v = excluded.v,\n\
             \x20\x20\x20\x20saved_at = excluded.saved_at",
        );
    }

    #[test]
    fn upsert_with_indexes_includes_each_column_three_times() {
        let schema = TableSchema {
            table: "Job",
            index_columns: &["status", "priority"],
        };
        let sql = d().render_upsert(&schema);
        // Column list, placeholders, and update assignments must all mention
        // each index column. Regression for off-by-one placeholder indexes.
        assert!(sql.contains("(id, v, saved_at, \"status\", \"priority\")"));
        assert!(sql.contains("VALUES (?1, ?2, unixepoch(), ?3, ?4)"));
        assert!(sql.contains("\"status\" = excluded.\"status\""));
        assert!(sql.contains("\"priority\" = excluded.\"priority\""));
    }

    // ── DML: where clause ────────────────────────────────────────────────

    #[test]
    fn where_clause_empty_returns_empty_string() {
        let (sql, binds) = d().render_where_clause(&[]);
        assert_eq!(sql, "");
        assert!(binds.is_empty());
    }

    #[test]
    fn where_clause_single_eq() {
        let (sql, binds) =
            d().render_where_clause(&[Filter::Eq("status".into(), "pending".into())]);
        assert_eq!(sql, "\"status\" = ?1");
        assert_eq!(binds, vec!["pending".to_string()]);
    }

    #[test]
    fn where_clause_in_expands_placeholders_in_order() {
        let (sql, binds) = d().render_where_clause(&[Filter::In(
            "status".into(),
            vec!["a".into(), "b".into(), "c".into()],
        )]);
        assert_eq!(sql, "\"status\" IN (?1, ?2, ?3)");
        assert_eq!(binds, vec!["a", "b", "c"]);
    }

    #[test]
    fn where_clause_combines_eq_and_in_with_sequential_placeholders() {
        let (sql, binds) = d().render_where_clause(&[
            Filter::Eq("kind".into(), "task".into()),
            Filter::In("status".into(), vec!["pending".into(), "running".into()]),
        ]);
        assert_eq!(sql, "\"kind\" = ?1 AND \"status\" IN (?2, ?3)");
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
    fn select_with_eq_filter() {
        let req = QueryRequest {
            table: "Job",
            filters: vec![Filter::Eq("status".into(), "pending".into())],
            order_by: vec![],
            limit: None,
        };
        let r = d().render_select(&req, "SELECT v");
        assert_eq!(r.sql, "SELECT v FROM \"Job\" WHERE \"status\" = ?1");
        assert_eq!(r.binds, vec!["pending"]);
    }

    #[test]
    fn select_with_in_filter_expands_placeholders() {
        let req = QueryRequest {
            table: "Job",
            filters: vec![Filter::In("status".into(), vec!["a".into(), "b".into()])],
            order_by: vec![],
            limit: None,
        };
        let r = d().render_select(&req, "SELECT v");
        assert_eq!(r.sql, "SELECT v FROM \"Job\" WHERE \"status\" IN (?1, ?2)");
        assert_eq!(r.binds, vec!["a", "b"]);
    }

    #[test]
    fn select_with_order_by_and_limit_appends_in_order() {
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

    #[test]
    fn select_with_count_clause_uses_provided_projection() {
        let req = QueryRequest {
            table: "Job",
            filters: vec![Filter::Eq("status".into(), "pending".into())],
            order_by: vec![],
            limit: None,
        };
        let r = d().render_select(&req, "SELECT COUNT(*)");
        assert_eq!(r.sql, "SELECT COUNT(*) FROM \"Job\" WHERE \"status\" = ?1");
    }

    // ── contract: every public dialect method produces non-empty SQL ──────

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
