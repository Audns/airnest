//! The `Persistent` trait — implemented automatically by `#[persistent]`.

use serde::{Serialize, de::DeserializeOwned};

use crate::AirId;

/// Marks a struct as persistable via `Store`.
///
/// Do not implement this manually — use `#[persistent]`.
///
/// # What the macro generates
///
/// ```ignore
/// #[persistent]
/// pub struct User {
///     pub name: String,
/// }
/// // expands to:
/// pub struct User {
///     pub id: AirId<User>,
///     pub name: String,
/// }
/// impl User {
///     pub fn new(name: String) -> Self { ... }
///     pub fn id(&self) -> AirId<Self> { self.id }
/// }
/// impl Persistent for User { ... }
/// ```
///
/// # Architecture patterns
///
/// In large codebases, `#[persistent]` should be used sparingly — only on
/// aggregates and domain entities that represent state worth saving. Child
/// structs nested inside a persistent root should be plain `Serialize +
/// Deserialize` values.
///
/// See the `airnest` crate README for a full guide on persistence boundaries,
/// aggregate-root modelling, repository layers, and domain-module conventions.
pub trait Persistent: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// The auto-generated `UUIDv7` id embedded in every persisted value.
    fn id(&self) -> AirId<Self>;

    /// `SQLite` table name — defaults to the struct name.
    const TABLE: &'static str;

    /// Names of extra columns stored alongside the blob for indexed queries.
    /// Populated by `#[persistent(index(field_a, field_b))]`.
    fn index_columns() -> &'static [&'static str];

    /// Current values for each index column, in the same order as `index_columns()`.
    fn index_values(&self) -> Vec<String>;

    /// Uniqueness groups over index columns, one entry per `unique(...)`
    /// group in `#[persistent]`. Each group becomes a `UNIQUE` index, so
    /// the database — not application code — rejects duplicates.
    ///
    /// Defaults to no constraints, keeping manual implementations (discouraged)
    /// compiling.
    fn unique_constraints() -> &'static [&'static [&'static str]] {
        &[]
    }

    /// Read a blob the current shape cannot decode: a row an older
    /// binary wrote before the struct changed. Populated by
    /// `#[persistent(upgrade = path)]`, where `path` is a
    /// `fn(&Legacy<'_>) -> Option<Self>`.
    ///
    /// Every read path (load, query, scan, transactions) falls back to
    /// this hook, so a migration lives beside the row type and never in
    /// raw SQL. Defaults to `None`: an undecodable row stays an error.
    fn upgrade(raw: &crate::codec::Legacy<'_>) -> Option<Self> {
        let _ = raw;
        None
    }
}
