//! `ToIndexValue` — converts a field into a string stored as a real SQLite column.
//!
//! Any type that implements `Display` automatically gets this trait, so most
//! primitive types (integers, bools, strings, UUIDs) work out of the box.
//! For custom types, implement `Display` or implement `ToIndexValue` directly.

/// Convert a field value into a SQLite index column value (stored as TEXT).
///
/// Implemented automatically for any `T: Display`.
pub trait ToIndexValue {
    fn to_index_value(&self) -> String;
}

impl<T: std::fmt::Display> ToIndexValue for T {
    fn to_index_value(&self) -> String {
        self.to_string()
    }
}
