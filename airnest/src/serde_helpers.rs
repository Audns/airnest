//! Serde helpers for `#[stored(json)]` fields.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Serialize a value to JSON string before passing to the underlying serializer.
pub fn json_ser<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: Serialize,
    S: Serializer,
{
    let s = serde_json::to_string(value).map_err(serde::ser::Error::custom)?;
    serializer.serialize_str(&s)
}

/// Deserialize a JSON string into a value.
pub fn json_de<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: for<'a> Deserialize<'a>,
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    serde_json::from_str(&s).map_err(serde::de::Error::custom)
}

/// Serialize a value to a JSON string for index columns.
pub fn json_string<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap_or_default()
}
