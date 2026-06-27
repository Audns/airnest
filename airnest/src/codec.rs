//! Codec abstraction for serialization backends.

use serde::{Serialize, de::DeserializeOwned};

use crate::error::StoreError;

/// Serialization backend selection.
#[derive(Clone, Copy, Debug, Default)]
pub enum Codec {
    /// Default bitcode codec (compact, fast).
    #[default]
    Bitcode,
    /// JSON codec (human-readable, good for debugging).
    Json,
    /// Postcard codec (compact, no-std friendly).
    #[cfg(feature = "postcard")]
    Postcard,
}

impl Codec {
    /// Serialize a value to bytes.
    pub fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, StoreError> {
        match self {
            Codec::Bitcode => bitcode::serialize(value).map_err(StoreError::Encode),
            Codec::Json => serde_json::to_vec(value).map_err(|e| StoreError::Codec(e.to_string())),
            #[cfg(feature = "postcard")]
            Codec::Postcard => {
                postcard::to_stdvec(value).map_err(|e| StoreError::Codec(e.to_string()))
            }
        }
    }

    /// Deserialize a value from bytes.
    pub fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, StoreError> {
        match self {
            Codec::Bitcode => bitcode::deserialize(bytes).map_err(StoreError::Encode),
            Codec::Json => {
                serde_json::from_slice(bytes).map_err(|e| StoreError::Codec(e.to_string()))
            }
            #[cfg(feature = "postcard")]
            Codec::Postcard => {
                postcard::from_bytes(bytes).map_err(|e| StoreError::Codec(e.to_string()))
            }
        }
    }
}
