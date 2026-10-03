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

/// A stored blob the current shape could not decode, handed to
/// [`Persistent::upgrade`](crate::Persistent::upgrade) so a type can read
/// rows written by an older binary.
///
/// Positional codecs (bitcode, postcard) carry no schema: a blob written
/// before a field was appended fails to decode under the longer struct.
/// The upgrade hook decodes it as the historical shape instead and maps
/// it forward. The bytes stay private; [`Legacy::decode`] uses the
/// store's own codec, so callers never name the encoding.
pub struct Legacy<'a> {
    bytes: &'a [u8],
    codec: Codec,
}

impl Legacy<'_> {
    /// Decode the blob as `T` (a historical row shape) with the store's
    /// codec; `None` when it is not that shape either.
    #[must_use]
    pub fn decode<T: DeserializeOwned>(&self) -> Option<T> {
        self.codec.decode(self.bytes).ok()
    }
}

/// Decode one stored row: the current shape first, then the type's
/// [`upgrade`](crate::Persistent::upgrade) hook. When both fail, the
/// current-shape error is reported.
pub(crate) fn decode_row<T: crate::Persistent>(
    codec: Codec,
    bytes: &[u8],
) -> Result<T, StoreError> {
    match codec.decode::<T>(bytes) {
        Ok(value) => Ok(value),
        Err(err) => T::upgrade(&Legacy { bytes, codec }).ok_or(err),
    }
}
