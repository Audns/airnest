//! Typed UUID wrapper — every saved value gets a unique `AirId<T>`.

use std::marker::PhantomData;

/// A type-tagged `UUIDv7` id. The tag `T` is zero-sized; the id carries no runtime
/// overhead beyond a [`uuid::Uuid`].
///
/// Created by [`Store::save`](crate::Store::save) and used with
/// [`Store::load`](crate::Store::load), [`Store::delete`](crate::Store::delete), etc.
#[derive(Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct AirId<T> {
    pub(crate) uuid: uuid::Uuid,
    #[serde(skip)]
    _tag: PhantomData<T>,
}

impl<T> Clone for AirId<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for AirId<T> {}

impl<T> AirId<T> {
    /// Generate a fresh `UUIDv7`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            uuid: uuid::Uuid::now_v7(),
            _tag: PhantomData,
        }
    }

    /// String form for display/logging (`uuid::Uuid::to_string`).
    #[must_use]
    pub fn to_string_id(&self) -> String {
        self.uuid.to_string()
    }

    /// 16-byte binary form stored in `SQLite` — stack-allocated, no heap.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; 16] {
        *self.uuid.as_bytes()
    }

    /// Heap-allocated form for APIs that require `Vec<u8>`.
    #[must_use]
    pub fn to_bytes_vec(&self) -> Vec<u8> {
        self.to_bytes().to_vec()
    }

    /// Construct from raw 16 bytes.
    #[must_use]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self {
            uuid: uuid::Uuid::from_bytes(bytes),
            _tag: PhantomData,
        }
    }
}

impl<T> Default for AirId<T> {
    fn default() -> Self {
        Self::new()
    }
}
