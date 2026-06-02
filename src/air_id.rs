//! Typed UUID wrapper — every saved value gets a unique `AirId<T>`.

use std::marker::PhantomData;

/// A type-tagged UUIDv7 id. The tag `T` is zero-sized; the id carries no runtime
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
    /// Generate a fresh UUIDv7.
    pub fn new() -> Self {
        Self {
            uuid: uuid::Uuid::now_v7(),
            _tag: PhantomData,
        }
    }

    /// String form for display/logging (`uuid::Uuid::to_string`).
    pub fn to_string_id(&self) -> String {
        self.uuid.to_string()
    }

    /// 16-byte binary form stored in SQLite.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.uuid.as_bytes().to_vec()
    }
}

impl<T> Default for AirId<T> {
    fn default() -> Self {
        Self::new()
    }
}
