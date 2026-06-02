//! Convert something into an `AirId<T>` — enables ergonomic `store.load(&value)`.

use crate::{AirId, Persistent};

/// Anything that can yield an `AirId<T>` for use with [`Store`](crate::Store).
///
/// Implemented for:
/// - `AirId<T>` itself (identity)
/// - `&T` where `T: Persistent` (reads the embedded id)
pub trait IntoAirId<T> {
    fn into_air_id(self) -> AirId<T>;
}

impl<T> IntoAirId<T> for AirId<T> {
    fn into_air_id(self) -> AirId<T> {
        self
    }
}

impl<T: Persistent> IntoAirId<T> for &T {
    fn into_air_id(self) -> AirId<T> {
        self.id()
    }
}
