//! airnest 🪹 — Silent, async SQLite (and redb) persistence for Rust.
//!
//! Derive `#[persistent]` once, store forever. See the crate README for
//! architecture patterns for large codebases (persistence boundaries,
//! aggregate-root modelling, repository layers, and more).

pub mod air_id;
pub mod backend;
pub mod codec;
pub mod error;
pub mod index;
pub mod into_air_id;
pub mod persistent;
pub mod serde_helpers;
pub mod store;

pub use air_id::AirId;
pub use airnest_macros::persistent;
pub use backend::Order;
pub use codec::Codec;
pub use error::StoreError;
pub use index::ToIndexValue;
pub use into_air_id::IntoAirId;
pub use persistent::Persistent;
pub use serde_helpers::{json_de, json_ser, json_string};
pub use store::{
    BackendKind, InitMany, Query, ReplaceBuilder, Store, StoreBatch, UpsertBuilder,
    UpsertModifyBuilder,
};

#[cfg(feature = "postcard")]
pub use codec::Codec as PostcardCodec;
