use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] sqlx::Error),

    #[error("encode/decode: {0}")]
    Encode(#[from] bitcode::Error),

    #[error("codec: {0}")]
    Codec(String),

    #[error("task join: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("mutex poisoned")]
    Poisoned,

    #[cfg(feature = "redb")]
    #[error("redb: {0}")]
    Redb(String),

    #[error("bad id: {0}")]
    BadId(String),

    /// A uniqueness constraint rejected the write. Returned instead of a
    /// backend error when an `INSERT` collides with a declared
    /// `unique(...)` group, so callers can distinguish "already exists"
    /// from "storage failed" without parsing database messages.
    #[error("unique constraint violated: {0}")]
    Conflict(String),
}
