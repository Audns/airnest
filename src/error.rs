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

    #[error("bad id: {0}")]
    BadId(String),
}
