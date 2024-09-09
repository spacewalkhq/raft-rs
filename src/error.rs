// organization : SpacewalkHq
// License : MIT License

use std::net::SocketAddr;
use thiserror::Error;

/// wrapper around std library error
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    // Storage layer specific error
    #[error("Storage error {0}")]
    Store(#[from] StorageError),
    // Network layer specific error
    #[error("Network error {0}")]
    Network(#[from] NetworkError),
    // To handle all std lib io error
    #[error("File error {0}")]
    Io(#[from] std::io::Error),
    /// Some other error occurred.
    #[error("unknown error {0}")]
    Unknown(#[from] Box<dyn std::error::Error + Sync + Send>),
    /// To handle all bincode error
    #[error("Bincode error {0}")]
    BincodeError(#[from] bincode::Error),
}

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("Accepting incoming connection failed")]
    AcceptError,
    #[error("Connection is closed")]
    ConnectionClosedError,
    #[error("Connection to {0} failed")]
    ConnectError(SocketAddr),
    #[error("Failed binding to {0}")]
    BindError(SocketAddr),
    #[error("Broadcast failed, errmsg: {0}")]
    BroadcastError(String),
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Path not found")]
    PathNotFound,
    #[error("File is empty")]
    EmptyFile,
    #[error("File is corrupted")]
    CorruptFile,
    #[error("Data integrity check failed!")]
    DataIntegrityError,
    #[error("Storing log failed")]
    StoreError,
    #[error("Log compaction failed")]
    CompactionError,
    #[error("Log retrieval failed")]
    RetrieveError,
    #[error("Reading file metadata failed")]
    MetaDataError,
}
