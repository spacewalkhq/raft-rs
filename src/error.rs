// organization : SpacewalkHq
// License : MIT License

use std::net::SocketAddr;
use thiserror::Error;

/// wrapper around std library error
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Storage error {0}")]
    Store(#[from] StorageError),
    #[error("Network error {0}")]
    Network(#[from] NetworkError),
    #[error("File error {0}")]
    FileError(#[from] FileError),
    /// Some other error occurred.
    #[error("unknown error {0}")]
    Unknown(#[from] Box<dyn std::error::Error + Sync + Send>),
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
    #[error("Broadcast failed")]
    BroadcastError,
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("File is empty")]
    EmptyFile,
    #[error("File is potentially malicious")]
    MaliciousFile,
    #[error("Data integrity check failed!")]
    DataIntegrityError,
    #[error("Storing log failed")]
    StoreError,
    #[error("Log compaction failed")]
    CompactionError,
    #[error("Log retrieval failed")]
    RetrieveError,
}

#[derive(Error, Debug)]
pub enum FileError {
    #[error("Write operation failed")]
    WriteError,
    #[error("Flush operation failed")]
    FlushError,
    #[error("Creating file failed")]
    CreateError,
    #[error("Opening file failed")]
    OpenError,
    #[error("Reading file failed")]
    ReadError,
    #[error("Removing file failed")]
    RemoveFileError,
    #[error("Reading file metadata failed")]
    MetaDataError,
}
