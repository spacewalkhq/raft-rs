use thiserror::Error;

/// wrapper around library error
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("storage error {0}")]
    Store(#[from] StorageError),
    #[error("network error {0}")]
    Network(#[from] NetworkError),
    #[error("file error {0}")]
    FileError(#[from] FileError),
    /// Some other error occurred.
    #[error("unknown error {0}")]
    Unknown(#[from] Box<dyn std::error::Error + Sync + Send>),
}

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("opening connection failed")]
    OpenConnectionError,
    #[error("closing connection failed")]
    CloseConnectionError,
    #[error("listening on port = {0} failed")]
    ListenError(u8),
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("file is empty")]
    EmptyFile,
    #[error("File is potentially malicious")]
    MaliciousFile,
    #[error("error data integrity check failed!")]
    DataIntegrityError,
    #[error("storing log failed")]
    StoreError,
    #[error("log compaction failed")]
    CompactionError,
    #[error("log retrieval failed")]
    RetrieveError,
}

#[derive(Error, Debug)]
pub enum FileError {
    #[error("write all operation failed")]
    WriteError,
    #[error("flush operation failed")]
    FlushError,
    #[error("creating file failed")]
    CreateError,
    #[error("opening file failed")]
    OpenError,
    #[error("reading file failed")]
    ReadError,
    #[error("removing file failed")]
    RemoveFileError,
    #[error("reading file metadata failed")]
    MetaDataError
}