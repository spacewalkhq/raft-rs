// Organization: SpacewalkHq
// License: MIT License

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use hex;
use sha2::{Digest, Sha256};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::{Error, Result, StorageError};
use crate::error::StorageError::MaliciousFile;

const MAX_FILE_SIZE: u64 = 1_000_000;
const CHECKSUM_LEN: usize = 64;

#[async_trait]
pub trait Storage {
    async fn store(&self, data: &[u8]) -> Result<()>;
    async fn retrieve(&self) -> Result<Vec<u8>>;
    async fn compaction(&self) -> Result<()>;
    async fn delete(&self) -> Result<()>;
    async fn turned_malicious(&self) -> Result<()>;
}

#[derive(Clone)]
pub struct LocalStorage {
    path: PathBuf,
}

impl LocalStorage {
    pub fn new(path: String) -> Self {
        LocalStorage { path: path.into() }
    }

    pub fn new_from_path(path: &Path) -> Self {
        LocalStorage {
            path: PathBuf::from(path),
        }
    }

    async fn store_async(&self, data: &[u8]) -> Result<()> {
        let checksum = calculate_checksum(data);
        let data_with_checksum = [data, checksum.as_slice()].concat();

        let mut file = File::create(&self.path)
            .await
            .map_err(Error::Io)?;
        file.write_all(&data_with_checksum)
            .await
            .map_err(Error::Io)?;
        file.flush().await.map_err(Error::Io)?;
        Ok(())
    }

    async fn retrieve_async(&self) -> Result<Vec<u8>> {
        let mut file = File::open(&self.path)
            .await
            .map_err(Error::Io)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .await
            .map_err(Error::Io)?;

        if buffer.is_empty() {
            return Err(Error::Store(StorageError::EmptyFile));
        }

        if buffer.len() < CHECKSUM_LEN {
            return Err(Error::Store(StorageError::MaliciousFile));
        }

        let data = &buffer[..buffer.len() - 64];
        let stored_checksum = retrieve_checksum(&buffer);
        let calculated_checksum = calculate_checksum(data);

        if stored_checksum != calculated_checksum {
            return Err(Error::Store(StorageError::DataIntegrityError));
        }

        Ok(data.to_vec())
    }

    async fn delete_async(&self) -> Result<()> {
        fs::remove_file(&self.path)
            .await
            .map_err(Error::Io)?;
        Ok(())
    }

    async fn compaction_async(&self) -> Result<()> {
        // If file size is greater than 1MB, then compact it
        let metadata = fs::metadata(&self.path)
            .await
            .map_err(Error::Io)?;
        if metadata.len() > MAX_FILE_SIZE {
            self.delete_async().await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Storage for LocalStorage {
    async fn store(&self, data: &[u8]) -> Result<()> {
        self.store_async(data).await
    }

    async fn retrieve(&self) -> Result<Vec<u8>> {
        self.retrieve_async().await
    }

    async fn compaction(&self) -> Result<()> {
        self.compaction_async().await
    }

    async fn delete(&self) -> Result<()> {
        self.delete_async().await
    }

    async fn turned_malicious(&self) -> Result<()> {
        // Check if the file is tampered with
        self.retrieve().await?;
        let metadata = fs::metadata(&self.path)
            .await
            .map_err(Error::Io)?;

        if metadata.len() > MAX_FILE_SIZE {
            return Err(Error::Store(MaliciousFile));
        }
        Ok(())
    }
}

/// This function computes the SHA-256 hash of the given byte slice and returns
/// a fixed-size array of bytes (`[u8; CHECKSUM_LEN]`).
/// The resulting checksum is encoded in hexadecimal format.
fn calculate_checksum(data: &[u8]) -> [u8; CHECKSUM_LEN] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    let mut checksum = [0u8; 64];
    checksum.copy_from_slice(hex::encode(result).as_bytes());
    checksum
}

/// Helper function to extract the checksum from the end of a given byte slice.
/// It assumes that the checksum is of a fixed length `CHECKSUM_LEN` and is located
/// at the end of the provided data slice.
///
/// This function will panic if the length of the provided data slice is less than `CHECKSUM_LEN`.
fn retrieve_checksum(data: &[u8]) -> [u8; CHECKSUM_LEN] {
    assert!(data.len() >= CHECKSUM_LEN);
    let mut op = [0; 64];
    op.copy_from_slice(&data[data.len() - CHECKSUM_LEN..]);
    op
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom, Write};

    use tempfile::NamedTempFile;

    use crate::storage::{
        calculate_checksum, CHECKSUM_LEN, LocalStorage, retrieve_checksum, Storage,
    };

    #[test]
    fn test_retrieve_checksum() {
        let data_str = "Some data followed by a checksum".as_bytes();
        let calculated_checksum = calculate_checksum(data_str);

        let data = [data_str, calculated_checksum.as_slice()].concat();

        let retrieved_checksum = retrieve_checksum(&data);
        assert_eq!(calculated_checksum, retrieved_checksum);
    }

    #[tokio::test]
    async fn test_store_async() {
        let mut tmp_file = NamedTempFile::new().unwrap();
        let storage: Box<dyn Storage> = Box::new(LocalStorage::new_from_path(tmp_file.path()));
        let payload_data = "Some data to test raft".as_bytes();
        let store_result = storage.store(payload_data).await;
        assert!(store_result.is_ok());

        tmp_file.as_file().sync_all().unwrap();

        let mut buffer = vec![];
        tmp_file.read_to_end(&mut buffer).unwrap();

        assert_eq!(payload_data.len() + CHECKSUM_LEN, buffer.len());

        let data = &buffer[..buffer.len() - CHECKSUM_LEN];
        assert_eq!(payload_data, data);
    }

    #[tokio::test]
    async fn test_delete() {
        let tmp_file = NamedTempFile::new().unwrap();
        let storage: Box<dyn Storage> = Box::new(LocalStorage::new_from_path(tmp_file.path()));
        let delete_result = storage.delete().await;
        assert!(delete_result.is_ok());
        assert!(!tmp_file.path().exists());
    }

    #[tokio::test]
    async fn test_compaction_file_lt_max_file_size() {
        let tmp_file = NamedTempFile::new().unwrap();
        let storage: Box<dyn Storage> = Box::new(LocalStorage::new_from_path(tmp_file.path()));
        let mock_data = vec![0u8; 1_000_000 /*1 MB*/  - 500];
        let store_result = storage.store(&mock_data).await;
        assert!(store_result.is_ok());

        tmp_file.as_file().sync_all().unwrap();

        let compaction_result = storage.compaction().await;
        assert!(compaction_result.is_ok());

        assert!(tmp_file.path().exists());
    }

    #[tokio::test]
    async fn test_compaction_file_gt_max_file_size() {
        let tmp_file = NamedTempFile::new().unwrap();
        let storage: Box<dyn Storage> = Box::new(LocalStorage::new_from_path(tmp_file.path()));
        let mock_data = vec![0u8; 1_000_000 /*1 MB*/];
        let store_result = storage.store(&mock_data).await;
        assert!(store_result.is_ok());

        tmp_file.as_file().sync_all().unwrap();

        let compaction_result = storage.compaction().await;
        assert!(compaction_result.is_ok());

        assert!(!tmp_file.path().exists());
    }

    #[tokio::test]
    async fn test_retrieve() {
        let tmp_file = NamedTempFile::new().unwrap();
        let storage: Box<dyn Storage> = Box::new(LocalStorage::new_from_path(tmp_file.path()));
        let test_data = "Some mocked data".as_bytes();

        storage.store(test_data).await.unwrap();

        let retrieved_result = storage.retrieve().await;
        assert!(retrieved_result.is_ok());

        assert_eq!(test_data, retrieved_result.unwrap());
    }

    #[tokio::test]
    async fn test_turned_malicious_file_corrupted() {
        let mut tmp_file = NamedTempFile::new().unwrap();
        let storage: Box<dyn Storage> = Box::new(LocalStorage::new_from_path(tmp_file.path()));
        storage.store("Java is awesome".as_bytes()).await.unwrap();

        tmp_file.as_file().sync_all().unwrap();

        // corrupt the file
        tmp_file.seek(SeekFrom::Start(0)).unwrap();
        tmp_file.write_all("Raft".as_bytes()).unwrap();

        tmp_file.as_file().sync_all().unwrap();

        let result = storage.turned_malicious().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_turned_malicious_happy_case() {
        let tmp_file = NamedTempFile::new().unwrap();
        let storage: Box<dyn Storage> = Box::new(LocalStorage::new_from_path(tmp_file.path()));
        storage.store("Java is awesome".as_bytes()).await.unwrap();

        tmp_file.as_file().sync_all().unwrap();

        let result = storage.turned_malicious().await;
        assert!(result.is_ok());
    }
}
