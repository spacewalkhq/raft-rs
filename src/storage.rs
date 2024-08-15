// Organization: SpacewalkHq
// License: MIT License

use std::io::{self, Cursor, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use hex;
use sha2::{Digest, Sha256};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::error::StorageError::CorruptFile;
use crate::error::{Error, Result};
use crate::server::LogEntry;

const MAX_FILE_SIZE: u64 = 1_000_000;
pub const CHECKSUM_LEN: usize = 64;

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
    file: Arc<Mutex<File>>,
}

impl LocalStorage {
    pub async fn new(path: String) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.clone())
            .await
            .unwrap();

        LocalStorage {
            path: path.into(),
            file: Arc::new(Mutex::new(file)),
        }
    }

    pub async fn new_from_path(path: &Path) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .await
            .unwrap();

        LocalStorage {
            path: path.into(),
            file: Arc::new(Mutex::new(file)),
        }
    }

    pub async fn check_storage(&self) -> io::Result<()> {
        if let Some(parent_path) = self.path.parent() {
            fs::create_dir_all(parent_path).await?;
        }

        if !self.path.exists() {
            fs::File::create(&self.path).await?;
        }

        Ok(())
    }

    /// Asynchronously stores the provided data along with its checksum into a file.
    ///
    /// # Arguments
    /// * `data` - A slice of bytes representing the data to be stored.
    async fn store_async(&self, data: &[u8]) -> Result<()> {
        let checksum = calculate_checksum(data);
        let data_with_checksum = [data, checksum.as_slice()].concat();

        let file = Arc::clone(&self.file);
        let mut locked_file = file.lock().await;

        locked_file.seek(SeekFrom::End(0)).await.unwrap();

        locked_file
            .write_all(&data_with_checksum)
            .await
            .map_err(Error::Io)?;

        // Attempts to sync all OS-internal metadata to disk.
        locked_file.sync_all().await.map_err(Error::Io)?;

        Ok(())
    }

    /// Asynchronously retrieves all data from the file.
    async fn retrieve_async(&self) -> Result<Vec<u8>> {
        let file = Arc::clone(&self.file);
        let mut locked_file = file.lock().await;
        locked_file.seek(SeekFrom::Start(0)).await.unwrap();

        let mut buffer = Vec::new();
        locked_file
            .read_to_end(&mut buffer)
            .await
            .map_err(Error::Io)?;

        Ok(buffer)
    }

    async fn delete_async(&self) -> Result<()> {
        fs::remove_file(&self.path).await.map_err(Error::Io)?;
        Ok(())
    }

    async fn compaction_async(&self) -> Result<()> {
        // If file size is greater than 1MB, then compact it
        let file = Arc::clone(&self.file);
        let locked_file = file.lock().await;
        let metadata = locked_file.metadata().await.map_err(Error::Io)?;
        if metadata.len() > MAX_FILE_SIZE {
            self.delete_async().await?;
        }
        Ok(())
    }

    async fn is_file_size_exceeded(&self) -> Result<()> {
        let file = Arc::clone(&self.file);
        let locked_file = file.lock().await;

        let md = locked_file.metadata().await.map_err(Error::Io)?;
        if md.len() > MAX_FILE_SIZE {
            return Err(Error::Store(CorruptFile));
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
        self.is_file_size_exceeded().await.unwrap();

        let disk_data = self.retrieve().await?;
        let log_entry_size = std::mem::size_of::<LogEntry>();

        if disk_data.len() % (log_entry_size + CHECKSUM_LEN) != 0 {
            return Err(Error::Store(CorruptFile));
        }

        let mut cursor = Cursor::new(&disk_data);
        loop {
            let mut bytes_data = vec![0u8; log_entry_size];
            if let Err(err) = cursor.read_exact(&mut bytes_data).await {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                } else {
                    return Err(Error::Io(err));
                }
            }

            let byte_data_checksum = calculate_checksum(&bytes_data);

            let mut checksum = [0u8; CHECKSUM_LEN];
            if let Err(err) = cursor.read_exact(&mut checksum).await {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                } else {
                    return Err(Error::Io(err));
                }
            }

            if byte_data_checksum.ne(&checksum) {
                return Err(Error::Store(CorruptFile));
            }
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

#[cfg(test)]
mod tests {

    use std::io::{Cursor, SeekFrom};

    use tempfile::NamedTempFile;
    use tokio::{
        fs::OpenOptions,
        io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    };

    use crate::{
        server::{LogCommand, LogEntry},
        storage::{calculate_checksum, LocalStorage, Storage, CHECKSUM_LEN},
    };

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
        let tmp_file = NamedTempFile::new().unwrap();
        let storage: Box<dyn Storage> =
            Box::new(LocalStorage::new_from_path(tmp_file.path()).await);

        let payload_data = "Some data to test raft".as_bytes();
        let store_result = storage.store(payload_data).await;
        assert!(store_result.is_ok());

        let buffer = storage.retrieve().await.unwrap();

        // Verify the length of the stored data (original data + checksum).
        assert_eq!(
            payload_data.len() + CHECKSUM_LEN,
            buffer.len(),
            "Stored data length mismatch"
        );

        let stored_data = &buffer[..buffer.len() - CHECKSUM_LEN];
        // Verify the original data matches the input data.
        assert_eq!(payload_data, stored_data, "Stored data mismatch");
    }

    #[tokio::test]
    async fn test_delete() {
        let tmp_file = NamedTempFile::new().unwrap();
        let storage: Box<dyn Storage> =
            Box::new(LocalStorage::new_from_path(tmp_file.path()).await);

        let delete_result = storage.delete().await;
        assert!(delete_result.is_ok());
        assert!(!tmp_file.path().exists());
    }

    #[tokio::test]
    async fn test_compaction_file_lt_max_file_size() {
        let tmp_file = NamedTempFile::new().unwrap();
        let storage: Box<dyn Storage> =
            Box::new(LocalStorage::new_from_path(tmp_file.path()).await);
        let mock_data = vec![0u8; 1_000_000 /*1 MB*/  - 500];

        let store_result = storage.store(&mock_data).await;
        assert!(store_result.is_ok());

        let compaction_result = storage.compaction().await;
        assert!(compaction_result.is_ok());

        assert!(tmp_file.path().exists());
    }

    #[tokio::test]
    async fn test_compaction_file_gt_max_file_size() {
        let tmp_file = NamedTempFile::new().unwrap();
        let storage: Box<dyn Storage> =
            Box::new(LocalStorage::new_from_path(tmp_file.path()).await);
        let mock_data = vec![0u8; 1_000_000 /*1 MB*/];

        let store_result = storage.store(&mock_data).await;
        assert!(store_result.is_ok());

        let compaction_result = storage.compaction().await;
        assert!(compaction_result.is_ok());

        assert!(!tmp_file.path().exists());
    }

    #[tokio::test]
    async fn test_retrieve() {
        let tmp_file = NamedTempFile::new().unwrap();
        let storage: Box<dyn Storage> =
            Box::new(LocalStorage::new_from_path(tmp_file.path()).await);
        let log_entry_size = std::mem::size_of::<LogEntry>();

        // Insert the first data first
        let entry1 = LogEntry {
            leader_id: 1,
            server_id: 1,
            term: 1,
            command: LogCommand::Set,
            data: 1,
        };
        let serialize_data = bincode::serialize(&entry1).unwrap();
        storage.store(&serialize_data).await.unwrap();
        let disk_data = storage.retrieve().await.unwrap();
        let log_entry_bytes = &disk_data[0..log_entry_size];
        let disk_entry: LogEntry = bincode::deserialize(log_entry_bytes).unwrap();
        assert_eq!(entry1, disk_entry);

        // Then insert the second data
        let entry2 = LogEntry {
            leader_id: 2,
            server_id: 2,
            term: 2,
            command: LogCommand::Set,
            data: 2,
        };
        let serialize_data = bincode::serialize(&entry2).unwrap();
        storage.store(&serialize_data).await.unwrap();
        let disk_data = storage.retrieve().await.unwrap();

        // Try to read two pieces of data and sit down to compare
        let mut log_entrys = vec![];
        let mut cursor = Cursor::new(&disk_data);
        loop {
            let mut bytes_data = vec![0u8; log_entry_size];
            if cursor.read_exact(&mut bytes_data).await.is_err() {
                break;
            }
            let struct_data: LogEntry = bincode::deserialize(&bytes_data).unwrap();

            let mut checksum = [0u8; CHECKSUM_LEN];
            if cursor.read_exact(&mut checksum).await.is_err() {
                break;
            }

            log_entrys.push(struct_data);
        }

        assert_eq!(vec![entry1, entry2], log_entrys);
    }

    #[tokio::test]
    async fn test_turned_malicious_file_corrupted() {
        let tmp_file = NamedTempFile::new().unwrap();
        let storage: Box<dyn Storage> =
            Box::new(LocalStorage::new_from_path(tmp_file.path()).await);

        // Try to write the data once
        let entry1 = LogEntry {
            leader_id: 1,
            server_id: 1,
            term: 1,
            command: LogCommand::Set,
            data: 1,
        };
        let serialize_data = bincode::serialize(&entry1).unwrap();
        let store_result = storage.store(&serialize_data).await;
        assert!(store_result.is_ok());

        // We will go to simulate that the data is corrupted and does not conform to the original format
        // [(LogEntry, checksum), ...]
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(tmp_file.path())
            .await
            .unwrap();
        file.seek(SeekFrom::Start(0)).await.unwrap();

        file.write_all("Raft".as_bytes()).await.unwrap();
        file.seek(SeekFrom::Start(0)).await.unwrap();
        let mut buffer = vec![];
        file.read_to_end(&mut buffer).await.unwrap();
        file.sync_all().await.unwrap();

        let storage: Box<dyn Storage> =
            Box::new(LocalStorage::new_from_path(tmp_file.path()).await);
        let result = storage.turned_malicious().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_turned_malicious_happy_case() {
        let tmp_file = NamedTempFile::new().unwrap();
        let storage: Box<dyn Storage> =
            Box::new(LocalStorage::new_from_path(tmp_file.path()).await);

        // Try to write the data once
        let entry1 = LogEntry {
            leader_id: 1,
            server_id: 1,
            term: 1,
            command: LogCommand::Set,
            data: 1,
        };
        let serialize_data = bincode::serialize(&entry1).unwrap();
        let store_result = storage.store(&serialize_data).await;
        assert!(store_result.is_ok());

        let result = storage.turned_malicious().await;
        assert!(result.is_ok());
    }
}
