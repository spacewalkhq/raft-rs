// Author: Vipul Vaibhaw
// Organization: SpacewalkHq
// License: MIT License

use std::error::Error;
use std::path::PathBuf;

use async_trait::async_trait;
use hex;
use sha2::{Digest, Sha256};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_FILE_SIZE: u64 = 1_000_000;
const CHECKSUM_LEN: usize = 64;

#[async_trait]
pub trait Storage {
    async fn store(&self, data: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn retrieve(&self) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>>;
    async fn compaction(&self) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn delete(&self) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn turned_malicious(&self) -> Result<(), Box<dyn Error + Send + Sync>>;
}

#[derive(Clone)]
pub struct LocalStorage {
    path: PathBuf,
}

impl LocalStorage {
    pub fn new(path: String) -> Self {
        LocalStorage { path: path.into() }
    }

    async fn store_async(&self, data: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
        let checksum = Self::calculate_checksum(data);
        let data_with_checksum = [data, checksum.as_slice()].concat();

        let mut file = File::create(&self.path).await?;
        file.write_all(&data_with_checksum).await?;
        Ok(())
    }

    async fn retrieve_async(&self) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let mut file = File::open(&self.path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        if buffer.len() < CHECKSUM_LEN {
            return Err("File is potentially malicious".into());
        }

        let data = &buffer[..buffer.len() - 64];
        let stored_checksum = Self::retrieve_checksum(&buffer);
        let calculated_checksum = Self::calculate_checksum(data);

        if stored_checksum != calculated_checksum {
            return Err("Data integrity check failed!".into());
        }

        Ok(data.to_vec())
    }

    async fn delete_async(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        fs::remove_file(&self.path).await?;
        Ok(())
    }

    async fn compaction_async(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // If file size is greater than 1MB, then compact it
        let metadata = fs::metadata(&self.path).await?;
        if metadata.len() > MAX_FILE_SIZE {
            self.delete_async().await?;
        }
        Ok(())
    }

    fn calculate_checksum(data: &[u8]) -> [u8; CHECKSUM_LEN] {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let result = hasher.finalize();
        let mut checksum = [0u8; 64];
        checksum.copy_from_slice(hex::encode(result).as_bytes());
        checksum
    }

    fn retrieve_checksum(data: &[u8]) -> [u8; CHECKSUM_LEN] {
        assert!(data.len() >= CHECKSUM_LEN);
        let mut op = [0; 64];
        op.copy_from_slice(&data[data.len() - CHECKSUM_LEN..]);
        op
    }
}

#[async_trait]
impl Storage for LocalStorage {
    async fn store(&self, data: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.store_async(data).await
    }

    async fn retrieve(&self) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        self.retrieve_async().await
    }

    async fn compaction(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.compaction_async().await
    }

    async fn delete(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.delete_async().await
    }

    async fn turned_malicious(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Check if the file is tampered with
        self.retrieve().await?;
        let metadata = fs::metadata(&self.path).await?;

        if metadata.len() > MAX_FILE_SIZE {
            return Err("File is potentially malicious".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_calculate_checksum() {
        unimplemented!()
    }

    #[test]
    fn test_retrieve_checksum() {
        unimplemented!()
    }
}
