// Author: Vipul Vaibhaw
// Organization: SpacewalkHq
// License: MIT License

use async_trait::async_trait;
use hex;
use sha2::{Digest, Sha256};
use std::error::Error;
use std::path::Path;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[async_trait]
pub trait Storage {
    async fn store(&self, data: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn retrieve(&self) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>>;
    async fn compaction(&self) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn delete(&self) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn turned_malicious(&self) -> Result<(), Box<dyn Error + Send + Sync>>;
}

pub struct LocalStorage {
    path: String,
}

impl LocalStorage {
    pub fn new(path: String) -> Self {
        LocalStorage { path }
    }

    async fn store_async(&self, data: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
        let checksum = Self::calculate_checksum(data);
        let data_with_checksum = [data, checksum.as_bytes()].concat();

        let path = Path::new(&self.path);
        let mut file = File::create(&path).await?;
        file.write_all(&data_with_checksum).await?;
        Ok(())
    }

    async fn retrieve_async(&self) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let path = Path::new(&self.path);
        let mut file = File::open(&path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        let data = &buffer[..buffer.len() - 64];
        let stored_checksum = String::from_utf8(buffer[buffer.len() - 64..].to_vec())?;
        let calculated_checksum = Self::calculate_checksum(data);

        if stored_checksum != calculated_checksum {
            return Err("Data integrity check failed!".into());
        }

        Ok(data.to_vec())
    }

    async fn delete_async(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let path = Path::new(&self.path);
        fs::remove_file(path).await?;
        Ok(())
    }

    async fn compaction_async(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // If file size is greater than 1MB, then compact it
        let path = Path::new(&self.path);
        let metadata = fs::metadata(path).await?;
        if metadata.len() > 1_000_000 {
            self.delete_async().await?;
        }
        Ok(())
    }

    fn calculate_checksum(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let result = hasher.finalize();
        hex::encode(result)
    }
}

#[async_trait]
impl Storage for LocalStorage {
    async fn store(&self, data: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
        let data = data.to_vec();
        let storage = self.clone();
        tokio::spawn(async move { storage.store_async(&data).await }).await??;
        Ok(())
    }

    async fn retrieve(&self) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let storage = self.clone();
        tokio::spawn(async move { storage.retrieve_async().await }).await?
    }

    async fn compaction(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let storage = self.clone();
        tokio::spawn(async move { storage.compaction_async().await }).await??;
        Ok(())
    }

    async fn delete(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let storage = self.clone();
        tokio::spawn(async move { storage.delete_async().await }).await??;
        Ok(())
    }

    async fn turned_malicious(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Check if the file is tampered with
        let data = self.retrieve().await?;
        let checksum = Self::calculate_checksum(&data);
        let path = Path::new(&self.path);
        let metadata = fs::metadata(path).await?;

        if metadata.len() > 1_000_000 || checksum != Self::calculate_checksum(&data) {
            return Err("File is potentially malicious".into());
        }
        Ok(())
    }
}

impl Clone for LocalStorage {
    fn clone(&self) -> Self {
        LocalStorage {
            path: self.path.clone(),
        }
    }
}
