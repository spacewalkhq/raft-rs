// Author: Vipul Vaibhaw
// Organization: SpacewalkHq
// License: MIT License

use std::error::Error;
use std::path::Path;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub trait Storage {
    async fn store(&self, data: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn retrieve(&self) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>>;
    async fn compaction(&self) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn delete(&self) -> Result<(), Box<dyn Error + Send + Sync>>;
}

pub struct LocalStorage {
    path: String,
}

impl LocalStorage {
    pub fn new(path: String) -> Self {
        LocalStorage { path }
    }

    async fn store_async(&self, data: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
        let path = Path::new(&self.path);
        let mut file = File::create(&path).await?;
        file.write_all(data).await?;
        Ok(())
    }

    async fn retrieve_async(&self) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let path = Path::new(&self.path);
        let mut file = File::open(&path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;
        Ok(buffer)
    }

    async fn delete_async(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let path = Path::new(&self.path);
        fs::remove_file(path).await?;
        Ok(())
    }

    async fn compaction_async(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // if file size is greater than 1MB, then compact it
        let path = Path::new(&self.path);
        let metadata = fs::metadata(path).await?;
        if metadata.len() > 1_000_000 {
            self.delete_async().await?;
        }
        Ok(())
    }
}

impl Storage for LocalStorage {
    async fn store(&self, data: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
        let data = data.to_vec();
        let storage = self.clone();
        tokio::spawn(async move {
            storage.store_async(&data).await
        }).await??;
        Ok(())
    }

    async fn retrieve(&self) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let storage = self.clone();
        tokio::spawn(async move {
            storage.retrieve_async().await
        }).await?
    }

    async fn compaction(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let storage = self.clone();
        tokio::spawn(async move {
            storage.compaction_async().await
        }).await??;
        Ok(())
    }

    async fn delete(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let storage = self.clone();
        tokio::spawn(async move {
            storage.delete_async().await
        }).await??;
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
