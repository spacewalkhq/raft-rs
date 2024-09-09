use std::path::PathBuf;
use std::time::Duration;
use std::{fmt::Debug, path::Path};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::{fs::OpenOptions, io::AsyncWriteExt, time::Instant};

use crate::error::StorageError::PathNotFound;
use crate::{error::Error, error::Result, server::LogEntry};

#[async_trait]
pub trait StateMachine: Debug + Send + Sync {
    // Retrieve the current term stored in the state machine
    async fn get_term(&self) -> u32;

    // Retrieve the current log index stored in the state machine
    async fn get_index(&self) -> u32;

    // Apply a single log entry to the state machine, updating term, index, and log entries
    async fn apply_log_entry(
        &mut self,
        last_included_term: u32,
        last_included_index: u32,
        log_entry: LogEntry,
    );

    // Apply multiple log entries to the state machine in bulk
    async fn apply_log_entrys(
        &mut self,
        last_included_term: u32,
        last_included_index: u32,
        mut log_entrys: Vec<LogEntry>,
    );

    // Retrieve all log entries currently stored in the state machine
    async fn get_log_entry(&mut self) -> Result<Vec<LogEntry>>;

    // Create a snapshot of the current state to the file system, and clear the log data after
    async fn create_snapshot(&mut self) -> Result<()>;

    // Check if a snapshot is required based on the interval since the last snapshot
    async fn need_create_snapshot(&mut self) -> bool;
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileStateMachine {
    last_included_term: u32,
    last_included_index: u32,
    data: Vec<LogEntry>,

    #[serde(skip)]
    snapshot_path: Option<Box<Path>>,

    /// The time interval between snapshots.
    #[serde(skip)]
    snapshot_interval: Duration,

    /// The time when snapshot generation started
    #[serde(skip)]
    snapshot_start_time: Option<Instant>,

    /// Whether the state machine is currently generating a snapshot
    #[serde(skip)]
    is_snapshotting: bool,

    /// The time when the last snapshot was completed
    #[serde(skip)]
    last_snapshot_complete_time: Option<Instant>,
}

impl FileStateMachine {
    /// Create a new FileStateMachine with initial values
    pub fn new(snapshot_path: &Path, snapshot_interval: Duration) -> Self {
        let current_time = Instant::now();
        Self {
            snapshot_path: Some(PathBuf::from(snapshot_path).into_boxed_path()),
            last_included_term: 0,
            last_included_index: 0,
            data: Vec::new(),
            snapshot_interval,
            snapshot_start_time: Some(current_time),
            is_snapshotting: false,
            last_snapshot_complete_time: Some(current_time),
        }
    }
}

/// Implement the StateMachine trait for FileStateMachine
/// Generate snapshots based on time intervals as I record start, end time
/// The data in memory is cleared after generating a snapshot, to save memory
#[async_trait]
impl StateMachine for FileStateMachine {
    async fn get_term(&self) -> u32 {
        self.last_included_term
    }

    async fn get_index(&self) -> u32 {
        self.last_included_index
    }

    async fn apply_log_entry(
        &mut self,
        last_included_term: u32,
        last_included_index: u32,
        log_entry: LogEntry,
    ) {
        self.last_included_term = last_included_term;
        self.last_included_index = last_included_index;
        self.data.push(log_entry);
    }

    async fn apply_log_entrys(
        &mut self,
        last_included_term: u32,
        last_included_index: u32,
        mut log_entrys: Vec<LogEntry>,
    ) {
        self.last_included_term = last_included_term;
        self.last_included_index = last_included_index;
        self.data.append(&mut log_entrys);
    }

    async fn create_snapshot(&mut self) -> Result<()> {
        let snapshot_path = if let Some(ref path) = self.snapshot_path {
            path
        } else {
            return Err(Error::Store(PathNotFound));
        };

        self.snapshot_start_time = Some(Instant::now());
        self.is_snapshotting = true;

        // Step 1: Read the existing snapshot from file if it exists
        let mut existing_fsm = FileStateMachine::new(snapshot_path, Duration::from_secs(0));
        if fs::metadata(snapshot_path).await.is_ok() {
            let mut file = OpenOptions::new().read(true).open(snapshot_path).await?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer).await.map_err(Error::Io)?;

            if !buffer.is_empty() {
                existing_fsm = bincode::deserialize(&buffer).map_err(Error::BincodeError)?;
            }
        }

        // Step 2: Merge existing snapshot data
        self.data.splice(0..0, existing_fsm.data.drain(..));

        // Step 3: Write the merged state back to the snapshot file
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(snapshot_path)
            .await?;
        let bytes = bincode::serialize(&self).map_err(Error::BincodeError)?;
        file.write_all(&bytes).await?;
        file.sync_all().await.map_err(Error::Io)?;

        // Step 4: Clear `data` after snapshot is created successfully
        self.data.clear();

        self.last_snapshot_complete_time = Some(Instant::now());
        self.is_snapshotting = false;

        Ok(())
    }

    /// Check if a new snapshot is needed.
    async fn need_create_snapshot(&mut self) -> bool {
        if self.is_snapshotting {
            return false; // If we are currently snapshotting, we don't need another snapshot
        }

        if let Some(last_snapshot_time) = self.last_snapshot_complete_time {
            // Calculate the time since the last snapshot was completed
            let time_since_last_snapshot = Instant::now().duration_since(last_snapshot_time);

            // If the time since the last snapshot is greater than the snapshot interval, return true
            if time_since_last_snapshot >= self.snapshot_interval {
                // self.last_snapshot_complete_time = Some(Instant::now());
                return true;
            }
        } else {
            // If we never completed a snapshot, we need to create one
            return true;
        }

        false
    }

    async fn get_log_entry(&mut self) -> Result<Vec<LogEntry>> {
        let snapshot_path = if let Some(ref path) = self.snapshot_path {
            path
        } else {
            return Err(Error::Store(PathNotFound));
        };

        let mut existing_fsm = FileStateMachine::new(snapshot_path, Duration::from_secs(0));
        if fs::metadata(snapshot_path).await.is_ok() {
            let mut file = OpenOptions::new().read(true).open(snapshot_path).await?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer).await.map_err(Error::Io)?;

            if !buffer.is_empty() {
                existing_fsm = bincode::deserialize(&buffer).map_err(Error::BincodeError)?;
            }
        }

        self.data.splice(0..0, existing_fsm.data.drain(..));

        Ok(self.data.clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::server::LogCommand;

    use super::*;
    use tempfile::NamedTempFile;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_apply_log_entry() {
        let tmp_file = NamedTempFile::new().unwrap();
        let snapshot_path = tmp_file.path().to_str().unwrap();

        let mut fsm = FileStateMachine {
            snapshot_path: Option::Some(PathBuf::from(snapshot_path).into_boxed_path()),
            last_included_term: 0,
            last_included_index: 0,
            data: vec![],
            snapshot_interval: Duration::from_secs(300),
            snapshot_start_time: None,
            is_snapshotting: false,
            last_snapshot_complete_time: None,
        };

        let log_entry = LogEntry {
            term: 1,
            command: LogCommand::Set,
            leader_id: 1,
            server_id: 1,
            data: 1,
        };

        fsm.apply_log_entry(1, 1, log_entry.clone()).await;

        let log_entries = fsm.get_log_entry().await.unwrap();
        assert_eq!(log_entries.len(), 1);
        assert_eq!(log_entries[0], log_entry);
        assert_eq!(fsm.last_included_term, 1);
        assert_eq!(fsm.last_included_index, 1);
    }

    #[tokio::test]
    async fn test_need_create_snapshot() {
        let mut fsm = FileStateMachine {
            snapshot_path: None,
            last_included_term: 0,
            last_included_index: 0,
            data: vec![],
            snapshot_interval: Duration::from_secs(1),
            snapshot_start_time: None,
            is_snapshotting: false,
            last_snapshot_complete_time: Some(Instant::now()),
        };

        // Immediately after completing snapshot, no snapshot should be needed
        assert!(!fsm.need_create_snapshot().await);

        // Wait for more than the interval and check again
        sleep(Duration::from_secs(2)).await;
        assert!(fsm.need_create_snapshot().await);
    }

    #[tokio::test]
    async fn test_create_snapshot() {
        let tmp_file = NamedTempFile::new().unwrap();
        let snapshot_path = tmp_file.path().to_str().unwrap();

        // Create a FileStateMachine with some data
        let mut fsm = FileStateMachine {
            snapshot_path: Some(PathBuf::from(snapshot_path).into_boxed_path()),
            last_included_term: 1,
            last_included_index: 1,
            data: vec![
                LogEntry {
                    term: 1,
                    command: LogCommand::Set,
                    leader_id: 1,
                    server_id: 1,
                    data: 1,
                },
                LogEntry {
                    term: 2,
                    command: LogCommand::Set,
                    leader_id: 2,
                    server_id: 2,
                    data: 2,
                },
            ],
            snapshot_interval: Duration::from_secs(300),
            snapshot_start_time: None,
            is_snapshotting: false,
            last_snapshot_complete_time: None,
        };

        // Call create_snapshot and check result
        let result = fsm.create_snapshot().await;
        assert!(result.is_ok(), "Snapshot creation failed");

        // Check that the snapshot file is created
        let metadata = std::fs::metadata(snapshot_path);
        assert!(metadata.is_ok(), "Snapshot file was not created");

        // Read the file back and deserialize it to check contents
        let snapshot_data = std::fs::read(snapshot_path).unwrap();
        let deserialized_fsm: FileStateMachine = bincode::deserialize(&snapshot_data).unwrap();

        // Check that the deserialized data matches the original state machine
        assert_eq!(deserialized_fsm.data.len(), 2);

        // Check that the snapshot start and complete times were set
        assert!(
            fsm.snapshot_start_time.is_some(),
            "Snapshot start time was not set"
        );
        assert!(
            fsm.last_snapshot_complete_time.is_some(),
            "Snapshot complete time was not set"
        );

        // Check that is_snapshotting was set to false after completion
        assert!(
            !fsm.is_snapshotting,
            "is_snapshotting should be false after snapshot completion"
        );
    }
}
