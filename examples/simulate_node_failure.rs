// Organization: SpacewalkHq
// License: MIT License

use raft_rs::log::get_logger;
use raft_rs::server::{Server, ServerConfig};
use rand::Rng;
use slog::{info, warn};
use std::collections::HashMap;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    let log = get_logger();

    // Define cluster configuration
    let cluster_nodes = vec![1, 2, 3, 4, 5];
    let mut id_to_address_mapping = HashMap::new();
    id_to_address_mapping.insert(1, "127.0.0.1:5001".to_string());
    id_to_address_mapping.insert(2, "127.0.0.1:5002".to_string());
    id_to_address_mapping.insert(3, "127.0.0.1:5003".to_string());
    id_to_address_mapping.insert(4, "127.0.0.1:5004".to_string());
    id_to_address_mapping.insert(5, "127.0.0.1:5005".to_string());

    // Create server configs
    let configs: Vec<_> = cluster_nodes
        .iter()
        .map(|&id| ServerConfig {
            election_timeout: Duration::from_millis(200),
            address: "127.0.0.1".to_string(),
            port: 5000 + id as u16,
            cluster_nodes: cluster_nodes.clone(),
            id_to_address_mapping: id_to_address_mapping.clone(),
            default_leader: Some(1),
            leadership_preferences: HashMap::new(),
            storage_location: Some("logs/".to_string()),
        })
        .collect();

    // Start servers asynchronously
    let mut server_handles = vec![];
    for (i, config) in configs.into_iter().enumerate() {
        let id = cluster_nodes[i];
        let server_handle = tokio::spawn(async move {
            let mut server = Server::new(id, config);
            server.start().await;
        });
        server_handles.push(server_handle);
    }

    // Simulate stopping and restarting servers
    let mut rng = rand::thread_rng();
    for _ in 0..10 {
        let sleep_time = rng.gen_range(3..=5);
        sleep(Duration::from_secs(sleep_time)).await;

        let server_to_stop = rng.gen_range(1..=5);
        warn!(log, "Stopping server {}", server_to_stop);

        // Cancel the selected server's task
        server_handles[server_to_stop - 1].abort();

        // Simulate Raft leader election process
        sleep(Duration::from_secs(3)).await;

        warn!(log, "Restarting server {}", server_to_stop);
        let config = ServerConfig {
            election_timeout: Duration::from_millis(200),
            address: "127.0.0.1".to_string(),
            port: 5000 + server_to_stop as u16,
            cluster_nodes: cluster_nodes.clone(),
            id_to_address_mapping: id_to_address_mapping.clone(),
            default_leader: Some(1),
            leadership_preferences: HashMap::new(),
            storage_location: Some("logs/".to_string()),
        };
        let server_handle = tokio::spawn(async move {
            let mut server = Server::new(server_to_stop.try_into().unwrap(), config);
            server.start().await;
        });
        server_handles[server_to_stop - 1] = server_handle;
    }

    // Wait for all server tasks to complete (if they haven't been aborted)
    for handle in server_handles {
        let _ = handle.await;
    }

    info!(log, "Test completed successfully.");
}
