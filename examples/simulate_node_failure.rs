// Organization: SpacewalkHq
// License: MIT License

use raft_rs::cluster::{ClusterConfig, NodeMeta};
use raft_rs::log::get_logger;
use raft_rs::server::{Server, ServerConfig};
use rand::Rng;
use slog::{info, warn};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    let log = get_logger();

    // Define cluster configuration
    let cluster_nodes = vec![1, 2, 3, 4, 5];
    let peers = vec![
        NodeMeta::from((1, SocketAddr::from_str("127.0.0.1:5001").unwrap())),
        NodeMeta::from((2, SocketAddr::from_str("127.0.0.1:5002").unwrap())),
        NodeMeta::from((3, SocketAddr::from_str("127.0.0.1:5003").unwrap())),
        NodeMeta::from((4, SocketAddr::from_str("127.0.0.1:5004").unwrap())),
        NodeMeta::from((5, SocketAddr::from_str("127.0.0.1:5005").unwrap())),
    ];
    let cluster_config = ClusterConfig::new(peers.clone());

    // Create server configs
    let configs: Vec<_> = peers
        .clone()
        .iter()
        .map(|n| ServerConfig {
            election_timeout: Duration::from_millis(200),
            address: n.address,
            default_leader: Some(1),
            leadership_preferences: HashMap::new(),
            storage_location: Some("logs/".to_string()),
        })
        .collect();

    // Start servers asynchronously
    let mut server_handles = vec![];
    for (i, config) in configs.into_iter().enumerate() {
        let id = cluster_nodes[i];
        let cc = cluster_config.clone();
        let server_handle = tokio::spawn(async move {
            let mut server = Server::new(id, config, cc);
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
            address: SocketAddr::from_str(format!("127.0.0.1:{}", 5000 + server_to_stop).as_str())
                .unwrap(),
            default_leader: Some(1),
            leadership_preferences: HashMap::new(),
            storage_location: Some("logs/".to_string()),
        };
        let cc = cluster_config.clone();
        let server_handle = tokio::spawn(async move {
            let mut server = Server::new(server_to_stop.try_into().unwrap(), config, cc);
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
