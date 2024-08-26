// Organization: SpacewalkHq
// License: MIT License

// We create a cluster of 5 nodes and simulate different scenarios of storage failure and recovery.

use raft_rs::cluster::{ClusterConfig, NodeMeta};
use raft_rs::log::get_logger;
use raft_rs::server::{Server, ServerConfig};
use rand::Rng;
use slog::{info, warn};
use std::collections::HashMap;
use std::fs;
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
            // Simulate storage corruption when starting up
            let storage_location = "logs".to_string();
            let corrupted = rand::thread_rng().gen_bool(0.3); // 30% chance of corruption
            if corrupted {
                fs::create_dir_all(&storage_location).unwrap();
                fs::write(format!("{}server_{}.log", storage_location, id), b"").unwrap(); // Simulate corruption
                warn!(get_logger(), "Storage for server {} is corrupted", id);
            }

            let mut server = Server::new(id, config, cc).await;
            server.start().await;
        });
        server_handles.push(server_handle);
    }
    info!(log, "Cluster is up and running");

    // Simulate a random storage failure and recovery while servers are running
    let mut rng = rand::thread_rng();
    for _ in 0..10 {
        let sleep_time = rng.gen_range(3..=5);
        sleep(Duration::from_secs(sleep_time)).await;

        let server_to_fail = rng.gen_range(1..=5);
        warn!(
            log,
            "Simulating storage corruption for server {}", server_to_fail
        );

        // Simulate storage corruption on a running server
        let storage_path = format!("logs/");
        fs::create_dir_all(&storage_path).unwrap();
        fs::write(
            format!("{}server_{}.log", storage_path, server_to_fail),
            b"",
        )
        .unwrap(); // Simulate corruption

        // Restart the corrupted server to simulate recovery
        let cc: ClusterConfig = cluster_config.clone();
        let server_handle = tokio::spawn(async move {
            let config = ServerConfig {
                election_timeout: Duration::from_millis(200),
                address: SocketAddr::from_str(
                    format!("127.0.0.1:{}", 5000 + server_to_fail).as_str(),
                )
                .unwrap(),
                default_leader: Some(1),
                leadership_preferences: HashMap::new(),
                storage_location: Some(storage_path.clone()),
            };
            let mut server = Server::new(server_to_fail.try_into().unwrap(), config, cc).await;
            server.start().await;
            // Handle recovery of corrupted storage
            info!(
                get_logger(),
                "Server {} has recovered from storage corruption", server_to_fail
            );
        });

        server_handles[server_to_fail - 1] = server_handle;
    }

    // Wait for all server tasks to complete (if they haven't been aborted)
    for handle in server_handles {
        let _ = handle.await;
    }

    info!(log, "Test completed successfully.");
}
