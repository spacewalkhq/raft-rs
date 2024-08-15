// Organization: SpacewalkHq
// License: MIT License

// make this file executable with `chmod +x examples/simple_run.rs`

use raft_rs::cluster::{ClusterConfig, NodeMeta};
use raft_rs::log::get_logger;
use slog::{error, info};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use tokio::time::Duration;

use raft_rs::network::{NetworkLayer, TCPManager};
use raft_rs::server::{Server, ServerConfig};

#[tokio::main]
async fn main() {
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
            election_timeout: Duration::from_millis(1000),
            address: n.address,
            default_leader: Some(1u32),
            leadership_preferences: HashMap::new(),
            storage_location: Some("logs/".to_string()),
        })
        .collect();

    // Start servers in separate threads
    let mut handles = vec![];
    for (i, config) in configs.into_iter().enumerate() {
        let id = cluster_nodes[i];
        let cc = cluster_config.clone();
        handles.push(tokio::spawn(async move {
            let mut server = Server::new(id, config, cc).await;
            server.start().await;
        }));
    }

    // Simulate a client request after some delay
    tokio::time::sleep(Duration::from_secs(20)).await;
    client_request(1, 42u32).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    for handle in handles {
        handle.await.unwrap();
    }
}

async fn client_request(client_id: u32, data: u32) {
    let log = get_logger();

    let server_address = SocketAddr::from_str("127.0.0.1:5001").unwrap(); // Assuming server 1 is the leader
    let network_manager = TCPManager::new(server_address);

    let request_data = vec![
        client_id.to_be_bytes().to_vec(),
        10u32.to_be_bytes().to_vec(),
        6u32.to_be_bytes().to_vec(),
        data.to_be_bytes().to_vec(),
    ]
    .concat();

    if let Err(e) = network_manager.send(&server_address, &request_data).await {
        error!(log, "Failed to send client request: {}", e);
    }

    // sleep for a while to allow the server to process the request
    tokio::time::sleep(Duration::from_secs(5)).await;

    let response = network_manager.receive().await.unwrap();
    info!(log, "Received response: {:?}", response);
}
