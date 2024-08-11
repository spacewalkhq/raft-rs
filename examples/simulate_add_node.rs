// Organization: SpacewalkHq
// License: MIT License

use raft_rs::cluster::{ClusterConfig, NodeMeta};
use raft_rs::log::get_logger;
use slog::error;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::thread;
use tokio::runtime::Runtime;
use tokio::time::Duration;

use raft_rs::network::{NetworkLayer, TCPManager};
use raft_rs::server::{Server, ServerConfig};

#[tokio::main]
async fn main() {
    // Define cluster configuration
    let mut cluster_nodes = vec![1, 2, 3, 4, 5];
    let peers = vec![
        NodeMeta::from((1, SocketAddr::from_str("127.0.0.1:5001").unwrap())),
        NodeMeta::from((2, SocketAddr::from_str("127.0.0.1:5002").unwrap())),
        NodeMeta::from((3, SocketAddr::from_str("127.0.0.1:5003").unwrap())),
        NodeMeta::from((4, SocketAddr::from_str("127.0.0.1:5004").unwrap())),
        NodeMeta::from((5, SocketAddr::from_str("127.0.0.1:5005").unwrap())),
    ];
    let mut cluster_config = ClusterConfig::new(peers.clone());
    // Create server configs
    let configs: Vec<_> = peers
        .clone()
        .iter()
        .map(|n| ServerConfig {
            election_timeout: Duration::from_millis(1000),
            address: n.address,
            default_leader: Some(1 as u32),
            leadership_preferences: HashMap::new(),
            storage_location: Some("logs/".to_string()),
        })
        .collect();

    // Start servers in separate threads
    let mut handles = vec![];
    for (i, config) in configs.into_iter().enumerate() {
        let id = cluster_nodes[i];
        let cc = cluster_config.clone();
        handles.push(thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            let mut server = Server::new(id, config, cc);
            rt.block_on(server.start());
        }));
    }

    // Simulate adding a new node
    // The following defines the basic configuration of the new node
    tokio::time::sleep(Duration::from_secs(10)).await;
    let new_node_id = 6;
    let new_node_port = 5006;
    let new_node_address =
        SocketAddr::from_str(format!("127.0.0.1:{}", new_node_port).as_str()).unwrap();
    cluster_nodes.push(new_node_id);

    cluster_config.add_server(NodeMeta::from((
        new_node_id,
        new_node_address.clone().into(),
    )));
    let new_node_conf = ServerConfig {
        election_timeout: Duration::from_millis(1000),
        address: new_node_address.clone().into(),
        default_leader: Some(1 as u32),
        leadership_preferences: HashMap::new(),
        storage_location: Some("logs/".to_string()),
    };

    // Launching a new node
    handles.push(thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        let mut server = Server::new(6, new_node_conf, cluster_config);
        rt.block_on(server.start());
    }));

    // Simulate sending a Raft Join request after a few seconds
    // Because we need to wait until the new node has started
    tokio::time::sleep(Duration::from_secs(3)).await;
    add_node_request(new_node_id, new_node_address).await;

    // Wait for all servers to finish
    for handle in handles {
        handle.join().unwrap();
    }
}

async fn add_node_request(new_node_id: u32, addr: SocketAddr) {
    let log = get_logger();

    let server_address = addr;
    let network_manager = TCPManager::new(server_address);

    let request_data = vec![
        new_node_id.to_be_bytes().to_vec(),
        0u32.to_be_bytes().to_vec(),
        10u32.to_be_bytes().to_vec(),
        addr.to_string().as_bytes().to_vec(),
    ]
    .concat();

    // Let's assume that 5001 is the port of the leader node.
    if let Err(e) = network_manager.send(&server_address, &request_data).await {
        error!(log, "Failed to send client request: {}", e);
    }
}
