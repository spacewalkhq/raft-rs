// Organization: SpacewalkHq
// License: MIT License

use raft_rs::log::get_logger;
use slog::error;
use std::collections::HashMap;
use std::thread;
use tokio::runtime::Runtime;
use tokio::time::Duration;

use raft_rs::network::{NetworkLayer, TCPManager};
use raft_rs::server::{Server, ServerConfig};

#[tokio::main]
async fn main() {
    // Define cluster configuration
    let mut cluster_nodes = vec![1, 2, 3, 4, 5];
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
            election_timeout: Duration::from_millis(1000),
            address: "127.0.0.1".to_string(),
            port: 5000 + id as u16,
            cluster_nodes: cluster_nodes.clone(),
            id_to_address_mapping: id_to_address_mapping.clone(),
            default_leader: Some(1 as u32),
            leadership_preferences: HashMap::new(),
            storage_location: Some("logs/".to_string()),
        })
        .collect();

    // Start servers in separate threads
    let mut handles = vec![];
    for (i, config) in configs.into_iter().enumerate() {
        let id = cluster_nodes[i];
        handles.push(thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            let mut server = Server::new(id, config);
            rt.block_on(server.start());
        }));
    }

    // Simulate adding a new node
    // The following defines the basic configuration of the new node
    tokio::time::sleep(Duration::from_secs(10)).await;
    let new_node_id = 6;
    let new_node_ip = "127.0.0.1";
    let new_node_port = 5006;
    let new_node_address = format!("{}:{}", new_node_ip, new_node_port);
    cluster_nodes.push(new_node_id);
    id_to_address_mapping.insert(new_node_id, new_node_address.to_string());
    let new_node_conf = ServerConfig {
        election_timeout: Duration::from_millis(1000),
        address: new_node_ip.to_string(),
        port: new_node_port as u16,
        cluster_nodes: cluster_nodes.clone(),
        id_to_address_mapping: id_to_address_mapping.clone(),
        default_leader: Some(1 as u32),
        leadership_preferences: HashMap::new(),
        storage_location: Some("logs/".to_string()),
    };

    // Launching a new node
    handles.push(thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        let mut server = Server::new(6, new_node_conf);
        rt.block_on(server.start());
    }));

    // Simulate sending a Raft Join request after a few seconds
    // Because we need to wait until the new node has started
    tokio::time::sleep(Duration::from_secs(3)).await;
    add_node_request(new_node_id, new_node_address, new_node_port).await;

    // Wait for all servers to finish
    for handle in handles {
        handle.join().unwrap();
    }
}

async fn add_node_request(new_node_id: u32, addr: String, port: u32) {
    let log = get_logger();

    let server_address = "127.0.0.1";
    let network_manager = TCPManager::new(server_address.to_string(), port.try_into().unwrap());

    let request_data = vec![
        new_node_id.to_be_bytes().to_vec(),
        0u32.to_be_bytes().to_vec(),
        10u32.to_be_bytes().to_vec(),
        addr.as_bytes().to_vec(),
    ]
    .concat();

    // Let's assume that 5001 is the port of the leader node.
    if let Err(e) = network_manager
        .send(server_address, "5001", &request_data)
        .await
    {
        error!(log, "Failed to send client request: {}", e);
    }
}
