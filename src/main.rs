pub mod server;
mod network;
mod storage;
use std::collections::HashMap;
use std::thread;
use network::{NetworkLayer, TCPManager};
use server::{Server, ServerConfig};
use tokio::runtime::Runtime;
use tokio::time::Duration;

#[tokio::main]
async fn main() {
    // Define cluster configuration
    let cluster_nodes = vec![1, 2, 3, 4, 5];
    let mut id_to_address_mapping = HashMap::new();
    id_to_address_mapping.insert(1, "127.0.0.1:5001".to_string());
    id_to_address_mapping.insert(2, "127.0.0.1:5002".to_string());
    id_to_address_mapping.insert(3, "127.0.0.1:5003".to_string());
    id_to_address_mapping.insert(4, "127.0.0.1:5004".to_string());
    id_to_address_mapping.insert(5, "127.0.0.1:5005".to_string());

    // Create server configs
    let configs: Vec<_> = cluster_nodes.iter().map(|&id| {
        ServerConfig {
            election_timeout: Duration::from_secs(5),
            address: "127.0.0.1".to_string(),
            port: 5000 + id as u16,
            cluster_nodes: cluster_nodes.clone(),
            id_to_address_mapping: id_to_address_mapping.clone(),
            default_leader: Some(1 as u32),
            leadership_preferences: HashMap::new(),
        }
    }).collect();

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

    // Simulate a client request after some delay
    thread::sleep(Duration::from_secs(5));
    client_request(1, 42 as u32).await;    
    thread::sleep(Duration::from_secs(2));
    // Join all server threads
    for handle in handles {
        handle.join().unwrap();
    }
}

async fn client_request(client_id: u32, data: u32) {
    let server_address = "127.0.0.1"; // Assuming server 1 is the leader
    let network_manager = TCPManager::new(server_address.to_string(), 5001);

    let request_data = vec![client_id.to_be_bytes().to_vec(), 1u32.to_be_bytes().to_vec(), 6u32.to_be_bytes().to_vec(), data.to_be_bytes().to_vec()].concat();

    if let Err(e) = network_manager.send(server_address, "5001", &request_data).await {
        eprintln!("Failed to send client request: {}", e);
    }

    // sleep for a while to allow the server to process the request
    tokio::time::sleep(Duration::from_secs(5)).await;

    let response = network_manager.receive().await.unwrap();
    println!("Received response: {:?}", response);
}
