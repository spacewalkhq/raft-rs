use std::collections::HashMap;
use std::time::{Duration, Instant};
use crate::network::{NetworkLayer, TCPManager};

// Define default leader ID and leadership preferences
const DEFAULT_LEADER_ID: u32 = 1; // Example ID for default leader

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RaftState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
struct ServerState {
    current_term: u32,
    state: RaftState,
    voted_for: Option<u32>,
    log: Vec<LogEntry>,
    commit_index: u32,
    last_applied: u32,
    next_index: Vec<u32>,
    match_index: Vec<u32>,
    election_timeout: Duration,
    last_heartbeat: Instant,
    votes_received: HashMap<u32, bool>,
    // Add leadership preference map
    leadership_preferences: HashMap<u32, u32>, // key: follower_id, value: preference_score
}

#[derive(Debug)]
enum LogCommand {
    Noop,
    Set,
    Delete,
}

#[derive(Debug)]
struct LogEntry {
    term: u32,
    index: u32,
    command: LogCommand,
}

#[derive(Debug)]
pub struct ServerConfig {
    election_timeout: Duration,
    heartbeat_interval: Duration,
    address: String,
    port: u16,
    cluster_nodes: Vec<u32>,
    id_to_address_mapping: HashMap<u32, String>,
    // Include default leader and leadership preferences
    default_leader: u32,
    leadership_preferences: HashMap<u32, u32>,
}

pub struct Server {
    pub id: u32,
    state: ServerState,
    peers: Vec<u32>,
    config: ServerConfig,
    current_term: u32,
    network_manager: TCPManager,
    // Add write buffer and debounce timer
    write_buffer: Vec<LogEntry>,
    debounce_timer: Instant,
}

impl Server {
    pub fn new(id: u32, config: ServerConfig) -> Server {
        let peers: Vec<u32> = config.cluster_nodes.iter().filter(|&&x| x != id).cloned().collect();
        let state = ServerState {
            current_term: 0,
            state: RaftState::Follower,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: vec![0; peers.len()],
            match_index: vec![0; peers.len()],
            election_timeout: config.election_timeout,
            last_heartbeat: Instant::now(),
            votes_received: HashMap::new(),
            // Initialize leadership preferences
            leadership_preferences: config.leadership_preferences.clone(),
        };
        let network_manager = TCPManager::new(config.address.clone(), config.port);

        Server {
            id,
            state,
            peers,
            config,
            current_term: 0,
            network_manager,
            // Initialize write buffer and debounce timer
            write_buffer: Vec::new(),
            debounce_timer: Instant::now(),
        }
    }

    pub async fn start(&mut self) {
        self.network_manager.open().await.unwrap(); // Open network manager

        loop {
            match self.state.state {
                RaftState::Follower => self.follower().await,
                RaftState::Candidate => self.candidate().await,
                RaftState::Leader => self.leader().await,
            }
        }
    }

    async fn follower(&mut self) {
        let now = Instant::now();
        if now.duration_since(self.state.last_heartbeat) > self.state.election_timeout {
            self.state.state = RaftState::Candidate;
            return;
        }
        // Receive requests for append entries and vote
        self.receive_rpc().await;
    }

    async fn candidate(&mut self) {
        // if current term is 0, increment term and and assume leadership to default leader
        if self.current_term == 0 {
            self.current_term += 1;
            self.state.current_term = self.current_term;
            if self.id == DEFAULT_LEADER_ID {
                self.state.state = RaftState::Leader;
            } else {
                self.state.state = RaftState::Follower;
            }
            return;
        }

        self.current_term += 1;
        self.state.current_term = self.current_term;

        // Vote for self
        self.state.voted_for = Some(self.id);
        self.state.votes_received.insert(self.id, true);

        // Send RequestVote RPCs with leadership preferences
        let vote_requests = self.peers.iter().map(|peer_id| {
            let peer_address = self.config.id_to_address_mapping.get(peer_id).unwrap().clone();
            let data = self.prepare_request_vote();
            let network_manager = self.network_manager.clone();
            async move {
                network_manager.send(&data).await
            }
        });

        // Wait for election timeout
        let now = Instant::now();
        while now.duration_since(self.state.last_heartbeat) < self.state.election_timeout {
            self.receive_rpc().await;
        }

        // Check if majority votes received
        let votes_received = self.state.votes_received.values().filter(|&&x| x).count();
        if votes_received > self.peers.len() / 2 {
            self.state.state = RaftState::Leader;
        } else {
            self.state.state = RaftState::Follower;
        }
    }

    async fn leader(&mut self) {
        // Send heartbeats to all peers using broadcast
        let heartbeat_data = self.prepare_heartbeat();
        // using id_to_address_mapping to get the address of the peers
        let addresses: Vec<String> = self.peers.iter().map(|peer_id| {
            self.config.id_to_address_mapping.get(peer_id).unwrap().clone()
        }).collect();
        self.network_manager.broadcast(&heartbeat_data, addresses).await.unwrap();

        // Write coalescing with debouncing
        if self.write_buffer.len() > 0 && Instant::now().duration_since(self.debounce_timer) > Duration::from_millis(100) {
            // Append log entries to disk
            self.append_log().await;
            self.write_buffer.clear();
            self.debounce_timer = Instant::now();
        }

        // Wait for heartbeat interval
        let now = Instant::now();
        while now.duration_since(self.state.last_heartbeat) < self.config.heartbeat_interval {
            self.receive_rpc().await;
        }
    }

    async fn receive_rpc(&mut self) {
        // Receive RPC from peer
        let data = self.network_manager.receive().await.unwrap();
        self.handle_rpc(data).await;
    }

    fn prepare_request_vote(&self) -> Vec<u8> {
        // Prepare data for RequestVote RPC
        vec![]
    }

    fn prepare_heartbeat(&self) -> Vec<u8> {
        [self.id.to_be_bytes(), self.current_term.to_be_bytes()].concat()
    }

    async fn handle_rpc(&mut self, data: Vec<u8>) {
        // Handle RPC data
        let peer_id = u32::from_be_bytes(data[0..4].try_into().unwrap());
        let term = u32::from_be_bytes(data[4..8].try_into().unwrap());
        let rpc_type = u32::from_be_bytes(data[8..12].try_into().unwrap());
        
        // Handle different types of RPCs
        match rpc_type {
            // Add logic to handle specific RPC types
            _ => {},
        }
    }

    async fn append_log(&mut self) {
        // Append log entries to disk
        println!("Appending logs to disk");
        // Logic to write logs to disk
    }

    fn is_quorum(&self, votes: u32) -> bool {
        votes > (self.peers.len() / 2).try_into().unwrap_or_default()
    }
}
