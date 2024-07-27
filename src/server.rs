use std::collections::HashMap;
use std::time::{Duration, Instant};
use crate::network::{NetworkLayer, TCPManager};

#[derive(Debug, Clone, PartialEq)]
enum RaftState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
enum MesageType {
    RequestVote,
    RequestVoteResponse,
    AppendEntries,
    AppendEntriesResponse,
    Heartbeat,
    HeartbeatResponse,
    ClientRequest,
}

#[derive(Debug)]
struct ServerState {
    current_term: u32,
    state: RaftState,
    voted_for: Option<u32>,
    log: Vec<LogEntry>,
    commit_index: u32, 
    last_applied: u32, // index of the highest log entry applied to state machine
    next_index: Vec<u32>, // index of the next log entry to send to each server
    match_index: Vec<u32>,
    election_timeout: Duration,
    last_heartbeat: Instant,
    votes_received: HashMap<u32, bool>,
    // Add leadership preference map
    leadership_preferences: HashMap<u32, u32>, // key: follower_id, value: preference_score
}

#[derive(Debug, Clone)]
enum LogCommand {
    Noop,
    Set,
    Delete,
}

#[derive(Debug, Clone)]
struct LogEntry {
    term: u32,
    index: u32,
    command: LogCommand,
    data: u32,
}

#[derive(Debug)]
pub struct ServerConfig {
    election_timeout: Duration,
    address: String,
    port: u16,
    cluster_nodes: Vec<u32>,
    id_to_address_mapping: HashMap<u32, String>,
    // Include default leader and leadership preferences
    default_leader: Option<u32>,
    leadership_preferences: HashMap<u32, u32>,
}

pub struct Server {
    pub id: u32,
    state: ServerState,
    peers: Vec<u32>,
    config: ServerConfig,
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
            network_manager,
            // Initialize write buffer and debounce timer
            write_buffer: Vec::new(),
            debounce_timer: Instant::now(),
        }
    }

    pub async fn start(&mut self) {
        if let Err(e) = self.network_manager.open().await {
            eprintln!("Failed to open network manager: {}", e);
            return;
        }

        loop {
            match self.state.state {
                RaftState::Follower => self.follower().await,
                RaftState::Candidate => self.candidate().await,
                RaftState::Leader => self.leader().await,
            }
        }
    }

    async fn follower(&mut self) {
        if self.state.state != RaftState::Follower {
            return;
        }

        let now = Instant::now();
        if now.duration_since(self.state.last_heartbeat) > self.state.election_timeout {
            self.state.state = RaftState::Candidate;
            return;
        }
        self.receive_rpc().await;
    }

    async fn candidate(&mut self) {
        if self.state.state != RaftState::Candidate {
            return;
        }
        
        // if current term is 0, increment term and and assume leadership to default leader
        if self.state.current_term == 0 {
            self.state.current_term += 1;
            self.state.current_term = self.state.current_term;
            
            match self.config.default_leader {
                Some(leader_id) => {
                    if self.id == leader_id {
                        self.state.state = RaftState::Leader;
                        return;
                    }
                }
                None => {}
            }
        }

        self.state.current_term += 1;
        self.state.current_term = self.state.current_term;

        // Vote for self
        self.state.voted_for = Some(self.id);
        self.state.votes_received.insert(self.id, true);

        // TODO: Send RequestVote RPCs with leadership preferences
        let data = self.prepare_request_vote(self.id, self.state.current_term);
        let addresses: Vec<String> = self.peers.iter().map(|peer_id| {
            self.config.id_to_address_mapping.get(peer_id).unwrap().clone()
        }).collect();
        let _ = self.network_manager.broadcast(&data, addresses);

        // Wait for election timeout
        let now = Instant::now();
        while now.duration_since(self.state.last_heartbeat) < self.state.election_timeout {
            self.receive_rpc().await;
        }

        if self.is_quorum(self.state.votes_received.len() as u32) {
            self.state.state = RaftState::Leader;
        } else {
            self.state.votes_received.clear();
            self.state.state = RaftState::Follower;
        }
        
    }

    async fn leader(&mut self) {
        loop {
            if self.state.state != RaftState::Leader {
                return;
            }

            let now = Instant::now();
            if now.duration_since(self.state.last_heartbeat) > self.state.election_timeout {
                self.state.state = RaftState::Candidate;
                return;
            }

            let heartbeat_data = self.prepare_heartbeat();
            let addresses: Vec<String> = self.peers.iter().map(|peer_id| {
                self.config.id_to_address_mapping.get(peer_id).unwrap().clone()
            }).collect();

            if let Err(e) = self.network_manager.broadcast(&heartbeat_data, addresses).await {
                eprintln!("Failed to send heartbeats: {}", e);
            }

            // Write coalescing with debouncing
            if !self.write_buffer.is_empty() && Instant::now().duration_since(self.debounce_timer) > Duration::from_millis(100) {
                // Batch write to disk
                // TODO: Implement batch write to disk

                // Broadcast batched logs to all peers
                let prev_log_index = self.state.log.len() as u32;
                let data = self.prepare_append_batch(self.id, self.state.current_term, prev_log_index, self.state.commit_index, self.write_buffer.clone());
                let addresses: Vec<String> = self.peers.iter().map(|peer_id| {
                    self.config.id_to_address_mapping.get(peer_id).unwrap().clone()
                }).collect();

                if let Err(e) = self.network_manager.broadcast(&data, addresses).await {
                    eprintln!("Failed to broadcast append entries: {}", e);
                }

                // Clear write buffer and reset debounce timer
                self.write_buffer.clear();
                self.debounce_timer = Instant::now();
            }

            self.receive_rpc().await;
        }
    }
    
    async fn receive_rpc(&mut self) {
        // Receive RPC from peer
        let data = self.network_manager.receive().await.unwrap();
        self.handle_rpc(data).await;
    }

    fn prepare_append_batch(&self, id: u32, term: u32, prev_log_index: u32, commit_index: u32, write_buffer: Vec<LogEntry>) -> Vec<u8> {
        let mut data = [id.to_be_bytes(), term.to_be_bytes(), 2u32.to_be_bytes(), prev_log_index.to_be_bytes(), commit_index.to_be_bytes()].concat();
        for entry in write_buffer {
            let entry_data = [entry.term.to_be_bytes(), entry.index.to_be_bytes(), entry.data.to_be_bytes()].concat();
            data.extend_from_slice(&entry_data);
        }
        data
    }

    fn prepare_request_vote(&self, id: u32, term: u32) -> Vec<u8> {
        [id.to_be_bytes(), term.to_be_bytes(), 0u32.to_be_bytes()].concat()
    }

    fn prepare_heartbeat(&self) -> Vec<u8> {
        [self.id.to_be_bytes(), self.state.current_term.to_be_bytes()].concat()
    }

    async fn handle_rpc(&mut self, data: Vec<u8>) {
        // Handle RPC data
        let peer_id = u32::from_be_bytes(data[0..4].try_into().unwrap());
        let term = u32::from_be_bytes(data[4..8].try_into().unwrap());
        let message_type = u32::from_be_bytes(data[8..12].try_into().unwrap());

        if term < self.state.current_term {
            return;
        }

        if term > self.state.current_term {
            self.state.current_term = term;
            self.state.state = RaftState::Follower;
        }

        // covert message_type to enum
        let message_type = match message_type {
            0 => MesageType::RequestVote,
            1 => MesageType::RequestVoteResponse,
            2 => MesageType::AppendEntries,
            3 => MesageType::AppendEntriesResponse,
            4 => MesageType::Heartbeat,
            5 => MesageType::HeartbeatResponse,
            6 => MesageType::ClientRequest,
            _ => panic!("Invalid message type"),
        };
        
        match message_type {
            MesageType::RequestVote => {
                self.handle_request_vote(peer_id).await;
            }
            MesageType::RequestVoteResponse => {
                self.handle_request_vote_response(peer_id).await;
            }
            MesageType::AppendEntries => {
                self.handle_append_entries(data).await;
            }
            MesageType::AppendEntriesResponse => {
                self.handle_append_entries_response().await;
            }
            MesageType::Heartbeat => {
                self.handle_heartbeat().await;
            }
            MesageType::HeartbeatResponse => {
                self.handle_heartbeat_response(data).await;
            }
            MesageType::ClientRequest => {
                self.handle_client_request(data).await;
            }
        }
    }

    async fn handle_client_request(&mut self, data: Vec<u8>) {
        let term = self.state.current_term;
        let index = self.state.log.len() as u32;
        let command = LogCommand::Set;
        let data = u32::from_be_bytes(data[12..16].try_into().unwrap());
        let entry = LogEntry { term, index, command, data };
        self.write_buffer.push(entry);
    }

    async fn handle_request_vote(&mut self, peer_id: u32) {
        // Only Follower can vote, because Candidate voted for itself
        if self.state.state != RaftState::Follower {
            return;
        }


        self.state.voted_for = Some(peer_id);

        let data = [self.id.to_be_bytes(), self.state.current_term.to_be_bytes(), 1u32.to_be_bytes()].concat();
        let voteresponse = self.network_manager.send(&data).await;
        if let Err(e) = voteresponse {
            eprintln!("Failed to send vote response: {}", e);
        }
    }

    async fn handle_request_vote_response(&mut self, peer_id: u32) {
        if self.state.state != RaftState::Candidate {
            return;
        }


        self.state.votes_received.insert(peer_id, true);
    }

    async fn handle_append_entries(&mut self, batch_data: Vec<u8>) {
        if self.state.state != RaftState::Follower {
            return;
        }


        // get data from the message
        let id = u32::from_be_bytes(batch_data[0..4].try_into().unwrap());
        let leader_term = u32::from_be_bytes(batch_data[4..8].try_into().unwrap());

        if leader_term < self.state.current_term {
            return;
        }

        let message_type = u32::from_be_bytes(batch_data[8..12].try_into().unwrap());
        if message_type != 2 {
            return;
        }
        
        let prev_log_index = u32::from_be_bytes(batch_data[12..16].try_into().unwrap());
        let commit_index = u32::from_be_bytes(batch_data[16..20].try_into().unwrap());
        let data = &batch_data[20..];
        self.append_log(id, data).await;
    }

    async fn handle_append_entries_response(&mut self) {
        let data = [self.id.to_be_bytes(), self.state.current_term.to_be_bytes(), 3u32.to_be_bytes()].concat();
        let response = self.network_manager.send(&data).await;
        if let Err(e) = response {
            eprintln!("Failed to send append entries response: {}", e);
        }
    }

    async fn handle_heartbeat(&mut self) {
        let data = [self.id.to_be_bytes(), self.state.current_term.to_be_bytes(), 5u32.to_be_bytes()].concat();
        let response = self.network_manager.send(&data).await;
        if let Err(e) = response {
            eprintln!("Failed to send heartbeat: {}", e);
        }
    }

    async fn handle_heartbeat_response(&mut self, data: Vec<u8>) {

        let message_type = u32::from_be_bytes(data[8..12].try_into().unwrap());
        if message_type != 5 {
            return;
        }

        self.state.last_heartbeat = Instant::now();
    }

    async fn append_log(&mut self, id:u32, data: &[u8]) {
        // Append log entries to disk
        println!("Appending logs to disk from peer: {}", id);
        println!("Data: {:?}", data);
        // Logic to write logs to disk
        // convert data to LogEntry and append to log

    }

    fn is_quorum(&self, votes: u32) -> bool {
        votes > (self.peers.len() / 2).try_into().unwrap_or_default()
    }
}
