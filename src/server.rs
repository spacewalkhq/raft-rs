use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use serde::{Serialize, Deserialize};
use tokio::time::sleep;
use crate::network::{NetworkLayer, TCPManager};
use crate::storage::{LocalStorage, Storage};

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
    ClientResponse,
}

#[derive(Debug)]
struct ServerState {
    current_term: u32,
    state: RaftState,
    voted_for: Option<u32>,
    log: VecDeque<LogEntry>,
    commit_index: u32,
    previous_log_index: u32, 
    next_index: Vec<u32>,
    match_index: Vec<u32>,
    election_timeout: Duration,
    last_heartbeat: Instant,
    votes_received: HashMap<u32, bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum LogCommand {
    Noop,
    Set,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogEntry {
    leader_id: u32,
    server_id: u32,
    term: u32,
    command: LogCommand,
    data: u32,
}

#[derive(Debug)]
pub struct ServerConfig {
    pub election_timeout: Duration,
    pub address: String,
    pub port: u16,
    pub cluster_nodes: Vec<u32>,
    pub id_to_address_mapping: HashMap<u32, String>,
    // Include default leader and leadership preferences
    pub default_leader: Option<u32>,
    pub leadership_preferences: HashMap<u32, u32>,
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
    storage: LocalStorage,
}

impl Server {
    pub fn new(id: u32, config: ServerConfig) -> Server {
        let peers: Vec<u32> = config.cluster_nodes.iter().filter(|&&x| x != id).cloned().collect();
        let state = ServerState {
            current_term: 0,
            state: RaftState::Follower,
            voted_for: None,
            log: VecDeque::new(),
            commit_index: 0,
            previous_log_index: 0,
            next_index: vec![0; peers.len()],
            match_index: vec![0; peers.len()],
            election_timeout: config.election_timeout + Duration::from_secs(2*id as u64), 
            last_heartbeat: Instant::now(),
            votes_received: HashMap::new(),
        };
        let network_manager = TCPManager::new(config.address.clone(), config.port);

        Server {
            id,
            state,
            peers,
            config,
            network_manager,
            write_buffer: Vec::new(),
            debounce_timer: Instant::now(),
            storage: LocalStorage::new(format!("server_{}.log", id)),
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

        self.state.match_index = vec![0; self.peers.len()+1];
        self.state.next_index = vec![0; self.peers.len()+1];

        println!("Server {} is a follower", self.id);
        // default leader
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
        loop {
            let timeout_duration = self.state.election_timeout;
            
            let timeout_future = async {
                sleep(timeout_duration).await;
            };

            let rpc_future = self.receive_rpc();

            tokio::select! {
                _ = timeout_future => {
                    self.state.state = RaftState::Candidate;
                    self.state.last_heartbeat = Instant::now();
                    break
                }
                _ = rpc_future => {
                }
            }
        }
    }

    async fn candidate(&mut self) {
        if self.state.state != RaftState::Candidate {
            return;
        }
        println!("Server {} is a candidate", self.id);
        self.state.last_heartbeat = Instant::now(); // reset election timeout

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
        let _ = self.network_manager.broadcast(&data, addresses).await;

        loop {
            let timeout_duration = self.state.election_timeout;

            let timeout_future = async {
                sleep(timeout_duration).await;
            };

            let rpc_future = self.receive_rpc();
            tokio::select! {
                _ = timeout_future => {
                    if Instant::now().duration_since(self.state.last_heartbeat) >= timeout_duration {
                        println!("Election timeout");
                        self.state.state = RaftState::Follower;
                        self.state.votes_received.clear();
                        break;
                    }
                }
                _ = rpc_future => {
                    if self.is_quorum(self.state.votes_received.len() as u32) {
                        println!("Quorum reached");
                        self.state.state = RaftState::Leader;
                        break;
                    }
                }
            }
        }

        if self.state.state == RaftState::Leader {
            self.state.current_term += 1;
        } else {
            self.state.state = RaftState::Follower;
            self.state.votes_received.clear();
        }
    }

    async fn leader(&mut self) {
        if self.state.state != RaftState::Leader {
            return;
        }
        println!("Server {} is the leader", self.id);
        println!("Leader state: {:?}", self.state);

        let mut heartbeat_interval = tokio::time::interval(Duration::from_millis(300));


        loop {
            let rpc_future = self.receive_rpc();
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    if self.state.state != RaftState::Leader {
                        break;
                    }

                    let now = Instant::now();
                    self.state.last_heartbeat = now;
    
                    let heartbeat_data = self.prepare_heartbeat();
                    let addresses: Vec<String> = self.peers.iter().map(|peer_id| {
                        self.config.id_to_address_mapping.get(peer_id).unwrap().clone()
                    }).collect();
    
                    if let Err(e) = self.network_manager.broadcast(&heartbeat_data, addresses).await {
                        eprintln!("Failed to send heartbeats: {}", e);
                    }
                },
                _ = rpc_future => {
                    if self.state.state != RaftState::Leader {
                        break;
                    }
                    // TODO: Write coalescing with debouncing
                    // Move this to a separate thread to avoid blocking the main loop
                    if !self.write_buffer.is_empty() {
                        let append_batch = self.prepare_append_batch(self.id, self.state.current_term, self.state.previous_log_index, self.state.commit_index, self.write_buffer.clone());

                        for entry in self.write_buffer.clone() {
                            let data = [1u32.to_be_bytes(), entry.data.to_be_bytes()].concat();
                            self.append_log(self.id, self.state.current_term, &data).await;
                        }

                        let addresses: Vec<String> = self.peers.iter().map(|peer_id| {
                            self.config.id_to_address_mapping.get(peer_id).unwrap().clone()
                        }).collect();
                        if let Err(e) = self.network_manager.broadcast(&append_batch, addresses).await {
                            eprintln!("Failed to send append batch: {}", e);
                        }

                        self.write_buffer.clear();
                        self.debounce_timer = Instant::now();            
                    }

                    println!("Leader state: {:?}", self.state);
                },
            }
        }
    }
    
    async fn receive_rpc(&mut self) {
        let data = self.network_manager.receive().await.unwrap();
        self.handle_rpc(data).await;
    }

    fn prepare_append_batch(&self, id: u32, term: u32, prev_log_index: u32, commit_index: u32, write_buffer: Vec<LogEntry>) -> Vec<u8> {
        let mut data = [id.to_be_bytes(), term.to_be_bytes(), 2u32.to_be_bytes(), prev_log_index.to_be_bytes(), commit_index.to_be_bytes()].concat();
        for entry in write_buffer {
            let entry_data = [entry.term.to_be_bytes(), entry.data.to_be_bytes()].concat();
            data.extend_from_slice(&entry_data);
        }
        data
    }

    fn prepare_request_vote(&self, id: u32, term: u32) -> Vec<u8> {
        [id.to_be_bytes(), term.to_be_bytes(), 0u32.to_be_bytes()].concat()
    }

    fn prepare_heartbeat(&self) -> Vec<u8> {
        [self.id.to_be_bytes(), self.state.current_term.to_be_bytes(), 4u32.to_be_bytes()].concat()
    }

    async fn handle_rpc(&mut self, data: Vec<u8>) {
        let term = u32::from_be_bytes(data[4..8].try_into().unwrap());
        let message_type: u32 = u32::from_be_bytes(data[8..12].try_into().unwrap());

        if term < self.state.current_term && message_type != 3 {
            return;
        }

        let message_type = match message_type {
            0 => MesageType::RequestVote,
            1 => MesageType::RequestVoteResponse,
            2 => MesageType::AppendEntries,
            3 => MesageType::AppendEntriesResponse,
            4 => MesageType::Heartbeat,
            5 => MesageType::HeartbeatResponse,
            6 => MesageType::ClientRequest,
            7 => MesageType::ClientResponse,
            _ => return,
        };
        
        match message_type {
            MesageType::RequestVote => {
                self.handle_request_vote(&data).await;
            }
            MesageType::RequestVoteResponse => {
                self.handle_request_vote_response(&data).await;
            }
            MesageType::AppendEntries => {
                self.handle_append_entries(data).await;
            }
            MesageType::AppendEntriesResponse => {
                self.handle_append_entries_response(&data).await;
            }
            MesageType::Heartbeat => {
                self.handle_heartbeat().await;
            }
            MesageType::HeartbeatResponse => {
                self.handle_heartbeat_response().await;
            }
            MesageType::ClientRequest => {
                self.handle_client_request(data).await;
            }
            MesageType::ClientResponse => {
                // TODO: get implementation from user based on the application
                println!("Received client response: {:?}", data);
                let data = u32::from_be_bytes(data[12..16].try_into().unwrap());
                if data == 1 {
                    println!("Consensus reached!");
                } else {
                    println!("Consensus not reached!");
                }
            }
        }
    }

    async fn handle_client_request(&mut self, data: Vec<u8>) {
        if self.state.state != RaftState::Leader {
            return;
        }

        let term = self.state.current_term;
        let command = LogCommand::Set;
        let data = u32::from_be_bytes(data[12..16].try_into().unwrap());
        let entry = LogEntry { leader_id: self.id, server_id: self.id, term, command, data };
        println!("Received client request: {:?}", entry);
        self.state.previous_log_index += 1;
        self.state.commit_index += 1;
        self.state.current_term += 1;
        self.write_buffer.push(entry);
    }

    async fn handle_request_vote(&mut self, data: &[u8]) {
        // Only Follower can vote, because Candidate voted for itself
        let candidate_id = u32::from_be_bytes(data[0..4].try_into().unwrap());
        let candidate_term = u32::from_be_bytes(data[4..8].try_into().unwrap());

        if self.state.state != RaftState::Follower {
            return;
        }

        if candidate_term < self.state.current_term {
            return;
        }

        self.state.voted_for = Some(candidate_id);
        self.state.current_term = candidate_term;

        // get candidate address from config
        let candidate_address = self.config.id_to_address_mapping.get(&candidate_id);
        if candidate_address.is_none() {
            // no dynamic membership changes
            println!("Candidate address not found");
            return;
        }
        let candidate_ip = candidate_address.unwrap().split(":").collect::<Vec<&str>>()[0];
        let candidate_port = candidate_address.unwrap().split(":").collect::<Vec<&str>>()[1];

        let data = [self.id.to_be_bytes(), self.state.current_term.to_be_bytes(), 1u32.to_be_bytes(), 1u32.to_be_bytes()].concat();

        let voteresponse = self.network_manager.send(candidate_ip, candidate_port, &data).await;
        if let Err(e) = voteresponse {
            eprintln!("Failed to send vote response: {}", e);
        }
    }

    async fn handle_request_vote_response(&mut self, data: &[u8]) {
        if self.state.state != RaftState::Candidate {
            return;
        }

        let voter_id = u32::from_be_bytes(data[0..4].try_into().unwrap());
        let term = u32::from_be_bytes(data[4..8].try_into().unwrap());
        let vote_granted = u32::from_be_bytes(data[8..12].try_into().unwrap()) == 1;

        // if follower and your term and candidate term are same, and your id is less than candidate id, vote for candidate
        // leader preference
        if term >= self.state.current_term && self.id > voter_id && self.state.state == RaftState::Candidate {
            self.state.state = RaftState::Follower;
        }

        self.state.votes_received.insert(voter_id, vote_granted);
        println!("Votes received: {:?}", self.state.votes_received);
    }

    async fn handle_append_entries(&mut self, data: Vec<u8>) {
        if self.state.state != RaftState::Follower {
            return;
        }

        self.state.last_heartbeat = Instant::now();

        let id = u32::from_be_bytes(data[0..4].try_into().unwrap());
        let leader_term = u32::from_be_bytes(data[4..8].try_into().unwrap());

        if leader_term < self.state.current_term {
            return;
        }

        let message_type = u32::from_be_bytes(data[8..12].try_into().unwrap());
        if message_type != 2 {
            return;
        }
        
        let prev_log_index = u32::from_be_bytes(data[12..16].try_into().unwrap());
        if prev_log_index > self.state.previous_log_index {
            self.state.previous_log_index = prev_log_index;
        } else {
            return;
        }

        let commit_index = u32::from_be_bytes(data[16..20].try_into().unwrap());
        if commit_index > self.state.commit_index {
            self.state.commit_index = commit_index;
        } else {
            return;
        }

        let set_command = 1u32.to_be_bytes();
        let data = [set_command.to_vec(), 42u32.to_be_bytes().to_vec()].concat();

        let _ = self.append_log(id, leader_term, &data).await;

        self.state.current_term += 1; // increment term on successful append for follower
        
        let response = [self.id.to_be_bytes(), self.state.current_term.to_be_bytes(), 3u32.to_be_bytes(), 1u32.to_be_bytes()].concat();
        let leader_address = self.config.id_to_address_mapping.get(&id).unwrap();
        let leader_ip = leader_address.split(":").collect::<Vec<&str>>()[0];
        let leader_port = leader_address.split(":").collect::<Vec<&str>>()[1];
        println!("Sending append entries response to leader: {}", id);
        if let Err(e) = self.network_manager.send(leader_ip, leader_port, &response).await {
            eprintln!("Failed to send append entries response: {}", e);
        }

    }

    async fn handle_append_entries_response(&mut self, data: &[u8]) {
        if self.state.state != RaftState::Leader {
            return;
        }

        let sender_id = u32::from_be_bytes(data[0..4].try_into().unwrap());
        let term = u32::from_be_bytes(data[4..8].try_into().unwrap());
        let success = u32::from_be_bytes(data[12..16].try_into().unwrap()) == 1;

        println!("Append entries response from peer: {} with term: {} and success: {}", sender_id, term, success);
        
        if term > self.state.current_term {
            return;
        }

        if success {
            // check if you got a quorum
            let last_log_index = self.state.previous_log_index;
            self.state.match_index[sender_id as usize - 1] = last_log_index;
            self.state.next_index[sender_id as usize - 1] = last_log_index + 1;

            let mut match_indices = self.state.match_index.clone();
            match_indices.sort();
            let quorum_index = match_indices[self.peers.len() / 2];
            if quorum_index >= self.state.commit_index {
                self.state.commit_index = quorum_index;
                // return client response
                let response_data = [self.id.to_be_bytes(), self.state.current_term.to_be_bytes(), 7u32.to_be_bytes(), 1u32.to_be_bytes()].concat();
                if let Err(e) = self.network_manager.send(self.config.address.as_str(), self.config.port.to_string().as_str(), &response_data).await {
                    eprintln!("Failed to send client response: {}", e);
                }
                println!("Quorum decision reached to commit index: {}", self.state.commit_index);
            }
        } else {
            self.state.next_index[sender_id as usize - 1] -= 1;
        }
    }

    async fn handle_heartbeat(&mut self) {
        if self.state.state != RaftState::Follower || self.state.state != RaftState::Candidate {
            return;
        }
        self.state.last_heartbeat = Instant::now();
    }

    async fn handle_heartbeat_response(&mut self) {
        // Noop
    }

    async fn append_log(&mut self, id: u32, term: u32, data: &[u8]) {
        println!("Appending logs to disk from peer: {} to server: {}", id, self.id);
        println!("Data: {:?}", data);

        let log_entries = self.deserialize_log_entries(id, term, data);

        // Log Compaction
        if self.state.log.len() > 50 {
            self.state.log.truncate(25);
        }

        for entry in log_entries {
            self.state.log.push_front(entry.clone());
            let serialized_entry = bincode::serialize(&entry).unwrap();
            if let Err(e) = self.storage.store(&serialized_entry).await {
                eprintln!("Failed to store log entry to disk: {}", e);
            }
        }

        println!("Log after appending: {:?}", self.state.log);
    }

    fn deserialize_log_entries(&self, sender_id: u32, term: u32, data: &[u8]) -> Vec<LogEntry> {
        let mut entries = Vec::new();
        let mut index = 0;
        while index < data.len() {
            let command_type = u32::from_be_bytes(data[index..index + 4].try_into().unwrap());
            index += 4;
            let command = match command_type {
                0 => LogCommand::Noop,
                1 => LogCommand::Set,
                2 => LogCommand::Delete,
                _ => panic!("Invalid command type"),
            };
            let entry_data = u32::from_be_bytes(data[index..index + 4].try_into().unwrap());
            index += 4;

            let entry = LogEntry {
                leader_id: sender_id,
                server_id: self.id,
                term,
                command,
                data: entry_data,
            };
            entries.push(entry);
        }
        entries
    }

    fn is_quorum(&self, votes: u32) -> bool {
        votes > (self.peers.len() / 2).try_into().unwrap_or_default()
    }

    async fn stop(&self) {
        if let Err(e) = self.network_manager.close().await {
            eprintln!("Failed to close network manager: {}", e);
        }
    }
}
