// organization : SpacewalkHq
// License : MIT License

use crate::cluster::{ClusterConfig, NodeMeta};
use crate::log::get_logger;
use crate::network::{NetworkLayer, TCPManager};
use crate::storage::{LocalStorage, Storage, CHECKSUM_LEN};
use serde::{Deserialize, Serialize};
use slog::{error, info, o};
use std::collections::{HashMap, VecDeque};
use std::io::Cursor;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::time::sleep;

#[derive(Debug, Clone, PartialEq)]
enum RaftState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
enum MessageType {
    RequestVote,
    RequestVoteResponse,
    AppendEntries,
    AppendEntriesResponse,
    Heartbeat,
    HeartbeatResponse,
    ClientRequest,
    ClientResponse,
    RepairRequest,
    RepairResponse,
    // dynamic membership changes
    JoinRequest,
    JoinResponse,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LogCommand {
    Noop,
    Set,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogEntry {
    pub leader_id: u32,
    pub server_id: u32,
    pub term: u32,
    pub command: LogCommand,
    pub data: u32,
}

#[derive(Debug)]
pub struct ServerConfig {
    pub election_timeout: Duration,
    pub address: SocketAddr,
    // Include default leader and leadership preferences
    pub default_leader: Option<u32>,
    pub leadership_preferences: HashMap<u32, u32>,
    pub storage_location: Option<String>,
}

pub struct Server {
    pub id: u32,
    state: ServerState,
    config: ServerConfig,
    network_manager: TCPManager,
    cluster_config: ClusterConfig,
    // Add write buffer and debounce timer
    write_buffer: Vec<LogEntry>,
    debounce_timer: Instant,
    storage: LocalStorage,
    log: slog::Logger,
}

impl Server {
    pub async fn new(id: u32, config: ServerConfig, cluster_config: ClusterConfig) -> Server {
        let log = get_logger();
        let log = log.new(
            o!("ip" => config.address.ip().to_string(), "port" => config.address.port(), "id" => id),
        );

        let peer_count = cluster_config.peer_count(id);
        let state = ServerState {
            current_term: 0,
            state: RaftState::Follower,
            voted_for: None,
            log: VecDeque::new(),
            commit_index: 0,
            previous_log_index: 0,
            next_index: vec![0; peer_count],
            match_index: vec![0; peer_count],
            election_timeout: config.election_timeout + Duration::from_millis(20 * id as u64),
            last_heartbeat: Instant::now(),
            votes_received: HashMap::new(),
        };
        let network_manager = TCPManager::new(config.address);

        // if storage location is provided, use it else set empty string to use default location
        let storage_location = match config.storage_location.clone() {
            Some(location) => location + &format!("server_{}.log", id),
            None => format!("server_{}.log", id),
        };
        let storage = LocalStorage::new(storage_location).await;

        Server {
            id,
            state,
            config,
            network_manager,
            cluster_config,
            write_buffer: Vec::new(),
            debounce_timer: Instant::now(),
            storage,
            log,
        }
    }

    pub async fn start(&mut self) {
        if let Err(e) = self.network_manager.open().await {
            error!(self.log, "Failed to open network manager: {}", e);
            return;
        }

        // there should be at-least 3 peers to form a quorum
        if self.peers().len() < 2 {
            error!(self.log, "At least 3 peers are required to form a quorum");
            return;
        }

        // if the storage path is not exist, create it
        if let Err(e) = self.storage.check_storage().await {
            error!(self.log, "Failed to check storage: {}", e);
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

    pub fn is_leader(&self) -> bool {
        self.state.state == RaftState::Leader
    }

    async fn follower(&mut self) {
        if self.state.state != RaftState::Follower {
            return;
        }

        let log_byte = self.storage.retrieve().await.unwrap();
        let log_entry_size = std::mem::size_of::<LogEntry>();

        // Data integrity check failed
        // try repair the log from other peers
        if log_byte.len() % (log_entry_size + CHECKSUM_LEN) != 0 {
            error!(self.log, "Data integrity check failed");

            // step1 delete the log file
            if let Err(e) = self.storage.delete().await {
                error!(self.log, "Failed to delete log file: {}", e);
            }

            // step2 get the log from other peers
            // ping all the peers to get the log
            let addresses: Vec<SocketAddr> = self.peers_address();
            let data = [
                self.id.to_be_bytes(),
                0u32.to_be_bytes(),
                2u32.to_be_bytes(),
            ]
            .concat();
            self.network_manager
                .broadcast(&data, &addresses)
                .await
                .unwrap();
            return;
        }

        let mut cursor = Cursor::new(&log_byte);
        loop {
            let mut bytes_data = vec![0u8; log_entry_size + CHECKSUM_LEN];
            if cursor.read_exact(&mut bytes_data).await.is_err() {
                break;
            }
            bytes_data = bytes_data[0..log_entry_size].to_vec();

            let log_entry = self.deserialize_log_entries(&bytes_data);
            if log_entry.term > self.state.current_term {
                self.state.current_term = log_entry.term;
            }
            self.state.log.push_front(log_entry);
        }
        info!(
            self.log,
            "Log after reading from disk: {:?}", self.state.log
        );

        self.state.match_index = vec![0; self.peer_count() + 1];
        self.state.next_index = vec![0; self.peer_count() + 1];

        info!(self.log, "Server {} is a follower", self.id);
        // default leader
        if self.state.current_term == 0 {
            self.state.current_term += 1;
            if let Some(leader_id) = self.config.default_leader {
                if self.id == leader_id {
                    self.state.state = RaftState::Leader;
                    return;
                }
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
        info!(self.log, "Server {} is a candidate", self.id);
        self.state.last_heartbeat = Instant::now(); // reset election timeout

        self.state.current_term += 1;

        // Vote for self
        self.state.voted_for = Some(self.id);
        self.state.votes_received.insert(self.id, true);

        // TODO: Send RequestVote RPCs with leadership preferences
        let data = self.prepare_request_vote(self.id, self.state.current_term);
        let addresses: Vec<SocketAddr> = self.peers_address();
        info!(
            self.log,
            "Starting election, id: {}, term: {}", self.id, self.state.current_term
        );
        let _ = self.network_manager.broadcast(&data, &addresses).await;

        loop {
            let timeout_duration = self.state.election_timeout;

            let timeout_future = async {
                sleep(timeout_duration).await;
            };

            let rpc_future = self.receive_rpc();
            tokio::select! {
                _ = timeout_future => {
                    if Instant::now().duration_since(self.state.last_heartbeat) >= timeout_duration {
                        info!(self.log, "Election timeout");
                        self.state.state = RaftState::Follower;
                        self.state.votes_received.clear();
                        break;
                    }
                }
                _ = rpc_future => {
                    if self.is_quorum(self.state.votes_received.len() as u32) {
                        info!(self.log, "Quorum reached");
                        info!(self.log, "I am the leader {}", self.id);
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
        info!(self.log, "Server {} is the leader", self.id);

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
                    let addresses: Vec<SocketAddr> = self.peers_address();

                    if let Err(e) = self.network_manager.broadcast(&heartbeat_data, &addresses).await {
                        error!(self.log, "Failed to send heartbeats: {}", e);
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
                            let data = bincode::serialize(&entry).unwrap();
                            self.persist_to_disk(self.id, &data).await;
                        }

                        let addresses: Vec<SocketAddr> = self.peers_address();
                        if let Err(e) = self.network_manager.broadcast(&append_batch, &addresses).await {
                            error!(self.log, "Failed to send append batch: {}", e);
                        }

                        self.write_buffer.clear();
                        self.debounce_timer = Instant::now();
                    }
                },
            }
        }
    }

    async fn receive_rpc(&mut self) {
        let data = self.network_manager.receive().await.unwrap();
        self.handle_rpc(data).await;
    }

    fn prepare_append_batch(
        &self,
        id: u32,
        term: u32,
        prev_log_index: u32,
        commit_index: u32,
        write_buffer: Vec<LogEntry>,
    ) -> Vec<u8> {
        let mut data = [
            id.to_be_bytes(),
            term.to_be_bytes(),
            2u32.to_be_bytes(),
            prev_log_index.to_be_bytes(),
            commit_index.to_be_bytes(),
        ]
        .concat();
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
        [
            self.id.to_be_bytes(),
            self.state.current_term.to_be_bytes(),
            4u32.to_be_bytes(),
        ]
        .concat()
    }

    async fn handle_rpc(&mut self, data: Vec<u8>) {
        let message_type: u32 = u32::from_be_bytes(data[8..12].try_into().unwrap());

        let message_type = match message_type {
            0 => MessageType::RequestVote,
            1 => MessageType::RequestVoteResponse,
            2 => MessageType::AppendEntries,
            3 => MessageType::AppendEntriesResponse,
            4 => MessageType::Heartbeat,
            5 => MessageType::HeartbeatResponse,
            6 => MessageType::ClientRequest,
            7 => MessageType::ClientResponse,
            8 => MessageType::RepairRequest,
            9 => MessageType::RepairResponse,
            10 => MessageType::JoinRequest,
            11 => MessageType::JoinResponse,
            _ => return,
        };

        match message_type {
            MessageType::RequestVote => {
                self.handle_request_vote(&data).await;
            }
            MessageType::RequestVoteResponse => {
                self.handle_request_vote_response(&data).await;
            }
            MessageType::AppendEntries => {
                self.handle_append_entries(data).await;
            }
            MessageType::AppendEntriesResponse => {
                self.handle_append_entries_response(&data).await;
            }
            MessageType::Heartbeat => {
                self.handle_heartbeat(&data).await;
            }
            MessageType::HeartbeatResponse => {
                self.handle_heartbeat_response().await;
            }
            MessageType::ClientRequest => {
                self.handle_client_request(data).await;
            }
            MessageType::ClientResponse => {
                // TODO: get implementation from user based on the application
                info!(self.log, "Received client response: {:?}", data);
                let data = u32::from_be_bytes(data[12..16].try_into().unwrap());
                if data == 1 {
                    info!(self.log, "Consensus reached!");
                } else {
                    info!(self.log, "Consensus not reached!");
                }
            }
            MessageType::RepairRequest => {
                self.handle_repair_request(&data).await;
            }
            MessageType::RepairResponse => {
                self.handle_repair_response(&data).await;
            }
            MessageType::JoinRequest => {
                info!(
                    self.log,
                    "Received join request: {:?}",
                    String::from_utf8_lossy(&data)
                );
                self.handle_join_request(&data).await;
            }
            MessageType::JoinResponse => {
                self.handle_join_response(&data).await;
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
        let entry = LogEntry {
            leader_id: self.id,
            server_id: self.id,
            term,
            command,
            data,
        };
        info!(self.log, "Received client request: {:?}", entry);
        self.write_buffer.push(entry.clone());

        self.state.log.push_front(entry);
        self.state.previous_log_index += 1;
        self.state.commit_index += 1;
        self.state.current_term += 1;
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
        let candidate_address = self.cluster_config.address(candidate_id);
        if candidate_address.is_none() {
            // no dynamic membership changes
            info!(self.log, "Candidate address not found");
            return;
        }

        let data = [
            self.id.to_be_bytes(),
            self.state.current_term.to_be_bytes(),
            1u32.to_be_bytes(),
            1u32.to_be_bytes(),
        ]
        .concat();

        let voteresponse = self
            .network_manager
            .send(&candidate_address.unwrap(), &data)
            .await;
        if let Err(e) = voteresponse {
            error!(self.log, "Failed to send vote response: {}", e);
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
        if term >= self.state.current_term
            && self.id > voter_id
            && self.state.state == RaftState::Candidate
        {
            self.state.state = RaftState::Follower;
        }

        self.state.votes_received.insert(voter_id, vote_granted);
        info!(self.log, "Votes received: {:?}", self.state.votes_received);
    }

    async fn handle_append_entries(&mut self, data: Vec<u8>) {
        if self.state.state != RaftState::Follower {
            return;
        }

        self.state.last_heartbeat = Instant::now();

        let id = u32::from_be_bytes(data[0..4].try_into().unwrap());
        let leader_term = u32::from_be_bytes(data[4..8].try_into().unwrap());
        let message_type = u32::from_be_bytes(data[8..12].try_into().unwrap());
        let prev_log_index = u32::from_be_bytes(data[12..16].try_into().unwrap());
        let commit_index = u32::from_be_bytes(data[16..20].try_into().unwrap());
        info!(
            self.log,
            "Node {} received append entries request from Node {}, \
             (term: self={}, receive={}), \
             (prev_log_index: self={}, receive={}), \
             (commit_index: self={}, receive={})",
            self.id,
            id,
            self.state.current_term,
            leader_term,
            self.state.previous_log_index,
            prev_log_index,
            self.state.commit_index,
            commit_index
        );

        if leader_term < self.state.current_term {
            return;
        }

        if message_type != 2 {
            return;
        }

        if prev_log_index > self.state.previous_log_index {
            self.state.previous_log_index = prev_log_index;
        } else {
            return;
        }

        if commit_index > self.state.commit_index {
            self.state.commit_index = commit_index;
        } else {
            return;
        }

        let log_entry: LogEntry = LogEntry {
            leader_id: id,
            server_id: self.id,
            term: leader_term,
            command: LogCommand::Set,
            data: u32::from_be_bytes(data[24..28].try_into().unwrap()),
        };

        // serialize log entry and append to log
        let data = bincode::serialize(&log_entry).unwrap();

        let _ = self.persist_to_disk(id, &data).await;

        self.state.current_term += 1; // increment term on successful append for follower

        let response = [
            self.id.to_be_bytes(),
            self.state.current_term.to_be_bytes(),
            3u32.to_be_bytes(),
            1u32.to_be_bytes(),
        ]
        .concat();

        let leader_address = self.cluster_config.address(id).unwrap();
        info!(
            self.log,
            "Sending append entries response to leader: {}", id
        );
        if let Err(e) = self.network_manager.send(&leader_address, &response).await {
            info!(self.log, "Failed to send append entries response: {}", e);
        }
    }

    async fn handle_append_entries_response(&mut self, data: &[u8]) {
        if self.state.state != RaftState::Leader {
            return;
        }

        let sender_id = u32::from_be_bytes(data[0..4].try_into().unwrap());
        let term = u32::from_be_bytes(data[4..8].try_into().unwrap());
        let success = u32::from_be_bytes(data[12..16].try_into().unwrap()) == 1;

        info!(
            self.log,
            "Append entries response from peer: {} with term: {} and success: {}",
            sender_id,
            term,
            success
        );

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
            let quorum_index = match_indices[self.peer_count() / 2];

            info!(
                self.log,
                "Append entry response received from node {}: (match_index = {}, next_index = {}), current quorum_index: {}", 
                sender_id,
                self.state.match_index[sender_id as usize - 1],
                self.state.next_index[sender_id as usize - 1],
                quorum_index
            );

            if quorum_index >= self.state.commit_index {
                self.state.commit_index = quorum_index;
                // return client response
                let response_data = [
                    self.id.to_be_bytes(),
                    self.state.current_term.to_be_bytes(),
                    7u32.to_be_bytes(),
                    1u32.to_be_bytes(),
                ]
                .concat();
                if let Err(e) = self
                    .network_manager
                    .send(&self.config.address, &response_data)
                    .await
                {
                    error!(self.log, "Failed to send client response: {}", e);
                }
                info!(
                    self.log,
                    "Quorum decision reached to commit index: {}", self.state.commit_index
                );
            }
        } else {
            self.state.next_index[sender_id as usize - 1] -= 1;
        }
    }

    async fn handle_heartbeat(&mut self, data: &[u8]) {
        if self.state.state != RaftState::Follower || self.state.state != RaftState::Candidate {
            return;
        }
        let term = u32::from_be_bytes(data[4..8].try_into().unwrap());
        if term < self.state.current_term {
            return;
        }

        // if a leader gets a heartbeat from a leader with a higher term, it should step down
        if term > self.state.current_term {
            self.state.state = RaftState::Follower;
        }

        // if a leader gets a heartbeat from a leader same term, it should step down if it has a higher id
        if term == self.state.current_term {
            let leader_id = u32::from_be_bytes(data[0..4].try_into().unwrap());
            if self.config.default_leader.is_none() {
                if self.id < leader_id {
                    self.state.state = RaftState::Follower;
                    self.state.current_term = term;
                }
            } else if self.id != self.config.default_leader.unwrap() {
                self.state.state = RaftState::Follower;
                self.state.current_term = term;
            } else {
                self.state.state = RaftState::Leader;
            }
        }

        self.state.last_heartbeat = Instant::now();
    }

    async fn handle_heartbeat_response(&mut self) {
        // Noop
    }

    async fn handle_repair_request(&mut self, data: &[u8]) {
        if self.state.state != RaftState::Follower || self.state.state != RaftState::Leader {
            return;
        }

        let peer_id = u32::from_be_bytes(data[0..4].try_into().unwrap());

        let log_byte = self.storage.retrieve().await.unwrap();
        let log_entry_size = std::mem::size_of::<LogEntry>();

        // Data integrity check failed
        if log_byte.len() % (log_entry_size + CHECKSUM_LEN) != 0 {
            error!(self.log, "Data integrity check failed");
            return;
        }

        let mut cursor = Cursor::new(&log_byte);
        let mut repair_data = Vec::new();
        loop {
            let mut bytes_data = vec![0u8; log_entry_size + CHECKSUM_LEN];
            if cursor.read_exact(&mut bytes_data).await.is_err() {
                break;
            }
            repair_data.extend_from_slice(&bytes_data[0..log_entry_size]);
        }
        info!(
            self.log,
            "Send repair data from {} to {}, log_entry: {:?}", self.id, peer_id, self.state.log
        );

        let mut response = [
            self.id.to_be_bytes(),
            self.state.current_term.to_be_bytes(),
            9u32.to_be_bytes(),
            1u32.to_be_bytes(),
        ]
        .concat();

        for entry in repair_data {
            response = [response.clone(), entry.to_be_bytes().to_vec()].concat();
        }

        let peer_address = self.cluster_config.address(peer_id).unwrap();
        if let Err(e) = self.network_manager.send(&peer_address, &response).await {
            error!(self.log, "Failed to send repair response: {}", e);
        }
    }

    async fn handle_repair_response(&mut self, data: &[u8]) {
        if self.state.state != RaftState::Leader {
            return;
        }

        if self.storage.turned_malicious().await.is_err() {
            self.state.state = RaftState::Follower;
            return;
        }

        let term = u32::from_be_bytes(data[4..8].try_into().unwrap());
        if term < self.state.current_term {
            return;
        }

        let log_entries = data[16..].to_vec();
        if let Err(e) = self.storage.store(&log_entries).await {
            error!(self.log, "Failed to store log entries to disk: {}", e);
        }
    }

    async fn handle_join_request(&mut self, data: &[u8]) {
        if self.state.state != RaftState::Leader {
            return;
        }

        let node_id = u32::from_be_bytes(data[0..4].try_into().unwrap());
        let term = u32::from_be_bytes(data[4..8].try_into().unwrap());
        let node_ip_address = String::from_utf8(data[12..].to_vec()).unwrap();

        info!(
            self.log,
            "Current cluster nodes: {:?}, want join node: {}",
            self.cluster_config
                .peers()
                .iter()
                .map(|x| x.address.to_string())
                .collect::<Vec<_>>(),
            node_ip_address
        );

        if self.cluster_config.contains_server(node_id) {
            error!(
                self.log,
                "Node already exists in the cluster, Ignoring join request."
            );
            return;
        }

        if term != 0 {
            error!(self.log, "Invalid term for join request, term should be 0.");
            return;
        }

        self.cluster_config
            .add_server((node_id, node_ip_address.parse::<SocketAddr>().unwrap()).into());

        let mut response = [
            self.id.to_be_bytes(),
            self.state.current_term.to_be_bytes(),
            11u32.to_be_bytes(),
        ]
        .concat();
        response.extend_from_slice(&self.state.commit_index.to_be_bytes());
        response.extend_from_slice(&self.state.previous_log_index.to_be_bytes());
        response.extend_from_slice(&self.peer_count().to_be_bytes());

        let peer_address = self.cluster_config.address(node_id).unwrap();
        if let Err(e) = self.network_manager.send(&peer_address, &response).await {
            error!(self.log, "Failed to send join response: {}", e);
        }
    }

    async fn handle_join_response(&mut self, data: &[u8]) {
        if self.state.state != RaftState::Follower {
            return;
        }

        let leader_id = u32::from_be_bytes(data[0..4].try_into().unwrap());
        let current_term = u32::from_be_bytes(data[4..8].try_into().unwrap());
        let commit_index = u32::from_be_bytes(data[12..16].try_into().unwrap());
        let previous_log_index = u32::from_be_bytes(data[16..20].try_into().unwrap());
        // FIXME
        let _peers_count = u32::from_be_bytes(data[20..24].try_into().unwrap());

        self.state.current_term = current_term;
        self.state.commit_index = commit_index;
        self.state.previous_log_index = previous_log_index;

        let request_data = [
            self.id.to_be_bytes(),
            self.state.current_term.to_be_bytes(),
            8u32.to_be_bytes(),
        ]
        .concat();
        let leader_address = self.cluster_config.address(leader_id).unwrap();

        if let Err(e) = self
            .network_manager
            .send(&leader_address, &request_data)
            .await
        {
            error!(self.log, "Failed to send repair request: {}", e);
        }

        info!(
            self.log,
            "Joined the cluster with leader: {}, own id: {}", leader_id, self.id
        );
    }

    async fn persist_to_disk(&mut self, id: u32, data: &[u8]) {
        info!(
            self.log,
            "Persisting logs to disk from peer: {} to server: {}", id, self.id
        );

        // Log Compaction
        if let Err(e) = self.storage.compaction().await {
            error!(self.log, "Failed to do compaction on disk: {}", e);
        }

        if self.state.state == RaftState::Follower {
            // deserialize log entries and append to log
            let log_entry = self.deserialize_log_entries(data);
            self.state.log.push_front(log_entry);
        }
        if let Err(e) = self.storage.store(data).await {
            error!(self.log, "Failed to store log entry to disk: {}", e);
        }

        info!(
            self.log,
            "Log persistence complete, current log count: {}",
            self.state.log.len()
        );
    }

    fn deserialize_log_entries(&self, data: &[u8]) -> LogEntry {
        // convert data to logEntry using bincode
        bincode::deserialize(data).unwrap()
    }

    fn is_quorum(&self, votes: u32) -> bool {
        votes > (self.peer_count() / 2).try_into().unwrap_or_default()
    }

    #[allow(dead_code)]
    async fn stop(self) {
        if let Err(e) = self.network_manager.close().await {
            error!(self.log, "Failed to close network manager: {}", e);
        }
    }

    // Helper function to access cluster config
    fn peers(&self) -> Vec<&NodeMeta> {
        self.cluster_config.peers_for(self.id)
    }

    fn peers_address(&self) -> Vec<SocketAddr> {
        self.cluster_config.peer_address_for(self.id)
    }

    fn peer_count(&self) -> usize {
        self.peers().len()
    }
}
