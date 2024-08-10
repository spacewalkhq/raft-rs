use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct NodeMeta {
    id: u32,
    address: SocketAddr,
}

impl NodeMeta {
    fn new(id: u32, address: SocketAddr) -> NodeMeta {
        Self { id, address }
    }
}

impl From<(u32, SocketAddr)> for NodeMeta {
    fn from((id, address): (u32, SocketAddr)) -> Self {
        Self::new(id, address)
    }
}

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    peers: Vec<NodeMeta>,
    id_node_map: HashMap<u32, NodeMeta>,
}

impl ClusterConfig {
    pub fn new(peers: Vec<NodeMeta>) -> ClusterConfig {
        let id_node_map = peers
            .clone()
            .into_iter()
            .map(|x| (x.id, x))
            .collect::<HashMap<u32, NodeMeta>>();
        ClusterConfig { peers, id_node_map }
    }

    // Return meta of peers for a node
    pub fn peers(&self, id: u32) -> Vec<&NodeMeta> {
        self.peers.iter().filter(|x| x.id != id).collect::<Vec<_>>()
    }

    // Return address of peers for a node
    pub fn peer_address(&self, id: u32) -> Vec<SocketAddr> {
        self.peers
            .iter()
            .filter(|x| x.id != id)
            .map(|x| x.address)
            .collect::<Vec<_>>()
    }

    pub fn address(&self, id: u32) -> Option<SocketAddr> {
        self.id_node_map.get(&id).map(|x| x.address)
    }
    pub fn meta(&self, node_id: u32) -> Option<&NodeMeta> {
        self.id_node_map.get(&node_id)
    }

    pub fn has_peer(&self, _node_id: u32) -> bool {
        unimplemented!()
    }

    pub fn add_server(&mut self, n: NodeMeta) {
        self.peers.push(n.clone());
        self.id_node_map.insert(n.id, n);
    }

    pub fn peer_count(&self, id: u32) -> usize {
        self.peers(id).len()
    }
}
