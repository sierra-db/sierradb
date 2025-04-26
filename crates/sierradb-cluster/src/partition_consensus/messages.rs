use bincode::{Decode, Encode};
use libp2p::PeerId;
use std::collections::{HashMap, HashSet};

/// Regular heartbeat from a node to indicate it's still alive
#[derive(Debug, Clone, Encode, Decode)]
pub struct Heartbeat {
    #[bincode(with_serde)]
    pub node_id: PeerId,
    pub owned_partitions: Vec<u16>, // Partitions owned by this node
}

/// Messages related to partition ownership and node presence
#[derive(Debug, Clone, Encode, Decode)]
pub enum OwnershipMessage {
    /// Request for current ownership info (typically sent by new nodes joining)
    OwnershipRequest {
        #[bincode(with_serde)]
        node_id: PeerId,
        owned_partitions: Vec<u16>, // Partitions owned by the requesting node
    },

    /// Response with current ownership state
    OwnershipResponse {
        #[bincode(with_serde)]
        partition_owners: HashMap<u16, PeerId>, // partition_id -> owner
        #[bincode(with_serde)]
        active_nodes: HashSet<PeerId>, // set of nodes considered active
    },
}
