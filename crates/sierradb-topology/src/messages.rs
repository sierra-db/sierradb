use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use bincode::{Decode, Encode};
use libp2p::PeerId;
use sierradb::bucket::PartitionId;

/// Regular heartbeat from a node to indicate it's still alive
#[derive(Clone, Debug, Encode, Decode)]
pub struct Heartbeat<T> {
    pub cluster_ref: T,
    pub owned_partitions: HashSet<PartitionId>,
    pub alive_since: u64,
    pub node_index: usize,
    pub total_node_count: usize,
}

/// Messages related to partition ownership and node presence
#[derive(Clone, Debug, Encode, Decode)]
pub enum OwnershipMessage<T: Eq + Hash> {
    /// Request for current ownership info (typically sent by new nodes joining)
    OwnershipRequest {
        cluster_ref: T,
        owned_partitions: HashSet<PartitionId>,
        alive_since: u64,
        node_index: usize,
        total_node_count: usize,
    },

    /// Response with current ownership state
    OwnershipResponse {
        /// Maps partition_id to list of replica nodes
        partition_replicas: HashMap<T, HashSet<PartitionId>>,
        /// Set of nodes considered active with their configured indices
        #[bincode(with_serde)]
        active_nodes: HashMap<PeerId, (u64, usize)>, // PeerId -> (alive_since, node_index)
    },
}

impl<T: Eq + Hash> OwnershipMessage<T> {
    pub fn map<U, F>(self, mut f: F) -> OwnershipMessage<U>
    where
        U: Eq + Hash,
        F: FnMut(T) -> U,
    {
        match self {
            OwnershipMessage::OwnershipRequest {
                cluster_ref,
                owned_partitions,
                alive_since,
                node_index,
                total_node_count,
            } => OwnershipMessage::OwnershipRequest {
                cluster_ref: f(cluster_ref),
                owned_partitions,
                alive_since,
                node_index,
                total_node_count,
            },
            OwnershipMessage::OwnershipResponse {
                partition_replicas,
                active_nodes,
            } => OwnershipMessage::OwnershipResponse {
                partition_replicas: partition_replicas
                    .into_iter()
                    .map(move |(key, partition_ids)| (f(key), partition_ids))
                    .collect(),
                active_nodes,
            },
        }
    }
}
