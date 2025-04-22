use bincode::{Decode, Encode};
use kameo::Actor;
use kameo::message::{Context, Message};
use libp2p::PeerId;
use std::time::Instant;
use uuid::Uuid;

// Number of partitions in the system
pub const NUM_PARTITIONS: u16 = 10;
// Timeout after which a leader is considered failed
pub const LEADER_TIMEOUT_MS: u64 = 5000;

/// Leadership status of a node for a particular partition
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeadershipStatus {
    /// Node is the confirmed leader for this partition
    Leader,
    /// Node is attempting to become leader
    Candidate,
    /// Node is not the leader
    Follower,
}

/// Information about a partition's leadership
#[derive(Debug, Clone)]
pub struct PartitionLeaderInfo {
    /// The partition id
    pub partition_id: u16,
    /// The current term/epoch for this partition
    pub term: u64,
    /// The current leader's peer ID
    pub leader_id: PeerId,
    /// Whether this leadership has been confirmed by a quorum
    pub confirmed: bool,
    /// When the last heartbeat was received from this leader
    pub last_heartbeat: Instant,
}

/// A leadership claim message sent by a node
#[derive(Debug, Clone, Encode, Decode)]
pub struct LeadershipClaim {
    /// The partition this claim is for
    pub partition_id: u16,
    /// The term the node is claiming
    pub term: u64,
    /// The node claiming leadership
    #[bincode(with_serde)]
    pub node_id: PeerId,
    /// Unique identifier for this claim
    #[bincode(with_serde)]
    pub claim_id: Uuid,
}

/// A leadership confirmation message
#[derive(Debug, Clone, Encode, Decode)]
pub struct LeadershipConfirmation {
    /// The partition this confirmation is for
    pub partition_id: u16,
    /// The term being confirmed
    pub term: u64,
    /// The node being confirmed as leader
    #[bincode(with_serde)]
    pub leader_id: PeerId,
    /// The node sending the confirmation
    #[bincode(with_serde)]
    pub confirming_node: PeerId,
    /// The claim ID being confirmed
    #[bincode(with_serde)]
    pub claim_id: Uuid,
}

/// Gossip messages for the consensus protocol
#[derive(Debug, Clone, Encode, Decode)]
pub enum ConsensusMessage {
    /// A node is claiming leadership
    Claim(LeadershipClaim),
    /// A node is confirming a leadership claim
    Confirmation(LeadershipConfirmation),
    /// A leader sending a heartbeat
    Heartbeat {
        partition_id: u16,
        term: u64,
        #[bincode(with_serde)]
        leader_id: PeerId,
    },
}

/// Events emitted by the consensus module
#[derive(Debug, Clone)]
pub enum ConsensusEvent {
    /// This node became leader for a partition
    BecameLeader { partition_id: u16, term: u64 },
    /// This node lost leadership for a partition
    LostLeadership { partition_id: u16 },
    /// The leader changed for a partition
    LeaderChanged {
        partition_id: u16,
        new_leader: PeerId,
        term: u64,
    },
}

/// Result of a leadership claim attempt
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeadershipClaimResult {
    /// Successfully claimed leadership
    Success,
    /// Failed because a higher term was known
    HigherTermExists(u64),
    /// Failed because not enough nodes confirmed
    InsufficientConfirmation,
    /// Failed for some other reason
    OtherFailure(String),
}

// Re-export actor modules
pub mod gossip;
pub mod leadership;
pub mod term_store;

// Create a type alias for common error type
pub type ConsensusError = Box<dyn std::error::Error + Send + Sync>;
