use libp2p::request_response::OutboundFailure;
use libp2p::{BehaviourBuilderError, PeerId};
use serde::{Deserialize, Serialize};
use sierradb::bucket::PartitionId;
use thiserror::Error;

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum SwarmError {
    #[error("insufficient partitions alive for quorum ({alive}/{required})")]
    InsufficientPartitionsForQuorum { alive: u8, required: u8 },
    #[error(transparent)]
    #[serde(skip)]
    OutboundFailure(#[from] OutboundFailure),
    #[error("partition actor not found")]
    PartitionActorNotFound { partition_id: PartitionId },
    #[error("partition unavailable")]
    PartitionUnavailable,
    #[error("no available leaders")]
    NoAvailableLeaders,
    #[error("quorum not achieved")]
    QuorumNotAchieved { confirmed: u8, required: u8 },
    #[error("write timed out")]
    WriteTimeout,
    #[error("too many forwards")]
    TooManyForwards,
    #[error("read error: {0}")]
    Read(String),
    #[error("write error: {0}")]
    Write(String),
    #[error("wrong leader node for append")]
    WrongLeaderNode { correct_leader: PeerId },
    #[error(transparent)]
    #[serde(skip)]
    SwarmBuilderError(#[from] BehaviourBuilderError),
}
