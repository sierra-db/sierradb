use libp2p::request_response::OutboundFailure;
use libp2p::{BehaviourBuilderError, PeerId};
use serde::{Deserialize, Serialize};
use sierradb::bucket::{BucketId, PartitionId};
use thiserror::Error;

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum SwarmError {
    #[error("peer not found for bucket {bucket_id}")]
    BucketPeerNotFound { bucket_id: BucketId },
    #[error("insufficient partitions alive for quorum ({alive}/{required})")]
    InsufficientPartitionsForQuorum { alive: u8, required: u8 },
    #[error("failed to encode consensus message: {0}")]
    EncodeConsensusMessage(String),
    #[error(transparent)]
    #[serde(skip)]
    OutboundFailure(#[from] OutboundFailure),
    #[error("partition actor not found")]
    PartitionActorNotFound { partition_id: PartitionId },
    #[error("failed to send write to partition actor")]
    PartitionActorSendError,
    #[error("failed to send to swarm actor")]
    SwarmActorSendError,
    #[error("partition unavailable")]
    PartitionUnavailable,
    #[error("bucket id {bucket_id} not found for request")]
    RequestBucketIdNotFound { bucket_id: BucketId },
    #[error("stream version mismatch")]
    StreamVersionMismatch,
    #[error("no available leaders")]
    NoAvailableLeaders,
    #[error("quorum not achieved")]
    QuorumNotAchieved { confirmed: u8, required: u8 },
    #[error("subscription error: {0}")]
    Subscription(String),
    #[error("swarm not running")]
    SwarmNotRunning,
    #[error("timeout")]
    Timeout,
    #[error("too many forwards")]
    TooManyForwards,
    #[error("write error: {0}")]
    Write(String),
    #[error("wrong leader node for append")]
    WrongLeaderNode { correct_leader: PeerId },
    #[error(transparent)]
    #[serde(skip)]
    SwarmBuilderError(#[from] BehaviourBuilderError),
}
