use eventus_core::bucket::BucketId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum SwarmError {
    #[error("peer not found for bucket {bucket_id}")]
    BucketPeerNotFound { bucket_id: BucketId },
    #[error("failed to encode consensus message: {0}")]
    EncodeConsensusMessage(String),
    #[error("bucket id {bucket_id} not found for request")]
    RequestBucketIdNotFound { bucket_id: BucketId },
    #[error("stream version mismatch")]
    StreamVersionMismatch,
    #[error("subscription error: {0}")]
    Subscription(String),
    #[error("swarm not running")]
    SwarmNotRunning,
    #[error("write error: {0}")]
    Write(String),
}
