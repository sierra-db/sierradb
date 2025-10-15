use std::time::Duration;

use kameo::prelude::*;
use serde::{Deserialize, Serialize};
use sierradb::{
    bucket::PartitionId,
    database::{CurrentVersion, ExpectedVersion},
};
use thiserror::Error;
use tracing::error;

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum WriteError {
    #[error("all replica nodes failed to process the write request")]
    AllReplicasFailed,

    #[error("circuit breaker open: estimated recovery time: {estimated_recovery_time:?}")]
    CircuitBreakerOpen {
        estimated_recovery_time: Option<Duration>,
    },

    #[error("failed to update coordinator confirmation count: {0}")]
    ConfirmationFailed(String),

    #[error("failed to update replication confirmation count: {0}")]
    ReplicationConfirmationFailed(String),

    #[error("database operation failed: {0}")]
    DatabaseOperationFailed(String),

    #[error(
        "insufficient healthy replicas for write quorum ({available}/{required} replicas available)"
    )]
    InsufficientHealthyReplicas { available: u8, required: u8 },

    #[error("coordinator is not healthy")]
    InvalidSender,

    #[error("stale write: coordinator has been alive longer than our local record of it")]
    StaleWrite,

    #[error("transaction is missing an expected partition sequence")]
    MissingExpectedPartitionSequence,

    #[error("partition {partition_id} is not owned by this node")]
    PartitionNotOwned { partition_id: PartitionId },

    #[error(
        "write replication quorum not achieved ({confirmed}/{required} confirmations received)"
    )]
    ReplicationQuorumFailed { confirmed: u8, required: u8 },

    #[error("write operation timed out")]
    RequestTimeout,

    #[error("write request exceeded maximum forward hops ({max} allowed)")]
    MaximumForwardsExceeded { max: u8 },

    #[error("remote operation failed: {0}")]
    RemoteOperationFailed(Box<RemoteSendError<WriteError>>),

    #[error("current partition sequence is {current} but expected {expected}")]
    WrongExpectedSequence {
        current: CurrentVersion,
        expected: ExpectedVersion,
    },
}

impl From<sierradb::error::WriteError> for WriteError {
    fn from(err: sierradb::error::WriteError) -> Self {
        WriteError::DatabaseOperationFailed(err.to_string())
    }
}

impl From<RemoteSendError<WriteError>> for WriteError {
    fn from(err: RemoteSendError<WriteError>) -> Self {
        WriteError::RemoteOperationFailed(Box::new(err))
    }
}

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum ConfirmTransactionError {
    #[error("events length mismatch")]
    EventsLengthMismatch,
    #[error("event id mismatch")]
    EventIdMismatch,
    #[error("partition sequence mismatch (expected {expected}, got {actual})")]
    PartitionSequenceMismatch { expected: u64, actual: u64 },
    #[error("transaction not found")]
    TransactionNotFound,
    #[error("read error: {0}")]
    Read(String),
    #[error("write error: {0}")]
    Write(String),
}
