use std::time::SystemTimeError;

use kameo::error::SendError;
use sierradb_cluster::ClusterError;
use sierradb_cluster::write::error::WriteError;
use sierradb_protocol::ErrorCode;

use crate::value::Value;

/// Extension trait for converting results to Redis error format.
pub trait MapRedisError<T> {
    fn map_redis_err(self) -> Result<T, Value>;
}

impl<T, E: AsRedisError> MapRedisError<T> for Result<T, E> {
    fn map_redis_err(self) -> Result<T, Value> {
        self.map_err(|err| Value::Error(err.as_redis_error()))
    }
}

/// Trait for converting errors to Redis-compatible error strings.
pub trait AsRedisError {
    fn as_redis_error(&self) -> String;
}

impl<M, E: AsRedisError> AsRedisError for SendError<M, E> {
    fn as_redis_error(&self) -> String {
        match self {
            SendError::ActorNotRunning(_) => ErrorCode::ActorDown.with_message("Actor not running"),
            SendError::ActorStopped => {
                ErrorCode::ActorStopped.with_message("Actor died when executing request")
            }
            SendError::MailboxFull(_) => {
                ErrorCode::MailboxFull.with_message("Actor mailbox is full")
            }
            SendError::HandlerError(err) => err.as_redis_error(),
            SendError::Timeout(_) => ErrorCode::Timeout.to_string(),
        }
    }
}

impl AsRedisError for WriteError {
    fn as_redis_error(&self) -> String {
        let code = match self {
            WriteError::AllReplicasFailed => ErrorCode::AllReplicasFailed,
            WriteError::BufferEvicted => ErrorCode::BufferEvicted,
            WriteError::BufferFull => ErrorCode::BufferFull,
            WriteError::CircuitBreakerOpen { .. } => ErrorCode::CircuitOpen,
            WriteError::ConfirmationFailed(_) => ErrorCode::ConfirmFailed,
            WriteError::ReplicationConfirmationFailed(_) => ErrorCode::ReplConfirmFailed,
            WriteError::DatabaseOperationFailed(_) => ErrorCode::DbOpFailed,
            WriteError::InsufficientHealthyReplicas { .. } => ErrorCode::InsufficientReplicas,
            WriteError::InvalidSender => ErrorCode::InvalidSender,
            WriteError::StaleWrite => ErrorCode::StaleWrite,
            WriteError::MissingExpectedPartitionSequence => ErrorCode::MissingPartSeq,
            WriteError::PartitionNotOwned { .. } => ErrorCode::PartNotOwned,
            WriteError::ReplicationQuorumFailed { .. } => ErrorCode::QuorumFailed,
            WriteError::RequestTimeout => ErrorCode::Timeout,
            WriteError::MaximumForwardsExceeded { .. } => ErrorCode::MaxForwards,
            WriteError::RemoteOperationFailed(_) => ErrorCode::RemoteOpFailed,
            WriteError::SequenceConflict => ErrorCode::SeqConflict,
            WriteError::WrongExpectedSequence { .. } => ErrorCode::WrongSeq,
            WriteError::WrongExpectedVersion { .. } => ErrorCode::WrongVer,
        };

        format!("{code} {self}")
    }
}

impl AsRedisError for SystemTimeError {
    fn as_redis_error(&self) -> String {
        ErrorCode::ClockErr.with_message(&self.to_string())
    }
}

impl AsRedisError for ClusterError {
    fn as_redis_error(&self) -> String {
        let code = match self {
            ClusterError::InsufficientPartitionsForQuorum { .. } => ErrorCode::ClusterDown,
            ClusterError::NoAvailablePartitions => ErrorCode::ClusterDown,
            ClusterError::PartitionUnavailable => ErrorCode::ClusterDown,
            ClusterError::NoAvailableLeaders => ErrorCode::ClusterDown,
            ClusterError::QuorumNotAchieved { .. } => ErrorCode::TryAgain,
            ClusterError::WriteTimeout => ErrorCode::Timeout,
            ClusterError::TooManyForwards => ErrorCode::MaxForwards,
            ClusterError::CircuitBreakerOpen { .. } => ErrorCode::CircuitOpen,
            ClusterError::ConfirmationFailure(_) => ErrorCode::ConfirmFailed,
            ClusterError::Read(_) => ErrorCode::ReadErr,
            ClusterError::Write(_) => ErrorCode::WriteErr,
            ClusterError::RemoteSend(_) => ErrorCode::RemoteErr,
            ClusterError::Noise(_) => ErrorCode::NoiseErr,
            ClusterError::Transport(_) => ErrorCode::TransportErr,
            ClusterError::SwarmBuilder(_) => ErrorCode::SwarmErr,
        };

        match self {
            ClusterError::InsufficientPartitionsForQuorum { alive, required } => code.with_message(
                &format!("Insufficient partitions alive for quorum ({alive}/{required})"),
            ),
            ClusterError::NoAvailablePartitions => code.with_message("No available partitions"),
            ClusterError::PartitionUnavailable => code.with_message("Partition unavailable"),
            ClusterError::NoAvailableLeaders => code.with_message("No available leaders"),
            ClusterError::QuorumNotAchieved {
                confirmed,
                required,
            } => code.with_message(&format!("Quorum not achieved ({confirmed}/{required})")),
            ClusterError::WriteTimeout => code.with_message("Write timed out"),
            ClusterError::TooManyForwards => code.with_message("Too many forwards"),
            ClusterError::CircuitBreakerOpen {
                estimated_recovery_time,
            } => match estimated_recovery_time {
                Some(time) => code.with_message(&format!(
                    "Circuit breaker open: estimated recovery time: {time:?}"
                )),
                None => code.with_message("Circuit breaker open"),
            },
            ClusterError::ConfirmationFailure(msg) => {
                code.with_message(&format!("Failed to write confirmation count: {msg}"))
            }
            ClusterError::Read(msg) => code.with_message(&format!("Read error: {msg}")),
            ClusterError::Write(msg) => code.with_message(&format!("Write error: {msg}")),
            ClusterError::RemoteSend(err) => {
                code.with_message(&format!("Remote send error: {err}"))
            }
            ClusterError::Noise(err) => code.with_message(&format!("Noise error: {err}")),
            ClusterError::Transport(err) => code.with_message(&format!("Transport error: {err}")),
            ClusterError::SwarmBuilder(err) => {
                code.with_message(&format!("Swarm builder error: {err}"))
            }
        }
    }
}
