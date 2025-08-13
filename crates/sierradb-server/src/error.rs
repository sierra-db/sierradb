use std::time::SystemTimeError;

use kameo::error::SendError;
use sierradb_cluster::ClusterError;
use sierradb_cluster::write::error::WriteError;
use sierradb_protocol::ErrorCode;

/// Extension trait for converting results to Redis error format.
pub trait MapRedisError<T> {
    fn map_redis_err(self) -> Result<T, String>;
}

impl<T, E: AsRedisError> MapRedisError<T> for Result<T, E> {
    fn map_redis_err(self) -> Result<T, String> {
        self.map_err(|err| err.as_redis_error())
    }
}

/// Trait for converting errors to Redis-compatible error strings.
pub trait AsRedisError {
    fn as_redis_error(&self) -> String;
}

impl AsRedisError for String {
    fn as_redis_error(&self) -> String {
        self.clone()
    }
}

impl AsRedisError for &str {
    fn as_redis_error(&self) -> String {
        self.to_string()
    }
}

impl<M, E: AsRedisError> AsRedisError for SendError<M, E> {
    fn as_redis_error(&self) -> String {
        match self {
            SendError::ActorNotRunning(_) => ErrorCode::ActorDown.with_message("actor not running"),
            SendError::ActorStopped => {
                ErrorCode::ActorStopped.with_message("actor died when executing request")
            }
            SendError::MailboxFull(_) => {
                ErrorCode::MailboxFull.with_message("actor mailbox is full")
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
        ErrorCode::ClockErr.with_message(self.to_string())
    }
}

impl AsRedisError for kameo::error::Infallible {
    fn as_redis_error(&self) -> String {
        unreachable!()
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
                format!("insufficient partitions alive for quorum ({alive}/{required})"),
            ),
            ClusterError::NoAvailablePartitions => code.with_message("no available partitions"),
            ClusterError::PartitionUnavailable => code.with_message("partition unavailable"),
            ClusterError::NoAvailableLeaders => code.with_message("no available leaders"),
            ClusterError::QuorumNotAchieved {
                confirmed,
                required,
            } => code.with_message(format!("quorum not achieved ({confirmed}/{required})")),
            ClusterError::WriteTimeout => code.with_message("write timed out"),
            ClusterError::TooManyForwards => code.with_message("too many forwards"),
            ClusterError::CircuitBreakerOpen {
                estimated_recovery_time,
            } => match estimated_recovery_time {
                Some(time) => code.with_message(format!(
                    "circuit breaker open: estimated recovery time: {time:?}"
                )),
                None => code.with_message("circuit breaker open"),
            },
            ClusterError::ConfirmationFailure(msg) => {
                code.with_message(format!("failed to write confirmation count: {msg}"))
            }
            ClusterError::Read(msg) => code.with_message(format!("read error: {msg}")),
            ClusterError::Write(msg) => code.with_message(format!("write error: {msg}")),
            ClusterError::RemoteSend(err) => code.with_message(format!("remote send error: {err}")),
            ClusterError::Noise(err) => code.with_message(format!("noise error: {err}")),
            ClusterError::Transport(err) => code.with_message(format!("transport error: {err}")),
            ClusterError::SwarmBuilder(err) => {
                code.with_message(format!("swarm builder error: {err}"))
            }
        }
    }
}
