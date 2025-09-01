use std::fmt;

use strum::{AsRefStr, Display, EnumString, IntoStaticStr};

/// Error codes used in Sierra server responses.
///
/// These codes follow Redis conventions where applicable, with Sierra-specific
/// extensions for domain-specific error conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, Display, EnumString, IntoStaticStr)]
#[strum(serialize_all = "UPPERCASE")]
pub enum ErrorCode {
    // Redis-standard error codes
    /// Cluster is down or partitions unavailable
    ClusterDown,
    /// Temporary failure, client should retry
    TryAgain,
    /// Request timed out
    Timeout,
    /// Invalid syntax in command arguments
    Syntax,
    /// Invalid argument provided
    InvalidArg,
    /// Resource not found
    NotFound,

    // Sierra-specific error codes
    /// Too many request forwards (loop prevention)
    MaxForwards,
    /// Circuit breaker is open
    CircuitOpen,
    /// Confirmation operation failed
    ConfirmFailed,
    /// Replication confirmation failed
    ReplConfirmFailed,
    /// System clock error
    ClockErr,
    /// Read operation error
    ReadErr,
    /// Write operation error
    WriteErr,
    /// Remote communication error
    RemoteErr,
    /// Network noise protocol error
    NoiseErr,
    /// Transport layer error
    TransportErr,
    /// Swarm initialization error
    SwarmErr,
    /// Actor not running
    ActorDown,
    /// Actor stopped or died
    ActorStopped,
    /// Actor mailbox is full
    MailboxFull,

    // WriteError-specific codes
    /// All replicas failed
    AllReplicasFailed,
    /// Buffer was evicted
    BufferEvicted,
    /// Buffer is full
    BufferFull,
    /// Database operation failed
    DbOpFailed,
    /// Insufficient healthy replicas
    InsufficientReplicas,
    /// Invalid sender
    InvalidSender,
    /// Stale write detected
    StaleWrite,
    /// Missing expected partition sequence
    MissingPartSeq,
    /// Partition not owned by this node
    PartNotOwned,
    /// Replication quorum failed
    QuorumFailed,
    /// Remote operation failed
    RemoteOpFailed,
    /// Sequence conflict detected
    SeqConflict,
    /// Wrong expected sequence
    WrongSeq,
    /// Wrong expected version
    WrongVer,

    // Subscription-specific error codes
    /// Redirect request to another node
    Redirect,
}

impl ErrorCode {
    /// Format this error code with a message.
    ///
    /// # Example
    /// ```
    /// use sierradb_protocol::ErrorCode;
    ///
    /// let error = ErrorCode::InvalidArg.with_message("invalid timestamp format");
    /// assert_eq!(error, "INVALIDARG invalid timestamp format");
    /// ```
    pub fn with_message(&self, message: impl fmt::Display) -> String {
        format!("{self} {message}")
    }

    /// Check if this error code indicates a temporary failure that should be
    /// retried.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ErrorCode::TryAgain
                | ErrorCode::Timeout
                | ErrorCode::CircuitOpen
                | ErrorCode::ActorDown
                | ErrorCode::ActorStopped
                | ErrorCode::MailboxFull
        )
    }

    /// Check if this error code indicates a cluster availability issue.
    pub fn is_cluster_issue(&self) -> bool {
        matches!(
            self,
            ErrorCode::ClusterDown
                | ErrorCode::ReadErr
                | ErrorCode::WriteErr
                | ErrorCode::RemoteErr
                | ErrorCode::NoiseErr
                | ErrorCode::TransportErr
                | ErrorCode::SwarmErr
        )
    }

    /// Check if this error code indicates a client argument error.
    pub fn is_client_error(&self) -> bool {
        matches!(self, ErrorCode::Syntax | ErrorCode::InvalidArg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_parsing() {
        assert_eq!(
            "CLUSTERDOWN".parse::<ErrorCode>().unwrap(),
            ErrorCode::ClusterDown
        );
        assert_eq!(
            "INVALIDARG".parse::<ErrorCode>().unwrap(),
            ErrorCode::InvalidArg
        );
        assert!("UNKNOWN".parse::<ErrorCode>().is_err());
    }

    #[test]
    fn test_error_categories() {
        assert!(ErrorCode::TryAgain.is_retryable());
        assert!(!ErrorCode::Syntax.is_retryable());

        assert!(ErrorCode::ClusterDown.is_cluster_issue());
        assert!(!ErrorCode::InvalidArg.is_cluster_issue());

        assert!(ErrorCode::InvalidArg.is_client_error());
        assert!(!ErrorCode::ClusterDown.is_client_error());
    }

    #[test]
    fn test_with_message() {
        let error = ErrorCode::InvalidArg.with_message("invalid timestamp format");
        assert_eq!(error, "INVALIDARG invalid timestamp format");
    }
}
