/// Error codes used in Sierra server responses.
///
/// These codes follow Redis conventions where applicable, with Sierra-specific
/// extensions for domain-specific error conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
}

impl ErrorCode {
    /// Format this error code with a message.
    ///
    /// # Example
    /// ```
    /// use sierradb_protocol::ErrorCode;
    ///
    /// let error = ErrorCode::InvalidArg.with_message("Invalid timestamp format");
    /// assert_eq!(error, "INVALIDARG Invalid timestamp format");
    /// ```
    pub fn with_message(&self, message: &str) -> String {
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

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code = match self {
            // Redis-standard error codes
            ErrorCode::ClusterDown => "CLUSTERDOWN",
            ErrorCode::TryAgain => "TRYAGAIN",
            ErrorCode::Timeout => "TIMEOUT",
            ErrorCode::Syntax => "SYNTAX",
            ErrorCode::InvalidArg => "INVALIDARG",

            // Sierra-specific error codes
            ErrorCode::MaxForwards => "MAXFORWARDS",
            ErrorCode::CircuitOpen => "CIRCUITOPEN",
            ErrorCode::ConfirmFailed => "CONFIRMFAILED",
            ErrorCode::ReplConfirmFailed => "REPLCONFIRMFAILED",
            ErrorCode::ClockErr => "CLOCKERR",
            ErrorCode::ReadErr => "READERR",
            ErrorCode::WriteErr => "WRITEERR",
            ErrorCode::RemoteErr => "REMOTEERR",
            ErrorCode::NoiseErr => "NOISEERR",
            ErrorCode::TransportErr => "TRANSPORTERR",
            ErrorCode::SwarmErr => "SWARMERR",
            ErrorCode::ActorDown => "ACTORDOWN",
            ErrorCode::ActorStopped => "ACTORSTOPPED",
            ErrorCode::MailboxFull => "MAILBOXFULL",

            // WriteError-specific codes
            ErrorCode::AllReplicasFailed => "ALLREPLICASFAILED",
            ErrorCode::BufferEvicted => "BUFFEREVICTED",
            ErrorCode::BufferFull => "BUFFERFULL",
            ErrorCode::DbOpFailed => "DBOPFAILED",
            ErrorCode::InsufficientReplicas => "INSUFFICIENTREPLICAS",
            ErrorCode::InvalidSender => "INVALIDSENDER",
            ErrorCode::StaleWrite => "STALEWRITE",
            ErrorCode::MissingPartSeq => "MISSINGPARTSEQ",
            ErrorCode::PartNotOwned => "PARTNOTOWNED",
            ErrorCode::QuorumFailed => "QUORUMFAILED",
            ErrorCode::RemoteOpFailed => "REMOTEOPFAILED",
            ErrorCode::SeqConflict => "SEQCONFLICT",
            ErrorCode::WrongSeq => "WRONGSEQ",
            ErrorCode::WrongVer => "WRONGVER",
        };
        write!(f, "{code}")
    }
}

impl std::str::FromStr for ErrorCode {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            // Redis-standard error codes
            "CLUSTERDOWN" => Ok(ErrorCode::ClusterDown),
            "TRYAGAIN" => Ok(ErrorCode::TryAgain),
            "TIMEOUT" => Ok(ErrorCode::Timeout),
            "SYNTAX" => Ok(ErrorCode::Syntax),
            "INVALIDARG" => Ok(ErrorCode::InvalidArg),

            // Sierra-specific error codes
            "MAXFORWARDS" => Ok(ErrorCode::MaxForwards),
            "CIRCUITOPEN" => Ok(ErrorCode::CircuitOpen),
            "CONFIRMFAILED" => Ok(ErrorCode::ConfirmFailed),
            "REPLCONFIRMFAILED" => Ok(ErrorCode::ReplConfirmFailed),
            "CLOCKERR" => Ok(ErrorCode::ClockErr),
            "READERR" => Ok(ErrorCode::ReadErr),
            "WRITEERR" => Ok(ErrorCode::WriteErr),
            "REMOTEERR" => Ok(ErrorCode::RemoteErr),
            "NOISEERR" => Ok(ErrorCode::NoiseErr),
            "TRANSPORTERR" => Ok(ErrorCode::TransportErr),
            "SWARMERR" => Ok(ErrorCode::SwarmErr),
            "ACTORDOWN" => Ok(ErrorCode::ActorDown),
            "ACTORSTOPPED" => Ok(ErrorCode::ActorStopped),
            "MAILBOXFULL" => Ok(ErrorCode::MailboxFull),

            // WriteError-specific codes
            "ALLREPLICASFAILED" => Ok(ErrorCode::AllReplicasFailed),
            "BUFFEREVICTED" => Ok(ErrorCode::BufferEvicted),
            "BUFFERFULL" => Ok(ErrorCode::BufferFull),
            "DBOPFAILED" => Ok(ErrorCode::DbOpFailed),
            "INSUFFICIENTREPLICAS" => Ok(ErrorCode::InsufficientReplicas),
            "INVALIDSENDER" => Ok(ErrorCode::InvalidSender),
            "STALEWRITE" => Ok(ErrorCode::StaleWrite),
            "MISSINGPARTSEQ" => Ok(ErrorCode::MissingPartSeq),
            "PARTNOTOWNED" => Ok(ErrorCode::PartNotOwned),
            "QUORUMFAILED" => Ok(ErrorCode::QuorumFailed),
            "REMOTEOPFAILED" => Ok(ErrorCode::RemoteOpFailed),
            "SEQCONFLICT" => Ok(ErrorCode::SeqConflict),
            "WRONGSEQ" => Ok(ErrorCode::WrongSeq),
            _ => Err(()),
        }
    }
}

/// A parsed error from a Sierra server response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedError {
    /// The error code
    pub code: ErrorCode,
    /// The error message (without the code prefix)
    pub message: String,
}

impl ParsedError {
    /// Parse an error string from a Sierra server response.
    ///
    /// # Example
    /// ```
    /// use sierradb_protocol::{ParsedError, ErrorCode};
    ///
    /// let parsed = ParsedError::parse("CLUSTERDOWN No available partitions").unwrap();
    /// assert_eq!(parsed.code, ErrorCode::ClusterDown);
    /// assert_eq!(parsed.message, "No available partitions");
    /// ```
    pub fn parse(error_str: &str) -> Option<ParsedError> {
        let mut parts = error_str.splitn(2, ' ');
        let code_str = parts.next()?;
        let message = parts.next().unwrap_or("").to_string();

        let code = code_str.parse().ok()?;

        Some(ParsedError { code, message })
    }

    /// Check if this error should be retried.
    pub fn is_retryable(&self) -> bool {
        self.code.is_retryable()
    }

    /// Check if this is a cluster availability issue.
    pub fn is_cluster_issue(&self) -> bool {
        self.code.is_cluster_issue()
    }

    /// Check if this is a client argument error.
    pub fn is_client_error(&self) -> bool {
        self.code.is_client_error()
    }
}

impl std::fmt::Display for ParsedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.message.is_empty() {
            write!(f, "{}", self.code)
        } else {
            write!(f, "{} {}", self.code, self.message)
        }
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
    fn test_parsed_error() {
        let parsed = ParsedError::parse("CLUSTERDOWN No available partitions").unwrap();
        assert_eq!(parsed.code, ErrorCode::ClusterDown);
        assert_eq!(parsed.message, "No available partitions");
        assert_eq!(parsed.to_string(), "CLUSTERDOWN No available partitions");
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
        let error = ErrorCode::InvalidArg.with_message("Invalid timestamp format");
        assert_eq!(error, "INVALIDARG Invalid timestamp format");
    }
}
