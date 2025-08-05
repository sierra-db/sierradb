use sierradb_protocol::{ErrorCode, ParsedError};
use std::fmt;

/// Errors that can occur when using the SierraDB client.
#[derive(Debug, Clone)]
pub enum SierraError {
    /// A protocol-level error returned by the Sierra server
    Protocol { code: ErrorCode, message: String },
    /// A Redis client error (connection, parsing, etc.)
    Redis(String),
    /// An invalid argument was provided by the client
    InvalidArgument(String),
    /// A generic error with a message
    Other(String),
}

impl SierraError {
    /// Parse a Redis error response into a SierraError.
    ///
    /// This attempts to parse Sierra error codes from Redis error responses.
    /// If parsing fails, it falls back to a generic Redis error.
    pub fn from_redis_error(redis_error: &redis::RedisError) -> Self {
        let error_msg = redis_error.to_string();

        // Try to parse as a Sierra protocol error
        if let Some(parsed) = ParsedError::parse(&error_msg) {
            SierraError::Protocol {
                code: parsed.code,
                message: parsed.message,
            }
        } else {
            // Fall back to generic Redis error
            SierraError::Redis(error_msg)
        }
    }

    /// Check if this error indicates a temporary failure that should be
    /// retried.
    pub fn is_retryable(&self) -> bool {
        match self {
            SierraError::Protocol { code, .. } => code.is_retryable(),
            SierraError::Redis(_) => false, // Conservative - don't retry unknown Redis errors
            SierraError::InvalidArgument(_) => false,
            SierraError::Other(_) => false,
        }
    }

    /// Check if this error indicates a cluster availability issue.
    pub fn is_cluster_issue(&self) -> bool {
        match self {
            SierraError::Protocol { code, .. } => code.is_cluster_issue(),
            _ => false,
        }
    }

    /// Check if this error indicates a client argument error.
    pub fn is_client_error(&self) -> bool {
        match self {
            SierraError::Protocol { code, .. } => code.is_client_error(),
            SierraError::InvalidArgument(_) => true,
            _ => false,
        }
    }

    /// Get the error code if this is a protocol error.
    pub fn error_code(&self) -> Option<ErrorCode> {
        match self {
            SierraError::Protocol { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// Get a suggested retry delay in milliseconds for retryable errors.
    /// Returns None if the error should not be retried.
    pub fn retry_delay_ms(&self) -> Option<u64> {
        if !self.is_retryable() {
            return None;
        }

        match self {
            SierraError::Protocol { code, .. } => match code {
                ErrorCode::TryAgain => Some(100), // Short delay for temporary failures
                ErrorCode::Timeout => Some(1000), // Longer delay for timeouts
                ErrorCode::CircuitOpen => Some(5000), // Wait for circuit breaker
                ErrorCode::ActorDown | ErrorCode::ActorStopped => Some(500), // Actor restart time
                ErrorCode::MailboxFull => Some(200), // Mailbox drain time
                _ => Some(1000),                  // Default retry delay
            },
            _ => None,
        }
    }
}

impl fmt::Display for SierraError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SierraError::Protocol { code, message } => {
                if message.is_empty() {
                    write!(f, "Sierra protocol error: {code}")
                } else {
                    write!(f, "Sierra protocol error: {code} {message}")
                }
            }
            SierraError::Redis(msg) => write!(f, "Redis error: {msg}"),
            SierraError::InvalidArgument(msg) => write!(f, "Invalid argument: {msg}"),
            SierraError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for SierraError {}

impl From<redis::RedisError> for SierraError {
    fn from(error: redis::RedisError) -> Self {
        SierraError::from_redis_error(&error)
    }
}

/// Result type for Sierra client operations.
pub type SierraResult<T> = Result<T, SierraError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sierra_error_parsing() {
        // Test parsing Sierra protocol errors directly
        let sierra_err = SierraError::Protocol {
            code: ErrorCode::ClusterDown,
            message: "No available partitions".to_string(),
        };

        assert_eq!(sierra_err.error_code(), Some(ErrorCode::ClusterDown));
        assert!(!sierra_err.is_retryable());
        assert!(sierra_err.is_cluster_issue());

        // Test parsing from error string
        if let Some(parsed) = ParsedError::parse("CLUSTERDOWN No available partitions") {
            assert_eq!(parsed.code, ErrorCode::ClusterDown);
            assert_eq!(parsed.message, "No available partitions");
        } else {
            panic!("Failed to parse error string");
        }
    }

    #[test]
    fn test_error_categorization() {
        let protocol_err = SierraError::Protocol {
            code: ErrorCode::TryAgain,
            message: "Try again later".to_string(),
        };

        assert!(protocol_err.is_retryable());
        assert!(!protocol_err.is_client_error());
        assert!(!protocol_err.is_cluster_issue());
        assert_eq!(protocol_err.error_code(), Some(ErrorCode::TryAgain));
    }

    #[test]
    fn test_retry_delay() {
        let try_again_err = SierraError::Protocol {
            code: ErrorCode::TryAgain,
            message: "".to_string(),
        };
        assert_eq!(try_again_err.retry_delay_ms(), Some(100));

        let client_err = SierraError::InvalidArgument("Bad arg".to_string());
        assert_eq!(client_err.retry_delay_ms(), None);
    }
}
