use redis::RetryMethod;
use sierradb_protocol::ErrorCode;
use std::{fmt, str::FromStr};

/// Errors that can occur when using the SierraDB client.
#[derive(Debug)]
pub enum SierraError {
    /// A protocol-level error returned by the Sierra server
    Protocol {
        code: ErrorCode,
        message: Option<String>,
    },
    /// A Redis client error (connection, parsing, etc.)
    Redis(redis::RedisError),
}

impl SierraError {
    /// Parse a Redis error response into a SierraError.
    pub fn from_redis_error(redis_error: redis::RedisError) -> Self {
        let code = redis_error
            .code()
            .and_then(|code| ErrorCode::from_str(code).ok());
        match code {
            Some(code) => SierraError::Protocol {
                code,
                message: redis_error.detail().map(ToOwned::to_owned),
            },
            None => SierraError::Redis(redis_error),
        }
    }

    /// Check if this error indicates a temporary failure that should be
    /// retried.
    pub fn is_retryable(&self) -> bool {
        match self {
            SierraError::Protocol { code, .. } => code.is_retryable(),
            SierraError::Redis(err) => !matches!(err.retry_method(), RetryMethod::NoRetry),
        }
    }

    /// Check if this error indicates a cluster availability issue.
    pub fn is_cluster_issue(&self) -> bool {
        match self {
            SierraError::Protocol { code, .. } => code.is_cluster_issue(),
            SierraError::Redis(err) => err.is_cluster_error(),
        }
    }

    /// Check if this error indicates a client argument error.
    pub fn is_client_error(&self) -> bool {
        match self {
            SierraError::Protocol { code, .. } => code.is_client_error(),
            SierraError::Redis(_) => false,
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
            SierraError::Protocol { code, message } => match message {
                Some(msg) => {
                    write!(f, "sierra protocol error: {code} {msg}")
                }
                None => {
                    write!(f, "sierra protocol error: {code}")
                }
            },
            SierraError::Redis(err) => write!(f, "redis error: {err}"),
        }
    }
}

impl std::error::Error for SierraError {}

impl From<redis::RedisError> for SierraError {
    fn from(err: redis::RedisError) -> Self {
        SierraError::from_redis_error(err)
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
            message: Some("No available partitions".to_string()),
        };

        assert_eq!(sierra_err.error_code(), Some(ErrorCode::ClusterDown));
        assert!(!sierra_err.is_retryable());
        assert!(sierra_err.is_cluster_issue());
    }

    #[test]
    fn test_error_categorization() {
        let protocol_err = SierraError::Protocol {
            code: ErrorCode::TryAgain,
            message: Some("Try again later".to_string()),
        };

        assert!(protocol_err.is_retryable());
        assert!(!protocol_err.is_client_error());
        assert!(!protocol_err.is_cluster_issue());
        assert_eq!(protocol_err.error_code(), Some(ErrorCode::TryAgain));
    }
}
