use std::{
    sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum CircuitState {
    Closed = 0,   // Normal operation
    Open = 1,     // Failing fast, blocking requests
    HalfOpen = 2, // Testing if system recovered
}

impl From<u8> for CircuitState {
    fn from(value: u8) -> Self {
        match value {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed, // Default fallback
        }
    }
}

pub struct WriteCircuitBreaker {
    state: AtomicU8, // Represents CircuitState
    failure_count: AtomicU32,
    last_failure_time: AtomicU64,
    last_success_time: AtomicU64,

    // Half-open state tracking
    half_open_call_count: AtomicU32,
    half_open_success_count: AtomicU32,

    // Configuration
    failure_threshold: u32,           // e.g., 10 failures
    recovery_timeout: Duration,       // e.g., 30 seconds
    half_open_max_calls: u32,         // e.g., 3 test calls
    half_open_success_threshold: u32, // e.g., 2 successes needed
}

impl WriteCircuitBreaker {
    pub fn new(
        failure_threshold: u32,
        recovery_timeout: Duration,
        half_open_max_calls: u32,
        half_open_success_threshold: u32,
    ) -> Self {
        Self {
            state: AtomicU8::new(CircuitState::Closed as u8),
            failure_count: AtomicU32::new(0),
            last_failure_time: AtomicU64::new(0),
            last_success_time: AtomicU64::new(current_timestamp()),
            half_open_call_count: AtomicU32::new(0),
            half_open_success_count: AtomicU32::new(0),
            failure_threshold,
            recovery_timeout,
            half_open_max_calls,
            half_open_success_threshold,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(
            10,                      // 10 failures
            Duration::from_secs(30), // 30 seconds recovery
            3,                       // 3 test calls
            2,                       // 2 successes needed
        )
    }

    pub fn should_allow_request(&self) -> bool {
        match self.current_state() {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if enough time has passed to try recovery
                let now = current_timestamp();
                let last_failure = self.last_failure_time.load(Ordering::Acquire);

                if now - last_failure >= self.recovery_timeout.as_millis() as u64 {
                    // Transition to half-open to test recovery
                    self.transition_to_half_open();
                    true
                } else {
                    false // Still in failure mode
                }
            }
            CircuitState::HalfOpen => {
                // Allow limited requests to test system recovery
                let current_calls = self.half_open_call_count.fetch_add(1, Ordering::AcqRel);
                current_calls < self.half_open_max_calls
            }
        }
    }

    pub fn record_success(&self) {
        self.last_success_time
            .store(current_timestamp(), Ordering::Release);

        match self.current_state() {
            CircuitState::Closed => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::Release);
            }
            CircuitState::HalfOpen => {
                let successes = self.half_open_success_count.fetch_add(1, Ordering::AcqRel) + 1;

                if successes >= self.half_open_success_threshold {
                    // Enough successes - close the circuit
                    self.transition_to_closed();
                }
            }
            CircuitState::Open => {
                // This shouldn't happen, but if it does, transition to half-open
                self.transition_to_half_open();
            }
        }
    }

    pub fn record_failure(&self) {
        self.last_failure_time
            .store(current_timestamp(), Ordering::Release);

        match self.current_state() {
            CircuitState::Closed => {
                let failures = self.failure_count.fetch_add(1, Ordering::AcqRel) + 1;
                if failures >= self.failure_threshold {
                    self.transition_to_open();
                }
            }
            CircuitState::HalfOpen => {
                // Any failure in half-open immediately opens circuit
                self.transition_to_open();
            }
            CircuitState::Open => {
                // Already open, just update timestamp
            }
        }
    }

    pub fn current_state(&self) -> CircuitState {
        let state_value = self.state.load(Ordering::Acquire);
        CircuitState::from(state_value)
    }

    pub fn estimated_recovery_time(&self) -> Option<Duration> {
        match self.current_state() {
            CircuitState::Open => {
                let now = current_timestamp();
                let last_failure = self.last_failure_time.load(Ordering::Acquire);
                let elapsed = Duration::from_millis(now - last_failure);

                if elapsed >= self.recovery_timeout {
                    Some(Duration::ZERO) // Ready to recover now
                } else {
                    Some(self.recovery_timeout - elapsed)
                }
            }
            CircuitState::HalfOpen => Some(Duration::ZERO), // Testing recovery
            CircuitState::Closed => None,                   // No recovery needed
        }
    }

    pub fn failure_count(&self) -> u32 {
        self.failure_count.load(Ordering::Acquire)
    }

    pub fn last_failure_time(&self) -> Option<Duration> {
        let timestamp = self.last_failure_time.load(Ordering::Acquire);
        if timestamp == 0 {
            None
        } else {
            Some(Duration::from_millis(timestamp))
        }
    }

    fn transition_to_open(&self) {
        self.state
            .store(CircuitState::Open as u8, Ordering::Release);
        // Reset half-open counters
        self.half_open_call_count.store(0, Ordering::Release);
        self.half_open_success_count.store(0, Ordering::Release);
    }

    fn transition_to_half_open(&self) {
        // Only transition if we're currently Open
        let _ = self.state.compare_exchange(
            CircuitState::Open as u8,
            CircuitState::HalfOpen as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        // Reset half-open counters
        self.half_open_call_count.store(0, Ordering::Release);
        self.half_open_success_count.store(0, Ordering::Release);
    }

    fn transition_to_closed(&self) {
        self.state
            .store(CircuitState::Closed as u8, Ordering::Release);
        // Reset all counters
        self.failure_count.store(0, Ordering::Release);
        self.half_open_call_count.store(0, Ordering::Release);
        self.half_open_success_count.store(0, Ordering::Release);
    }
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_circuit_breaker_starts_closed() {
        let cb = WriteCircuitBreaker::with_defaults();
        assert_eq!(cb.current_state(), CircuitState::Closed);
        assert!(cb.should_allow_request());
    }

    #[test]
    fn test_circuit_breaker_opens_after_failures() {
        let cb = WriteCircuitBreaker::new(
            2, // 2 failures to open
            Duration::from_secs(1),
            1,
            1,
        );

        // First failure - should stay closed
        cb.record_failure();
        assert_eq!(cb.current_state(), CircuitState::Closed);
        assert!(cb.should_allow_request());

        // Second failure - should open
        cb.record_failure();
        assert_eq!(cb.current_state(), CircuitState::Open);
        assert!(!cb.should_allow_request());
    }

    #[test]
    fn test_circuit_breaker_recovery() {
        let cb = WriteCircuitBreaker::new(
            1,                         // 1 failure to open
            Duration::from_millis(10), // Very short recovery time
            1,
            1,
        );

        // Cause failure to open circuit
        cb.record_failure();
        assert_eq!(cb.current_state(), CircuitState::Open);
        assert!(!cb.should_allow_request());

        // Wait for recovery timeout
        std::thread::sleep(Duration::from_millis(15));

        // Should transition to half-open and allow request
        assert!(cb.should_allow_request());
        assert_eq!(cb.current_state(), CircuitState::HalfOpen);

        // Record success to close circuit
        cb.record_success();
        assert_eq!(cb.current_state(), CircuitState::Closed);
        assert!(cb.should_allow_request());
    }

    #[test]
    fn test_success_resets_failure_count() {
        let cb = WriteCircuitBreaker::new(
            3, // 3 failures to open
            Duration::from_secs(1),
            1,
            1,
        );

        // Record some failures
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.failure_count(), 2);
        assert_eq!(cb.current_state(), CircuitState::Closed);

        // Record success - should reset failure count
        cb.record_success();
        assert_eq!(cb.failure_count(), 0);
        assert_eq!(cb.current_state(), CircuitState::Closed);
    }
}
