use std::pin::Pin;
use std::time::Duration;
use tokio::time::Instant;
use tokio::time::Sleep;
use tokio::time::sleep;

use crate::write::ordered_queue::{self, InsertResult, OrderedQueue, OrderedValue};

pub trait TimedOrderedValue: OrderedValue {
    fn received_at(&self) -> Instant;
}

#[derive(Debug)]
pub struct TimeoutOrderedQueue<K, V> {
    pub queue: OrderedQueue<K, V>,
    timeout_duration: Duration,
    current_timeout: Option<Pin<Box<Sleep>>>,
    current_timeout_key: Option<K>, // Track which key the current timeout is set for
}

impl<K, V> TimeoutOrderedQueue<K, V>
where
    K: Ord + Clone,
    V: TimedOrderedValue,
{
    pub fn new(initial_next: K, limit: usize, timeout_duration: Duration) -> Self {
        Self {
            queue: OrderedQueue::new(initial_next, limit),
            timeout_duration,
            current_timeout: None,
            current_timeout_key: None,
        }
    }

    pub fn insert(
        &mut self,
        key: K,
        value: V,
    ) -> Result<InsertResult<K, V>, ordered_queue::Error<K, V>> {
        let result = self.queue.insert(key.clone(), value);

        let should_update = match &result {
            Ok(insert_result) => {
                if insert_result.next.is_some() {
                    // If ready, the item was consumed immediately, so update timeout
                    // since the buffer state might have changed
                    true
                } else {
                    // If buffered and merged, might need to update timeout due to time change
                    // If buffered and not merged, might need to update if it's the earliest key
                    insert_result.merged_with_existing || self.should_update_timeout_for_key(&key)
                }
            }
            Err(_) => false,
        };

        if should_update {
            self.update_timeout();
        }

        result
    }

    pub fn pop(&mut self) -> Option<V> {
        let result = self.queue.pop();

        if result.is_some() {
            // Always update timeout after pop since the earliest item might have changed
            self.update_timeout();
        }

        result
    }

    pub fn progress_to(&mut self, next: K) {
        self.queue.progress_to(next);
        // Always update timeout after progress since it affects what's considered
        // "next"
        self.update_timeout();
    }

    pub fn next(&self) -> &K {
        self.queue.next()
    }

    // Check if the timeout has expired
    pub fn timeout_future(&mut self) -> Option<&mut Pin<Box<Sleep>>> {
        self.current_timeout.as_mut()
    }

    // Call this when timeout expires to get expired items
    pub fn handle_timeout(&mut self) -> Vec<(K, V)> {
        let now = Instant::now();

        // Find and remove expired items
        let expired = self
            .queue
            .map
            .extract_if(.., |_, v| {
                now.duration_since(v.received_at()) >= self.timeout_duration
            })
            .collect();

        // Update timeout for remaining items
        self.update_timeout();

        expired
    }

    fn should_update_timeout_for_key(&self, new_key: &K) -> bool {
        match &self.current_timeout_key {
            None => true, // No current timeout, so any new key triggers an update
            Some(current_key) => new_key < current_key, // Only update if new key is smaller
        }
    }

    pub fn update_timeout(&mut self) {
        match self.queue.map.first_key_value() {
            Some((earliest_key, earliest_value)) => {
                let deadline = earliest_value.received_at() + self.timeout_duration;
                let now = Instant::now();
                let duration = if deadline > now {
                    deadline - now
                } else {
                    // Already expired, set immediate timeout
                    Duration::ZERO
                };
                match &mut self.current_timeout {
                    Some(timeout) => {
                        timeout.as_mut().reset(now + duration);
                    }
                    None => {
                        self.current_timeout = Some(Box::pin(sleep(duration)));
                    }
                }
                self.current_timeout_key = Some(earliest_key.clone());
            }
            None => {
                self.current_timeout = None;
                self.current_timeout_key = None;
            }
        }
    }
}

#[cfg(test)]
mod timeout_tests {
    use super::*;

    // Test implementation of TimedOrderedValue
    #[derive(Debug, Clone, PartialEq)]
    struct TimedTestValue {
        id: String,
        data: Vec<u8>,
        received_at: Instant,
    }

    impl TimedTestValue {
        fn new(id: &str, data: Vec<u8>, received_at: Instant) -> Self {
            TimedTestValue {
                id: id.to_string(),
                data,
                received_at,
            }
        }

        fn new_now(id: &str, data: Vec<u8>) -> Self {
            Self::new(id, data, Instant::now())
        }
    }

    impl OrderedValue for TimedTestValue {
        fn key_eq(&self, other: &Self) -> bool {
            self.id == other.id
        }

        fn merge(&mut self, new: Self) {
            self.data.extend(new.data);
            // Keep the earlier received_at time when merging
            if new.received_at < self.received_at {
                self.received_at = new.received_at;
            }
        }
    }

    impl TimedOrderedValue for TimedTestValue {
        fn received_at(&self) -> Instant {
            self.received_at
        }
    }

    #[test]
    fn test_new_timeout_queue() {
        let queue: TimeoutOrderedQueue<u64, TimedTestValue> =
            TimeoutOrderedQueue::new(0, 5, Duration::from_millis(100));

        assert!(queue.current_timeout.is_none());
        assert!(queue.current_timeout_key.is_none());
    }

    #[test]
    fn test_insert_ready_no_timeout_needed() {
        let mut queue = TimeoutOrderedQueue::new(5u64, 3, Duration::from_millis(100));
        let value = TimedTestValue::new_now("test", vec![1, 2, 3]);

        // Insert at next key (should be ready immediately)
        let result = queue.insert(5, value.clone()).unwrap();

        assert_eq!(result.next, Some(value));
        assert!(!result.merged_with_existing);
        assert_eq!(result.evicted, None);

        // Should not have set a timeout since buffer is empty
        assert!(queue.current_timeout.is_none());
        assert!(queue.current_timeout_key.is_none());
    }

    #[test]
    fn test_insert_ready_with_merge_updates_timeout() {
        let mut queue = TimeoutOrderedQueue::new(5u64, 3, Duration::from_millis(100));
        let now = Instant::now();
        let value1 = TimedTestValue::new("test", vec![1, 2], now);
        let value2 = TimedTestValue::new("test", vec![3, 4], now);

        // Pre-populate the queue at the next key
        queue.queue.map.insert(5, value1);

        // Insert should merge and be ready immediately
        let result = queue.insert(5, value2).unwrap();

        assert!(result.next.is_some());
        assert!(result.merged_with_existing);
        assert_eq!(result.evicted, None);

        let merged_value = result.next.unwrap();
        assert_eq!(merged_value.data, vec![1, 2, 3, 4]);

        // Should not have a timeout since buffer is now empty
        assert!(queue.current_timeout.is_none());
        assert!(queue.current_timeout_key.is_none());
    }

    #[tokio::test]
    async fn test_insert_buffered_sets_timeout() {
        let mut queue = TimeoutOrderedQueue::new(5u64, 3, Duration::from_millis(100));
        let value = TimedTestValue::new_now("test", vec![1, 2, 3]);

        // Insert future key (should be buffered and set timeout)
        let result = queue.insert(7, value).unwrap();

        assert_eq!(result.next, None);
        assert!(!result.merged_with_existing);
        assert_eq!(result.evicted, None);

        // Should have set a timeout since buffer now has an item
        assert!(queue.current_timeout.is_some());
        assert_eq!(queue.current_timeout_key, Some(7));
    }

    #[tokio::test]
    async fn test_insert_smaller_key_updates_timeout() {
        let mut queue = TimeoutOrderedQueue::new(0u64, 3, Duration::from_millis(100));
        let now = Instant::now();

        let later_value = TimedTestValue::new("later", vec![2], now + Duration::from_millis(50));
        let earlier_value = TimedTestValue::new("earlier", vec![1], now);

        // Insert later key first
        queue.insert(10, later_value).unwrap();
        assert_eq!(queue.current_timeout_key, Some(10));
        let first_deadline = queue.current_timeout.as_ref().unwrap().deadline();

        // Insert earlier key - should update timeout to earlier deadline
        queue.insert(5, earlier_value).unwrap();
        assert_eq!(queue.current_timeout_key, Some(5));
        let second_deadline = queue.current_timeout.as_ref().unwrap().deadline();

        // New deadline should be earlier since the value was received earlier
        assert!(second_deadline < first_deadline);
    }

    #[tokio::test]
    async fn test_insert_larger_key_no_timeout_update() {
        let mut queue = TimeoutOrderedQueue::new(0u64, 3, Duration::from_millis(100));
        let now = Instant::now();

        let earlier_value = TimedTestValue::new("earlier", vec![1], now);
        let later_value = TimedTestValue::new("later", vec![2], now + Duration::from_millis(50));

        // Insert earlier key first
        queue.insert(5, earlier_value).unwrap();
        assert_eq!(queue.current_timeout_key, Some(5));
        let first_deadline = queue.current_timeout.as_ref().unwrap().deadline();

        // Insert later key - should NOT update timeout
        queue.insert(10, later_value).unwrap();
        assert_eq!(queue.current_timeout_key, Some(5)); // Should still be 5
        let second_deadline = queue.current_timeout.as_ref().unwrap().deadline();

        // Deadline should be the same since timeout wasn't updated
        assert_eq!(second_deadline, first_deadline);
    }

    #[test]
    fn test_insert_error_no_timeout_update() {
        let mut queue = TimeoutOrderedQueue::new(5u64, 3, Duration::from_millis(100));
        let value = TimedTestValue::new_now("test", vec![1, 2, 3]);

        // Insert stale key (should error and not update timeout)
        let result = queue.insert(3, value);

        assert!(result.is_err());
        assert!(queue.current_timeout.is_none());
        assert!(queue.current_timeout_key.is_none());
    }

    #[tokio::test]
    async fn test_pop_updates_timeout_deadline() {
        let timeout_duration = Duration::from_millis(100);
        let mut queue = TimeoutOrderedQueue::new(5u64, 3, timeout_duration);
        let base_time = Instant::now();

        let early_time = base_time - Duration::from_millis(20);
        let late_time = base_time;

        let early_value = TimedTestValue::new("early", vec![1], early_time);
        let late_value = TimedTestValue::new("late", vec![2], late_time);

        // Pre-populate the queue with two items
        queue.queue.map.insert(5, early_value.clone());
        queue.queue.map.insert(10, late_value);
        queue.update_timeout();

        // Timeout should be based on earliest item (key 5)
        assert_eq!(queue.current_timeout_key, Some(5));
        let initial_deadline = queue.current_timeout.as_ref().unwrap().deadline();

        // Pop the earliest item
        let popped = queue.pop();
        assert_eq!(popped, Some(early_value));

        // Timeout should now be based on the remaining item (key 10)
        assert_eq!(queue.current_timeout_key, Some(10));
        let new_deadline = queue.current_timeout.as_ref().unwrap().deadline();

        // New deadline should be later than initial (since remaining item was received
        // later)
        assert!(new_deadline > initial_deadline);
    }

    #[test]
    fn test_pop_empty_no_timeout_update() {
        let mut queue =
            TimeoutOrderedQueue::<u64, TimedTestValue>::new(5u64, 3, Duration::from_millis(100));

        // Pop from empty queue should not crash or set timeout
        let popped = queue.pop();

        assert_eq!(popped, None);
        assert!(queue.current_timeout.is_none());
        assert!(queue.current_timeout_key.is_none());
    }

    #[tokio::test]
    async fn test_progress_to_updates_timeout() {
        let mut queue = TimeoutOrderedQueue::new(5u64, 3, Duration::from_millis(100));
        let value = TimedTestValue::new_now("test", vec![1, 2, 3]);

        // Add a buffered item
        queue.queue.map.insert(10, value);

        // progress_to should always update timeout
        queue.progress_to(7);

        assert!(queue.current_timeout.is_some());
        assert_eq!(queue.current_timeout_key, Some(10));
    }

    #[tokio::test]
    async fn test_timeout_future_returns_current_timeout() {
        let mut queue = TimeoutOrderedQueue::new(5u64, 3, Duration::from_millis(100));

        // Initially no timeout
        assert!(queue.timeout_future().is_none());

        // Add buffered item to trigger timeout
        let value = TimedTestValue::new_now("test", vec![1]);
        queue.insert(10, value).unwrap();

        // Should now have a timeout
        assert!(queue.timeout_future().is_some());
    }

    #[tokio::test]
    async fn test_handle_timeout_removes_expired_items() {
        let timeout_duration = Duration::from_millis(50);
        let mut queue = TimeoutOrderedQueue::new(0u64, 5, timeout_duration);
        let base_time = Instant::now();

        let old_time = base_time - Duration::from_millis(100); // Already expired
        let future_time = base_time + Duration::from_millis(100); // Not expired

        let expired_value = TimedTestValue::new("expired", vec![1], old_time);
        let fresh_value = TimedTestValue::new("fresh", vec![2], future_time);

        // Insert both values
        queue.insert(5, expired_value.clone()).unwrap();
        queue.insert(10, fresh_value.clone()).unwrap();

        // Get the initial timeout key (should be based on expired item - key 5)
        assert_eq!(queue.current_timeout_key, Some(5));

        // Handle timeout should remove only the expired item
        let expired = queue.handle_timeout();

        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].0, 5);
        assert_eq!(expired[0].1, expired_value);

        // Fresh item should still be in the queue
        assert!(queue.queue.map.contains_key(&10));
        assert!(!queue.queue.map.contains_key(&5));

        // Timeout should now be set for the fresh item
        assert_eq!(queue.current_timeout_key, Some(10));
    }

    #[tokio::test]
    async fn test_handle_timeout_updates_timeout_for_remaining() {
        let mut queue = TimeoutOrderedQueue::new(0u64, 5, Duration::from_millis(50));
        let old_time = Instant::now() - Duration::from_millis(100);
        let future_time = Instant::now() + Duration::from_millis(1000);

        let expired_value = TimedTestValue::new("expired", vec![1], old_time);
        let fresh_value = TimedTestValue::new("fresh", vec![2], future_time);

        queue.insert(5, expired_value).unwrap();
        queue.insert(10, fresh_value).unwrap();

        // Handle timeout
        queue.handle_timeout();

        // Should still have a timeout for the remaining fresh item
        assert!(queue.current_timeout.is_some());
        assert_eq!(queue.current_timeout_key, Some(10));
    }

    #[tokio::test]
    async fn test_handle_timeout_clears_timeout_when_all_expired() {
        let mut queue = TimeoutOrderedQueue::new(0u64, 5, Duration::from_millis(50));
        let old_time = Instant::now() - Duration::from_millis(100);

        let expired_value1 = TimedTestValue::new("expired1", vec![1], old_time);
        let expired_value2 = TimedTestValue::new("expired2", vec![2], old_time);

        queue.insert(5, expired_value1).unwrap();
        queue.insert(10, expired_value2).unwrap();

        // Handle timeout should remove all items
        let expired = queue.handle_timeout();

        assert_eq!(expired.len(), 2);
        assert!(queue.current_timeout.is_none());
        assert!(queue.current_timeout_key.is_none());
        assert!(queue.queue.map.is_empty());
    }

    #[tokio::test]
    async fn test_timeout_calculation_with_earliest_item() {
        let mut queue = TimeoutOrderedQueue::new(0u64, 5, Duration::from_millis(100));
        let now = Instant::now();

        // Insert items with different received times
        let early_value = TimedTestValue::new("early", vec![1], now - Duration::from_millis(50));
        let late_value = TimedTestValue::new("late", vec![2], now);

        // Insert in reverse order to test that timeout uses earliest
        queue.insert(20, late_value).unwrap();
        assert_eq!(queue.current_timeout_key, Some(20));

        queue.insert(10, early_value).unwrap();
        // Should update to key 10 since it's smaller
        assert_eq!(queue.current_timeout_key, Some(10));

        // Timeout should be based on the earliest item (key 10)
        assert!(queue.current_timeout.is_some());
    }

    #[tokio::test]
    async fn test_merge_preserves_earliest_time() {
        let mut queue = TimeoutOrderedQueue::new(0u64, 5, Duration::from_millis(100));
        let early_time = Instant::now() - Duration::from_millis(50);
        let late_time = Instant::now();

        let early_fragment = TimedTestValue::new("msg", vec![1], early_time);
        let late_fragment = TimedTestValue::new("msg", vec![2], late_time);

        // Insert early fragment first
        queue.insert(10, early_fragment).unwrap();

        // Insert late fragment (should merge and keep early time)
        let result = queue.insert(10, late_fragment).unwrap();

        assert_eq!(result.next, None);
        assert!(result.merged_with_existing);
        assert_eq!(result.evicted, None);

        let merged = queue.queue.map.get(&10).unwrap();
        assert_eq!(merged.received_at, early_time); // Should keep earlier time
        assert_eq!(merged.data, vec![1, 2]); // Should merge data
    }

    #[tokio::test]
    async fn test_complete_timeout_workflow() {
        let timeout_duration = Duration::from_millis(100);
        let mut queue = TimeoutOrderedQueue::new(0u64, 3, timeout_duration);
        let base_time = Instant::now();

        // Insert some out-of-order messages with specific times
        let msg0_time = base_time - Duration::from_millis(10);
        let msg1_time = base_time;
        let msg2_time = base_time + Duration::from_millis(10);

        let msg1 = TimedTestValue::new("msg1", vec![1], msg1_time);
        let msg2 = TimedTestValue::new("msg2", vec![2], msg2_time);
        let msg0 = TimedTestValue::new("msg0", vec![0], msg0_time);

        // Insert message 2 (buffered, sets timeout for key 2)
        let result = queue.insert(2, msg2).unwrap();
        assert_eq!(result.next, None);
        assert!(!result.merged_with_existing);
        assert_eq!(queue.current_timeout_key, Some(2));

        // Insert message 1 (buffered, should update timeout to key 1 since it's
        // smaller)
        let result = queue.insert(1, msg1).unwrap();
        assert_eq!(result.next, None);
        assert!(!result.merged_with_existing);
        assert_eq!(queue.current_timeout_key, Some(1));

        // Insert message 0 (ready immediately)
        let result = queue.insert(0, msg0.clone()).unwrap();
        assert_eq!(result.next, Some(msg0));
        assert!(!result.merged_with_existing);
        // Timeout should still be based on key 1 (earliest buffered)
        assert_eq!(queue.current_timeout_key, Some(1));

        // Progress and pop message 1
        queue.progress_to(1);
        let popped = queue.pop().unwrap();
        assert_eq!(popped.data, vec![1]);
        // Timeout should now be based on key 2
        assert_eq!(queue.current_timeout_key, Some(2));

        // Progress and pop message 2
        queue.progress_to(2);
        let popped = queue.pop().unwrap();
        assert_eq!(popped.data, vec![2]);
        assert!(queue.current_timeout.is_none()); // No more buffered items
        assert!(queue.current_timeout_key.is_none());
    }

    #[tokio::test]
    async fn test_eviction_updates_timeout() {
        let mut queue = TimeoutOrderedQueue::new(0u64, 2, Duration::from_millis(100));
        let now = Instant::now();

        let value1 = TimedTestValue::new("v1", vec![1], now);
        let value2 = TimedTestValue::new("v2", vec![2], now + Duration::from_millis(10));
        let value3 = TimedTestValue::new("v3", vec![3], now + Duration::from_millis(5));

        // Fill the buffer
        queue.insert(10, value1).unwrap();
        assert_eq!(queue.current_timeout_key, Some(10));

        queue.insert(20, value2).unwrap();
        // Should not update timeout since 20 > 10
        assert_eq!(queue.current_timeout_key, Some(10));

        // Insert should cause eviction of key 20 and update timeout
        let result = queue.insert(15, value3).unwrap();

        assert_eq!(result.next, None);
        assert!(!result.merged_with_existing);
        assert!(result.evicted.is_some());
        assert_eq!(result.evicted.unwrap().0, 20);

        // Timeout should still be based on key 10 (smallest remaining)
        assert_eq!(queue.current_timeout_key, Some(10));
        assert!(queue.current_timeout.is_some());
    }
}
