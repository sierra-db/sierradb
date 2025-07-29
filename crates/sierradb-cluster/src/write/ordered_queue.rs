use std::collections::{BTreeMap, btree_map::Entry};

use thiserror::Error;

pub trait OrderedValue {
    fn key_eq(&self, other: &Self) -> bool;
    fn merge(&mut self, new: Self);
}

#[derive(Clone, Debug)]
pub struct OrderedQueue<K, V> {
    pub map: BTreeMap<K, V>,
    next: K,
    limit: usize,
}

impl<K, V> OrderedQueue<K, V> {
    pub fn new(initial_next: K, limit: usize) -> Self {
        assert!(limit > 0, "limit must be greater than 0");
        OrderedQueue {
            map: BTreeMap::new(),
            next: initial_next,
            limit,
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<InsertResult<K, V>, Error<K, V>>
    where
        K: Ord,
        V: OrderedValue,
    {
        if key == self.next {
            // Return, merging with any existing entry
            match self.map.entry(key) {
                Entry::Vacant(_) => {
                    return Ok(InsertResult {
                        next: Some(value),
                        merged_with_existing: false,
                        evicted: None,
                    });
                }
                Entry::Occupied(entry) => {
                    if value.key_eq(entry.get()) {
                        let mut existing = entry.remove();
                        existing.merge(value);
                        return Ok(InsertResult {
                            next: Some(existing),
                            merged_with_existing: true,
                            evicted: None,
                        });
                    } else {
                        return Err(Error::Conflict { value });
                    }
                }
            }
        } else if key < self.next {
            return Err(Error::Stale { key, value });
        }

        // Evict a record if we're full
        let mut evicted = None;
        if self.map.len() >= self.limit {
            let last_entry = self.map.last_entry().expect("limit must be greater than 0");
            if last_entry.key() > &key {
                evicted = Some(last_entry.remove_entry());
            } else {
                return Err(Error::Full { key, value });
            }
        }

        // Insert or merge
        match self.map.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(value);
                Ok(InsertResult {
                    next: None,
                    merged_with_existing: false,
                    evicted,
                })
            }
            Entry::Occupied(mut entry) => {
                if value.key_eq(entry.get()) {
                    entry.get_mut().merge(value);
                    Ok(InsertResult {
                        next: None,
                        merged_with_existing: true,
                        evicted,
                    })
                } else {
                    Err(Error::Conflict { value })
                }
            }
        }
    }

    pub fn pop(&mut self) -> Option<V>
    where
        K: Ord,
    {
        self.map.remove(&self.next)
    }

    pub fn progress_to(&mut self, next: K) {
        self.next = next;
    }

    pub fn next(&self) -> &K {
        &self.next
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq, PartialOrd, Ord)]
pub struct InsertResult<K, V> {
    pub next: Option<V>,
    pub merged_with_existing: bool,
    pub evicted: Option<(K, V)>,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum Error<K, V> {
    #[error("conflict")]
    Conflict { value: V },
    #[error("queue full")]
    Full { key: K, value: V },
    #[error("stale insert")]
    Stale { key: K, value: V },
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test implementation of OrderedValue
    #[derive(Debug, Clone, PartialEq)]
    struct TestValue {
        id: String,
        data: Vec<u8>,
    }

    impl OrderedValue for TestValue {
        fn key_eq(&self, other: &Self) -> bool {
            self.id == other.id
        }

        fn merge(&mut self, new: Self) {
            self.data.extend(new.data);
        }
    }

    impl TestValue {
        fn new(id: &str, data: Vec<u8>) -> Self {
            TestValue {
                id: id.to_string(),
                data,
            }
        }
    }

    #[test]
    fn test_new_queue() {
        let queue: OrderedQueue<u64, TestValue> = OrderedQueue::new(0, 5);
        assert_eq!(queue.next, 0);
        assert_eq!(queue.limit, 5);
        assert!(queue.map.is_empty());
    }

    #[test]
    #[should_panic(expected = "limit must be greater than 0")]
    fn test_new_queue_zero_limit() {
        let _queue: OrderedQueue<u64, TestValue> = OrderedQueue::new(0, 0);
    }

    #[test]
    fn test_insert_next_key_vacant() {
        let mut queue = OrderedQueue::new(5u64, 3);
        let value = TestValue::new("test", vec![1, 2, 3]);

        let result = queue.insert(5, value.clone()).unwrap();
        assert_eq!(
            result,
            InsertResult {
                next: Some(value),
                merged_with_existing: false,
                evicted: None,
            }
        );
    }

    #[test]
    fn test_insert_next_key_occupied_mergeable() {
        let mut queue = OrderedQueue::new(5u64, 3);
        let value1 = TestValue::new("test", vec![1, 2]);
        let value2 = TestValue::new("test", vec![3, 4]);

        // First insert to occupy the slot
        queue.map.insert(5, value1);

        let result = queue.insert(5, value2).unwrap();
        assert_eq!(
            result,
            InsertResult {
                next: Some(TestValue {
                    id: "test".to_string(),
                    data: vec![1, 2, 3, 4]
                }),
                merged_with_existing: true,
                evicted: None,
            }
        );
    }

    #[test]
    fn test_insert_next_key_occupied_conflict() {
        let mut queue = OrderedQueue::new(5u64, 3);
        let value1 = TestValue::new("test1", vec![1, 2]);
        let value2 = TestValue::new("test2", vec![3, 4]);

        // First insert to occupy the slot
        queue.map.insert(5, value1);

        let result = queue.insert(5, value2.clone());
        match result {
            Err(Error::Conflict { value }) => {
                assert_eq!(value, value2);
            }
            _ => panic!("Expected Conflict error"),
        }
    }

    #[test]
    fn test_insert_stale_key() {
        let mut queue = OrderedQueue::new(5u64, 3);
        let value = TestValue::new("test", vec![1, 2, 3]);

        let result = queue.insert(3, value.clone());
        match result {
            Err(Error::Stale {
                key,
                value: returned_value,
            }) => {
                assert_eq!(key, 3);
                assert_eq!(returned_value, value);
            }
            _ => panic!("Expected Stale error"),
        }
    }

    #[test]
    fn test_insert_future_key_vacant() {
        let mut queue = OrderedQueue::new(5u64, 3);
        let value = TestValue::new("test", vec![1, 2, 3]);

        let result = queue.insert(7, value).unwrap();
        assert_eq!(
            result,
            InsertResult {
                next: None,
                merged_with_existing: false,
                evicted: None,
            }
        );

        assert!(queue.map.contains_key(&7));
    }

    #[test]
    fn test_insert_future_key_mergeable() {
        let mut queue = OrderedQueue::new(5u64, 3);
        let value1 = TestValue::new("test", vec![1, 2]);
        let value2 = TestValue::new("test", vec![3, 4]);

        // First insert
        queue.insert(7, value1).unwrap();

        // Second insert should merge
        let result = queue.insert(7, value2).unwrap();
        assert_eq!(
            result,
            InsertResult {
                next: None,
                merged_with_existing: true,
                evicted: None,
            }
        );

        let buffered_value = queue.map.get(&7).unwrap();
        assert_eq!(buffered_value.data, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_insert_future_key_conflict() {
        let mut queue = OrderedQueue::new(5u64, 3);
        let value1 = TestValue::new("test1", vec![1, 2]);
        let value2 = TestValue::new("test2", vec![3, 4]);

        // First insert
        queue.insert(7, value1).unwrap();

        // Second insert should conflict
        let result = queue.insert(7, value2.clone());
        match result {
            Err(Error::Conflict { value }) => {
                assert_eq!(value, value2);
            }
            _ => panic!("Expected Conflict error"),
        }
    }

    #[test]
    fn test_eviction_when_at_limit() {
        let mut queue = OrderedQueue::new(5u64, 2);
        let value1 = TestValue::new("v1", vec![1]);
        let value2 = TestValue::new("v2", vec![2]);
        let value3 = TestValue::new("v3", vec![3]);

        // Fill the buffer
        queue.insert(10, value1.clone()).unwrap();
        queue.insert(20, value2.clone()).unwrap();

        // Insert a value that should evict the largest key (20)
        let result = queue.insert(15, value3.clone()).unwrap();
        assert_eq!(
            result,
            InsertResult {
                next: None,
                merged_with_existing: false,
                evicted: Some((20, value2)),
            }
        );

        assert!(!queue.map.contains_key(&20));
        assert!(queue.map.contains_key(&10));
        assert!(queue.map.contains_key(&15));
    }

    #[test]
    fn test_full_error_when_new_key_is_largest() {
        let mut queue = OrderedQueue::new(5u64, 2);
        let value1 = TestValue::new("v1", vec![1]);
        let value2 = TestValue::new("v2", vec![2]);
        let value3 = TestValue::new("v3", vec![3]);

        // Fill the buffer
        queue.insert(10, value1).unwrap();
        queue.insert(20, value2).unwrap();

        // Try to insert a value larger than all existing keys
        let result = queue.insert(30, value3.clone());
        match result {
            Err(Error::Full { key, value }) => {
                assert_eq!(key, 30);
                assert_eq!(value, value3);
            }
            _ => panic!("Expected Full error"),
        }
    }

    #[test]
    fn test_pop_existing() {
        let mut queue = OrderedQueue::new(5u64, 3);
        let value = TestValue::new("test", vec![1, 2, 3]);

        queue.map.insert(5, value.clone());

        let popped = queue.pop();
        assert_eq!(popped, Some(value));
        assert!(!queue.map.contains_key(&5));
    }

    #[test]
    fn test_pop_non_existing() {
        let mut queue = OrderedQueue::<_, Vec<TestValue>>::new(5u64, 3);
        let popped = queue.pop();
        assert_eq!(popped, None);
    }

    #[test]
    fn test_progress_to() {
        let mut queue = OrderedQueue::<_, Vec<TestValue>>::new(5u64, 3);
        queue.progress_to(10);
        assert_eq!(queue.next, 10);
    }

    #[test]
    fn test_complete_workflow() {
        let mut queue = OrderedQueue::new(0u64, 3);

        // Insert out-of-order messages
        let msg1 = TestValue::new("msg", vec![1]);
        let msg2 = TestValue::new("msg", vec![2]);
        let msg3 = TestValue::new("msg", vec![3]);

        // Insert message 2 (should be buffered)
        let result = queue.insert(2, msg2).unwrap();
        assert_eq!(
            result,
            InsertResult {
                next: None,
                merged_with_existing: false,
                evicted: None,
            }
        );

        // Insert message 1 (should be buffered)
        let result = queue.insert(1, msg1).unwrap();
        assert_eq!(
            result,
            InsertResult {
                next: None,
                merged_with_existing: false,
                evicted: None,
            }
        );

        // Insert message 0 (should be ready immediately)
        let result = queue
            .insert(0, TestValue::new("immediate", vec![0]))
            .unwrap();
        assert_eq!(
            result,
            InsertResult {
                next: Some(TestValue::new("immediate", vec![0])),
                merged_with_existing: false,
                evicted: None,
            }
        );

        // Progress and pop message 1
        queue.progress_to(1);
        let popped = queue.pop().unwrap();
        assert_eq!(popped.data, vec![1]);

        // Progress and pop message 2
        queue.progress_to(2);
        let popped = queue.pop().unwrap();
        assert_eq!(popped.data, vec![2]);

        // Insert and immediately get message 3
        queue.progress_to(3);
        let result = queue.insert(3, msg3.clone()).unwrap();
        assert_eq!(
            result,
            InsertResult {
                next: Some(msg3),
                merged_with_existing: false,
                evicted: None,
            }
        );
    }

    #[test]
    fn test_merge_behavior_with_buffered_values() {
        let mut queue = OrderedQueue::new(0u64, 3);

        // Insert first fragment
        let frag1 = TestValue::new("message", vec![1, 2]);
        queue.insert(5, frag1).unwrap();

        // Insert second fragment (should merge)
        let frag2 = TestValue::new("message", vec![3, 4]);
        let result = queue.insert(5, frag2).unwrap();
        assert_eq!(
            result,
            InsertResult {
                next: None,
                merged_with_existing: true,
                evicted: None,
            }
        );

        // Check that merge actually happened
        let merged = queue.map.get(&5).unwrap();
        assert_eq!(merged.data, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_ordering_with_different_key_types() {
        // Test with string keys to ensure ordering works with different types
        let mut queue = OrderedQueue::new("b".to_string(), 3);

        let val1 = TestValue::new("v1", vec![1]);
        let val2 = TestValue::new("v2", vec![2]);
        let val3 = TestValue::new("v3", vec![3]);

        // Insert in different order
        queue.insert("d".to_string(), val1).unwrap();
        queue.insert("c".to_string(), val2).unwrap();

        // Insert at current next (should be Ready)
        let result = queue.insert("b".to_string(), val3.clone()).unwrap();
        assert_eq!(
            result,
            InsertResult {
                next: Some(val3),
                merged_with_existing: false,
                evicted: None,
            }
        );

        // Check that buffered items are still there and properly ordered
        assert!(queue.map.contains_key("c"));
        assert!(queue.map.contains_key("d"));
    }

    #[test]
    fn test_eviction_preserves_smaller_keys() {
        let mut queue = OrderedQueue::new(0u64, 2);

        // Fill buffer with keys 10 and 20
        queue.insert(10, TestValue::new("v10", vec![10])).unwrap();
        queue.insert(20, TestValue::new("v20", vec![20])).unwrap();

        // Insert key 5 - should evict 20 (largest), keep 10
        let result = queue.insert(5, TestValue::new("v5", vec![5])).unwrap();
        assert_eq!(
            result,
            InsertResult {
                next: None,
                merged_with_existing: false,
                evicted: Some((20, TestValue::new("v20", vec![20]))),
            }
        );

        assert!(queue.map.contains_key(&5));
        assert!(queue.map.contains_key(&10));
        assert!(!queue.map.contains_key(&20));
    }
}
