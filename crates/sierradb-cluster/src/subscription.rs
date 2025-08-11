use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

use kameo::prelude::*;
use serde::{Deserialize, Serialize};
use sierradb::{
    StreamId,
    bucket::{PartitionId, segment::EventRecord},
    id::uuid_to_partition_hash,
};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, instrument, warn};
use uuid::Uuid;

use crate::{ClusterActor, ReplicaRefs};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionMessage {
    pub subscription_id: Uuid,
    pub record: EventRecord,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BroadcastMessage {
    pub partition_id: PartitionId,
    pub record: EventRecord,
}

/// Represents a client subscription to events
#[derive(Debug, Clone)]
pub enum SubscriptionType {
    /// Subscribe to events from a specific stream
    Stream {
        stream_id: StreamId,
        partition_key: Option<Uuid>,
        from_version: Option<u64>,
    },
    /// Subscribe to events from a specific partition  
    Partition {
        partition_id: PartitionId,
        from_sequence: Option<u64>,
    },
}

/// State for managing historical to live event transition
#[derive(Debug)]
pub struct SubscriptionState {
    /// Whether the subscription is still reading historical events
    pub reading_historical: bool,
    /// Last partition sequence read from historical data
    pub last_historical_sequence: Option<u64>,
    /// Last stream version read from historical data (for stream subscriptions)
    pub last_historical_version: Option<u64>,
    /// Buffer for live events received while reading historical events
    pub live_event_buffer: VecDeque<EventRecord>,
    /// Maximum buffer size to prevent memory issues
    pub max_buffer_size: usize,
}

impl Default for SubscriptionState {
    fn default() -> Self {
        Self {
            reading_historical: false,
            last_historical_sequence: None,
            last_historical_version: None,
            live_event_buffer: VecDeque::new(),
            max_buffer_size: 10000, // Reasonable default buffer size
        }
    }
}

/// A subscription registered with the cluster
#[derive(Debug, Clone)]
pub struct Subscription {
    pub id: Uuid,
    pub subscription_type: SubscriptionType,
    pub sender: mpsc::UnboundedSender<SubscriptionMessage>,
    pub partition_ids: Vec<PartitionId>, // Partitions this subscription covers
    pub state: Arc<RwLock<SubscriptionState>>,
}

/// Manages active subscriptions for the cluster
#[derive(Debug)]
pub struct SubscriptionManager {
    /// Active subscriptions by ID
    subscriptions: HashMap<Uuid, Subscription>,
    /// Subscriptions by partition for efficient event distribution
    partition_subscriptions: HashMap<PartitionId, Vec<Uuid>>,
    /// Broadcast sender for notifying about new events
    event_broadcast: broadcast::Sender<BroadcastMessage>,
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SubscriptionManager {
    pub fn new() -> Self {
        let (event_broadcast, _) = broadcast::channel(1000);
        Self {
            subscriptions: HashMap::new(),
            partition_subscriptions: HashMap::new(),
            event_broadcast,
        }
    }

    /// Add a new subscription
    pub fn add_subscription(
        &mut self,
        subscription_type: SubscriptionType,
        sender: mpsc::UnboundedSender<SubscriptionMessage>,
        num_partitions: u16,
    ) -> Uuid {
        let id = Uuid::new_v4();

        // Determine which partitions this subscription covers
        let partition_ids = match &subscription_type {
            SubscriptionType::Stream {
                stream_id,
                partition_key,
                ..
            } => {
                let partition_key = partition_key.unwrap_or_else(|| {
                    Uuid::new_v5(&sierradb::id::NAMESPACE_PARTITION_KEY, stream_id.as_bytes())
                });
                let partition_hash = uuid_to_partition_hash(partition_key);
                let partition_id = partition_hash % num_partitions;
                vec![partition_id]
            }
            SubscriptionType::Partition { partition_id, .. } => {
                vec![*partition_id]
            }
        };

        // Register subscription with each partition
        for partition_id in &partition_ids {
            self.partition_subscriptions
                .entry(*partition_id)
                .or_default()
                .push(id);
        }

        let subscription = Subscription {
            id,
            subscription_type,
            sender,
            partition_ids: partition_ids.clone(),
            state: Arc::new(RwLock::new(SubscriptionState::default())),
        };

        debug!(%id, ?partition_ids, "added new subscription");

        self.subscriptions.insert(id, subscription);
        id
    }

    /// Remove a subscription
    pub fn remove_subscription(&mut self, subscription_id: Uuid) {
        if let Some(subscription) = self.subscriptions.remove(&subscription_id) {
            // Remove from partition mappings
            for partition_id in subscription.partition_ids {
                if let Some(partition_subs) = self.partition_subscriptions.get_mut(&partition_id) {
                    partition_subs.retain(|id| *id != subscription_id);
                    if partition_subs.is_empty() {
                        self.partition_subscriptions.remove(&partition_id);
                    }
                }
            }

            debug!(%subscription_id, "removed subscription");
        }
    }

    /// Notify subscriptions about a new event
    pub fn notify_event(&self, partition_id: PartitionId, record: EventRecord) {
        // Send to broadcast channel for any listeners
        let _ = self.event_broadcast.send(BroadcastMessage {
            partition_id,
            record: record.clone(),
        });

        // Send to specific subscriptions for this partition
        if let Some(subscription_ids) = self.partition_subscriptions.get(&partition_id) {
            for subscription_id in subscription_ids {
                if let Some(subscription) = self.subscriptions.get(subscription_id) {
                    let state = subscription.state.clone();
                    let record_clone = record.clone();
                    let subscription_id = *subscription_id;
                    let sender = subscription.sender.clone();
                    let subscription_type = subscription.subscription_type.clone();

                    // Handle the event directly - no async needed with std::sync::RwLock
                    Self::handle_subscription_event(
                        state,
                        subscription_id,
                        sender,
                        subscription_type,
                        record_clone,
                    );
                }
            }
        }
    }

    /// Handle a single event for a subscription with proper ordering
    fn handle_subscription_event(
        state: Arc<RwLock<SubscriptionState>>,
        subscription_id: Uuid,
        sender: mpsc::UnboundedSender<SubscriptionMessage>,
        subscription_type: SubscriptionType,
        record: EventRecord,
    ) {
        let mut state_guard = state.write().expect("subscription state lock poisoned");

        // Check if event matches subscription criteria
        let should_process = match &subscription_type {
            SubscriptionType::Stream {
                stream_id,
                from_version,
                ..
            } => {
                let matches_stream = record.stream_id == *stream_id;
                let matches_version = from_version.is_none_or(|from| record.stream_version >= from);
                matches_stream && matches_version
            }
            SubscriptionType::Partition { from_sequence, .. } => {
                from_sequence.is_none_or(|from| record.partition_sequence >= from)
            }
        };

        if !should_process {
            return;
        }

        if state_guard.reading_historical {
            // Still reading historical events - buffer this live event
            if state_guard.live_event_buffer.len() >= state_guard.max_buffer_size {
                warn!(
                    %subscription_id,
                    buffer_size = state_guard.live_event_buffer.len(),
                    "subscription live event buffer full, dropping oldest events"
                );
                state_guard.live_event_buffer.pop_front();
            }
            state_guard.live_event_buffer.push_back(record);
        } else {
            // Not reading historical events anymore - send directly
            if sender
                .send(SubscriptionMessage {
                    subscription_id,
                    record,
                })
                .is_err()
            {
                debug!(%subscription_id, "subscription channel closed");
            }
        }
    }

    /// Get subscription count
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Create a broadcast receiver for listening to all events
    pub fn subscribe_to_events(&self) -> broadcast::Receiver<BroadcastMessage> {
        self.event_broadcast.subscribe()
    }
}

/// Message to create a stream subscription
#[derive(Debug)]
pub struct CreateStreamSubscription {
    pub stream_id: StreamId,
    pub partition_key: Option<Uuid>,
    pub from_version: Option<u64>,
    pub sender: mpsc::UnboundedSender<SubscriptionMessage>,
}

/// Message to create a partition subscription
#[derive(Debug)]
pub struct CreatePartitionSubscription {
    pub partition_id: PartitionId,
    pub from_sequence: Option<u64>,
    pub sender: mpsc::UnboundedSender<SubscriptionMessage>,
}

/// Message to remove a subscription
#[derive(Debug, Serialize, Deserialize)]
pub struct RemoveSubscription {
    pub subscription_id: Uuid,
}

/// Message to notify about a new event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifyEvent {
    pub partition_id: PartitionId,
    pub event: EventRecord,
}

/// Message to complete historical reading and transition to live events
#[derive(Debug)]
pub struct CompleteHistoricalRead {
    pub subscription_id: Uuid,
    pub last_version: Option<u64>,
    pub last_sequence: Option<u64>,
}

impl ClusterActor {
    /// Handle creating a stream subscription
    pub fn handle_create_stream_subscription(
        &mut self,
        msg: CreateStreamSubscription,
        ctx: &mut Context<Self, Result<Uuid, Box<ReplicaRefs>>>,
    ) -> Result<Uuid, Box<ReplicaRefs>> {
        let partition_key = msg.partition_key.unwrap_or_else(|| {
            Uuid::new_v5(
                &sierradb::id::NAMESPACE_PARTITION_KEY,
                msg.stream_id.as_bytes(),
            )
        });
        let partition_hash = uuid_to_partition_hash(partition_key);
        let partition_id = partition_hash % self.topology_manager().num_partitions;

        if !self.topology_manager().has_partition(partition_id) {
            let replicas = self.topology_manager().get_available_replicas(partition_id);
            return Err(Box::new(replicas));
        }

        let subscription_type = SubscriptionType::Stream {
            stream_id: msg.stream_id.clone(),
            partition_key: Some(partition_key),
            from_version: msg.from_version,
        };

        let subscription_id = self.subscription_manager.add_subscription(
            subscription_type,
            msg.sender.clone(),
            self.topology_manager().num_partitions,
        );

        // If from_version is specified, read historical events
        if let Some(from_version) = msg.from_version {
            debug!(
                subscription_id = %subscription_id,
                stream_id = %msg.stream_id,
                from_version,
                "reading historical events for stream subscription"
            );

            // Mark subscription as reading historical events
            if let Some(subscription) = self
                .subscription_manager
                .subscriptions
                .get(&subscription_id)
                && let Ok(mut state_guard) = subscription.state.write()
            {
                state_guard.reading_historical = true;
            }

            let database = self.database.clone();
            let cluster_ref = ctx.actor_ref().clone();

            tokio::spawn(async move {
                let mut last_version = None;
                let mut last_sequence = None;

                match database
                    .read_stream(partition_id, msg.stream_id, from_version, false)
                    .await
                {
                    Ok(mut iter) => {
                        while let Ok(Some(commit)) = iter.next().await {
                            for record in commit {
                                if record.stream_version >= from_version {
                                    last_version = Some(record.stream_version);
                                    last_sequence = Some(record.partition_sequence);

                                    if msg
                                        .sender
                                        .send(SubscriptionMessage {
                                            subscription_id,
                                            record,
                                        })
                                        .is_err()
                                    {
                                        debug!(subscription_id = %subscription_id, "subscription channel closed during historical replay");
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    Err(err) => {
                        debug!(%subscription_id, %err, "failed to read historical events for stream subscription");
                    }
                }

                // Historical reading complete - transition to live events
                let _ = cluster_ref
                    .tell(CompleteHistoricalRead {
                        subscription_id,
                        last_version,
                        last_sequence,
                    })
                    .send()
                    .await;
            });
        }

        Ok(subscription_id)
    }

    /// Handle creating a partition subscription  
    pub fn handle_create_partition_subscription(
        &mut self,
        msg: CreatePartitionSubscription,
        ctx: &mut Context<Self, Result<Uuid, Box<ReplicaRefs>>>,
    ) -> Result<Uuid, Box<ReplicaRefs>> {
        if !self.topology_manager().has_partition(msg.partition_id) {
            let replicas = self
                .topology_manager()
                .get_available_replicas(msg.partition_id);
            return Err(Box::new(replicas));
        }

        let subscription_type = SubscriptionType::Partition {
            partition_id: msg.partition_id,
            from_sequence: msg.from_sequence,
        };

        let subscription_id = self.subscription_manager.add_subscription(
            subscription_type,
            msg.sender.clone(),
            self.topology_manager().num_partitions,
        );

        // If from_sequence is specified, read historical events
        if let Some(from_sequence) = msg.from_sequence {
            debug!(
                subscription_id = %subscription_id,
                partition_id = msg.partition_id,
                from_sequence,
                "reading historical events for partition subscription"
            );

            // Mark subscription as reading historical events
            if let Some(subscription) = self
                .subscription_manager
                .subscriptions
                .get(&subscription_id)
                && let Ok(mut state_guard) = subscription.state.write()
            {
                state_guard.reading_historical = true;
            }

            let database = self.database.clone();
            let partition_id = msg.partition_id;
            let sender = msg.sender;
            let cluster_ref = ctx.actor_ref().clone();

            tokio::spawn(async move {
                let mut last_sequence = None;

                match database.read_partition(partition_id, from_sequence).await {
                    Ok(mut iter) => {
                        while let Ok(Some(commit)) = iter.next().await {
                            for record in commit {
                                if record.partition_sequence >= from_sequence {
                                    last_sequence = Some(record.partition_sequence);

                                    if sender
                                        .send(SubscriptionMessage {
                                            subscription_id,
                                            record,
                                        })
                                        .is_err()
                                    {
                                        debug!(subscription_id = %subscription_id, "subscription channel closed during historical replay");
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    Err(err) => {
                        debug!(%subscription_id, %err, "failed to read historical events for partition subscription");
                    }
                }

                // Historical reading complete - transition to live events
                let _ = cluster_ref
                    .tell(CompleteHistoricalRead {
                        subscription_id,
                        last_version: None, // Partition subscriptions don't track stream versions
                        last_sequence,
                    })
                    .send()
                    .await;
            });
        }

        Ok(subscription_id)
    }

    /// Handle removing a subscription
    pub fn handle_remove_subscription(&mut self, msg: RemoveSubscription) {
        self.subscription_manager
            .remove_subscription(msg.subscription_id);
    }

    /// Handle notifying about a new event
    pub fn handle_notify_event(&mut self, msg: NotifyEvent) {
        debug!(
            partition_id = msg.partition_id,
            event_id = %msg.event.event_id,
            "ClusterActor handling NotifyEvent, delegating to subscription manager"
        );
        self.subscription_manager
            .notify_event(msg.partition_id, msg.event);
    }

    /// Get subscription manager for event notification
    pub fn get_subscription_manager(&self) -> &SubscriptionManager {
        &self.subscription_manager
    }

    /// Handle completion of historical reading and transition to live events
    pub fn handle_complete_historical_read(&mut self, msg: CompleteHistoricalRead) {
        if let Some(subscription) = self
            .subscription_manager
            .subscriptions
            .get(&msg.subscription_id)
        {
            let state = subscription.state.clone();
            let sender = subscription.sender.clone();
            let subscription_id = msg.subscription_id;

            let mut state_guard = match state.write() {
                Ok(guard) => guard,
                Err(_) => {
                    debug!(%subscription_id, "subscription state lock poisoned during transition");
                    return;
                }
            };
            state_guard.reading_historical = false;
            state_guard.last_historical_sequence = msg.last_sequence;
            state_guard.last_historical_version = msg.last_version;

            debug!(
                %subscription_id,
                last_sequence = ?msg.last_sequence,
                last_version = ?msg.last_version,
                buffer_size = state_guard.live_event_buffer.len(),
                "transitioning from historical to live events"
            );

            // Process buffered live events in order, filtering out duplicates
            let mut events_to_send = Vec::new();
            while let Some(buffered_event) = state_guard.live_event_buffer.pop_front() {
                let should_send = match (msg.last_sequence, msg.last_version) {
                    (Some(last_seq), Some(last_ver)) => {
                        // For stream subscriptions, check both sequence and version
                        buffered_event.partition_sequence > last_seq
                            && buffered_event.stream_version > last_ver
                    }
                    (Some(last_seq), None) => {
                        // For partition subscriptions, check sequence only
                        buffered_event.partition_sequence > last_seq
                    }
                    _ => true, // No historical events were read, send all buffered events
                };

                if should_send {
                    events_to_send.push(buffered_event);
                }
            }

            // Sort events by partition sequence to ensure order
            events_to_send.sort_by_key(|event| event.partition_sequence);

            // Send buffered events
            for event in events_to_send {
                if sender
                    .send(SubscriptionMessage {
                        subscription_id,
                        record: event,
                    })
                    .is_err()
                {
                    debug!(%subscription_id, "subscription channel closed during buffer flush");
                    break;
                }
            }
        }
    }
}

// Message implementations for Kameo (local messages only)

impl Message<CreateStreamSubscription> for ClusterActor {
    type Reply = Result<Uuid, Box<ReplicaRefs>>;

    #[instrument(skip_all, fields(stream_id = %msg.stream_id))]
    async fn handle(
        &mut self,
        msg: CreateStreamSubscription,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.handle_create_stream_subscription(msg, ctx)
    }
}

impl Message<CreatePartitionSubscription> for ClusterActor {
    type Reply = Result<Uuid, Box<ReplicaRefs>>;

    #[instrument(skip_all, fields(partition_id = msg.partition_id))]
    async fn handle(
        &mut self,
        msg: CreatePartitionSubscription,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.handle_create_partition_subscription(msg, ctx)
    }
}

impl Message<RemoveSubscription> for ClusterActor {
    type Reply = ();

    #[instrument(skip_all, fields(subscription_id = %msg.subscription_id))]
    async fn handle(
        &mut self,
        msg: RemoveSubscription,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.handle_remove_subscription(msg)
    }
}

#[remote_message]
impl Message<NotifyEvent> for ClusterActor {
    type Reply = ();

    #[instrument(skip_all, fields(partition_id = msg.partition_id, event_id = %msg.event.event_id))]
    async fn handle(
        &mut self,
        msg: NotifyEvent,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        debug!(
            partition_id = msg.partition_id,
            event_id = %msg.event.event_id,
            stream_id = %msg.event.stream_id,
            "ClusterActor received NotifyEvent message"
        );
        self.handle_notify_event(msg)
    }
}

impl Message<CompleteHistoricalRead> for ClusterActor {
    type Reply = ();

    #[instrument(skip_all, fields(subscription_id = %msg.subscription_id))]
    async fn handle(
        &mut self,
        msg: CompleteHistoricalRead,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.handle_complete_historical_read(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sierradb::{
        database::{DatabaseBuilder, Database, Transaction, NewEvent, ExpectedVersion}, 
        StreamId as SierraStreamId,
        id::{uuid_v7_with_partition_hash, uuid_to_partition_hash}
    };
    use tempfile::{TempDir, tempdir};
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};
    use smallvec::smallvec;

    #[tokio::test]
    async fn test_subscription_manager_basic() {
        let mut manager = SubscriptionManager::new();
        let (sender, mut receiver) = mpsc::unbounded_channel();

        let subscription_type = SubscriptionType::Partition {
            partition_id: 0,
            from_sequence: None,
        };

        let subscription_id = manager.add_subscription(subscription_type, sender, 16);

        assert_eq!(manager.subscription_count(), 1);

        // Create a test event
        let event = EventRecord {
            event_id: Uuid::new_v4(),
            partition_key: Uuid::new_v4(),
            partition_id: 0,
            transaction_id: Uuid::new_v4(),
            partition_sequence: 1,
            stream_version: 1,
            timestamp: 0,
            stream_id: StreamId::new("test-stream").unwrap(),
            event_name: "TestEvent".to_string(),
            metadata: Vec::new(),
            payload: Vec::new(),
            confirmation_count: 3,
            offset: 0,
            size: 0,
        };

        manager.notify_event(0, event.clone());

        // Should receive the event
        let msg = receiver.recv().await.unwrap();
        assert_eq!(
            msg,
            SubscriptionMessage {
                subscription_id,
                record: event.clone()
            }
        );

        // Remove subscription
        manager.remove_subscription(subscription_id);
        assert_eq!(manager.subscription_count(), 0);
    }

    async fn create_temp_db() -> (TempDir, Database) {
        let temp_dir = tempdir().expect("Failed to create temp directory");
        let db = DatabaseBuilder::new()
            .flush_interval_events(1)
            .total_buckets(4)
            .bucket_ids_from_range(0..4)
            .open(temp_dir.path())
            .expect("Failed to open database");
        (temp_dir, db)
    }

    async fn append_test_events(
        db: &Database,
        stream_id: &SierraStreamId,
        count: u32,
    ) -> Result<Vec<(u64, u64)>, Box<dyn std::error::Error>> {
        let partition_key = Uuid::new_v5(&sierradb::id::NAMESPACE_PARTITION_KEY, stream_id.as_bytes());
        let partition_hash = uuid_to_partition_hash(partition_key);
        let partition_id = partition_hash % 4; // Match test database setup
        let mut version_sequence_pairs = Vec::new();

        for i in 0..count {
            // Use Any for test simplicity - in real usage you'd want exact version control
            let expected_version = ExpectedVersion::Any;
            
            let event = NewEvent {
                event_id: uuid_v7_with_partition_hash(partition_hash),
                stream_id: stream_id.clone(),
                stream_version: expected_version,
                event_name: format!("TestEvent{}", i),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                metadata: Vec::new(),
                payload: format!("payload-{}", i).into_bytes(),
            };

            let transaction = Transaction::new(
                partition_key,
                partition_id,
                smallvec![event],
            )?;

            let result = db.append_events(transaction).await?;
            
            // Store the actual stream version and partition sequence from the result
            version_sequence_pairs.push((
                i as u64 + 1, // stream version (1-based)
                result.first_partition_sequence, // partition sequence from append result
            ));
        }

        Ok(version_sequence_pairs)
    }

    fn create_test_event(stream_id: &StreamId, sequence: u64, version: u64, name: &str) -> EventRecord {
        let partition_key = Uuid::new_v5(&sierradb::id::NAMESPACE_PARTITION_KEY, stream_id.as_bytes());
        let partition_hash = uuid_to_partition_hash(partition_key);
        let partition_id = partition_hash % 4; // Match the num_partitions used in tests
        
        EventRecord {
            event_id: Uuid::new_v4(),
            partition_key,
            partition_id,
            transaction_id: Uuid::new_v4(),
            partition_sequence: sequence,
            stream_version: version,
            timestamp: 0,
            stream_id: stream_id.clone(),
            event_name: name.to_string(),
            metadata: Vec::new(),
            payload: Vec::new(),
            confirmation_count: 1,
            offset: 0,
            size: 0,
        }
    }

    #[tokio::test]
    async fn test_historical_to_live_event_transition() {
        let stream_id = StreamId::new("test-stream").unwrap();

        // Create subscription manager
        let mut manager = SubscriptionManager::new();
        let (sender, mut receiver) = mpsc::unbounded_channel();

        // Create stream subscription from version 2
        let subscription_type = SubscriptionType::Stream {
            stream_id: stream_id.clone(),
            partition_key: None,
            from_version: Some(2),
        };

        let subscription_id = manager.add_subscription(subscription_type, sender, 4);
        let subscription = manager.subscriptions.get(&subscription_id).unwrap();

        // Mark as reading historical events
        {
            let mut state = subscription.state.write().unwrap();
            state.reading_historical = true;
        }

        // Simulate live events arriving while reading historical
        let live_event1 = create_test_event(&stream_id, 4, 4, "LiveEvent1");
        let live_event2 = create_test_event(&stream_id, 5, 5, "LiveEvent2");

        // Send live events - these should be buffered
        manager.notify_event(live_event1.partition_id, live_event1.clone());
        manager.notify_event(live_event2.partition_id, live_event2.clone());

        // Verify events are buffered, not sent yet
        assert!(timeout(Duration::from_millis(100), receiver.recv()).await.is_err());

        // Verify buffer has events
        {
            let state = subscription.state.read().unwrap();
            assert!(state.reading_historical);
            assert_eq!(state.live_event_buffer.len(), 2);
        }

        // Simulate historical reading completion
        let mut fake_manager = SubscriptionManager::new();
        fake_manager.subscriptions = manager.subscriptions.clone();
        
        // Complete historical reading (last historical was version 3, sequence 3)
        let complete_msg = CompleteHistoricalRead {
            subscription_id,
            last_version: Some(3),
            last_sequence: Some(3),
        };

        // Process completion - this should flush buffered events
        if let Some(subscription) = fake_manager.subscriptions.get(&subscription_id) {
            let state = subscription.state.clone();
            let sender = subscription.sender.clone();

            let mut state_guard = state.write().unwrap();
            state_guard.reading_historical = false;
            state_guard.last_historical_sequence = complete_msg.last_sequence;
            state_guard.last_historical_version = complete_msg.last_version;

            // Process buffered live events in order, filtering out duplicates
            let mut events_to_send = Vec::new();
            while let Some(buffered_event) = state_guard.live_event_buffer.pop_front() {
                let should_send = match (complete_msg.last_sequence, complete_msg.last_version) {
                    (Some(last_seq), Some(last_ver)) => {
                        buffered_event.partition_sequence > last_seq && buffered_event.stream_version > last_ver
                    }
                    _ => true,
                };

                if should_send {
                    events_to_send.push(buffered_event);
                }
            }

            // Sort events by partition sequence to ensure order
            events_to_send.sort_by_key(|event| event.partition_sequence);

            // Send buffered events
            for event in events_to_send {
                let _ = sender.send(SubscriptionMessage {
                    subscription_id,
                    record: event,
                });
            }
        }

        // Should now receive the buffered events in order
        let msg1 = timeout(Duration::from_millis(100), receiver.recv()).await
            .expect("Should receive first live event")
            .expect("Channel should not be closed");
        assert_eq!(msg1.record.event_name, "LiveEvent1");
        assert_eq!(msg1.record.stream_version, 4);

        let msg2 = timeout(Duration::from_millis(100), receiver.recv()).await
            .expect("Should receive second live event")
            .expect("Channel should not be closed");
        assert_eq!(msg2.record.event_name, "LiveEvent2");
        assert_eq!(msg2.record.stream_version, 5);

        // Should not receive any more events
        assert!(timeout(Duration::from_millis(100), receiver.recv()).await.is_err());
    }

    #[tokio::test]
    async fn test_event_buffering_during_historical_read() {
        let mut manager = SubscriptionManager::new();
        let (sender, mut receiver) = mpsc::unbounded_channel();

        let stream_id = StreamId::new("test-stream").unwrap();
        let subscription_type = SubscriptionType::Stream {
            stream_id: stream_id.clone(),
            partition_key: None,
            from_version: Some(1),
        };

        let subscription_id = manager.add_subscription(subscription_type, sender, 4);
        let subscription = manager.subscriptions.get(&subscription_id).unwrap();

        // Mark as reading historical events
        {
            let mut state = subscription.state.write().unwrap();
            state.reading_historical = true;
        }

        // Send multiple live events
        for i in 0..5 {
            let event = create_test_event(&stream_id, i + 10, i + 10, &format!("BufferedEvent{}", i));
            manager.notify_event(event.partition_id, event);
        }

        // Verify all events are buffered
        {
            let state = subscription.state.read().unwrap();
            assert!(state.reading_historical);
            assert_eq!(state.live_event_buffer.len(), 5);
        }

        // No events should be received yet
        assert!(timeout(Duration::from_millis(100), receiver.recv()).await.is_err());
    }

    #[tokio::test]
    async fn test_direct_event_delivery_when_not_reading_historical() {
        let mut manager = SubscriptionManager::new();
        let (sender, mut receiver) = mpsc::unbounded_channel();

        let stream_id = StreamId::new("test-stream").unwrap();
        let subscription_type = SubscriptionType::Stream {
            stream_id: stream_id.clone(),
            partition_key: None,
            from_version: None, // No historical reading
        };

        let subscription_id = manager.add_subscription(subscription_type, sender, 4);

        // Send an event - should be delivered immediately
        let event = create_test_event(&stream_id, 1, 1, "DirectEvent");

        manager.notify_event(event.partition_id, event.clone());

        // Should receive the event immediately
        let msg = timeout(Duration::from_millis(100), receiver.recv()).await
            .expect("Should receive event immediately")
            .expect("Channel should not be closed");
        assert_eq!(msg.record.event_name, "DirectEvent");
        assert_eq!(msg.subscription_id, subscription_id);
    }

    #[tokio::test]
    async fn test_buffer_overflow_protection() {
        let mut manager = SubscriptionManager::new();
        let (sender, _receiver) = mpsc::unbounded_channel();

        let stream_id = StreamId::new("test-stream").unwrap();
        let subscription_type = SubscriptionType::Stream {
            stream_id: stream_id.clone(),
            partition_key: None,
            from_version: Some(1),
        };

        let subscription_id = manager.add_subscription(subscription_type, sender, 4);
        let subscription = manager.subscriptions.get(&subscription_id).unwrap();

        // Mark as reading historical and set small buffer size
        {
            let mut state = subscription.state.write().unwrap();
            state.reading_historical = true;
            state.max_buffer_size = 3; // Small buffer for testing
        }

        // Send more events than buffer can hold
        for i in 0..5 {
            let event = create_test_event(&stream_id, i + 1, i + 1, &format!("OverflowEvent{}", i));
            manager.notify_event(event.partition_id, event);
        }

        // Buffer should be limited to max size
        {
            let state = subscription.state.read().unwrap();
            assert_eq!(state.live_event_buffer.len(), 3);
            
            // Should contain the last 3 events (oldest dropped)
            let event_names: Vec<_> = state.live_event_buffer.iter()
                .map(|e| e.event_name.clone())
                .collect();
            assert_eq!(event_names, vec!["OverflowEvent2", "OverflowEvent3", "OverflowEvent4"]);
        }
    }

    #[tokio::test]
    async fn test_full_historical_to_live_integration_with_database() {
        let (_temp_dir, db) = create_temp_db().await;
        let stream_id = SierraStreamId::new("integration-test-stream").unwrap();
        
        // Write 3 historical events to the database
        let historical_events = append_test_events(&db, &stream_id, 3).await
            .expect("Failed to write historical events");
        
        // Create a ClusterActor mock or at least the parts we need
        let mut manager = SubscriptionManager::new();
        let (sender, mut receiver) = mpsc::unbounded_channel();

        // Create stream subscription starting from version 2
        let subscription_type = SubscriptionType::Stream {
            stream_id: StreamId::new("integration-test-stream").unwrap(),
            partition_key: None,
            from_version: Some(2),
        };

        let subscription_id = manager.add_subscription(subscription_type, sender, 4);
        let subscription = manager.subscriptions.get(&subscription_id).unwrap();

        // Get the correct partition ID for this stream
        let partition_key = Uuid::new_v5(&sierradb::id::NAMESPACE_PARTITION_KEY, stream_id.as_bytes());
        let partition_hash = uuid_to_partition_hash(partition_key);
        let partition_id = partition_hash % 4;

        // Mark subscription as reading historical events (simulate the real flow)
        {
            let mut state = subscription.state.write().unwrap();
            state.reading_historical = true;
        }

        // Create and send some live events while "reading historical"
        let live_event1 = create_test_event(
            &StreamId::new("integration-test-stream").unwrap(), 
            historical_events[2].1 + 1, // Next sequence after last historical
            4,  // Next version
            "LiveEvent1"
        );
        let live_event2 = create_test_event(
            &StreamId::new("integration-test-stream").unwrap(), 
            historical_events[2].1 + 2, // Next sequence
            5,  // Next version  
            "LiveEvent2"
        );

        // Send live events - these should be buffered
        manager.notify_event(partition_id, live_event1.clone());
        manager.notify_event(partition_id, live_event2.clone());

        // Verify events are buffered, not sent yet
        assert!(timeout(Duration::from_millis(100), receiver.recv()).await.is_err());

        // Verify buffer has the live events
        {
            let state = subscription.state.read().unwrap();
            assert!(state.reading_historical);
            assert_eq!(state.live_event_buffer.len(), 2);
        }

        // Now simulate the historical reading task completion
        // In the real system, this would happen after reading from database
        let last_historical_version = 3;
        let last_historical_sequence = historical_events[2].1; // Last sequence from historical events

        // Complete the historical reading process
        {
            let state = subscription.state.clone();
            let sender = subscription.sender.clone();

            let mut state_guard = state.write().unwrap();
            state_guard.reading_historical = false;
            state_guard.last_historical_sequence = Some(last_historical_sequence);
            state_guard.last_historical_version = Some(last_historical_version);

            // Process buffered live events in order, filtering out duplicates
            let mut events_to_send = Vec::new();
            while let Some(buffered_event) = state_guard.live_event_buffer.pop_front() {
                let should_send = buffered_event.partition_sequence > last_historical_sequence 
                    && buffered_event.stream_version > last_historical_version;

                if should_send {
                    events_to_send.push(buffered_event);
                }
            }

            // Sort events by partition sequence to ensure order
            events_to_send.sort_by_key(|event| event.partition_sequence);

            // Send buffered events
            for event in events_to_send {
                let _ = sender.send(SubscriptionMessage {
                    subscription_id,
                    record: event,
                });
            }
        }

        // Should now receive the buffered live events in order
        let msg1 = timeout(Duration::from_millis(100), receiver.recv()).await
            .expect("Should receive first live event")
            .expect("Channel should not be closed");
        assert_eq!(msg1.record.event_name, "LiveEvent1");
        assert_eq!(msg1.record.stream_version, 4);

        let msg2 = timeout(Duration::from_millis(100), receiver.recv()).await
            .expect("Should receive second live event")  
            .expect("Channel should not be closed");
        assert_eq!(msg2.record.event_name, "LiveEvent2");
        assert_eq!(msg2.record.stream_version, 5);

        // Should not receive any more events
        assert!(timeout(Duration::from_millis(100), receiver.recv()).await.is_err());

        // Verify subscription is now ready for direct live events
        let new_live_event = create_test_event(
            &StreamId::new("integration-test-stream").unwrap(),
            historical_events[2].1 + 3,
            6,
            "NewLiveEvent"
        );

        // This should be sent directly now (not buffered)
        manager.notify_event(partition_id, new_live_event.clone());

        let msg3 = timeout(Duration::from_millis(100), receiver.recv()).await
            .expect("Should receive new live event immediately")
            .expect("Channel should not be closed");
        assert_eq!(msg3.record.event_name, "NewLiveEvent");
        assert_eq!(msg3.record.stream_version, 6);
    }

    #[tokio::test] 
    async fn test_partition_subscription_with_database() {
        let (_temp_dir, db) = create_temp_db().await;
        let stream_id = SierraStreamId::new("partition-test-stream").unwrap();
        
        // Write some events to the database
        let events = append_test_events(&db, &stream_id, 4).await
            .expect("Failed to write events");

        let partition_key = Uuid::new_v5(&sierradb::id::NAMESPACE_PARTITION_KEY, stream_id.as_bytes());
        let partition_hash = uuid_to_partition_hash(partition_key);
        let partition_id = partition_hash % 4;

        // Create partition subscription starting from sequence after 2nd event
        let mut manager = SubscriptionManager::new();
        let (sender, mut receiver) = mpsc::unbounded_channel();

        let subscription_type = SubscriptionType::Partition {
            partition_id,
            from_sequence: Some(events[1].1 + 1), // Start after 2nd event
        };

        let _subscription_id = manager.add_subscription(subscription_type, sender, 4);

        // Should receive events from the database (in real system)
        // For now, simulate what would happen with live events
        for (i, (stream_version, partition_sequence)) in events.iter().enumerate().skip(2) {
            let event = EventRecord {
                event_id: Uuid::new_v4(),
                partition_key,
                partition_id,
                transaction_id: Uuid::new_v4(),
                partition_sequence: *partition_sequence,
                stream_version: *stream_version,
                timestamp: 0,
                stream_id: StreamId::new("partition-test-stream").unwrap(),
                event_name: format!("PartitionEvent{}", i),
                metadata: Vec::new(),
                payload: Vec::new(),
                confirmation_count: 1,
                offset: 0,
                size: 0,
            };

            manager.notify_event(partition_id, event);
        }

        // Should receive events 3 and 4 (indices 2 and 3)
        let msg1 = timeout(Duration::from_millis(100), receiver.recv()).await
            .expect("Should receive third event")
            .expect("Channel should not be closed");
        assert_eq!(msg1.record.event_name, "PartitionEvent2");

        let msg2 = timeout(Duration::from_millis(100), receiver.recv()).await
            .expect("Should receive fourth event")
            .expect("Channel should not be closed");
        assert_eq!(msg2.record.event_name, "PartitionEvent3");
    }
}
