use std::collections::HashMap;

use kameo::prelude::*;
use serde::{Deserialize, Serialize};
use sierradb::{
    StreamId,
    bucket::{PartitionId, segment::EventRecord},
    id::uuid_to_partition_hash,
};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, instrument};
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

/// A subscription registered with the cluster
#[derive(Debug)]
pub struct Subscription {
    pub id: Uuid,
    pub subscription_type: SubscriptionType,
    pub sender: mpsc::UnboundedSender<SubscriptionMessage>,
    pub partition_ids: Vec<PartitionId>, // Partitions this subscription covers
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
                    // Check if event matches subscription criteria
                    let should_send = match &subscription.subscription_type {
                        SubscriptionType::Stream {
                            stream_id,
                            from_version,
                            ..
                        } => {
                            let matches_stream = record.stream_id == *stream_id;
                            let matches_version =
                                from_version.is_none_or(|from| record.stream_version >= from);
                            matches_stream && matches_version
                        }
                        SubscriptionType::Partition { from_sequence, .. } => {
                            from_sequence.is_none_or(|from| record.partition_sequence >= from)
                        }
                    };

                    if should_send
                        && subscription
                            .sender
                            .send(SubscriptionMessage {
                                subscription_id: *subscription_id,
                                record: record.clone(),
                            })
                            .is_err()
                    {
                        // Subscription channel closed, will be cleaned up later
                        debug!(%subscription_id, "subscription channel closed");
                    }
                }
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

impl ClusterActor {
    /// Handle creating a stream subscription
    pub fn handle_create_stream_subscription(
        &mut self,
        msg: CreateStreamSubscription,
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

            let database = self.database.clone();

            tokio::spawn(async move {
                match database
                    .read_stream(partition_id, msg.stream_id, from_version, false)
                    .await
                {
                    Ok(mut iter) => {
                        while let Ok(Some(commit)) = iter.next().await {
                            for record in commit {
                                if record.stream_version >= from_version
                                    && msg
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
                    Err(err) => {
                        debug!(%subscription_id, %err, "failed to read historical events for stream subscription");
                    }
                }
            });
        }

        Ok(subscription_id)
    }

    /// Handle creating a partition subscription  
    pub fn handle_create_partition_subscription(
        &mut self,
        msg: CreatePartitionSubscription,
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

            let database = self.database.clone();
            let partition_id = msg.partition_id;
            let sender = msg.sender;

            tokio::spawn(async move {
                match database.read_partition(partition_id, from_sequence).await {
                    Ok(mut iter) => {
                        while let Ok(Some(commit)) = iter.next().await {
                            for record in commit {
                                if record.partition_sequence >= from_sequence
                                    && sender
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
                    Err(err) => {
                        debug!(%subscription_id, %err, "failed to read historical events for partition subscription");
                    }
                }
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
}

// Message implementations for Kameo (local messages only)

impl Message<CreateStreamSubscription> for ClusterActor {
    type Reply = Result<Uuid, Box<ReplicaRefs>>;

    #[instrument(skip_all, fields(stream_id = %msg.stream_id))]
    async fn handle(
        &mut self,
        msg: CreateStreamSubscription,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.handle_create_stream_subscription(msg)
    }
}

impl Message<CreatePartitionSubscription> for ClusterActor {
    type Reply = Result<Uuid, Box<ReplicaRefs>>;

    #[instrument(skip_all, fields(partition_id = msg.partition_id))]
    async fn handle(
        &mut self,
        msg: CreatePartitionSubscription,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.handle_create_partition_subscription(msg)
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

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
}
