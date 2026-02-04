//! Actor module for managing confirmation state in Sierra
//!
//! This module provides a Kameo actor wrapper around
//! `BucketConfirmationManager` to enable concurrent, fault-tolerant
//! confirmation state management.
//!
//! ## Usage
//!
//! ```ignore
//! use std::path::PathBuf;
//! use actor::ConfirmationActor;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Spawn the confirmation actor
//!     let actor_ref = ConfirmationActor::spawn(
//!         PathBuf::from("/data"),
//!         16,  // num_buckets
//!         3,   // replication_factor
//!     );
//!
//!     // Update event confirmation
//!     let watermark_advanced = actor_ref
//!         .update_event_confirmation(1, 1, 2)
//!         .await?;
//!
//!     // Get current watermark
//!     let watermark = actor_ref
//!         .get_partition_watermark(1)
//!         .await?;
//!
//!     // Check system health
//!     let health = actor_ref.health_check().await?;
//!     println!("System healthy: {}", health.healthy);
//!
//!     Ok(())
//! }
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};

use kameo::prelude::*;
use serde::{Deserialize, Serialize};
use sierradb::IterDirection;
use sierradb::{bucket::segment::EventRecord, database::Database};
use smallvec::SmallVec;
use tokio::sync::broadcast;

use crate::DEFAULT_BATCH_SIZE;

use super::{BucketConfirmationManager, ConfirmationError, PartitionId};

/// Actor that manages confirmation state for buckets and partitions
///
/// This actor wraps `BucketConfirmationManager` to provide concurrent,
/// fault-tolerant access to confirmation state operations. It handles:
///
/// - Updating confirmation counts for events
/// - Tracking watermarks for partitions (highest contiguously confirmed event)
/// - Managing persistence of confirmation state
/// - Administrative operations for recovery scenarios
/// - Validation and statistics gathering
///
/// The actor ensures thread-safe access to the confirmation state and
/// provides asynchronous message-based API for distributed systems.
#[derive(Actor)]
pub struct ConfirmationActor {
    pub manager: BucketConfirmationManager,
    pub database: Database,
    pub broadcast_tx: broadcast::Sender<EventRecord>,
    pub pending_events: HashMap<PartitionId, BTreeMap<u64, EventRecord>>,
    pub next_broadcast_seq: HashMap<PartitionId, u64>,
}

impl ConfirmationActor {
    pub async fn new(
        database: Database,
        replication_factor: u8,
        assigned_partitions: HashSet<PartitionId>,
    ) -> Result<Self, ConfirmationError> {
        let mut manager = BucketConfirmationManager::new(
            database.dir().clone(),
            database.total_buckets(),
            replication_factor,
            assigned_partitions,
        );
        manager.initialize(&database).await?;

        let (broadcast_tx, _) = broadcast::channel(1_000);

        // Initialize next_broadcast_seq to empty HashMap
        // It will be populated with 0 on first access via or_insert(0)
        // This ensures all confirmed events get broadcast when subscriptions start
        let next_broadcast_seq = HashMap::new();

        Ok(Self {
            manager,
            database,
            broadcast_tx,
            pending_events: HashMap::new(),
            next_broadcast_seq,
        })
    }

    pub fn broadcaster(&self) -> broadcast::Sender<EventRecord> {
        self.broadcast_tx.clone()
    }

    fn broadcast_confirmed_events(
        &mut self,
        partition_id: PartitionId,
        old_watermark: u64,
        new_watermark: u64,
    ) {
        let Some(partition_events) = self.pending_events.get_mut(&partition_id) else {
            return;
        };

        // Broadcast events in sequence order from old_watermark+1 to new_watermark
        let mut events_to_broadcast = Vec::new();
        let mut sequences_to_remove = Vec::new();

        for sequence in (old_watermark + 1)..=new_watermark {
            if let Some(event) = partition_events.get(&sequence) {
                events_to_broadcast.push(event.clone());
                sequences_to_remove.push(sequence);
            }
        }

        // Broadcast events in order
        for event in events_to_broadcast {
            match self.broadcast_tx.send(event) {
                Ok(0) | Err(_) => break, // No subscribers or channel closed
                Ok(_) => {}              // Successfully broadcast
            }
        }

        // Clean up broadcast events from buffer
        for sequence in sequences_to_remove {
            partition_events.remove(&sequence);
        }

        // Clean up empty partition entries
        if partition_events.is_empty() {
            self.pending_events.remove(&partition_id);
        }
    }
}

// Message types for different operations

/// Update confirmation and broadcast confirmed events atomically
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateConfirmationWithBroadcast {
    pub partition_id: PartitionId,
    pub versions: SmallVec<[u64; 4]>,
    pub confirmation_count: u8,
    pub partition_sequences: (u64, u64), // (first, last)
}

impl Message<UpdateConfirmationWithBroadcast> for ConfirmationActor {
    type Reply = Result<SmallVec<[bool; 4]>, ConfirmationError>;

    async fn handle(
        &mut self,
        msg: UpdateConfirmationWithBroadcast,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Update confirmations
        let mut results = SmallVec::new();
        for version in &msg.versions {
            let advanced = self
                .manager
                .update_confirmation(msg.partition_id, *version, msg.confirmation_count)
                .await?;
            results.push(advanced);
        }

        let watermark = self
            .manager
            .get_watermark(msg.partition_id)
            .map(|w| w.get())
            .unwrap_or(0);

        let next_to_broadcast = self.next_broadcast_seq.entry(msg.partition_id).or_insert(0);

        if watermark == 0 {
            return Ok(results);
        }

        let highest_confirmed_seq = watermark - 1;

        if *next_to_broadcast <= highest_confirmed_seq {
            let broadcast_from = *next_to_broadcast;
            let broadcast_to = highest_confirmed_seq;

            let mut broadcasted_count = 0;
            let mut last_broadcast_seq = broadcast_from.saturating_sub(1);

            if let Ok(mut iter) = self
                .database
                .read_partition(msg.partition_id, broadcast_from, IterDirection::Forward)
                .await
            {
                'outer: while let Ok(Some(commits)) = iter.next_batch(DEFAULT_BATCH_SIZE).await {
                    for commit in commits {
                        for event in commit {
                            let p = event.partition_sequence;
                            if event.partition_sequence >= broadcast_from
                                && event.partition_sequence <= broadcast_to
                            {
                                match self.broadcast_tx.send(event.clone()) {
                                    Ok(0) => {
                                        // No active subscribers, stop broadcasting
                                        // Don't update next_to_broadcast so we retry later
                                        break 'outer;
                                    }
                                    Ok(_) => {
                                        broadcasted_count += 1;
                                        last_broadcast_seq = event.partition_sequence;
                                    }
                                    Err(_) => {
                                        // Channel closed
                                        break 'outer;
                                    }
                                }
                            }

                            if p > broadcast_to {
                                break 'outer;
                            }
                        }
                    }
                }
            }

            // Only update next_to_broadcast if we successfully broadcast something
            // and we're advancing forward
            if broadcasted_count > 0 {
                let new_next_to_broadcast = last_broadcast_seq + 1;
                if new_next_to_broadcast > *next_to_broadcast {
                    *next_to_broadcast = new_next_to_broadcast;
                }
            }
        }

        Ok(results)
    }
}

/// Trigger broadcast of any confirmed but un-broadcast events
/// This should be called when a new subscription is created
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerBroadcast;

impl Message<TriggerBroadcast> for ConfirmationActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: TriggerBroadcast,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // For each partition, check if there are confirmed events that haven't been broadcast
        let watermarks: Vec<_> = self
            .manager
            .get_watermarks()
            .into_iter()
            .map(|(pid, wm)| (pid, wm.get()))
            .collect();

        for (partition_id, watermark) in watermarks {
            if watermark == 0 {
                continue;
            }

            let next_to_broadcast = self.next_broadcast_seq.entry(partition_id).or_insert(0);
            let highest_confirmed_seq = watermark - 1;

            if *next_to_broadcast <= highest_confirmed_seq {
                let broadcast_from = *next_to_broadcast;
                let broadcast_to = highest_confirmed_seq;

                let mut broadcasted_count = 0;
                let mut last_broadcast_seq = broadcast_from.saturating_sub(1);

                if let Ok(mut iter) = self
                    .database
                    .read_partition(partition_id, broadcast_from, IterDirection::Forward)
                    .await
                {
                    'outer: while let Ok(Some(commits)) = iter.next_batch(DEFAULT_BATCH_SIZE).await
                    {
                        for commit in commits {
                            for event in commit {
                                if event.partition_sequence >= broadcast_from
                                    && event.partition_sequence <= broadcast_to
                                {
                                    match self.broadcast_tx.send(event.clone()) {
                                        Ok(0) => break 'outer, // No subscribers
                                        Ok(_) => {
                                            broadcasted_count += 1;
                                            last_broadcast_seq = event.partition_sequence;
                                        }
                                        Err(_) => break 'outer, // Channel closed
                                    }
                                }

                                if event.partition_sequence > broadcast_to {
                                    break 'outer;
                                }
                            }
                        }
                    }
                }

                if broadcasted_count > 0 {
                    let new_next_to_broadcast = last_broadcast_seq + 1;
                    if new_next_to_broadcast > *next_to_broadcast {
                        *next_to_broadcast = new_next_to_broadcast;
                    }
                }
            }
        }
    }
}

/// Update confirmation count for an event
///
/// This message updates the confirmation count for a specific event version
/// in a partition. When enough confirmations are received (quorum), the
/// watermark may advance if there are no gaps.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateConfirmation {
    pub partition_id: PartitionId,
    pub versions: SmallVec<[u64; 4]>,
    pub confirmation_count: u8,
}

impl Message<UpdateConfirmation> for ConfirmationActor {
    type Reply = Result<SmallVec<[bool; 4]>, ConfirmationError>;

    async fn handle(
        &mut self,
        msg: UpdateConfirmation,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let mut results = SmallVec::new();
        let old_watermark = self
            .manager
            .get_watermark(msg.partition_id)
            .map(|w| w.get())
            .unwrap_or(0);

        for version in msg.versions {
            let advanced = self
                .manager
                .update_confirmation(msg.partition_id, version, msg.confirmation_count)
                .await?;
            results.push(advanced);
        }

        // If watermark advanced, broadcast newly confirmed events
        if results.iter().any(|&advanced| advanced) {
            let new_watermark = self
                .manager
                .get_watermark(msg.partition_id)
                .map(|w| w.get())
                .unwrap_or(0);

            self.broadcast_confirmed_events(msg.partition_id, old_watermark, new_watermark);
        }

        Ok(results)
    }
}
