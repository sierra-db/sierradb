use std::{collections::HashSet, time::Duration};

use kameo::prelude::*;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use sierradb::{
    StreamId,
    bucket::{PartitionHash, PartitionId, segment::EventRecord},
    id::uuid_to_partition_hash,
};
use tracing::{debug, instrument, warn};
use uuid::Uuid;

use crate::{ClusterActor, ClusterError, DEFAULT_BATCH_SIZE, ReplicaRefs};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadRequestMetadata {
    /// Nodes that have already tried to process this request
    pub tried_peers: HashSet<PeerId>,
    /// Original partition hash for the event
    pub partition_hash: PartitionHash,
    /// Number of nodes that have successfully returned "not found"
    pub not_found_count: u8,
}

#[derive(Debug)]
pub enum ReadDestination {
    Local {
        partition_id: PartitionId,
        available_replicas: ReplicaRefs,
    },
    Remote {
        cluster_ref: RemoteActorRef<ClusterActor>,
        available_replicas: ReplicaRefs,
    },
}

impl ClusterActor {
    /// Resolves where a read request should be directed
    fn resolve_read_destination(
        &self,
        metadata: &ReadRequestMetadata,
    ) -> Result<ReadDestination, ClusterError> {
        // Get all partitions that could contain this event (all replicas)
        let partition_id = metadata.partition_hash % self.topology_manager().num_partitions;
        let replicas = self.topology_manager().get_available_replicas(partition_id);

        if replicas.is_empty() {
            return Err(ClusterError::PartitionUnavailable);
        }

        // Check if we have any of the partitions locally
        // We can read from any replica, not just the leader
        for (cluster_ref, _) in &replicas {
            if self.topology_manager().has_partition(partition_id)
                && cluster_ref.id().peer_id().unwrap() == &self.local_peer_id
            {
                return Ok(ReadDestination::Local {
                    partition_id,
                    available_replicas: replicas,
                });
            }
        }

        // We don't have the partition locally, need to read from remote
        // Find the best available remote partition (prefer ones we haven't tried)
        for (cluster_ref, _) in &replicas {
            if !metadata
                .tried_peers
                .contains(cluster_ref.id().peer_id().unwrap())
            {
                return Ok(ReadDestination::Remote {
                    cluster_ref: cluster_ref.clone(),
                    available_replicas: replicas,
                });
            }
        }

        // All available partitions have been tried
        Err(ClusterError::NoAvailablePartitions)
    }

    fn route_read_request(
        &mut self,
        destination: ReadDestination,
        event_id: Uuid,
        metadata: ReadRequestMetadata,
        reply_sender: ReplySender<Result<Option<EventRecord>, ClusterError>>,
    ) {
        match destination {
            ReadDestination::Local {
                partition_id,
                available_replicas,
            } => {
                self.handle_local_read(
                    partition_id,
                    event_id,
                    metadata,
                    available_replicas,
                    reply_sender,
                );
            }
            ReadDestination::Remote {
                cluster_ref,
                available_replicas,
            } => {
                // Forward to remote node
                self.send_read_forward_request(
                    cluster_ref,
                    event_id,
                    metadata,
                    available_replicas,
                    reply_sender,
                );
            }
        }
    }

    fn handle_local_read(
        &mut self,
        partition_id: PartitionId,
        event_id: Uuid,
        mut metadata: ReadRequestMetadata,
        available_replicas: ReplicaRefs,
        reply_sender: ReplySender<Result<Option<EventRecord>, ClusterError>>,
    ) {
        let database = self.database.clone();
        let required_quorum = (self.replication_factor / 2) + 1;
        let watermark = self.watermarks.get(&partition_id).cloned();
        let local_peer_id = self.local_peer_id;

        // Mark this node as tried
        metadata.tried_peers.insert(local_peer_id);

        tokio::spawn(async move {
            // 1. Read the event from local storage
            let event = match database.read_event(partition_id, event_id).await {
                Ok(event) => event,
                Err(err) => {
                    reply_sender.send(Err(ClusterError::Read(err.to_string())));
                    return;
                }
            };

            let Some(event) = event else {
                debug!("event doesn't exist on this partition");
                // Local node didn't have the event - this counts as a "not found"
                metadata.not_found_count += 1;

                // Check if we've tried majority of replicas
                if metadata.not_found_count >= required_quorum {
                    debug!(
                        not_found_count = metadata.not_found_count,
                        required_quorum,
                        "majority of replicas returned not found - event doesn't exist"
                    );
                    reply_sender.send(Ok(None));
                } else {
                    // Try next replica
                    debug!(
                        not_found_count = metadata.not_found_count,
                        required_quorum, "local replica didn't have event, trying next replica"
                    );
                    Self::try_next_replica_for_not_found(
                        available_replicas,
                        event_id,
                        metadata,
                        required_quorum,
                        reply_sender,
                    )
                    .await;
                }
                return;
            };

            // 2. Check if event meets quorum requirements
            if event.confirmation_count < required_quorum {
                debug!("event exists but is not confirmed");
                // Event exists but not confirmed - this counts as a "not found"
                metadata.not_found_count += 1;

                // Check if we've tried majority of replicas
                if metadata.not_found_count >= required_quorum {
                    debug!(
                        not_found_count = metadata.not_found_count,
                        required_quorum,
                        "majority of replicas returned not found - event doesn't exist"
                    );
                    reply_sender.send(Ok(None));
                } else {
                    // Try next replica
                    debug!(
                        not_found_count = metadata.not_found_count,
                        required_quorum,
                        "local replica didn't have confirmed event, trying next replica"
                    );
                    Self::try_next_replica_for_not_found(
                        available_replicas,
                        event_id,
                        metadata,
                        required_quorum,
                        reply_sender,
                    )
                    .await;
                }
                return;
            };

            // 3. Check watermark - only return if within confirmed range
            let watermark = watermark.map(|w| w.get()).unwrap_or(0);
            if event.partition_sequence + 1 > watermark {
                debug!(
                    event_partition_sequence = event.partition_sequence,
                    watermark, "event exists but is beyond watermark"
                );
                // Event exists but beyond watermark - this counts as a "not found"
                metadata.not_found_count += 1;

                // Check if we've tried majority of replicas
                if metadata.not_found_count >= required_quorum {
                    debug!(
                        not_found_count = metadata.not_found_count,
                        required_quorum,
                        "majority of replicas returned not found - event doesn't exist"
                    );
                    reply_sender.send(Ok(None));
                } else {
                    // Try next replica
                    debug!(
                        not_found_count = metadata.not_found_count,
                        required_quorum,
                        "local replica has event beyond watermark, trying next replica"
                    );
                    Self::try_next_replica_for_not_found(
                        available_replicas,
                        event_id,
                        metadata,
                        required_quorum,
                        reply_sender,
                    )
                    .await;
                }
                return;
            }

            // Found a valid, confirmed event!
            reply_sender.send(Ok(Some(event)));
        });
    }

    fn send_read_forward_request(
        &mut self,
        cluster_ref: RemoteActorRef<ClusterActor>,
        event_id: Uuid,
        mut metadata: ReadRequestMetadata,
        available_replicas: ReplicaRefs,
        reply_sender: ReplySender<Result<Option<EventRecord>, ClusterError>>,
    ) {
        let required_quorum = (self.replication_factor / 2) + 1;

        // Increment hop count and add this peer to tried list
        metadata
            .tried_peers
            .insert(*cluster_ref.id().peer_id().unwrap());

        tokio::spawn(async move {
            let res = cluster_ref
                .ask(&ReadEvent {
                    event_id,
                    metadata: metadata.clone(),
                })
                .mailbox_timeout(Duration::from_secs(5))
                .reply_timeout(Duration::from_secs(5))
                .await;

            match res {
                Ok(Some(event)) => {
                    // Found the event! Return it immediately
                    reply_sender.send(Ok(Some(event)));
                }
                Ok(None) => {
                    // Node returned not found - increment counter and try next if needed
                    let mut updated_metadata = metadata;
                    updated_metadata.not_found_count += 1;

                    if updated_metadata.not_found_count >= required_quorum {
                        debug!(
                            not_found_count = updated_metadata.not_found_count,
                            required_quorum,
                            "majority of replicas returned not found - event doesn't exist"
                        );
                        reply_sender.send(Ok(None));
                    } else {
                        debug!(
                            not_found_count = updated_metadata.not_found_count,
                            required_quorum, "replica returned not found, trying next replica"
                        );
                        // Try next available replica
                        Self::try_next_replica_for_not_found(
                            available_replicas,
                            event_id,
                            updated_metadata,
                            required_quorum,
                            reply_sender,
                        )
                        .await;
                    }
                }
                Err(err) => {
                    // Network failure - try next replica without incrementing not_found_count
                    warn!("failed to contact replica: {err:?}");
                    Self::try_next_replica(
                        available_replicas,
                        event_id,
                        metadata,
                        required_quorum,
                        reply_sender,
                    )
                    .await;
                }
            }
        });
    }

    /// Try next replica when previous replica returned "not found"
    async fn try_next_replica_for_not_found(
        available_replicas: ReplicaRefs,
        event_id: Uuid,
        mut metadata: ReadRequestMetadata,
        required_quorum: u8,
        reply_sender: ReplySender<Result<Option<EventRecord>, ClusterError>>,
    ) {
        // Find the next untried partition
        for (cluster_ref, _) in available_replicas {
            if !metadata
                .tried_peers
                .contains(cluster_ref.id().peer_id().unwrap())
            {
                metadata
                    .tried_peers
                    .insert(*cluster_ref.id().peer_id().unwrap());

                let res = cluster_ref
                    .ask(&ReadEvent {
                        event_id,
                        metadata: metadata.clone(),
                    })
                    .mailbox_timeout(Duration::from_secs(5))
                    .reply_timeout(Duration::from_secs(5))
                    .await;

                match res {
                    Ok(Some(event)) => {
                        // Found it!
                        reply_sender.send(Ok(Some(event)));
                        return;
                    }
                    Ok(None) => {
                        // This replica also doesn't have it
                        metadata.not_found_count += 1;

                        if metadata.not_found_count >= required_quorum {
                            debug!(
                                not_found_count = metadata.not_found_count,
                                required_quorum,
                                "majority of replicas returned not found - event doesn't exist"
                            );
                            reply_sender.send(Ok(None));
                            return;
                        }
                        // Continue to next replica
                        continue;
                    }
                    Err(_) => {
                        // Network failure with this replica - continue to next without counting as
                        // not_found
                        continue;
                    }
                }
            }
        }

        // No more partitions to try, or we've reached majority threshold
        reply_sender.send(Ok(None));
    }

    /// Try next replica on network failure (doesn't count as "not found")
    async fn try_next_replica(
        available_replicas: ReplicaRefs,
        event_id: Uuid,
        mut metadata: ReadRequestMetadata,
        required_quorum: u8,
        reply_sender: ReplySender<Result<Option<EventRecord>, ClusterError>>,
    ) {
        // Find the next untried partition
        for (cluster_ref, _) in available_replicas.iter() {
            if !metadata
                .tried_peers
                .contains(cluster_ref.id().peer_id().unwrap())
            {
                metadata
                    .tried_peers
                    .insert(*cluster_ref.id().peer_id().unwrap());

                let res = cluster_ref
                    .ask(&ReadEvent {
                        event_id,
                        metadata: metadata.clone(),
                    })
                    .mailbox_timeout(Duration::from_secs(5))
                    .reply_timeout(Duration::from_secs(5))
                    .await;

                match res {
                    Ok(Some(event)) => {
                        reply_sender.send(Ok(Some(event)));
                        return;
                    }
                    Ok(None) => {
                        // This replica doesn't have it - continue the not_found logic
                        metadata.not_found_count += 1;

                        if metadata.not_found_count >= required_quorum {
                            reply_sender.send(Ok(None));
                            return;
                        }

                        // Continue to next replica
                        Self::try_next_replica_for_not_found(
                            available_replicas,
                            event_id,
                            metadata,
                            required_quorum,
                            reply_sender,
                        )
                        .await;
                        return;
                    }
                    Err(_) => {
                        // Network failure - continue to next replica
                        continue;
                    }
                }
            }
        }

        // No more partitions to try
        reply_sender.send(Err(ClusterError::NoAvailablePartitions));
    }

    fn handle_partition_read_locally(
        &mut self,
        partition_id: PartitionId,
        start_sequence: u64,
        end_sequence: Option<u64>,
        count: u64,
        reply_sender: ReplySender<Result<PartitionEvents, ClusterError>>,
    ) {
        let database = self.database.clone();
        let watermark = self
            .watermarks
            .get(&partition_id)
            .map(|w| w.get())
            .unwrap_or(0);

        // Adjust end_sequence to respect watermark
        let effective_end_sequence = match end_sequence {
            Some(end) => end.min(watermark),
            None => watermark,
        };

        debug!(
            partition_id,
            start_sequence,
            ?end_sequence,
            ?effective_end_sequence,
            watermark,
            count,
            "reading partition locally"
        );

        // If start_sequence is beyond watermark, no events to return
        if start_sequence > watermark {
            reply_sender.send(Ok(PartitionEvents {
                events: Vec::new(),
                has_more: false,
            }));
            return;
        }

        tokio::spawn(async move {
            // Create iterator and collect events
            let mut iter = match database.read_partition(partition_id, start_sequence).await {
                Ok(iter) => iter,
                Err(err) => {
                    reply_sender.send(Err(ClusterError::Read(err.to_string())));
                    return;
                }
            };

            let mut events = Vec::new();
            let mut events_collected = 0;
            let mut last_read_sequence = 0;

            'iter: while let Some(commits) = match iter
                .next_batch(
                    (effective_end_sequence.saturating_sub(last_read_sequence) as usize)
                        .min(DEFAULT_BATCH_SIZE),
                )
                .await
            {
                Ok(commits) => commits,
                Err(err) => {
                    reply_sender.send(Err(ClusterError::Read(err.to_string())));
                    return;
                }
            } {
                for commit in commits {
                    for event in commit {
                        // Check if we've reached the count limit
                        if events_collected >= count {
                            break 'iter;
                        }

                        // Check if event is beyond effective end sequence
                        if event.partition_sequence > effective_end_sequence {
                            break 'iter;
                        }

                        last_read_sequence = event.partition_sequence + 1;
                        events.push(event);
                        events_collected += 1;
                    }
                }

                // Break if we hit limits
                if events_collected >= count {
                    break;
                }

                if events.last().map(|e| e.partition_sequence).unwrap_or(0)
                    >= effective_end_sequence
                {
                    break;
                }
            }

            let has_more = watermark
                .checked_sub(1)
                .map(|latest_sequence| last_read_sequence <= latest_sequence)
                .unwrap_or(false);

            debug!(
                events_returned = events.len(),
                has_more, "completed local partition read"
            );

            reply_sender.send(Ok(PartitionEvents { events, has_more }));
        });
    }

    fn forward_partition_read(
        &mut self,
        partition_owner: RemoteActorRef<ClusterActor>,
        msg: ReadPartition,
        reply_sender: ReplySender<Result<PartitionEvents, ClusterError>>,
    ) {
        tokio::spawn(async move {
            debug!(
                partition_id = msg.partition_id,
                target_peer = ?partition_owner.id().peer_id(),
                "forwarding partition read to owner"
            );

            let result = partition_owner
                .ask(&msg)
                .mailbox_timeout(Duration::from_secs(10))
                .reply_timeout(Duration::from_secs(10))
                .await;

            match result {
                Ok(partition_events) => {
                    reply_sender.send(Ok(partition_events));
                }
                Err(err) => {
                    warn!("failed to forward partition read: {err:?}");
                    reply_sender.send(Err(ClusterError::from(err)));
                }
            }
        });
    }

    fn handle_stream_read_locally(
        &mut self,
        partition_id: PartitionId,
        stream_id: StreamId,
        start_version: u64,
        end_version: Option<u64>,
        count: u64,
        reply_sender: ReplySender<Result<StreamEvents, ClusterError>>,
    ) {
        let database = self.database.clone();
        let watermark = self
            .watermarks
            .get(&partition_id)
            .map(|w| w.get())
            .unwrap_or(0);

        tokio::spawn(async move {
            debug!(
                partition_id,
                %stream_id,
                start_version,
                ?end_version,
                watermark,
                count,
                "reading stream locally"
            );

            // Create iterator and collect events
            let mut iter = match database
                .read_stream(partition_id, stream_id, start_version, false)
                .await
            {
                Ok(iter) => iter,
                Err(err) => {
                    reply_sender.send(Err(ClusterError::Read(err.to_string())));
                    return;
                }
            };

            let mut events = Vec::new();
            let mut has_more = false;
            let mut events_collected = 0;
            let mut last_read_version = 0;

            'iter: while let Some(commits) = match iter
                .next_batch({
                    end_version
                        .map(|end| {
                            (end.saturating_sub(last_read_version) as usize)
                                .clamp(1, DEFAULT_BATCH_SIZE)
                        })
                        .unwrap_or(DEFAULT_BATCH_SIZE)
                })
                .await
            {
                Ok(commits) => commits,
                Err(err) => {
                    reply_sender.send(Err(ClusterError::Read(err.to_string())));
                    return;
                }
            } {
                for commit in commits {
                    for event in commit {
                        // Check if we've reached the count limit
                        if events_collected >= count {
                            has_more = true;
                            break 'iter;
                        }

                        // Check if event is beyond watermark (safety check - uses
                        // partition_sequence)
                        if event.partition_sequence > watermark {
                            break 'iter;
                        }

                        // Check if event is beyond end_version (uses stream_version)
                        if let Some(end_ver) = end_version
                            && event.stream_version > end_ver
                        {
                            has_more = true;
                            break;
                        }

                        last_read_version = event.stream_version;
                        events.push(event);
                        events_collected += 1;
                    }

                    // Break if we hit limits
                    if events_collected >= count {
                        has_more = true;
                        break 'iter;
                    }

                    if let Some(end_ver) = end_version
                        && events.last().map(|e| e.stream_version).unwrap_or(0) >= end_ver
                    {
                        break 'iter;
                    }
                }
            }

            // Note: We can't easily determine if there are more events beyond the watermark
            // for stream reads since watermark is based on partition_sequence, not
            // stream_version. We rely on the iterator ending naturally or
            // hitting our limits.

            debug!(
                events_returned = events.len(),
                has_more, "completed local stream read"
            );

            reply_sender.send(Ok(StreamEvents { events, has_more }));
        });
    }

    fn forward_stream_read(
        &mut self,
        partition_owner: RemoteActorRef<ClusterActor>,
        msg: ReadStream,
        reply_sender: ReplySender<Result<StreamEvents, ClusterError>>,
    ) {
        tokio::spawn(async move {
            debug!(
                partition_id = msg.partition_id,
                stream_id = %msg.stream_id,
                target_peer = ?partition_owner.id().peer_id(),
                "forwarding stream read to owner"
            );

            let result = partition_owner
                .ask(&msg)
                .mailbox_timeout(Duration::from_secs(10))
                .reply_timeout(Duration::from_secs(10))
                .await;

            match result {
                Ok(stream_events) => {
                    reply_sender.send(Ok(stream_events));
                }
                Err(err) => {
                    warn!("failed to forward stream read: {err:?}");
                    reply_sender.send(Err(ClusterError::from(err)));
                }
            }
        });
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadEvent {
    pub event_id: Uuid,
    pub metadata: ReadRequestMetadata,
}

impl ReadEvent {
    pub fn new(event_id: Uuid) -> Self {
        let partition_hash = uuid_to_partition_hash(event_id);

        let metadata = ReadRequestMetadata {
            tried_peers: HashSet::new(),
            partition_hash,
            not_found_count: 0,
        };

        ReadEvent { event_id, metadata }
    }
}

#[remote_message]
impl Message<ReadEvent> for ClusterActor {
    type Reply = DelegatedReply<Result<Option<EventRecord>, ClusterError>>;

    #[instrument(skip_all, fields(event_id = %msg.event_id))]
    async fn handle(
        &mut self,
        msg: ReadEvent,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let (delegated_reply, reply_sender) = ctx.reply_sender();

        let Some(reply_sender) = reply_sender else {
            warn!("ignoring read event with no reply");
            return delegated_reply;
        };

        match self.resolve_read_destination(&msg.metadata) {
            Ok(dest) => {
                debug!(
                    destination = match &dest {
                        ReadDestination::Local { .. } => "local",
                        ReadDestination::Remote { .. } => "remote",
                    },
                    "routed read request"
                );
                self.route_read_request(dest, msg.event_id, msg.metadata, reply_sender);
            }
            Err(err) => {
                reply_sender.send(Err(err));
            }
        }

        delegated_reply
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadPartition {
    pub partition_id: PartitionId,
    pub start_sequence: u64,
    pub end_sequence: Option<u64>,
    pub count: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionEvents {
    pub events: Vec<EventRecord>,
    pub has_more: bool, // whether there are more events beyond what we returned
}

#[remote_message]
impl Message<ReadPartition> for ClusterActor {
    type Reply = DelegatedReply<Result<PartitionEvents, ClusterError>>;

    #[instrument(skip_all, fields(
        partition_id = msg.partition_id,
        start_sequence = msg.start_sequence,
        end_sequence = msg.end_sequence,
        count = msg.count,
    ))]
    async fn handle(
        &mut self,
        msg: ReadPartition,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let (delegated_reply, reply_sender) = ctx.reply_sender();

        let Some(reply_sender) = reply_sender else {
            warn!("ignoring partition read with no reply");
            return delegated_reply;
        };

        // Check if we own this partition
        if self.topology_manager().has_partition(msg.partition_id) {
            debug!(
                partition_id = msg.partition_id,
                "handling partition read locally"
            );

            self.handle_partition_read_locally(
                msg.partition_id,
                msg.start_sequence,
                msg.end_sequence,
                msg.count,
                reply_sender,
            );
        } else {
            // We don't own this partition - forward to the partition owner
            let available_replicas = self
                .topology_manager()
                .get_available_replicas(msg.partition_id);

            let Some((partition_owner, _)) = available_replicas.into_iter().next() else {
                reply_sender.send(Err(ClusterError::PartitionUnavailable));
                return delegated_reply;
            };

            self.forward_partition_read(partition_owner, msg, reply_sender);
        }

        delegated_reply
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadStream {
    pub partition_id: PartitionId,
    pub stream_id: StreamId,
    pub start_version: u64,
    pub end_version: Option<u64>,
    pub count: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StreamEvents {
    pub events: Vec<EventRecord>,
    pub has_more: bool, // whether there are more events beyond what we returned
}

#[remote_message]
impl Message<ReadStream> for ClusterActor {
    type Reply = DelegatedReply<Result<StreamEvents, ClusterError>>;

    #[instrument(skip_all, fields(
        partition_id = msg.partition_id,
        stream_id = %msg.stream_id,
        start_version = msg.start_version,
        end_version = msg.end_version,
        count = msg.count,
    ))]
    async fn handle(
        &mut self,
        msg: ReadStream,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let (delegated_reply, reply_sender) = ctx.reply_sender();

        let Some(reply_sender) = reply_sender else {
            warn!("ignoring stream read with no reply");
            return delegated_reply;
        };

        // Check if we own this partition
        if self.topology_manager().has_partition(msg.partition_id) {
            debug!(
                partition_id = msg.partition_id,
                stream_id = %msg.stream_id,
                "handling stream read locally"
            );

            self.handle_stream_read_locally(
                msg.partition_id,
                msg.stream_id,
                msg.start_version,
                msg.end_version,
                msg.count,
                reply_sender,
            );
        } else {
            // We don't own this partition - forward to the partition owner
            let available_replicas = self
                .topology_manager()
                .get_available_replicas(msg.partition_id);

            let Some((partition_owner, _)) = available_replicas.into_iter().next() else {
                reply_sender.send(Err(ClusterError::PartitionUnavailable));
                return delegated_reply;
            };

            self.forward_stream_read(partition_owner, msg, reply_sender);
        }

        delegated_reply
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetPartitionSequence {
    pub partition_id: PartitionId,
}

#[remote_message]
impl Message<GetPartitionSequence> for ClusterActor {
    type Reply = DelegatedReply<Result<Option<u64>, ClusterError>>;

    #[instrument(skip_all, fields(partition_id = msg.partition_id))]
    async fn handle(
        &mut self,
        msg: GetPartitionSequence,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if let Some(watermark) = self.watermarks.get(&msg.partition_id) {
            return ctx.reply(Ok(watermark.get().checked_sub(1)));
        }

        let (delegated_reply, reply_sender) = ctx.reply_sender();
        let Some(reply_sender) = reply_sender else {
            warn!("ignoring get partition sequence with no reply");
            return delegated_reply;
        };

        // Check if we own this partition
        let available_replicas = self
            .topology_manager()
            .get_available_replicas(msg.partition_id);

        let Some((partition_owner, _)) = available_replicas
            .into_iter()
            .find(|(replica, _)| replica.id().peer_id().unwrap() != &self.local_peer_id)
        else {
            reply_sender.send(Err(ClusterError::PartitionUnavailable));
            return delegated_reply;
        };

        tokio::spawn(async move {
            debug!(
                partition_id = msg.partition_id,
                target_peer = ?partition_owner.id().peer_id(),
                "forwarding stream read to owner"
            );

            let result = partition_owner
                .ask(&msg)
                .mailbox_timeout(Duration::from_secs(10))
                .reply_timeout(Duration::from_secs(10))
                .await;

            match result {
                Ok(seq) => {
                    reply_sender.send(Ok(seq));
                }
                Err(err) => {
                    warn!("failed to forward stream read: {err:?}");
                    reply_sender.send(Err(ClusterError::from(err)));
                }
            }
        });

        delegated_reply
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetStreamVersion {
    pub partition_id: PartitionId,
    pub stream_id: StreamId,
}

#[remote_message]
impl Message<GetStreamVersion> for ClusterActor {
    type Reply = DelegatedReply<Result<Option<u64>, ClusterError>>;

    #[instrument(skip_all, fields(
        partition_id = msg.partition_id,
        stream_id = %msg.stream_id,
    ))]
    async fn handle(
        &mut self,
        msg: GetStreamVersion,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Check if we own this partition
        match self.watermarks.get(&msg.partition_id) {
            Some(watermark) => {
                debug!(
                    partition_id = msg.partition_id,
                    stream_id = %msg.stream_id,
                    "handling get stream version locally"
                );
                let database = self.database.clone();
                let watermark = watermark.get();

                ctx.spawn(async move {
                    let mut iter = database
                        .read_stream(msg.partition_id, msg.stream_id, u64::MAX, true)
                        .await
                        .map_err(|err| ClusterError::Read(err.to_string()))?;

                    while let Some(commits) = iter
                        .next_batch(DEFAULT_BATCH_SIZE)
                        .await
                        .map_err(|err| ClusterError::Read(err.to_string()))?
                    {
                        if let Some(stream_version) = commits
                            .into_iter()
                            .flat_map(|commit| commit.into_iter())
                            .find_map(|event| {
                                (event.partition_sequence < watermark)
                                    .then_some(event.stream_version)
                            })
                        {
                            return Ok(Some(stream_version));
                        }
                    }

                    Ok(None)
                })
            }
            _ => {
                // We don't own this partition - forward to the partition owner
                let available_replicas = self
                    .topology_manager()
                    .get_available_replicas(msg.partition_id);

                let Some((partition_owner, _)) = available_replicas.into_iter().next() else {
                    return ctx.reply(Err(ClusterError::PartitionUnavailable));
                };

                ctx.spawn(async move {
                    debug!(
                        partition_id = msg.partition_id,
                        stream_id = %msg.stream_id,
                        target_peer = ?partition_owner.id().peer_id(),
                        "forwarding stream read to owner"
                    );

                    let version = partition_owner
                        .ask(&msg)
                        .mailbox_timeout(Duration::from_secs(10))
                        .reply_timeout(Duration::from_secs(10))
                        .await?;

                    Ok(version)
                })
            }
        }
    }
}
