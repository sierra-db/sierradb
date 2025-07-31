use std::{collections::HashSet, panic::panic_any, time::Duration};

use arrayvec::ArrayVec;
use kameo::prelude::*;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use sierradb::{
    MAX_REPLICATION_FACTOR, StreamId,
    bucket::{PartitionHash, PartitionId, segment::EventRecord},
    id::uuid_to_partition_hash,
};
use tracing::{debug, instrument};
use uuid::Uuid;

use crate::{ClusterActor, ClusterError, MAX_FORWARDS};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadRequestMetadata {
    /// Number of hops this request has taken
    pub hop_count: u8,
    /// Nodes that have already tried to process this request
    pub tried_peers: HashSet<PeerId>,
    /// Original partition hash for the event
    pub partition_hash: PartitionHash,
}

#[derive(Debug)]
pub enum ReadDestination {
    Local {
        partition_id: PartitionId,
    },
    Remote {
        cluster_ref: RemoteActorRef<ClusterActor>,
        available_replicas:
            Box<ArrayVec<(RemoteActorRef<ClusterActor>, u64), MAX_REPLICATION_FACTOR>>,
    },
}

impl ClusterActor {
    /// Resolves where a read request should be directed
    fn resolve_read_destination(
        &self,
        metadata: &ReadRequestMetadata,
    ) -> Result<ReadDestination, ClusterError> {
        // Check for too many forwards
        if metadata.hop_count > MAX_FORWARDS {
            return Err(ClusterError::TooManyForwards);
        }

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
                return Ok(ReadDestination::Local { partition_id });
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
                    available_replicas: Box::new(replicas),
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
        reply_sender: Option<ReplySender<Result<Option<EventRecord>, ClusterError>>>,
    ) {
        match destination {
            ReadDestination::Local { partition_id } => {
                // Handle locally
                self.handle_local_read(partition_id, event_id, reply_sender);
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
                    *available_replicas,
                    reply_sender,
                );
            }
        }
    }

    #[instrument(skip(self, reply_sender))]
    fn handle_local_read(
        &mut self,
        partition_id: PartitionId,
        event_id: Uuid,
        reply_sender: Option<ReplySender<Result<Option<EventRecord>, ClusterError>>>,
    ) {
        let database = self.database.clone();
        let required_quorum = (self.replication_factor as usize / 2) + 1;
        let watermark = self.watermarks.get(&partition_id).cloned();

        let task = async move {
            // 1. Read the event from local storage
            let event = database
                .read_event(partition_id, event_id, false)
                .await
                .map_err(|err| ClusterError::Read(err.to_string()))?;

            let Some(event) = event else {
                debug!("event doesn't exist on this partition");
                return Ok(None); // Event doesn't exist on this partition
            };

            // 2. Check if event meets quorum requirements
            if event.confirmation_count < required_quorum as u8 {
                debug!("event exists but is not confirmed");
                return Ok(None); // Event exists but not confirmed
            };

            // 3. Check watermark - only return if within confirmed range
            let watermark = watermark.map(|w| w.get()).unwrap_or(0);
            if event.partition_sequence + 1 > watermark {
                debug!(
                    event_partition_sequence = event.partition_sequence,
                    watermark, "event exists but is beyond watermark"
                );
                return Ok(None); // Event exists but beyond watermark
            }

            Ok(Some(event))
        };

        if let Some(tx) = reply_sender {
            tokio::spawn(async move {
                let result = task.await;
                tx.send(result);
            });
        } else {
            tokio::spawn(task);
        }
    }

    #[instrument(skip(self, reply_sender))]
    fn send_read_forward_request(
        &mut self,
        cluster_ref: RemoteActorRef<ClusterActor>,
        event_id: Uuid,
        mut metadata: ReadRequestMetadata,
        available_replicas: ArrayVec<(RemoteActorRef<ClusterActor>, u64), MAX_REPLICATION_FACTOR>,
        reply_sender: Option<ReplySender<Result<Option<EventRecord>, ClusterError>>>,
    ) {
        // Increment hop count and add this peer to tried list
        metadata.hop_count += 1;
        metadata
            .tried_peers
            .insert(*cluster_ref.id().peer_id().unwrap());

        match reply_sender {
            Some(tx) => {
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
                        Ok(result) => tx.send(Ok(result)),
                        Err(_) => {
                            // If this peer failed, try the next available partition
                            // This provides fault tolerance when some replicas are down
                            Self::try_next_replica(available_replicas, event_id, metadata, tx)
                                .await;
                        }
                    }
                });
            }
            None => {
                cluster_ref
                    .tell(&ReadEvent { event_id, metadata })
                    .send()
                    .expect("read event cannot fail serialization");
            }
        }
    }

    async fn try_next_replica(
        available_replicas: ArrayVec<(RemoteActorRef<ClusterActor>, u64), MAX_REPLICATION_FACTOR>,
        event_id: Uuid,
        mut metadata: ReadRequestMetadata,
        reply_sender: ReplySender<Result<Option<EventRecord>, ClusterError>>,
    ) {
        // Find the next untried partition
        for (cluster_ref, _) in available_replicas.iter() {
            if !metadata
                .tried_peers
                .contains(cluster_ref.id().peer_id().unwrap())
            {
                metadata.hop_count += 1;
                metadata
                    .tried_peers
                    .insert(*cluster_ref.id().peer_id().unwrap());

                let res = cluster_ref
                    .ask(&ReadEvent { event_id, metadata })
                    .mailbox_timeout(Duration::from_secs(5))
                    .reply_timeout(Duration::from_secs(5))
                    .await;

                reply_sender.send(res.map_err(ClusterError::from));
                return;
            }
        }

        // No more partitions to try
        reply_sender.send(Err(ClusterError::NoAvailablePartitions));
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
            hop_count: 0,
            tried_peers: HashSet::new(),
            partition_hash,
        };

        ReadEvent { event_id, metadata }
    }
}

#[remote_message("85746195-02a-44a1-b88c-6485a03723c")]
impl Message<ReadEvent> for ClusterActor {
    type Reply = DelegatedReply<Result<Option<EventRecord>, ClusterError>>;

    #[instrument(skip(self, ctx))]
    async fn handle(
        &mut self,
        msg: ReadEvent,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let (delegated_reply, reply_sender) = ctx.reply_sender();

        match self.resolve_read_destination(&msg.metadata) {
            Ok(dest) => {
                debug!(?dest, "routed read request");
                self.route_read_request(dest, msg.event_id, msg.metadata, reply_sender);
            }
            Err(err) => match reply_sender {
                Some(tx) => {
                    tx.send(Err(err));
                }
                None => {
                    panic_any(err);
                }
            },
        }

        delegated_reply
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadPartition {
    pub partition_id: PartitionId,
    pub start_sequence: u64,
    pub end_sequence: u64,
    pub limit: Option<u32>, // Optional limit for pagination
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionEventsResponse {
    pub events: Vec<EventRecord>,
    pub next_sequence: Option<u64>, // For pagination
    pub watermark: u64,             // Current watermark for this partition
}

impl Message<ReadPartition> for ClusterActor {
    type Reply = DelegatedReply<Result<PartitionEventsResponse, ClusterError>>;

    #[instrument(skip(self, ctx))]
    async fn handle(
        &mut self,
        msg: ReadPartition,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let database = self.database.clone();
        let required_quorum = ((self.replication_factor as usize / 2) + 1) as u8;
        let watermark = self
            .watermarks
            .get(&msg.partition_id)
            .map(|w| w.get())
            .unwrap_or(0);

        // Don't read beyond watermark
        if msg.start_sequence + 1 > watermark {
            return ctx.reply(Ok(PartitionEventsResponse {
                events: vec![],
                next_sequence: None,
                watermark,
            }));
        }

        ctx.spawn(async move {
            // Create iterator and collect events up to watermark
            let mut iter = database
                .read_partition(msg.partition_id, msg.start_sequence)
                .await
                .map_err(|err| ClusterError::Read(err.to_string()))?;

            let mut events = Vec::new();
            let limit = msg.limit.unwrap_or(100) as usize; // Default limit
            let mut next_sequence = None;

            'outer: while events.len() < limit {
                match iter
                    .next(false)
                    .await
                    .map_err(|err| ClusterError::Read(err.to_string()))?
                {
                    Some(commit) => {
                        for event in commit {
                            // Only include events within watermark
                            if event.partition_sequence <= watermark {
                                assert!(
                                    event.confirmation_count >= required_quorum,
                                    "watermark should only be here if the event has been confirmed"
                                );

                                events.push(event);
                            } else {
                                // Hit watermark boundary
                                break 'outer;
                            }
                        }
                    }
                    None => break, // No more events
                }
            }

            // Set next_sequence if we hit the limit
            if events.len() == limit {
                next_sequence = events.last().map(|e| e.partition_sequence + 1);
            }

            Ok(PartitionEventsResponse {
                events,
                next_sequence,
                watermark,
            })
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadStream {
    pub partition_id: PartitionId,
    pub stream_id: StreamId,
    pub from_version: Option<u64>, // Optional starting version
    pub limit: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StreamEventsResponse {
    pub events: Vec<EventRecord>,
    pub next_version: Option<u64>,
    pub watermark: u64,
}

impl Message<ReadStream> for ClusterActor {
    type Reply = DelegatedReply<Result<StreamEventsResponse, ClusterError>>;

    #[instrument(skip(self, ctx))]
    async fn handle(
        &mut self,
        msg: ReadStream,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let database = self.database.clone();
        let required_quorum = ((self.replication_factor as usize / 2) + 1) as u8;
        let watermark = self
            .watermarks
            .get(&msg.partition_id)
            .map(|w| w.get())
            .unwrap_or(0);

        ctx.spawn(async move {
            // Similar pattern to partition reads
            let mut iter = database
                .read_stream(msg.partition_id, msg.stream_id)
                .await
                .map_err(|err| ClusterError::Read(err.to_string()))?;

            let mut events = Vec::new();
            let limit = msg.limit.unwrap_or(100) as usize;
            let from_version = msg.from_version.unwrap_or(0);

            while events.len() < limit {
                match iter
                    .next(false)
                    .await
                    .map_err(|err| ClusterError::Read(err.to_string()))?
                {
                    Some(commit) => {
                        for event in commit {
                            // Filter by version and watermark
                            if event.stream_version >= from_version
                                && event.partition_sequence <= watermark
                            {
                                assert!(
                                    event.confirmation_count >= required_quorum,
                                    "watermark should only be here if the event has been confirmed"
                                );

                                events.push(event);
                            }
                        }
                    }
                    None => break,
                }
            }

            let next_version = events.last().map(|e| e.stream_version + 1);

            Ok(StreamEventsResponse {
                events,
                next_version,
                watermark,
            })
        })
    }
}
