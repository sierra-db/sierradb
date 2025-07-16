use std::{collections::HashSet, iter, panic::panic_any, time::Duration};

use arrayvec::ArrayVec;
use kameo::prelude::*;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use sierradb::{
    MAX_REPLICATION_FACTOR,
    bucket::{PartitionHash, PartitionId, segment::CommittedEvents},
    database::Transaction,
    id::uuid_to_partition_hash,
    writer_thread_pool::AppendResult,
};
use smallvec::{SmallVec, smallvec};
use thiserror::Error;
use tracing::{error, instrument};
use uuid::Uuid;

use crate::{
    ClusterActor, ClusterError, MAX_FORWARDS,
    confirmation::actor::UpdateConfirmation,
    transaction::{self, set_confirmations_with_retry},
};

impl ClusterActor {
    /// Resolves where a write request should be directed
    fn resolve_write_destination(
        &self,
        metadata: &WriteRequestMetadata,
    ) -> Result<WriteDestination, ClusterError> {
        // Check for too many forwards
        if metadata.hop_count > MAX_FORWARDS {
            return Err(ClusterError::TooManyForwards);
        }

        // Get partition distribution
        let partitions = self
            .swarm
            .behaviour()
            .partitions
            .manager
            .get_available_partitions_for_key(metadata.partition_hash);

        // Check quorum requirements
        let required_quorum =
            (self.swarm.behaviour().partitions.manager.replication_factor as usize / 2) + 1;
        if partitions.len() < required_quorum {
            return Err(ClusterError::InsufficientPartitionsForQuorum {
                alive: partitions.len() as u8,
                required: required_quorum as u8,
            });
        }

        // Get leader partition info
        let (primary_partition_id, primary_cluster_ref) = partitions
            .first()
            .cloned()
            .ok_or(ClusterError::PartitionUnavailable)?;

        // Check if we are the leader
        if self
            .swarm
            .behaviour()
            .partitions
            .manager
            .has_partition(primary_partition_id)
            && primary_cluster_ref.id().peer_id().unwrap() == &self.local_peer_id
        {
            // Extract replica partitions from the partitions list
            // Skip the first partition (primary) and only include partitions we're not the
            // leader for
            let replica_partitions = partitions
                .iter()
                .skip(1) // Skip the primary partition (first in the list)
                .cloned()
                .collect::<Vec<_>>();

            Ok(WriteDestination::Local {
                primary_partition_id,
                replica_partitions,
            })
        } else {
            Ok(WriteDestination::Remote {
                primary_cluster_ref,
                partitions: Box::new(partitions),
            })
        }
    }

    /// Routes a write request to the appropriate destination
    fn route_write_request(
        &mut self,
        destination: WriteDestination,
        transaction: Transaction,
        metadata: WriteRequestMetadata,
        reply_sender: Option<ReplySender<Result<AppendResult, ClusterError>>>,
    ) {
        // Check circuit breaker before proceeding
        if !self.circuit_breaker.should_allow_request() {
            if let Some(tx) = reply_sender {
                tx.send(Err(ClusterError::CircuitBreakerOpen {
                    estimated_recovery_time: self.circuit_breaker.estimated_recovery_time(),
                }));
            }
            return;
        }

        match destination {
            WriteDestination::Local {
                primary_partition_id,
                replica_partitions,
            } => {
                transaction::spawn(
                    self.database.clone(),
                    self.confirmation_ref.clone(),
                    primary_partition_id,
                    replica_partitions,
                    self.swarm.behaviour().partitions.manager.replication_factor,
                    transaction,
                    self.circuit_breaker.clone(),
                    reply_sender,
                );
            }
            WriteDestination::Remote {
                primary_cluster_ref,
                partitions,
            } => {
                // We need to forward to the leader or an alternative peer
                // Check if we've already tried this peer (to prevent loops)
                if metadata
                    .tried_peers
                    .contains(primary_cluster_ref.id().peer_id().unwrap())
                {
                    // Try the next best partition/peer if available
                    for (_partition_id, cluster_ref) in partitions.iter().skip(1) {
                        if !metadata
                            .tried_peers
                            .contains(cluster_ref.id().peer_id().unwrap())
                        {
                            // Found an untried peer, forward to them
                            self.send_forward_request(
                                cluster_ref.clone(),
                                transaction,
                                metadata,
                                reply_sender,
                            );
                            return;
                        }
                    }

                    // No untried peers left
                    if let Some(tx) = reply_sender {
                        tx.send(Err(ClusterError::NoAvailableLeaders));
                    }

                    return;
                }

                // Forward to the leader peer
                self.send_forward_request(primary_cluster_ref, transaction, metadata, reply_sender);
            }
        }
    }

    /// Sends a forward request to a specific peer
    fn send_forward_request(
        &mut self,
        cluster_ref: RemoteActorRef<ClusterActor>,
        transaction: Transaction,
        mut metadata: WriteRequestMetadata,
        reply_sender: Option<ReplySender<Result<AppendResult, ClusterError>>>,
    ) {
        // Add this peer to the tried list
        metadata
            .tried_peers
            .insert(*cluster_ref.id().peer_id().unwrap());

        match reply_sender {
            Some(tx) => {
                tokio::spawn(async move {
                    let res = cluster_ref
                        .ask(&ExecuteTransaction {
                            transaction,
                            metadata,
                        })
                        .mailbox_timeout(Duration::from_secs(10))
                        .reply_timeout(Duration::from_secs(10))
                        .await;

                    tx.send(res.map_err(ClusterError::from));
                });
            }
            None => {
                cluster_ref
                    .tell(&ExecuteTransaction {
                        transaction,
                        metadata,
                    })
                    .send()
                    .expect("execute transaction cannot fail serialization");
            }
        }
    }
}

/// Represents a destination for a write request
enum WriteDestination {
    /// Process locally as the leader
    Local {
        primary_partition_id: PartitionId,
        replica_partitions: Vec<(PartitionId, RemoteActorRef<ClusterActor>)>,
    },
    /// Forward to a remote leader
    Remote {
        primary_cluster_ref: RemoteActorRef<ClusterActor>,
        partitions:
            Box<ArrayVec<(PartitionId, RemoteActorRef<ClusterActor>), MAX_REPLICATION_FACTOR>>,
    },
}

/// Message to replicate a write to another partition
#[derive(Debug, Serialize, Deserialize)]
pub struct ExecuteTransaction {
    pub transaction: Transaction,
    pub metadata: WriteRequestMetadata,
}

impl ExecuteTransaction {
    pub fn new(transaction: Transaction) -> Self {
        let partition_hash = uuid_to_partition_hash(transaction.partition_key());

        let metadata = WriteRequestMetadata {
            hop_count: 0,
            tried_peers: HashSet::new(),
            partition_hash,
        };

        ExecuteTransaction {
            transaction,
            metadata,
        }
    }
}

#[remote_message("e0e82b26-9528-4afb-a819-c5914c08b218")]
impl Message<ExecuteTransaction> for ClusterActor {
    type Reply = DelegatedReply<Result<AppendResult, ClusterError>>;

    #[instrument(skip(self, ctx))]
    async fn handle(
        &mut self,
        msg: ExecuteTransaction,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let (delegated_reply, reply_sender) = ctx.reply_sender();

        match self.resolve_write_destination(&msg.metadata) {
            Ok(destination) => {
                self.route_write_request(destination, msg.transaction, msg.metadata, reply_sender)
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

/// Message to replicate a write to another partition
#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicateWrite {
    pub partition_id: PartitionId,
    pub transaction: Transaction,
    pub transaction_id: Uuid,
    pub origin_partition: PartitionId,
}

#[remote_message("ae8dc4cc-e382-4a68-9451-d10c5347d3c9")]
impl Message<ReplicateWrite> for ClusterActor {
    type Reply = DelegatedReply<Result<AppendResult, ClusterError>>;

    #[instrument(skip(self, ctx))]
    async fn handle(
        &mut self,
        msg: ReplicateWrite,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if !self
            .swarm
            .behaviour()
            .partitions
            .manager
            .has_partition(msg.partition_id)
        {
            panic!("got a replicate write request when we dont own the partition");
        }

        let database = self.database.clone();

        // Replicate the write to our local database
        ctx.spawn(async move {
            database
                .append_events(msg.partition_id, msg.transaction)
                .await
                .map_err(ClusterError::from)
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfirmTransaction {
    pub partition_id: PartitionId,
    pub transaction_id: Uuid,
    pub event_ids: SmallVec<[Uuid; 4]>,
    pub event_versions: SmallVec<[u64; 4]>,
    pub confirmation_count: u8,
}

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum ConfirmTransactionError {
    #[error("events length mismatch")]
    EventsLengthMismatch,
    #[error("event id mismatch")]
    EventIdMismatch,
    #[error("transaction not found")]
    TransactionNotFound,
    #[error("read error: {0}")]
    Read(String),
    #[error("write error: {0}")]
    Write(String),
}

#[remote_message("93087af8-6b1f-4df0-8225-13fe694203c1")]
impl Message<ConfirmTransaction> for ClusterActor {
    type Reply = DelegatedReply<Result<(), ConfirmTransactionError>>;

    #[instrument(skip(self, ctx))]
    async fn handle(
        &mut self,
        msg: ConfirmTransaction,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let database = self.database.clone();
        let confirmation_ref = self.confirmation_ref.clone();

        ctx.spawn(async move {
            let Some(first_event_id) = msg.event_ids.first() else {
                return Err(ConfirmTransactionError::EventsLengthMismatch);
            };

            let offsets = match database
                .read_transaction(msg.partition_id, *first_event_id, true)
                .await
                .map_err(|err| ConfirmTransactionError::Read(err.to_string()))?
            {
                Some(CommittedEvents::Single(event)) => {
                    smallvec![event.offset]
                }
                Some(CommittedEvents::Transaction { events, commit }) => {
                    if events.len() != msg.event_ids.len() {
                        return Err(ConfirmTransactionError::EventsLengthMismatch);
                    }

                    events
                        .into_iter()
                        .zip(msg.event_ids)
                        .map(|(event, event_id)| {
                            if event.event_id != event_id {
                                return Err(ConfirmTransactionError::EventIdMismatch);
                            }

                            Ok(event.offset)
                        })
                        .chain(iter::once(Ok(commit.offset)))
                        .collect::<Result<_, _>>()?
                }
                None => {
                    return Err(ConfirmTransactionError::TransactionNotFound);
                }
            };

            // Retry confirmation updates for replica consistency
            match set_confirmations_with_retry(
                &database,
                msg.partition_id,
                offsets,
                msg.transaction_id,
                msg.confirmation_count,
            )
            .await
            {
                Ok(()) => {
                    // Update watermark after successful confirmation
                    let _ = confirmation_ref
                        .tell(UpdateConfirmation {
                            partition_id: msg.partition_id,
                            versions: msg.event_versions,
                            confirmation_count: msg.confirmation_count,
                        })
                        .await;

                    Ok(())
                }
                Err(err) => {
                    // Log the error but don't fail the message since primary has moved on
                    error!(
                        transaction_id = %msg.transaction_id,
                        partition_id = msg.partition_id,
                        ?err,
                        "replica failed to update confirmations after retries"
                    );

                    // Increment replica failure metrics here for monitoring
                    // REPLICA_CONFIRMATION_FAILURES.inc();

                    // Return success to avoid unnecessary error propagation
                    // The primary doesn't need to know about this failure
                    Ok(())
                }
            }
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteRequestMetadata {
    /// Number of hops this request has taken
    pub hop_count: u8,
    /// Nodes that have already tried to process this request
    pub tried_peers: HashSet<PeerId>,
    /// Original partition hash
    pub partition_hash: PartitionHash,
}
