use std::{collections::HashSet, panic::panic_any, time::Duration};

use kameo::prelude::*;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use sierradb::bucket::{PartitionHash, PartitionId};
use sierradb::{
    database::Transaction, id::uuid_to_partition_hash, writer_thread_pool::AppendResult,
};
use tracing::instrument;

use crate::write::transaction::WriteConfig;
use crate::{ClusterActor, MAX_FORWARDS, ReplicaRefs};

use super::{error::WriteError, transaction};

impl ClusterActor {
    /// Resolves where a write request should be directed
    fn resolve_write_destination(
        &self,
        partition_id: PartitionId,
        metadata: &WriteRequestMetadata,
    ) -> Result<WriteDestination, WriteError> {
        // Check for too many forwards
        if metadata.hop_count > MAX_FORWARDS {
            return Err(WriteError::MaximumForwardsExceeded { max: MAX_FORWARDS });
        }

        // Get partition distribution
        let replicas = self
            .topology_manager()
            .get_available_replicas_for_key(metadata.partition_hash);

        // Check quorum requirements
        let required_quorum = (self.replication_factor as usize / 2) + 1;
        if replicas.len() < required_quorum {
            return Err(WriteError::InsufficientHealthyReplicas {
                available: replicas.len() as u8,
                required: required_quorum as u8,
            });
        }

        // Get leader partition info
        let (primary_cluster_ref, _) = replicas
            .first()
            .cloned()
            .expect("there should be at least 1 replica");

        // Check if we are the leader
        if self.topology_manager().has_partition(partition_id)
            && primary_cluster_ref.id().peer_id().unwrap() == &self.local_peer_id
        {
            // Skip the first partition (primary) and only include partitions we're not the
            // leader for
            let replicas = replicas
                .iter()
                .skip(1) // Skip the primary partition (first in the list)
                .cloned()
                .collect();

            Ok(WriteDestination::Local { replicas })
        } else {
            Ok(WriteDestination::Remote {
                primary_cluster_ref,
                replicas,
            })
        }
    }

    /// Routes a write request to the appropriate destination
    fn route_write_request(
        &mut self,
        local_cluster_ref: &ActorRef<ClusterActor>,
        destination: WriteDestination,
        transaction: Transaction,
        metadata: WriteRequestMetadata,
        reply_sender: Option<ReplySender<Result<AppendResult, WriteError>>>,
    ) {
        // Check circuit breaker before proceeding
        if !self.circuit_breaker.should_allow_request() {
            if let Some(tx) = reply_sender {
                tx.send(Err(WriteError::CircuitBreakerOpen {
                    estimated_recovery_time: self.circuit_breaker.estimated_recovery_time(),
                }));
            }
            return;
        }

        match destination {
            WriteDestination::Local { replicas } => {
                // Perform the write
                transaction::spawn(
                    WriteConfig {
                        database: self.database.clone(),
                        local_cluster_ref: local_cluster_ref.clone(),
                        local_remote_cluster_ref: self.topology_manager().local_cluster_ref.clone(),
                        local_alive_since: self.topology_manager().alive_since,
                        confirmation_ref: self.confirmation_ref.clone(),
                        replicas,
                        replication_factor: self.replication_factor,
                        circuit_breaker: self.circuit_breaker.clone(),
                        broadcast_tx: self.subscription_manager.broadcaster(),
                    },
                    transaction,
                    reply_sender,
                );
            }
            WriteDestination::Remote {
                primary_cluster_ref,
                replicas,
            } => {
                // We need to forward to the leader or an alternative peer
                // Check if we've already tried this peer (to prevent loops)
                if metadata
                    .tried_peers
                    .contains(primary_cluster_ref.id().peer_id().unwrap())
                {
                    // Try the next best peer if available
                    for (cluster_ref, _) in replicas.iter().skip(1) {
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
                        tx.send(Err(WriteError::AllReplicasFailed));
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
        reply_sender: Option<ReplySender<Result<AppendResult, WriteError>>>,
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

                    tx.send(res.map_err(WriteError::from));
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
    Local { replicas: ReplicaRefs },
    /// Forward to a remote leader
    Remote {
        primary_cluster_ref: RemoteActorRef<ClusterActor>,
        replicas: ReplicaRefs,
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

#[remote_message]
impl Message<ExecuteTransaction> for ClusterActor {
    type Reply = DelegatedReply<Result<AppendResult, WriteError>>;

    #[instrument(skip_all, fields(
        partition_key = %msg.transaction.partition_key(),
        partition_id = msg.transaction.partition_id(),
        event_count = msg.transaction.events().len(),
        stream_events = msg.transaction.events().iter().map(|event| format!("{}: {}", event.stream_id, event.event_name)).collect::<smallvec::SmallVec<[_; 4]> >().join(", "),
    ))]
    async fn handle(
        &mut self,
        msg: ExecuteTransaction,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let (delegated_reply, reply_sender) = ctx.reply_sender();

        // Phase 1: Write Request Routing
        match self.resolve_write_destination(msg.transaction.partition_id(), &msg.metadata) {
            Ok(destination) => self.route_write_request(
                ctx.actor_ref(),
                destination,
                msg.transaction,
                msg.metadata,
                reply_sender,
            ),
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteRequestMetadata {
    /// Number of hops this request has taken
    pub hop_count: u8,
    /// Nodes that have already tried to process this request
    pub tried_peers: HashSet<PeerId>,
    /// Partition hash
    pub partition_hash: PartitionHash,
}
