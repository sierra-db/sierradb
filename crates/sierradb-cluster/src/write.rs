use std::{collections::HashSet, iter, panic::panic_any, time::Duration};

use arrayvec::ArrayVec;
use kameo::prelude::*;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use sierradb::{
    MAX_REPLICATION_FACTOR,
    bucket::{PartitionHash, PartitionId, segment::CommittedEvents},
    database::{CurrentVersion, ExpectedVersion, Transaction, VersionGap},
    id::uuid_to_partition_hash,
    writer_thread_pool::AppendResult,
};
use smallvec::{SmallVec, smallvec};
use thiserror::Error;
use tracing::{error, instrument};
use uuid::Uuid;

use crate::{
    ClusterActor, MAX_FORWARDS,
    confirmation::actor::UpdateConfirmation,
    transaction::{self, set_confirmations_with_retry},
};

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum WriteError {
    #[error("all replica nodes failed to process the write request")]
    AllReplicasFailed,

    #[error("circuit breaker open: estimated recovery time: {estimated_recovery_time:?}")]
    CircuitBreakerOpen {
        estimated_recovery_time: Option<Duration>,
    },

    #[error("failed to update coordinator confirmation count: {0}")]
    ConfirmationFailed(String),

    #[error("failed to update replication confirmation count: {0}")]
    ReplicationConfirmationFailed(String),

    #[error("database operation failed: {0}")]
    DatabaseOperationFailed(String),

    #[error(
        "insufficient healthy replicas for write quorum ({available}/{required} replicas available)"
    )]
    InsufficientHealthyReplicas { available: u8, required: u8 },

    #[error("coordinator is not healthy")]
    InvalidSender,

    #[error("stale write: coordinator has been alive longer than our local record of it")]
    StaleWrite,

    #[error("transaction is missing an expected partition sequence")]
    MissingExpectedPartitionSequence,

    #[error("partition {partition_id} is not owned by this node")]
    PartitionNotOwned { partition_id: PartitionId },

    #[error(
        "write replication quorum not achieved ({confirmed}/{required} confirmations received)"
    )]
    ReplicationQuorumFailed { confirmed: u8, required: u8 },

    #[error("write operation timed out")]
    RequestTimeout,

    #[error("write request exceeded maximum forward hops ({max} allowed)")]
    MaximumForwardsExceeded { max: u8 },

    #[error("remote operation failed: {0}")]
    RemoteOperationFailed(Box<RemoteSendError<WriteError>>),

    #[error("current partition sequence is {current} but expected {expected}")]
    WrongExpectedSequence {
        current: CurrentVersion,
        expected: ExpectedVersion,
    },
}

impl From<sierradb::error::WriteError> for WriteError {
    fn from(err: sierradb::error::WriteError) -> Self {
        WriteError::DatabaseOperationFailed(err.to_string())
    }
}

impl From<RemoteSendError<WriteError>> for WriteError {
    fn from(err: RemoteSendError<WriteError>) -> Self {
        WriteError::RemoteOperationFailed(Box::new(err))
    }
}

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
                    self.database.clone(),
                    self.topology_manager().local_cluster_ref.clone(),
                    self.topology_manager().alive_since,
                    self.confirmation_ref.clone(),
                    replicas,
                    self.replication_factor,
                    transaction,
                    self.circuit_breaker.clone(),
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
    Local {
        replicas: ArrayVec<(RemoteActorRef<ClusterActor>, u64), MAX_REPLICATION_FACTOR>,
    },
    /// Forward to a remote leader
    Remote {
        primary_cluster_ref: RemoteActorRef<ClusterActor>,
        replicas: ArrayVec<(RemoteActorRef<ClusterActor>, u64), MAX_REPLICATION_FACTOR>,
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
    type Reply = DelegatedReply<Result<AppendResult, WriteError>>;

    #[instrument(skip(self, ctx))]
    async fn handle(
        &mut self,
        msg: ExecuteTransaction,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let (delegated_reply, reply_sender) = ctx.reply_sender();

        // Phase 1: Write Request Routing
        match self.resolve_write_destination(msg.transaction.partition_id(), &msg.metadata) {
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
    pub coordinator_ref: RemoteActorRef<ClusterActor>,
    pub coordinator_alive_since: u64,
    pub transaction: Transaction,
}

#[remote_message("ae8dc4cc-e382-4a68-9451-d10c5347d3c9")]
impl Message<ReplicateWrite> for ClusterActor {
    type Reply = DelegatedReply<Result<AppendResult, WriteError>>;

    #[instrument(skip(self, ctx))]
    async fn handle(
        &mut self,
        msg: ReplicateWrite,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Ensure the transaction has the expected partition sequence set
        match msg.transaction.get_expected_partition_sequence() {
            ExpectedVersion::Any | ExpectedVersion::Exists => {
                return ctx.reply(Err(WriteError::MissingExpectedPartitionSequence));
            }
            ExpectedVersion::Empty | ExpectedVersion::Exact(_) => {}
        }

        // Check if we even own this partition
        if !self
            .topology_manager()
            .has_partition(msg.transaction.partition_id())
        {
            return ctx.reply(Err(WriteError::PartitionNotOwned {
                partition_id: msg.transaction.partition_id(),
            }));
        }

        // Ensure the coordinator is available from our perspective
        let Some((_, alive_since)) = self
            .topology_manager()
            .get_available_replicas(msg.transaction.partition_id())
            .into_iter()
            .find(|(cluster_ref, _)| cluster_ref == &msg.coordinator_ref)
        else {
            return ctx.reply(Err(WriteError::InvalidSender));
        };

        // Ensure the write is not stale
        if msg.coordinator_alive_since < alive_since {
            return ctx.reply(Err(WriteError::StaleWrite));
        }

        let database = self.database.clone();

        // Replicate the write to our local database
        ctx.spawn(async move {
            let res = database.append_events(msg.transaction).await;
            match res {
                Ok(append) => Ok(append),
                Err(sierradb::error::WriteError::WrongExpectedSequence {
                    current,
                    expected,
                    ..
                }) => {
                    match expected.gap_from(current) {
                        VersionGap::None => {}
                        VersionGap::Ahead(_) => {
                            // Duplicate/already processed
                        }
                        VersionGap::Behind(_) => {
                            // Gap detected, need catchup
                        }
                        VersionGap::Incompatible => {}
                    }

                    Err(WriteError::WrongExpectedSequence { current, expected })
                }
                Err(err) => Err(WriteError::DatabaseOperationFailed(err.to_string())),
            }
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfirmTransaction {
    pub partition_id: PartitionId,
    pub transaction_id: Uuid,
    pub event_ids: SmallVec<[Uuid; 4]>,
    pub event_partition_sequences: SmallVec<[u64; 4]>,
    pub confirmation_count: u8,
}

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum ConfirmTransactionError {
    #[error("events length mismatch")]
    EventsLengthMismatch,
    #[error("event id mismatch")]
    EventIdMismatch,
    #[error("partition sequence mismatch (expected {expected}, got {actual})")]
    PartitionSequenceMismatch { expected: u64, actual: u64 },
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

                    // Validate partition sequences match expected
                    for (event, expected_version) in
                        events.iter().zip(msg.event_partition_sequences.iter())
                    {
                        if event.partition_sequence != *expected_version {
                            return Err(ConfirmTransactionError::PartitionSequenceMismatch {
                                expected: *expected_version,
                                actual: event.partition_sequence,
                            });
                        }
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
                            versions: msg.event_partition_sequences,
                            confirmation_count: msg.confirmation_count,
                        })
                        .await;

                    Ok(())
                }
                Err(err) => {
                    error!(
                        transaction_id = %msg.transaction_id,
                        partition_id = msg.partition_id,
                        ?err,
                        "replica failed to update confirmations after retries"
                    );

                    // TODO:
                    //
                    // // Mark this replica as degraded for this partition
                    // self.mark_replica_degraded(msg.partition_id, &err);
                    //
                    // // Trigger background recovery process
                    // self.schedule_replica_recovery(msg.partition_id);

                    Err(ConfirmTransactionError::Write(err.to_string()))
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
    /// Partition hash
    pub partition_hash: PartitionHash,
}
