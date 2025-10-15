use std::collections::HashSet;

use kameo::error::Infallible;
use kameo::prelude::*;
use sierradb::bucket::PartitionId;
use sierradb::database::Database;
use sierradb::writer_thread_pool::{AppendEventsBatch, AppendResult};
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::error::SwarmError;
use crate::swarm_actor::{ConfirmWrite, ReplicateWrite, ReplyKind, SendReplicaResponse, Swarm};

/// Handles a single write operation, coordinating the distributed consensus
/// process.
pub struct WriteActor {
    swarm_ref: ActorRef<Swarm>,
    database: Database,
    partition_id: PartitionId,
    transaction_id: Uuid,
    append: AppendEventsBatch,
    reply: Option<ReplyKind>,
    replica_partitions: Vec<PartitionId>,

    // State that tracks the progress of the write operation
    append_result: Option<AppendResult>,
    confirmed_partitions: HashSet<PartitionId>,
    replication_factor: u8,
    // Track whether we've already sent the success response to avoid duplicate responses
    response_sent: bool,
}

impl WriteActor {
    pub fn new(
        swarm_ref: ActorRef<Swarm>,
        database: Database,
        partition_id: PartitionId,
        transaction_id: Uuid,
        append: AppendEventsBatch,
        reply: ReplyKind,
        replica_partitions: Vec<PartitionId>,
        replication_factor: u8,
    ) -> Self {
        Self {
            swarm_ref,
            database,
            partition_id,
            transaction_id,
            append,
            reply: Some(reply),
            replica_partitions,
            append_result: None,
            confirmed_partitions: HashSet::from([partition_id]), /* Primary partition is
                                                                  * auto-confirmed */
            replication_factor,
            response_sent: false,
        }
    }

    /// Calculates if quorum has been achieved
    fn has_quorum(&self) -> bool {
        let required_quorum = (self.replication_factor as usize / 2) + 1;
        self.confirmed_partitions.len() >= required_quorum
    }

    /// Sends success response to client
    async fn send_success(&mut self, result: AppendResult) {
        if self.response_sent {
            return;
        }

        debug!(transaction_id = ?self.transaction_id, "Sending success response");

        // Take ownership of the reply
        if let Some(reply) = self.reply.take() {
            match reply {
                ReplyKind::Local(tx) => {
                    // Send response to local client
                    let _ = tx.send(Ok(result));
                }
                ReplyKind::Remote(channel) => {
                    // Send response through libp2p response channel for remote clients
                    let _ = self
                        .swarm_ref
                        .tell(SendReplicaResponse {
                            channel, // Pass channel by value, don't clone
                            result: Ok(result),
                        })
                        .await;
                }
            }
            self.response_sent = true;
        }
    }

    /// Sends failure response to client
    async fn send_failure(&mut self, error: SwarmError) {
        if self.response_sent {
            return;
        }

        error!(transaction_id = ?self.transaction_id, ?error, "Sending failure response");

        // Take ownership of the reply
        if let Some(reply) = self.reply.take() {
            match reply {
                ReplyKind::Local(tx) => {
                    let _ = tx.send(Err(error));
                }
                ReplyKind::Remote(channel) => {
                    let _ = self
                        .swarm_ref
                        .tell(SendReplicaResponse {
                            channel,
                            result: Err(error),
                        })
                        .await;
                }
            }
            self.response_sent = true;
        }
    }

    /// Marks events as confirmed and broadcasts confirmation to replicas
    async fn complete_write(&self) {
        if let Some(ref result) = self.append_result {
            debug!(
                transaction_id = ?self.transaction_id,
                partition_id = ?self.partition_id,
                "Completing write operation"
            );

            // Mark events as confirmed in the database
            if let Err(err) = self
                .database
                .set_confirmations(
                    self.partition_id,
                    result.offsets.clone(),
                    self.transaction_id,
                    self.confirmed_partitions.len() as u8,
                )
                .await
            {
                error!(
                    transaction_id = ?self.transaction_id,
                    partition_id = ?self.partition_id,
                    ?err,
                    "Failed to mark confirmations"
                );
                // Note: we continue anyway since we've already told the client
                // the write succeeded
            }

            // Broadcast confirmation to all confirmed replica partitions
            for replica_id in &self.replica_partitions {
                if self.confirmed_partitions.contains(replica_id) {
                    debug!(
                        transaction_id = ?self.transaction_id,
                        replica_id = ?replica_id,
                        "Sending confirmation to replica"
                    );

                    let _ = self
                        .swarm_ref
                        .tell(ConfirmWrite {
                            partition_id: *replica_id,
                            transaction_id: self.transaction_id,
                            offsets: result.offsets.clone(),
                            confirmation_count: self.confirmed_partitions.len() as u8,
                        })
                        .await;
                }
            }
        }
    }
}

impl Actor for WriteActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        mut state: Self::Args,
        actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        debug!(transaction_id = %state.transaction_id, "Beginning write operation");

        // PHASE 1: LOCAL WRITE
        // Write events locally and assign versions
        match state
            .database
            .append_events(state.partition_id, state.append.clone())
            .await
        {
            Ok(result) => {
                debug!(
                    transaction_id = ?state.transaction_id,
                    partition_id = ?state.partition_id,
                    "Local write successful"
                );

                // Store the result for later use and create a clone for sending
                let result_clone = result.clone();
                state.append_result = Some(result);

                // PHASE 2: REPLICATION
                // Begin replication to replica partitions
                for replica_id in &state.replica_partitions {
                    debug!(
                        transaction_id = ?state.transaction_id,
                        partition_id = ?state.partition_id,
                        replica_id = ?replica_id,
                        "Replicating write"
                    );

                    let _ = state
                        .swarm_ref
                        .tell(ReplicateWrite {
                            partition_id: *replica_id,
                            append: state.append.clone(),
                            transaction_id: state.transaction_id,
                            origin_partition: state.partition_id,
                            write_actor_ref: actor_ref.clone(),
                        })
                        .await;
                }

                // Check if we've already achieved quorum (e.g., if replication factor is 1)
                if state.has_quorum() {
                    debug!(
                        transaction_id = ?state.transaction_id,
                        partition_id = ?state.partition_id,
                        "Quorum already achieved"
                    );

                    // Send success response to client with the cloned result
                    state.send_success(result_clone).await;

                    // Mark events as confirmed
                    state.complete_write().await;
                }
            }
            Err(err) => {
                error!(
                    transaction_id = ?state.transaction_id,
                    partition_id = ?state.partition_id,
                    ?err,
                    "Local write failed"
                );

                // Send failure response
                state.send_failure(SwarmError::Write(err.to_string())).await;
            }
        }

        Ok(state)
    }
}

/// Message received when a replica confirms a write
pub struct ReplicaConfirmation {
    pub partition_id: PartitionId,
    pub transaction_id: Uuid,
    pub success: bool,
}

impl Message<ReplicaConfirmation> for WriteActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: ReplicaConfirmation,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if msg.transaction_id != self.transaction_id {
            warn!(
                received_id = ?msg.transaction_id,
                expected_id = ?self.transaction_id,
                "Received confirmation for wrong request"
            );
            return;
        }

        debug!(
            transaction_id = ?self.transaction_id,
            partition_id = ?msg.partition_id,
            success = msg.success,
            "Received replica confirmation"
        );

        if msg.success {
            // Add partition to confirmed set
            self.confirmed_partitions.insert(msg.partition_id);

            // Check if we've achieved quorum
            if self.has_quorum() && self.append_result.is_some() && !self.response_sent {
                debug!(
                    transaction_id = ?self.transaction_id,
                    confirmed = self.confirmed_partitions.len(),
                    "Quorum achieved"
                );

                // Send success response to client with a clone of the result
                if let Some(ref result) = self.append_result.clone() {
                    self.send_success(result.clone()).await;
                }

                // Mark events as confirmed
                self.complete_write().await;
            }
        } else {
            // A replica failed to process the write
            // For now, we'll just log it and continue
            // In a more advanced implementation, we might adjust the quorum calculation
            warn!(
                transaction_id = ?self.transaction_id,
                partition_id = ?msg.partition_id,
                "Replica failed to process write"
            );

            // Check if we can still achieve quorum
            let required_quorum = (self.replication_factor as usize / 2) + 1;
            let max_possible = self.confirmed_partitions.len() + self.replica_partitions.len()
                - self.confirmed_partitions.len();

            if max_possible < required_quorum {
                error!(
                    transaction_id = ?self.transaction_id,
                    confirmed = self.confirmed_partitions.len(),
                    required = required_quorum,
                    "Cannot achieve quorum"
                );

                // Send failure response
                self.send_failure(SwarmError::QuorumNotAchieved {
                    confirmed: self.confirmed_partitions.len() as u8,
                    required: required_quorum as u8,
                })
                .await;
            }
        }
    }
}
