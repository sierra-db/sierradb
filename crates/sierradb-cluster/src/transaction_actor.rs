use std::collections::HashSet;
use std::pin::Pin;
use std::time::Duration;

use futures::FutureExt;
use kameo::error::Infallible;
use kameo::mailbox::Signal;
use kameo::prelude::*;
use sierradb::bucket::PartitionId;
use sierradb::database::{Database, Transaction};
use sierradb::writer_thread_pool::AppendResult;
use tokio::time::Sleep;
use tracing::{debug, error, instrument, warn};
use uuid::Uuid;

use crate::{ClusterActor, ClusterError, ConfirmWrite, ReplicateWrite};

const TIMEOUT: Duration = Duration::from_secs(10);

/// Handles a single write operation, coordinating the distributed consensus
/// process.
#[derive(RemoteActor)]
pub struct TransactionActor {
    database: Database,
    partition_id: PartitionId,
    transaction_id: Uuid,
    transaction: Transaction,
    reply_sender: Option<ReplySender<Result<AppendResult, ClusterError>>>,
    replica_partitions: Vec<(PartitionId, RemoteActorRef<ClusterActor>)>,
    timeout: Pin<Box<Sleep>>,

    // State that tracks the progress of the write operation
    append_result: Option<AppendResult>,
    confirmed_partitions: HashSet<PartitionId>,
    replication_factor: u8,
}

impl TransactionActor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database: Database,
        partition_id: PartitionId,
        transaction: Transaction,
        reply_sender: Option<ReplySender<Result<AppendResult, ClusterError>>>,
        replica_partitions: Vec<(PartitionId, RemoteActorRef<ClusterActor>)>,
        replication_factor: u8,
    ) -> Self {
        Self {
            database,
            partition_id,
            transaction_id: transaction.transaction_id(),
            transaction,
            reply_sender,
            replica_partitions,
            timeout: Box::pin(tokio::time::sleep(TIMEOUT)),
            append_result: None,
            confirmed_partitions: HashSet::from([partition_id]), // Primary partition auto-confirmed
            replication_factor,
        }
    }

    /// Calculates if quorum has been achieved
    pub fn has_quorum(&self) -> bool {
        let required_quorum = (self.replication_factor as usize / 2) + 1;
        self.confirmed_partitions.len() >= required_quorum
    }

    /// Sends success response to client
    async fn send_success(&mut self, result: AppendResult) {
        debug!(transaction_id = %self.transaction_id, "sending success response");

        // Take ownership of the reply
        if let Some(tx) = self.reply_sender.take() {
            tx.send(Ok(result));
        }
    }

    /// Sends failure response to client
    async fn send_failure(&mut self, err: ClusterError) {
        error!(transaction_id = %self.transaction_id, ?err, "sending failure response");

        // Take ownership of the reply
        if let Some(tx) = self.reply_sender.take() {
            tx.send(Err(err));
        }
    }

    /// Marks events as confirmed and broadcasts confirmation to replicas
    pub async fn complete_write(&mut self) {
        if let Some(result) = self.append_result.take() {
            debug!(
                transaction_id = %self.transaction_id,
                partition_id = %self.partition_id,
                "completing write operation"
            );

            // Mark events as confirmed in the database
            if let Err(err) = self
                .database
                .set_confirmations(
                    self.partition_id,
                    result.offsets,
                    self.transaction_id,
                    self.confirmed_partitions.len() as u8,
                )
                .await
            {
                error!(
                    transaction_id = %self.transaction_id,
                    partition_id = %self.partition_id,
                    ?err,
                    "failed to mark confirmations"
                );
                // Note: we continue anyway since we've already told the client
                // the write succeeded. We should handle this confirmation later with catchup.
            }

            // Broadcast confirmation to all confirmed replica partitions
            for (replica_id, cluster_ref) in &self.replica_partitions {
                if self.confirmed_partitions.contains(replica_id) {
                    debug!(
                        transaction_id = %self.transaction_id,
                        replica_id = %replica_id,
                        "sending confirmation to replica"
                    );

                    cluster_ref
                        .tell(&ConfirmWrite {
                            partition_id: *replica_id,
                            transaction_id: self.transaction_id,
                            event_ids: self
                                .transaction
                                .events()
                                .into_iter()
                                .map(|event| event.event_id)
                                .collect(),
                            confirmation_count: self.confirmed_partitions.len() as u8,
                        })
                        .send()
                        .expect("confirm write serialzation should succeed");
                }
            }
        }
    }
}

impl Actor for TransactionActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        mut state: Self::Args,
        actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        debug!(transaction_id = %state.transaction_id, "beginning write operation");

        let has_quorum = state.has_quorum();
        if has_quorum {
            state.transaction = state.transaction.with_confirmation_count(1);
        }

        // PHASE 1: LOCAL WRITE
        // Write events locally and assign versions
        match state
            .database
            .append_events(state.partition_id, state.transaction.clone())
            .await
        {
            Ok(result) => {
                debug!(
                    transaction_id = %state.transaction_id,
                    partition_id = %state.partition_id,
                    "local write successful"
                );

                // let _ = state
                //     .confirmation_ref
                //     .update_event_confirmation(
                //         state.partition_id,
                //         result.partition_sequence, // Use the sequence from the write result
                //         1,                         // First confirmation (local write)
                //     )
                //     .await;

                // PHASE 2: REPLICATION
                // Begin replication to replica partitions
                for (replica_id, cluster_ref) in &state.replica_partitions {
                    debug!(
                        transaction_id = %state.transaction_id,
                        partition_id = %state.partition_id,
                        replica_id = %replica_id,
                        "replicating write"
                    );

                    let _ = cluster_ref
                        .tell(&ReplicateWrite {
                            partition_id: *replica_id,
                            transaction: state.transaction.clone(),
                            transaction_id: state.transaction_id,
                            origin_partition: state.partition_id,
                            transaction_ref: actor_ref.into_remote_ref().await,
                        })
                        .send();
                }

                // Check if we've already achieved quorum (e.g., if replication factor is 1)
                if state.has_quorum() {
                    debug!(
                        transaction_id = %state.transaction_id,
                        partition_id = %state.partition_id,
                        "quorum already achieved"
                    );

                    // Send success response to client with the result
                    state.send_success(result).await;

                    if actor_ref.stop_gracefully().now_or_never().is_none() {
                        warn!("write actors mailbox is full, killing");
                        actor_ref.kill();
                    }
                } else {
                    // Store the result for later use for sending
                    state.append_result = Some(result);
                }
            }
            Err(err) => {
                error!(
                    transaction_id = %state.transaction_id,
                    partition_id = %state.partition_id,
                    ?err,
                    "local write failed"
                );

                // Send failure response
                state
                    .send_failure(ClusterError::Write(err.to_string()))
                    .await;

                if actor_ref.stop_gracefully().now_or_never().is_none() {
                    warn!("write actors mailbox is full, killing");
                    actor_ref.kill();
                }
            }
        }

        Ok(state)
    }

    async fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        _reason: ActorStopReason,
    ) -> Result<(), Self::Error> {
        self.complete_write().await;
        Ok(())
    }

    async fn next(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        mailbox_rx: &mut MailboxReceiver<Self>,
    ) -> Option<Signal<Self>> {
        tokio::select! {
            biased;
            _ = &mut self.timeout => {
                self.send_failure(ClusterError::WriteTimeout).await;
                None // Stop the actor when the timeout has been reached
            },
            msg = mailbox_rx.recv() => msg,
        }
    }
}

/// Message received when a replica confirms a write
#[derive(Debug)]
pub struct ReplicaConfirmation {
    pub partition_id: PartitionId,
    pub transaction_id: Uuid,
    pub result: Result<AppendResult, ClusterError>,
}

impl Message<ReplicaConfirmation> for TransactionActor {
    type Reply = ();

    #[instrument(skip(self, ctx))]
    async fn handle(
        &mut self,
        msg: ReplicaConfirmation,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if msg.transaction_id != self.transaction_id {
            warn!(
                received_id = %msg.transaction_id,
                expected_id = %self.transaction_id,
                "received confirmation for wrong request"
            );
            return;
        }

        match msg.result {
            Ok(_) => {
                self.confirmed_partitions.insert(msg.partition_id);
                if self.has_quorum() && self.append_result.is_some() {
                    debug!(
                        transaction_id = %self.transaction_id,
                        confirmed = self.confirmed_partitions.len(),
                        "quorum achieved"
                    );

                    // Send success response to client with the result
                    if let Some(result) = self.append_result.take() {
                        self.send_success(result).await;
                    }

                    if ctx.actor_ref().stop_gracefully().now_or_never().is_none() {
                        warn!("write actors mailbox is full, killing");
                        ctx.actor_ref().kill();
                    }
                }
            }
            Err(_) => {
                // A replica failed to process the write
                // For now, we'll just log it and continue
                // In a more advanced implementation, we might adjust the quorum calculation
                warn!(
                    transaction_id = %self.transaction_id,
                    partition_id = %msg.partition_id,
                    "replica failed to process write"
                );

                // Check if we can still achieve quorum
                let required_quorum = (self.replication_factor as usize / 2) + 1;
                let max_possible = self.confirmed_partitions.len() + self.replica_partitions.len()
                    - self.confirmed_partitions.len();

                if max_possible < required_quorum {
                    error!(
                        transaction_id = %self.transaction_id,
                        confirmed = self.confirmed_partitions.len(),
                        required = required_quorum,
                        "cannot achieve quorum"
                    );

                    // Send failure response
                    self.send_failure(ClusterError::QuorumNotAchieved {
                        confirmed: self.confirmed_partitions.len() as u8,
                        required: required_quorum as u8,
                    })
                    .await;

                    let self_ref = ctx.actor_ref();
                    if self_ref.stop_gracefully().now_or_never().is_none() {
                        warn!("write actors mailbox is full, killing");
                        self_ref.kill();
                    }
                }
            }
        }
    }
}
