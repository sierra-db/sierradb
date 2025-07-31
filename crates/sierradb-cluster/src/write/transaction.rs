use std::{sync::Arc, time::Duration};

use arrayvec::ArrayVec;
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use kameo::prelude::*;
use sierradb::{
    MAX_REPLICATION_FACTOR,
    bucket::PartitionId,
    database::{Database, ExpectedVersion, Transaction},
    writer_thread_pool::AppendResult,
};
use smallvec::SmallVec;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::{
    ClusterActor,
    circuit_breaker::WriteCircuitBreaker,
    confirmation::actor::{ConfirmationActor, UpdateConfirmation},
    write::confirm::ConfirmTransaction,
};

use super::{error::WriteError, replicate::ReplicateWrite};

const TIMEOUT: Duration = Duration::from_secs(10);

/// Configuration for distributed write operations
pub struct WriteConfig {
    pub database: Database,
    pub local_cluster_ref: RemoteActorRef<ClusterActor>,
    pub local_alive_since: u64,
    pub confirmation_ref: ActorRef<ConfirmationActor>,
    pub replicas: ArrayVec<(RemoteActorRef<ClusterActor>, u64), MAX_REPLICATION_FACTOR>,
    pub replication_factor: u8,
    pub circuit_breaker: Arc<WriteCircuitBreaker>,
}

pub fn spawn(
    config: WriteConfig,
    transaction: Transaction,
    reply_sender: Option<ReplySender<Result<AppendResult, WriteError>>>,
) {
    tokio::spawn(async move {
        let transaction_id = transaction.transaction_id();
        let partition_id = transaction.partition_id();
        debug!(%transaction_id, partition_id, "beginning distributed write");

        let event_ids: SmallVec<[Uuid; 4]> = transaction
            .events()
            .into_iter()
            .map(|event| event.event_id)
            .collect();

        match tokio::time::timeout(
            TIMEOUT,
            run(
                &config.database,
                &config.local_cluster_ref,
                config.local_alive_since,
                config.replicas,
                config.replication_factor,
                transaction,
            ),
        )
        .await
        {
            Ok(Ok((append, confirmed_replicas, mut pending_replies))) => {
                debug!(
                    %transaction_id,
                    "completing write operation"
                );

                // Phase 5: Write Confirmation
                let mut confirmation_count = confirmed_replicas.len() as u8;

                // CRITICAL: Set confirmations with retry logic
                match set_confirmations_with_retry(
                    &config.database,
                    partition_id,
                    append.offsets.clone(),
                    transaction_id,
                    confirmation_count,
                )
                .await
                {
                    Ok(()) => {
                        // Success - record in circuit breaker
                        config.circuit_breaker.record_success();

                        // Continue with watermark update and replica confirmations
                        let event_partition_sequences: SmallVec<[u64; 4]> =
                            (append.first_partition_sequence..=append.last_partition_sequence)
                                .collect();

                        let _ = config
                            .confirmation_ref
                            .tell(UpdateConfirmation {
                                partition_id,
                                versions: event_partition_sequences.clone(),
                                confirmation_count,
                            })
                            .await;

                        // Broadcast confirmation to all confirmed replica partitions
                        for cluster_ref in confirmed_replicas {
                            let Some(cluster_ref) = cluster_ref else {
                                continue;
                            };

                            debug!(
                                %transaction_id,
                                partition_id = partition_id,
                                "sending confirmation to replica"
                            );

                            cluster_ref
                                .tell(&ConfirmTransaction {
                                    partition_id,
                                    transaction_id,
                                    event_ids: event_ids.clone(),
                                    event_partition_sequences: event_partition_sequences.clone(),
                                    confirmation_count,
                                })
                                .send()
                                .expect("ConfirmTransaction serialzation should succeed");
                        }

                        if let Some(tx) = reply_sender {
                            tx.send(Ok(append));
                        }

                        // Confirm writes for other partitions which still succeeded
                        while let Ok(Some((cluster_ref, res))) =
                            tokio::time::timeout(TIMEOUT, pending_replies.next()).await
                        {
                            if res.is_ok() {
                                confirmation_count += 1;
                                cluster_ref
                                    .tell(&ConfirmTransaction {
                                        partition_id,
                                        transaction_id,
                                        event_ids: event_ids.clone(),
                                        event_partition_sequences: event_partition_sequences
                                            .clone(),
                                        confirmation_count,
                                    })
                                    .send()
                                    .expect("confirm write serialzation should succeed");
                            }
                        }
                    }
                    Err(err) => {
                        // CRITICAL FAILURE - we told replicas to write but can't confirm locally
                        config.circuit_breaker.record_failure();

                        error!(
                            %transaction_id,
                            partition_id,
                            ?err,
                            "CRITICAL: Failed to set confirmations after achieving quorum"
                        );

                        if let Some(tx) = reply_sender {
                            tx.send(Err(WriteError::ConfirmationFailed(err.to_string())));
                        }
                    }
                }
            }
            Ok(Err(err)) => match reply_sender {
                Some(tx) => {
                    tx.send(Err(err));
                }
                None => {
                    error!("distributed write failed: {err}");
                }
            },
            Err(elapsed) => match reply_sender {
                Some(tx) => {
                    tx.send(Err(WriteError::RequestTimeout));
                }
                None => {
                    error!("distributed write timed out after {elapsed}");
                }
            },
        }
    });
}

async fn run(
    database: &Database,
    local_cluster_ref: &RemoteActorRef<ClusterActor>,
    local_alive_since: u64,
    replicas: ArrayVec<(RemoteActorRef<ClusterActor>, u64), MAX_REPLICATION_FACTOR>,
    replication_factor: u8,
    mut transaction: Transaction,
) -> Result<
    (
        AppendResult,
        ArrayVec<Option<RemoteActorRef<ClusterActor>>, MAX_REPLICATION_FACTOR>,
        FuturesUnordered<
            impl Future<
                Output = (
                    RemoteActorRef<ClusterActor>,
                    Result<AppendResult, RemoteSendError<SendError<ReplicateWrite, WriteError>>>,
                ),
            > + 'static,
        >,
    ),
    WriteError,
> {
    // Phase 2: Coordinator Write Preparation
    let required_quorum = (replication_factor as usize / 2) + 1;
    let has_quorum = required_quorum <= 1;
    if has_quorum {
        transaction = transaction.with_confirmation_count(1);
    }

    let append = database.append_events(transaction.clone()).await?;

    // Phase 3: Replica Write Replication
    let expected_partition_sequence =
        ExpectedVersion::from_next_version(append.first_partition_sequence);

    // Check our new expected sequence doesn't mismatch the original expected
    // sequence
    #[cfg(debug_assertions)]
    {
        match transaction.get_expected_partition_sequence() {
            ExpectedVersion::Any => {}
            ExpectedVersion::Exists => {
                debug_assert_ne!(expected_partition_sequence, ExpectedVersion::Empty);
            }
            ExpectedVersion::Empty => {
                debug_assert_eq!(expected_partition_sequence, ExpectedVersion::Empty);
            }
            ExpectedVersion::Exact(exact) => {
                debug_assert_eq!(expected_partition_sequence, ExpectedVersion::Exact(exact));
            }
        }
    }

    transaction = transaction.expected_partition_sequence(expected_partition_sequence);

    let mut pending_replies: FuturesUnordered<_> = replicas
        .into_iter()
        .map(|(cluster_ref, _)| {
            cluster_ref
                .ask(&ReplicateWrite {
                    coordinator_ref: local_cluster_ref.clone(),
                    coordinator_alive_since: local_alive_since,
                    transaction: transaction.clone(),
                })
                .mailbox_timeout(TIMEOUT)
                .reply_timeout(TIMEOUT)
                .enqueue()
                .expect("ReplicateWrite message serialization should succeed")
                .map(move |res| (cluster_ref, res))
        })
        .collect();

    // Phase 4: Quorum Confirmation
    let mut confirmed_replicas = ArrayVec::from_iter([None]);

    // Early return for single node case - if no replicas, we already have quorum
    if pending_replies.is_empty() {
        let has_quorum = confirmed_replicas.len() >= required_quorum;
        if has_quorum {
            debug!(
                transaction_id = %transaction.transaction_id(),
                confirmed = confirmed_replicas.len(),
                "quorum achieved (single node)"
            );
            return Ok((append, confirmed_replicas, pending_replies));
        }
    }

    while let Some((cluster_ref, res)) = pending_replies.next().await {
        match res {
            Ok(_) => {
                confirmed_replicas.push(Some(cluster_ref));

                let has_quorum = confirmed_replicas.len() >= required_quorum;
                if has_quorum {
                    debug!(
                        transaction_id = %transaction.transaction_id(),
                        confirmed = confirmed_replicas.len(),
                        "quorum achieved"
                    );

                    return Ok((append, confirmed_replicas, pending_replies));
                }
            }
            Err(err) => {
                // A replica failed to process the write
                // For now, we'll just log it and continue
                // In a more advanced implementation, we might adjust the quorum calculation
                warn!(
                    transaction_id = %transaction.transaction_id(),
                    cluster_ref_id = %cluster_ref.id(),
                    "replica failed to process write: {err}"
                );

                // Check if we can still achieve quorum
                let required_quorum = (replication_factor as usize / 2) + 1;
                let max_possible = confirmed_replicas.len() + pending_replies.len();

                if max_possible < required_quorum {
                    error!(
                        transaction_id = %transaction.transaction_id(),
                        confirmed = confirmed_replicas.len(),
                        required = required_quorum,
                        "cannot achieve quorum"
                    );

                    // Send failure response
                    return Err(WriteError::ReplicationQuorumFailed {
                        confirmed: confirmed_replicas.len() as u8,
                        required: required_quorum as u8,
                    });
                }
            }
        }
    }

    Err(WriteError::ReplicationQuorumFailed {
        confirmed: confirmed_replicas.len() as u8,
        required: required_quorum as u8,
    })
}

pub async fn set_confirmations_with_retry(
    database: &Database,
    partition_id: PartitionId,
    offsets: SmallVec<[u64; 4]>,
    transaction_id: Uuid,
    confirmation_count: u8,
) -> Result<(), sierradb::error::WriteError> {
    const MAX_ATTEMPTS: u32 = 5;
    const BASE_DELAY: Duration = Duration::from_millis(100);
    const MAX_DELAY: Duration = Duration::from_secs(5);

    let mut attempts = 0;
    let mut delay = BASE_DELAY;

    loop {
        match database
            .set_confirmations(
                partition_id,
                offsets.clone(),
                transaction_id,
                confirmation_count,
            )
            .await
        {
            Ok(()) => return Ok(()),
            Err(err) => {
                attempts += 1;

                if attempts >= MAX_ATTEMPTS {
                    return Err(err);
                }

                warn!(
                    %transaction_id,
                    partition_id,
                    attempt = attempts,
                    ?err,
                    "retrying confirmation update"
                );

                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(MAX_DELAY);
            }
        }
    }
}
