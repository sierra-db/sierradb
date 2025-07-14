use std::{collections::HashMap, time::Duration};

use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use kameo::prelude::*;
use sierradb::{
    bucket::PartitionId,
    database::{Database, Transaction},
    writer_thread_pool::AppendResult,
};
use smallvec::SmallVec;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::{
    ClusterActor, ClusterError, ConfirmTransaction, ReplicateWrite,
    confirmation::actor::{ConfirmationActor, UpdateConfirmation},
};

const TIMEOUT: Duration = Duration::from_secs(10);

pub fn spawn(
    database: Database,
    confirmation_ref: ActorRef<ConfirmationActor>,
    partition_id: PartitionId,
    replica_partitions: Vec<(PartitionId, RemoteActorRef<ClusterActor>)>,
    replication_factor: u8,
    transaction: Transaction,
    reply_sender: Option<ReplySender<Result<AppendResult, ClusterError>>>,
) {
    tokio::spawn(async move {
        let transaction_id = transaction.transaction_id();
        debug!(%transaction_id, "beginning distributed write");

        let event_ids: SmallVec<[Uuid; 4]> = transaction
            .events()
            .into_iter()
            .map(|event| event.event_id)
            .collect();

        match tokio::time::timeout(
            TIMEOUT,
            run(
                &database,
                partition_id,
                replica_partitions.clone(),
                replication_factor,
                transaction,
            ),
        )
        .await
        {
            Ok(Ok((append, confirmed_partitions, mut pending_replies))) => {
                debug!(
                    %transaction_id,
                    partition_id = partition_id,
                    "completing write operation"
                );

                let mut confirmation_count = confirmed_partitions.len() as u8;

                // Mark events as confirmed in the database
                if let Err(err) = database
                    .set_confirmations(
                        partition_id,
                        append.offsets.clone(),
                        transaction_id,
                        confirmation_count,
                    )
                    .await
                {
                    error!(
                        %transaction_id,
                        partition_id = partition_id,
                        ?err,
                        "failed to mark confirmations"
                    );
                    // Note: we continue anyway since we've already told the client
                    // the write succeeded. We should handle this confirmation later with catchup.
                }

                let event_partition_sequences: SmallVec<[u64; 4]> =
                    (append.first_partition_sequence..=append.last_partition_sequence).collect();

                let _ = confirmation_ref
                    .tell(UpdateConfirmation {
                        partition_id,
                        versions: event_partition_sequences.clone(),
                        confirmation_count,
                    })
                    .await;

                // Broadcast confirmation to all confirmed replica partitions
                for (partition_id, cluster_ref) in confirmed_partitions {
                    let Some(cluster_ref) = cluster_ref else {
                        continue;
                    };

                    debug!(
                        %transaction_id,
                        partition_id,
                        "sending confirmation to replica"
                    );

                    cluster_ref
                        .tell(&ConfirmTransaction {
                            partition_id,
                            transaction_id,
                            event_ids: event_ids.clone(),
                            event_versions: event_partition_sequences.clone(),
                            confirmation_count,
                        })
                        .send()
                        .expect("ConfirmTransaction serialzation should succeed");
                }

                if let Some(tx) = reply_sender {
                    tx.send(Ok(append));
                }

                // Confirm writes for other partitions which still succeeded
                while let Ok(Some((partition_id, cluster_ref, res))) =
                    tokio::time::timeout(TIMEOUT, pending_replies.next()).await
                {
                    if res.is_ok() {
                        confirmation_count += 1;
                        cluster_ref
                            .tell(&ConfirmTransaction {
                                partition_id,
                                transaction_id,
                                event_ids: event_ids.clone(),
                                event_versions: event_partition_sequences.clone(),
                                confirmation_count,
                            })
                            .send()
                            .expect("confirm write serialzation should succeed");
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
                    tx.send(Err(ClusterError::WriteTimeout));
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
    partition_id: PartitionId,
    replica_partitions: Vec<(PartitionId, RemoteActorRef<ClusterActor>)>,
    replication_factor: u8,
    mut transaction: Transaction,
) -> Result<
    (
        AppendResult,
        HashMap<PartitionId, Option<RemoteActorRef<ClusterActor>>>,
        FuturesUnordered<
            impl Future<
                Output = (
                    PartitionId,
                    RemoteActorRef<ClusterActor>,
                    Result<AppendResult, RemoteSendError<ClusterError>>,
                ),
            > + 'static,
        >,
    ),
    ClusterError,
> {
    let required_quorum = (replication_factor as usize / 2) + 1;
    let has_quorum = required_quorum <= 1;
    if has_quorum {
        transaction = transaction.with_confirmation_count(1);
    }

    let append = database
        .append_events(partition_id, transaction.clone())
        .await?;

    let replica_partitions_len = replica_partitions.len();
    let mut pending_replies: FuturesUnordered<_> = replica_partitions
        .into_iter()
        .map(|(partition_id, cluster_ref)| {
            cluster_ref
                .ask(&ReplicateWrite {
                    partition_id,
                    transaction: transaction.clone(),
                    transaction_id: transaction.transaction_id(),
                    origin_partition: partition_id,
                })
                .enqueue()
                .expect("ReplicateWrite message serialization should succeed")
                .map(move |res| (partition_id, cluster_ref, res))
        })
        .collect();

    let mut confirmed_partitions = HashMap::from_iter([(partition_id, None)]);
    while let Some((partition_id, cluster_ref, res)) = pending_replies.next().await {
        match res {
            Ok(_) => {
                confirmed_partitions.insert(partition_id, Some(cluster_ref));

                let has_quorum = confirmed_partitions.len() >= required_quorum;
                if has_quorum {
                    debug!(
                        transaction_id = %transaction.transaction_id(),
                        confirmed = confirmed_partitions.len(),
                        "quorum achieved"
                    );

                    return Ok((append, confirmed_partitions, pending_replies));
                }
            }
            Err(err) => {
                // A replica failed to process the write
                // For now, we'll just log it and continue
                // In a more advanced implementation, we might adjust the quorum calculation
                warn!(
                    transaction_id = %transaction.transaction_id(),
                    partition_id,
                    "replica failed to process write: {err}"
                );

                // Check if we can still achieve quorum
                let required_quorum = (replication_factor as usize / 2) + 1;
                let max_possible = confirmed_partitions.len() + replica_partitions_len
                    - confirmed_partitions.len();

                if max_possible < required_quorum {
                    error!(
                        transaction_id = %transaction.transaction_id(),
                        confirmed = confirmed_partitions.len(),
                        required = required_quorum,
                        "cannot achieve quorum"
                    );

                    // Send failure response
                    return Err(ClusterError::QuorumNotAchieved {
                        confirmed: confirmed_partitions.len() as u8,
                        required: required_quorum as u8,
                    });
                }
            }
        }
    }

    Err(ClusterError::QuorumNotAchieved {
        confirmed: confirmed_partitions.len() as u8,
        required: required_quorum as u8,
    })
}
