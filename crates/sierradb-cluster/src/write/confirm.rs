use std::iter;

use kameo::prelude::*;
use serde::{Deserialize, Serialize};
use sierradb::bucket::{PartitionId, segment::CommittedEvents};
use smallvec::{SmallVec, smallvec};
use tracing::{error, instrument};
use uuid::Uuid;

use crate::{ClusterActor, confirmation::actor::UpdateConfirmation};

use super::{error::ConfirmTransactionError, transaction::set_confirmations_with_retry};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfirmTransaction {
    pub partition_id: PartitionId,
    pub transaction_id: Uuid,
    pub event_ids: SmallVec<[Uuid; 4]>,
    pub event_partition_sequences: SmallVec<[u64; 4]>,
    pub confirmation_count: u8,
}

#[remote_message]
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
        let broadcast_tx = self.subscription_manager.broadcaster();

        ctx.spawn(async move {
            let Some(first_event_id) = msg.event_ids.first() else {
                return Err(ConfirmTransactionError::EventsLengthMismatch);
            };

            let (offsets, commit) = match database
                .read_transaction(msg.partition_id, *first_event_id)
                .await
                .map_err(|err| ConfirmTransactionError::Read(err.to_string()))?
            {
                Some(CommittedEvents::Single(event)) => {
                    (smallvec![event.offset], CommittedEvents::Single(event))
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

                    let offsets = events
                        .iter()
                        .zip(msg.event_ids)
                        .map(|(event, event_id)| {
                            if event.event_id != event_id {
                                return Err(ConfirmTransactionError::EventIdMismatch);
                            }

                            Ok(event.offset)
                        })
                        .chain(iter::once(Ok(commit.offset)))
                        .collect::<Result<_, _>>()?;

                    (offsets, CommittedEvents::Transaction { events, commit })
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
                            versions: msg.event_partition_sequences.clone(),
                            confirmation_count: msg.confirmation_count,
                        })
                        .await;

                    // Notify subscriptions about confirmed events
                    for event in commit {
                        if broadcast_tx.send(event).is_err() {
                            break;
                        }
                    }

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
