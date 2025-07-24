use kameo::prelude::*;
use serde::{Deserialize, Serialize};
use sierradb::{
    database::{ExpectedVersion, Transaction, VersionGap},
    writer_thread_pool::AppendResult,
};
use tracing::instrument;

use crate::ClusterActor;

use super::error::WriteError;

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
                        VersionGap::Incompatible => {
                            unreachable!(
                                "we verified the transaction has a valid expected sequence"
                            )
                        }
                    }

                    Err(WriteError::WrongExpectedSequence { current, expected })
                }
                Err(err) => Err(WriteError::DatabaseOperationFailed(err.to_string())),
            }
        })
    }
}
