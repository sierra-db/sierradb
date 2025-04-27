use kameo::prelude::*;
use sierradb::bucket::PartitionId;
use sierradb::database::{Database, Transaction};
use tracing::debug;
use uuid::Uuid;

use crate::swarm_actor::{ReplyKind, Swarm};
use crate::write_actor::WriteActor;

/// Actor responsible for managing writes to a specific partition.
/// Ensures one-at-a-time processing of writes to maintain consistency.
#[derive(Actor)]
pub struct PartitionActor {
    swarm_ref: ActorRef<Swarm>,
    database: Database,
    partition_id: PartitionId,
}

impl PartitionActor {
    pub fn new(swarm_ref: ActorRef<Swarm>, database: Database, partition_id: PartitionId) -> Self {
        Self {
            swarm_ref,
            database,
            partition_id,
        }
    }
}

/// Message sent to PartitionActor to initiate a write operation.
pub struct LeaderWriteRequest {
    pub transaction: Transaction,
    pub reply: ReplyKind,
    pub replica_partitions: Vec<PartitionId>,
    pub transaction_id: Uuid,
    pub replication_factor: u8,
}

impl Message<LeaderWriteRequest> for PartitionActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: LeaderWriteRequest,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        debug!(partition_id = %self.partition_id, request_id = %msg.transaction_id, "Handling leader write request");

        // Create a WriteActor to handle this write operation
        let write_actor = WriteActor::new(
            self.swarm_ref.clone(),
            self.database.clone(),
            self.partition_id,
            msg.transaction_id,
            msg.transaction,
            msg.reply,
            msg.replica_partitions,
            msg.replication_factor,
        );

        // Run the write actor until completion
        // This blocks the partition actor (ensuring one-at-a-time processing)
        let _ = WriteActor::prepare().run(write_actor).await;
    }
}
