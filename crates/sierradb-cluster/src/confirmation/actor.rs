use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use kameo::prelude::*;
use sierradb::bucket::PartitionId;
use sierradb::database::Database;
use tracing::error;
use uuid::Uuid;

use super::{AtomicWatermark, BucketConfirmationManager, ConfirmationError};

pub struct ConfirmationActor {
    database: Database,
    manager: BucketConfirmationManager,
    owned_partitions: HashSet<PartitionId>,
    watermarks: HashMap<PartitionId, Arc<AtomicWatermark>>,
}

impl ConfirmationActor {
    async fn reconsiliate(&mut self, actor_ref: &ActorRef<Self>) {
        for partition_id in self.owned_partitions.iter().copied() {
            let watermark = self
                .watermarks
                .get(&partition_id)
                .or_else(|| self.manager.get_watermark(partition_id))
                .map(|watermark| watermark.get());

            let database = self.database.clone();
            let actor_ref = actor_ref.clone();
            tokio::spawn(async move {
                let mut partition_iter = match database
                    .read_partition(
                        partition_id,
                        watermark.map(|watermark| watermark + 1).unwrap_or(0),
                    )
                    .await
                {
                    Ok(iter) => iter,
                    Err(err) => {
                        error!("failed to read partition: {err}");
                        return;
                    }
                };

                let mut unconfirmed = Vec::with_capacity(100);
                while unconfirmed.len() <= 100 {
                    match partition_iter.next(true).await {
                        Ok(Some(event)) => {
                            unconfirmed.push(UnconfirmedEvent {
                                event_id: event.event_id,
                                partition_key: event.partition_key,
                                partition_sequence: event.partition_sequence,
                            });
                        }
                        Ok(None) => break,
                        Err(err) => {
                            error!("failed to read partition: {err}");
                            break;
                        }
                    }
                }
            });
        }
    }
}

impl Actor for ConfirmationActor {
    type Args = Self;
    type Error = ConfirmationError;

    async fn on_start(args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(args)
    }
}

pub struct UnconfirmedEvent {
    pub event_id: Uuid,
    pub partition_key: Uuid,
    pub partition_sequence: u64,
}

/// Request to mark an event as confirmed by a specific node
pub struct MarkEventConfirmed {
    pub partition_id: PartitionId,
    pub stream_id: StreamId,
    pub event_version: u64,
    pub partition_version: u64,
    pub peer_id: PeerId,
}

pub struct InitiateConfirmation {
    events: Vec<UnconfirmedEvent>,
}

impl Message<InitiateConfirmation> for ConfirmationActor {
    type Reply = ();

    async fn handle(&mut self, msg: InitiateConfirmation, _ctx: &mut Context<Self, Self::Reply>) {}
}
