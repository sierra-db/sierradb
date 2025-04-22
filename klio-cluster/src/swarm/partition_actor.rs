use std::sync::Arc;

use kameo::prelude::*;

use crate::{
    bucket::{BucketId, writer_thread_pool::AppendEventsBatch},
    database::Database,
};

#[derive(Actor)]
pub struct PartitionActor {
    database: Database,
    bucket_id: BucketId,
    next_partition_version: u64,
}

pub struct AppendEvents {
    events: Arc<AppendEventsBatch>,
}

impl Message<AppendEvents> for PartitionActor {
    type Reply = ();

    async fn handle(
        &mut self,
        AppendEvents { events }: AppendEvents,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Step 2: Leader validates its leadership term and reserves sequential stream & partition versions
        let latest = self.database.read_stream_latest_version(self.bucket_id);

        // Step 3: Leader assigns these versions to events in the append request
    }
}
