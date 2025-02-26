use std::{collections::BTreeMap, fs::File, io, sync::Arc};

use thiserror::Error;
use uuid::Uuid;

use crate::bucket::{
    BucketSegmentId,
    stream_index::{StreamIndexRecord, StreamOffsets},
};

/// Errors which can occur in background threads.
#[derive(Debug, Error)]
pub enum ThreadPoolError {
    #[error("failed to flush event index for {id}: {err}")]
    FlushEventIndex {
        id: BucketSegmentId,
        file: File,
        index: Arc<BTreeMap<Uuid, u64>>,
        num_slots: u64,
        err: io::Error,
    },
    #[error("failed to flush stream index for {id}: {err}")]
    FlushStreamIndex {
        id: BucketSegmentId,
        file: File,
        index: Arc<BTreeMap<Arc<str>, StreamIndexRecord<StreamOffsets>>>,
        num_slots: u64,
        err: io::Error,
    },
}
