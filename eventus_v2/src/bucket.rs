use std::fmt;

pub mod event_index;
pub mod flusher;
pub mod reader_thread_pool;
pub mod segment;
// pub mod segment_set;
pub mod stream_index;
pub mod writer_thread_pool;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct BucketSegmentId {
    pub bucket_id: u16,
    pub segment_id: u32,
}

impl BucketSegmentId {
    pub fn new(bucket_id: u16, segment_id: u32) -> Self {
        BucketSegmentId {
            bucket_id,
            segment_id,
        }
    }
}

impl fmt::Display for BucketSegmentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.bucket_id, self.segment_id)
    }
}

// pub struct BucketSet {
//     buckets: Box<[Bucket]>,
// }

// pub struct Bucket {
//     id: u16,
//     segments: Vec<BucketSegmentSet>,
// }

// pub struct BucketSegmentSet {
//     events: BucketSegmentWriter,
//     event_index: EventIndex,
//     stream_index: StreamIndex,
// }

pub enum SegmentKind {
    Events,
    EventIndex,
    StreamIndex,
}

impl fmt::Display for SegmentKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SegmentKind::Events => write!(f, "events"),
            SegmentKind::EventIndex => write!(f, "event_index"),
            SegmentKind::StreamIndex => write!(f, "stream_index"),
        }
    }
}

pub fn file_name(
    BucketSegmentId {
        bucket_id,
        segment_id,
    }: BucketSegmentId,
    kind: SegmentKind,
) -> String {
    format!("{bucket_id:05}-{segment_id:010}.{kind}.dat")
}
