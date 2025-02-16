pub mod event_index;
pub mod flusher;
pub mod segment;
// pub mod segment_set;
pub mod stream_index;

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
