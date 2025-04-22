use std::{fmt, str};

use thiserror::Error;

pub mod event_index;
pub mod partition_index;
pub mod segment;
pub mod stream_index;

pub type BucketId = u16;
pub type SegmentId = u32;
pub type PartitionId = u16;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct BucketSegmentId {
    pub bucket_id: BucketId,
    pub segment_id: SegmentId,
}

impl BucketSegmentId {
    pub fn new(bucket_id: BucketId, segment_id: SegmentId) -> Self {
        BucketSegmentId {
            bucket_id,
            segment_id,
        }
    }

    #[must_use]
    pub fn increment_segment_id(&self) -> Self {
        BucketSegmentId {
            bucket_id: self.bucket_id,
            segment_id: self.segment_id + 1,
        }
    }
}

impl fmt::Display for BucketSegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.bucket_id, self.segment_id)
    }
}

pub enum SegmentKind {
    Events,
    EventIndex,
    PartitionIndex,
    StreamIndex,
}

impl SegmentKind {
    pub const BUCKET_ID_LEN: usize = 5;
    pub const SEGMENT_ID_LEN: usize = 10;
    pub const EXTENSION_LEN: usize = 4;
    pub const FILE_NAME_LEN: usize =
        Self::BUCKET_ID_LEN + "-".len() + Self::SEGMENT_ID_LEN + ".".len() + Self::EXTENSION_LEN;

    pub fn file_name(
        &self,
        BucketSegmentId {
            bucket_id,
            segment_id,
        }: BucketSegmentId,
    ) -> String {
        format!("{bucket_id:05}-{segment_id:010}.{}", self)
    }

    pub fn parse_file_name(s: &str) -> Option<(BucketSegmentId, SegmentKind)> {
        if s.len() != SegmentKind::FILE_NAME_LEN {
            return None;
        }

        let mut pos = 0;

        let bucket_id = s[pos..pos + SegmentKind::BUCKET_ID_LEN].parse().ok()?;
        pos += SegmentKind::BUCKET_ID_LEN;
        pos += "-".len();

        let segment_id = s[pos..pos + SegmentKind::SEGMENT_ID_LEN].parse().ok()?;
        pos += SegmentKind::SEGMENT_ID_LEN;
        pos += ".".len();

        let segment_kind = s[pos..pos + SegmentKind::EXTENSION_LEN].parse().ok()?;

        let bucket_segment_id = BucketSegmentId::new(bucket_id, segment_id);

        Some((bucket_segment_id, segment_kind))
    }
}

impl fmt::Display for SegmentKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SegmentKind::Events => write!(f, "evts"),
            SegmentKind::EventIndex => write!(f, "eidx"),
            SegmentKind::PartitionIndex => write!(f, "pidx"),
            SegmentKind::StreamIndex => write!(f, "sidx"),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Error)]
#[error("invalid segment kind")]
pub struct InvalidSegmentKind;

impl str::FromStr for SegmentKind {
    type Err = InvalidSegmentKind;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "evts" => Ok(SegmentKind::Events),
            "eidx" => Ok(SegmentKind::EventIndex),
            "pidx" => Ok(SegmentKind::PartitionIndex),
            "sidx" => Ok(SegmentKind::StreamIndex),
            _ => Err(InvalidSegmentKind),
        }
    }
}
