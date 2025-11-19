use std::path::{Path, PathBuf};
use std::{fmt, str};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub mod event_index;
pub mod iter;
pub mod partition_index;
pub mod segment;
pub mod stream_index;

pub type BucketId = u16;
pub type SegmentId = u32;
pub type PartitionKey = Uuid;
pub type PartitionHash = u16;
pub type PartitionId = u16;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
    /// File names for each kind of segment file
    pub fn file_name(&self) -> &'static str {
        match self {
            SegmentKind::Events => "data.evts",
            SegmentKind::EventIndex => "index.eidx",
            SegmentKind::PartitionIndex => "partition.pidx",
            SegmentKind::StreamIndex => "stream.sidx",
        }
    }

    /// Get the full path for a specific segment file
    pub fn get_path(
        &self,
        base_dir: impl AsRef<Path>,
        bucket_segment_id: BucketSegmentId,
    ) -> PathBuf {
        let BucketSegmentId {
            bucket_id,
            segment_id,
        } = bucket_segment_id;

        base_dir
            .as_ref()
            .join("buckets")
            .join(format!("{bucket_id:05}"))
            .join("segments")
            .join(format!("{segment_id:010}"))
            .join(self.file_name())
    }

    /// Parse a full path back into bucket_segment_id and kind
    pub fn parse_path(path: impl AsRef<Path>) -> Option<(BucketSegmentId, SegmentKind)> {
        // Get filename
        let file_name = path.as_ref().file_name()?.to_str()?;

        // Determine segment kind from filename
        let kind = match file_name {
            "data.evts" => SegmentKind::Events,
            "index.eidx" => SegmentKind::EventIndex,
            "partition.pidx" => SegmentKind::PartitionIndex,
            "stream.sidx" => SegmentKind::StreamIndex,
            _ => return None,
        };

        // Extract segment_id from parent directory
        let segments_dir = path.as_ref().parent()?;
        let segment_id_str = segments_dir.file_name()?.to_str()?;
        let segment_id = segment_id_str.parse::<SegmentId>().ok()?;

        // Extract bucket_id from grandparent directory path
        let bucket_dir = segments_dir.parent()?.parent()?;
        let bucket_id_str = bucket_dir.file_name()?.to_str()?;
        let bucket_id = bucket_id_str.parse::<BucketId>().ok()?;

        let bucket_segment_id = BucketSegmentId::new(bucket_id, segment_id);

        Some((bucket_segment_id, kind))
    }

    /// Helper method to create the directory structure for a segment if it
    /// doesn't exist
    pub fn ensure_segment_dir(
        base_dir: impl AsRef<Path>,
        bucket_segment_id: BucketSegmentId,
    ) -> std::io::Result<PathBuf> {
        let BucketSegmentId {
            bucket_id,
            segment_id,
        } = bucket_segment_id;

        let segment_dir = base_dir
            .as_ref()
            .join("buckets")
            .join(format!("{bucket_id:05}"))
            .join("segments")
            .join(format!("{segment_id:010}"));

        if !segment_dir.exists() {
            std::fs::create_dir_all(&segment_dir)?;
        }

        Ok(segment_dir)
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
