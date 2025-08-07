pub mod actor;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bincode::{Decode, Encode};
use kameo::Reply;
use serde::{Deserialize, Serialize};
use sierradb::bucket::{BucketId, PartitionId};
use sierradb::database::Database;
use sierradb::error::PartitionIndexError;
use thiserror::Error;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::info;

/// Errors that can occur during confirmation state operations
#[derive(Error, Debug)]
pub enum ConfirmationError {
    #[error("invalid state format: {0}")]
    InvalidFormat(String),

    #[error("bucket {0} not found")]
    BucketNotFound(BucketId),

    #[error("partition {0} not found")]
    PartitionNotFound(PartitionId),

    #[error("corrupted state file due to crc32c hash mismatch")]
    CorruptedState,

    #[error("encode error: {0}")]
    Encode(#[from] bincode::error::EncodeError),

    #[error("decode error: {0}")]
    Decode(#[from] bincode::error::DecodeError),

    #[error(transparent)]
    PartitionIndex(#[from] PartitionIndexError),

    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Information about an unconfirmed event
#[derive(Debug, Clone, Encode, Decode)]
pub struct UnconfirmedEventInfo {
    pub version: u64,
    pub confirmation_count: u8,
    pub first_seen: u64,   // Unix timestamp
    pub last_attempt: u64, // Unix timestamp
    pub attempts: u8,
}

/// Tracks confirmation state for a partition
#[derive(Debug, Clone, Encode, Decode)]
pub struct PartitionConfirmationState {
    pub partition_id: PartitionId,
    pub highest_version: u64,
    pub confirmed_watermark: Arc<AtomicWatermark>,
    pub unconfirmed_events: BTreeMap<u64, UnconfirmedEventInfo>,
}

impl PartitionConfirmationState {
    pub fn new(partition_id: PartitionId) -> Self {
        Self {
            partition_id,
            highest_version: 0,
            confirmed_watermark: Arc::new(AtomicWatermark::new(0)),
            unconfirmed_events: BTreeMap::new(),
        }
    }

    /// Updates the confirmation count for an event and advances the watermark
    /// if possible
    pub fn update_confirmation(
        &mut self,
        version: u64,
        confirmation_count: u8,
        replication_factor: u8,
    ) -> bool {
        // Track highest version we've seen
        if version > self.highest_version {
            self.highest_version = version;
        }

        // If the version is already below or at the watermark, nothing to do
        let confirmed_watermark = self.confirmed_watermark.get();
        if version <= confirmed_watermark {
            return false;
        }

        // Update or create unconfirmed event entry
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs();

        let event =
            self.unconfirmed_events
                .entry(version)
                .or_insert_with(|| UnconfirmedEventInfo {
                    version,
                    confirmation_count: 0,
                    first_seen: now,
                    last_attempt: now,
                    attempts: 0,
                });

        // Update the event's confirmation status
        event.confirmation_count = confirmation_count;
        event.last_attempt = now;
        event.attempts += 1;

        // Check if we can advance the watermark
        let required_quorum = (replication_factor / 2) + 1;

        // Find the highest contiguous confirmed version
        let mut next_expected = confirmed_watermark + 1;
        let mut new_watermark = confirmed_watermark;

        // Try to advance watermark contiguously
        while let Some(event) = self.unconfirmed_events.get(&next_expected) {
            if event.confirmation_count >= required_quorum {
                new_watermark = next_expected;
                next_expected += 1;
            } else {
                break;
            }
        }

        let watermark_advanced = new_watermark > confirmed_watermark;

        // Update watermark if it advanced
        if watermark_advanced {
            if self.confirmed_watermark.advance(new_watermark) {
                info!(
                    "partition {} advanced watermark to {new_watermark}",
                    self.partition_id
                );
            }

            // Clean up unconfirmed events that are now below the watermark
            self.unconfirmed_events
                .retain(|&ver, _| ver > new_watermark);
        }

        watermark_advanced
    }

    /// Gets the confirmation gap (highest version - watermark)
    pub fn confirmation_gap(&self) -> u64 {
        self.highest_version
            .saturating_sub(self.confirmed_watermark.get())
    }

    /// Gets info about stuck events (events that haven't been confirmed despite
    /// retry attempts)
    pub fn get_stuck_events(&self, min_attempts: u8, min_age_secs: u64) -> Vec<u64> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs();

        self.unconfirmed_events
            .iter()
            .filter(|(_, info)| {
                info.attempts >= min_attempts && (now - info.first_seen) >= min_age_secs
            })
            .map(|(&version, _)| version)
            .collect()
    }
}

/// Persisted bucket confirmation state format
#[derive(Encode, Decode)]
pub struct BucketConfirmationState {
    pub version: u32,   // Schema version for future-proofing
    pub timestamp: u64, // When this state was written
    pub bucket_id: BucketId,
    // Maps partition_id -> confirmation state
    pub partition_states: HashMap<PartitionId, PartitionConfirmationState>,
}

/// Checksum wrapper for file integrity
#[derive(Encode, Decode)]
pub struct ChecksummedState {
    data: Vec<u8>, // Bincode encoded BucketConfirmationState
    crc32c: u32,   // Simple checksum of the data
}

impl ChecksummedState {
    fn new(state: &BucketConfirmationState) -> Result<Self, ConfirmationError> {
        let data = bincode::encode_to_vec(state, bincode::config::standard())?;
        let crc32c = crc32fast::hash(&data);

        Ok(Self { data, crc32c })
    }

    pub fn validate(&self) -> bool {
        crc32fast::hash(&self.data) == self.crc32c
    }

    pub fn deserialize(&self) -> Result<BucketConfirmationState, ConfirmationError> {
        if !self.validate() {
            return Err(ConfirmationError::CorruptedState);
        }

        let (state, _) = bincode::decode_from_slice(&self.data, bincode::config::standard())?;
        Ok(state)
    }
}

/// Manager for bucket confirmation state
pub struct BucketConfirmationManager {
    data_dir: PathBuf,
    num_buckets: u16,
    replication_factor: u8,
    buckets: HashMap<BucketId, HashMap<PartitionId, PartitionConfirmationState>>,
    assigned_partitions: HashSet<PartitionId>,
    last_persist_times: HashMap<BucketId, Instant>,
    changes_since_persist: HashMap<BucketId, usize>,
}

impl BucketConfirmationManager {
    pub fn new(
        data_dir: PathBuf,
        num_buckets: u16,
        replication_factor: u8,
        assigned_partitions: HashSet<PartitionId>,
    ) -> Self {
        Self {
            data_dir,
            num_buckets,
            replication_factor,
            assigned_partitions,
            buckets: HashMap::new(),
            last_persist_times: HashMap::new(),
            changes_since_persist: HashMap::new(),
        }
    }

    pub async fn initialize(&mut self, database: &Database) -> Result<(), ConfirmationError> {
        // Create confirmation directories if they don't exist
        for partition_id in self.assigned_partitions.clone() {
            let bucket_id = partition_id % self.num_buckets;
            let bucket_dir = self.get_bucket_confirmation_dir(bucket_id);
            let _ = fs::create_dir_all(&bucket_dir).await;

            // Load existing state if available
            self.load_bucket_state(bucket_id).await;
        }

        // Insert partitions if they dont exist
        for partition_id in &self.assigned_partitions {
            let bucket_id = partition_id % self.num_buckets;
            self.buckets
                .entry(bucket_id)
                .or_default()
                .entry(*partition_id)
                .or_insert_with(|| PartitionConfirmationState::new(*partition_id));
        }

        // Update confirmations for events on disk already but beyond the watermark
        let watermarks: Vec<_> = self
            .buckets
            .values()
            .flat_map(|partitions| {
                partitions
                    .iter()
                    .map(|(partition_id, state)| (*partition_id, state.confirmed_watermark.get()))
            })
            .collect();
        for (partition_id, watermark) in watermarks {
            let mut iter = database.read_partition(partition_id, watermark).await?;
            while let Some(commit) = iter.next().await? {
                for event in commit {
                    self.update_confirmation(
                        partition_id,
                        event.partition_sequence + 1,
                        event.confirmation_count,
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }

    /// Get the appropriate bucket for a partition
    fn get_bucket_for_partition(&self, partition_id: PartitionId) -> BucketId {
        // Simple modulo distribution
        partition_id % self.num_buckets
    }

    /// Get directory path for bucket confirmation files
    fn get_bucket_confirmation_dir(&self, bucket_id: BucketId) -> PathBuf {
        let bucket_path = format!("{bucket_id:05}");
        self.data_dir
            .join("buckets")
            .join(bucket_path)
            .join("confirmation")
    }

    /// Get current state file path for a bucket
    fn get_current_state_path(&self, bucket_id: BucketId) -> PathBuf {
        self.get_bucket_confirmation_dir(bucket_id)
            .join("bucket_state.current.dat")
    }

    /// Get previous state file path for a bucket
    fn get_previous_state_path(&self, bucket_id: BucketId) -> PathBuf {
        self.get_bucket_confirmation_dir(bucket_id)
            .join("bucket_state.previous.dat")
    }

    /// Get temporary state file path for a bucket
    fn get_temp_state_path(&self, bucket_id: BucketId) -> PathBuf {
        self.get_bucket_confirmation_dir(bucket_id)
            .join("bucket_state.temp.dat")
    }

    /// Load bucket state from disk
    async fn load_bucket_state(&mut self, bucket_id: BucketId) {
        let current_path = self.get_current_state_path(bucket_id);
        let previous_path = self.get_previous_state_path(bucket_id);

        // Try loading from current file first
        if let Ok(state) = self.load_state_file(&current_path).await {
            self.buckets.insert(bucket_id, state.partition_states);
            return;
        }

        // Fall back to previous file if current is corrupted/missing
        if let Ok(state) = self.load_state_file(&previous_path).await {
            self.buckets.insert(bucket_id, state.partition_states);
            return;
        }

        // If neither file exists/works, start with empty state
        self.buckets.insert(bucket_id, HashMap::new());
    }

    /// Load a state file and deserialize it
    async fn load_state_file(
        &self,
        path: &Path,
    ) -> Result<BucketConfirmationState, ConfirmationError> {
        if !path.exists() {
            return Err(ConfirmationError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "State file not found",
            )));
        }

        let mut file = File::open(path).await?;
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).await?;

        let (checksummed, _): (ChecksummedState, _) =
            bincode::decode_from_slice(&contents, bincode::config::standard())?;
        checksummed.deserialize()
    }

    /// Persist bucket state if enough changes or time has passed
    pub async fn persist_bucket_if_needed(
        &mut self,
        bucket_id: BucketId,
    ) -> Result<bool, ConfirmationError> {
        if !self.buckets.contains_key(&bucket_id) {
            return Err(ConfirmationError::BucketNotFound(bucket_id));
        }

        let now = Instant::now();
        let should_persist = {
            let changes = self.changes_since_persist.get(&bucket_id).copied();
            let last_time = self.last_persist_times.get(&bucket_id);
            match changes.zip(last_time) {
                Some((changes, last_time)) => {
                    changes > 100 || last_time.elapsed() > Duration::from_secs(5)
                }
                None => true,
            }
        };

        if should_persist {
            self.persist_bucket_state(bucket_id).await?;

            // Update tracking values
            self.last_persist_times.insert(bucket_id, now);
            self.changes_since_persist.insert(bucket_id, 0);

            return Ok(true);
        }

        Ok(false)
    }

    /// Force persist state regardless of timing
    pub async fn persist_bucket_state(
        &mut self,
        bucket_id: BucketId,
    ) -> Result<(), ConfirmationError> {
        if !self.buckets.contains_key(&bucket_id) {
            return Err(ConfirmationError::BucketNotFound(bucket_id));
        }

        let partition_states = self.buckets.get(&bucket_id).unwrap().clone();

        // Create state object
        let state = BucketConfirmationState {
            version: 1,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::from_secs(0))
                .as_secs(),
            bucket_id,
            partition_states,
        };

        // Create checksummed state
        let checksummed = ChecksummedState::new(&state)?;
        let state_bincode = bincode::encode_to_vec(&checksummed, bincode::config::standard())?;

        // Write to temp file first
        let temp_path = self.get_temp_state_path(bucket_id);
        let current_path = self.get_current_state_path(bucket_id);
        let previous_path = self.get_previous_state_path(bucket_id);

        {
            let mut file = File::create(&temp_path).await?;
            file.write_all(&state_bincode).await?;
            file.sync_all().await?;
        }

        // If current file exists, make it the previous backup
        if current_path.exists() {
            if previous_path.exists() {
                fs::remove_file(&previous_path).await?;
            }
            fs::rename(&current_path, &previous_path).await?;
        }

        // Make temp file the current file
        fs::rename(&temp_path, &current_path).await?;

        info!("wrote bucket confirmations to disk");

        Ok(())
    }

    /// Update confirmation count for an event
    pub async fn update_confirmation(
        &mut self,
        partition_id: PartitionId,
        version: u64,
        confirmation_count: u8,
    ) -> Result<bool, ConfirmationError> {
        let bucket_id = self.get_bucket_for_partition(partition_id);

        // Ensure bucket exists
        let partition_states = self.buckets.entry(bucket_id).or_default();

        // Ensure partition exists
        let state = partition_states
            .entry(partition_id)
            .or_insert_with(|| PartitionConfirmationState::new(partition_id));

        // Update confirmation state
        let watermark_advanced =
            state.update_confirmation(version, confirmation_count, self.replication_factor);

        // Track changes for persistence
        let changes = self.changes_since_persist.entry(bucket_id).or_insert(0);
        *changes += 1;

        // Try to persist if needed
        self.persist_bucket_if_needed(bucket_id).await?;

        Ok(watermark_advanced)
    }

    /// Get confirmed watermark for a partition
    pub fn get_watermark(&self, partition_id: PartitionId) -> Option<&Arc<AtomicWatermark>> {
        let bucket_id = self.get_bucket_for_partition(partition_id);

        // Look up the watermark from in-memory state
        if let Some(partition_states) = self.buckets.get(&bucket_id)
            && let Some(state) = partition_states.get(&partition_id)
        {
            return Some(&state.confirmed_watermark);
        }

        // If not found, return 0 (no events confirmed)
        None
    }

    /// Get confirmed watermarks for all partitions
    pub fn get_watermarks(&self) -> HashMap<PartitionId, Arc<AtomicWatermark>> {
        self.buckets
            .values()
            .flat_map(|partition_states| {
                partition_states.iter().map(|(partition_id, state)| {
                    (*partition_id, Arc::clone(&state.confirmed_watermark))
                })
            })
            .collect()
    }

    /// Get confirmation gap (highest - watermark) for a partition
    pub fn get_confirmation_gap(&self, partition_id: PartitionId) -> u64 {
        let bucket_id = self.get_bucket_for_partition(partition_id);

        if let Some(partition_states) = self.buckets.get(&bucket_id)
            && let Some(state) = partition_states.get(&partition_id)
        {
            return state.confirmation_gap();
        }

        // If partition not found, return 0 (no gap)
        0
    }

    /// Get list of stuck events for a partition
    pub fn get_stuck_events(
        &self,
        partition_id: PartitionId,
        min_attempts: u8,
        min_age_secs: u64,
    ) -> Vec<u64> {
        let bucket_id = self.get_bucket_for_partition(partition_id);

        if let Some(partition_states) = self.buckets.get(&bucket_id)
            && let Some(state) = partition_states.get(&partition_id)
        {
            return state.get_stuck_events(min_attempts, min_age_secs);
        }

        // If partition not found, return empty list
        Vec::new()
    }

    /// Force a watermark update - used for admin recovery of stuck partitions
    pub async fn admin_force_watermark(
        &mut self,
        partition_id: PartitionId,
        new_watermark: u64,
    ) -> Result<(), ConfirmationError> {
        let bucket_id = self.get_bucket_for_partition(partition_id);

        // Ensure bucket exists
        let partition_states = self.buckets.entry(bucket_id).or_default();

        // Ensure partition exists
        let state = partition_states
            .entry(partition_id)
            .or_insert_with(|| PartitionConfirmationState::new(partition_id));

        // Only allow advancing the watermark, not moving it backwards
        if new_watermark > state.confirmed_watermark.get() {
            state.confirmed_watermark.advance(new_watermark);

            // Clean up unconfirmed events that are now below the watermark
            state
                .unconfirmed_events
                .retain(|&ver, _| ver > new_watermark);

            // Force persistence
            self.persist_bucket_state(bucket_id).await?;
        }

        // No change needed
        Ok(())
    }

    /// Skip a specific event version - used for admin recovery of corrupted
    /// events
    pub async fn admin_skip_event(
        &mut self,
        partition_id: PartitionId,
        version: u64,
    ) -> Result<bool, ConfirmationError> {
        let bucket_id = self.get_bucket_for_partition(partition_id);

        // Ensure bucket exists
        let partition_states = self.buckets.entry(bucket_id).or_default();

        // Ensure partition exists
        let state = partition_states
            .entry(partition_id)
            .or_insert_with(|| PartitionConfirmationState::new(partition_id));

        // Only allow skipping events above the current watermark
        let confirmed_watermark = state.confirmed_watermark.get();
        if version <= confirmed_watermark {
            return Ok(false);
        }

        // Remove the event from unconfirmed events
        state.unconfirmed_events.remove(&version);

        // Try to advance watermark
        let mut next_expected = confirmed_watermark + 1;
        let mut new_watermark = confirmed_watermark;

        // Find the highest contiguous confirmed version
        let required_quorum = (self.replication_factor / 2) + 1;

        loop {
            // Skip the version we're explicitly skipping
            if next_expected == version {
                new_watermark = next_expected;
                next_expected += 1;
                continue;
            }

            if let Some(event) = state.unconfirmed_events.get(&next_expected) {
                if event.confirmation_count >= required_quorum {
                    new_watermark = next_expected;
                    next_expected += 1;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        let watermark_advanced = new_watermark > confirmed_watermark;

        // Update watermark if it advanced
        if watermark_advanced {
            state.confirmed_watermark.advance(new_watermark);

            // Clean up unconfirmed events that are now below the watermark
            state
                .unconfirmed_events
                .retain(|&ver, _| ver > new_watermark);

            // Force persistence
            self.persist_bucket_state(bucket_id).await?;
        }

        Ok(watermark_advanced)
    }

    /// Validate state against event data
    pub async fn validate_against_events(
        &mut self,
        bucket_id: BucketId,
        event_validator: &(dyn Fn(PartitionId, u64) -> bool + Send + Sync),
    ) -> Result<ValidationReport, ConfirmationError> {
        if !self.buckets.contains_key(&bucket_id) {
            return Err(ConfirmationError::BucketNotFound(bucket_id));
        }

        let mut report = ValidationReport {
            bucket_id,
            partitions_checked: 0,
            watermarks_valid: 0,
            watermarks_adjusted: 0,
        };

        if let Some(partition_states) = self.buckets.get_mut(&bucket_id) {
            for (partition_id, state) in partition_states.iter_mut() {
                report.partitions_checked += 1;

                // Check the current watermark
                let mut watermark_valid = true;

                // Check if watermark event exists and is confirmed
                let confirmed_watermark = state.confirmed_watermark.get();
                if confirmed_watermark > 0 && !event_validator(*partition_id, confirmed_watermark) {
                    watermark_valid = false;

                    // Find the highest valid watermark
                    let mut new_watermark = confirmed_watermark;

                    while new_watermark > 0 && !event_validator(*partition_id, new_watermark) {
                        new_watermark -= 1;
                    }

                    // Update the watermark
                    state.confirmed_watermark.advance(new_watermark);

                    // Clean up unconfirmed events
                    state
                        .unconfirmed_events
                        .retain(|&ver, _| ver > new_watermark);

                    report.watermarks_adjusted += 1;
                }

                if watermark_valid {
                    report.watermarks_valid += 1;
                }
            }
        }

        // Force persistence if we made any adjustments
        if report.watermarks_adjusted > 0 {
            self.persist_bucket_state(bucket_id).await?;
        }

        Ok(report)
    }

    /// Get stats about the confirmation state
    pub fn get_stats(&self) -> ConfirmationStats {
        let mut stats = ConfirmationStats {
            bucket_count: 0,
            partition_count: 0,
            total_confirmed_events: 0,
            total_unconfirmed_events: 0,
            max_confirmation_gap: 0,
        };

        for partition_states in self.buckets.values() {
            stats.bucket_count += 1;

            for state in partition_states.values() {
                stats.partition_count += 1;
                stats.total_confirmed_events += state.confirmed_watermark.get();
                stats.total_unconfirmed_events += state.unconfirmed_events.len() as u64;

                let gap = state.confirmation_gap();
                if gap > stats.max_confirmation_gap {
                    stats.max_confirmation_gap = gap;
                }
            }
        }

        stats
    }
}

/// Report from validation against event data
#[derive(Debug)]
pub struct ValidationReport {
    pub bucket_id: BucketId,
    pub partitions_checked: usize,
    pub watermarks_valid: usize,
    pub watermarks_adjusted: usize,
}

/// Statistics about the confirmation state
#[derive(Debug, Clone, Copy, Reply, Serialize, Deserialize)]
pub struct ConfirmationStats {
    pub bucket_count: usize,
    pub partition_count: usize,
    pub total_confirmed_events: u64,
    pub total_unconfirmed_events: u64,
    pub max_confirmation_gap: u64,
}

#[derive(Debug, Encode, Decode)]
pub struct AtomicWatermark {
    value: AtomicU64,
}

impl AtomicWatermark {
    fn new(initial: u64) -> Self {
        Self {
            value: AtomicU64::new(initial),
        }
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Acquire)
    }

    fn advance(&self, new_value: u64) -> bool {
        loop {
            let current = self.value.load(Ordering::Acquire);
            if new_value <= current {
                return false;
            }

            match self.value.compare_exchange(
                current,
                new_value,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(_) => continue, // Another thread updated, try again
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use sierradb::database::DatabaseBuilder;
    use tempfile::{TempDir, tempdir};

    use super::*;

    async fn create_temp_db() -> (TempDir, Database) {
        let temp_dir = tempdir().expect("Failed to create temp directory");
        let db = DatabaseBuilder::new()
            .flush_interval_events(1)
            .total_buckets(4)
            .bucket_ids_from_range(0..4)
            .open(temp_dir.path())
            .expect("Failed to open database");
        (temp_dir, db)
    }

    #[test]
    fn test_partition_confirmation_basic() {
        let mut state = PartitionConfirmationState::new(42);

        // Add event confirmations
        assert_eq!(state.confirmed_watermark.get(), 0);

        // First event (v1) - watermark doesn't advance yet because count < quorum
        assert!(!state.update_confirmation(1, 1, 2));
        assert_eq!(state.confirmed_watermark.get(), 0); // Not enough confirmations yet

        // Confirm event (v1) with quorum - watermark should advance
        assert!(state.update_confirmation(1, 2, 2));
        assert_eq!(state.confirmed_watermark.get(), 1); // Now confirmed

        // Add multiple events, all with quorum
        assert!(state.update_confirmation(2, 2, 2));
        assert_eq!(state.confirmed_watermark.get(), 2);

        assert!(state.update_confirmation(3, 2, 2));
        assert_eq!(state.confirmed_watermark.get(), 3);

        // Ensure gaps prevent watermark advancement
        assert!(!state.update_confirmation(5, 2, 2));
        assert_eq!(state.confirmed_watermark.get(), 3); // Still 3 due to gap at v4

        // Fill the gap
        assert!(state.update_confirmation(4, 2, 2));
        assert_eq!(state.confirmed_watermark.get(), 5); // Now advances to 5
    }

    #[tokio::test]
    async fn test_confirmation_persistence() -> Result<(), ConfirmationError> {
        let (_temp_dir, db) = create_temp_db().await;
        let mut manager = BucketConfirmationManager::new(
            db.dir().clone(),
            4,
            2,
            HashSet::from_iter([0, 1, 2, 3]),
        );

        // Initialize
        manager.initialize(&db).await?;

        // Add some confirmations
        manager.update_confirmation(1, 1, 2).await?;
        manager.update_confirmation(1, 2, 2).await?;
        manager.update_confirmation(1, 3, 2).await?;

        assert_eq!(manager.get_watermark(1).unwrap().get(), 3);

        // Force persist
        let bucket_id = manager.get_bucket_for_partition(1);
        manager.persist_bucket_state(bucket_id).await?;

        // Create a new manager and check if state loaded
        let mut new_manager = BucketConfirmationManager::new(
            db.dir().clone(),
            4,
            2,
            HashSet::from_iter([0, 1, 2, 3]),
        );
        new_manager.initialize(&db).await?;

        // Check if state was loaded correctly
        assert_eq!(new_manager.get_watermark(1).unwrap().get(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_skip_event() -> Result<(), ConfirmationError> {
        let mut state = PartitionConfirmationState::new(42);

        // Add event confirmations
        state.update_confirmation(1, 2, 2);
        state.update_confirmation(2, 2, 2);
        state.update_confirmation(3, 1, 2); // Not enough confirmations
        state.update_confirmation(4, 2, 2);

        // Current watermark should be 2
        assert_eq!(state.confirmed_watermark.get(), 2);

        // Create manager to test skip functionality
        let (_temp_dir, db) = create_temp_db().await;
        let mut manager = BucketConfirmationManager::new(
            db.dir().clone(),
            4,
            2,
            HashSet::from_iter([0, 1, 2, 3]),
        );

        // Initialize and manually insert our test state
        manager.initialize(&db).await?;
        let bucket_id = manager.get_bucket_for_partition(42);
        manager
            .buckets
            .entry(bucket_id)
            .or_default()
            .insert(42, state);

        // Skip the unconfirmed event
        assert!(manager.admin_skip_event(42, 3).await?);

        // Watermark should now be 4
        assert_eq!(manager.get_watermark(42).unwrap().get(), 4);

        Ok(())
    }
}
