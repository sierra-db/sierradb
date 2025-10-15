//! Actor module for managing confirmation state in Eventus
//!
//! This module provides a Kameo actor wrapper around `BucketConfirmationManager`
//! to enable concurrent, fault-tolerant confirmation state management.
//!
//! ## Usage
//!
//! ```ignore
//! use std::path::PathBuf;
//! use actor::ConfirmationActor;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Spawn the confirmation actor
//!     let actor_ref = ConfirmationActor::spawn(
//!         PathBuf::from("/data"),
//!         16,  // num_buckets
//!         3,   // replication_factor
//!     );
//!
//!     // Update event confirmation
//!     let watermark_advanced = actor_ref
//!         .update_event_confirmation(1, 1, 2)
//!         .await?;
//!
//!     // Get current watermark
//!     let watermark = actor_ref
//!         .get_partition_watermark(1)
//!         .await?;
//!
//!     // Check system health
//!     let health = actor_ref.health_check().await?;
//!     println!("System healthy: {}", health.healthy);
//!
//!     Ok(())
//! }
//! ```

use std::sync::Arc;
use std::{collections::HashSet, path::PathBuf};

use kameo::prelude::*;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use super::{
    BucketConfirmationManager, BucketId, ConfirmationError, ConfirmationStats, PartitionId,
    ValidationReport,
};

/// Actor that manages confirmation state for buckets and partitions
///
/// This actor wraps `BucketConfirmationManager` to provide concurrent,
/// fault-tolerant access to confirmation state operations. It handles:
///
/// - Updating confirmation counts for events
/// - Tracking watermarks for partitions (highest contiguously confirmed event)
/// - Managing persistence of confirmation state
/// - Administrative operations for recovery scenarios
/// - Validation and statistics gathering
///
/// The actor ensures thread-safe access to the confirmation state and
/// provides asynchronous message-based API for distributed systems.
#[derive(Actor)]
pub struct ConfirmationActor {
    pub manager: BucketConfirmationManager,
}

impl ConfirmationActor {
    pub fn new(
        data_dir: PathBuf,
        num_buckets: u16,
        replication_factor: u8,
        assigned_partitions: HashSet<PartitionId>,
    ) -> Self {
        let mut manager = BucketConfirmationManager::new(
            data_dir,
            num_buckets,
            replication_factor,
            assigned_partitions,
        );
        manager.initialize();

        Self { manager }
    }
}

// Message types for different operations

/// Update confirmation count for an event
///
/// This message updates the confirmation count for a specific event version
/// in a partition. When enough confirmations are received (quorum), the
/// watermark may advance if there are no gaps.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateConfirmation {
    pub partition_id: PartitionId,
    pub versions: SmallVec<[u64; 4]>,
    pub confirmation_count: u8,
}

impl Message<UpdateConfirmation> for ConfirmationActor {
    type Reply = Result<SmallVec<[bool; 4]>, ConfirmationError>;

    async fn handle(
        &mut self,
        msg: UpdateConfirmation,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        msg.versions
            .into_iter()
            .map(|version| {
                self.manager.update_confirmation(
                    msg.partition_id,
                    version + 1,
                    msg.confirmation_count,
                )
            })
            .collect()
    }
}

/// Get confirmed watermark for a partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetWatermark {
    pub partition_id: PartitionId,
}

impl Message<GetWatermark> for ConfirmationActor {
    type Reply = Option<u64>;

    async fn handle(
        &mut self,
        msg: GetWatermark,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.manager
            .get_watermark(msg.partition_id)
            .map(|watermark| watermark.get())
    }
}

/// Get confirmation gap (highest version - watermark) for a partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetConfirmationGap {
    pub partition_id: PartitionId,
}

impl Message<GetConfirmationGap> for ConfirmationActor {
    type Reply = u64;

    async fn handle(
        &mut self,
        msg: GetConfirmationGap,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.manager.get_confirmation_gap(msg.partition_id)
    }
}

/// Get list of stuck events for a partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetStuckEvents {
    pub partition_id: PartitionId,
    pub min_attempts: u8,
    pub min_age_secs: u64,
}

impl Message<GetStuckEvents> for ConfirmationActor {
    type Reply = Vec<u64>;

    async fn handle(
        &mut self,
        msg: GetStuckEvents,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.manager
            .get_stuck_events(msg.partition_id, msg.min_attempts, msg.min_age_secs)
    }
}

/// Force watermark update for a partition (admin operation)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminForceWatermark {
    pub partition_id: PartitionId,
    pub new_watermark: u64,
}

impl Message<AdminForceWatermark> for ConfirmationActor {
    type Reply = Result<(), ConfirmationError>;

    async fn handle(
        &mut self,
        msg: AdminForceWatermark,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.manager
            .admin_force_watermark(msg.partition_id, msg.new_watermark)
    }
}

/// Skip a specific event version (admin operation)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminSkipEvent {
    pub partition_id: PartitionId,
    pub version: u64,
}

impl Message<AdminSkipEvent> for ConfirmationActor {
    type Reply = Result<bool, ConfirmationError>;

    async fn handle(
        &mut self,
        msg: AdminSkipEvent,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.manager.admin_skip_event(msg.partition_id, msg.version)
    }
}

/// Validate confirmation state against event data
pub struct ValidateAgainstEvents {
    pub bucket_id: BucketId,
    pub event_validator: Arc<dyn Fn(PartitionId, u64) -> bool + Send + Sync>,
}

impl Message<ValidateAgainstEvents> for ConfirmationActor {
    type Reply = Result<ValidationReport, ConfirmationError>;

    async fn handle(
        &mut self,
        msg: ValidateAgainstEvents,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.manager
            .validate_against_events(msg.bucket_id, msg.event_validator.as_ref())
    }
}

/// Get confirmation statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetStats;

impl Message<GetStats> for ConfirmationActor {
    type Reply = ConfirmationStats;

    async fn handle(
        &mut self,
        _msg: GetStats,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.manager.get_stats()
    }
}

/// Health check for the confirmation system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheck {
    pub max_acceptable_gap: u64,
    pub max_stuck_events: usize,
}

#[derive(Debug, Clone, Reply, Serialize, Deserialize)]
pub struct HealthStatus {
    pub healthy: bool,
    pub issues: Vec<String>,
    pub stats: ConfirmationStats,
}

impl Message<HealthCheck> for ConfirmationActor {
    type Reply = HealthStatus;

    async fn handle(
        &mut self,
        msg: HealthCheck,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let stats = self.manager.get_stats();
        let mut issues = Vec::new();

        // Check for excessive confirmation gaps
        if stats.max_confirmation_gap > msg.max_acceptable_gap {
            issues.push(format!(
                "High confirmation gap detected: {} (max acceptable: {})",
                stats.max_confirmation_gap, msg.max_acceptable_gap
            ));
        }

        // Check for excessive unconfirmed events
        if stats.total_unconfirmed_events > msg.max_stuck_events as u64 {
            issues.push(format!(
                "Too many unconfirmed events: {} (max acceptable: {})",
                stats.total_unconfirmed_events, msg.max_stuck_events
            ));
        }

        HealthStatus {
            healthy: issues.is_empty(),
            issues,
            stats,
        }
    }
}

/// Force persistence of bucket state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistBucketState {
    pub bucket_id: BucketId,
}

impl Message<PersistBucketState> for ConfirmationActor {
    type Reply = Result<(), ConfirmationError>;

    async fn handle(
        &mut self,
        msg: PersistBucketState,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.manager.persist_bucket_state(msg.bucket_id)
    }
}

/// Get watermarks for multiple partitions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetMultipleWatermarks {
    pub partition_ids: Vec<PartitionId>,
}

impl Message<GetMultipleWatermarks> for ConfirmationActor {
    type Reply = Vec<(PartitionId, Option<u64>)>;

    async fn handle(
        &mut self,
        msg: GetMultipleWatermarks,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        msg.partition_ids
            .into_iter()
            .map(|partition_id| {
                let watermark = self.manager.get_watermark(partition_id).map(|w| w.get());
                (partition_id, watermark)
            })
            .collect()
    }
}

// Convenience methods for common operations
impl ConfirmationActor {
    /// Spawn a new confirmation actor
    pub fn spawn(
        data_dir: PathBuf,
        num_buckets: u16,
        replication_factor: u8,
        assigned_partitions: HashSet<PartitionId>,
    ) -> ActorRef<Self> {
        let actor = Self::new(
            data_dir,
            num_buckets,
            replication_factor,
            assigned_partitions,
        );
        Actor::spawn(actor)
    }
}

/// Extension trait for ConfirmationActor ActorRef to provide convenient methods
#[allow(async_fn_in_trait)]
pub trait ConfirmationActorExt {
    /// Update confirmation for a single event
    async fn update_event_confirmation(
        &self,
        partition_id: PartitionId,
        versions: SmallVec<[u64; 4]>,
        confirmation_count: u8,
    ) -> Result<SmallVec<[bool; 4]>, SendError<UpdateConfirmation, ConfirmationError>>;

    /// Get the current watermark for a partition
    async fn get_partition_watermark(
        &self,
        partition_id: PartitionId,
    ) -> Result<Option<u64>, SendError<GetWatermark>>;

    /// Check if a partition has any stuck events
    async fn has_stuck_events(
        &self,
        partition_id: PartitionId,
    ) -> Result<bool, SendError<GetStuckEvents>>;

    /// Perform a health check with default thresholds
    async fn health_check(&self) -> Result<HealthStatus, SendError<HealthCheck>>;
}

impl ConfirmationActorExt for ActorRef<ConfirmationActor> {
    async fn update_event_confirmation(
        &self,
        partition_id: PartitionId,
        versions: SmallVec<[u64; 4]>,
        confirmation_count: u8,
    ) -> Result<SmallVec<[bool; 4]>, SendError<UpdateConfirmation, ConfirmationError>> {
        let msg = UpdateConfirmation {
            partition_id,
            versions,
            confirmation_count,
        };
        self.ask(msg).await
    }

    async fn get_partition_watermark(
        &self,
        partition_id: PartitionId,
    ) -> Result<Option<u64>, SendError<GetWatermark>> {
        let msg = GetWatermark { partition_id };
        self.ask(msg).await
    }

    async fn has_stuck_events(
        &self,
        partition_id: PartitionId,
    ) -> Result<bool, SendError<GetStuckEvents>> {
        let msg = GetStuckEvents {
            partition_id,
            min_attempts: 3,
            min_age_secs: 300, // 5 minutes
        };
        let stuck_events = self.ask(msg).await?;
        Ok(!stuck_events.is_empty())
    }

    async fn health_check(&self) -> Result<HealthStatus, SendError<HealthCheck>> {
        let msg = HealthCheck {
            max_acceptable_gap: 1000, // Default: max 1000 event gap
            max_stuck_events: 100,    // Default: max 100 stuck events
        };
        self.ask(msg).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;
    use tempfile::tempdir;
    use tokio;

    #[tokio::test]
    async fn test_confirmation_actor_basic_operations() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let actor_ref =
            ConfirmationActor::spawn(temp_dir.path().to_path_buf(), 4, 3, HashSet::new());

        // Test update confirmation using extension trait
        let watermark_advanced = actor_ref
            .update_event_confirmation(1, smallvec![1], 2)
            .await?;
        assert!(watermark_advanced.iter().all(|b| *b));

        // Test get watermark using extension trait
        let watermark = actor_ref.get_partition_watermark(1).await?;
        assert_eq!(watermark, Some(1));

        // Test get confirmation gap
        let gap = actor_ref
            .ask(GetConfirmationGap { partition_id: 1 })
            .await?;
        assert_eq!(gap, 0);

        // Test check for stuck events using extension trait
        let has_stuck = actor_ref.has_stuck_events(1).await?;
        assert!(!has_stuck);

        // Test get stats
        let stats = actor_ref.ask(GetStats).await?;
        assert!(stats.total_confirmed_events > 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_health_check() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let actor_ref =
            ConfirmationActor::spawn(temp_dir.path().to_path_buf(), 4, 3, HashSet::new());

        // Add some confirmations
        actor_ref
            .update_event_confirmation(1, smallvec![1], 2)
            .await?;
        actor_ref
            .update_event_confirmation(1, smallvec![2], 2)
            .await?;

        // Test health check
        let health = actor_ref.health_check().await?;
        assert!(health.healthy);
        assert!(health.issues.is_empty());
        assert!(health.stats.total_confirmed_events > 0);

        // Test custom health check with strict limits
        let strict_health = actor_ref
            .ask(HealthCheck {
                max_acceptable_gap: 0,
                max_stuck_events: 0,
            })
            .await?;

        // Should still be healthy since we have no gaps or stuck events
        assert!(strict_health.healthy);

        Ok(())
    }

    #[tokio::test]
    async fn test_extension_trait_methods() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let actor_ref =
            ConfirmationActor::spawn(temp_dir.path().to_path_buf(), 4, 3, HashSet::new());

        // Test convenience methods from extension trait
        let watermark_advanced = actor_ref
            .update_event_confirmation(5, smallvec![1], 2)
            .await?;
        assert!(watermark_advanced.iter().all(|b| *b));

        let watermark = actor_ref.get_partition_watermark(5).await?;
        assert_eq!(watermark, Some(1));

        let has_stuck = actor_ref.has_stuck_events(5).await?;
        assert!(!has_stuck);

        Ok(())
    }

    #[tokio::test]
    async fn test_admin_operations() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let actor_ref =
            ConfirmationActor::spawn(temp_dir.path().to_path_buf(), 4, 3, HashSet::new());

        // Set up some initial state
        actor_ref
            .ask(UpdateConfirmation {
                partition_id: 1,
                versions: smallvec![1],
                confirmation_count: 2,
            })
            .await?;

        actor_ref
            .ask(UpdateConfirmation {
                partition_id: 1,
                versions: smallvec![2],
                confirmation_count: 2,
            })
            .await?;

        // Test admin force watermark
        actor_ref
            .ask(AdminForceWatermark {
                partition_id: 1,
                new_watermark: 5,
            })
            .await?;

        let watermark = actor_ref.ask(GetWatermark { partition_id: 1 }).await?;
        assert_eq!(watermark, Some(5));

        Ok(())
    }
}
