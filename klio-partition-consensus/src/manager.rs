use std::collections::{HashMap, HashSet};
use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use arrayvec::ArrayVec;
use libp2p::PeerId;
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::messages::OwnershipMessage;
use crate::store::PartitionOwnershipStore;
use crate::{MAX_REPLICATION_FACTOR, distribute_partition};

// Timeout for considering a node inactive
// const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(3000); // 3 seconds

/// Manages static assignment of partitions to nodes
#[derive(Debug, Clone)]
pub struct PartitionManager {
    pub local_peer_id: PeerId,
    pub cluster_nodes: Vec<PeerId>,
    pub num_partitions: u16,
    pub replication_factor: u8,
    pub partition_owners: HashMap<u16, PeerId>, // Maps partition_id to owner node
    pub assigned_partitions: Box<[u16]>,        // Partitions assigned to this node
    pub store: PartitionOwnershipStore,
    pub heartbeat_timeout: Duration,

    // Track active nodes
    pub active_nodes: HashSet<PeerId>,
    pub node_heartbeats: HashMap<PeerId, Instant>,
}

impl PartitionManager {
    pub fn new(
        local_peer_id: PeerId,
        cluster_nodes: Vec<PeerId>,
        num_partitions: u16,
        replication_factor: u8,
        assigned_partitions: Box<[u16]>, // Explicitly assigned partitions
        store_path: Option<&str>,
        heartbeat_timeout: Duration,
    ) -> Self {
        // Load partition ownership store if path provided
        let store = match store_path {
            Some(path) => match PartitionOwnershipStore::load(path) {
                Ok(store) => store,
                Err(err) => {
                    error!("failed to load partition ownership store: {err}");
                    PartitionOwnershipStore::new()
                }
            },
            None => PartitionOwnershipStore::new(),
        };

        // Initialize all nodes as active
        let active_nodes: HashSet<PeerId> = cluster_nodes.iter().cloned().collect();

        // Initialize partition owners from store or create new
        let mut partition_owners = store.get_partition_owners();

        // Register our assigned partitions - this is critical!
        // Ensure our own partitions are always registered
        for &partition_id in &assigned_partitions {
            partition_owners.insert(partition_id, local_peer_id);
        }

        let mut manager = Self {
            local_peer_id,
            cluster_nodes,
            num_partitions,
            replication_factor,
            partition_owners,
            assigned_partitions,
            store,
            heartbeat_timeout,
            active_nodes,
            node_heartbeats: HashMap::new(),
        };

        // Save our partitions to the store
        manager
            .store
            .set_partition_owners(&manager.partition_owners);

        // Initialize heartbeats for all nodes
        let now = Instant::now();
        for node in &manager.cluster_nodes {
            manager.node_heartbeats.insert(*node, now);
        }

        manager
    }

    #[inline]
    pub fn distribute_partition(
        &self,
        partition_key: u16,
    ) -> ArrayVec<u16, MAX_REPLICATION_FACTOR> {
        distribute_partition(partition_key, self.num_partitions, self.replication_factor)
    }

    // /// Distributes a partition key across multiple partitions with the
    // /// specified replication factor.
    // ///
    // /// # Arguments
    // /// * `partition_key` - The input partition key
    // ///
    // /// # Returns
    // /// A vector of partition IDs where the data should be stored
    // pub fn distribute_partition(&self, partition_key: u16) -> Vec<u16> {
    //     // Validate inputs
    //     if self.num_partitions == 0 {
    //         return Vec::new();
    //     }

    //     if self.replication_factor == 0 {
    //         return Vec::new();
    //     }

    //     if self.replication_factor as u16 > self.num_partitions {
    //         error!("replication factor exceeds number of partitions");
    //         return Vec::new();
    //     }

    //     let mut result = Vec::with_capacity(self.replication_factor as usize);

    //     // Start with the primary partition (simple modulo distribution)
    //     let primary_partition = partition_key % self.num_partitions;
    //     result.push(primary_partition);

    //     // For additional replicas, use a deterministic distribution method
    //     // that ensures partitions are spread out
    //     if self.replication_factor > 1 {
    //         // Use a jump value that's relatively prime to num_partitions to
    // ensure         // we visit all partitions before repeating
    //         let jump = self.compute_jump();

    //         let mut current = primary_partition;
    //         for _ in 1..self.replication_factor {
    //             // Jump to the next partition, wrapping around if necessary
    //             current = (current + jump) % self.num_partitions;
    //             result.push(current);
    //         }
    //     }

    //     result
    // }

    // /// Computes a jump value that's relatively prime to num_partitions
    // /// This ensures we'll visit all partitions before repeating
    // fn compute_jump(&self) -> u16 {
    //     // A common approach is to use a value close to num_partitions/2
    //     // For even numbers, we use an odd number to ensure relative primality
    //     if self.num_partitions <= 2 {
    //         return 1;
    //     }

    //     let candidate = self.num_partitions / 2 + 1;

    //     // If num_partitions is even and candidate is even, add 1 to make it odd
    //     if self.num_partitions % 2 == 0 && candidate % 2 == 0 {
    //         return candidate + 1;
    //     }

    //     candidate
    // }

    /// Determine whether this node has the specified partition
    pub fn has_partition(&self, partition_id: u16) -> bool {
        self.assigned_partitions.contains(&partition_id)
    }

    /// Get the availability status of a partition
    pub fn is_partition_available(&self, partition_id: u16) -> bool {
        match self.partition_owners.get(&partition_id) {
            Some(owner) => self.active_nodes.contains(owner),
            None => false,
        }
    }

    /// Determine if a partition key is available on this node
    pub fn is_key_available_locally(&self, partition_key: u16) -> bool {
        let partition_ids =
            distribute_partition(partition_key, self.num_partitions, self.replication_factor);

        for &partition_id in &partition_ids {
            if self.has_partition(partition_id) {
                return true;
            }
        }

        false
    }

    /// Get a list of available partitions for a given key
    pub fn get_available_partitions_for_key(
        &self,
        partition_key: u16,
    ) -> ArrayVec<(u16, PeerId), MAX_REPLICATION_FACTOR> {
        let partition_ids =
            distribute_partition(partition_key, self.num_partitions, self.replication_factor);

        partition_ids
            .into_iter()
            .filter_map(|pid| match self.partition_owners.get(&pid) {
                Some(owner) if self.active_nodes.contains(owner) => Some((pid, *owner)),
                _ => None,
            })
            .collect()
    }

    /// Handle node coming online
    pub fn on_node_connected(
        &mut self,
        peer_id: PeerId,
        owned_partitions: &[u16],
    ) -> Option<OwnershipMessage> {
        info!("node connected: {:?}", peer_id);

        // Register this node as active
        self.active_nodes.insert(peer_id);
        self.node_heartbeats.insert(peer_id, Instant::now());

        // Add to cluster if not already known
        if !self.cluster_nodes.contains(&peer_id) {
            self.cluster_nodes.push(peer_id);
        }

        // Register the partitions this node owns
        for &partition_id in owned_partitions {
            // Only insert if it's not already assigned to us
            if !self.has_partition(partition_id) {
                self.partition_owners.insert(partition_id, peer_id);
            }
        }

        // Save to store
        self.store.set_partition_owners(&self.partition_owners);

        // Create ownership response message
        Some(OwnershipMessage::OwnershipResponse {
            partition_owners: self.partition_owners.clone(),
            active_nodes: self.active_nodes.clone(),
        })
    }

    /// Handle node going offline
    pub fn on_node_disconnected(&mut self, peer_id: PeerId) {
        info!("node disconnected: {:?}", peer_id);

        // Mark as inactive
        self.active_nodes.remove(&peer_id);
        self.node_heartbeats.remove(&peer_id);

        info!(
            "node removed from active set. Active nodes: {:?}",
            self.active_nodes
        );
    }

    /// Process a heartbeat from a node
    pub fn on_heartbeat(&mut self, peer_id: PeerId, owned_partitions: &[u16]) -> bool {
        // Update heartbeat timestamp
        self.node_heartbeats.insert(peer_id, Instant::now());

        let mut status_changed = false;

        // If the node wasn't active before, mark it active now
        if !self.active_nodes.contains(&peer_id) {
            info!("node {:?} is now active due to heartbeat", peer_id);
            self.active_nodes.insert(peer_id);
            status_changed = true;
        }

        // Update partition ownership, respecting our own assignments
        for &partition_id in owned_partitions {
            // Only update if it's not one of our partitions
            if !self.has_partition(partition_id) {
                self.partition_owners.insert(partition_id, peer_id);
            }
        }

        status_changed
    }

    /// Check for node failures based on heartbeat timeouts
    pub fn check_heartbeat_timeouts(&mut self) -> bool {
        let now = Instant::now();
        let mut status_changed = false;

        let timed_out_peers: Vec<PeerId> = self
            .node_heartbeats
            .iter()
            .filter(|&(peer_id, last_heartbeat)| {
                peer_id != &self.local_peer_id
                    && self.active_nodes.contains(peer_id)
                    && now.duration_since(*last_heartbeat) > self.heartbeat_timeout
            })
            .map(|(node, _)| *node)
            .collect();

        for peer_id in timed_out_peers {
            info!("node {:?} timed out - marking inactive", peer_id);
            self.active_nodes.remove(&peer_id);
            status_changed = true;
        }

        status_changed
    }

    /// Handle an ownership response message (synchronize state)
    pub fn handle_ownership_response(
        &mut self,
        partition_owners: &HashMap<u16, PeerId>,
        active_nodes: HashSet<PeerId>,
    ) {
        // Update our partition ownership map, while preserving our own assignments
        let mut new_partition_owners = HashMap::new();

        // First, add all our assigned partitions
        for &partition_id in &self.assigned_partitions {
            new_partition_owners.insert(partition_id, self.local_peer_id);
        }

        // Then, add other partitions from the response (skipping our own)
        for (&partition_id, owner) in partition_owners {
            if !self.has_partition(partition_id) {
                new_partition_owners.insert(partition_id, *owner);
            }
        }

        // Replace our owners map with the merged version
        self.partition_owners = new_partition_owners;

        // Update active nodes
        self.active_nodes = active_nodes;

        // Make sure we're considered active
        self.active_nodes.insert(self.local_peer_id);

        // Update heartbeats for all active nodes
        let now = Instant::now();
        for node in &self.active_nodes {
            self.node_heartbeats.insert(*node, now);
        }

        // Save to store
        self.store.set_partition_owners(&self.partition_owners);

        info!("updated partition ownership from remote information");
    }

    /// Ensure our own partitions are registered correctly
    pub fn ensure_local_partitions(&mut self) {
        for &partition_id in &self.assigned_partitions {
            self.partition_owners
                .insert(partition_id, self.local_peer_id);
        }
    }

    /// Save partition ownership state
    pub fn save_store(&self, path: &str) -> io::Result<()> {
        self.store.save(path)
    }

    /// Save partition ownership state
    pub fn save_store_spawn(&self, path: PathBuf) -> JoinHandle<io::Result<()>> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let res = store.save(path);
            if let Err(err) = &res {
                error!("failed to save partition ownership store: {err}");
            }
            res
        })
    }
}
