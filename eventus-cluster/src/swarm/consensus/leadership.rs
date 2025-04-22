use std::{
    collections::{HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    time::{Duration, Instant},
};

use kameo::prelude::*;
use libp2p::PeerId;
use rand::Rng;
use tokio::sync::mpsc;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::swarm::{Command, SwarmSender};

use super::{
    ConsensusError, ConsensusEvent, ConsensusMessage, LEADER_TIMEOUT_MS, LeadershipClaim,
    LeadershipConfirmation, NUM_PARTITIONS, PartitionLeaderInfo,
    gossip::ConsensusGossipActor,
    term_store::{GetTerm, LoadTerms, StoreTerm, TermStoreActor},
};

#[derive(Actor)]
pub struct LeadershipActor {
    swarm_tx: SwarmSender,
    /// Local node's peer ID
    local_peer_id: PeerId,
    /// Information about leadership for all partitions
    partition_leaders: HashMap<u16, PartitionLeaderInfo>,
    /// Partitions this node is currently the leader for
    led_partitions: HashSet<u16>,
    /// Partitions this node is currently attempting to become leader for
    pending_claims: HashMap<Uuid, LeadershipClaim>,
    /// Confirmations received for pending claims
    claim_confirmations: HashMap<Uuid, HashSet<PeerId>>,
    /// Reference to term store actor
    term_store_ref: ActorRef<TermStoreActor>,
    /// Reference to gossip actor (will be set after initialization)
    gossip_ref: Option<ActorRef<ConsensusGossipActor>>,
    /// Replication factor (for calculating quorum)
    replication_factor: u8,
    /// Number of buckets in the system
    num_buckets: u16,
    /// Channel for emitting consensus events to the swarm
    event_tx: Option<mpsc::UnboundedSender<ConsensusEvent>>,
    known_peers: HashSet<PeerId>,
    last_term_update: HashMap<u16, Instant>,
    term_update_cooldown: Duration,
    claim_attempt_count: HashMap<u16, u8>,
    last_leader_change: HashMap<u16, Instant>,
}

// Initialize message
pub struct Initialize {
    pub gossip_ref: ActorRef<ConsensusGossipActor>,
    pub event_tx: mpsc::UnboundedSender<ConsensusEvent>,
}

impl Message<Initialize> for LeadershipActor {
    type Reply = Result<(), ConsensusError>;

    async fn handle(
        &mut self,
        msg: Initialize,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let actor_ref = ctx.actor_ref();

        self.gossip_ref = Some(msg.gossip_ref);
        self.event_tx = Some(msg.event_tx);

        // Load terms from storage
        self.term_store_ref.ask(LoadTerms).await?;

        // Add a delay to stagger startups
        tokio::time::sleep(Duration::from_millis(100 + (rand::random::<u64>() % 900))).await;

        // Initialize leadership state for ALL partitions
        info!(
            "Initializing leadership actor, checking all {} partitions",
            NUM_PARTITIONS
        );

        for partition_id in 0..NUM_PARTITIONS {
            // Check all partitions even if we're not the leader
            let should_lead = self.should_be_leader_for(partition_id);
            info!(
                "Partition {}: should_be_leader={}, peer_id={}",
                partition_id, should_lead, self.local_peer_id
            );

            // Only claim if we should be leader
            if should_lead {
                let term = self
                    .term_store_ref
                    .ask(GetTerm { partition_id })
                    .await?
                    .unwrap_or(0);

                // Initialize leadership info
                self.partition_leaders.insert(
                    partition_id,
                    PartitionLeaderInfo {
                        partition_id,
                        term,
                        leader_id: self.local_peer_id,
                        confirmed: false,
                        last_heartbeat: Instant::now(),
                    },
                );

                // Claim leadership
                info!("Claiming leadership for partition {}", partition_id);
                self.claim_leadership(partition_id).await?;
            }
        }

        // Schedule periodic checks for all partitions
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            if let Err(e) = actor_ref.tell(ForceLeadershipCheck).await {
                error!("Failed to send forced leadership check: {}", e);
            }
        });

        Ok(())
    }
}

// Message to claim leadership for a partition
pub struct ClaimLeadership {
    pub partition_id: u16,
}

impl Message<ClaimLeadership> for LeadershipActor {
    type Reply = Result<(), ConsensusError>;

    async fn handle(
        &mut self,
        msg: ClaimLeadership,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.claim_leadership(msg.partition_id).await
    }
}

// Message to process a leadership claim from another node
pub struct ProcessClaim {
    pub claim: LeadershipClaim,
}

impl Message<ProcessClaim> for LeadershipActor {
    type Reply = Result<(), ConsensusError>;

    async fn handle(
        &mut self,
        msg: ProcessClaim,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let partition_id = msg.claim.partition_id;
        let current_info = self.partition_leaders.get(&partition_id);

        // Check if we know about a higher term
        if let Some(info) = current_info {
            if info.term > msg.claim.term {
                // We know about a higher term, reject the claim
                debug!("claim term is too low, rejecting");
                return Ok(());
            }
        }

        // Update our knowledge about this partition's leadership
        self.partition_leaders.insert(
            partition_id,
            PartitionLeaderInfo {
                partition_id,
                term: msg.claim.term,
                leader_id: msg.claim.node_id,
                confirmed: false, // Not confirmed yet
                last_heartbeat: Instant::now(),
            },
        );

        // If we were the leader for this partition, we're not anymore
        if self.led_partitions.contains(&partition_id) {
            self.led_partitions.remove(&partition_id);
            debug!("leadership lost");
            if let Some(tx) = &self.event_tx {
                tx.send(ConsensusEvent::LostLeadership { partition_id })?;
            }
        }

        // If we're responsible for this partition (replication), send confirmation
        if self.is_responsible_for(partition_id) {
            let confirmation = LeadershipConfirmation {
                partition_id,
                term: msg.claim.term,
                leader_id: msg.claim.node_id,
                confirming_node: self.local_peer_id,
                claim_id: msg.claim.claim_id,
            };

            // Persist the term
            self.term_store_ref
                .tell(StoreTerm {
                    partition_id,
                    term: msg.claim.term,
                })
                .await?;

            // Send confirmation via gossip
            debug!("confirmed claim");
            let _ = self.swarm_tx.send(Command::PublishConsensusMessage {
                message: ConsensusMessage::Confirmation(confirmation),
            });
        }

        Ok(())
    }
}

// Message to process a leadership confirmation
pub struct ProcessConfirmation {
    pub confirmation: LeadershipConfirmation,
}

impl Message<ProcessConfirmation> for LeadershipActor {
    type Reply = Result<(), ConsensusError>;

    async fn handle(
        &mut self,
        msg: ProcessConfirmation,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let partition_id = msg.confirmation.partition_id;

        // If this is for a claim we made
        if let Some(claim) = self.pending_claims.get(&msg.confirmation.claim_id) {
            if claim.partition_id == partition_id && claim.term == msg.confirmation.term {
                // Add this confirmation
                if let Some(confirmations) =
                    self.claim_confirmations.get_mut(&msg.confirmation.claim_id)
                {
                    confirmations.insert(msg.confirmation.confirming_node);

                    // Check if we have enough confirmations for a quorum
                    if confirmations.len() >= self.quorum_size() {
                        // Mark as confirmed
                        if let Some(info) = self.partition_leaders.get_mut(&partition_id) {
                            info.confirmed = true;
                        }

                        // Add to our led partitions
                        self.led_partitions.insert(partition_id);

                        // Persist the term
                        self.term_store_ref
                            .tell(StoreTerm {
                                partition_id,
                                term: msg.confirmation.term,
                            })
                            .await?;

                        // Reset claim attempt counter
                        self.claim_attempt_count.insert(partition_id, 0);

                        // Emit an event that we became leader
                        if let Some(tx) = &self.event_tx {
                            tx.send(ConsensusEvent::BecameLeader {
                                partition_id,
                                term: msg.confirmation.term,
                            })?;
                        }

                        // Clean up the pending claim
                        self.pending_claims.remove(&msg.confirmation.claim_id);
                        self.claim_confirmations.remove(&msg.confirmation.claim_id);
                    }
                }
            }
        }
        // Or if this is confirming another node's leadership
        else if let Some(info) = self.partition_leaders.get_mut(&partition_id) {
            if info.leader_id == msg.confirmation.leader_id && info.term == msg.confirmation.term {
                // Update the last heartbeat
                info.last_heartbeat = Instant::now();

                // If this is a new leader, emit an event
                if !info.confirmed {
                    info.confirmed = true;

                    if let Some(tx) = &self.event_tx {
                        tx.send(ConsensusEvent::LeaderChanged {
                            partition_id,
                            new_leader: msg.confirmation.leader_id,
                            term: msg.confirmation.term,
                        })?;
                    }
                }
            }
        }

        Ok(())
    }
}

// Message to process a heartbeat from a leader
pub struct ProcessHeartbeat {
    pub partition_id: u16,
    pub term: u64,
    pub leader_id: PeerId,
}

impl Message<ProcessHeartbeat> for LeadershipActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: ProcessHeartbeat,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let now = Instant::now();

        // Only process a heartbeat if we aren't the claimed leader for this partition
        // This prevents oscillation from hearbeat processing
        if self.led_partitions.contains(&msg.partition_id) && msg.leader_id != self.local_peer_id {
            tracing::debug!(
                "Ignoring heartbeat for partition {} as we are the leader",
                msg.partition_id
            );
            return;
        }

        if let Some(info) = self.partition_leaders.get_mut(&msg.partition_id) {
            // Only process heartbeats with same or higher term
            if info.term <= msg.term {
                // Update the last heartbeat time regardless
                info.last_heartbeat = now;

                // If leadership information has actually changed
                let leadership_changed = info.leader_id != msg.leader_id || info.term != msg.term;

                // If leadership changed, update our records
                if leadership_changed {
                    tracing::debug!(
                        "Leadership changed from {:?}/{} to {:?}/{}",
                        info.leader_id,
                        info.term,
                        msg.leader_id,
                        msg.term
                    );

                    info.leader_id = msg.leader_id;
                    info.term = msg.term;
                    info.confirmed = true; // Consider heartbeats as confirmed leadership

                    // Check if we need to emit a leader changed event
                    // Ensure we don't spam events by checking the last time we emitted one
                    let last_change_time = self
                        .last_leader_change
                        .get(&msg.partition_id)
                        .copied()
                        .unwrap_or_else(|| Instant::now() - Duration::from_secs(10));

                    if now.duration_since(last_change_time) > Duration::from_secs(3) {
                        self.last_leader_change.insert(msg.partition_id, now);

                        // Emit an event that the leader changed
                        if let Some(tx) = &self.event_tx {
                            let _ = tx.send(ConsensusEvent::LeaderChanged {
                                partition_id: msg.partition_id,
                                new_leader: msg.leader_id,
                                term: msg.term,
                            });
                        }
                    }
                }
            }
        } else {
            // First time we've seen this partition
            self.partition_leaders.insert(
                msg.partition_id,
                PartitionLeaderInfo {
                    partition_id: msg.partition_id,
                    term: msg.term,
                    leader_id: msg.leader_id,
                    confirmed: true,
                    last_heartbeat: now,
                },
            );

            self.last_leader_change.insert(msg.partition_id, now);
        }
    }
}

// Message to check for failed leaders and claim leadership if needed
pub struct CheckFailedLeaders;

impl Message<CheckFailedLeaders> for LeadershipActor {
    type Reply = Result<(), ConsensusError>;

    async fn handle(
        &mut self,
        _: CheckFailedLeaders,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let now = Instant::now();
        let timeout = Duration::from_millis(LEADER_TIMEOUT_MS);

        let mut partitions_to_claim = Vec::new();

        // Check all partitions for failed leaders
        for (partition_id, info) in &self.partition_leaders {
            if now.duration_since(info.last_heartbeat) > timeout {
                // This leader has failed
                if self.should_be_leader_for(*partition_id) {
                    // We should be the leader for this partition
                    partitions_to_claim.push(*partition_id);
                }
            }
        }

        // Claim leadership for partitions with failed leaders
        for partition_id in partitions_to_claim {
            self.claim_leadership(partition_id).await?;
        }

        Ok(())
    }
}

// Add this new message type
pub struct ForceLeadershipCheck;

impl Message<ForceLeadershipCheck> for LeadershipActor {
    type Reply = Result<(), ConsensusError>;

    async fn handle(
        &mut self,
        _: ForceLeadershipCheck,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        tracing::info!("Performing forced leadership check for all partitions");

        for partition_id in 0..NUM_PARTITIONS {
            let should_lead = self.should_be_leader_for(partition_id);

            if should_lead && !self.led_partitions.contains(&partition_id) {
                tracing::info!("Will claim leadership for partition {}", partition_id);

                // Try to claim leadership for this partition
                self.claim_attempt_count.insert(partition_id, 0);
                self.claim_leadership(partition_id).await?;
            } else if !should_lead && self.led_partitions.contains(&partition_id) {
                tracing::info!("Will release leadership for partition {}", partition_id);

                // We shouldn't be leader anymore, but we are - update internal state
                self.led_partitions.remove(&partition_id);

                // Optional: Notify others we're abdicating
                // This could help speed up the transition
            }
        }

        Ok(())
    }
}

// Message to send heartbeats for partitions we're the leader of
pub struct SendHeartbeats;

impl Message<SendHeartbeats> for LeadershipActor {
    type Reply = Result<(), ConsensusError>;

    async fn handle(
        &mut self,
        _: SendHeartbeats,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        for partition_id in &self.led_partitions {
            if let Some(info) = self.partition_leaders.get(partition_id) {
                let heartbeat = ConsensusMessage::Heartbeat {
                    partition_id: *partition_id,
                    term: info.term,
                    leader_id: self.local_peer_id,
                };

                let _ = self
                    .swarm_tx
                    .send(Command::PublishConsensusMessage { message: heartbeat });
            }
        }

        Ok(())
    }
}

// Query to check if this node is leader for a partition
pub struct IsLeader {
    pub partition_id: u16,
}

impl Message<IsLeader> for LeadershipActor {
    type Reply = bool;

    async fn handle(
        &mut self,
        msg: IsLeader,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.led_partitions.contains(&msg.partition_id)
    }
}

// Query to get the leader for a partition
pub struct GetLeader {
    pub partition_id: u16,
}

impl Message<GetLeader> for LeadershipActor {
    type Reply = Option<PeerId>;

    async fn handle(
        &mut self,
        msg: GetLeader,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.partition_leaders
            .get(&msg.partition_id)
            .map(|info| info.leader_id)
    }
}

pub struct AddKnownPeer {
    pub peer_id: PeerId,
}

impl Message<AddKnownPeer> for LeadershipActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: AddKnownPeer,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.known_peers.insert(msg.peer_id);
    }
}

pub struct RemoveKnownPeer {
    pub peer_id: PeerId,
}

impl Message<RemoveKnownPeer> for LeadershipActor {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: RemoveKnownPeer,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.known_peers.remove(&msg.peer_id);
    }
}

pub struct PrintDiagnostics;

impl Message<PrintDiagnostics> for LeadershipActor {
    type Reply = ();

    async fn handle(&mut self, _: PrintDiagnostics, _ctx: &mut Context<Self, Self::Reply>) {
        tracing::info!("=== LEADERSHIP DIAGNOSTICS ===");
        tracing::info!("Local peer ID: {}", self.local_peer_id);
        tracing::info!(
            "Currently leading {} partitions: {:?}",
            self.led_partitions.len(),
            self.led_partitions
        );

        tracing::info!("Partition leader info:");
        for (partition_id, info) in &self.partition_leaders {
            tracing::info!(
                "Partition {}: Leader={}, Term={}, Confirmed={}, Last heartbeat={:?} ago",
                partition_id,
                info.leader_id,
                info.term,
                info.confirmed,
                Instant::now().duration_since(info.last_heartbeat)
            );
        }

        tracing::info!("Pending claims: {}", self.pending_claims.len());
        tracing::info!("===============================");
    }
}

// Implementation of helper methods for the LeadershipActor
impl LeadershipActor {
    pub fn new(
        swarm_tx: SwarmSender,
        local_peer_id: PeerId,
        term_store_ref: ActorRef<TermStoreActor>,
        replication_factor: u8,
        num_buckets: u16,
    ) -> Self {
        LeadershipActor {
            swarm_tx,
            local_peer_id,
            partition_leaders: HashMap::new(),
            led_partitions: HashSet::new(),
            pending_claims: HashMap::new(),
            claim_confirmations: HashMap::new(),
            term_store_ref,
            gossip_ref: None,
            replication_factor,
            num_buckets,
            event_tx: None,
            known_peers: HashSet::from_iter([local_peer_id]),
            last_term_update: HashMap::new(),
            term_update_cooldown: Duration::from_secs(5),
            claim_attempt_count: HashMap::new(),
            last_leader_change: HashMap::new(),
        }
    }

    /// Claim leadership for a partition
    async fn claim_leadership(&mut self, partition_id: u16) -> Result<(), ConsensusError> {
        // Make sure we don't wait too long due to backoff
        let now = Instant::now();
        let attempt_count = self.claim_attempt_count.entry(partition_id).or_insert(0);

        // Maximum backoff of 8 seconds to prevent excessive delays
        let max_backoff = 8;
        let backoff_seconds = (*attempt_count).min(3); // Cap at 8 seconds max (2^3)

        if backoff_seconds > 0 {
            info!(
                "Backing off for {} seconds before claiming leadership for partition {}",
                backoff_seconds, partition_id
            );
            tokio::time::sleep(Duration::from_secs(backoff_seconds as u64)).await;
        }

        *attempt_count += 1;

        // Update last term update time
        self.last_term_update.insert(partition_id, now);

        // Get the current term, or 0 if not known
        let current_term = self
            .term_store_ref
            .ask(GetTerm { partition_id })
            .await?
            .unwrap_or(0);
        let new_term = current_term + 1;

        // Create a claim with a new UUID
        let claim = LeadershipClaim {
            partition_id,
            term: new_term,
            node_id: self.local_peer_id,
            claim_id: Uuid::new_v4(),
        };

        // Store the claim
        let claim_id = claim.claim_id;
        self.pending_claims.insert(claim_id, claim.clone());
        self.claim_confirmations.insert(claim_id, HashSet::new());

        // Update our local state
        self.partition_leaders.insert(
            partition_id,
            PartitionLeaderInfo {
                partition_id,
                term: new_term,
                leader_id: self.local_peer_id,
                confirmed: false,
                last_heartbeat: Instant::now(),
            },
        );

        // Send the claim via gossip
        let _ = self.swarm_tx.send(Command::PublishConsensusMessage {
            message: ConsensusMessage::Claim(claim),
        });

        Ok(())
    }

    /// Determine if this node should be the leader for a partition
    fn should_be_leader_for(&self, partition_id: u16) -> bool {
        if self.known_peers.len() < 3 {
            // With fewer than 3 known peers, don't try to be leader yet
            return false;
        }

        // Get a sorted list of all known peer IDs in the system
        // This ensures everyone has the same view of node ordering
        let mut known_peers: Vec<_> = self.known_peers.iter().collect();

        // Sort for consistent ordering across all nodes
        known_peers.sort();

        if known_peers.is_empty() {
            return false;
        }

        // For testing, print the peers so we can confirm they're the same
        tracing::info!("Known sorted peers: {:?}", known_peers);

        // Assign partitions to peers in a round-robin fashion
        // This ensures a consistent assignment even with only 1 partition
        let peer_index = partition_id as usize % known_peers.len();
        let assigned_peer = &known_peers[peer_index];

        // Check if this node is the assigned leader
        let is_leader = *assigned_peer == &self.local_peer_id;

        tracing::info!(
            "Partition {} assigned to peer {}: is_leader={}",
            partition_id,
            assigned_peer,
            is_leader
        );

        is_leader
    }

    /// Check if this node is responsible for a partition (for replication)
    fn is_responsible_for(&self, partition_id: u16) -> bool {
        // Create a deterministic hash based on the partition ID and peer ID
        let peer_id_bytes = self.local_peer_id.to_bytes();
        let mut hasher = DefaultHasher::new();

        // Hash both the partition ID and peer ID bytes
        partition_id.hash(&mut hasher);
        for byte in &peer_id_bytes[0..4] {
            byte.hash(&mut hasher);
        }

        // A node is "responsible" for a partition if it's either the
        // primary leader or one of the backup nodes
        let hash_value = hasher.finish();
        hash_value % self.replication_factor as u64 <= (self.replication_factor / 2) as u64
    }

    /// Get the quorum size based on replication factor
    fn quorum_size(&self) -> usize {
        (self.replication_factor as usize / 2) + 1
    }
}
