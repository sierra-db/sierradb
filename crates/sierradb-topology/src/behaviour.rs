use std::collections::{HashMap, HashSet, VecDeque};
use std::task;
use std::time::Duration;

use arrayvec::ArrayVec;
use libp2p::core::Endpoint;
use libp2p::core::transport::PortUse;
use libp2p::gossipsub::{self, IdentTopic, PublishError};
use libp2p::swarm::{
    ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, THandler, THandlerInEvent,
    THandlerOutEvent, ToSwarm,
};
use libp2p::{Multiaddr, PeerId};
use sierradb::MAX_REPLICATION_FACTOR;
use sierradb::bucket::PartitionId;
use tokio::time::Interval;
use tracing::{debug, error, info, trace, warn};

use crate::ClusterKey;

use super::manager::TopologyManager;
use super::messages::{Heartbeat, OwnershipMessage};

// Topic names
const HEARTBEAT_TOPIC: &str = "sierra/heartbeat";
const OWNERSHIP_TOPIC: &str = "sierra/ownership";

pub struct Behaviour<T: ClusterKey> {
    pub gossipsub: gossipsub::Behaviour,
    pub manager: TopologyManager<T>,
    pub heartbeat_interval: Interval,
    pub timeout_check_interval: Interval,
    pub heartbeat_topic: gossipsub::IdentTopic,
    pub ownership_topic: gossipsub::IdentTopic,
    pub pending_events: VecDeque<PartitionBehaviourEvent<T>>,
    pub heartbeat_bytes: Box<[u8]>,
}

impl<T: ClusterKey> Behaviour<T> {
    pub fn new(
        mut gossipsub: gossipsub::Behaviour,
        manager: TopologyManager<T>,
        heartbeat_interval_duration: Duration,
    ) -> Self {
        // Set up intervals for periodic tasks
        let heartbeat_interval = tokio::time::interval(heartbeat_interval_duration);
        let timeout_check_interval = tokio::time::interval(heartbeat_interval_duration / 2);

        // Create topics
        let heartbeat_topic = IdentTopic::new(HEARTBEAT_TOPIC);
        let ownership_topic = IdentTopic::new(OWNERSHIP_TOPIC);

        // Subscribe to both topics
        let _ = gossipsub.subscribe(&heartbeat_topic);
        let _ = gossipsub.subscribe(&ownership_topic);

        // Pre-encode heartbeat message
        let heartbeat_bytes = bincode::encode_to_vec(
            Heartbeat {
                cluster_ref: bincode::serde::Compat(manager.local_cluster_ref.clone()),
                owned_partitions: manager.assigned_partitions.clone(),
                alive_since: manager.alive_since,
                node_index: manager.local_node_index,
                total_node_count: manager.total_node_count,
            },
            bincode::config::standard(),
        )
        .unwrap()
        .into();

        info!(
            "node {} owns {}/{} partitions as replicas",
            manager.local_node_index,
            manager.assigned_partitions.len(),
            manager.num_partitions,
        );
        trace!(
            "node {} is replica for partitions: {:?}",
            manager.local_node_index, manager.assigned_partitions,
        );

        Self {
            gossipsub,
            manager,
            heartbeat_interval,
            timeout_check_interval,
            heartbeat_topic,
            ownership_topic,
            pending_events: VecDeque::new(),
            heartbeat_bytes,
        }
    }

    pub fn add_explicit_peer(&mut self, peer_id: PeerId) {
        // Add peer to gossipsub
        self.gossipsub.add_explicit_peer(&peer_id);

        // Send an ownership request to the new peer
        let request = OwnershipMessage::OwnershipRequest {
            cluster_ref: self.manager.local_cluster_ref.clone(),
            owned_partitions: self.manager.assigned_partitions.clone(),
            alive_since: self.manager.alive_since,
            node_index: self.manager.local_node_index,
            total_node_count: self.manager.total_node_count,
        };

        self.send_ownership_message(request);
    }

    fn handle_gossipsub_event(&mut self, event: &gossipsub::Event) {
        if let gossipsub::Event::Message { message, .. } = event {
            match message.topic.as_str() {
                HEARTBEAT_TOPIC => {
                    self.handle_heartbeat_message(&message.data);
                }
                OWNERSHIP_TOPIC => {
                    self.handle_ownership_message(&message.data);
                }
                topic => {
                    warn!("received message on unknown topic: {topic}")
                }
            }
        }
    }

    fn handle_heartbeat_message(&mut self, data: &[u8]) {
        match bincode::decode_from_slice::<Heartbeat<bincode::serde::Compat<T>>, _>(
            data,
            bincode::config::standard(),
        ) {
            Ok((
                Heartbeat {
                    cluster_ref: bincode::serde::Compat(cluster_ref),
                    owned_partitions,
                    alive_since,
                    node_index,
                    total_node_count,
                },
                _,
            )) => {
                let cluster_id = cluster_ref.id();
                trace!("received heartbeat from {cluster_id} (index: {node_index})");

                // Skip our own heartbeats
                if cluster_ref.id() == self.manager.local_cluster_ref.id() {
                    return;
                }

                // Check if the node was previously inactive
                let was_inactive = !self
                    .manager
                    .active_nodes
                    .contains_key(cluster_ref.id().peer_id().unwrap());

                // Update heartbeat and ownership
                let status_changed = self.manager.on_heartbeat(
                    cluster_ref.clone(),
                    &owned_partitions,
                    alive_since,
                    node_index,
                    total_node_count,
                );

                // Make sure our own partitions are still registered
                self.manager.ensure_local_partitions();

                // If this is a new node, or it was inactive
                if status_changed || was_inactive {
                    info!(
                        "node {cluster_id} (index: {node_index}) is now active with {} replica partitions",
                        owned_partitions.len()
                    );

                    self.pending_events
                        .push_back(PartitionBehaviourEvent::NodeStatusChanged {
                            peer_id: *cluster_ref.id().peer_id().unwrap(),
                            is_active: true,
                            owned_partitions: owned_partitions.clone(),
                        });

                    self.pending_events.push_back(
                        PartitionBehaviourEvent::ReplicaAssignmentUpdated {
                            partition_replicas: self.manager.partition_replicas.clone(),
                            active_nodes: self.manager.active_nodes.clone(),
                        },
                    );
                }
            }
            Err(err) => {
                error!("failed to decode heartbeat message: {}", err);
            }
        }
    }

    fn handle_ownership_message(&mut self, data: &[u8]) {
        match bincode::decode_from_slice::<OwnershipMessage<bincode::serde::Compat<T>>, _>(
            data,
            bincode::config::standard(),
        ) {
            Ok((msg, _)) => {
                self.handle_partition_message(msg.map(|bincode::serde::Compat(key)| key));
            }
            Err(err) => {
                error!("failed to decode ownership message: {err}");
            }
        }
    }

    fn handle_partition_message(&mut self, message: OwnershipMessage<T>) {
        match message {
            OwnershipMessage::OwnershipRequest {
                cluster_ref,
                owned_partitions,
                alive_since,
                node_index,
                total_node_count,
            } => {
                // Skip our own requests
                if cluster_ref.id() == self.manager.local_cluster_ref.id() {
                    return;
                }

                info!(
                    "received ownership request from {} (index: {}) with {} replica partitions",
                    cluster_ref.id(),
                    node_index,
                    owned_partitions.len()
                );

                // Process the node connection
                if let Some(response) = self.manager.on_node_connected(
                    cluster_ref.clone(),
                    &owned_partitions,
                    alive_since,
                    node_index,
                    total_node_count,
                ) {
                    // Send a response with our full replica assignment information
                    self.send_ownership_message(response);

                    // Notify application
                    self.pending_events
                        .push_back(PartitionBehaviourEvent::NodeStatusChanged {
                            peer_id: *cluster_ref.id().peer_id().unwrap(),
                            is_active: true,
                            owned_partitions: owned_partitions.clone(),
                        });

                    self.pending_events.push_back(
                        PartitionBehaviourEvent::ReplicaAssignmentUpdated {
                            partition_replicas: self.manager.partition_replicas.clone(),
                            active_nodes: self.manager.active_nodes.clone(),
                        },
                    );
                }
            }
            OwnershipMessage::OwnershipResponse {
                partition_replicas,
                active_nodes,
            } => {
                info!(
                    "received ownership response with {} partition replica assignments and {} active nodes",
                    partition_replicas.len(),
                    active_nodes.len()
                );

                // Convert the response format back to our internal format
                let partition_replicas = partition_replicas
                    .into_iter()
                    .flat_map(|(cluster_ref, partition_ids)| {
                        partition_ids
                            .into_iter()
                            .map(move |partition_id| (partition_id, cluster_ref.clone()))
                    })
                    .fold(
                        HashMap::<PartitionId, ArrayVec<T, MAX_REPLICATION_FACTOR>>::new(),
                        |mut acc, (partition_id, cluster_ref)| {
                            acc.entry(partition_id).or_default().push(cluster_ref);
                            acc
                        },
                    );

                // Update our replica assignments from the response
                self.manager
                    .handle_ownership_response(&partition_replicas, active_nodes.clone());

                // Make sure our partitions are still registered correctly
                self.manager.ensure_local_partitions();

                // Notify application
                self.pending_events
                    .push_back(PartitionBehaviourEvent::ReplicaAssignmentUpdated {
                        partition_replicas: self.manager.partition_replicas.clone(),
                        active_nodes: self.manager.active_nodes.clone(),
                    });
            }
        }
    }

    fn send_ownership_message(&mut self, message: OwnershipMessage<T>) {
        match bincode::encode_to_vec(
            message.map(bincode::serde::Compat),
            bincode::config::standard(),
        ) {
            Ok(encoded) => {
                if let Err(err) = self.gossipsub.publish(self.ownership_topic.hash(), encoded) {
                    if !matches!(err, PublishError::NoPeersSubscribedToTopic) {
                        error!("error publishing ownership message: {err}");
                    }
                }
            }
            Err(err) => {
                error!("failed to encode ownership message: {err}");
            }
        }
    }

    fn send_heartbeat(&mut self) {
        // Always make sure our partitions are still registered correctly
        self.manager.ensure_local_partitions();

        trace!(
            "sending heartbeat with {} replica partitions",
            self.manager.assigned_partitions.len()
        );

        if let Err(err) = self
            .gossipsub
            .publish(self.heartbeat_topic.hash(), self.heartbeat_bytes.clone())
        {
            if !matches!(err, PublishError::NoPeersSubscribedToTopic) {
                error!("error publishing heartbeat: {err}");
            }
        }
    }
}

#[derive(Debug)]
pub enum PartitionBehaviourEvent<T> {
    Gossipsub(<gossipsub::Behaviour as NetworkBehaviour>::ToSwarm),
    NodeStatusChanged {
        peer_id: PeerId,
        is_active: bool,
        owned_partitions: HashSet<PartitionId>, // Partitions this node is a replica for
    },
    ReplicaAssignmentUpdated {
        partition_replicas: HashMap<PartitionId, ArrayVec<T, MAX_REPLICATION_FACTOR>>,
        active_nodes: HashMap<PeerId, (u64, usize)>, // PeerId -> (alive_since, node_index)
    },
}

impl<T: ClusterKey> NetworkBehaviour for Behaviour<T> {
    type ConnectionHandler = THandler<gossipsub::Behaviour>;
    type ToSwarm = PartitionBehaviourEvent<T>;

    fn handle_pending_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        NetworkBehaviour::handle_pending_inbound_connection(
            &mut self.gossipsub,
            connection_id,
            local_addr,
            remote_addr,
        )
    }

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        debug!("inbound connection established from {}", peer);
        self.gossipsub.handle_established_inbound_connection(
            connection_id,
            peer,
            local_addr,
            remote_addr,
        )
    }

    fn handle_pending_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        maybe_peer: Option<PeerId>,
        addresses: &[Multiaddr],
        effective_role: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        NetworkBehaviour::handle_pending_outbound_connection(
            &mut self.gossipsub,
            connection_id,
            maybe_peer,
            addresses,
            effective_role,
        )
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        addr: &Multiaddr,
        role_override: Endpoint,
        port_use: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        debug!("outbound connection established to {}", peer);
        self.gossipsub.handle_established_outbound_connection(
            connection_id,
            peer,
            addr,
            role_override,
            port_use,
        )
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        match &event {
            FromSwarm::ConnectionEstablished(ev) => {
                info!("connection established with {}", ev.peer_id);
                // Add the peer to our system
                self.add_explicit_peer(ev.peer_id);
            }
            FromSwarm::ConnectionClosed(ev) => {
                info!("connection closed with {}", ev.peer_id);
                // Remove from gossipsub
                self.gossipsub.remove_explicit_peer(&ev.peer_id);

                // Process node disconnection
                self.manager.on_node_disconnected(&ev.peer_id);

                // Notify application
                self.pending_events
                    .push_back(PartitionBehaviourEvent::NodeStatusChanged {
                        peer_id: ev.peer_id,
                        is_active: false,
                        owned_partitions: HashSet::new(), // No partitions when inactive
                    });

                self.pending_events
                    .push_back(PartitionBehaviourEvent::ReplicaAssignmentUpdated {
                        partition_replicas: self.manager.partition_replicas.clone(),
                        active_nodes: self.manager.active_nodes.clone(),
                    });
            }
            _ => {}
        }

        self.gossipsub.on_swarm_event(event);
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        NetworkBehaviour::on_connection_handler_event(
            &mut self.gossipsub,
            peer_id,
            connection_id,
            event,
        )
    }

    fn poll(
        &mut self,
        cx: &mut task::Context,
    ) -> task::Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Some(ev) = self.pending_events.pop_front() {
            return task::Poll::Ready(ToSwarm::GenerateEvent(ev));
        }

        if let task::Poll::Ready(ev) = NetworkBehaviour::poll(&mut self.gossipsub, cx) {
            if let ToSwarm::GenerateEvent(ev) = &ev {
                self.handle_gossipsub_event(ev);
            }

            return task::Poll::Ready(ev.map_out(PartitionBehaviourEvent::Gossipsub));
        }

        // Send heartbeats periodically
        if self.heartbeat_interval.poll_tick(cx).is_ready() {
            self.send_heartbeat();
        }

        // Check for node timeouts
        if self.timeout_check_interval.poll_tick(cx).is_ready() {
            let status_changed = self.manager.check_heartbeat_timeouts();

            if status_changed {
                // Notify application
                self.pending_events
                    .push_back(PartitionBehaviourEvent::ReplicaAssignmentUpdated {
                        partition_replicas: self.manager.partition_replicas.clone(),
                        active_nodes: self.manager.active_nodes.clone(),
                    });
            }
        }

        task::Poll::Pending
    }
}
