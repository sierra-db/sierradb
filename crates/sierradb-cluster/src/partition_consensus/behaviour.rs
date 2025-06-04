use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::task;
use std::time::Duration;

use libp2p::core::Endpoint;
use libp2p::core::transport::PortUse;
use libp2p::gossipsub::{self, IdentTopic, PublishError};
use libp2p::swarm::{
    ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, THandler, THandlerInEvent,
    THandlerOutEvent, ToSwarm,
};
use libp2p::{Multiaddr, PeerId};
use tokio::time::Interval;
use tracing::{debug, error, info, trace, warn};

use super::manager::PartitionManager;
use super::messages::{Heartbeat, OwnershipMessage};

// Topic names
const HEARTBEAT_TOPIC: &str = "sierra/heartbeat";
const OWNERSHIP_TOPIC: &str = "sierra/ownership";

pub struct Behaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub partition_manager: PartitionManager,
    pub heartbeat_interval: Interval,
    pub timeout_check_interval: Interval,
    pub heartbeat_topic: gossipsub::IdentTopic,
    pub ownership_topic: gossipsub::IdentTopic,
    pub store_path: Option<PathBuf>,
    pub pending_events: VecDeque<PartitionBehaviourEvent>,
    pub heartbeat_bytes: Box<[u8]>,
}

impl Behaviour {
    pub fn new(
        mut gossipsub: gossipsub::Behaviour,
        partition_manager: PartitionManager,
        store_path: Option<PathBuf>,
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

        let heartbeat_bytes = bincode::encode_to_vec(
            Heartbeat {
                node_id: partition_manager.local_peer_id,
                owned_partitions: partition_manager.assigned_partitions.to_vec(),
            },
            bincode::config::standard(),
        )
        .unwrap()
        .into();

        info!(
            "node owns {}/{} partitions",
            partition_manager.assigned_partitions.len(),
            partition_manager.num_partitions,
        );
        trace!(
            "node owns partitions: {:?}",
            partition_manager.assigned_partitions,
        );

        Self {
            gossipsub,
            partition_manager,
            heartbeat_interval,
            timeout_check_interval,
            heartbeat_topic,
            ownership_topic,
            store_path,
            pending_events: VecDeque::new(),
            heartbeat_bytes,
        }
    }

    pub fn add_explicit_peer(&mut self, peer_id: PeerId) {
        // Add peer to gossipsub
        self.gossipsub.add_explicit_peer(&peer_id);

        // Send an ownership request to the new peer
        let request = OwnershipMessage::OwnershipRequest {
            node_id: self.partition_manager.local_peer_id,
            owned_partitions: self.partition_manager.assigned_partitions.to_vec(),
        };

        self.send_ownership_message(&request);
    }

    fn handle_gossipsub_event(&mut self, event: &gossipsub::Event) {
        if let gossipsub::Event::Message { message, .. } = event {
            match message.topic.as_str() {
                HEARTBEAT_TOPIC => {
                    // Decode the heartbeat message
                    match bincode::decode_from_slice(&message.data, bincode::config::standard()) {
                        Ok((
                            Heartbeat {
                                node_id,
                                owned_partitions,
                            },
                            _,
                        )) => {
                            trace!("received heartbeat from {:?}", node_id);

                            // Check if the node was previously inactive
                            let was_inactive =
                                !self.partition_manager.active_nodes.contains(&node_id);

                            // Update heartbeat and ownership
                            let status_changed = self
                                .partition_manager
                                .on_heartbeat(node_id, &owned_partitions);

                            // Make sure our own partitions are still registered
                            self.partition_manager.ensure_local_partitions();

                            // If this is a new node, or it was inactive
                            if status_changed || was_inactive {
                                info!(
                                    "node {:?} is now active with {} partitions",
                                    node_id,
                                    owned_partitions.len()
                                );

                                self.pending_events.push_back(
                                    PartitionBehaviourEvent::NodeStatusChanged {
                                        peer_id: node_id,
                                        is_active: true,
                                        owned_partitions: owned_partitions.clone(),
                                    },
                                );

                                self.pending_events.push_back(
                                    PartitionBehaviourEvent::OwnershipUpdated {
                                        partition_owners: self
                                            .partition_manager
                                            .partition_owners
                                            .clone(),
                                        active_nodes: self.partition_manager.active_nodes.clone(),
                                    },
                                );

                                // Save the updated ownership info
                                if let Some(path) = &self.store_path {
                                    self.partition_manager.save_store_spawn(path.clone());
                                }
                            }
                        }
                        Err(err) => {
                            error!("failed to decode heartbeat message: {}", err);
                        }
                    }
                }
                OWNERSHIP_TOPIC => {
                    match bincode::decode_from_slice::<OwnershipMessage, _>(
                        &message.data,
                        bincode::config::standard(),
                    ) {
                        Ok((msg, _)) => {
                            self.handle_partition_message(&msg);
                        }
                        Err(err) => {
                            error!("failed to decode partition message: {err}");
                        }
                    }
                }
                topic => {
                    warn!("unknown topic: {topic}")
                }
            }
        }
    }

    fn handle_partition_message(&mut self, message: &OwnershipMessage) {
        match message {
            OwnershipMessage::OwnershipRequest {
                node_id,
                owned_partitions,
            } => {
                info!(
                    "received ownership request from {:?} with {} partitions",
                    node_id,
                    owned_partitions.len()
                );

                // Process the node connection
                if let Some(response) = self
                    .partition_manager
                    .on_node_connected(*node_id, owned_partitions)
                {
                    // Send a response with our full ownership information
                    self.send_ownership_message(&response);

                    // Notify application
                    self.pending_events
                        .push_back(PartitionBehaviourEvent::NodeStatusChanged {
                            peer_id: *node_id,
                            is_active: true,
                            owned_partitions: owned_partitions.clone(),
                        });

                    self.pending_events
                        .push_back(PartitionBehaviourEvent::OwnershipUpdated {
                            partition_owners: self.partition_manager.partition_owners.clone(),
                            active_nodes: self.partition_manager.active_nodes.clone(),
                        });
                }
            }
            OwnershipMessage::OwnershipResponse {
                partition_owners,
                active_nodes,
            } => {
                info!(
                    "received ownership response with {} partitions and {} active nodes",
                    partition_owners.len(),
                    active_nodes.len()
                );

                // Update our ownership info from the response
                self.partition_manager
                    .handle_ownership_response(partition_owners, active_nodes.clone());

                // Make sure our partitions are still registered correctly
                self.partition_manager.ensure_local_partitions();

                // Save store
                if let Some(path) = &self.store_path {
                    self.partition_manager.save_store_spawn(path.clone());
                }

                // Notify application
                self.pending_events
                    .push_back(PartitionBehaviourEvent::OwnershipUpdated {
                        partition_owners: self.partition_manager.partition_owners.clone(),
                        active_nodes: self.partition_manager.active_nodes.clone(),
                    });
            }
        }
    }

    fn send_ownership_message(&mut self, message: &OwnershipMessage) {
        // Encode and send
        match bincode::encode_to_vec(message, bincode::config::standard()) {
            Ok(encoded) => {
                if let Err(err) = self.gossipsub.publish(self.ownership_topic.hash(), encoded) {
                    error!("error publishing message: {err}");
                }
            }
            Err(err) => {
                error!("failed to encode message: {err}");
            }
        }
    }

    fn send_heartbeat(&mut self) {
        // Always make sure our partitions are still registered correctly
        self.partition_manager.ensure_local_partitions();

        trace!(
            "sending heartbeat with {} partitions",
            self.partition_manager.assigned_partitions.len()
        );
        if let Err(err) = self
            .gossipsub
            .publish(self.heartbeat_topic.hash(), self.heartbeat_bytes.clone())
            && !matches!(err, PublishError::InsufficientPeers)
        {
            error!("error publishing heartbeat: {err}");
        }
    }
}

#[derive(Debug)]
pub enum PartitionBehaviourEvent {
    Gossipsub(<gossipsub::Behaviour as NetworkBehaviour>::ToSwarm),
    NodeStatusChanged {
        peer_id: PeerId,
        is_active: bool,
        owned_partitions: Vec<u16>,
    },
    OwnershipUpdated {
        partition_owners: HashMap<u16, PeerId>,
        active_nodes: HashSet<PeerId>,
    },
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = THandler<gossipsub::Behaviour>;
    type ToSwarm = PartitionBehaviourEvent;

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
        )?;
        Ok(())
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
                self.partition_manager.on_node_disconnected(ev.peer_id);

                // Notify application
                self.pending_events
                    .push_back(PartitionBehaviourEvent::NodeStatusChanged {
                        peer_id: ev.peer_id,
                        is_active: false,
                        owned_partitions: Vec::new(), // No partitions when inactive
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
            let status_changed = self.partition_manager.check_heartbeat_timeouts();

            if status_changed {
                // Notify application
                self.pending_events
                    .push_back(PartitionBehaviourEvent::OwnershipUpdated {
                        partition_owners: self.partition_manager.partition_owners.clone(),
                        active_nodes: self.partition_manager.active_nodes.clone(),
                    });
            }
        }

        task::Poll::Pending
    }
}
