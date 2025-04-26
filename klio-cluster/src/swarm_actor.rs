use std::collections::{HashMap, HashSet};
use std::io;
use std::time::Duration;

use futures::StreamExt;
use kameo::error::Infallible;
use kameo::mailbox::{MailboxReceiver, Signal};
use kameo::prelude::*;
use klio_core::bucket::PartitionId;
use klio_core::database::Database;
use klio_core::id::uuid_to_partition_id;
use klio_core::writer_thread_pool::{AppendEventsBatch, AppendResult};
use klio_partition_consensus::manager::PartitionManager;
use libp2p::core::transport::ListenerId;
use libp2p::identity::Keypair;
use libp2p::request_response::{self, OutboundRequestId, ProtocolSupport, ResponseChannel};
use libp2p::swarm::SwarmEvent;
use libp2p::{Multiaddr, PeerId, StreamProtocol, TransportError, gossipsub, mdns};
use smallvec::SmallVec;
use tokio::sync::oneshot;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::behaviour::{Behaviour, BehaviourEvent, Req, Resp, WriteRequestMetadata};
use crate::error::SwarmError;
use crate::partition_actor::{LeaderWriteRequest, PartitionActor};
use crate::write_actor::{ReplicaConfirmation, WriteActor};

pub struct Swarm {
    swarm: libp2p::Swarm<Behaviour>,
    database: Database,
    partition_actors: HashMap<u16, ActorRef<PartitionActor>>,
    forwarded_appends:
        HashMap<OutboundRequestId, oneshot::Sender<Result<AppendResult, SwarmError>>>,
    pending_replications: HashMap<(Uuid, PartitionId), ActorRef<WriteActor>>,
    pending_requests: HashMap<Uuid, PartitionId>,
}

struct AppendRequest {
    /// Event batch to append
    append: AppendEventsBatch,
    /// How to reply to the request
    reply: ReplyKind,
    /// Request tracking metadata
    metadata: WriteRequestMetadata,
}

impl Swarm {
    pub fn new(
        key: Keypair,
        database: Database,
        num_partitions: u16,
        replication_factor: u8,
        assigned_partitions: HashSet<u16>,
        heartbeat_timeout: Duration,
        heartbeat_interval: Duration,
    ) -> Result<Self, SwarmError> {
        let swarm = libp2p::SwarmBuilder::with_existing_identity(key)
            .with_tokio()
            .with_quic()
            .with_behaviour(|key| {
                let local_peer_id = key.public().to_peer_id();

                let gossipsub_config = gossipsub::ConfigBuilder::default()
                    .heartbeat_interval(Duration::from_secs(1))
                    .validation_mode(gossipsub::ValidationMode::Strict)
                    .build()?;
                let gossipsub = gossipsub::Behaviour::new(
                    gossipsub::MessageAuthenticity::Signed(key.clone()),
                    gossipsub_config,
                )?;

                let req_resp = request_response::Behaviour::new(
                    [(
                        StreamProtocol::new("/klio-cluster/1"),
                        ProtocolSupport::Full,
                    )],
                    request_response::Config::default(),
                );

                let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;

                let partition_manager = PartitionManager::new(
                    local_peer_id,
                    vec![local_peer_id],
                    num_partitions,
                    replication_factor,
                    assigned_partitions.iter().copied().collect(),
                    None,
                    heartbeat_timeout,
                );

                let partition_ownership = klio_partition_consensus::behaviour::Behaviour::new(
                    gossipsub,
                    partition_manager,
                    None,
                    heartbeat_interval,
                );

                Ok(Behaviour {
                    partition_ownership,
                    mdns,
                    req_resp,
                })
            })?
            .build();

        Ok(Swarm {
            swarm,
            database,
            partition_actors: HashMap::new(),
            forwarded_appends: HashMap::new(),
            pending_replications: HashMap::new(),
            pending_requests: HashMap::new(),
        })
    }

    /// Starts listening on the given address.
    /// Returns an error if the address is not supported.
    ///
    /// Listeners report their new listening addresses as
    /// [`SwarmEvent::NewListenAddr`]. Depending on the underlying
    /// transport, one listener may have multiple listening addresses.
    pub fn listen_on(&mut self, addr: Multiaddr) -> Result<ListenerId, TransportError<io::Error>> {
        self.swarm.listen_on(addr)
    }

    fn partition_manager(&self) -> &PartitionManager {
        &self.swarm.behaviour().partition_ownership.partition_manager
    }

    /// Handle client write request (entry point for local client requests)
    pub fn client_write_request(
        &mut self,
        append: AppendEventsBatch,
    ) -> oneshot::Receiver<Result<AppendResult, SwarmError>> {
        let (reply_tx, reply_rx) = oneshot::channel();

        let partition_key = uuid_to_partition_id(append.partition_key());
        let request = AppendRequest {
            append,
            reply: ReplyKind::Local(reply_tx),
            metadata: WriteRequestMetadata {
                hop_count: 0,
                tried_peers: HashSet::from([*self.swarm.local_peer_id()]),
                partition_key,
            },
        };

        self.handle_append_request(request);

        reply_rx
    }

    /// Handle forwarded append request (entry point for requests from other
    /// nodes)
    fn handle_forwarded_append(
        &mut self,
        channel: ResponseChannel<Resp>,
        append: AppendEventsBatch,
        metadata: WriteRequestMetadata,
    ) {
        // Increment hop count when receiving a forwarded request
        let metadata = WriteRequestMetadata {
            hop_count: metadata.hop_count + 1,
            tried_peers: metadata.tried_peers,
            partition_key: metadata.partition_key,
        };

        let request = AppendRequest {
            append,
            reply: ReplyKind::Remote(channel),
            metadata,
        };

        self.handle_append_request(request);
    }

    /// Unified function to handle write requests from any source
    fn handle_append_request(&mut self, request: AppendRequest) {
        const MAX_FORWARDS: u8 = 3; // Maximum number of forwards to prevent loops

        // Helper for sending failure responses
        let send_failure = |swarm: &mut Swarm, reply: ReplyKind, error: SwarmError| match reply {
            ReplyKind::Local(tx) => {
                let _ = tx.send(Err(error));
            }
            ReplyKind::Remote(channel) => {
                let _ = swarm
                    .swarm
                    .behaviour_mut()
                    .req_resp
                    .send_response(channel, Resp::AppendEventsFailure { error });
            }
        };

        // Check for too many forwards
        if request.metadata.hop_count > MAX_FORWARDS {
            send_failure(self, request.reply, SwarmError::TooManyForwards);
            return;
        }

        // Get partition distribution
        let partitions = self
            .partition_manager()
            .get_available_partitions_for_key(request.metadata.partition_key);

        // Check quorum requirements
        let required_quorum = (self.partition_manager().replication_factor as usize / 2) + 1;
        if partitions.len() < required_quorum {
            send_failure(
                self,
                request.reply,
                SwarmError::InsufficientPartitionsForQuorum {
                    alive: partitions.len() as u8,
                    required: required_quorum as u8,
                },
            );
            return;
        }

        // Get leader partition info
        let (primary_partition_id, primary_peer_id) = match partitions.first().copied() {
            Some(info) => info,
            None => {
                send_failure(self, request.reply, SwarmError::PartitionUnavailable);
                return;
            }
        };

        // Check if we are the leader
        if self.partition_manager().has_partition(primary_partition_id)
            && primary_peer_id == *self.swarm.local_peer_id()
        {
            // We are the leader - get the partition actor
            let partition_actor_ref = match self.partition_actors.get(&primary_partition_id) {
                Some(actor_ref) => actor_ref,
                None => {
                    send_failure(
                        self,
                        request.reply,
                        SwarmError::PartitionActorNotFound {
                            partition_id: primary_partition_id,
                        },
                    );
                    return;
                }
            };

            let transaction_id = request.append.transaction_id();

            // Extract replica partitions from the partitions list
            // Skip the first partition (primary) and only include partitions we're not the
            // leader for
            let replica_partitions = partitions
                .iter()
                .skip(1) // Skip the primary partition (first in the list)
                .map(|(partition_id, _)| *partition_id)
                .collect::<Vec<_>>();

            // Forward to partition actor
            let leader_request = LeaderWriteRequest {
                append: request.append,
                reply: request.reply,
                replica_partitions,
                transaction_id,
                replication_factor: self.partition_manager().replication_factor,
            };

            let tell_res = partition_actor_ref.tell(leader_request).try_send();
            match tell_res {
                Ok(()) => {}
                Err(err) => {
                    let reply = match err {
                        SendError::ActorNotRunning(LeaderWriteRequest { reply, .. }) => Some(reply),
                        SendError::ActorStopped => None,
                        SendError::MailboxFull(LeaderWriteRequest { reply, .. }) => Some(reply),
                        SendError::HandlerError(_) => None,
                        SendError::Timeout(Some(LeaderWriteRequest { reply, .. })) => Some(reply),
                        SendError::Timeout(None) => None,
                    };
                    if let Some(reply) = reply {
                        send_failure(self, reply, SwarmError::PartitionActorSendError);
                    }
                }
            }
        } else {
            // We need to forward to the leader

            // Check if we've already tried this peer (to prevent loops)
            if request.metadata.tried_peers.contains(&primary_peer_id) {
                // Try the next best partition/peer if available
                for (_partition_id, peer_id) in partitions.iter().skip(1) {
                    if !request.metadata.tried_peers.contains(peer_id) {
                        // Found an untried peer, forward to them
                        self.forward_to_peer(request, *peer_id);
                        return;
                    }
                }

                // No untried peers left
                send_failure(self, request.reply, SwarmError::NoAvailableLeaders);
                return;
            }

            // Forward to the leader peer
            self.forward_to_peer(request, primary_peer_id);
        }
    }

    /// Forward a request to another peer
    fn forward_to_peer(&mut self, mut request: AppendRequest, peer_id: PeerId) {
        // Add this peer to the tried list
        request.metadata.tried_peers.insert(peer_id);

        match request.reply {
            ReplyKind::Local(tx) => {
                // Local request being forwarded to another node
                let req_id = self.swarm.behaviour_mut().req_resp.send_request(
                    &peer_id,
                    Req::AppendEvents {
                        append: request.append,
                        metadata: request.metadata,
                    },
                );

                // Store the sender to respond when we get a reply
                self.forwarded_appends.insert(req_id, tx);
            }
            ReplyKind::Remote(channel) => {
                // Remote request being forwarded again
                // For simplicity, we'll respond to the original requester that they
                // should try again with the correct leader
                let _ = self.swarm.behaviour_mut().req_resp.send_response(
                    channel,
                    Resp::AppendEventsFailure {
                        error: SwarmError::WrongLeaderNode {
                            correct_leader: peer_id,
                        },
                    },
                );
            }
        }
    }

    fn handle_replication_response(&mut self, transaction_id: Uuid, success: bool) {
        // Look up the request details
        if let Some(partition_id) = self.pending_requests.remove(&transaction_id) {
            debug!(
                transaction_id = ?transaction_id,
                partition_id = ?partition_id,
                success,
                "Received replication response from network"
            );

            // Look up the WriteActor reference
            if let Some(write_actor_ref) = self
                .pending_replications
                .remove(&(transaction_id, partition_id))
            {
                // Forward the confirmation to the WriteActor
                let _ = write_actor_ref
                    .tell(ReplicaConfirmation {
                        partition_id,
                        transaction_id,
                        success,
                    })
                    .try_send();
            } else {
                warn!(
                    transaction_id = ?transaction_id,
                    partition_id = ?partition_id,
                    "No write actor found for replication response"
                );
            }
        } else {
            warn!(
                req_id = ?transaction_id,
                "No pending request found for response"
            );
        }
    }
}

impl Actor for Swarm {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        mut state: Self::Args,
        actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        state.partition_actors = state
            .swarm
            .behaviour()
            .partition_ownership
            .partition_manager
            .assigned_partitions
            .iter()
            .copied()
            .map(|partition_id| {
                (
                    partition_id,
                    PartitionActor::spawn(PartitionActor::new(
                        actor_ref.clone(),
                        state.database.clone(),
                        partition_id,
                    )),
                )
            })
            .collect();

        Ok(state)
    }

    async fn next(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        mailbox_rx: &mut MailboxReceiver<Self>,
    ) -> Option<Signal<Self>> {
        tokio::select! {
            signal = mailbox_rx.recv() => signal,
            Some(event) = self.swarm.next() => {
                let actor_ref = actor_ref.upgrade()?;
                Some(Signal::Message { message: Box::new(event), actor_ref, reply: None, sent_within_actor: true })
            },
        }
    }
}

/// Message sent from PartitionActor to SwarmActor when an append operation is
/// completed
pub struct AppendRequestCompleted {
    pub result: Result<AppendResult, SwarmError>,
    pub response_channel: ResponseChannel<Resp>,
}

impl Message<AppendRequestCompleted> for Swarm {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: AppendRequestCompleted,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let response = match msg.result {
            Ok(result) => Resp::AppendEventsSuccess { result },
            Err(error) => Resp::AppendEventsFailure { error },
        };

        let _ = self
            .swarm
            .behaviour_mut()
            .req_resp
            .send_response(msg.response_channel, response);
    }
}

#[derive(Clone, Debug)]
pub struct AppendEventsFailed {
    pub transaction_id: Uuid,
    pub partition_id: PartitionId,
}

impl Message<AppendEventsFailed> for Swarm {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: AppendEventsFailed,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Implementation omitted for now
        warn!("AppendEventsFailed handler not implemented");
    }
}

impl Message<AppendEventsBatch> for Swarm {
    type Reply = oneshot::Receiver<Result<AppendResult, SwarmError>>;

    async fn handle(
        &mut self,
        append: AppendEventsBatch,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.client_write_request(append)
    }
}

impl Message<SwarmEvent<BehaviourEvent>> for Swarm {
    type Reply = ();

    async fn handle(
        &mut self,
        event: SwarmEvent<BehaviourEvent>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match event {
            SwarmEvent::Behaviour(BehaviourEvent::ReqResp(event)) => match event {
                request_response::Event::Message { message, .. } => match message {
                    request_response::Message::Request {
                        request, channel, ..
                    } => match request {
                        Req::AppendEvents { append, metadata } => {
                            self.handle_forwarded_append(channel, append, metadata);
                        }
                        Req::ReplicateWrite {
                            partition_id,
                            append,
                            transaction_id,
                            origin_partition: _,
                            origin_peer,
                        } => {
                            debug!(
                                transaction_id = ?transaction_id,
                                partition_id = ?partition_id,
                                origin_peer = ?origin_peer,
                                "Received ReplicateWrite request from network"
                            );

                            // Process the replication locally
                            let result = self
                                .database
                                .append_events(partition_id, append.clone())
                                .await;
                            let success = result.is_ok();

                            // Send response back to the requester
                            let resp = if success {
                                Resp::ReplicateWriteSuccess {
                                    transaction_id,
                                    partition_id,
                                }
                            } else {
                                Resp::ReplicateWriteFailure {
                                    transaction_id,
                                    partition_id,
                                    error: "Failed to replicate write".into(),
                                }
                            };

                            if let Err(err) = self
                                .swarm
                                .behaviour_mut()
                                .req_resp
                                .send_response(channel, resp)
                            {
                                error!(?err, "Failed to send replication response");
                            }
                        }
                        Req::ConfirmWrite {
                            partition_id,
                            transaction_id,
                            offsets,
                            confirmation_count,
                        } => {
                            debug!(
                                transaction_id = ?transaction_id,
                                partition_id = ?partition_id,
                                "Received ConfirmWrite request from network"
                            );

                            // Look up the event to get offsets
                            let success = self
                                .database
                                .set_confirmations(
                                    partition_id,
                                    offsets,
                                    transaction_id,
                                    confirmation_count,
                                )
                                .await
                                .is_ok();

                            // Send response
                            let resp = if success {
                                Resp::ConfirmWriteSuccess {
                                    transaction_id,
                                    partition_id,
                                }
                            } else {
                                Resp::ConfirmWriteFailure {
                                    transaction_id,
                                    partition_id,
                                    error: "Failed to confirm write".into(),
                                }
                            };

                            if let Err(err) = self
                                .swarm
                                .behaviour_mut()
                                .req_resp
                                .send_response(channel, resp)
                            {
                                error!(?err, "Failed to send confirmation response");
                            }
                        }
                    },
                    request_response::Message::Response {
                        request_id,
                        response,
                    } => match response {
                        Resp::AppendEventsSuccess { result } => {
                            if let Some(tx) = self.forwarded_appends.remove(&request_id) {
                                let _ = tx.send(Ok(result));
                            }
                        }
                        Resp::AppendEventsFailure { error } => {
                            if let Some(tx) = self.forwarded_appends.remove(&request_id) {
                                let _ = tx.send(Err(error));
                            }
                        }
                        Resp::ReplicateWriteSuccess { transaction_id, .. } => {
                            // Convert response back to our internal request ID mapping
                            if self.pending_requests.contains_key(&transaction_id) {
                                self.handle_replication_response(transaction_id, true);
                            }
                        }
                        Resp::ReplicateWriteFailure { transaction_id, .. } => {
                            // Convert response back to our internal request ID mapping
                            if self.pending_requests.contains_key(&transaction_id) {
                                self.handle_replication_response(transaction_id, false);
                            }
                        }
                        _ => {}
                    },
                },
                request_response::Event::OutboundFailure {
                    request_id, error, ..
                } => {
                    if let Some(tx) = self.forwarded_appends.remove(&request_id) {
                        let _ = tx.send(Err(SwarmError::OutboundFailure(error)));
                    }
                }
                _ => {}
            },
            SwarmEvent::Behaviour(BehaviourEvent::Mdns(event)) => match event {
                mdns::Event::Discovered(peers) => {
                    for (peer_id, _) in peers {
                        self.swarm
                            .behaviour_mut()
                            .partition_ownership
                            .add_explicit_peer(peer_id);
                    }
                }
                mdns::Event::Expired(_) => {}
            },
            _ => {}
        }
    }
}

pub struct ReplicateWrite {
    pub partition_id: PartitionId,
    pub append: AppendEventsBatch,
    pub transaction_id: Uuid,
    pub origin_partition: PartitionId,
    pub write_actor_ref: ActorRef<WriteActor>,
}

impl Message<ReplicateWrite> for Swarm {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: ReplicateWrite,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        debug!(
            transaction_id = ?msg.transaction_id,
            partition_id = ?msg.partition_id,
            origin_partition = ?msg.origin_partition,
            "Handling replication request"
        );

        // Check if we own the target partition
        if self.partition_manager().has_partition(msg.partition_id) {
            // We own this partition, so handle the replication locally
            debug!(
                transaction_id = ?msg.transaction_id,
                partition_id = ?msg.partition_id,
                "Replicating write to local partition"
            );

            // Replicate the write to our local database
            let result = self
                .database
                .append_events(msg.partition_id, msg.append.clone())
                .await;

            // Send confirmation back to the origin
            let success = result.is_ok();

            debug!(
                transaction_id = ?msg.transaction_id,
                partition_id = ?msg.partition_id,
                success,
                "Local replication result"
            );

            // Send confirmation back to the write actor
            let _ = msg
                .write_actor_ref
                .tell(ReplicaConfirmation {
                    partition_id: msg.partition_id,
                    transaction_id: msg.transaction_id,
                    success,
                })
                .await;
        } else {
            // We don't own this partition - need to forward to the correct node
            let partitions = self
                .partition_manager()
                .get_available_partitions_for_key(msg.partition_id);

            if let Some((_, peer_id)) = partitions.first() {
                debug!(
                    transaction_id = ?msg.transaction_id,
                    partition_id = ?msg.partition_id,
                    peer_id = ?peer_id,
                    "Forwarding replication request to remote node"
                );

                // Store the write actor ref for when we get a response
                self.pending_replications.insert(
                    (msg.transaction_id, msg.partition_id),
                    msg.write_actor_ref.clone(),
                );

                // Forward the replication request to the correct node
                let local_peer_id = *self.swarm.local_peer_id();
                self.swarm.behaviour_mut().req_resp.send_request(
                    peer_id,
                    Req::ReplicateWrite {
                        partition_id: msg.partition_id,
                        append: msg.append,
                        transaction_id: msg.transaction_id,
                        origin_partition: msg.origin_partition,
                        origin_peer: local_peer_id,
                    },
                );

                // Map the transaction_id to the partition_id and transaction_id for later
                // lookup
                self.pending_requests
                    .insert(msg.transaction_id, msg.partition_id);
            } else {
                // Can't find a node for this partition
                warn!(
                    transaction_id = ?msg.transaction_id,
                    partition_id = ?msg.partition_id,
                    "No node found for partition"
                );

                let _ = msg
                    .write_actor_ref
                    .tell(ReplicaConfirmation {
                        partition_id: msg.partition_id,
                        transaction_id: msg.transaction_id,
                        success: false,
                    })
                    .await;
            }
        }
    }
}

pub struct ConfirmWrite {
    pub partition_id: PartitionId,
    pub transaction_id: Uuid,
    pub offsets: SmallVec<[u64; 4]>,
    pub confirmation_count: u8,
}

impl Message<ConfirmWrite> for Swarm {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: ConfirmWrite,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        debug!(
            transaction_id = ?msg.transaction_id,
            partition_id = ?msg.partition_id,
            "Handling confirmation request"
        );

        // Check if we own the target partition
        if self.partition_manager().has_partition(msg.partition_id) {
            // We own this partition, so handle the confirmation locally
            debug!(
                transaction_id = ?msg.transaction_id,
                partition_id = ?msg.partition_id,
                "Confirming write locally"
            );

            // Mark the write as confirmed in our database
            match self
                .database
                .set_confirmations(
                    msg.partition_id,
                    msg.offsets,
                    msg.transaction_id,
                    msg.confirmation_count,
                )
                .await
            {
                Ok(_) => {
                    debug!(
                        transaction_id = ?msg.transaction_id,
                        partition_id = ?msg.partition_id,
                        "Successfully confirmed write"
                    );
                }
                Err(err) => {
                    error!(
                        transaction_id = ?msg.transaction_id,
                        partition_id = ?msg.partition_id,
                        ?err,
                        "Failed to confirm write"
                    );
                }
            }
        } else {
            // We don't own this partition - need to forward to the correct node
            let partitions = self
                .partition_manager()
                .get_available_partitions_for_key(msg.partition_id);

            if let Some((_, peer_id)) = partitions.first() {
                debug!(
                    transaction_id = ?msg.transaction_id,
                    partition_id = ?msg.partition_id,
                    peer_id = ?peer_id,
                    "Forwarding confirmation to remote node"
                );

                // Forward the confirmation to the correct node
                let _ = self.swarm.behaviour_mut().req_resp.send_request(
                    peer_id,
                    Req::ConfirmWrite {
                        partition_id: msg.partition_id,
                        transaction_id: msg.transaction_id,
                        offsets: msg.offsets,
                        confirmation_count: msg.confirmation_count,
                    },
                );
            } else {
                warn!(
                    transaction_id = ?msg.transaction_id,
                    partition_id = ?msg.partition_id,
                    "No node found for partition"
                );
            }
        }
    }
}

pub struct SendReplicaResponse {
    pub channel: ResponseChannel<Resp>,
    pub result: Result<AppendResult, SwarmError>,
}

impl Message<SendReplicaResponse> for Swarm {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: SendReplicaResponse,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        debug!("Sending replica response");

        // Send response through the libp2p response channel
        let resp = match msg.result {
            Ok(result) => {
                debug!("Sending success response");
                Resp::AppendEventsSuccess { result }
            }
            Err(error) => {
                debug!(error = ?error, "Sending failure response");
                Resp::AppendEventsFailure { error }
            }
        };

        // Send the response through the libp2p channel
        if let Err(err) = self
            .swarm
            .behaviour_mut()
            .req_resp
            .send_response(msg.channel, resp)
        {
            error!(?err, "Failed to send response");
        }
    }
}

pub enum ReplyKind {
    Local(oneshot::Sender<Result<AppendResult, SwarmError>>),
    Remote(ResponseChannel<Resp>),
}
