use std::collections::{HashMap, HashSet};
use std::io;
use std::time::Duration;

use futures::StreamExt;
use kameo::mailbox::{MailboxReceiver, Signal};
use kameo::prelude::*;
use libp2p::request_response::OutboundRequestId;
use libp2p::{
    Multiaddr, StreamProtocol, TransportError,
    core::transport::ListenerId,
    gossipsub,
    identity::Keypair,
    mdns,
    request_response::{self, ProtocolSupport, ResponseChannel},
    swarm::SwarmEvent,
};
use sierradb::{
    bucket::PartitionId,
    database::{Database, Transaction},
    error::WriteError,
    writer_thread_pool::AppendResult,
};
use smallvec::SmallVec;
use tokio::sync::oneshot;
use tracing::{debug, error, trace};
use uuid::Uuid;

use crate::{
    behaviour::{Behaviour, BehaviourEvent, Req, Resp},
    error::SwarmError,
    partition_actor::PartitionActor,
    partition_consensus::{self, PartitionManager},
    write_actor::WriteActor,
};

use super::write_manager::{ConfirmTransactionError, WriteManager};

/// Manages the peer-to-peer network communication for distributed database
/// operations
///
/// Swarm is responsible for handling all network-related operations, including:
/// - Processing write requests (both local and remote)
/// - Managing replication across partitions
/// - Confirming transactions
/// - Discovering and communicating with peers in the network
pub struct Swarm {
    /// The underlying libp2p swarm instance
    swarm: libp2p::Swarm<Behaviour>,
    /// Manages distributed write operations
    write_manager: WriteManager,
}

/// Configuration parameters for creating a new Swarm actor
pub struct SwarmArgs {
    /// Identity key for this node
    pub key: Keypair,
    /// Reference to the local database instance
    pub database: Database,
    /// Total number of partitions in the system
    pub partition_count: u16,
    /// Number of replicas to maintain for each partition
    pub replication_factor: u8,
    /// Partitions assigned to this node
    pub assigned_partitions: HashSet<u16>,
    /// Maximum time to wait for a heartbeat before considering a peer down
    pub heartbeat_timeout: Duration,
    /// Interval between heartbeat messages
    pub heartbeat_interval: Duration,
}

impl Swarm {
    /// Returns a reference to the partition manager
    fn partition_manager(&self) -> &PartitionManager {
        &self.swarm.behaviour().partition_ownership.partition_manager
    }

    /// Sends a response through the libp2p response channel
    fn send_response<T>(&mut self, channel: ResponseChannel<Resp>, response: Resp, context: T)
    where
        T: std::fmt::Debug,
    {
        if let Err(err) = self
            .swarm
            .behaviour_mut()
            .req_resp
            .send_response(channel, response)
        {
            error!(?err, ?context, "Failed to send response");
        }
    }
}

impl Actor for Swarm {
    type Args = SwarmArgs;
    type Error = SwarmError;

    async fn on_start(
        SwarmArgs {
            key,
            database,
            partition_count: num_partitions,
            replication_factor,
            assigned_partitions,
            heartbeat_timeout,
            heartbeat_interval,
        }: Self::Args,
        actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        let local_peer_id = key.public().to_peer_id();
        trace!(
            %local_peer_id,
            partitions = %assigned_partitions.len(),
            "Starting swarm actor"
        );

        // Build the libp2p swarm with all required behaviors
        let swarm = libp2p::SwarmBuilder::with_existing_identity(key)
            .with_tokio()
            .with_quic()
            .with_behaviour(|key| {
                // Configure gossipsub for partition ownership messages
                let gossipsub_config = gossipsub::ConfigBuilder::default()
                    .heartbeat_interval(Duration::from_secs(1))
                    .validation_mode(gossipsub::ValidationMode::Strict)
                    .build()?;
                let gossipsub = gossipsub::Behaviour::new(
                    gossipsub::MessageAuthenticity::Signed(key.clone()),
                    gossipsub_config,
                )?;

                // Configure request/response behavior for direct messages
                let req_resp = request_response::Behaviour::new(
                    [(
                        StreamProtocol::new("/sierra-cluster/1"),
                        ProtocolSupport::Full,
                    )],
                    request_response::Config::default(),
                );

                // Configure mDNS for peer discovery
                let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;

                // Create partition manager to track partition ownership
                let partition_manager = PartitionManager::new(
                    local_peer_id,
                    vec![local_peer_id],
                    num_partitions,
                    replication_factor,
                    assigned_partitions.iter().copied().collect(),
                    None,
                    heartbeat_timeout,
                );

                // Create partition ownership behavior
                let partition_ownership = partition_consensus::Behaviour::new(
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

        // Create actors for each assigned partition
        let partition_actors: HashMap<_, _> = swarm
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
                        database.clone(),
                        partition_id,
                    )),
                )
            })
            .collect();

        // Create write manager to handle distributed writes
        let write_manager =
            WriteManager::new(actor_ref.clone(), database, local_peer_id, partition_actors);

        Ok(Swarm {
            swarm,
            write_manager,
        })
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
                Some(Signal::Message {
                    message: Box::new(event),
                    actor_ref,
                    reply: None,
                    sent_within_actor: true
                })
            },
        }
    }
}

/// Message to instruct the swarm to listen on a specific address
pub struct ListenOn {
    pub addr: Multiaddr,
}

impl Message<ListenOn> for Swarm {
    type Reply = Result<ListenerId, TransportError<io::Error>>;

    async fn handle(
        &mut self,
        msg: ListenOn,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        debug!(%msg.addr, "Starting to listen on address");
        self.swarm.listen_on(msg.addr)
    }
}

impl Message<Transaction> for Swarm {
    type Reply = oneshot::Receiver<Result<AppendResult, SwarmError>>;

    async fn handle(
        &mut self,
        transaction: Transaction,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        debug!("Processing local transaction");
        let behaviour = self.swarm.behaviour_mut();
        self.write_manager.process_local_write(
            &behaviour.partition_ownership.partition_manager,
            &mut behaviour.req_resp,
            transaction,
        )
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
            SwarmEvent::Behaviour(BehaviourEvent::ReqResp(event)) => {
                self.handle_req_resp_event(event).await;
            }
            SwarmEvent::Behaviour(BehaviourEvent::Mdns(event)) => {
                self.handle_mdns_event(event);
            }
            _ => {}
        }
    }
}

impl Swarm {
    /// Handles a request/response event from the network
    async fn handle_req_resp_event(&mut self, event: request_response::Event<Req, Resp>) {
        match event {
            request_response::Event::Message { message, .. } => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    self.handle_network_request(request, channel).await;
                }
                request_response::Message::Response {
                    request_id,
                    response,
                } => {
                    self.handle_network_response(request_id, response);
                }
            },
            request_response::Event::OutboundFailure {
                request_id, error, ..
            } => {
                self.write_manager
                    .process_outbound_failure(&request_id, error);
            }
            _ => {}
        }
    }

    /// Handles incoming network requests
    async fn handle_network_request(&mut self, request: Req, channel: ResponseChannel<Resp>) {
        let behaviour = self.swarm.behaviour_mut();

        match request {
            Req::AppendEvents {
                transaction,
                metadata,
            } => {
                debug!("Received append request from network");
                self.write_manager.process_network_write(
                    &behaviour.partition_ownership.partition_manager,
                    &mut behaviour.req_resp,
                    channel,
                    transaction,
                    metadata,
                );
            }
            Req::ReplicateWrite {
                partition_id,
                transaction,
                transaction_id,
                origin_partition: _,
                origin_peer,
            } => {
                debug!(%transaction_id, partition_id, "Received replication request from network");
                self.write_manager.process_network_replication(
                    channel,
                    partition_id,
                    transaction,
                    transaction_id,
                    origin_peer,
                );
            }
            Req::ConfirmWrite {
                partition_id,
                transaction_id,
                event_ids,
                confirmation_count,
            } => {
                debug!(%transaction_id, partition_id, "Received confirm write request from network");
                self.write_manager.process_network_commit(
                    channel,
                    partition_id,
                    transaction_id,
                    event_ids,
                    confirmation_count,
                );
            }
        }
    }

    /// Handles responses received from the network
    fn handle_network_response(&mut self, request_id: OutboundRequestId, response: Resp) {
        match response {
            Resp::AppendEventsSuccess { result } => {
                debug!("Received successful append response");
                self.write_manager
                    .process_append_result(&request_id, Ok(result));
            }
            Resp::AppendEventsFailure { error } => {
                debug!(?error, "Received failed append response");
                self.write_manager
                    .process_append_result(&request_id, Err(error));
            }
            Resp::ReplicateWriteSuccess {
                transaction_id,
                partition_id,
            } => {
                debug!(%transaction_id, "Received successful replication response");
                self.write_manager
                    .process_replication_response(transaction_id, partition_id, true);
            }
            Resp::ReplicateWriteFailure {
                transaction_id,
                partition_id,
                ..
            } => {
                debug!(%transaction_id, "Received failed replication response");
                self.write_manager.process_replication_response(
                    transaction_id,
                    partition_id,
                    false,
                );
            }
            _ => {}
        }
    }

    /// Handles mDNS discovery events
    fn handle_mdns_event(&mut self, event: mdns::Event) {
        match event {
            mdns::Event::Discovered(peers) => {
                for (peer_id, _) in peers {
                    debug!(%peer_id, "Discovered peer via mDNS");
                    self.swarm
                        .behaviour_mut()
                        .partition_ownership
                        .add_explicit_peer(peer_id);
                }
            }
            mdns::Event::Expired(_) => {}
        }
    }
}

/// Message to replicate a write to another partition
pub struct ReplicateWrite {
    pub partition_id: PartitionId,
    pub transaction: Transaction,
    pub transaction_id: Uuid,
    pub origin_partition: PartitionId,
    pub write_actor_ref: ActorRef<WriteActor>,
}

impl Message<ReplicateWrite> for Swarm {
    type Reply = ();

    async fn handle(
        &mut self,
        ReplicateWrite {
            partition_id,
            transaction,
            transaction_id,
            origin_partition,
            write_actor_ref,
        }: ReplicateWrite,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        debug!(
            %transaction_id,
            partition_id,
            origin_partition,
            "Processing replication request"
        );

        let behaviour = self.swarm.behaviour_mut();
        self.write_manager.process_replication(
            &behaviour.partition_ownership.partition_manager,
            &mut behaviour.req_resp,
            partition_id,
            transaction,
            transaction_id,
            origin_partition,
            write_actor_ref,
        );
    }
}

/// Message to confirm a write across replicas
pub struct ConfirmWrite {
    pub partition_id: PartitionId,
    pub transaction_id: Uuid,
    pub event_ids: SmallVec<[Uuid; 4]>,
    pub confirmation_count: u8,
}

impl Message<ConfirmWrite> for Swarm {
    type Reply = ();

    async fn handle(
        &mut self,
        ConfirmWrite {
            partition_id,
            transaction_id,
            event_ids,
            confirmation_count,
        }: ConfirmWrite,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        debug!(
            %transaction_id,
            partition_id,
            "Processing confirmation request"
        );

        // Verify we own the target partition
        if !self.partition_manager().has_partition(partition_id) {
            // warn!(%transaction_id, partition_id, "Received confirmation request for
            // unowned partition");
            return;
        }

        self.write_manager.process_primary_commit(
            partition_id,
            transaction_id,
            event_ids,
            confirmation_count,
        );
    }
}

/// Message to send an append response through the network
pub struct SendAppendResponse {
    pub channel: ResponseChannel<Resp>,
    pub partition_id: PartitionId,
    pub transaction_id: Uuid,
    pub result: Result<AppendResult, WriteError>,
}

impl Message<SendAppendResponse> for Swarm {
    type Reply = ();

    async fn handle(
        &mut self,
        SendAppendResponse {
            channel,
            partition_id,
            transaction_id,
            result,
        }: SendAppendResponse,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let context = format!(
            "transaction_id={} partition_id={}",
            transaction_id, partition_id
        );

        let resp = if result.is_ok() {
            debug!(%transaction_id, partition_id, "Sending successful replication response");
            Resp::ReplicateWriteSuccess {
                transaction_id,
                partition_id,
            }
        } else {
            debug!(%transaction_id, partition_id, "Sending failed replication response");
            Resp::ReplicateWriteFailure {
                transaction_id,
                partition_id,
                error: "Failed to replicate write".into(),
            }
        };

        self.send_response(channel, resp, context);
    }
}

/// Message to send a replica response to a request
pub struct SendReplicaResponse {
    pub channel: ResponseChannel<Resp>,
    pub result: Result<AppendResult, SwarmError>,
}

impl Message<SendReplicaResponse> for Swarm {
    type Reply = ();

    async fn handle(
        &mut self,
        SendReplicaResponse { channel, result }: SendReplicaResponse,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let context = "replica_response";

        // Prepare response based on result
        let resp = match result {
            Ok(result) => {
                debug!("Sending successful append response");
                Resp::AppendEventsSuccess { result }
            }
            Err(error) => {
                debug!(?error, "Sending failed append response");
                Resp::AppendEventsFailure { error }
            }
        };

        self.send_response(channel, resp, context);
    }
}

/// Message to send a transaction confirmation response
pub struct SendConfirmTransactionResponse {
    pub channel: ResponseChannel<Resp>,
    pub partition_id: PartitionId,
    pub transaction_id: Uuid,
    pub result: Result<(), ConfirmTransactionError>,
}

impl Message<SendConfirmTransactionResponse> for Swarm {
    type Reply = ();

    async fn handle(
        &mut self,
        SendConfirmTransactionResponse {
            channel,
            partition_id,
            transaction_id,
            result,
        }: SendConfirmTransactionResponse,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let context = format!(
            "transaction_id={} partition_id={}",
            transaction_id, partition_id
        );

        let resp = match result {
            Ok(()) => {
                debug!(%transaction_id, partition_id, "Sending successful confirmation response");
                Resp::ConfirmWriteSuccess {
                    transaction_id,
                    partition_id,
                }
            }
            Err(err) => {
                debug!(%transaction_id, partition_id, ?err, "Sending failed confirmation response");
                Resp::ConfirmWriteFailure {
                    transaction_id,
                    partition_id,
                    error: err.to_string(),
                }
            }
        };

        self.send_response(channel, resp, context);
    }
}

pub struct SendResponse {
    pub channel: ResponseChannel<Resp>,
    pub response: Resp,
}

impl Message<SendResponse> for Swarm {
    type Reply = ();

    async fn handle(
        &mut self,
        SendResponse { channel, response }: SendResponse,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if let Err(err) = self
            .swarm
            .behaviour_mut()
            .req_resp
            .send_response(channel, response)
        {
            error!(?err, "Failed to send response");
        }
    }
}

/// Represents the kind of reply to send for a write operation
pub enum ReplyKind<T> {
    /// Reply to a local request with a oneshot channel
    Local(oneshot::Sender<Result<T, SwarmError>>),
    /// Reply to a remote request with a libp2p response channel
    Remote(ResponseChannel<Resp>),
}
