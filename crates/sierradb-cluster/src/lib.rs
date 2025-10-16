use std::{
    collections::{HashMap, HashSet},
    io,
    sync::Arc,
    time::Duration,
};

use arrayvec::ArrayVec;
use circuit_breaker::WriteCircuitBreaker;
use confirmation::{actor::ConfirmationActor, AtomicWatermark};
use futures::StreamExt;
use kameo::{mailbox::Signal, prelude::*};
use libp2p::{
    gossipsub, identity::Keypair, mdns, noise, swarm::{behaviour::toggle::Toggle, NetworkBehaviour}, tcp,
    yamux,
    BehaviourBuilderError, Multiaddr,
    PeerId,
    Swarm, TransportError,
};
use serde::{Deserialize, Serialize};
use sierradb::{
    bucket::PartitionId, database::Database, error::WriteError, MAX_REPLICATION_FACTOR,
};
use sierradb_topology::TopologyManager;
use thiserror::Error;
use tracing::trace;

use crate::{
    subscription::SubscriptionManager,
    write::replicate::{PartitionReplicatorActor, PartitionReplicatorActorArgs},
};

pub mod circuit_breaker;
pub mod confirmation;
pub mod read;
pub mod subscription;
pub mod write;

pub type ReplicaRefs = ArrayVec<(RemoteActorRef<ClusterActor>, u64), MAX_REPLICATION_FACTOR>;

/// Maximum number of request forwards allowed to prevent loops
const MAX_FORWARDS: u8 = 3;

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    pub kameo: remote::Behaviour,
    pub topology: sierradb_topology::Behaviour<RemoteActorRef<ClusterActor>>,
    pub mdns: Toggle<mdns::tokio::Behaviour>,
}

#[derive(RemoteActor)]
pub struct ClusterActor {
    local_peer_id: PeerId,
    database: Database,
    swarm: Swarm<Behaviour>,
    replication_factor: u8,
    confirmation_ref: ActorRef<ConfirmationActor>,
    watermarks: HashMap<PartitionId, Arc<AtomicWatermark>>,
    circuit_breaker: Arc<WriteCircuitBreaker>,
    replicator_refs: HashMap<PartitionId, ActorRef<PartitionReplicatorActor>>,
    subscription_manager: SubscriptionManager,
}

impl ClusterActor {
    fn topology_manager(&self) -> &TopologyManager<RemoteActorRef<Self>> {
        &self.swarm.behaviour().topology.manager
    }
}

/// Configuration parameters for creating a new Swarm actor
pub struct ClusterArgs {
    /// Identity key for this node
    pub keypair: Keypair,
    /// Reference to the local database instance
    pub database: Database,
    /// List of addresses to listen on
    pub listen_addrs: Vec<Multiaddr>,
    /// Total number of nodes in the cluster
    pub node_count: usize,
    /// Zero-based index of this node in the cluster
    pub node_index: usize,
    /// Total number of buckets in the system
    pub bucket_count: u16,
    /// Total number of partitions in the system
    pub partition_count: u16,
    /// Number of replicas to maintain for each partition
    pub replication_factor: u8,
    /// Partitions assigned to this node
    pub assigned_partitions: HashSet<PartitionId>,
    /// Maximum time to wait for a heartbeat before considering a peer down
    pub heartbeat_timeout: Duration,
    /// Interval between heartbeat messages
    pub heartbeat_interval: Duration,
    /// Maximum number of out-of-order writes to buffer per partition
    pub replication_buffer_size: usize,
    /// Maximum time to keep buffered writes before timing out
    pub replication_buffer_timeout: Duration,
    /// Maximum time before requesting a catchup
    pub replication_catchup_timeout: Duration,
    /// Whether mdns is enabled
    pub mdns: bool,
}

impl Actor for ClusterActor {
    type Args = ClusterArgs;
    type Error = ClusterError;

    async fn on_start(
        ClusterArgs {
            keypair,
            database,
            listen_addrs,
            node_count,
            node_index,
            bucket_count,
            partition_count,
            replication_factor,
            assigned_partitions,
            heartbeat_timeout,
            heartbeat_interval,
            replication_buffer_size,
            replication_buffer_timeout,
            replication_catchup_timeout,
            mdns: mdns_enabled,
        }: Self::Args,
        actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        let local_peer_id = keypair.public().to_peer_id();
        trace!(
            %local_peer_id,
            partitions = %assigned_partitions.len(),
            "starting swarm actor"
        );

        let kameo = remote::Behaviour::new(
            keypair.public().to_peer_id(),
            remote::messaging::Config::default(),
        );

        kameo.init_global();

        let cluster_ref = actor_ref.into_remote_ref().await;

        // Build the libp2p swarm with all required behaviors
        let mut swarm = libp2p::SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
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

                // Configure mDNS for peer discovery
                let mdns = mdns_enabled
                    .then(|| mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id))
                    .transpose()?;

                // Create partition manager to track partition ownership
                let manager = TopologyManager::new(
                    cluster_ref,
                    node_index,
                    node_count,
                    partition_count,
                    bucket_count,
                    replication_factor,
                    heartbeat_timeout,
                );

                // Create partition ownership behavior
                let topology =
                    sierradb_topology::Behaviour::new(gossipsub, manager, heartbeat_interval);

                Ok(Behaviour {
                    kameo,
                    topology,
                    mdns: Toggle::from(mdns),
                })
            })?
            .build();

        for addr in listen_addrs {
            swarm.listen_on(addr)?;
        }

        let confirmation_actor = ConfirmationActor::new(
            database.clone(),
            replication_factor,
            assigned_partitions.clone(),
        )
        .await
        .map_err(|err| ClusterError::ConfirmationFailure(err.to_string()))?;
        let watermarks = confirmation_actor.manager.get_watermarks();
        let broadcaster = confirmation_actor.broadcaster();
        let confirmation_ref = Actor::spawn(confirmation_actor);

        let replicator_refs = assigned_partitions
            .iter()
            .map(|partition_id| {
                (
                    *partition_id,
                    PartitionReplicatorActor::spawn_with_mailbox(
                        PartitionReplicatorActorArgs {
                            partition_id: *partition_id,
                            database: database.clone(),
                            confirmation_ref: confirmation_ref.clone(),
                            buffer_size: replication_buffer_size,
                            buffer_timeout: replication_buffer_timeout,
                            catchup_timeout: replication_catchup_timeout,
                        },
                        mailbox::bounded(1_000),
                    ),
                )
            })
            .collect();

        let subscription_manager = SubscriptionManager::new(
            database.clone(),
            Arc::new(assigned_partitions),
            Arc::new(watermarks.clone()),
            partition_count,
            broadcaster,
        );

        let circuit_breaker = Arc::new(WriteCircuitBreaker::with_defaults());

        Ok(ClusterActor {
            local_peer_id,
            database,
            swarm,
            replication_factor,
            confirmation_ref,
            watermarks,
            circuit_breaker,
            replicator_refs,
            subscription_manager,
        })
    }

    async fn next(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        mailbox_rx: &mut MailboxReceiver<Self>,
    ) -> Option<Signal<Self>> {
        loop {
            tokio::select! {
                signal = mailbox_rx.recv() => return signal,
                _event = self.swarm.select_next_some() => {},
            }
        }
    }
}

/// For testing purposes.
pub struct ResetCluster {
    pub database: Database,
}

impl Message<ResetCluster> for ClusterActor {
    type Reply = Result<(), ClusterError>;

    async fn handle(
        &mut self,
        ResetCluster { database }: ResetCluster,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let assigned_partitions = self.topology_manager().assigned_partitions.clone();
        let partition_count = self.topology_manager().num_partitions;

        let confirmation_actor = ConfirmationActor::new(
            database.clone(),
            self.replication_factor,
            assigned_partitions.clone(),
        )
        .await
        .map_err(|err| ClusterError::ConfirmationFailure(err.to_string()))?;
        let watermarks = confirmation_actor.manager.get_watermarks();
        let broadcaster = confirmation_actor.broadcaster();
        let confirmation_ref = Actor::spawn(confirmation_actor);

        let replicator_refs = assigned_partitions
            .iter()
            .map(|partition_id| {
                (
                    *partition_id,
                    PartitionReplicatorActor::spawn_with_mailbox(
                        PartitionReplicatorActorArgs {
                            partition_id: *partition_id,
                            database: database.clone(),
                            confirmation_ref: confirmation_ref.clone(),
                            buffer_size: 1_000,
                            buffer_timeout: Duration::from_millis(8_000),
                            catchup_timeout: Duration::from_millis(1_000),
                        },
                        mailbox::bounded(1_000),
                    ),
                )
            })
            .collect();

        let subscription_manager = SubscriptionManager::new(
            database.clone(),
            Arc::new(assigned_partitions),
            Arc::new(watermarks.clone()),
            partition_count,
            broadcaster,
        );

        let circuit_breaker = Arc::new(WriteCircuitBreaker::with_defaults());

        self.replicator_refs = replicator_refs;
        self.confirmation_ref = confirmation_ref;
        self.watermarks = watermarks;
        self.subscription_manager = subscription_manager;
        self.circuit_breaker = circuit_breaker;
        self.database = database;

        Ok(())
    }
}

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum ClusterError {
    #[error("insufficient partitions alive for quorum ({alive}/{required})")]
    InsufficientPartitionsForQuorum { alive: u8, required: u8 },
    #[error("insufficient partitions alive")]
    NoAvailablePartitions,
    // #[error(transparent)]
    // #[serde(skip)]
    // OutboundFailure(#[from] OutboundFailure),
    // #[error("partition actor not found")]
    // PartitionActorNotFound { partition_id: PartitionId },
    #[error("partition unavailable")]
    PartitionUnavailable,
    #[error("no available leaders")]
    NoAvailableLeaders,
    #[error("quorum not achieved")]
    QuorumNotAchieved { confirmed: u8, required: u8 },
    #[error("write timed out")]
    WriteTimeout,
    #[error("too many forwards")]
    TooManyForwards,
    #[error("circuit breaker open: estimated recovery time: {estimated_recovery_time:?}")]
    CircuitBreakerOpen {
        estimated_recovery_time: Option<Duration>,
    },
    #[error("failed to write confirmation count: {0}")]
    ConfirmationFailure(String),
    #[error("read error: {0}")]
    Read(String),
    #[error("write error: {0}")]
    Write(String),
    // #[error("wrong leader node for append")]
    // WrongLeaderNode { correct_leader: PeerId },
    #[error(transparent)]
    RemoteSend(#[from] RemoteSendError),
    #[error(transparent)]
    #[serde(skip)]
    Noise(#[from] noise::Error),
    #[error(transparent)]
    #[serde(skip)]
    Transport(#[from] TransportError<io::Error>),
    #[error(transparent)]
    #[serde(skip)]
    SwarmBuilder(#[from] BehaviourBuilderError),
}

impl From<WriteError> for ClusterError {
    fn from(err: WriteError) -> Self {
        ClusterError::Write(err.to_string())
    }
}

impl From<RemoteSendError<ClusterError>> for ClusterError {
    fn from(err: RemoteSendError<ClusterError>) -> Self {
        match err {
            RemoteSendError::ActorNotRunning => {
                ClusterError::RemoteSend(RemoteSendError::ActorNotRunning)
            }
            RemoteSendError::ActorStopped => {
                ClusterError::RemoteSend(RemoteSendError::ActorStopped)
            }
            RemoteSendError::UnknownActor { actor_remote_id } => {
                ClusterError::RemoteSend(RemoteSendError::UnknownActor { actor_remote_id })
            }
            RemoteSendError::UnknownMessage {
                actor_remote_id,
                message_remote_id,
            } => ClusterError::RemoteSend(RemoteSendError::UnknownMessage {
                actor_remote_id,
                message_remote_id,
            }),
            RemoteSendError::BadActorType => {
                ClusterError::RemoteSend(RemoteSendError::BadActorType)
            }
            RemoteSendError::MailboxFull => ClusterError::RemoteSend(RemoteSendError::MailboxFull),
            RemoteSendError::ReplyTimeout => {
                ClusterError::RemoteSend(RemoteSendError::ReplyTimeout)
            }
            RemoteSendError::HandlerError(err) => err,
            RemoteSendError::SerializeMessage(err) => {
                ClusterError::RemoteSend(RemoteSendError::SerializeMessage(err))
            }
            RemoteSendError::DeserializeMessage(err) => {
                ClusterError::RemoteSend(RemoteSendError::DeserializeMessage(err))
            }
            RemoteSendError::SerializeReply(err) => {
                ClusterError::RemoteSend(RemoteSendError::SerializeReply(err))
            }
            RemoteSendError::SerializeHandlerError(err) => {
                ClusterError::RemoteSend(RemoteSendError::SerializeHandlerError(err))
            }
            RemoteSendError::DeserializeHandlerError(err) => {
                ClusterError::RemoteSend(RemoteSendError::DeserializeHandlerError(err))
            }
            RemoteSendError::SwarmNotBootstrapped => {
                ClusterError::RemoteSend(RemoteSendError::SwarmNotBootstrapped)
            }
            RemoteSendError::DialFailure => ClusterError::RemoteSend(RemoteSendError::DialFailure),
            RemoteSendError::NetworkTimeout => {
                ClusterError::RemoteSend(RemoteSendError::NetworkTimeout)
            }
            RemoteSendError::ConnectionClosed => {
                ClusterError::RemoteSend(RemoteSendError::ConnectionClosed)
            }
            RemoteSendError::UnsupportedProtocols => {
                ClusterError::RemoteSend(RemoteSendError::UnsupportedProtocols)
            }
            RemoteSendError::Io(err) => ClusterError::RemoteSend(RemoteSendError::Io(err)),
        }
    }
}
