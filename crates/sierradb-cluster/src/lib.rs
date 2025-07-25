use std::{
    collections::{HashMap, HashSet},
    io,
    sync::Arc,
    time::Duration,
};

use circuit_breaker::WriteCircuitBreaker;
use confirmation::{AtomicWatermark, actor::ConfirmationActor};
use futures::StreamExt;
use kameo::{mailbox::Signal, prelude::*};
use libp2p::{
    BehaviourBuilderError, Multiaddr, PeerId, Swarm, TransportError, gossipsub, identity::Keypair,
    mdns, noise, swarm::NetworkBehaviour, tcp, yamux,
};
use serde::{Deserialize, Serialize};
use sierradb::{bucket::PartitionId, database::Database, error::WriteError};
use sierradb_topology::TopologyManager;
use thiserror::Error;
use tracing::{error, trace};

use crate::write::replicate::{PartitionReplicatorActor, PartitionReplicatorActorArgs};

pub mod circuit_breaker;
pub mod confirmation;
pub mod read;
pub mod write;

/// Maximum number of request forwards allowed to prevent loops
const MAX_FORWARDS: u8 = 3;

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    pub kameo: remote::Behaviour,
    pub topology: sierradb_topology::Behaviour<RemoteActorRef<ClusterActor>>,
    pub mdns: mdns::tokio::Behaviour,
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
    // / Number of sequences behind before triggering catch-up
    // pub replication_catch_up_threshold: u64,
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
            // replication_catch_up_threshold,
        }: Self::Args,
        actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        let local_peer_id = keypair.public().to_peer_id();
        trace!(
            %local_peer_id,
            partitions = %assigned_partitions.len(),
            "Starting swarm actor"
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
                let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;

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
                    mdns,
                })
            })?
            .build();

        for addr in listen_addrs {
            swarm.listen_on(addr)?;
        }

        let replicator_refs = assigned_partitions
            .iter()
            .map(|partition_id| {
                (
                    *partition_id,
                    PartitionReplicatorActor::spawn_with_mailbox(
                        PartitionReplicatorActorArgs {
                            partition_id: *partition_id,
                            database: database.clone(),
                            buffer_size: replication_buffer_size,
                            buffer_timeout: replication_buffer_timeout,
                        },
                        mailbox::bounded(1_000),
                    ),
                )
            })
            .collect();

        let confirmation_actor = ConfirmationActor::new(
            database.dir().clone(),
            database.total_buckets(),
            replication_factor,
            assigned_partitions,
        );
        let watermarks = confirmation_actor.manager.get_watermarks();
        let confirmation_ref = Actor::spawn_in_thread(confirmation_actor);

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
