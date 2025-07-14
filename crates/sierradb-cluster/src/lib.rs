use std::{
    collections::{HashMap, HashSet},
    io, iter,
    panic::panic_any,
    sync::Arc,
    time::Duration,
};

use arrayvec::ArrayVec;
use confirmation::{
    AtomicWatermark,
    actor::{ConfirmationActor, UpdateConfirmation},
};
use futures::StreamExt;
use kameo::{mailbox::Signal, prelude::*};
use libp2p::{
    BehaviourBuilderError, Multiaddr, PeerId, Swarm, TransportError, gossipsub, identity::Keypair,
    mdns, noise, tcp, yamux,
};
use partition_consensus::PartitionManager;
use serde::{Deserialize, Serialize};
use sierradb::{
    MAX_REPLICATION_FACTOR,
    bucket::{PartitionHash, PartitionId, segment::CommittedEvents},
    database::{Database, Transaction},
    error::WriteError,
    id::uuid_to_partition_hash,
    writer_thread_pool::AppendResult,
};
use smallvec::{SmallVec, smallvec};
use thiserror::Error;
use tracing::{error, instrument, trace};
use uuid::Uuid;

use crate::behaviour::Behaviour;

pub mod behaviour;
pub mod confirmation;
pub mod partition_consensus;
pub mod transaction;

/// Maximum number of request forwards allowed to prevent loops
const MAX_FORWARDS: u8 = 3;

#[derive(RemoteActor)]
pub struct ClusterActor {
    local_peer_id: PeerId,
    database: Database,
    swarm: Swarm<Behaviour>,
    confirmation_ref: ActorRef<ConfirmationActor>,
    watermarks: HashMap<PartitionId, Arc<AtomicWatermark>>,
}

impl ClusterActor {
    /// Resolves where a write request should be directed
    fn resolve_write_destination(
        &self,
        metadata: &WriteRequestMetadata,
    ) -> Result<WriteDestination, ClusterError> {
        // Check for too many forwards
        if metadata.hop_count > MAX_FORWARDS {
            return Err(ClusterError::TooManyForwards);
        }

        // Get partition distribution
        let partitions = self
            .swarm
            .behaviour()
            .partitions
            .manager
            .get_available_partitions_for_key(metadata.partition_hash);

        // Check quorum requirements
        let required_quorum =
            (self.swarm.behaviour().partitions.manager.replication_factor as usize / 2) + 1;
        if partitions.len() < required_quorum {
            return Err(ClusterError::InsufficientPartitionsForQuorum {
                alive: partitions.len() as u8,
                required: required_quorum as u8,
            });
        }

        // Get leader partition info
        let (primary_partition_id, primary_cluster_ref) = partitions
            .first()
            .cloned()
            .ok_or(ClusterError::PartitionUnavailable)?;

        // Check if we are the leader
        if self
            .swarm
            .behaviour()
            .partitions
            .manager
            .has_partition(primary_partition_id)
            && primary_cluster_ref.id().peer_id().unwrap() == &self.local_peer_id
        {
            // Extract replica partitions from the partitions list
            // Skip the first partition (primary) and only include partitions we're not the
            // leader for
            let replica_partitions = partitions
                .iter()
                .skip(1) // Skip the primary partition (first in the list)
                .cloned()
                .collect::<Vec<_>>();

            Ok(WriteDestination::Local {
                primary_partition_id,
                replica_partitions,
            })
        } else {
            Ok(WriteDestination::Remote {
                primary_cluster_ref,
                partitions: Box::new(partitions),
            })
        }
    }

    /// Routes a write request to the appropriate destination
    fn route_write_request(
        &mut self,
        destination: WriteDestination,
        transaction: Transaction,
        metadata: WriteRequestMetadata,
        reply_sender: Option<ReplySender<Result<AppendResult, ClusterError>>>,
    ) {
        match destination {
            WriteDestination::Local {
                primary_partition_id,
                replica_partitions,
            } => {
                transaction::spawn(
                    self.database.clone(),
                    self.confirmation_ref.clone(),
                    primary_partition_id,
                    replica_partitions,
                    self.swarm.behaviour().partitions.manager.replication_factor,
                    transaction,
                    reply_sender,
                );
            }
            WriteDestination::Remote {
                primary_cluster_ref,
                partitions,
            } => {
                // We need to forward to the leader or an alternative peer
                // Check if we've already tried this peer (to prevent loops)
                if metadata
                    .tried_peers
                    .contains(primary_cluster_ref.id().peer_id().unwrap())
                {
                    // Try the next best partition/peer if available
                    for (_partition_id, cluster_ref) in partitions.iter().skip(1) {
                        if !metadata
                            .tried_peers
                            .contains(cluster_ref.id().peer_id().unwrap())
                        {
                            // Found an untried peer, forward to them
                            self.send_forward_request(
                                cluster_ref.clone(),
                                transaction,
                                metadata,
                                reply_sender,
                            );
                            return;
                        }
                    }

                    // No untried peers left
                    if let Some(tx) = reply_sender {
                        tx.send(Err(ClusterError::NoAvailableLeaders));
                    }

                    return;
                }

                // Forward to the leader peer
                self.send_forward_request(primary_cluster_ref, transaction, metadata, reply_sender);
            }
        }
    }

    /// Sends a forward request to a specific peer
    fn send_forward_request(
        &mut self,
        cluster_ref: RemoteActorRef<ClusterActor>,
        transaction: Transaction,
        mut metadata: WriteRequestMetadata,
        reply_sender: Option<ReplySender<Result<AppendResult, ClusterError>>>,
    ) {
        // Add this peer to the tried list
        metadata
            .tried_peers
            .insert(*cluster_ref.id().peer_id().unwrap());

        match reply_sender {
            Some(tx) => {
                tokio::spawn(async move {
                    let res = cluster_ref
                        .ask(&ExecuteTransaction {
                            transaction,
                            metadata,
                        })
                        .mailbox_timeout(Duration::from_secs(10))
                        .reply_timeout(Duration::from_secs(10))
                        .await;

                    tx.send(res.map_err(ClusterError::from));
                });
            }
            None => {
                cluster_ref
                    .tell(&ExecuteTransaction {
                        transaction,
                        metadata,
                    })
                    .send()
                    .expect("execute transaction cannot fail serialization");
            }
        }
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

impl Actor for ClusterActor {
    type Args = ClusterArgs;
    type Error = ClusterError;

    async fn on_start(
        ClusterArgs {
            keypair: key,
            database,
            listen_addrs,
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

        let kameo = remote::Behaviour::new(
            key.public().to_peer_id(),
            remote::messaging::Config::default(),
        );

        kameo.init_global();

        let cluster_ref = actor_ref.into_remote_ref().await;

        // Build the libp2p swarm with all required behaviors
        let mut swarm = libp2p::SwarmBuilder::with_existing_identity(key)
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
                let partition_manager = PartitionManager::new(
                    cluster_ref.clone(),
                    vec![cluster_ref],
                    num_partitions,
                    replication_factor,
                    assigned_partitions.iter().copied().collect(),
                    None,
                    heartbeat_timeout,
                );

                // Create partition ownership behavior
                let partitions = partition_consensus::Behaviour::new(
                    gossipsub,
                    partition_manager,
                    None,
                    heartbeat_interval,
                );

                Ok(Behaviour {
                    kameo,
                    partitions,
                    mdns,
                })
            })?
            .build();

        for addr in listen_addrs {
            swarm.listen_on(addr)?;
        }

        let confirmation_actor = ConfirmationActor::new(
            database.dir().clone(),
            database.total_buckets(),
            replication_factor,
        );
        let watermarks = confirmation_actor.manager.get_watermarks();
        let confirmation_ref = Actor::spawn_in_thread(confirmation_actor);

        Ok(ClusterActor {
            local_peer_id,
            database,
            swarm,
            confirmation_ref,
            watermarks,
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

/// Represents a destination for a write request
enum WriteDestination {
    /// Process locally as the leader
    Local {
        primary_partition_id: PartitionId,
        replica_partitions: Vec<(PartitionId, RemoteActorRef<ClusterActor>)>,
    },
    /// Forward to a remote leader
    Remote {
        primary_cluster_ref: RemoteActorRef<ClusterActor>,
        partitions:
            Box<ArrayVec<(PartitionId, RemoteActorRef<ClusterActor>), MAX_REPLICATION_FACTOR>>,
    },
}

/// Message to replicate a write to another partition
#[derive(Debug, Serialize, Deserialize)]
pub struct ExecuteTransaction {
    pub transaction: Transaction,
    pub metadata: WriteRequestMetadata,
}

impl ExecuteTransaction {
    pub fn new(transaction: Transaction) -> Self {
        let partition_hash = uuid_to_partition_hash(transaction.partition_key());

        let metadata = WriteRequestMetadata {
            hop_count: 0,
            tried_peers: HashSet::new(),
            partition_hash,
        };

        ExecuteTransaction {
            transaction,
            metadata,
        }
    }
}

#[remote_message("e0e82b26-9528-4afb-a819-c5914c08b218")]
impl Message<ExecuteTransaction> for ClusterActor {
    type Reply = DelegatedReply<Result<AppendResult, ClusterError>>;

    #[instrument(skip(self, ctx))]
    async fn handle(
        &mut self,
        msg: ExecuteTransaction,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let (delegated_reply, reply_sender) = ctx.reply_sender();

        match self.resolve_write_destination(&msg.metadata) {
            Ok(destination) => {
                self.route_write_request(destination, msg.transaction, msg.metadata, reply_sender)
            }
            Err(err) => match reply_sender {
                Some(tx) => {
                    tx.send(Err(err));
                }
                None => {
                    panic_any(err);
                }
            },
        }

        delegated_reply
    }
}

/// Message to replicate a write to another partition
#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicateWrite {
    pub partition_id: PartitionId,
    pub transaction: Transaction,
    pub transaction_id: Uuid,
    pub origin_partition: PartitionId,
}

#[remote_message("ae8dc4cc-e382-4a68-9451-d10c5347d3c9")]
impl Message<ReplicateWrite> for ClusterActor {
    type Reply = DelegatedReply<Result<AppendResult, ClusterError>>;

    #[instrument(skip(self, ctx))]
    async fn handle(
        &mut self,
        msg: ReplicateWrite,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if !self
            .swarm
            .behaviour()
            .partitions
            .manager
            .has_partition(msg.partition_id)
        {
            panic!("got a replicate write request when we dont own the partition");
        }

        let (delegated_reply, reply_sender) = ctx.reply_sender();

        let database = self.database.clone();

        tokio::spawn(async move {
            // Replicate the write to our local database
            let result = database
                .append_events(msg.partition_id, msg.transaction)
                .await;

            match reply_sender {
                Some(tx) => {
                    tx.send(result.map_err(ClusterError::from));
                }
                None => {
                    if let Err(err) = &result {
                        error!(
                            transaction_id = %msg.transaction_id,
                            partition_id = msg.partition_id,
                            ?err,
                            "local replication failed with error"
                        );
                    }
                }
            }
        });

        delegated_reply
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfirmTransaction {
    pub partition_id: PartitionId,
    pub transaction_id: Uuid,
    pub event_ids: SmallVec<[Uuid; 4]>,
    pub event_versions: SmallVec<[u64; 4]>,
    pub confirmation_count: u8,
}

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum ConfirmTransactionError {
    #[error("events length mismatch")]
    EventsLengthMismatch,
    #[error("event id mismatch")]
    EventIdMismatch,
    #[error("transaction not found")]
    TransactionNotFound,
    #[error("read error: {0}")]
    Read(String),
    #[error("write error: {0}")]
    Write(String),
}

#[remote_message("93087af8-6b1f-4df0-8225-13fe694203c1")]
impl Message<ConfirmTransaction> for ClusterActor {
    type Reply = Result<(), ConfirmTransactionError>;

    #[instrument(skip(self, _ctx))]
    async fn handle(
        &mut self,
        msg: ConfirmTransaction,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let Some(first_event_id) = msg.event_ids.first() else {
            return Err(ConfirmTransactionError::EventsLengthMismatch);
        };

        let offsets = match self
            .database
            .read_transaction(msg.partition_id, *first_event_id, true)
            .await
            .map_err(|err| ConfirmTransactionError::Read(err.to_string()))?
        {
            Some(CommittedEvents::Single(event)) => {
                smallvec![event.offset]
            }
            Some(CommittedEvents::Transaction { events, commit }) => {
                if events.len() != msg.event_ids.len() {
                    return Err(ConfirmTransactionError::EventsLengthMismatch);
                }

                events
                    .into_iter()
                    .zip(msg.event_ids)
                    .map(|(event, event_id)| {
                        if event.event_id != event_id {
                            return Err(ConfirmTransactionError::EventIdMismatch);
                        }

                        Ok(event.offset)
                    })
                    .chain(iter::once(Ok(commit.offset)))
                    .collect::<Result<_, _>>()?
            }
            None => {
                return Err(ConfirmTransactionError::TransactionNotFound);
            }
        };

        self.database
            .set_confirmations(
                msg.partition_id,
                offsets,
                msg.transaction_id,
                msg.confirmation_count,
            )
            .await
            .map_err(|err| ConfirmTransactionError::Write(err.to_string()))?;

        let _ = self
            .confirmation_ref
            .tell(UpdateConfirmation {
                partition_id: msg.partition_id,
                versions: msg.event_versions,
                confirmation_count: msg.confirmation_count,
            })
            .await;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteRequestMetadata {
    /// Number of hops this request has taken
    pub hop_count: u8,
    /// Nodes that have already tried to process this request
    pub tried_peers: HashSet<PeerId>,
    /// Original partition hash
    pub partition_hash: PartitionHash,
}

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum ClusterError {
    #[error("insufficient partitions alive for quorum ({alive}/{required})")]
    InsufficientPartitionsForQuorum { alive: u8, required: u8 },
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
    // #[error("read error: {0}")]
    // Read(String),
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
