pub mod consensus;
pub mod partition_actor;

use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    future::Future,
    io,
    ops::ControlFlow,
    pin,
    sync::Arc,
    task,
    time::Duration,
};

use consensus::{
    ConsensusError, ConsensusEvent, ConsensusMessage,
    gossip::{ConsensusGossipActor, ProcessGossipMessage},
    leadership::{
        AddKnownPeer, CheckFailedLeaders, GetLeader, Initialize, IsLeader, LeadershipActor,
        PrintDiagnostics, RemoveKnownPeer, SendHeartbeats,
    },
    term_store::TermStoreActor,
};
use futures::{FutureExt, StreamExt, ready};
use kameo::{actor::ActorRef, request::MessageSendSync};
use libp2p::{
    Multiaddr, PeerId, StreamProtocol, TransportError,
    core::transport::ListenerId,
    gossipsub::{self, IdentTopic, MessageId, PublishError},
    identify,
    identity::Keypair,
    mdns, ping,
    request_response::{
        self, InboundRequestId, OutboundRequestId, ProtocolSupport, ResponseChannel,
    },
    swarm::{ConnectionId, NetworkBehaviour, SwarmEvent},
};
use libp2p_partition_ownership::manager::PartitionManager;
use partition_actor::PartitionActor;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    bucket::{BucketId, writer_thread_pool::AppendEventsBatch},
    database::{CurrentVersion, Database},
    error::SwarmError,
};

#[derive(Clone, Debug)]
pub struct SwarmRef {
    swarm_tx: SwarmSender,
    local_peer_id: PeerId,
}

impl SwarmRef {
    pub fn append_events(
        &self,
        partition_id: u16,
        batch: Arc<AppendEventsBatch>,
    ) -> Result<SwarmFuture<Result<(), SwarmError>>, SwarmError> {
        self.swarm_tx
            .send_with_reply(|reply_tx| Command::AppendEvents {
                partition_id,
                batch,
                reply_tx,
            })
    }
}

pub struct Swarm {
    swarm: libp2p::Swarm<Behaviour>,
    database: Database,
    num_buckets: u16,
    replication_factor: u8,
    cmd_tx: mpsc::UnboundedSender<Command>,
    cmd_rx: mpsc::UnboundedReceiver<Command>,
    owned_partitions: HashSet<u16>,
    pending_appends: HashMap<Uuid, AppendEventsRequest>,
    partition_actors: HashMap<u16, ActorRef<PartitionActor>>,
}

impl Swarm {
    pub fn new(
        key: Keypair,
        database: Database,
        num_partitions: u16,
        replication_factor: u8,
        assigned_partitions: HashSet<u16>,
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
                        StreamProtocol::new("/eventus/events/1"),
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
                    Duration::from_secs(6),
                );

                let partition_ownership = libp2p_partition_ownership::behaviour::Behaviour::new(
                    gossipsub,
                    partition_manager,
                    None,
                    Duration::from_secs(1),
                );

                Ok(Behaviour {
                    partition_ownership,
                    mdns,
                    req_resp,
                })
            })?
            .build();

        Self::with_swarm(swarm, database, num_partitions, replication_factor)
    }

    pub fn with_swarm(
        swarm: libp2p::Swarm<Behaviour>,
        database: Database,
        num_buckets: u16,
        replication_factor: u8,
    ) -> Result<Self, SwarmError> {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        let partition_actors = swarm
            .behaviour()
            .partition_ownership
            .partition_manager
            .assigned_partitions
            .iter()
            .copied()
            .map(|partition_id| (partition_id, kameo::spawn(PartitionActor)))
            .collect();

        // Create the consensus event channel
        // let (event_tx, event_rx) = mpsc::unbounded_channel();

        // // Create the leadership actor
        // let leadership_actor = LeadershipActor::new(
        //     SwarmSender(cmd_tx.clone()),
        //     swarm.local_peer_id().clone(),
        //     term_store_ref.clone(),
        //     replication_factor,
        //     num_buckets,
        // );
        // let leadership_ref = kameo::spawn(leadership_actor);

        // Create the gossip actor
        // let gossip_actor = ConsensusGossipActor::new(
        //     "eventus/consensus/1",
        //     swarm.local_peer_id().clone(),
        //     leadership_ref.clone(),
        // );
        // let consensus_topic = gossip_actor.topic().clone();
        // let gossip_ref = kameo::spawn(gossip_actor);

        // Subscribe to the consensus topic
        // swarm
        //     .behaviour_mut()
        //     .gossipsub
        //     .subscribe(&consensus_topic)
        //     .unwrap(); // TODO: Unwrap shouldnt be used

        // leadership_ref
        //     .tell(Initialize {
        //         gossip_ref: gossip_ref.clone(),
        //         event_tx,
        //     })
        //     .send_sync()
        //     .unwrap();

        Ok(Swarm {
            swarm,
            database,
            // leadership_ref,
            // gossip_ref,
            num_buckets,
            replication_factor,
            cmd_tx,
            cmd_rx,
            // consensus_event_rx: event_rx,
            // consensus_topic,
            // owned_buckets,
            owned_partitions: HashSet::new(),
            // peer_buckets: HashMap::new(),
            pending_appends: HashMap::new(),
            // bucket_owners_topic,
            partition_actors,
        })
    }

    /// Starts listening on the given address.
    /// Returns an error if the address is not supported.
    ///
    /// Listeners report their new listening addresses as [`SwarmEvent::NewListenAddr`].
    /// Depending on the underlying transport, one listener may have multiple listening addresses.
    pub fn listen_on(&mut self, addr: Multiaddr) -> Result<ListenerId, TransportError<io::Error>> {
        self.swarm.listen_on(addr)
    }

    pub fn spawn(self) -> SwarmRef {
        let tx = self.cmd_tx.clone();
        let swarm_ref = SwarmRef {
            swarm_tx: SwarmSender(tx),
            local_peer_id: *self.swarm.local_peer_id(),
        };
        tokio::spawn(self.run());

        swarm_ref
    }

    pub async fn next(&mut self) {
        tokio::select! {
            Some(cmd) = self.cmd_rx.recv() => self.handle_command(cmd),
            Some(event) = self.swarm.next() => self.handle_swarm_event(event).await,
            // Some(event) = self.consensus_event_rx.recv() => self.handle_consensus_event(event),
        }
    }

    async fn run(mut self) {
        loop {
            self.next().await;
        }
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::AppendEvents {
                partition_id,
                batch,
                reply_tx,
            } => {
                self.handle_append_events_command(partition_id, batch, reply_tx);
            }
            Command::AppendEventsResult {
                request_id,
                bucket_id,
                channel,
                result,
            } => match channel {
                Some(channel) => {
                    info!("sending append events result to channel");
                    let resp = match result {
                        Ok(latest_versions) => Resp::AppendEventsSuccess {
                            request_id,
                            bucket_id,
                            latest_versions,
                        },
                        Err(err) => Resp::AppendEventsFailure {
                            request_id,
                            bucket_id,
                            error: err,
                        },
                    };

                    let _ = self
                        .swarm
                        .behaviour_mut()
                        .req_resp
                        .send_response(channel, resp);
                }
                None => {
                    info!("handling append events result without sending to any channel");
                    let status = match result {
                        Ok(latest_versions) => AppendEventsStatus::Success(latest_versions),
                        Err(err) => AppendEventsStatus::Failed(err),
                    };

                    self.update_pending_request(request_id, bucket_id, status);
                }
            },
        }
    }

    fn handle_append_events_command(
        &mut self,
        partition_key: u16,
        events: Arc<AppendEventsBatch>,
        reply_tx: oneshot::Sender<Result<(), SwarmError>>,
    ) {
        // Distributed Write Protocol
        //
        // Phase 1: Leader Selection & Version Assignment
        // Step 1: Client request is forwarded to the partition leader (determined by the first alive partition in the replicated partitions)
        // Step 2: Leader validates its leadership term and reserves sequential stream & partition versions
        // Step 3: Leader assigns these versions to events in the append request
        //
        // Phase 2: Distributed Replication
        // Step 4: Leader broadcasts write requests to all replica partitions (determined by replication factor)
        // Step 5: Each replica immediately writes events to disk in "unconfirmed" state
        // Step 6: Replicas respond to leader with success/failure of their write operation
        // Step 7: Leader collects responses until quorum threshold is reached: (replication_factor/2)+1
        //
        // Phase 3: Commitment & Response
        // Step 8: If quorum achieved, leader responds to client with success and assigned versions
        // Step 9: Leader broadcasts confirmation message to all successful replicas
        // Step 10: Replicas mark events as "confirmed" in their local storage
        //
        // Failure Conditions:
        // - Leadership contention: Replica rejects write if it believes another node is the leader
        // - Version mismatch: Replica rejects if stream/partition versions don't align with local state
        // - Timeout: Leader fails the operation if quorum isn't reached within timeout period
        // - Node failure: Background reconciliation process handles any partially confirmed writes
        //
        // Recovery guarantees:
        // - Events remain in "unconfirmed" state until explicit confirmation
        // - Background processes reconcile unconfirmed events during node recovery
        // - Only confirmed events are visible to consumers

        let mut target_partition_ids = self
            .swarm
            .behaviour()
            .partition_ownership
            .partition_manager
            .get_available_partitions_for_key(partition_key);

        // let mut owned_target_partition_ids = Vec::new();
        // // https://doc.rust-lang.org/stable/std/vec/struct.Vec.html#method.extract_if
        // {
        //     let mut i = 0;
        //     while i < target_partition_ids.len() {
        //         if self
        //             .swarm
        //             .behaviour()
        //             .partition_ownership
        //             .partition_manager
        //             .has_partition(target_partition_ids[i])
        //         {
        //             let bucket_id = target_partition_ids.remove(i);
        //             owned_target_partition_ids.push(bucket_id);
        //         } else {
        //             i += 1;
        //         }
        //     }
        // };

        // let buckets_peers_res = target_partition_ids
        //     .into_iter()
        //     .map(|bucket_id| {
        //         self.peer_buckets
        //             .get(&bucket_id)
        //             .map(|peer_id| (bucket_id, peer_id))
        //             .ok_or(SwarmError::BucketPeerNotFound { bucket_id })
        //     })
        //     .collect::<Result<Vec<_>, _>>();
        let buckets_peers = match buckets_peers_res {
            Ok(buckets_peers) => buckets_peers,
            Err(err) => {
                let _ = reply_tx.send(Err(err));
                return;
            }
        };

        let request_id = Uuid::new_v4();
        let mut bucket_statuses = HashMap::with_capacity(buckets_peers.len());

        for (bucket_id, peer_id) in buckets_peers {
            self.swarm.behaviour_mut().req_resp.send_request(
                peer_id,
                Req::AppendEvents {
                    bucket_id,
                    events: Arc::clone(&events),
                    request_id,
                },
            );
            bucket_statuses.insert(bucket_id, AppendEventsStatus::Pending);
        }
        for bucket_id in owned_target_partition_ids {
            let db = self.database.clone();
            let cmd_tx = self.cmd_tx.clone();
            let events = Arc::clone(&events);

            tokio::spawn(async move {
                let res = db.append_events(bucket_id, events).await;
                if let Err(err) = &res {
                    error!("failed to append events: {err}");
                }
                let _ = cmd_tx.send(Command::AppendEventsResult {
                    request_id,
                    bucket_id,
                    channel: None,
                    result: res.map_err(|err| SwarmError::Write(err.to_string())),
                });
            });

            bucket_statuses.insert(bucket_id, AppendEventsStatus::Processing);
        }

        self.pending_appends.insert(
            request_id,
            AppendEventsRequest {
                bucket_statuses,
                reply_tx: Some(reply_tx),
            },
        );
    }

    fn handle_consensus_event(&mut self, event: ConsensusEvent) {
        match event {
            ConsensusEvent::BecameLeader { partition_id, term } => {
                info!(
                    "Became leader for partition {} with term {}",
                    partition_id, term
                );
                // Handle becoming leader
                let changed = self.owned_partitions.insert(partition_id);

                if changed {
                    let s = self
                        .owned_partitions
                        .iter()
                        .map(|partition_id| partition_id.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    println!("{s}\n");
                }
            }
            ConsensusEvent::LostLeadership { partition_id } => {
                info!("Lost leadership for partition {}", partition_id);
                // Handle losing leadership
                let changed = self.owned_partitions.remove(&partition_id);

                if changed {
                    let s = self
                        .owned_partitions
                        .iter()
                        .map(|partition_id| partition_id.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    println!("{s}\n");
                }
            }
            ConsensusEvent::LeaderChanged {
                partition_id,
                new_leader,
                term,
            } => {
                info!(
                    "Leader for partition {} changed to {:?} with term {}",
                    partition_id, new_leader, term
                );
                // Update local knowledge about leaders
                let changed = if &new_leader == self.swarm.local_peer_id() {
                    self.owned_partitions.insert(partition_id)
                } else {
                    self.owned_partitions.remove(&partition_id)
                };

                if changed {
                    let s = self
                        .owned_partitions
                        .iter()
                        .map(|partition_id| partition_id.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    println!("{s}\n");
                }
            }
        }
    }

    async fn handle_swarm_event(&mut self, event: SwarmEvent<BehaviourEvent>) {
        match event {
            SwarmEvent::Behaviour(BehaviourEvent::ReqResp(event)) => {
                self.handle_req_resp_event(event)
            }
            SwarmEvent::Behaviour(BehaviourEvent::Mdns(event)) => {
                self.handle_mdns_event(event).await
            }
            SwarmEvent::ConnectionEstablished {
                peer_id,
                connection_id,
                endpoint,
                num_established,
                concurrent_dial_errors,
                established_in,
            } => {}
            SwarmEvent::ConnectionClosed {
                peer_id,
                connection_id,
                endpoint,
                num_established,
                cause,
            } => {}
            SwarmEvent::IncomingConnection {
                connection_id,
                local_addr,
                send_back_addr,
            } => {}
            SwarmEvent::IncomingConnectionError {
                connection_id,
                local_addr,
                send_back_addr,
                error,
            } => {}
            SwarmEvent::OutgoingConnectionError {
                connection_id,
                peer_id,
                error,
            } => {}
            SwarmEvent::NewListenAddr {
                listener_id,
                address,
            } => {}
            SwarmEvent::ExpiredListenAddr {
                listener_id,
                address,
            } => {}
            SwarmEvent::ListenerClosed {
                listener_id,
                addresses,
                reason,
            } => {}
            SwarmEvent::ListenerError { listener_id, error } => {}
            SwarmEvent::Dialing {
                peer_id,
                connection_id,
            } => {}
            SwarmEvent::NewExternalAddrCandidate { address } => {}
            SwarmEvent::ExternalAddrConfirmed { address } => {}
            SwarmEvent::ExternalAddrExpired { address } => {}
            SwarmEvent::NewExternalAddrOfPeer { peer_id, address } => {}
            _ => {}
        }
    }

    async fn handle_gossipsub_event(&mut self, event: gossipsub::Event) {
        match event {
            gossipsub::Event::Message {
                propagation_source,
                message_id,
                message,
            } => {
                self.handle_gossipsub_message(propagation_source, message_id, message)
                    .await;
            }
            gossipsub::Event::Subscribed { peer_id, topic } => {}
            gossipsub::Event::Unsubscribed { peer_id, topic } => todo!(),
            gossipsub::Event::GossipsubNotSupported { peer_id } => todo!(),
            gossipsub::Event::SlowPeer {
                peer_id,
                failed_messages,
            } => todo!(),
        }
    }

    fn handle_req_resp_event(&mut self, event: request_response::Event<Req, Resp>) {
        match event {
            request_response::Event::Message {
                peer,
                connection_id,
                message,
            } => match message {
                request_response::Message::Request {
                    request_id,
                    request,
                    channel,
                } => self.handle_req(peer, connection_id, request_id, request, channel),
                request_response::Message::Response {
                    request_id,
                    response,
                } => self.handle_resp(peer, connection_id, request_id, response),
            },
            request_response::Event::OutboundFailure {
                peer,
                connection_id,
                request_id,
                error,
            } => {
                error!(%peer, %connection_id, %request_id, "outbound failure: {error}");
            }
            request_response::Event::InboundFailure {
                peer,
                connection_id,
                request_id,
                error,
            } => {
                error!("inbound failure: {error}");
            }
            request_response::Event::ResponseSent {
                peer,
                connection_id,
                request_id,
            } => {}
        }
    }

    fn handle_req(
        &mut self,
        peer: PeerId,
        connection_id: ConnectionId,
        request_id: InboundRequestId,
        request: Req,
        channel: ResponseChannel<Resp>,
    ) {
        match request {
            Req::AppendEvents {
                bucket_id,
                events,
                request_id,
            } => {
                info!(%bucket_id, "handling append events");
                self.pending_appends
                    .entry(request_id)
                    .and_modify(|req| {
                        req.bucket_statuses.insert(bucket_id, AppendEventsStatus::Processing);
                    })
                    .or_insert_with(|| AppendEventsRequest {
                        bucket_statuses: HashMap::from_iter([(
                            bucket_id,
                            AppendEventsStatus::Pending,
                        )]),
                        reply_tx: None
                    });

                let db = self.database.clone();
                let cmd_tx = self.cmd_tx.clone();

                tokio::spawn(async move {
                    let res = db.append_events(bucket_id, events).await;
                    if let Err(err) = &res {
                        error!("failed to append events: {err}");
                    }
                    let _ = cmd_tx.send(Command::AppendEventsResult {
                        request_id,
                        bucket_id,
                        channel: Some(channel),
                        result: res.map_err(|err| SwarmError::Write(err.to_string())),
                    });
                });
            }
            Req::ConfirmEvents {
                // bucket_id,
                // event_ids,
                // request_id,
            } => todo!(),
        }
    }

    fn handle_resp(
        &mut self,
        peer: PeerId,
        connection_id: ConnectionId,
        request_id: OutboundRequestId,
        response: Resp,
    ) {
        match response {
            Resp::AppendEventsSuccess {
                request_id,
                bucket_id,
                latest_versions,
            } => {
                self.update_pending_request(
                    request_id,
                    bucket_id,
                    AppendEventsStatus::Success(latest_versions),
                );
            }
            Resp::AppendEventsFailure {
                request_id,
                bucket_id,
                error,
            } => {
                self.update_pending_request(
                    request_id,
                    bucket_id,
                    AppendEventsStatus::Failed(error),
                );
            }
            Resp::ConfirmEventsSuccess {
                request_id,
                bucket_id,
            } => {}
            Resp::ConfirmEventsFailure {
                request_id,
                bucket_id,
                error,
            } => {}
        }
    }

    async fn handle_mdns_event(&mut self, event: mdns::Event) {
        match event {
            mdns::Event::Discovered(list) => {
                for (peer_id, _multiaddr) in list {
                    info!("mDNS discovered a new peer: {peer_id}");
                    self.swarm
                        .behaviour_mut()
                        .partition_ownership
                        .add_explicit_peer(peer_id);
                }
            }
            mdns::Event::Expired(list) => {
                for (peer_id, _multiaddr) in list {
                    info!("mDNS discover peer has expired: {peer_id}");
                    // self.swarm
                    //     .behaviour_mut()
                    //     .partition_ownership
                    //     .remove_explicit_peer(&peer_id);
                }
            }
        }
    }

    fn update_pending_request(
        &mut self,
        request_id: Uuid,
        bucket_id: BucketId,
        status: AppendEventsStatus,
    ) {
        let Entry::Occupied(mut requests) = self.pending_appends.entry(request_id) else {
            warn!("request id not found in pending appends");
            return;
        };

        let Some(bucket_status) = requests.get_mut().bucket_statuses.get_mut(&bucket_id) else {
            warn!("outbound bucket id not found in pending appends");
            return;
        };

        *bucket_status = status;

        if requests.get().bucket_statuses.values().all(|status| {
            !matches!(
                status,
                AppendEventsStatus::Pending | AppendEventsStatus::Processing
            )
        }) {
            // No more pending requests, time to process the results
            let requests = requests.remove();
            let latest_versions =
                requests
                    .bucket_statuses
                    .values()
                    .try_fold(None, |acc, status| match status {
                        AppendEventsStatus::Failed(_) => ControlFlow::Break(None),
                        AppendEventsStatus::Success(latest_versions) => match acc {
                            Some(acc_latest_versions) => {
                                if &acc_latest_versions != latest_versions {
                                    ControlFlow::Break(Some(()))
                                } else {
                                    ControlFlow::Continue(Some(acc_latest_versions))
                                }
                            }
                            None => ControlFlow::Continue(Some(latest_versions.clone())),
                        },
                        AppendEventsStatus::Pending => ControlFlow::Break(None),
                        AppendEventsStatus::Processing => ControlFlow::Break(None),
                    });
            let mut errors: Vec<_> = requests
                .bucket_statuses
                .into_iter()
                .filter_map(|(_, status)| match status {
                    AppendEventsStatus::Failed(err) => Some(err),
                    _ => None,
                })
                .collect();
            if matches!(latest_versions, ControlFlow::Break(Some(()))) {
                errors.push(SwarmError::StreamVersionMismatch);
            }

            if errors.is_empty() {
                // All successful
                println!("ALL SUCCESSFUL");
                if let Some(reply_tx) = requests.reply_tx {
                    let _ = reply_tx.send(Ok(()));
                }
                // TODO: Perform confirmations
            } else {
                // There's been an error
                // We need to contact all the successful responses to tell this request failed,
                // marking the events as deleted.
                println!("THERES BEEN AN ERROR: {errors:#?}");
            }
        }
    }
}

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    pub partition_ownership: libp2p_partition_ownership::behaviour::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
    pub req_resp: request_response::cbor::Behaviour<Req, Resp>,
}

pub enum Command {
    AppendEvents {
        partition_id: u16,
        batch: Arc<AppendEventsBatch>,
        reply_tx: oneshot::Sender<Result<(), SwarmError>>,
    },
    AppendEventsResult {
        request_id: Uuid,
        bucket_id: BucketId,
        channel: Option<ResponseChannel<Resp>>,
        result: Result<HashMap<Arc<str>, CurrentVersion>, SwarmError>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Req {
    AppendEvents {
        bucket_id: BucketId,
        events: Arc<AppendEventsBatch>,
        request_id: Uuid,
    },
    ConfirmEvents {
        // bucket_id: BucketId,
        // event_ids: Vec<EventId>,
        // request_id: Uuid,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Resp {
    AppendEventsSuccess {
        request_id: Uuid,
        bucket_id: BucketId,
        latest_versions: HashMap<Arc<str>, CurrentVersion>,
    },
    AppendEventsFailure {
        request_id: Uuid,
        bucket_id: BucketId,
        error: SwarmError,
    },
    ConfirmEventsSuccess {
        request_id: Uuid,
        bucket_id: BucketId,
    },
    ConfirmEventsFailure {
        request_id: Uuid,
        bucket_id: BucketId,
        error: SwarmError,
    },
}

struct AppendEventsRequest {
    bucket_statuses: HashMap<BucketId, AppendEventsStatus>,
    reply_tx: Option<oneshot::Sender<Result<(), SwarmError>>>,
}

enum AppendEventsStatus {
    /// Waiting for the write to begin.
    Pending,
    /// The write has started and its being processed.
    Processing,
    /// The write finished with an error.
    Failed(SwarmError),
    /// The write succeeded.
    Success(HashMap<Arc<str>, CurrentVersion>),
}

#[derive(Serialize, Deserialize)]
pub struct BucketOwnerMsg {
    pub bucket_ids: Vec<BucketId>,
    pub peer_id: PeerId,
}

#[derive(Clone, Debug)]
pub struct SwarmSender(mpsc::UnboundedSender<Command>);

impl SwarmSender {
    pub(crate) fn send(&self, cmd: Command) -> Result<(), SwarmError> {
        self.0.send(cmd).map_err(|_| SwarmError::SwarmNotRunning)
    }

    pub(crate) fn send_with_reply<T>(
        &self,
        cmd_fn: impl FnOnce(oneshot::Sender<T>) -> Command,
    ) -> Result<SwarmFuture<T>, SwarmError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let cmd = cmd_fn(reply_tx);
        self.send(cmd)?;

        Ok(SwarmFuture(reply_rx))
    }
}

/// `SwarmFuture` represents a future that contains the response from a remote actor.
///
/// This future is returned when sending a message to a remote actor via the actor swarm.
/// If the response is not needed, the future can simply be dropped without awaiting it.
#[derive(Debug)]
pub struct SwarmFuture<T>(oneshot::Receiver<T>);

impl<T> Future for SwarmFuture<T> {
    type Output = T;

    fn poll(mut self: pin::Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        task::Poll::Ready(
            ready!(self.0.poll_unpin(cx))
                .expect("the oneshot sender should never be dropped before being sent to"),
        )
    }
}
