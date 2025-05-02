use std::collections::{HashMap, HashSet};
use std::iter;

use arrayvec::ArrayVec;
use kameo::actor::ActorRef;
use libp2p::PeerId;
use libp2p::request_response::{self, OutboundFailure, OutboundRequestId, ResponseChannel};
use sierradb::MAX_REPLICATION_FACTOR;
use sierradb::bucket::PartitionId;
use sierradb::bucket::segment::CommittedEvents;
use sierradb::database::{Database, Transaction};
use sierradb::error::{ReadError, WriteError};
use sierradb::id::uuid_to_partition_hash;
use sierradb::writer_thread_pool::AppendResult;
use smallvec::{SmallVec, smallvec};
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::{debug, error, warn};
use uuid::Uuid;

use super::ReplyKind;
use crate::behaviour::{Req, Resp, WriteRequestMetadata};
use crate::error::SwarmError;
use crate::partition_actor::{LeaderWriteRequest, PartitionActor};
use crate::partition_consensus::PartitionManager;
use crate::swarm::{SendResponse, Swarm};
use crate::write_actor::{ReplicaConfirmation, WriteActor};

/// Maximum number of request forwards allowed to prevent loops
const MAX_FORWARDS: u8 = 3;

/// Manages distributed write operations across a cluster
pub struct WriteManager {
    swarm_ref: ActorRef<Swarm>,
    database: Database,
    local_peer_id: PeerId,
    partition_actors: HashMap<u16, ActorRef<PartitionActor>>,
    forwarded_appends:
        HashMap<OutboundRequestId, oneshot::Sender<Result<AppendResult, SwarmError>>>,
    pending_replications: HashMap<(Uuid, PartitionId), ActorRef<WriteActor>>,
}

impl WriteManager {
    /// Creates a new WriteManager
    pub fn new(
        swarm_ref: ActorRef<Swarm>,
        database: Database,
        local_peer_id: PeerId,
        partition_actors: HashMap<u16, ActorRef<PartitionActor>>,
    ) -> Self {
        WriteManager {
            swarm_ref,
            database,
            local_peer_id,
            partition_actors,
            forwarded_appends: HashMap::new(),
            pending_replications: HashMap::new(),
        }
    }

    /// Initiates a write operation locally, forwarding to the leader if
    /// necessary
    pub fn process_local_write(
        &mut self,
        partition_manager: &PartitionManager,
        req_resp: &mut request_response::cbor::Behaviour<Req, Resp>,
        transaction: Transaction,
    ) -> oneshot::Receiver<Result<AppendResult, SwarmError>> {
        let partition_hash = uuid_to_partition_hash(transaction.partition_key());
        let (reply_tx, reply_rx) = oneshot::channel();

        let metadata = WriteRequestMetadata {
            hop_count: 0,
            tried_peers: HashSet::from([self.local_peer_id]),
            partition_hash,
        };

        match self.resolve_write_destination(partition_manager, &metadata) {
            Ok(destination) => self.route_write_request(
                partition_manager,
                req_resp,
                destination,
                transaction,
                metadata,
                ReplyKind::Local(reply_tx),
            ),
            Err(err) => {
                let _ = reply_tx.send(Err(err));
            }
        }

        reply_rx
    }

    /// Processes a write request forwarded through the network
    pub fn process_network_write(
        &mut self,
        partition_manager: &PartitionManager,
        req_resp: &mut request_response::cbor::Behaviour<Req, Resp>,
        channel: ResponseChannel<Resp>,
        transaction: Transaction,
        mut metadata: WriteRequestMetadata,
    ) {
        // Increment hop count when receiving a forwarded request
        metadata.hop_count += 1;

        match self.resolve_write_destination(partition_manager, &metadata) {
            Ok(destination) => self.route_write_request(
                partition_manager,
                req_resp,
                destination,
                transaction,
                metadata,
                ReplyKind::Remote(channel),
            ),
            Err(err) => {
                let _ = req_resp.send_response(channel, Resp::AppendEvents(Err(err)));
            }
        }
    }

    /// Processes a replication request for a transaction
    #[allow(clippy::too_many_arguments)]
    pub fn process_replication(
        &mut self,
        partition_manager: &PartitionManager,
        req_resp: &mut request_response::cbor::Behaviour<Req, Resp>,
        partition_id: PartitionId,
        transaction: Transaction,
        transaction_id: Uuid,
        origin_partition: PartitionId,
        write_actor_ref: ActorRef<WriteActor>,
    ) {
        if partition_manager.has_partition(partition_id) {
            self.handle_local_replication(
                transaction_id,
                partition_id,
                transaction,
                write_actor_ref,
            );
        } else {
            self.forward_replication(
                req_resp,
                partition_manager,
                partition_id,
                transaction,
                transaction_id,
                origin_partition,
                write_actor_ref,
            );
        }
    }

    /// Handles a replication locally
    fn handle_local_replication(
        &self,
        transaction_id: Uuid,
        partition_id: PartitionId,
        transaction: Transaction,
        write_actor_ref: ActorRef<WriteActor>,
    ) {
        debug!(
            %transaction_id,
            partition_id,
            "Replicating write to local partition"
        );

        let database = self.database.clone();

        tokio::spawn(async move {
            // Replicate the write to our local database
            let result = database.append_events(partition_id, transaction).await;

            debug!(
                %transaction_id,
                partition_id,
                ?result,
                "Local replication result"
            );

            if let Err(err) = &result {
                error!(%transaction_id, partition_id, ?err, "Local replication failed with error");
            }

            // Send confirmation back to the write actor
            let _ = write_actor_ref
                .tell(ReplicaConfirmation {
                    partition_id,
                    transaction_id,
                    result: result.map_err(|err| SwarmError::Write(err.to_string())),
                })
                .await;
        });
    }

    /// Forwards a replication request to another node
    #[allow(clippy::too_many_arguments)]
    fn forward_replication(
        &mut self,
        req_resp: &mut request_response::cbor::Behaviour<Req, Resp>,
        partition_manager: &PartitionManager,
        partition_id: PartitionId,
        transaction: Transaction,
        transaction_id: Uuid,
        origin_partition: PartitionId,
        write_actor_ref: ActorRef<WriteActor>,
    ) {
        let partition_owner = partition_manager.get_partition_owner(partition_id);

        if let Some(peer_id) = partition_owner {
            debug!(
                %transaction_id,
                partition_id,
                %peer_id,
                "Forwarding replication request to remote node"
            );

            // Store the write actor ref for when we get a response
            self.pending_replications
                .insert((transaction_id, partition_id), write_actor_ref);

            // Forward the replication request to the correct node
            req_resp.send_request(
                peer_id,
                Req::ReplicateWrite {
                    partition_id,
                    transaction,
                    transaction_id,
                    origin_partition,
                    origin_peer: self.local_peer_id,
                },
            );
        } else {
            // Can't find a node for this partition
            warn!(
                %transaction_id,
                partition_id,
                "No node found for partition during replication"
            );

            self.send_failed_replication_confirmation(
                transaction_id,
                partition_id,
                write_actor_ref,
                SwarmError::NoAvailableLeaders,
            );
        }
    }

    /// Handles a replication request received from the network
    pub fn process_network_replication(
        &self,
        channel: ResponseChannel<Resp>,
        partition_id: PartitionId,
        transaction: Transaction,
        transaction_id: Uuid,
        origin_peer: PeerId,
    ) {
        debug!(
            %transaction_id,
            partition_id,
            %origin_peer,
            "Received replication request from network"
        );

        let swarm_ref = self.swarm_ref.clone();
        let database = self.database.clone();

        tokio::spawn(async move {
            // Process the replication locally
            let result = database
                .append_events(partition_id, transaction)
                .await
                .map_err(|err| SwarmError::Write(err.to_string()));
            let _ = swarm_ref
                .tell(SendResponse {
                    channel,
                    response: Resp::ReplicateWrite {
                        partition_id,
                        transaction_id,
                        result,
                    },
                })
                .await;
        });
    }

    /// Handles a replication response from the network
    pub fn process_replication_response(
        &mut self,
        transaction_id: Uuid,
        partition_id: PartitionId,
        result: Result<AppendResult, SwarmError>,
    ) {
        if let Some(write_actor_ref) = self
            .pending_replications
            .remove(&(transaction_id, partition_id))
        {
            self.send_replication_confirmation(
                transaction_id,
                partition_id,
                result,
                write_actor_ref,
            );
        }
    }

    /// Handles a commit initiated locally as the primary partition
    pub fn process_primary_commit(
        &self,
        partition_id: PartitionId,
        transaction_id: Uuid,
        event_ids: SmallVec<[Uuid; 4]>,
        confirmation_count: u8,
    ) {
        let database = self.database.clone();

        tokio::spawn(async move {
            let result = confirm_transaction(
                &database,
                partition_id,
                transaction_id,
                event_ids,
                confirmation_count,
            )
            .await;

            if let Err(err) = &result {
                error!(%transaction_id, partition_id, "Failed to confirm transaction: {err}");
            }
        });
    }

    /// Handles a commit received from the network as a replica partition
    pub fn process_network_commit(
        &self,
        channel: ResponseChannel<Resp>,
        partition_id: PartitionId,
        transaction_id: Uuid,
        event_ids: SmallVec<[Uuid; 4]>,
        confirmation_count: u8,
    ) {
        let swarm_ref = self.swarm_ref.clone();
        let database = self.database.clone();

        tokio::spawn(async move {
            let result = confirm_transaction(
                &database,
                partition_id,
                transaction_id,
                event_ids,
                confirmation_count,
            )
            .await;

            if let Err(err) = &result {
                error!(%transaction_id, partition_id, "Failed to confirm network transaction: {err}");
            }

            let _ = swarm_ref
                .tell(SendResponse {
                    channel,
                    response: Resp::ConfirmWrite {
                        partition_id,
                        transaction_id,
                        result: result.map_err(|err| SwarmError::Write(err.to_string())),
                    },
                })
                .await;
        });
    }

    /// Handles the result of a forwarded append operation
    pub fn process_append_result(
        &mut self,
        request_id: &OutboundRequestId,
        result: Result<AppendResult, SwarmError>,
    ) {
        if let Some(tx) = self.forwarded_appends.remove(request_id) {
            let _ = tx.send(result);
        }
    }

    /// Handles outbound failure for a request
    pub fn process_outbound_failure(
        &mut self,
        request_id: &OutboundRequestId,
        error: OutboundFailure,
    ) {
        if let Some(tx) = self.forwarded_appends.remove(request_id) {
            let _ = tx.send(Err(SwarmError::OutboundFailure(error)));
        }
    }

    /// Sends a failed replication confirmation to a write actor
    fn send_failed_replication_confirmation(
        &self,
        transaction_id: Uuid,
        partition_id: PartitionId,
        write_actor_ref: ActorRef<WriteActor>,
        err: SwarmError,
    ) {
        tokio::spawn(async move {
            let _ = write_actor_ref
                .tell(ReplicaConfirmation {
                    partition_id,
                    transaction_id,
                    result: Err(err),
                })
                .await;
        });
    }

    /// Sends a replication confirmation to a write actor
    fn send_replication_confirmation(
        &self,
        transaction_id: Uuid,
        partition_id: PartitionId,
        result: Result<AppendResult, SwarmError>,
        write_actor_ref: ActorRef<WriteActor>,
    ) {
        tokio::spawn(async move {
            let _ = write_actor_ref
                .tell(ReplicaConfirmation {
                    partition_id,
                    transaction_id,
                    result,
                })
                .await;
        });
    }

    /// Resolves where a write request should be directed
    fn resolve_write_destination(
        &self,
        partition_manager: &PartitionManager,
        metadata: &WriteRequestMetadata,
    ) -> Result<WriteDestination, SwarmError> {
        // Check for too many forwards
        if metadata.hop_count > MAX_FORWARDS {
            return Err(SwarmError::TooManyForwards);
        }

        // Get partition distribution
        let partitions =
            partition_manager.get_available_partitions_for_key(metadata.partition_hash);

        // Check quorum requirements
        let required_quorum = (partition_manager.replication_factor as usize / 2) + 1;
        if partitions.len() < required_quorum {
            return Err(SwarmError::InsufficientPartitionsForQuorum {
                alive: partitions.len() as u8,
                required: required_quorum as u8,
            });
        }

        // Get leader partition info
        let (primary_partition_id, primary_peer_id) = partitions
            .first()
            .copied()
            .ok_or(SwarmError::PartitionUnavailable)?;

        // Check if we are the leader
        if partition_manager.has_partition(primary_partition_id)
            && primary_peer_id == self.local_peer_id
        {
            // We are the leader - get the partition actor
            let partition_actor_ref = self
                .partition_actors
                .get(&primary_partition_id)
                .ok_or(SwarmError::PartitionActorNotFound {
                    partition_id: primary_partition_id,
                })?
                .clone();

            // Extract replica partitions from the partitions list
            // Skip the first partition (primary) and only include partitions we're not the
            // leader for
            let replica_partitions = partitions
                .iter()
                .skip(1) // Skip the primary partition (first in the list)
                .map(|(partition_id, _)| *partition_id)
                .collect::<Vec<_>>();

            Ok(WriteDestination::Local {
                partition_actor_ref,
                replica_partitions,
            })
        } else {
            Ok(WriteDestination::Remote {
                primary_peer_id,
                partitions: Box::new(partitions),
            })
        }
    }

    /// Routes a write request to the appropriate destination
    fn route_write_request(
        &mut self,
        partition_manager: &PartitionManager,
        req_resp: &mut request_response::cbor::Behaviour<Req, Resp>,
        destination: WriteDestination,
        transaction: Transaction,
        metadata: WriteRequestMetadata,
        reply: ReplyKind<AppendResult>,
    ) {
        match destination {
            WriteDestination::Local {
                partition_actor_ref,
                replica_partitions,
            } => {
                // Process locally as the leader
                let leader_request = LeaderWriteRequest {
                    transaction,
                    reply,
                    replica_partitions,
                    replication_factor: partition_manager.replication_factor,
                };

                tokio::spawn(async move {
                    let _ = partition_actor_ref.tell(leader_request).await;
                });
            }
            WriteDestination::Remote {
                primary_peer_id,
                partitions,
            } => {
                // We need to forward to the leader or an alternative peer
                self.forward_write_request(
                    req_resp,
                    primary_peer_id,
                    *partitions,
                    transaction,
                    metadata,
                    reply,
                );
            }
        }
    }

    /// Forwards a write request to another peer
    fn forward_write_request(
        &mut self,
        req_resp: &mut request_response::cbor::Behaviour<Req, Resp>,
        primary_peer_id: PeerId,
        partitions: ArrayVec<(PartitionId, PeerId), MAX_REPLICATION_FACTOR>,
        transaction: Transaction,
        metadata: WriteRequestMetadata,
        reply: ReplyKind<AppendResult>,
    ) {
        // Check if we've already tried this peer (to prevent loops)
        if metadata.tried_peers.contains(&primary_peer_id) {
            // Try the next best partition/peer if available
            for (_partition_id, peer_id) in partitions.iter().skip(1) {
                if !metadata.tried_peers.contains(peer_id) {
                    // Found an untried peer, forward to them
                    self.send_forward_request(*peer_id, req_resp, transaction, metadata, reply);
                    return;
                }
            }

            // No untried peers left
            self.handle_no_available_peers(reply);
            return;
        }

        // Forward to the leader peer
        self.send_forward_request(primary_peer_id, req_resp, transaction, metadata, reply);
    }

    /// Sends a forward request to a specific peer
    fn send_forward_request(
        &mut self,
        peer_id: PeerId,
        req_resp: &mut request_response::cbor::Behaviour<Req, Resp>,
        transaction: Transaction,
        mut metadata: WriteRequestMetadata,
        reply: ReplyKind<AppendResult>,
    ) {
        // Add this peer to the tried list
        metadata.tried_peers.insert(peer_id);

        match reply {
            ReplyKind::Local(tx) => {
                // Local request being forwarded to another node
                let req_id = req_resp.send_request(
                    &peer_id,
                    Req::AppendEvents {
                        transaction,
                        metadata,
                    },
                );

                // Store the sender to respond when we get a reply
                self.forwarded_appends.insert(req_id, tx);
            }
            ReplyKind::Remote(channel) => {
                // Remote request being forwarded again
                // For simplicity, respond that they should try again with the correct leader
                let _ = req_resp.send_response(
                    channel,
                    Resp::AppendEvents(Err(SwarmError::WrongLeaderNode {
                        correct_leader: peer_id,
                    })),
                );
            }
        }
    }

    /// Handles the case when no available peers are left to forward to
    fn handle_no_available_peers(&self, reply: ReplyKind<AppendResult>) {
        match reply {
            ReplyKind::Local(tx) => {
                let _ = tx.send(Err(SwarmError::NoAvailableLeaders));
            }
            ReplyKind::Remote(_) => {
                warn!("No peers left to forward to for network request");
                // Could add a more specific error response here
            }
        }
    }
}

/// Represents a destination for a write request
enum WriteDestination {
    /// Process locally as the leader
    Local {
        partition_actor_ref: ActorRef<PartitionActor>,
        replica_partitions: Vec<PartitionId>,
    },
    /// Forward to a remote leader
    Remote {
        primary_peer_id: PeerId,
        partitions: Box<ArrayVec<(PartitionId, PeerId), MAX_REPLICATION_FACTOR>>,
    },
}

#[derive(Debug, Error)]
pub enum ConfirmTransactionError {
    #[error("events length mismatch")]
    EventsLengthMismatch,
    #[error("event id mismatch")]
    EventIdMismatch,
    #[error("transaction not found")]
    TransactionNotFound,
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    Write(#[from] WriteError),
}

/// Confirms a transaction by updating confirmation counts
async fn confirm_transaction(
    database: &Database,
    partition_id: PartitionId,
    transaction_id: Uuid,
    event_ids: SmallVec<[Uuid; 4]>,
    confirmation_count: u8,
) -> Result<(), ConfirmTransactionError> {
    let Some(first_event_id) = event_ids.first() else {
        return Err(ConfirmTransactionError::EventsLengthMismatch);
    };

    let offsets = match database
        .read_transaction(partition_id, *first_event_id)
        .await?
    {
        Some(CommittedEvents::Single(event)) => {
            smallvec![event.offset]
        }
        Some(CommittedEvents::Transaction { events, commit }) => {
            if events.len() != event_ids.len() {
                return Err(ConfirmTransactionError::EventsLengthMismatch);
            }

            events
                .into_iter()
                .zip(event_ids.into_iter())
                .map(|(event, event_id)| {
                    if event.event_id != event_id {
                        return Err(ConfirmTransactionError::EventIdMismatch);
                    }

                    Ok(event.offset)
                })
                .chain(iter::once(Ok(commit.offset)))
                .collect::<Result<_, _>>()?
        }
        Some(CommittedEvents::None { .. }) | None => {
            return Err(ConfirmTransactionError::TransactionNotFound);
        }
    };

    database
        .set_confirmations(partition_id, offsets, transaction_id, confirmation_count)
        .await?;

    Ok(())
}
