use arrayvec::ArrayVec;
use kameo::actor::ActorRef;
use libp2p::{PeerId, request_response};
use sierradb::{
    MAX_REPLICATION_FACTOR,
    bucket::{PartitionId, segment::EventRecord},
    database::Database,
};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::{
    behaviour::{ReadRequestMetadata, Req, Resp},
    error::SwarmError,
    partition_consensus::PartitionManager,
};

use super::actor::{ReplyKind, SendResponse, Swarm};

const MAX_FORWARDS: u8 = 3;

pub struct ReadManager {
    swarm_ref: ActorRef<Swarm>,
    database: Database,
    local_peer_id: PeerId,
    required_quorum: u8,

}

impl ReadManager {
    pub fn new(swarm_ref: ActorRef<Swarm>, database: Database, local_peer_id: PeerId, replication_factor: u8) -> Self {
        ReadManager {
            swarm_ref, database, local_peer_id, required_quorum:  (replication_factor  / 2) + 1
        }
    }

    /// Initiates a read operation locally, forwarding to the leader if
    /// necessary
    pub fn process_local_read(
        &mut self,
        partition_id: PartitionId,
        event_id: Uuid,
    ) -> oneshot::Receiver<Result<Option<EventRecord>, SwarmError>> {
        todo!()
    }

    fn resolve_read_destination(
        &self,
        partition_manager: &PartitionManager,
        metadata: &ReadRequestMetadata,
    ) -> Result<ReadDestination, SwarmError> {
        // Check for too many forwards
        if metadata.hop_count > MAX_FORWARDS {
            return Err(SwarmError::TooManyForwards);
        }

        // Get partition distribution
        let partitions =
            partition_manager.get_available_partitions_for_key(metadata.partition_hash);

        let Some((primary_partition_id, primary_peer_id)) = partitions.first().copied() else {
            return Err(SwarmError::NoAvailableLeaders);
        };

        // Check if we are the leader
        if partition_manager.has_partition(primary_partition_id)
            && primary_peer_id == self.local_peer_id
        {
            // Extract replica partitions from the partitions list
            // Skip the first partition (primary) and only include partitions we're not the
            // leader for
            let replica_partitions = partitions
                .iter()
                .skip(1) // Skip the primary partition (first in the list)
                .map(|(partition_id, _)| *partition_id)
                .collect::<Vec<_>>();

            Ok(ReadDestination::Local { replica_partitions })
        } else {
            Ok(ReadDestination::Remote {
                primary_peer_id,
                partitions: Box::new(partitions),
            })
        }
    }

    /// Routes a write request to the appropriate destination
    fn route_read_request(
        &mut self,
        partition_manager: &PartitionManager,
        req_resp: &mut request_response::cbor::Behaviour<Req, Resp>,
        destination: ReadDestination,
        partition_id: PartitionId,
        event_id: Uuid,
        metadata: ReadRequestMetadata,
        reply: ReplyKind<Option<EventRecord>>,
    ) {
        match destination {
            ReadDestination::Local { replica_partitions } => {
                // Process locally as the leader
                let swarm_ref = self.swarm_ref.clone();
                tokio::spawn(async move {
                    let res = self
                        .database
                        .read_event(partition_id, event_id)
                        .await
                        .map_err(|err| SwarmError::Read(err.to_string()));
                    let response = match res {
                        Ok(Some(event)) => {
                            event.confirmation_count
                        }
                        Ok(None) => Ok(None),
                        Err(err) => {
                            Err(err)
                        },
                    }
                    match reply {
                        ReplyKind::Local(tx) => {
                            let _ = tx.send(res);
                        }
                        ReplyKind::Remote(channel) => {
                            let _ = swarm_ref
                                .tell(SendResponse {
                                    channel,
                                    response: Resp::Read(res),
                                })
                                .await;
                        }
                    }
                });
            }
            ReadDestination::Remote {
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
}

/// Represents a destination for a write request
enum ReadDestination {
    /// Process locally as the leader
    Local {
        replica_partitions: Vec<PartitionId>,
    },
    /// Forward to a remote leader
    Remote {
        primary_peer_id: PeerId,
        partitions: Box<ArrayVec<(PartitionId, PeerId), MAX_REPLICATION_FACTOR>>,
    },
}
