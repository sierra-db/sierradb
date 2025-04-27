use std::collections::HashSet;

use libp2p::swarm::NetworkBehaviour;
use libp2p::{PeerId, mdns, request_response};
use serde::{Deserialize, Serialize};
use sierradb::bucket::PartitionId;
use sierradb::database::Transaction;
use sierradb::writer_thread_pool::AppendResult;
use smallvec::SmallVec;
use uuid::Uuid;

use crate::error::SwarmError;
use crate::partition_consensus;

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    pub mdns: mdns::tokio::Behaviour,
    pub req_resp: request_response::cbor::Behaviour<Req, Resp>,
    pub partition_ownership: partition_consensus::behaviour::Behaviour,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Req {
    AppendEvents {
        transaction: Transaction,
        metadata: WriteRequestMetadata,
    },
    ReplicateWrite {
        partition_id: PartitionId,
        append: Transaction,
        transaction_id: Uuid,
        origin_partition: PartitionId,
        origin_peer: PeerId,
    },
    ConfirmWrite {
        partition_id: PartitionId,
        transaction_id: Uuid,
        offsets: SmallVec<[u64; 4]>,
        confirmation_count: u8,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Resp {
    AppendEventsSuccess {
        result: AppendResult,
    },
    AppendEventsFailure {
        error: SwarmError,
    },
    ReplicateWriteSuccess {
        transaction_id: Uuid,
        partition_id: PartitionId,
    },
    ReplicateWriteFailure {
        transaction_id: Uuid,
        partition_id: PartitionId,
        error: String,
    },
    ConfirmWriteSuccess {
        transaction_id: Uuid,
        partition_id: PartitionId,
    },
    ConfirmWriteFailure {
        transaction_id: Uuid,
        partition_id: PartitionId,
        error: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteRequestMetadata {
    /// Number of hops this request has taken
    pub hop_count: u8,
    /// Nodes that have already tried to process this request
    pub tried_peers: HashSet<PeerId>,
    /// Original partition hash
    pub partition_hash: u16,
}
