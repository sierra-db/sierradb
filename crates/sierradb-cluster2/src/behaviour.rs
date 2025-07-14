use std::collections::HashSet;

use kameo::prelude::*;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{PeerId, mdns, request_response};
use serde::{Deserialize, Serialize};
use sierradb::bucket::segment::EventRecord;
use sierradb::bucket::{PartitionHash, PartitionId};
use sierradb::database::Transaction;
use sierradb::writer_thread_pool::AppendResult;
use smallvec::SmallVec;
use uuid::Uuid;

use crate::error::SwarmError;
use crate::partition_consensus;

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    // pub req_resp: request_response::cbor::Behaviour<Req, Resp>,
    pub kameo: remote::Behaviour,
    pub partition_ownership: partition_consensus::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
}

// #[derive(Debug, Serialize, Deserialize)]
// pub enum Req {
//     AppendEvents {
//         transaction: Transaction,
//         metadata: WriteRequestMetadata,
//     },
//     ReplicateWrite {
//         partition_id: PartitionId,
//         transaction: Transaction,
//         transaction_id: Uuid,
//         origin_partition: PartitionId,
//         origin_peer: PeerId,
//     },
//     // ConfirmWrite {
//     //     partition_id: PartitionId,
//     //     transaction_id: Uuid,
//     //     event_ids: SmallVec<[Uuid; 4]>,
//     //     confirmation_count: u8,
//     // },
// }

// #[derive(Debug, Serialize, Deserialize)]
// pub enum Resp {
//     AppendEvents(Result<AppendResult, SwarmError>),
//     ReplicateWrite {
//         partition_id: PartitionId,
//         transaction_id: Uuid,
//         result: Result<AppendResult, SwarmError>,
//     },
//     // ConfirmWrite {
//     //     partition_id: PartitionId,
//     //     transaction_id: Uuid,
//     //     result: Result<(), SwarmError>,
//     // },
//     // Read(Result<Option<EventRecord>, SwarmError>),
// }

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct ReadRequestMetadata {
//     /// Number of hops this request has taken
//     pub hop_count: u8,
//     /// Nodes that have already tried to process this request
//     pub tried_peers: HashSet<PeerId>,
//     /// Original partition hash
//     pub partition_hash: PartitionHash,
// }
