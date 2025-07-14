use kameo::remote;
use libp2p::{mdns, swarm::NetworkBehaviour};

use crate::partition_consensus;

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    pub kameo: remote::Behaviour,
    pub partitions: partition_consensus::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
}
