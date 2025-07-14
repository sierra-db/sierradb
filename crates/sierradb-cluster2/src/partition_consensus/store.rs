use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

use bincode::{Decode, Encode, config};
use libp2p::PeerId;
use tracing::{error, info};

#[derive(Debug, Default, Clone, Encode, Decode)]
pub struct PartitionOwnershipStore {
    // Maps partition_id to owner
    partition_owners: HashMap<u16, Vec<u8>>, // PeerId stored as bytes
}

impl PartitionOwnershipStore {
    pub fn new() -> Self {
        Self {
            partition_owners: HashMap::new(),
        }
    }

    pub fn load<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                info!(
                    "no existing partition store found at {:?}, creating new",
                    path.as_ref()
                );
                return Ok(Self::new());
            }
            Err(err) => return Err(err),
        };

        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;

        match bincode::decode_from_slice::<Self, _>(&buffer, config::standard()) {
            Ok((store, _)) => {
                info!(
                    "loaded partition ownership store with {} entries",
                    store.partition_owners.len()
                );
                Ok(store)
            }
            Err(err) => {
                error!("failed to decode partition ownership store: {}", err);
                Ok(Self::new())
            }
        }
    }

    pub fn save<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let encoded = bincode::encode_to_vec(self, config::standard()).map_err(io::Error::other)?;

        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(&encoded)?;
        writer.flush()?;

        info!("saved partition ownership store to {:?}", path.as_ref());
        Ok(())
    }

    pub fn set_partition_owners(&mut self, owners: &HashMap<u16, PeerId>) {
        self.partition_owners.clear();

        for (&partition_id, owner) in owners {
            self.partition_owners.insert(partition_id, owner.to_bytes());
        }
    }

    pub fn get_partition_owners(&self) -> HashMap<u16, PeerId> {
        self.partition_owners
            .iter()
            .filter_map(
                |(partition_id, owner_bytes)| match PeerId::from_bytes(owner_bytes) {
                    Ok(peer_id) => Some((*partition_id, peer_id)),
                    Err(err) => {
                        error!("failed to parse PeerId for partition {partition_id}: {err}");
                        None
                    }
                },
            )
            .collect()
    }
}
