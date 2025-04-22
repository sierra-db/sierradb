use std::collections::HashMap;
use std::path::PathBuf;

use tokio::fs;
use tokio::io;

use super::*;

// Term Store actor for persisting leadership terms
#[derive(Actor)]
pub struct TermStoreActor {
    data_dir: PathBuf,
    terms: HashMap<u16, u64>,
}

// Message to store a term for a partition
pub struct StoreTerm {
    pub partition_id: u16,
    pub term: u64,
}

impl Message<StoreTerm> for TermStoreActor {
    type Reply = Result<(), io::Error>;

    async fn handle(
        &mut self,
        msg: StoreTerm,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Update in-memory cache
        self.terms.insert(msg.partition_id, msg.term);

        // Ensure directory exists
        let dir = self.data_dir.join("terms");
        fs::create_dir_all(&dir).await?;

        // Write to file
        let path = dir.join(format!("{}.term", msg.partition_id));
        let content = format!("{},{}", msg.partition_id, msg.term);
        fs::write(path, content).await
    }
}

// Message to get a term for a partition
pub struct GetTerm {
    pub partition_id: u16,
}

impl Message<GetTerm> for TermStoreActor {
    type Reply = Option<u64>;

    async fn handle(&mut self, msg: GetTerm, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.terms.get(&msg.partition_id).copied()
    }
}

// Message to load all terms from storage
pub struct LoadTerms;

impl Message<LoadTerms> for TermStoreActor {
    type Reply = Result<(), io::Error>;

    async fn handle(&mut self, _: LoadTerms, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let dir = self.data_dir.join("terms");

        // Create directory if it doesn't exist
        if !dir.exists() {
            fs::create_dir_all(&dir).await?;
            return Ok(());
        }

        // Read all term files
        let mut entries = fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            if path.extension().and_then(|ext| ext.to_str()) == Some("term") {
                let content = fs::read_to_string(&path).await?;
                let parts: Vec<&str> = content.split(',').collect();

                if parts.len() == 2 {
                    if let (Ok(partition_id), Ok(term)) =
                        (parts[0].parse::<u16>(), parts[1].parse::<u64>())
                    {
                        self.terms.insert(partition_id, term);
                    }
                }
            }
        }

        Ok(())
    }
}

impl TermStoreActor {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            terms: HashMap::new(),
        }
    }
}
