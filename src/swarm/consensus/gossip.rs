use std::{collections::HashSet, time::Duration};

use kameo::{error::Infallible, prelude::*};
use libp2p::{
    PeerId,
    gossipsub::{self, IdentTopic, MessageId},
};
use tracing::{debug, error};

use super::{
    ConsensusError, ConsensusMessage,
    leadership::{LeadershipActor, ProcessClaim, ProcessConfirmation, ProcessHeartbeat},
};

pub struct ConsensusGossipActor {
    /// Topic for consensus messages
    topic: IdentTopic,
    /// Local peer ID
    local_peer_id: PeerId,
    /// Seen message IDs to prevent duplicate processing
    seen_message_ids: HashSet<MessageId>,
    /// Seen logical messages to prevent duplicate processing of logically equivalent messages
    seen_logical_messages: HashSet<String>,
    /// Reference to leadership actor
    leadership_ref: ActorRef<LeadershipActor>,
}

impl Actor for ConsensusGossipActor {
    type Mailbox = UnboundedMailbox<Self>;
    type Error = Infallible;

    async fn on_start(&mut self, actor_ref: ActorRef<Self>) -> Result<(), Self::Error> {
        let gossip_ref = actor_ref.downgrade();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                match gossip_ref.upgrade() {
                    Some(gossip_ref) => {
                        let _ = gossip_ref.tell(CleanupSeenMessages).await;
                    }
                    None => break,
                }
            }
        });

        Ok(())
    }
}

// impl Message<PublishConsensusMessage> for ConsensusGossipActor {
//     type Reply = Result<(), ConsensusError>;

//     async fn handle(
//         &mut self,
//         msg: PublishConsensusMessage,
//         _ctx: &mut Context<Self, Self::Reply>,
//     ) -> Self::Reply {
//         if let Some(gossipsub) = &self.gossipsub {
//             let encoded = bincode::encode_to_vec(&msg.message, bincode::config::standard())?;
//             let mut gossipsub_lock = gossipsub.lock().await;
//             gossipsub_lock.publish(self.topic.clone(), encoded)?;
//         }
//         Ok(())
//     }
// }

// Message to process an incoming gossip message
pub struct ProcessGossipMessage {
    pub message_id: MessageId,
    pub message: gossipsub::Message,
}

impl Message<ProcessGossipMessage> for ConsensusGossipActor {
    type Reply = Result<(), ConsensusError>;

    async fn handle(
        &mut self,
        msg: ProcessGossipMessage,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Skip messages we've already seen by ID
        if !self.seen_message_ids.insert(msg.message_id) {
            return Ok(());
        }

        // Try to deserialize the message
        match bincode::decode_from_slice::<ConsensusMessage, _>(
            &msg.message.data,
            bincode::config::standard(),
        ) {
            Ok((consensus_message, _)) => {
                // Create a logical fingerprint of the message to detect duplicates
                // even when message IDs differ
                match &consensus_message {
                    ConsensusMessage::Claim(claim) => {
                        let key = format!(
                            "claim-{}-{}-{}",
                            claim.partition_id, claim.term, claim.node_id
                        );

                        if self.seen_logical_messages.contains(&key) {
                            return Ok(());
                        }

                        self.seen_logical_messages.insert(key);
                        self.leadership_ref
                            .tell(ProcessClaim {
                                claim: claim.clone(),
                            })
                            .await?;
                    }
                    ConsensusMessage::Confirmation(confirm) => {
                        let key = format!(
                            "confirm-{}-{}-{}-{}",
                            confirm.partition_id,
                            confirm.term,
                            confirm.leader_id,
                            confirm.confirming_node
                        );

                        if self.seen_logical_messages.contains(&key) {
                            return Ok(());
                        }

                        self.seen_logical_messages.insert(key);
                        self.leadership_ref
                            .tell(ProcessConfirmation {
                                confirmation: confirm.clone(),
                            })
                            .await?;
                    }
                    ConsensusMessage::Heartbeat {
                        partition_id,
                        term,
                        leader_id,
                    } => {
                        let key = format!("heartbeat-{}-{}-{}", partition_id, term, leader_id);

                        if self.seen_logical_messages.contains(&key) {
                            return Ok(());
                        }

                        // Only keep heartbeat messages for a short time to allow new ones through
                        self.seen_logical_messages.insert(key);

                        self.leadership_ref
                            .tell(ProcessHeartbeat {
                                partition_id: *partition_id,
                                term: *term,
                                leader_id: *leader_id,
                            })
                            .await?;
                    }
                }
            }
            Err(err) => {
                error!("failed to decode consensus message: {err}");
            }
        }

        Ok(())
    }
}

pub struct CleanupSeenMessages;

impl Message<CleanupSeenMessages> for ConsensusGossipActor {
    type Reply = ();

    async fn handle(&mut self, _: CleanupSeenMessages, _ctx: &mut Context<Self, Self::Reply>) {
        // Keep the most recent 1000 message IDs
        if self.seen_message_ids.len() > 1000 {
            // This is a simple approach - ideally you'd have timestamps
            // and remove oldest messages first
            self.seen_message_ids.clear();
        }

        // Clear logical messages (especially heartbeats which are frequent)
        self.seen_logical_messages.clear();
    }
}

impl ConsensusGossipActor {
    pub fn new(
        topic_name: &str,
        local_peer_id: PeerId,
        leadership_ref: ActorRef<LeadershipActor>,
    ) -> Self {
        Self {
            topic: IdentTopic::new(topic_name),
            local_peer_id,
            seen_message_ids: HashSet::new(),
            seen_logical_messages: HashSet::new(),
            leadership_ref,
        }
    }

    pub fn topic(&self) -> &IdentTopic {
        &self.topic
    }
}
