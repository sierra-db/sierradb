use std::collections::{HashMap, HashSet};

use combine::{Parser, choice, many1, optional};
use redis_protocol::resp3;
use redis_protocol::resp3::types::BytesFrame;
use sierradb::bucket::PartitionId;
use sierradb_cluster::subscription::{FromSequences, Subscribe, SubscriptionMatcher};
use tokio::io::{self, AsyncWriteExt};
use tokio::sync::{mpsc, watch};
use tracing::debug;
use uuid::Uuid;

use crate::error::AsRedisError;
use crate::parser::{
    FrameStream, all_selector, keyword, number_u64, number_u64_min, partition_id,
    partition_id_sequence, partition_ids,
};
use crate::request::{HandleRequest, number, simple_str};
use crate::server::Conn;

/// Subscribe to events from one or more partitions.
///
/// # Syntax
/// ```text
/// # All partitions
/// EPSUB * [FROM LATEST | FROM <sequence> | FROM MAP <p1>=<s1> <p2>=<s2>... [DEFAULT <seq>]] [WINDOW <size>]
///
/// # Single partition
/// EPSUB <partition_id> [FROM <sequence>] [WINDOW <size>]
///
/// # Multiple partitions
/// EPSUB <p1>,<p2>,<p3> [FROM LATEST | FROM <sequence> | FROM MAP <p1>=<s1> <p2>=<s2>... [DEFAULT <seq>]] [WINDOW <size>]
/// ```
///
/// # Examples
/// ```text
/// EPSUB *                                               # All partitions, latest, no window
/// EPSUB * WINDOW 100                                    # All partitions, latest, window 100
/// EPSUB * FROM 1000 WINDOW 100                          # All partitions, from seq 1000, window 100
/// EPSUB 5 FROM 100 WINDOW 50                            # Partition 5, from seq 100, window 50
/// EPSUB 1,2,3 FROM MAP 1=100 2=200 DEFAULT 0 WINDOW 500
/// ```
///
/// **Note:** Establishes a persistent connection to receive real-time events
/// from the specified partitions.
#[derive(Debug)]
pub struct EPSub {
    pub matcher: SubscriptionMatcher,
    pub window_size: Option<u64>,
}

impl EPSub {
    pub fn parser<'a>() -> impl Parser<FrameStream<'a>, Output = EPSub> + 'a {
        (
            Selector::parser(),
            optional(from_sequences()),
            optional(window()),
        )
            .map(|(selector, from_sequences, window_size)| {
                let matcher = match selector {
                    Selector::All => SubscriptionMatcher::AllPartitions {
                        from_sequences: from_sequences.unwrap_or(FromSequences::Latest),
                    },
                    Selector::Partition(partition_id) => match from_sequences {
                        Some(FromSequences::Latest) => SubscriptionMatcher::Partition {
                            partition_id,
                            from_sequence: None,
                        },
                        Some(FromSequences::Partitions {
                            from_sequences,
                            fallback,
                        }) => SubscriptionMatcher::Partition {
                            partition_id,
                            from_sequence: from_sequences.get(&partition_id).copied().or(fallback),
                        },
                        Some(FromSequences::AllPartitions(sequence)) => {
                            SubscriptionMatcher::Partition {
                                partition_id,
                                from_sequence: Some(sequence),
                            }
                        }
                        None => SubscriptionMatcher::Partition {
                            partition_id,
                            from_sequence: None,
                        },
                    },
                    Selector::Partitions(partition_ids) => SubscriptionMatcher::Partitions {
                        partition_ids,
                        from_sequences: from_sequences.unwrap_or(FromSequences::Latest),
                    },
                };
                EPSub {
                    matcher,
                    window_size,
                }
            })
    }
}

enum Selector {
    All,
    Partition(PartitionId),
    Partitions(HashSet<PartitionId>),
}

impl Selector {
    fn parser<'a>() -> impl Parser<FrameStream<'a>, Output = Self> + 'a {
        choice!(
            all_selector().map(|_| Selector::All),
            partition_id().map(Selector::Partition),
            partition_ids().map(Selector::Partitions)
        )
    }
}

// [FROM LATEST | FROM <sequence> | FROM MAP <p1>=<s1> <p2>=<s2>... [DEFAULT
// <seq>]]
fn from_sequences<'a>() -> impl Parser<FrameStream<'a>, Output = FromSequences> + 'a {
    let latest = keyword("LATEST").map(|_| FromSequences::Latest);
    let sequence = number_u64().map(FromSequences::AllPartitions);
    let map = (keyword("MAP").with((
        many1::<HashMap<_, _>, _, _>(partition_id_sequence()),
        optional(keyword("DEFAULT").with(number_u64())),
    )))
    .map(|(from_sequences, fallback)| FromSequences::Partitions {
        from_sequences,
        fallback,
    });

    keyword("FROM").with(choice((latest, sequence, map)))
}

fn window<'a>() -> impl Parser<FrameStream<'a>, Output = u64> + 'a {
    keyword("WINDOW").with(number_u64_min(1))
}

impl HandleRequest for EPSub {
    type Error = String;
    type Ok = BytesFrame;

    async fn handle_request(self, conn: &mut Conn) -> Result<Option<Self::Ok>, Self::Error> {
        let sender = match conn
            .subscription_channel
            .as_ref()
            .and_then(|(weak_sender, _)| weak_sender.upgrade())
        {
            Some(sender) => sender,
            None => {
                let (sender, receiver) = mpsc::unbounded_channel();
                conn.subscription_channel = Some((sender.downgrade(), receiver));
                sender
            }
        };

        let subscription_id = Uuid::new_v4();
        let (last_ack_tx, last_ack_rx) = watch::channel(None);
        conn.cluster_ref
            .ask(Subscribe {
                subscription_id,
                matcher: self.matcher,
                last_ack_rx,
                update_tx: sender,
                window_size: self.window_size.unwrap_or(1_000),
            })
            .await
            .map_err(|err| {
                err.map_err::<&'static str, _>(|_| unreachable!("infallible error"))
                    .as_redis_error()
            })?;

        conn.subscriptions.insert(subscription_id, last_ack_tx);

        debug!(
            subscription_id = %subscription_id,
            "created subscription"
        );

        Ok(Some(simple_str(subscription_id.to_string())))
    }

    async fn handle_request_failable(
        self,
        conn: &mut Conn,
    ) -> Result<Option<BytesFrame>, io::Error> {
        let subscription_id = match self.handle_request(conn).await {
            Ok(Some(subscription_id)) => subscription_id,
            Ok(None) => unreachable!("always returns some"),
            Err(err) => {
                return Ok(Some(BytesFrame::SimpleError {
                    data: err.into(),
                    attributes: None,
                }));
            }
        };

        resp3::encode::complete::extend_encode(&mut conn.write, &subscription_id, false)
            .map_err(io::Error::other)?;

        resp3::encode::complete::extend_encode(
            &mut conn.write,
            &BytesFrame::Push {
                data: vec![
                    simple_str("subscribe"),
                    subscription_id.clone(),
                    number(conn.subscriptions.len() as i64),
                ],
                attributes: None,
            },
            false,
        )
        .map_err(io::Error::other)?;

        conn.stream.write_all(&conn.write).await?;
        conn.stream.flush().await?;
        conn.write.clear();

        Ok(None)
    }
}
