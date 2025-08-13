use std::collections::{HashMap, HashSet};

use combine::{Parser, choice, many1, optional};
use kameo::error::Infallible;
use redis_protocol::resp3;
use redis_protocol::resp3::types::BytesFrame;
use sierradb::StreamId;
use sierradb::id::NAMESPACE_PARTITION_KEY;
use sierradb_cluster::subscription::{FromVersions, Subscribe, SubscriptionMatcher};
use tokio::io::{self, AsyncWriteExt};
use tokio::sync::{mpsc, watch};
use tracing::debug;
use uuid::Uuid;

use crate::error::AsRedisError;
use crate::parser::{
    FrameStream, keyword, number_u64, number_u64_min, partition_key, stream_id, stream_id_version,
};
use crate::request::{HandleRequest, number, simple_str};
use crate::server::Conn;

/// Subscribe to events from one or more streams.
///
/// # Syntax
/// ```text
/// # Single stream
/// ESUB <stream_id> [PARTITION_KEY <partition_key>] [FROM <version>] [WINDOW <size>]
///
/// # Multiple streams
/// ESUB <stream_id_1> [PARTITION_KEY <pk_1>] <stream_id_2> [PARTITION_KEY <pk_2>] ... [FROM LATEST | FROM <version> | FROM MAP <stream>=<ver>...] [WINDOW <size>]
/// ```
///
/// # Examples
/// ```text
/// ESUB user-123                                         # Single stream, latest, no window
/// ESUB user-123 WINDOW 100                              # Single stream, latest, window 100
/// ESUB user-123 FROM 50 WINDOW 100                      # Single stream, from version 50, window 100
/// ESUB user-123 PARTITION_KEY abc-def FROM 50           # Single stream with partition key
/// ESUB user-123 PARTITION_KEY abc-def FROM 50 WINDOW 100  # With partition key and window
///
/// # Multiple streams
/// ESUB user-1 user-2 user-3                             # Multiple streams, latest, no window
/// ESUB user-1 user-2 user-3 WINDOW 500                  # Multiple streams with window
/// ESUB user-1 user-2 user-3 FROM LATEST WINDOW 500      # Explicit latest with window
/// ESUB user-1 user-2 user-3 FROM 100 WINDOW 500         # All from version 100
///
/// # Multiple streams with partition keys
/// ESUB user-1 PARTITION_KEY abc user-2 PARTITION_KEY def user-3 PARTITION_KEY ghi FROM LATEST WINDOW 100
/// ESUB user-1 PARTITION_KEY abc user-2 user-3 PARTITION_KEY ghi FROM LATEST WINDOW 100  # Mixed
///
/// # Multiple streams with per-stream versions
/// ESUB user-1 user-2 user-3 FROM MAP user-1=10 user-2=20 user-3=30 WINDOW 50
/// ESUB stream1 PARTITION_KEY pk1 stream2 stream3 PARTITION_KEY pk3 FROM MAP stream1=10 stream2=20 stream3=30 WINDOW 50
/// ```
///
/// **Note:** Establishes a persistent connection to receive real-time stream
/// events.
pub struct ESub {
    pub matcher: SubscriptionMatcher,
    pub window_size: Option<u64>,
}

impl ESub {
    pub fn parser<'a>() -> impl Parser<FrameStream<'a>, Output = ESub> + 'a {
        (
            Selector::parser(),
            optional(from_versions()),
            optional(window()),
        )
            .map(|(selector, from_versions, window_size)| {
                let matcher = match selector {
                    Selector::StreamId {
                        stream_id,
                        partition_key,
                    } => {
                        let partition_key = partition_key.unwrap_or_else(|| {
                            Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id.as_bytes())
                        });
                        match from_versions {
                            Some(FromVersionsArg::Latest) | None => SubscriptionMatcher::Stream {
                                partition_key,
                                stream_id,
                                from_version: None,
                            },
                            Some(FromVersionsArg::Streams(from_versions)) => {
                                SubscriptionMatcher::Stream {
                                    partition_key,
                                    from_version: from_versions.get(&stream_id).copied(),
                                    stream_id,
                                }
                            }
                            Some(FromVersionsArg::AllStreams(from_version)) => {
                                SubscriptionMatcher::Stream {
                                    partition_key,
                                    stream_id,
                                    from_version: Some(from_version),
                                }
                            }
                        }
                    }
                    Selector::StreamIds(stream_ids) => {
                        let stream_ids: HashSet<_> = stream_ids
                            .into_iter()
                            .map(|(stream_id, partition_key)| {
                                let partition_key = partition_key.unwrap_or_else(|| {
                                    Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id.as_bytes())
                                });
                                (partition_key, stream_id)
                            })
                            .collect();
                        SubscriptionMatcher::Streams {
                            from_versions: match from_versions {
                                Some(FromVersionsArg::Latest) | None => FromVersions::Latest,
                                Some(FromVersionsArg::Streams(from_versions)) => {
                                    FromVersions::Streams(
                                        from_versions
                                            .into_iter()
                                            .filter_map(|(stream_id, version)| {
                                                let (partition_key, _) = stream_ids
                                                    .iter()
                                                    .find(|(_, sid)| sid == &stream_id)?;
                                                Some(((*partition_key, stream_id), version))
                                            })
                                            .collect(),
                                    )
                                }
                                Some(FromVersionsArg::AllStreams(from_version)) => {
                                    FromVersions::AllStreams(from_version)
                                }
                            },
                            stream_ids,
                        }
                    }
                };
                ESub {
                    matcher,
                    window_size,
                }
            })
    }
}

enum Selector {
    StreamId {
        stream_id: StreamId,
        partition_key: Option<Uuid>,
    },
    StreamIds(HashSet<(StreamId, Option<Uuid>)>),
}

impl Selector {
    // <stream_id_1> [PARTITION_KEY <pk_1>] <stream_id_2> [PARTITION_KEY <pk_2>]
    fn parser<'a>() -> impl Parser<FrameStream<'a>, Output = Self> + 'a {
        many1::<HashSet<_>, _, _>((
            stream_id(),
            optional(keyword("PARTITION_KEY").with(partition_key())),
        ))
        .map(|stream_ids| {
            if stream_ids.len() == 1 {
                // SAFETY: We just verified the set has exactly one element
                let (stream_id, partition_key) =
                    unsafe { stream_ids.into_iter().next().unwrap_unchecked() };
                return Selector::StreamId {
                    stream_id,
                    partition_key,
                };
            }

            Selector::StreamIds(stream_ids)
        })
    }
}

pub enum FromVersionsArg {
    Latest,
    Streams(HashMap<StreamId, u64>),
    AllStreams(u64),
}

// FROM LATEST | FROM <version> | FROM MAP <stream>=<ver>...
fn from_versions<'a>() -> impl Parser<FrameStream<'a>, Output = FromVersionsArg> + 'a {
    let latest = keyword("LATEST").map(|_| FromVersionsArg::Latest);
    let sequence = number_u64().map(FromVersionsArg::AllStreams);
    let map = (keyword("MAP").with(many1::<HashMap<_, _>, _, _>(stream_id_version())))
        .map(FromVersionsArg::Streams);

    keyword("FROM").with(choice((latest, sequence, map)))
}

fn window<'a>() -> impl Parser<FrameStream<'a>, Output = u64> + 'a {
    keyword("WINDOW").with(number_u64_min(1))
}

impl HandleRequest for ESub {
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
            .map_err(|err| match err {
                // SendError::HandlerError(replicas) => ErrorCode::Redirect.with_message(
                //     replicas
                //         .into_iter()
                //         .map(|(replica, _)| replica.id().peer_id().unwrap().to_base58())
                //         .collect::<Vec<_>>()
                //         .join(" "),
                // ),
                err => err
                    .map_err::<Infallible, _>(|_| unreachable!())
                    .as_redis_error(),
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
                    subscription_id,
                    number(conn.subscriptions.len() as i64),
                ],
                attributes: None,
            },
            false,
        )
        .map_err(io::Error::other)?;

        conn.socket.write_all(&conn.write).await?;
        conn.socket.flush().await?;
        conn.write.clear();

        Ok(None)
    }
}
