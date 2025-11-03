use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use futures::{FutureExt, future::BoxFuture};
use kameo::prelude::*;
use rand::{SeedableRng, rngs::StdRng, seq::IteratorRandom};
use sierradb::{
    StreamId,
    bucket::{PartitionId, segment::EventRecord},
    database::Database,
    error::{PartitionIndexError, StreamIndexError},
    id::uuid_to_partition_hash,
};
use thiserror::Error;
use tokio::{
    sync::{
        broadcast,
        mpsc::{self, UnboundedSender},
        watch,
    },
    task::JoinHandle,
};
use uuid::Uuid;

use crate::{ClusterActor, confirmation::AtomicWatermark};

pub struct Subscribe {
    pub subscription_id: Uuid,
    pub matcher: SubscriptionMatcher,
    pub last_ack_rx: watch::Receiver<Option<u64>>,
    pub update_tx: UnboundedSender<SubscriptionEvent>,
    pub window_size: u64,
}

impl Message<Subscribe> for ClusterActor {
    type Reply = ();

    async fn handle(
        &mut self,
        Subscribe {
            subscription_id,
            matcher,
            last_ack_rx,
            update_tx,
            window_size,
        }: Subscribe,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.subscription_manager.subscribe(
            subscription_id,
            matcher,
            last_ack_rx,
            update_tx,
            window_size,
        );
    }
}

#[derive(Debug)]
pub enum SubscriptionEvent {
    Record {
        subscription_id: Uuid,
        cursor: u64,
        record: EventRecord,
    },
    Error {
        subscription_id: Uuid,
        error: SubscriptionError,
    },
    Closed {
        subscription_id: Uuid,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubscriptionMatcher {
    AllPartitions {
        from_sequences: FromSequences,
    },
    Partition {
        partition_id: PartitionId,
        from_sequence: Option<u64>,
    },
    Partitions {
        partition_ids: HashSet<PartitionId>,
        from_sequences: FromSequences,
    },
    Stream {
        partition_key: Uuid,
        stream_id: StreamId,
        from_version: Option<u64>,
    },
    Streams {
        stream_ids: HashSet<(Uuid, StreamId)>,
        from_versions: FromVersions,
    },
}

impl SubscriptionMatcher {
    fn has_seen(&self, record: &EventRecord) -> bool {
        match self {
            SubscriptionMatcher::AllPartitions { from_sequences } => match from_sequences {
                FromSequences::Latest => false,
                FromSequences::Partitions {
                    from_sequences,
                    fallback,
                } => {
                    let Some(from_sequence) = from_sequences
                        .get(&record.partition_id)
                        .or(fallback.as_ref())
                    else {
                        return false;
                    };
                    &record.partition_sequence < from_sequence
                }
                FromSequences::AllPartitions(from_sequence) => {
                    &record.partition_sequence < from_sequence
                }
            },
            SubscriptionMatcher::Partition {
                partition_id,
                from_sequence,
            } => {
                if &record.partition_id != partition_id {
                    return true; // Ignore this event, it doesn't match our criteria
                }

                match from_sequence {
                    Some(from_sequence) => &record.partition_sequence < from_sequence,
                    None => false,
                }
            }
            SubscriptionMatcher::Partitions {
                partition_ids,
                from_sequences,
            } => {
                if !partition_ids.contains(&record.partition_id) {
                    return true; // Ignore this event, it doesn't match our criteria
                }

                match from_sequences {
                    FromSequences::Latest => false,
                    FromSequences::Partitions {
                        from_sequences,
                        fallback,
                    } => {
                        let from_sequence = from_sequences
                            .get(&record.partition_id)
                            .or(fallback.as_ref());
                        match from_sequence {
                            Some(from_sequence) => &record.partition_sequence < from_sequence,
                            None => false,
                        }
                    }
                    FromSequences::AllPartitions(from_sequence) => {
                        &record.partition_sequence < from_sequence
                    }
                }
            }
            SubscriptionMatcher::Stream {
                partition_key,
                stream_id,
                from_version,
            } => {
                if &record.partition_key != partition_key || &record.stream_id != stream_id {
                    return true; // Ignore this event, it doesn't match our criteria
                }

                match from_version {
                    Some(from_version) => &record.stream_version < from_version,
                    None => false,
                }
            }
            SubscriptionMatcher::Streams {
                stream_ids,
                from_versions,
            } => {
                let key = (record.partition_key, record.stream_id.clone());
                if !stream_ids.contains(&key) {
                    return true; // Ignore this event, it doesn't match our criteria
                }

                match from_versions {
                    FromVersions::Latest => false,
                    FromVersions::Streams(streams) => {
                        let from_version = streams.get(&key);
                        match from_version {
                            Some(from_version) => &record.stream_version < from_version,
                            None => false,
                        }
                    }
                    FromVersions::AllStreams(from_version) => &record.stream_version < from_version,
                }
            }
        }
    }

    fn update_state(
        &mut self,
        partition_id: PartitionId,
        partition_sequence: u64,
        partition_key: Uuid,
        stream_id: StreamId,
        stream_version: u64,
    ) {
        match self {
            SubscriptionMatcher::AllPartitions { from_sequences } => {
                Self::update_from_sequences(from_sequences, partition_id, partition_sequence);
            }
            SubscriptionMatcher::Partition {
                partition_id: pid,
                from_sequence,
            } => {
                if pid == &partition_id {
                    *from_sequence = Some(partition_sequence + 1);
                }
            }
            SubscriptionMatcher::Partitions {
                partition_ids,
                from_sequences,
            } => {
                if partition_ids.contains(&partition_id) {
                    Self::update_from_sequences(from_sequences, partition_id, partition_sequence);
                }
            }
            SubscriptionMatcher::Stream {
                partition_key: pk,
                stream_id: sid,
                from_version,
            } => {
                if pk == &partition_key && sid == &stream_id {
                    *from_version = Some(stream_version + 1);
                }
            }
            SubscriptionMatcher::Streams {
                stream_ids,
                from_versions,
            } => {
                let key = (partition_key, stream_id);
                if stream_ids.contains(&key) {
                    match from_versions {
                        FromVersions::Latest => {
                            *from_versions = FromVersions::Streams(HashMap::from_iter([(
                                key,
                                stream_version + 1,
                            )]));
                        }
                        FromVersions::Streams(streams) => {
                            streams.insert(key, stream_version + 1);
                        }
                        FromVersions::AllStreams(_) => {
                            *from_versions = FromVersions::Streams(HashMap::from_iter([(
                                key,
                                stream_version + 1,
                            )]));
                        }
                    }
                }
            }
        }
    }

    fn update_from_sequences(
        from_sequences: &mut FromSequences,
        partition_id: PartitionId,
        partition_sequence: u64,
    ) {
        match from_sequences {
            FromSequences::Latest => {
                *from_sequences = FromSequences::Partitions {
                    from_sequences: HashMap::from_iter([(partition_id, partition_sequence + 1)]),
                    fallback: None,
                };
            }
            FromSequences::Partitions {
                from_sequences: sequences,
                fallback: _,
            } => {
                sequences.insert(partition_id, partition_sequence + 1);
            }
            FromSequences::AllPartitions(_) => {
                *from_sequences = FromSequences::Partitions {
                    from_sequences: HashMap::from_iter([(partition_id, partition_sequence + 1)]),
                    fallback: None,
                };
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FromSequences {
    Latest,
    Partitions {
        from_sequences: HashMap<PartitionId, u64>,
        fallback: Option<u64>,
    },
    AllPartitions(u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FromVersions {
    Latest,
    Streams(HashMap<(Uuid, StreamId), u64>),
    AllStreams(u64),
}

#[derive(Actor)]
pub struct SubscriptionManager {
    database: Database,
    owned_partitions: Arc<HashSet<PartitionId>>,
    watermarks: Arc<HashMap<PartitionId, Arc<AtomicWatermark>>>,
    num_partitions: u16,
    broadcast_tx: broadcast::Sender<EventRecord>,
}

impl SubscriptionManager {
    pub fn new(
        database: Database,
        owned_partitions: Arc<HashSet<PartitionId>>,
        watermarks: Arc<HashMap<PartitionId, Arc<AtomicWatermark>>>,
        num_partitions: u16,
        broadcast_tx: broadcast::Sender<EventRecord>,
    ) -> Self {
        SubscriptionManager {
            database,
            owned_partitions,
            watermarks,
            num_partitions,
            broadcast_tx,
        }
    }

    pub fn subscribe(
        &mut self,
        subscription_id: Uuid,
        matcher: SubscriptionMatcher,
        last_ack_rx: watch::Receiver<Option<u64>>,
        update_tx: UnboundedSender<SubscriptionEvent>,
        window_size: u64,
    ) {
        let subscription = Subscription {
            database: self.database.clone(),
            owned_partitions: self.owned_partitions.clone(),
            watermarks: self.watermarks.clone(),
            num_partitions: self.num_partitions,
            broadcast_rx: self.broadcast_tx.subscribe(),
            update_tx,
            last_ack_rx,
            window_size,
            subscription_id,
            cursor: 0,
        };
        subscription.spawn(matcher);
    }

    pub fn broadcast(&self, record: EventRecord) {
        let _ = self.broadcast_tx.send(record);
    }

    pub fn broadcaster(&self) -> broadcast::Sender<EventRecord> {
        self.broadcast_tx.clone()
    }
}

struct Subscription {
    database: Database,
    owned_partitions: Arc<HashSet<PartitionId>>,
    watermarks: Arc<HashMap<PartitionId, Arc<AtomicWatermark>>>,
    num_partitions: u16,
    broadcast_rx: broadcast::Receiver<EventRecord>,
    update_tx: UnboundedSender<SubscriptionEvent>,
    last_ack_rx: watch::Receiver<Option<u64>>,
    window_size: u64,
    subscription_id: Uuid,
    cursor: u64,
}

impl Subscription {
    fn spawn(mut self, matcher: SubscriptionMatcher) -> JoinHandle<()> {
        tokio::spawn(async move {
            match self.run(matcher).await {
                Ok(()) => {
                    let _ = self.update_tx.send(SubscriptionEvent::Closed {
                        subscription_id: self.subscription_id,
                    });
                }
                Err(SubscriptionError::ReceiverClosed) => {}
                Err(err) => {
                    let _ = self.update_tx.send(SubscriptionEvent::Error {
                        subscription_id: self.subscription_id,
                        error: err,
                    });
                }
            }
        })
    }

    async fn run(&mut self, mut matcher: SubscriptionMatcher) -> Result<(), SubscriptionError> {
        self.read_history(&mut matcher).await?;

        loop {
            match self.broadcast_rx.recv().await {
                Ok(record) => {
                    if matcher.has_seen(&record) {
                        continue;
                    }

                    let partition_id = record.partition_id;
                    let partition_sequence = record.partition_sequence;
                    let partition_key = record.partition_key;
                    let stream_id = record.stream_id.clone();
                    let stream_version = record.stream_version;

                    self.send_record(record).await?;

                    matcher.update_state(
                        partition_id,
                        partition_sequence,
                        partition_key,
                        stream_id,
                        stream_version,
                    );
                }
                Err(broadcast::error::RecvError::Closed) => break,
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    self.broadcast_rx = self.broadcast_rx.resubscribe();
                    self.read_history(&mut matcher).await?;
                }
            }
        }

        Ok(())
    }

    async fn send_record(&mut self, record: EventRecord) -> Result<(), SubscriptionError> {
        self.last_ack_rx
            .wait_for(|last_ack| {
                let gap = match last_ack {
                    Some(last_ack) => self.cursor.saturating_sub(*last_ack),
                    None => self.cursor + 1,
                };
                gap <= self.window_size
            })
            .await?;

        self.update_tx.send(SubscriptionEvent::Record {
            subscription_id: self.subscription_id,
            cursor: self.cursor,
            record,
        })?;

        self.cursor += 1;

        Ok(())
    }

    async fn read_history(
        &mut self,
        matcher: &mut SubscriptionMatcher,
    ) -> Result<(), SubscriptionError> {
        match matcher {
            SubscriptionMatcher::AllPartitions { from_sequences } => {
                self.read_all_partitions_history(from_sequences).await
            }
            SubscriptionMatcher::Partition {
                partition_id,
                from_sequence: Some(from_sequence),
            } => {
                self.read_partition_history(*partition_id, from_sequence)
                    .await
            }
            SubscriptionMatcher::Partition {
                from_sequence: None,
                ..
            } => Ok(()),
            SubscriptionMatcher::Partitions {
                partition_ids,
                from_sequences,
            } => {
                self.read_partitions_history(Cow::Borrowed(partition_ids), from_sequences)
                    .await
            }
            SubscriptionMatcher::Stream {
                partition_key,
                stream_id,
                from_version: Some(from_version),
            } => {
                self.read_stream_history(*partition_key, stream_id.clone(), from_version)
                    .await
            }
            SubscriptionMatcher::Stream {
                from_version: None, ..
            } => Ok(()),
            SubscriptionMatcher::Streams {
                stream_ids,
                from_versions,
            } => self.read_streams_history(stream_ids, from_versions).await,
        }
    }

    async fn read_partition_history(
        &mut self,
        partition_id: PartitionId,
        from_sequence: &mut u64,
    ) -> Result<(), SubscriptionError> {
        let watermark = self
            .watermarks
            .get(&partition_id)
            .ok_or(SubscriptionError::PartitionWatermarkNotFound { partition_id })?
            .clone();
        if !watermark.can_read(*from_sequence) {
            return Ok(());
        }

        let mut iter = self
            .database
            .read_partition(partition_id, *from_sequence)
            .await?;
        while let Some(commit) = iter.next().await? {
            let Some(first_partition_sequence) = commit.first_partition_sequence() else {
                continue;
            };

            if !watermark.can_read(first_partition_sequence) {
                break;
            }

            for event in commit {
                debug_assert!(watermark.can_read(event.partition_sequence));

                let sequence = event.partition_sequence;
                self.send_record(event).await?;

                *from_sequence = sequence + 1;
            }
        }

        Ok(())
    }

    fn read_partitions_history<'a>(
        &'a mut self,
        partition_ids: Cow<'a, HashSet<PartitionId>>,
        from_sequences: &'a mut FromSequences,
    ) -> BoxFuture<'a, Result<(), SubscriptionError>> {
        async move {
            match from_sequences {
                FromSequences::Latest => {}
                FromSequences::Partitions {
                    from_sequences: sequences,
                    fallback: Some(fallback),
                } => {
                    // There exists a fallback, lets hydrate the from sequences and call it again
                    *from_sequences = FromSequences::Partitions {
                        from_sequences: partition_ids
                            .iter()
                            .map(|partition_id| {
                                let from_sequence = sequences.get(partition_id).unwrap_or(fallback);
                                (*partition_id, *from_sequence)
                            })
                            .collect(),
                        fallback: None,
                    };
                    self.read_partitions_history(partition_ids, from_sequences)
                        .await?;
                }
                FromSequences::Partitions {
                    from_sequences,
                    fallback: None,
                } => {
                    let mut partition_iters = BTreeMap::new();
                    for (partition_id, from_sequence) in from_sequences
                        .iter_mut()
                        .filter(|(partition_id, _)| partition_ids.contains(partition_id))
                    {
                        let iter = self
                            .database
                            .read_partition(*partition_id, *from_sequence)
                            .await?;
                        partition_iters.insert(*partition_id, (iter, from_sequence));
                    }

                    let mut rng = StdRng::from_rng(&mut rand::rng());

                    while let Some((partition_id, (iter, from_sequence))) =
                        partition_iters.iter_mut().choose(&mut rng)
                    {
                        let Some(commit) = iter.next().await? else {
                            let partition_id = *partition_id;
                            partition_iters.remove(&partition_id);
                            continue;
                        };

                        let Some(first_partition_sequence) = commit.first_partition_sequence()
                        else {
                            continue;
                        };

                        let watermark = self
                            .watermarks
                            .get(partition_id)
                            .ok_or(SubscriptionError::PartitionWatermarkNotFound {
                                partition_id: *partition_id,
                            })?
                            .clone();
                        if !watermark.can_read(first_partition_sequence) {
                            let partition_id = *partition_id;
                            partition_iters.remove(&partition_id);
                            continue;
                        }

                        for event in commit {
                            debug_assert!(watermark.can_read(event.partition_sequence));

                            let sequence = event.partition_sequence;
                            self.send_record(event).await?;

                            **from_sequence = sequence + 1;
                        }
                    }
                }
                FromSequences::AllPartitions(from_sequence) => {
                    // Lets switch to partitions using all partitions and call it again
                    *from_sequences = FromSequences::Partitions {
                        from_sequences: partition_ids
                            .iter()
                            .map(|partition_id| (*partition_id, *from_sequence))
                            .collect(),
                        fallback: None,
                    };
                    self.read_partitions_history(partition_ids, from_sequences)
                        .await?;
                }
            }

            Ok(())
        }
        .boxed()
    }

    async fn read_all_partitions_history(
        &mut self,
        from_sequences: &mut FromSequences,
    ) -> Result<(), SubscriptionError> {
        self.read_partitions_history(
            Cow::Owned(self.owned_partitions.as_ref().clone()),
            from_sequences,
        )
        .await
    }

    async fn read_stream_history(
        &mut self,
        partition_key: Uuid,
        stream_id: StreamId,
        from_version: &mut u64,
    ) -> Result<(), SubscriptionError> {
        let partition_hash = uuid_to_partition_hash(partition_key);
        let partition_id = partition_hash % self.num_partitions;

        let watermark = self
            .watermarks
            .get(&partition_id)
            .ok_or(SubscriptionError::PartitionWatermarkNotFound { partition_id })?
            .clone();

        let mut iter = self
            .database
            .read_stream(partition_id, stream_id, *from_version, false)
            .await?;
        while let Some(commits) = iter.next_batch(50).await? {
            for commit in commits {
                let Some(first_partition_sequence) = commit.first_partition_sequence() else {
                    continue;
                };

                if !watermark.can_read(first_partition_sequence) {
                    break;
                }

                for event in commit {
                    debug_assert!(watermark.can_read(event.partition_sequence));

                    let version = event.stream_version;
                    self.send_record(event).await?;

                    *from_version = version + 1;
                }
            }
        }

        Ok(())
    }

    fn read_streams_history<'a>(
        &'a mut self,
        stream_ids: &'a HashSet<(Uuid, StreamId)>,
        from_versions: &'a mut FromVersions,
    ) -> BoxFuture<'a, Result<(), SubscriptionError>> {
        async move {
            match from_versions {
                FromVersions::Latest => {}
                FromVersions::Streams(from_versions) => {
                    for ((partition_key, stream_id), from_version) in from_versions {
                        self.read_stream_history(*partition_key, stream_id.clone(), from_version)
                            .await?;
                    }
                }
                FromVersions::AllStreams(from_version) => {
                    *from_versions = FromVersions::Streams(
                        stream_ids
                            .iter()
                            .map(|(partition_key, stream_id)| {
                                ((*partition_key, stream_id.clone()), *from_version)
                            })
                            .collect(),
                    );

                    self.read_streams_history(stream_ids, from_versions).await?;
                }
            }

            Ok(())
        }
        .boxed()
    }
}

#[derive(Debug, Error)]
pub enum SubscriptionError {
    #[error("receiver closed")]
    ReceiverClosed,
    #[error("partition watermark not found for partition {partition_id}")]
    PartitionWatermarkNotFound { partition_id: PartitionId },
    #[error(transparent)]
    PartitionIndex(#[from] PartitionIndexError),
    #[error(transparent)]
    StreamIndex(#[from] StreamIndexError),
}

impl From<mpsc::error::SendError<SubscriptionEvent>> for SubscriptionError {
    fn from(_err: mpsc::error::SendError<SubscriptionEvent>) -> Self {
        SubscriptionError::ReceiverClosed
    }
}

impl From<watch::error::RecvError> for SubscriptionError {
    fn from(_err: watch::error::RecvError) -> Self {
        SubscriptionError::ReceiverClosed
    }
}

#[cfg(test)]
mod tests {
    use sierradb::{
        database::{Database, DatabaseBuilder, ExpectedVersion, NewEvent, Transaction},
        id::{NAMESPACE_PARTITION_KEY, uuid_v7_with_partition_hash},
    };
    use smallvec::smallvec;
    use tempfile::{TempDir, tempdir};

    use super::*;

    async fn create_temp_db() -> (TempDir, Database) {
        let temp_dir = tempdir().expect("Failed to create temp directory");
        let db = DatabaseBuilder::new()
            .total_buckets(4)
            .bucket_ids_from_range(0..4)
            .open(temp_dir.path())
            .expect("Failed to open database");
        (temp_dir, db)
    }

    async fn append_event(
        db: &Database,
        partition_key: Uuid,
        num_partitions: u16,
        stream_id: StreamId,
    ) {
        let partition_hash = uuid_to_partition_hash(partition_key);
        db.append_events(
            Transaction::new(
                partition_key,
                partition_hash % num_partitions,
                smallvec![NewEvent {
                    event_id: uuid_v7_with_partition_hash(partition_hash),
                    stream_id,
                    stream_version: ExpectedVersion::Any,
                    event_name: "MyEvent".to_string(),
                    timestamp: 1000,
                    metadata: vec![],
                    payload: vec![],
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn subscription_matcher_hydration_stream() {
        let (_dir, db) = create_temp_db().await;
        let owned_partitions = Arc::new(HashSet::from_iter([0, 1, 2, 3]));
        let num_partitions = owned_partitions.len() as u16;
        let (_broadcast_tx, broadcast_rx) = broadcast::channel(10);
        let (update_tx, _update_rx) = mpsc::unbounded_channel();
        let (_last_ack_tx, last_ack_rx) = watch::channel(None);
        let window_size = 10;
        let subscription_id = Uuid::new_v4();

        // Create dummy watermarks for testing
        let watermarks = Arc::new(
            owned_partitions
                .iter()
                .map(|&id| (id, Arc::new(AtomicWatermark::new(0))))
                .collect::<HashMap<_, _>>(),
        );

        let mut subscription = Subscription {
            database: db.clone(),
            owned_partitions,
            watermarks,
            num_partitions,
            broadcast_rx,
            update_tx,
            last_ack_rx,
            window_size,
            subscription_id,
            cursor: 0,
        };

        let stream_id = StreamId::new("my-stream").unwrap();
        let partition_key = Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id.as_bytes());

        let mut matcher = SubscriptionMatcher::Stream {
            partition_key,
            stream_id: stream_id.clone(),
            from_version: None,
        };
        subscription.read_history(&mut matcher).await.unwrap();
        assert_eq!(
            &matcher,
            &SubscriptionMatcher::Stream {
                partition_key,
                stream_id: stream_id.clone(),
                from_version: None,
            }
        );

        append_event(&db, partition_key, num_partitions, stream_id.clone()).await;

        // Update watermark to allow reading the event we just wrote
        let partition_hash = uuid_to_partition_hash(partition_key);
        let partition_id = partition_hash % num_partitions;
        subscription
            .watermarks
            .get(&partition_id)
            .unwrap()
            .advance(1);

        let mut matcher = SubscriptionMatcher::Stream {
            partition_key,
            stream_id: stream_id.clone(),
            from_version: Some(0),
        };
        subscription.read_history(&mut matcher).await.unwrap();
        assert_eq!(
            &matcher,
            &SubscriptionMatcher::Stream {
                partition_key,
                stream_id: stream_id.clone(),
                from_version: Some(1),
            }
        );

        append_event(&db, partition_key, num_partitions, stream_id.clone()).await;

        // Update watermark to allow reading the second event
        let partition_hash = uuid_to_partition_hash(partition_key);
        let partition_id = partition_hash % num_partitions;
        subscription
            .watermarks
            .get(&partition_id)
            .unwrap()
            .advance(2);

        subscription.read_history(&mut matcher).await.unwrap();
        assert_eq!(
            &matcher,
            &SubscriptionMatcher::Stream {
                partition_key,
                stream_id: stream_id.clone(),
                from_version: Some(2),
            }
        );
    }

    #[tokio::test]
    async fn subscription_matcher_hydration_streams() {
        let (_dir, db) = create_temp_db().await;
        let owned_partitions = Arc::new(HashSet::from_iter([0, 1, 2, 3]));
        let num_partitions = owned_partitions.len() as u16;
        let (_broadcast_tx, broadcast_rx) = broadcast::channel(10);
        let (update_tx, _update_rx) = mpsc::unbounded_channel();
        let (_last_ack_tx, last_ack_rx) = watch::channel(None);
        let window_size = 10;
        let subscription_id = Uuid::new_v4();

        // Create dummy watermarks for testing
        let watermarks = Arc::new(
            owned_partitions
                .iter()
                .map(|&id| (id, Arc::new(AtomicWatermark::new(0))))
                .collect::<HashMap<_, _>>(),
        );

        let mut subscription = Subscription {
            database: db.clone(),
            owned_partitions,
            watermarks,
            num_partitions,
            broadcast_rx,
            update_tx,
            last_ack_rx,
            window_size,
            subscription_id,
            cursor: 0,
        };

        let stream_id_a = StreamId::new("my-stream-a").unwrap();
        let partition_key_a = Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id_a.as_bytes());
        let stream_id_b = StreamId::new("my-stream-b").unwrap();
        let partition_key_b = Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id_b.as_bytes());
        let stream_ids = HashSet::from_iter([
            (partition_key_a, stream_id_a.clone()),
            (partition_key_b, stream_id_b.clone()),
        ]);

        let mut matcher = SubscriptionMatcher::Streams {
            stream_ids: stream_ids.clone(),
            from_versions: FromVersions::Latest,
        };
        subscription.read_history(&mut matcher).await.unwrap();
        assert_eq!(
            &matcher,
            &SubscriptionMatcher::Streams {
                stream_ids: stream_ids.clone(),
                from_versions: FromVersions::Latest,
            }
        );

        append_event(&db, partition_key_a, num_partitions, stream_id_a.clone()).await;

        let mut matcher = SubscriptionMatcher::Streams {
            stream_ids: stream_ids.clone(),
            from_versions: FromVersions::Latest,
        };
        subscription.read_history(&mut matcher).await.unwrap();
        assert_eq!(
            &matcher,
            &SubscriptionMatcher::Streams {
                stream_ids: stream_ids.clone(),
                from_versions: FromVersions::Latest,
            }
        );

        // Update watermark to allow reading the event we just wrote
        let partition_hash_a = uuid_to_partition_hash(partition_key_a);
        let partition_id_a = partition_hash_a % num_partitions;
        subscription
            .watermarks
            .get(&partition_id_a)
            .unwrap()
            .advance(1);

        let mut matcher = SubscriptionMatcher::Streams {
            stream_ids: stream_ids.clone(),
            from_versions: FromVersions::Streams(HashMap::from_iter([(
                (partition_key_a, stream_id_a.clone()),
                0,
            )])),
        };
        subscription.read_history(&mut matcher).await.unwrap();
        assert_eq!(
            &matcher,
            &SubscriptionMatcher::Streams {
                stream_ids: stream_ids.clone(),
                from_versions: FromVersions::Streams(HashMap::from_iter([(
                    (partition_key_a, stream_id_a.clone()),
                    1,
                )])),
            }
        );

        append_event(&db, partition_key_a, num_partitions, stream_id_a.clone()).await;

        let mut matcher = SubscriptionMatcher::Streams {
            stream_ids: stream_ids.clone(),
            from_versions: FromVersions::Streams(HashMap::from_iter([(
                (partition_key_b, stream_id_b.clone()),
                0,
            )])),
        };
        subscription.read_history(&mut matcher).await.unwrap();
        assert_eq!(
            &matcher,
            &SubscriptionMatcher::Streams {
                stream_ids: stream_ids.clone(),
                from_versions: FromVersions::Streams(HashMap::from_iter([(
                    (partition_key_b, stream_id_b.clone()),
                    0,
                )])),
            }
        );

        // Update watermark for second event on stream A
        subscription
            .watermarks
            .get(&partition_id_a)
            .unwrap()
            .advance(2);

        let mut matcher = SubscriptionMatcher::Streams {
            stream_ids: stream_ids.clone(),
            from_versions: FromVersions::AllStreams(0),
        };
        subscription.read_history(&mut matcher).await.unwrap();
        assert_eq!(
            &matcher,
            &SubscriptionMatcher::Streams {
                stream_ids: stream_ids.clone(),
                from_versions: FromVersions::Streams(HashMap::from_iter([
                    ((partition_key_a, stream_id_a.clone()), 2),
                    ((partition_key_b, stream_id_b.clone()), 0),
                ])),
            }
        );

        append_event(&db, partition_key_b, num_partitions, stream_id_b.clone()).await;

        // Update watermark for event on stream B
        let partition_hash_b = uuid_to_partition_hash(partition_key_b);
        let partition_id_b = partition_hash_b % num_partitions;
        subscription
            .watermarks
            .get(&partition_id_b)
            .unwrap()
            .advance(1);

        let mut matcher = SubscriptionMatcher::Streams {
            stream_ids: stream_ids.clone(),
            from_versions: FromVersions::AllStreams(0),
        };
        subscription.read_history(&mut matcher).await.unwrap();
        assert_eq!(
            &matcher,
            &SubscriptionMatcher::Streams {
                stream_ids: stream_ids.clone(),
                from_versions: FromVersions::Streams(HashMap::from_iter([
                    ((partition_key_a, stream_id_a.clone()), 2),
                    ((partition_key_b, stream_id_b.clone()), 1),
                ])),
            }
        );
    }

    #[tokio::test]
    async fn subscription_matcher_hydration_partition() {
        let (_dir, db) = create_temp_db().await;
        let owned_partitions = Arc::new(HashSet::from_iter([0, 1, 2, 3]));
        let num_partitions = owned_partitions.len() as u16;
        let (_broadcast_tx, broadcast_rx) = broadcast::channel(10);
        let (update_tx, _update_rx) = mpsc::unbounded_channel();
        let (_last_ack_tx, last_ack_rx) = watch::channel(None);
        let window_size = 10;
        let subscription_id = Uuid::new_v4();

        // Create dummy watermarks for testing
        let watermarks = Arc::new(
            owned_partitions
                .iter()
                .map(|&id| (id, Arc::new(AtomicWatermark::new(0))))
                .collect::<HashMap<_, _>>(),
        );

        let mut subscription = Subscription {
            database: db.clone(),
            owned_partitions,
            watermarks,
            num_partitions,
            broadcast_rx,
            update_tx,
            last_ack_rx,
            window_size,
            subscription_id,
            cursor: 0,
        };

        let stream_id = StreamId::new("my-stream").unwrap();
        let partition_key = Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id.as_bytes());
        let partition_hash = uuid_to_partition_hash(partition_key);
        let partition_id = partition_hash % 4;

        let mut matcher = SubscriptionMatcher::Partition {
            partition_id,
            from_sequence: None,
        };
        subscription.read_history(&mut matcher).await.unwrap();
        assert_eq!(
            &matcher,
            &SubscriptionMatcher::Partition {
                partition_id,
                from_sequence: None,
            }
        );

        append_event(&db, partition_key, num_partitions, stream_id.clone()).await;

        // Update watermark to allow reading the event we just wrote
        subscription
            .watermarks
            .get(&partition_id)
            .unwrap()
            .advance(1);

        let mut matcher = SubscriptionMatcher::Partition {
            partition_id,
            from_sequence: Some(0),
        };
        subscription.read_history(&mut matcher).await.unwrap();
        assert_eq!(
            &matcher,
            &SubscriptionMatcher::Partition {
                partition_id,
                from_sequence: Some(1),
            }
        );

        append_event(&db, partition_key, num_partitions, stream_id.clone()).await;

        // Update watermark to allow reading the second event
        subscription
            .watermarks
            .get(&partition_id)
            .unwrap()
            .advance(2);

        subscription.read_history(&mut matcher).await.unwrap();
        assert_eq!(
            &matcher,
            &SubscriptionMatcher::Partition {
                partition_id,
                from_sequence: Some(2),
            }
        );
    }

    #[tokio::test]
    async fn subscription_matcher_hydration_partitions() {
        let (_dir, db) = create_temp_db().await;
        let owned_partitions = Arc::new(HashSet::from_iter([0, 1, 2, 3]));
        let num_partitions = owned_partitions.len() as u16;
        let (_broadcast_tx, broadcast_rx) = broadcast::channel(10);
        let (update_tx, _update_rx) = mpsc::unbounded_channel();
        let (_last_ack_tx, last_ack_rx) = watch::channel(None);
        let window_size = 10;
        let subscription_id = Uuid::new_v4();

        // Create dummy watermarks for testing
        let watermarks = Arc::new(
            owned_partitions
                .iter()
                .map(|&id| (id, Arc::new(AtomicWatermark::new(0))))
                .collect::<HashMap<_, _>>(),
        );

        let mut subscription = Subscription {
            database: db.clone(),
            owned_partitions,
            watermarks,
            num_partitions,
            broadcast_rx,
            update_tx,
            last_ack_rx,
            window_size,
            subscription_id,
            cursor: 0,
        };

        let stream_id_a = StreamId::new("my-stream-a").unwrap();
        let partition_key_a = Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id_a.as_bytes());
        let partition_hash_a = uuid_to_partition_hash(partition_key_a);
        let partition_id_a = partition_hash_a % 4;

        let stream_id_b = StreamId::new("my-stream-b").unwrap();
        let partition_key_b = Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id_b.as_bytes());
        let partition_hash_b = uuid_to_partition_hash(partition_key_b);
        let partition_id_b = partition_hash_b % 4;

        let partition_ids = HashSet::from_iter([partition_id_a, partition_id_b]);

        let mut matcher = SubscriptionMatcher::Partitions {
            partition_ids: partition_ids.clone(),
            from_sequences: FromSequences::Latest,
        };
        subscription.read_history(&mut matcher).await.unwrap();
        assert_eq!(
            &matcher,
            &SubscriptionMatcher::Partitions {
                partition_ids: partition_ids.clone(),
                from_sequences: FromSequences::Latest,
            }
        );

        append_event(&db, partition_key_a, num_partitions, stream_id_a.clone()).await;

        let mut matcher = SubscriptionMatcher::Partitions {
            partition_ids: partition_ids.clone(),
            from_sequences: FromSequences::Latest,
        };
        subscription.read_history(&mut matcher).await.unwrap();
        assert_eq!(
            &matcher,
            &SubscriptionMatcher::Partitions {
                partition_ids: partition_ids.clone(),
                from_sequences: FromSequences::Latest,
            }
        );

        // Update watermark to allow reading the event we just wrote
        subscription
            .watermarks
            .get(&partition_id_a)
            .unwrap()
            .advance(1);

        let mut matcher = SubscriptionMatcher::Partitions {
            partition_ids: partition_ids.clone(),
            from_sequences: FromSequences::Partitions {
                from_sequences: HashMap::from_iter([(partition_id_a, 0)]),
                fallback: None,
            },
        };
        subscription.read_history(&mut matcher).await.unwrap();
        assert_eq!(
            &matcher,
            &SubscriptionMatcher::Partitions {
                partition_ids: partition_ids.clone(),
                from_sequences: FromSequences::Partitions {
                    from_sequences: HashMap::from_iter([(partition_id_a, 1)]),
                    fallback: None,
                },
            }
        );

        append_event(&db, partition_key_a, num_partitions, stream_id_a.clone()).await;

        let mut matcher = SubscriptionMatcher::Partitions {
            partition_ids: partition_ids.clone(),
            from_sequences: FromSequences::Partitions {
                from_sequences: HashMap::from_iter([(partition_id_b, 0)]),
                fallback: None,
            },
        };
        subscription.read_history(&mut matcher).await.unwrap();
        assert_eq!(
            &matcher,
            &SubscriptionMatcher::Partitions {
                partition_ids: partition_ids.clone(),
                from_sequences: FromSequences::Partitions {
                    from_sequences: HashMap::from_iter([(partition_id_b, 0)]),
                    fallback: None,
                },
            }
        );

        // Update watermark for second event on partition A
        subscription
            .watermarks
            .get(&partition_id_a)
            .unwrap()
            .advance(2);

        let mut matcher = SubscriptionMatcher::Partitions {
            partition_ids: partition_ids.clone(),
            from_sequences: FromSequences::Partitions {
                from_sequences: HashMap::from_iter([(partition_id_a, 0)]),
                fallback: Some(0),
            },
        };
        subscription.read_history(&mut matcher).await.unwrap();
        assert_eq!(
            &matcher,
            &SubscriptionMatcher::Partitions {
                partition_ids: partition_ids.clone(),
                from_sequences: FromSequences::Partitions {
                    from_sequences: HashMap::from_iter([(partition_id_a, 2), (partition_id_b, 0)]),
                    fallback: None,
                },
            }
        );

        let mut matcher = SubscriptionMatcher::Partitions {
            partition_ids: partition_ids.clone(),
            from_sequences: FromSequences::AllPartitions(0),
        };
        subscription.read_history(&mut matcher).await.unwrap();
        assert_eq!(
            &matcher,
            &SubscriptionMatcher::Partitions {
                partition_ids: partition_ids.clone(),
                from_sequences: FromSequences::Partitions {
                    from_sequences: HashMap::from_iter([(partition_id_a, 2), (partition_id_b, 0)]),
                    fallback: None,
                },
            }
        );

        append_event(&db, partition_key_b, num_partitions, stream_id_b.clone()).await;

        // Update watermark for event on partition B
        subscription
            .watermarks
            .get(&partition_id_b)
            .unwrap()
            .advance(1);

        let mut matcher = SubscriptionMatcher::Partitions {
            partition_ids: partition_ids.clone(),
            from_sequences: FromSequences::AllPartitions(0),
        };
        subscription.read_history(&mut matcher).await.unwrap();
        assert_eq!(
            &matcher,
            &SubscriptionMatcher::Partitions {
                partition_ids: partition_ids.clone(),
                from_sequences: FromSequences::Partitions {
                    from_sequences: HashMap::from_iter([(partition_id_a, 2), (partition_id_b, 1)]),
                    fallback: None,
                },
            }
        );
    }
}
