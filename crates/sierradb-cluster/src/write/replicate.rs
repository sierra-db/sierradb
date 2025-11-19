use std::time::Duration;

use failsafe::{
    StateMachine,
    backoff::{self, EqualJittered},
    failure_policy::{self, ConsecutiveFailures},
    futures::CircuitBreaker,
};
use futures::future::Either;
use kameo::{mailbox::Signal, prelude::*};
use serde::{Deserialize, Serialize};
use sierradb::{
    bucket::{PartitionId, segment::CommittedEvents},
    database::{Database, ExpectedVersion, NewEvent, PartitionLatestSequence, Transaction},
    writer_thread_pool::AppendResult,
};
use smallvec::{SmallVec, smallvec};
use tokio::{select, time::Instant};
use tracing::{debug, error, instrument, warn};

use crate::{
    ClusterActor, ClusterError, DEFAULT_BATCH_SIZE,
    confirmation::actor::{ConfirmationActor, UpdateConfirmationWithBroadcast},
    write::{
        ordered_queue::{self, OrderedValue},
        timeout_ordered_queue::{TimedOrderedValue, TimeoutOrderedQueue},
    },
};

use super::error::WriteError;

pub struct PartitionReplicatorActor {
    partition_id: PartitionId,
    database: Database,
    confirmation_ref: ActorRef<ConfirmationActor>,
    buffered_writes: TimeoutOrderedQueue<u64, BufferedWrite>,
    buffer_timeout: Duration,
    catching_up: bool,
    breaker: StateMachine<ConsecutiveFailures<EqualJittered>, ()>,
}

pub struct PartitionReplicatorActorArgs {
    pub partition_id: PartitionId,
    pub database: Database,
    pub confirmation_ref: ActorRef<ConfirmationActor>,
    /// Maximum number of out-of-order writes to buffer per partition
    pub buffer_size: usize,
    /// Maximum time to keep buffered writes before timing out
    pub buffer_timeout: Duration,
    /// Maximum time before requesting a catchup
    pub catchup_timeout: Duration,
}

impl Actor for PartitionReplicatorActor {
    type Args = PartitionReplicatorActorArgs;
    type Error = WriteError;

    async fn on_start(
        PartitionReplicatorActorArgs {
            partition_id,
            database,
            confirmation_ref,
            buffer_size,
            buffer_timeout,
            catchup_timeout,
        }: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        let next_expected_seq = match database
            .get_partition_sequence(partition_id)
            .await
            .map_err(|err| WriteError::DatabaseOperationFailed(err.to_string()))?
        {
            Some(PartitionLatestSequence::LatestSequence { sequence, .. }) => sequence + 1,
            Some(PartitionLatestSequence::ExternalBucket { .. }) => {
                todo!()
            }
            None => 0,
        };

        let breaker = failsafe::Config::new()
            .failure_policy(failure_policy::consecutive_failures(
                5,
                backoff::equal_jittered(Duration::from_secs(1), Duration::from_secs(30)),
            ))
            .build();

        Ok(PartitionReplicatorActor {
            partition_id,
            database,
            confirmation_ref,
            buffered_writes: TimeoutOrderedQueue::new(
                next_expected_seq,
                buffer_size,
                catchup_timeout,
            ),
            buffer_timeout,
            catching_up: false,
            breaker,
        })
    }

    async fn next(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        mailbox_rx: &mut MailboxReceiver<Self>,
    ) -> Option<Signal<Self>> {
        loop {
            let catchup_fut = if !self.catching_up {
                self.buffered_writes
                    .timeout_future()
                    .map(Either::Left)
                    .unwrap_or_else(|| Either::Right(futures::future::pending()))
            } else {
                Either::Right(futures::future::pending())
            };
            select! {
                msg = mailbox_rx.recv() => return msg,
                _ = catchup_fut => {
                    self.detect_and_handle_gaps(&actor_ref);
                }
            }
        }
    }
}

impl PartitionReplicatorActor {
    fn detect_and_handle_gaps(&mut self, partition_ref: &WeakActorRef<Self>) {
        // Delete records which have expired
        let mut records_deleted = false;
        while let Some(mut entry) = self.buffered_writes.queue.map.first_entry() {
            if entry.get_mut().garbage_collect(self.buffer_timeout) {
                break;
            } else {
                entry.remove();
                records_deleted = true;
            }
        }
        if records_deleted {
            self.buffered_writes.update_timeout();
        }

        if !self.breaker.is_call_permitted() {
            return;
        }

        // Look for the oldest buffered write to detect gaps
        if let Some((&oldest_buffered_seq, oldest_write)) =
            self.buffered_writes.queue.map.first_key_value()
        {
            let from_seq = *self.buffered_writes.next();
            let to_seq = oldest_buffered_seq - 1;
            let gap_size = oldest_buffered_seq - from_seq;
            let wait_time = oldest_write
                .reply_senders
                .first()
                .map(|reply| reply.received_at.elapsed());

            if gap_size > 0 && !self.catching_up {
                warn!(
                    partition_id = self.partition_id,
                    expected_seq = from_seq,
                    oldest_buffered_seq,
                    gap_size,
                    wait_time_ms = wait_time
                        .map(|wait_time| wait_time.as_millis())
                        .unwrap_or_default(),
                    "sequence gap detected, triggering catch-up"
                );

                self.trigger_catch_up(
                    partition_ref,
                    oldest_write.coordinator_ref.clone(),
                    from_seq,
                    to_seq,
                );
            }
        }
    }

    fn trigger_catch_up(
        &mut self,
        partition_ref: &WeakActorRef<PartitionReplicatorActor>,
        coordinator_ref: RemoteActorRef<ClusterActor>,
        from_seq: u64,
        to_seq: u64,
    ) {
        // Mark the range as being caught up
        self.catching_up = true;

        let partition_id = self.partition_id;
        debug!(
            partition_id,
            from_seq, to_seq, "starting catch-up for sequence range"
        );

        let breaker = self.breaker.clone();

        if let Some(partition_ref) = partition_ref.upgrade() {
            tokio::spawn(async move {
                let result = breaker
                    .call(
                        coordinator_ref
                            .ask(&PartitionSyncRequest {
                                partition_id,
                                from_seq,
                                to_seq,
                            })
                            .into_future(),
                    )
                    .await;

                let _ = partition_ref.tell(PartitionSyncResponse { result }).await;
            });
        }
    }

    /// Handles incoming write requests by either processing them immediately if
    /// they're the next expected sequence, or buffering them for future
    /// sequential processing, with duplicate detection and conflict resolution.
    fn buffer_write(
        &mut self,
        coordinator_ref: RemoteActorRef<ClusterActor>,
        tx: Transaction,
        reply_sender: Option<ReplySender<Result<AppendResult, WriteError>>>,
    ) -> Result<Option<BufferedWrite>, BufferWriteError> {
        let expected_next_sequence = tx
            .get_expected_partition_sequence()
            .into_next_version()
            .expect("partition sequence should not reach max");
        match self.buffered_writes.insert(
            expected_next_sequence,
            BufferedWrite::new(coordinator_ref, tx, reply_sender),
        ) {
            Ok(insert) => {
                if let Some((evicted_seq, evicted_write)) = insert.evicted {
                    debug!(
                        "evicting buffered write seq {evicted_seq} to make room for seq {expected_next_sequence}",
                    );
                    for reply in evicted_write
                        .garbage_collected(self.buffer_timeout)
                        .map(|buffered_write| buffered_write.reply_senders)
                        .unwrap_or_default()
                    {
                        reply.tx.send(Err(WriteError::BufferEvicted));
                    }
                }

                Ok(insert
                    .next
                    .and_then(|write| write.garbage_collected(self.buffer_timeout)))
            }
            Err(err) => match err {
                ordered_queue::Error::Conflict { value } => Err(BufferWriteError::new(
                    WriteError::SequenceConflict,
                    value.reply_senders.into_iter().next().map(|reply| reply.tx),
                )),
                ordered_queue::Error::Full { key: _, value } => Err(BufferWriteError::new(
                    WriteError::BufferFull,
                    value.reply_senders.into_iter().next().map(|reply| reply.tx),
                )),
                ordered_queue::Error::Stale { key: _, value } => Err(BufferWriteError::new(
                    WriteError::StaleWrite,
                    value.reply_senders.into_iter().next().map(|reply| reply.tx),
                )),
            },
        }
    }

    fn pop_next_buffered_write(&mut self) -> Option<BufferedWrite> {
        while let Some(mut write) = self.buffered_writes.pop() {
            if write.garbage_collect(self.buffer_timeout) {
                return Some(write);
            }
        }

        None
    }

    async fn write_buffered(&mut self, write: BufferedWrite) -> Result<AppendResult, WriteError> {
        let res = self.write_transaction(write.tx).await;

        for reply in write.reply_senders {
            reply.tx.send(res.clone());
        }

        res
    }

    async fn write_transaction(&mut self, tx: Transaction) -> Result<AppendResult, WriteError> {
        let confirmation_count = tx.confirmation_count();
        let res = self
            .database
            .append_events(tx)
            .await
            .map_err(|err| match err {
                sierradb::error::WriteError::WrongExpectedSequence {
                    partition_id,
                    current,
                    expected,
                } => WriteError::WrongExpectedSequence {
                    partition_id,
                    current,
                    expected,
                },
                err => WriteError::DatabaseOperationFailed(err.to_string()),
            });

        match &res {
            Ok(append) => {
                debug!(
                    "successfully wrote partition {} sequences {}-{}",
                    self.partition_id,
                    append.first_partition_sequence,
                    append.last_partition_sequence,
                );
                self.buffered_writes
                    .progress_to(append.last_partition_sequence + 1);

                // Buffer events for potential broadcast when confirmed
                let event_partition_sequences: SmallVec<[u64; 4]> =
                    (append.first_partition_sequence..=append.last_partition_sequence).collect();

                let _ = self
                    .confirmation_ref
                    .tell(UpdateConfirmationWithBroadcast {
                        partition_id: self.partition_id,
                        versions: event_partition_sequences,
                        confirmation_count,
                        partition_sequences: (
                            append.first_partition_sequence,
                            append.last_partition_sequence,
                        ),
                    })
                    .await;
            }
            Err(err) => {
                error!("failed to replicate write: {err}");
            }
        }

        res
    }
}

#[derive(Debug)]
struct BufferWriteError {
    error: WriteError,
    reply_sender: Option<ReplySender<Result<AppendResult, WriteError>>>,
}

impl BufferWriteError {
    pub fn new(
        error: WriteError,
        reply_sender: Option<ReplySender<Result<AppendResult, WriteError>>>,
    ) -> Self {
        Self {
            error,
            reply_sender,
        }
    }
}

#[derive(Debug)]
struct BufferedWrite {
    coordinator_ref: RemoteActorRef<ClusterActor>,
    tx: Transaction,
    reply_senders: SmallVec<[ReplySenderTimed; 4]>,
}

#[derive(Debug)]
struct ReplySenderTimed {
    tx: ReplySender<Result<AppendResult, WriteError>>,
    received_at: Instant,
}

impl ReplySenderTimed {
    pub fn new(tx: ReplySender<Result<AppendResult, WriteError>>) -> Self {
        ReplySenderTimed {
            tx,
            received_at: Instant::now(),
        }
    }
}

impl BufferedWrite {
    fn new(
        coordinator_ref: RemoteActorRef<ClusterActor>,
        tx: Transaction,
        reply_sender: Option<ReplySender<Result<AppendResult, WriteError>>>,
    ) -> Self {
        BufferedWrite {
            coordinator_ref,
            tx,
            reply_senders: reply_sender
                .map(|tx| smallvec![ReplySenderTimed::new(tx)])
                .unwrap_or_default(),
        }
    }

    fn garbage_collect(&mut self, buffer_timeout: Duration) -> bool {
        self.reply_senders
            .retain(|reply| reply.received_at.elapsed() <= buffer_timeout);
        !self.reply_senders.is_empty()
    }

    fn garbage_collected(mut self, buffer_timeout: Duration) -> Option<Self> {
        self.garbage_collect(buffer_timeout).then_some(self)
    }
}

impl OrderedValue for BufferedWrite {
    fn key_eq(&self, other: &Self) -> bool {
        self.tx.transaction_id() == other.tx.transaction_id()
    }

    fn merge(&mut self, new: Self) {
        self.reply_senders.extend(new.reply_senders);
    }
}

impl TimedOrderedValue for BufferedWrite {
    fn received_at(&self) -> Instant {
        self.reply_senders.first().unwrap().received_at
    }
}

/// Message to replicate a write to another partition
#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicateWrite {
    pub coordinator_ref: RemoteActorRef<ClusterActor>,
    pub coordinator_alive_since: u64,
    pub transaction: Transaction,
}

#[remote_message]
impl Message<ReplicateWrite> for ClusterActor {
    type Reply = ForwardedReply<ReplicateWrite, DelegatedReply<Result<AppendResult, WriteError>>>;

    #[instrument(skip_all, fields(coordinator_ref_id = %msg.coordinator_ref.id(), transaction_id = %msg.transaction.transaction_id(), events = msg.transaction.events().len()))]
    async fn handle(
        &mut self,
        msg: ReplicateWrite,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Ensure the transaction has the expected partition sequence set
        match msg.transaction.get_expected_partition_sequence() {
            ExpectedVersion::Any | ExpectedVersion::Exists => {
                return ForwardedReply::from_err(WriteError::MissingExpectedPartitionSequence);
            }
            ExpectedVersion::Empty | ExpectedVersion::Exact(_) => {}
        }

        // Ensure the coordinator is available from our perspective
        let Some((_, alive_since)) = self
            .topology_manager()
            .get_available_replicas(msg.transaction.partition_id())
            .into_iter()
            .find(|(cluster_ref, _)| cluster_ref == &msg.coordinator_ref)
        else {
            return ForwardedReply::from_err(WriteError::InvalidSender);
        };

        // Ensure the write is not stale
        if msg.coordinator_alive_since < alive_since {
            return ForwardedReply::from_err(WriteError::StaleWrite);
        }

        match self.replicator_refs.get(&msg.transaction.partition_id()) {
            Some(replicator_ref) => ctx.try_forward(replicator_ref, msg),
            None => ForwardedReply::from_err(WriteError::PartitionNotOwned {
                partition_id: msg.transaction.partition_id(),
            }),
        }
    }
}

impl Message<ReplicateWrite> for PartitionReplicatorActor {
    type Reply = DelegatedReply<Result<AppendResult, WriteError>>;

    #[instrument(skip_all, fields(coordinator_ref_id = %msg.coordinator_ref.id(), transaction_id = %msg.transaction.transaction_id(), events = msg.transaction.events().len()))]
    async fn handle(
        &mut self,
        msg: ReplicateWrite,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let (delegated_reply, reply_sender) = ctx.reply_sender();

        let mut next_write =
            match self.buffer_write(msg.coordinator_ref, msg.transaction, reply_sender) {
                Ok(Some(write)) => Some(write),
                Ok(None) => self.pop_next_buffered_write(),
                Err(BufferWriteError {
                    error,
                    reply_sender,
                }) => {
                    if let Some(tx) = reply_sender {
                        tx.send(Err(error));
                    } else {
                        error!("{error}");
                    }

                    return delegated_reply;
                }
            };

        while let Some(write) = next_write {
            match self.write_buffered(write).await {
                Ok(_) => {
                    next_write = self.pop_next_buffered_write();
                }
                Err(_) => break,
            }
        }

        delegated_reply
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct PartitionSyncRequest {
    partition_id: PartitionId,
    from_seq: u64,
    to_seq: u64,
}

#[remote_message]
impl Message<PartitionSyncRequest> for ClusterActor {
    type Reply = DelegatedReply<Result<Vec<CommittedEvents>, ClusterError>>;

    async fn handle(
        &mut self,
        msg: PartitionSyncRequest,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let db = self.database.clone();
        let Some(watermark) = self.watermarks.get(&msg.partition_id).map(|w| w.get()) else {
            return ctx.reply(Ok(vec![]));
        };

        ctx.spawn(async move {
            let mut iter = db
                .read_partition(msg.partition_id, msg.from_seq)
                .await
                .map_err(|err| ClusterError::Read(err.to_string()))?;
            let mut all_commits = Vec::new();
            'iter: while let Some(commits) = iter
                .next_batch(DEFAULT_BATCH_SIZE)
                .await
                .map_err(|err| ClusterError::Read(err.to_string()))?
            {
                for commit in commits {
                    let Some(first_seq) = commit.first_partition_sequence() else {
                        error!("found commit with zero events - this should not happen");
                        continue;
                    };
                    if first_seq >= watermark {
                        break 'iter;
                    }

                    debug_assert!(
                        commit.last_partition_sequence().unwrap() <= watermark,
                        "if the first events partition sequence is valid, then so should the last"
                    );

                    match commit {
                        CommittedEvents::Single(ref event) => {
                            if event.partition_sequence <= msg.to_seq {
                                all_commits.push(commit);
                            } else {
                                break 'iter;
                            }
                        }
                        CommittedEvents::Transaction { ref events, .. } => {
                            if let Some(first_event) = events.first() {
                                if first_event.partition_sequence <= msg.to_seq {
                                    all_commits.push(commit);
                                } else {
                                    break 'iter;
                                }
                            }
                        }
                    }
                }
            }

            Ok(all_commits)
        })
    }
}

struct PartitionSyncResponse {
    result: Result<Vec<CommittedEvents>, failsafe::Error<RemoteSendError<ClusterError>>>,
}

impl Message<PartitionSyncResponse> for PartitionReplicatorActor {
    type Reply = ();

    async fn handle(
        &mut self,
        PartitionSyncResponse { result }: PartitionSyncResponse,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.catching_up = false;

        match result {
            Ok(commits) => {
                for commit in commits {
                    let Some(first) = commit.first() else {
                        warn!("partition sync response returned commit with no events");
                        continue;
                    };

                    let tx_id = *commit.transaction_id();
                    let confirmation_count = commit.confirmation_count();
                    let tx = Transaction::new(
                        first.partition_key,
                        first.partition_id,
                        commit
                            .into_iter()
                            .map(|event| NewEvent {
                                event_id: event.event_id,
                                stream_id: event.stream_id,
                                stream_version: ExpectedVersion::from_next_version(
                                    event.stream_version,
                                ),
                                event_name: event.event_name,
                                timestamp: event.timestamp,
                                metadata: event.metadata,
                                payload: event.payload,
                            })
                            .collect(),
                    )
                    .unwrap()
                    .with_transaction_id(tx_id)
                    .with_confirmation_count(confirmation_count);
                    match self.write_transaction(tx).await {
                        Ok(append) => {
                            debug!(
                                "appended {} events for partition {} from sequence {} to {}",
                                append.offsets.len(),
                                self.partition_id,
                                append.first_partition_sequence,
                                append.last_partition_sequence
                            );
                        }
                        Err(err) => {
                            error!("partition sync append events failed: {err}");
                            self.buffered_writes.update_timeout();
                            return;
                        }
                    }
                }
            }
            Err(err) => {
                error!("partition sync failed: {err}");
                self.buffered_writes.update_timeout();
                return;
            }
        }

        while let Some(write) = self.pop_next_buffered_write() {
            match self.write_buffered(write).await {
                Ok(_) => {}
                Err(_) => break,
            }
        }

        self.buffered_writes.update_timeout();
    }
}

// #[cfg(test)]
// mod tests {
//     use std::{
//         collections::{BTreeMap, HashSet},
//         time::Duration,
//     };

//     use kameo::{
//         Actor,
//         actor::RemoteActorRef,
//         prelude::{Context, Message},
//         reply::{DelegatedReply, ReplySender},
//     };
//     use sierradb::{
//         StreamId,
//         bucket::PartitionId,
//         database::{
//             Database, DatabaseBuilder, ExpectedVersion, NewEvent,
// PartitionLatestSequence,             Transaction,
//         },
//         id::{uuid_to_partition_hash, uuid_v7_with_partition_hash},
//         writer_thread_pool::AppendResult,
//     };
//     use smallvec::smallvec;
//     use tempfile::tempdir;
//     use tokio::sync::oneshot;
//     use uuid::Uuid;

//     use crate::{
//         ClusterActor,
//         write::{
//             error::WriteError,
//             replicate::{BufferWriteError, PartitionReplicatorActor},
//         },
//     };

//     async fn create_temp_db() -> (tempfile::TempDir, Database) {
//         let temp_dir = tempdir().expect("failed to create temp directory");
//         let db = DatabaseBuilder::new()
//             .flush_interval_events(1)
//             .writer_threads(2)
//             .reader_threads(2)
//             .total_buckets(4)
//             .bucket_ids_from_range(0..4)
//             .open(temp_dir.path())
//             .expect("failed to open database");
//         (temp_dir, db)
//     }

//     fn create_test_event(
//         partition_key: Uuid,
//         stream_id_str: &str,
//         version: ExpectedVersion,
//     ) -> NewEvent {
//         NewEvent {
//             event_id:
// uuid_v7_with_partition_hash(uuid_to_partition_hash(partition_key)),
//             stream_id: StreamId::new(stream_id_str).expect("invalid stream
// ID"),             stream_version: version,
//             event_name: "test_event".to_string(),
//             timestamp: 12345678,     // Fixed timestamp for testing
//             metadata: vec![1, 2, 3], // Some test metadata
//             payload: b"test payload".to_vec(),
//         }
//     }

//     fn create_test_transaction(expected_sequence: ExpectedVersion) ->
// Transaction {         let partition_key = Uuid::new_v4();
//         Transaction::new(
//             partition_key,
//             1,
//             smallvec![create_test_event(
//                 partition_key,
//                 "hii",
//                 ExpectedVersion::Any
//             )],
//         )
//         .unwrap()
//         .expected_partition_sequence(expected_sequence)
//     }

//     async fn setup_partition_replicator(
//         partition_id: PartitionId,
//         database: Database,
//         next_expected_seq: Option<u64>,
//     ) -> PartitionReplicatorActor {
//         let next_expected_seq = match next_expected_seq {
//             Some(n) => n,
//             None => match
// database.get_partition_sequence(partition_id).await.unwrap() {
// Some(PartitionLatestSequence::LatestSequence { sequence, .. }) => sequence +
// 1,                 Some(PartitionLatestSequence::ExternalBucket { .. }) => {
//                     todo!()
//                 }
//                 None => 0,
//             },
//         };

//         PartitionReplicatorActor {
//             partition_id,
//             database,
//             next_expected_seq,
//             buffered_writes: BTreeMap::new(),
//             buffer_timeout: Duration::from_secs(10),
//             catchup_timeout: Duration::from_secs(10),
//             catchup_ranges: BTreeMap::new(),
//             next_catchup_seq: u64::MAX,
//             next_catchup_timeout:
// Box::pin(tokio::time::sleep(Duration::MAX)),         }
//     }

//     async fn setup_coordinator_ref() -> RemoteActorRef<ClusterActor> {
//         ClusterActor::prepare().actor_ref().into_remote_ref().await
//     }

//     async fn new_reply_sender() -> ReplySender<Result<AppendResult,
// WriteError>> {         #[derive(Actor)]
//         struct ReplySenderObtainer;

//         struct
// ObtainReplySender(oneshot::Sender<ReplySender<Result<AppendResult,
// WriteError>>>);

//         impl Message<ObtainReplySender> for ReplySenderObtainer {
//             type Reply = DelegatedReply<Result<AppendResult, WriteError>>;

//             async fn handle(
//                 &mut self,
//                 msg: ObtainReplySender,
//                 ctx: &mut Context<Self, Self::Reply>,
//             ) -> Self::Reply {
//                 let (delegated_reply, reply_sender) = ctx.reply_sender();
//                 let _ = msg.0.send(reply_sender.unwrap());
//                 delegated_reply
//             }
//         }

//         let obtainer_ref = ReplySenderObtainer::spawn(ReplySenderObtainer);
//         let (reply_tx, reply_rx) = oneshot::channel();
//         #[allow(clippy::let_underscore_future)]
//         let _ = obtainer_ref
//             .ask(ObtainReplySender(reply_tx))
//             .enqueue()
//             .await
//             .unwrap();
//         reply_rx.await.unwrap()
//     }

//     #[tokio::test]
//     async fn test_sequential_write_immediate_processing() {
//         let (_temp_dir, db) = create_temp_db().await;
//         let mut replicator = setup_partition_replicator(1, db, None).await;
//         let coordinator_ref = setup_coordinator_ref().await;

//         let reply_sender = new_reply_sender().await;
//         let tx = create_test_transaction(ExpectedVersion::Empty);
//         let result = replicator.buffer_write(coordinator_ref, tx,
// Some(reply_sender));

//         assert!(matches!(result, Ok(Some(_))));
//         assert_eq!(replicator.buffered_writes.len(), 0);
//     }

//     #[tokio::test]
//     async fn test_out_of_order_write_buffering() {
//         let (_temp_dir, db) = create_temp_db().await;
//         let mut replicator = setup_partition_replicator(1, db,
// Some(5)).await;         let coordinator_ref = setup_coordinator_ref().await;

//         let reply_sender = new_reply_sender().await;
//         let tx = create_test_transaction(ExpectedVersion::Exact(7));
//         let result = replicator.buffer_write(coordinator_ref, tx,
// Some(reply_sender));

//         assert!(matches!(result, Ok(None)));
//         assert_eq!(replicator.buffered_writes.len(), 1);
//         assert!(replicator.buffered_writes.contains_key(&8));
//     }

//     #[tokio::test]
//     async fn test_stale_write_rejection() {
//         let (_temp_dir, db) = create_temp_db().await;
//         let mut replicator = setup_partition_replicator(1, db,
// Some(10)).await;         let coordinator_ref = setup_coordinator_ref().await;

//         let reply_sender = new_reply_sender().await;
//         let tx = create_test_transaction(ExpectedVersion::Exact(5));
//         let result = replicator.buffer_write(coordinator_ref, tx,
// Some(reply_sender));

//         assert!(matches!(
//             result,
//             Err(BufferWriteError {
//                 error: WriteError::StaleWrite,
//                 reply_sender: Some(_)
//             })
//         ));
//         assert_eq!(replicator.buffered_writes.len(), 0);
//     }

//     #[tokio::test]
//     async fn test_duplicate_write_handling() {
//         let (_temp_dir, db) = create_temp_db().await;
//         let mut replicator = setup_partition_replicator(1, db,
// Some(5)).await;         let coordinator_ref = setup_coordinator_ref().await;

//         let tx = create_test_transaction(ExpectedVersion::Exact(7));

//         // First write
//         let reply_sender = new_reply_sender().await;
//         let result1 =
//             replicator.buffer_write(coordinator_ref.clone(), tx.clone(),
// Some(reply_sender));         assert!(matches!(result1, Ok(None)));

//         // Duplicate write
//         let reply_sender = new_reply_sender().await;
//         let result2 = replicator.buffer_write(coordinator_ref, tx,
// Some(reply_sender));         assert!(matches!(result2, Ok(None)));

//         assert_eq!(replicator.buffered_writes.len(), 1);
//         assert_eq!(
//             replicator
//                 .buffered_writes
//                 .get(&8)
//                 .unwrap()
//                 .reply_senders
//                 .len(),
//             2
//         );
//     }

//     #[tokio::test]
//     async fn test_sequence_conflict() {
//         let (_temp_dir, db) = create_temp_db().await;
//         let mut replicator = setup_partition_replicator(1, db,
// Some(5)).await;         let coordinator_ref = setup_coordinator_ref().await;

//         let tx1 = create_test_transaction(ExpectedVersion::Exact(7));
//         let tx2 = create_test_transaction(ExpectedVersion::Exact(7));

//         // First write
//         let reply_sender = new_reply_sender().await;
//         let result1 = replicator.buffer_write(coordinator_ref.clone(), tx1,
// Some(reply_sender));         assert!(matches!(result1, Ok(None)));

//         // Conflicting write
//         let reply_sender = new_reply_sender().await;
//         let result2 = replicator.buffer_write(coordinator_ref, tx2,
// Some(reply_sender));         assert!(matches!(
//             result2,
//             Err(BufferWriteError {
//                 error: WriteError::SequenceConflict,
//                 reply_sender: Some(_)
//             })
//         ));
//     }

//     #[tokio::test]
//     async fn test_buffer_size_limit_with_eviction() {
//         let (_temp_dir, db) = create_temp_db().await;
//         let mut replicator = setup_partition_replicator(1, db,
// Some(5)).await;         replicator.buffer_size = 2;
//         let coordinator_ref = setup_coordinator_ref().await;

//         // Fill buffer to capacity
//         let reply_sender = new_reply_sender().await;
//         let tx = create_test_transaction(ExpectedVersion::Exact(7));
//         replicator
//             .buffer_write(coordinator_ref.clone(), tx, Some(reply_sender))
//             .unwrap();

//         let reply_sender = new_reply_sender().await;
//         let tx = create_test_transaction(ExpectedVersion::Exact(8));
//         replicator
//             .buffer_write(coordinator_ref.clone(), tx, Some(reply_sender))
//             .unwrap();

//         assert_eq!(replicator.buffered_writes.len(), 2);

//         // Add lower sequence - should evict highest
//         let reply_sender = new_reply_sender().await;
//         let tx = create_test_transaction(ExpectedVersion::Exact(6));
//         let result = replicator.buffer_write(coordinator_ref, tx,
// Some(reply_sender));         assert!(matches!(result, Ok(None)));

//         // Should have evicted sequence 8
//         assert_eq!(replicator.buffered_writes.len(), 2);
//         assert!(replicator.buffered_writes.contains_key(&7));
//         assert!(replicator.buffered_writes.contains_key(&8));
//         assert!(!replicator.buffered_writes.contains_key(&9));
//     }

//     #[tokio::test]
//     async fn test_buffer_size_limit_rejection() {
//         let (_temp_dir, db) = create_temp_db().await;
//         let mut replicator = setup_partition_replicator(1, db,
// Some(5)).await;         replicator.buffer_size = 2;
//         let coordinator_ref = setup_coordinator_ref().await;

//         // Fill buffer with lower sequences
//         let reply_sender = new_reply_sender().await;
//         let tx = create_test_transaction(ExpectedVersion::Exact(7));
//         replicator
//             .buffer_write(coordinator_ref.clone(), tx, Some(reply_sender))
//             .unwrap();

//         let reply_sender = new_reply_sender().await;
//         let tx = create_test_transaction(ExpectedVersion::Exact(8));
//         replicator
//             .buffer_write(coordinator_ref.clone(), tx, Some(reply_sender))
//             .unwrap();

//         // Try to add higher sequence - should be rejected
//         let reply_sender = new_reply_sender().await;
//         let tx = create_test_transaction(ExpectedVersion::Exact(10));
//         let result = replicator.buffer_write(coordinator_ref, tx,
// Some(reply_sender));

//         assert!(matches!(
//             result,
//             Err(BufferWriteError {
//                 error: WriteError::BufferFull,
//                 reply_sender: Some(_)
//             })
//         ));
//     }

//     #[tokio::test]
//     async fn test_pop_next_buffered_write() {
//         let (_temp_dir, db) = create_temp_db().await;
//         let mut replicator = setup_partition_replicator(1, db,
// Some(5)).await;         let coordinator_ref = setup_coordinator_ref().await;

//         // Buffer some writes
//         let reply_sender = new_reply_sender().await;
//         let tx = create_test_transaction(ExpectedVersion::Exact(6));
//         replicator
//             .buffer_write(coordinator_ref.clone(), tx, Some(reply_sender))
//             .unwrap();

//         let reply_sender = new_reply_sender().await;
//         let tx = create_test_transaction(ExpectedVersion::Exact(5));
//         replicator
//             .buffer_write(coordinator_ref.clone(), tx, Some(reply_sender))
//             .unwrap();

//         let reply_sender = new_reply_sender().await;
//         let tx = create_test_transaction(ExpectedVersion::Exact(4));
//         replicator
//             .buffer_write(coordinator_ref, tx, Some(reply_sender))
//             .unwrap();

//         // Advance to 6
//         replicator.next_expected_seq += 1;
//         let popped = replicator.pop_next_buffered_write();
//         assert!(popped.is_some());
//         assert_eq!(
//             popped.unwrap().tx.get_expected_partition_sequence(),
//             ExpectedVersion::Exact(5)
//         );

//         // Advance to 7
//         replicator.next_expected_seq += 1;
//         let popped = replicator.pop_next_buffered_write();
//         assert!(popped.is_some());
//         assert_eq!(
//             popped.unwrap().tx.get_expected_partition_sequence(),
//             ExpectedVersion::Exact(6)
//         );

//         // No more writes
//         replicator.next_expected_seq += 1;
//         let popped = replicator.pop_next_buffered_write();
//         assert!(popped.is_none());
//     }

//     #[tokio::test]
//     async fn test_immediate_processing_with_duplicate() {
//         let (_temp_dir, db) = create_temp_db().await;
//         let mut replicator = setup_partition_replicator(1, db,
// Some(5)).await;         let coordinator_ref = setup_coordinator_ref().await;

//         let tx = create_test_transaction(ExpectedVersion::Exact(5));

//         // First write at next expected sequence
//         let reply_sender = new_reply_sender().await;
//         let result1 =
//             replicator.buffer_write(coordinator_ref.clone(), tx.clone(),
// Some(reply_sender));

//         // Should be buffered
//         assert!(matches!(result1, Ok(None)));

//         // Duplicate of same transaction
//         replicator.next_expected_seq += 1;
//         let reply_sender = new_reply_sender().await;
//         let result2 = replicator.buffer_write(coordinator_ref, tx,
// Some(reply_sender));

//         // Should merge reply senders and return merged write
//         assert!(matches!(result2, Ok(Some(_))));
//         if let Ok(Some(merged_write)) = result2 {
//             assert_eq!(merged_write.reply_senders.len(), 2);
//         }
//     }

//     #[tokio::test]
//     async fn test_complex_buffering_scenario() {
//         let (_temp_dir, db) = create_temp_db().await;
//         let mut replicator = setup_partition_replicator(1, db,
// Some(11)).await;         let coordinator_ref = setup_coordinator_ref().await;

//         // Add writes: 12, 15, 11, 13, 10 (10 should be immediate)
//         let sequences = vec![12, 15, 11, 13, 10];
//         let mut immediate_count = 0;

//         for seq in sequences {
//             let tx = create_test_transaction(ExpectedVersion::Exact(seq));
//             let reply_sender = new_reply_sender().await;
//             match replicator.buffer_write(coordinator_ref.clone(), tx,
// Some(reply_sender)) {                 Ok(Some(_)) => immediate_count += 1,
//                 Ok(None) => {}
//                 Err(err) => panic!("unexpected error: {err:?}"),
//             }
//         }

//         assert_eq!(immediate_count, 1); // Only sequence 10
//         assert_eq!(replicator.buffered_writes.len(), 4); // 11, 12, 13, 15

//         // Process in order
//         replicator.next_expected_seq += 1; // Now expecting 11
//         let next = replicator.pop_next_buffered_write();
//         assert!(next.is_some());
//         assert_eq!(
//             next.unwrap().tx.get_expected_partition_sequence(),
//             ExpectedVersion::Exact(11)
//         );

//         replicator.next_expected_seq += 1; // Now expecting 12
//         let next = replicator.pop_next_buffered_write();
//         assert_eq!(
//             next.unwrap().tx.get_expected_partition_sequence(),
//             ExpectedVersion::Exact(12)
//         );

//         replicator.next_expected_seq += 1; // Now expecting 13
//         let next = replicator.pop_next_buffered_write();
//         assert_eq!(
//             next.unwrap().tx.get_expected_partition_sequence(),
//             ExpectedVersion::Exact(13)
//         );

//         replicator.next_expected_seq += 1; // Now expecting 14
//         let next = replicator.pop_next_buffered_write();
//         assert!(next.is_none()); // Gap at 14

//         // Add 14
//         let tx = create_test_transaction(ExpectedVersion::Exact(14));
//         let reply_sender = new_reply_sender().await;
//         replicator
//             .buffer_write(coordinator_ref, tx, Some(reply_sender))
//             .unwrap();

//         replicator.next_expected_seq += 1; // Now expecting 15
//         let next = replicator.pop_next_buffered_write();
//         assert!(next.is_some());

//         assert_eq!(replicator.buffered_writes.len(), 0);
//     }

//     #[tokio::test]
//     async fn test_reply_sender_merging() {
//         let (_temp_dir, db) = create_temp_db().await;
//         let mut replicator = setup_partition_replicator(1, db,
// Some(5)).await;         let coordinator_ref = setup_coordinator_ref().await;

//         let tx = create_test_transaction(ExpectedVersion::Exact(6));

//         // First write
//         let reply_sender = new_reply_sender().await;
//         replicator
//             .buffer_write(coordinator_ref.clone(), tx.clone(),
// Some(reply_sender))             .unwrap();

//         // Duplicate with different reply sender
//         let reply_sender = new_reply_sender().await;
//         replicator
//             .buffer_write(coordinator_ref, tx, Some(reply_sender))
//             .unwrap();

//         // Check that reply senders were merged
//         let buffered = replicator.buffered_writes.get(&7).unwrap();
//         dbg!(&buffered.reply_senders);
//         assert_eq!(buffered.reply_senders.len(), 2);
//     }

//     // // ==========================================
//     // // Property-Based Tests
//     // // ==========================================

//     #[tokio::test]
//     async fn test_buffer_invariants() {
//         let (_temp_dir, db) = create_temp_db().await;
//         let mut replicator = setup_partition_replicator(1, db,
// Some(0)).await;         replicator.buffer_size = 100;
//         let coordinator_ref = setup_coordinator_ref().await;

//         // Add 1000 random writes
//         for i in 0..1000 {
//             let seq = (i * 17 + 7) % 200; // Generate pseudo-random but
// deterministic sequences             let tx =
// create_test_transaction(ExpectedVersion::Exact(seq));             let
// reply_sender = new_reply_sender().await;             let _ =
//                 replicator.buffer_write(coordinator_ref.clone(), tx.clone(),
// Some(reply_sender));         }

//         // Verify buffer size constraint
//         assert!(replicator.buffered_writes.len() <= 100);

//         // Verify all buffered sequences are >= next_expected_seq
//         for &seq in replicator.buffered_writes.keys() {
//             assert!(seq >= replicator.next_expected_seq);
//         }

//         // Verify no duplicates (BTreeMap guarantees this, but good to
// verify)         let unique_sequences: HashSet<_> =
// replicator.buffered_writes.keys().collect();         assert_eq!
// (unique_sequences.len(), replicator.buffered_writes.len());     }
// }
