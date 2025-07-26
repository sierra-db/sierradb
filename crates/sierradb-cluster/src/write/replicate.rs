use std::{
    collections::{BTreeMap, btree_map::Entry},
    iter,
    time::{Duration, Instant},
};

use kameo::{mailbox::Signal, prelude::*};
use serde::{Deserialize, Serialize};
use sierradb::{
    bucket::PartitionId,
    database::{Database, ExpectedVersion, PartitionLatestSequence, Transaction, VersionGap},
    writer_thread_pool::AppendResult,
};
use smallvec::{SmallVec, smallvec};
use tokio::{
    select,
    time::{Interval, MissedTickBehavior},
};
use tracing::{debug, error, instrument, warn};

use crate::ClusterActor;

use super::error::WriteError;

pub struct PartitionReplicatorActor {
    partition_id: PartitionId,
    database: Database,
    next_expected_seq: u64,
    buffered_writes: BTreeMap<u64, BufferedWrite>,
    buffer_size: usize,
    buffer_timeout: Duration,
    gap_detection_timeout: Duration,
    catch_up_ranges: BTreeMap<u64, u64>, // start_seq -> end_seq (inclusive)
    gc_interval: Interval,
}

pub struct PartitionReplicatorActorArgs {
    pub partition_id: PartitionId,
    pub database: Database,
    /// Maximum number of out-of-order writes to buffer per partition
    pub buffer_size: usize,
    /// Maximum time to keep buffered writes before timing out
    pub buffer_timeout: Duration,
    // /// Number of sequences behind before triggering catch-up
    // pub catch_up_threshold: u64,
    pub gap_detection_timeout: Duration,
}

impl Actor for PartitionReplicatorActor {
    type Args = PartitionReplicatorActorArgs;
    type Error = WriteError;

    async fn on_start(
        PartitionReplicatorActorArgs {
            partition_id,
            database,
            buffer_size,
            buffer_timeout,
            gap_detection_timeout,
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

        let mut gc_interval =
            tokio::time::interval((buffer_timeout / 5).max(gap_detection_timeout / 3));
        gc_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        Ok(PartitionReplicatorActor {
            partition_id,
            database,
            next_expected_seq,
            buffered_writes: BTreeMap::new(),
            buffer_size,
            buffer_timeout,
            gap_detection_timeout,
            catch_up_ranges: BTreeMap::new(),
            gc_interval,
        })
    }

    async fn next(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        mailbox_rx: &mut MailboxReceiver<Self>,
    ) -> Option<Signal<Self>> {
        loop {
            select! {
                msg = mailbox_rx.recv() => return msg,
                _ = self.gc_interval.tick() => {
                    self.detect_and_handle_gaps();
                    self.garbage_collect();
                }
            }
        }
    }
}

impl PartitionReplicatorActor {
    fn detect_and_handle_gaps(&mut self) {
        // Look for the oldest buffered write to detect gaps
        if let Some((&oldest_buffered_seq, oldest_write)) = self.buffered_writes.first_key_value() {
            let expected_seq = self.next_expected_seq;
            let gap_size = oldest_buffered_seq - expected_seq;
            let wait_time = oldest_write.received_at.elapsed();

            // Gap detected if:
            // 1. We have buffered writes waiting for missing sequences (gap_size > 0)
            // 2. They've been waiting longer than our gap detection threshold
            // 3. We're not already catching up this range
            if gap_size > 0
                && wait_time > self.gap_detection_timeout
                && !self.is_range_in_catch_up(expected_seq, oldest_buffered_seq - 1)
            {
                warn!(
                    partition_id = self.partition_id,
                    expected_seq,
                    oldest_buffered_seq,
                    gap_size,
                    wait_time_ms = wait_time.as_millis(),
                    "sequence gap detected, triggering catch-up"
                );

                // Mark this range as being caught up and trigger the catch-up process
                self.trigger_catch_up(expected_seq, oldest_buffered_seq - 1);
            }
        }
    }

    fn trigger_catch_up(&mut self, from_seq: u64, to_seq: u64) {
        // Mark the range as being caught up
        self.catch_up_ranges.insert(from_seq, to_seq);

        debug!(
            partition_id = self.partition_id,
            from_seq, to_seq, "Starting catch-up for sequence range"
        );

        // TODO: Actually trigger the catch-up process
        // This would involve:
        // 1. Getting the coordinator for this partition
        // 2. Sending a PartitionSyncRequest
        // 3. Applying the returned events
        // 4. Calling mark_catch_up_complete when done

        // For now, we'll just spawn a placeholder task
        let partition_id = self.partition_id;
        tokio::spawn(async move {
            // Placeholder: In real implementation, this would perform catch-up
            warn!(
                partition_id,
                from_seq, to_seq, "TODO: Implement actual catch-up logic"
            );
        });
    }

    fn is_sequence_in_catch_up_range(&self, seq: u64) -> bool {
        is_sequence_in_catch_up_range(&self.catch_up_ranges, seq)
    }

    fn is_range_in_catch_up(&self, from_seq: u64, to_seq: u64) -> bool {
        for (&start, &end) in &self.catch_up_ranges {
            // Check if the requested range overlaps with any existing catch-up range
            if !(to_seq < start || from_seq > end) {
                return true;
            }
        }
        false
    }

    fn mark_catch_up_complete(&mut self, from_seq: u64, to_seq: u64) {
        if let Some(&existing_end) = self.catch_up_ranges.get(&from_seq) {
            if existing_end == to_seq {
                self.catch_up_ranges.remove(&from_seq);
                debug!(
                    partition_id = self.partition_id,
                    from_seq, to_seq, "catch-up completed for sequence range"
                );
            } else {
                warn!(
                    partition_id = self.partition_id,
                    from_seq,
                    to_seq,
                    existing_end,
                    "catch-up completion mismatch - expected end sequence doesn't match"
                );
            }
        } else {
            warn!(
                partition_id = self.partition_id,
                from_seq, to_seq, "attempted to mark catch-up complete for unknown range"
            );
        }
    }

    fn garbage_collect(&mut self) {
        self.buffered_writes.retain(|&partition_sequence, write| {
            // Don't garbage collect writes that are part of active catch-up ranges
            if is_sequence_in_catch_up_range(&self.catch_up_ranges, partition_sequence) {
                debug!(
                    partition_id = self.partition_id,
                    partition_sequence, "preserving buffered write - part of active catch-up range"
                );
                return true;
            }

            if write.received_at.elapsed() <= self.buffer_timeout {
                return true;
            }

            debug!(
                partition_id = self.partition_id,
                partition_sequence,
                elapsed_ms = write.received_at.elapsed().as_millis(),
                "buffered replicate write timed out"
            );

            for tx in write.reply_senders.drain(..) {
                tx.send(Err(WriteError::RequestTimeout));
            }

            false
        });
    }

    /// Handles incoming write requests by either processing them immediately if
    /// they're the next expected sequence, or buffering them for future
    /// sequential processing, with duplicate detection and conflict resolution.
    fn buffer_write(
        &mut self,
        tx: Transaction,
        reply_sender: Option<ReplySender<Result<AppendResult, WriteError>>>,
    ) -> Result<Option<BufferedWrite>, BufferWriteError> {
        let expected_next_sequence = tx
            .get_expected_partition_sequence()
            .into_next_version()
            .expect("partition sequence should not reach max");
        if expected_next_sequence == self.next_expected_seq {
            match self.buffered_writes.entry(self.next_expected_seq) {
                Entry::Vacant(_) => {
                    return Ok(Some(BufferedWrite::new(tx, reply_sender)));
                }
                Entry::Occupied(entry) => {
                    if entry.get().tx.transaction_id() == tx.transaction_id() {
                        debug_assert_eq!(entry.get().tx, tx);
                        let existing = entry.remove();
                        let buffered_write = BufferedWrite::new_with_merged_reply_senders(
                            tx,
                            existing.reply_senders,
                            reply_sender,
                        );
                        return Ok(Some(buffered_write));
                    } else {
                        return Err(BufferWriteError::new(
                            WriteError::SequenceConflict,
                            reply_sender,
                        ));
                    }
                }
            }
        } else if expected_next_sequence < self.next_expected_seq {
            return Err(BufferWriteError::new(WriteError::StaleWrite, reply_sender));
        }

        if self.buffered_writes.len() >= self.buffer_size
            && let Some(last_entry) = self.buffered_writes.last_entry()
        {
            if last_entry.key() > &expected_next_sequence {
                debug!(
                    "evicting buffered write seq {} to make room for seq {}",
                    last_entry.key(),
                    expected_next_sequence
                );
                for tx in last_entry.remove().reply_senders {
                    tx.send(Err(WriteError::BufferEvicted));
                }
            } else {
                return Err(BufferWriteError::new(WriteError::BufferFull, reply_sender));
            }
        }

        match self.buffered_writes.entry(expected_next_sequence) {
            Entry::Vacant(entry) => {
                entry.insert(BufferedWrite::new(tx, reply_sender));
                Ok(None)
            }
            Entry::Occupied(mut entry) => {
                if entry.get().tx.transaction_id() == tx.transaction_id() {
                    debug_assert_eq!(entry.get().tx, tx);
                    debug!(
                        "received duplicate write for partition {} sequence {expected_next_sequence}",
                        self.partition_id
                    );
                    if let Some(tx) = reply_sender {
                        entry.get_mut().reply_senders.push(tx);
                    }
                    Ok(None) // Already buffered, treat as successful no-op
                } else {
                    // CONFLICT: Different transaction with same sequence
                    Err(BufferWriteError::new(
                        WriteError::SequenceConflict,
                        reply_sender,
                    ))
                }
            }
        }
    }

    fn pop_next_buffered_write(&mut self) -> Option<BufferedWrite> {
        self.buffered_writes.remove(&self.next_expected_seq)
    }
}

fn is_sequence_in_catch_up_range(catch_up_ranges: &BTreeMap<u64, u64>, seq: u64) -> bool {
    for (start, end) in catch_up_ranges {
        if &seq >= start && &seq <= end {
            return true;
        }
    }
    false
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

struct BufferedWrite {
    tx: Transaction,
    reply_senders: SmallVec<[ReplySender<Result<AppendResult, WriteError>>; 4]>,
    received_at: Instant,
}

impl BufferedWrite {
    fn new(
        tx: Transaction,
        reply_sender: Option<ReplySender<Result<AppendResult, WriteError>>>,
    ) -> Self {
        BufferedWrite {
            tx,
            reply_senders: reply_sender.map(|tx| smallvec![tx]).unwrap_or_default(),
            received_at: Instant::now(),
        }
    }

    fn new_with_merged_reply_senders(
        tx: Transaction,
        mut reply_senders: SmallVec<[ReplySender<Result<AppendResult, WriteError>>; 4]>,
        reply_sender: Option<ReplySender<Result<AppendResult, WriteError>>>,
    ) -> Self {
        BufferedWrite {
            tx,
            reply_senders: match reply_sender {
                Some(tx) => {
                    reply_senders.push(tx);
                    reply_senders
                }
                None => reply_senders,
            },
            received_at: Instant::now(),
        }
    }
}

/// Message to replicate a write to another partition
#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicateWrite {
    pub coordinator_ref: RemoteActorRef<ClusterActor>,
    pub coordinator_alive_since: u64,
    pub transaction: Transaction,
}

#[remote_message("ae8dc4cc-e382-4a68-9451-d10c5347d3c9")]
impl Message<ReplicateWrite> for ClusterActor {
    type Reply = ForwardedReply<ReplicateWrite, DelegatedReply<Result<AppendResult, WriteError>>>;

    #[instrument(skip(self, ctx))]
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

    #[instrument(skip(self, ctx))]
    async fn handle(
        &mut self,
        msg: ReplicateWrite,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let (delegated_reply, reply_sender) = ctx.reply_sender();

        let mut next_write = match self.buffer_write(msg.transaction, reply_sender) {
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
            let res = self.database.append_events(write.tx).await.map_err(|err| {
                match err {
                    sierradb::error::WriteError::WrongExpectedSequence {
                        current,
                        expected,
                        ..
                    } => {
                        match expected.gap_from(current) {
                            VersionGap::None => {}
                            VersionGap::Ahead(_) => {
                                // Duplicate/already processed
                            }
                            VersionGap::Behind(_) => {
                                // Gap detected, need catchup
                            }
                            VersionGap::Incompatible => {
                                unreachable!(
                                    "we verified the transaction has a valid expected sequence"
                                )
                            }
                        }

                        WriteError::WrongExpectedSequence { current, expected }
                    }
                    err => WriteError::DatabaseOperationFailed(err.to_string()),
                }
            });

            let seq_range = res
                .as_ref()
                .map(|append| {
                    (
                        append.first_partition_sequence,
                        append.last_partition_sequence,
                    )
                })
                .ok();

            for (res, tx) in iter::repeat_n(res, write.reply_senders.len()).zip(write.reply_senders)
            {
                tx.send(res);
            }

            match seq_range {
                Some((first, last)) => {
                    debug!(
                        "successfully wrote partition {} sequences {first}-{last}",
                        self.partition_id
                    );

                    assert_eq!(self.next_expected_seq, first);
                    self.next_expected_seq = last + 1;
                    next_write = self.pop_next_buffered_write();
                }
                None => break,
            }
        }

        delegated_reply
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashSet},
        time::Duration,
    };

    use kameo::{
        Actor,
        prelude::{Context, Message},
        reply::{DelegatedReply, ReplySender},
    };
    use sierradb::{
        StreamId,
        bucket::PartitionId,
        database::{
            Database, DatabaseBuilder, ExpectedVersion, NewEvent, PartitionLatestSequence,
            Transaction,
        },
        id::{uuid_to_partition_hash, uuid_v7_with_partition_hash},
        writer_thread_pool::AppendResult,
    };
    use smallvec::smallvec;
    use tempfile::tempdir;
    use tokio::sync::oneshot;
    use uuid::Uuid;

    use crate::write::{
        error::WriteError,
        replicate::{BufferWriteError, PartitionReplicatorActor},
    };

    async fn create_temp_db() -> (tempfile::TempDir, Database) {
        let temp_dir = tempdir().expect("Failed to create temp directory");
        let db = DatabaseBuilder::new()
            .flush_interval_events(1)
            .writer_threads(2)
            .reader_threads(2)
            .total_buckets(4)
            .bucket_ids_from_range(0..4)
            .open(temp_dir.path())
            .expect("Failed to open database");
        (temp_dir, db)
    }

    fn create_test_event(
        partition_key: Uuid,
        stream_id_str: &str,
        version: ExpectedVersion,
    ) -> NewEvent {
        NewEvent {
            event_id: uuid_v7_with_partition_hash(uuid_to_partition_hash(partition_key)),
            stream_id: StreamId::new(stream_id_str).expect("Invalid stream ID"),
            stream_version: version,
            event_name: "test_event".to_string(),
            timestamp: 12345678,     // Fixed timestamp for testing
            metadata: vec![1, 2, 3], // Some test metadata
            payload: b"test payload".to_vec(),
        }
    }

    fn create_test_transaction(expected_sequence: ExpectedVersion) -> Transaction {
        let partition_key = Uuid::new_v4();
        Transaction::new(
            partition_key,
            1,
            smallvec![create_test_event(
                partition_key,
                "hii",
                ExpectedVersion::Any
            )],
        )
        .unwrap()
        .expected_partition_sequence(expected_sequence)
    }

    async fn setup_partition_replicator(
        partition_id: PartitionId,
        database: Database,
        next_expected_seq: Option<u64>,
    ) -> PartitionReplicatorActor {
        let next_expected_seq = match next_expected_seq {
            Some(n) => n,
            None => match database.get_partition_sequence(partition_id).await.unwrap() {
                Some(PartitionLatestSequence::LatestSequence { sequence, .. }) => sequence + 1,
                Some(PartitionLatestSequence::ExternalBucket { .. }) => {
                    todo!()
                }
                None => 0,
            },
        };

        PartitionReplicatorActor {
            partition_id,
            database,
            next_expected_seq,
            buffered_writes: BTreeMap::new(),
            buffer_size: 100,
            buffer_timeout: Duration::from_secs(10),
            gap_detection_timeout: Duration::from_secs(10),
            gc_interval: tokio::time::interval(Duration::from_secs(10)),
        }
    }

    async fn new_reply_sender() -> ReplySender<Result<AppendResult, WriteError>> {
        #[derive(Actor)]
        struct ReplySenderObtainer;

        struct ObtainReplySender(oneshot::Sender<ReplySender<Result<AppendResult, WriteError>>>);

        impl Message<ObtainReplySender> for ReplySenderObtainer {
            type Reply = DelegatedReply<Result<AppendResult, WriteError>>;

            async fn handle(
                &mut self,
                msg: ObtainReplySender,
                ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                let (delegated_reply, reply_sender) = ctx.reply_sender();
                let _ = msg.0.send(reply_sender.unwrap());
                delegated_reply
            }
        }

        let obtainer_ref = ReplySenderObtainer::spawn(ReplySenderObtainer);
        let (reply_tx, reply_rx) = oneshot::channel();
        #[allow(clippy::let_underscore_future)]
        let _ = obtainer_ref
            .ask(ObtainReplySender(reply_tx))
            .enqueue()
            .await
            .unwrap();
        reply_rx.await.unwrap()
    }

    #[tokio::test]
    async fn test_sequential_write_immediate_processing() {
        let (_temp_dir, db) = create_temp_db().await;
        let mut replicator = setup_partition_replicator(1, db, None).await;

        let reply_sender = new_reply_sender().await;
        let tx = create_test_transaction(ExpectedVersion::Empty);
        let result = replicator.buffer_write(tx, Some(reply_sender));

        assert!(matches!(result, Ok(Some(_))));
        assert_eq!(replicator.buffered_writes.len(), 0);
    }

    #[tokio::test]
    async fn test_out_of_order_write_buffering() {
        let (_temp_dir, db) = create_temp_db().await;
        let mut replicator = setup_partition_replicator(1, db, Some(5)).await;

        let reply_sender = new_reply_sender().await;
        let tx = create_test_transaction(ExpectedVersion::Exact(7));
        let result = replicator.buffer_write(tx, Some(reply_sender));

        assert!(matches!(result, Ok(None)));
        assert_eq!(replicator.buffered_writes.len(), 1);
        assert!(replicator.buffered_writes.contains_key(&8));
    }

    #[tokio::test]
    async fn test_stale_write_rejection() {
        let (_temp_dir, db) = create_temp_db().await;
        let mut replicator = setup_partition_replicator(1, db, Some(10)).await;

        let reply_sender = new_reply_sender().await;
        let tx = create_test_transaction(ExpectedVersion::Exact(5));
        let result = replicator.buffer_write(tx, Some(reply_sender));

        assert!(matches!(
            result,
            Err(BufferWriteError {
                error: WriteError::StaleWrite,
                reply_sender: Some(_)
            })
        ));
        assert_eq!(replicator.buffered_writes.len(), 0);
    }

    #[tokio::test]
    async fn test_duplicate_write_handling() {
        let (_temp_dir, db) = create_temp_db().await;
        let mut replicator = setup_partition_replicator(1, db, Some(5)).await;

        let tx = create_test_transaction(ExpectedVersion::Exact(7));

        // First write
        let reply_sender = new_reply_sender().await;
        let result1 = replicator.buffer_write(tx.clone(), Some(reply_sender));
        assert!(matches!(result1, Ok(None)));

        // Duplicate write
        let reply_sender = new_reply_sender().await;
        let result2 = replicator.buffer_write(tx, Some(reply_sender));
        assert!(matches!(result2, Ok(None)));

        assert_eq!(replicator.buffered_writes.len(), 1);
        assert_eq!(
            replicator
                .buffered_writes
                .get(&8)
                .unwrap()
                .reply_senders
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn test_sequence_conflict() {
        let (_temp_dir, db) = create_temp_db().await;
        let mut replicator = setup_partition_replicator(1, db, Some(5)).await;

        let tx1 = create_test_transaction(ExpectedVersion::Exact(7));
        let tx2 = create_test_transaction(ExpectedVersion::Exact(7));

        // First write
        let reply_sender = new_reply_sender().await;
        let result1 = replicator.buffer_write(tx1, Some(reply_sender));
        assert!(matches!(result1, Ok(None)));

        // Conflicting write
        let reply_sender = new_reply_sender().await;
        let result2 = replicator.buffer_write(tx2, Some(reply_sender));
        assert!(matches!(
            result2,
            Err(BufferWriteError {
                error: WriteError::SequenceConflict,
                reply_sender: Some(_)
            })
        ));
    }

    #[tokio::test]
    async fn test_buffer_size_limit_with_eviction() {
        let (_temp_dir, db) = create_temp_db().await;
        let mut replicator = setup_partition_replicator(1, db, Some(5)).await;
        replicator.buffer_size = 2;

        // Fill buffer to capacity
        let reply_sender = new_reply_sender().await;
        let tx = create_test_transaction(ExpectedVersion::Exact(7));
        replicator.buffer_write(tx, Some(reply_sender)).unwrap();

        let reply_sender = new_reply_sender().await;
        let tx = create_test_transaction(ExpectedVersion::Exact(8));
        replicator.buffer_write(tx, Some(reply_sender)).unwrap();

        assert_eq!(replicator.buffered_writes.len(), 2);

        // Add lower sequence - should evict highest
        let reply_sender = new_reply_sender().await;
        let tx = create_test_transaction(ExpectedVersion::Exact(6));
        let result = replicator.buffer_write(tx, Some(reply_sender));
        assert!(matches!(result, Ok(None)));

        // Should have evicted sequence 8
        assert_eq!(replicator.buffered_writes.len(), 2);
        assert!(replicator.buffered_writes.contains_key(&7));
        assert!(replicator.buffered_writes.contains_key(&8));
        assert!(!replicator.buffered_writes.contains_key(&9));
    }

    #[tokio::test]
    async fn test_buffer_size_limit_rejection() {
        let (_temp_dir, db) = create_temp_db().await;
        let mut replicator = setup_partition_replicator(1, db, Some(5)).await;
        replicator.buffer_size = 2;

        // Fill buffer with lower sequences
        let reply_sender = new_reply_sender().await;
        let tx = create_test_transaction(ExpectedVersion::Exact(7));
        replicator.buffer_write(tx, Some(reply_sender)).unwrap();

        let reply_sender = new_reply_sender().await;
        let tx = create_test_transaction(ExpectedVersion::Exact(8));
        replicator.buffer_write(tx, Some(reply_sender)).unwrap();

        // Try to add higher sequence - should be rejected
        let reply_sender = new_reply_sender().await;
        let tx = create_test_transaction(ExpectedVersion::Exact(10));
        let result = replicator.buffer_write(tx, Some(reply_sender));

        assert!(matches!(
            result,
            Err(BufferWriteError {
                error: WriteError::BufferFull,
                reply_sender: Some(_)
            })
        ));
    }

    #[tokio::test]
    async fn test_pop_next_buffered_write() {
        let (_temp_dir, db) = create_temp_db().await;
        let mut replicator = setup_partition_replicator(1, db, Some(5)).await;

        // Buffer some writes
        let reply_sender = new_reply_sender().await;
        let tx = create_test_transaction(ExpectedVersion::Exact(6));
        replicator.buffer_write(tx, Some(reply_sender)).unwrap();

        let reply_sender = new_reply_sender().await;
        let tx = create_test_transaction(ExpectedVersion::Exact(5));
        replicator.buffer_write(tx, Some(reply_sender)).unwrap();

        let reply_sender = new_reply_sender().await;
        let tx = create_test_transaction(ExpectedVersion::Exact(4));
        replicator.buffer_write(tx, Some(reply_sender)).unwrap();

        // Advance to 6
        replicator.next_expected_seq += 1;
        let popped = replicator.pop_next_buffered_write();
        assert!(popped.is_some());
        assert_eq!(
            popped.unwrap().tx.get_expected_partition_sequence(),
            ExpectedVersion::Exact(5)
        );

        // Advance to 7
        replicator.next_expected_seq += 1;
        let popped = replicator.pop_next_buffered_write();
        assert!(popped.is_some());
        assert_eq!(
            popped.unwrap().tx.get_expected_partition_sequence(),
            ExpectedVersion::Exact(6)
        );

        // No more writes
        replicator.next_expected_seq += 1;
        let popped = replicator.pop_next_buffered_write();
        assert!(popped.is_none());
    }

    #[tokio::test]
    async fn test_immediate_processing_with_duplicate() {
        let (_temp_dir, db) = create_temp_db().await;
        let mut replicator = setup_partition_replicator(1, db, Some(5)).await;

        let tx = create_test_transaction(ExpectedVersion::Exact(5));

        // First write at next expected sequence
        let reply_sender = new_reply_sender().await;
        let result1 = replicator.buffer_write(tx.clone(), Some(reply_sender));

        // Should be buffered
        assert!(matches!(result1, Ok(None)));

        // Duplicate of same transaction
        replicator.next_expected_seq += 1;
        let reply_sender = new_reply_sender().await;
        let result2 = replicator.buffer_write(tx, Some(reply_sender));

        // Should merge reply senders and return merged write
        assert!(matches!(result2, Ok(Some(_))));
        if let Ok(Some(merged_write)) = result2 {
            assert_eq!(merged_write.reply_senders.len(), 2);
        }
    }

    #[tokio::test]
    async fn test_complex_buffering_scenario() {
        let (_temp_dir, db) = create_temp_db().await;
        let mut replicator = setup_partition_replicator(1, db, Some(11)).await;

        // Add writes: 12, 15, 11, 13, 10 (10 should be immediate)
        let sequences = vec![12, 15, 11, 13, 10];
        let mut immediate_count = 0;

        for seq in sequences {
            let tx = create_test_transaction(ExpectedVersion::Exact(seq));
            let reply_sender = new_reply_sender().await;
            match replicator.buffer_write(tx, Some(reply_sender)) {
                Ok(Some(_)) => immediate_count += 1,
                Ok(None) => {}
                Err(err) => panic!("Unexpected error: {err:?}"),
            }
        }

        assert_eq!(immediate_count, 1); // Only sequence 10
        assert_eq!(replicator.buffered_writes.len(), 4); // 11, 12, 13, 15

        // Process in order
        replicator.next_expected_seq += 1; // Now expecting 11
        let next = replicator.pop_next_buffered_write();
        assert!(next.is_some());
        assert_eq!(
            next.unwrap().tx.get_expected_partition_sequence(),
            ExpectedVersion::Exact(11)
        );

        replicator.next_expected_seq += 1; // Now expecting 12
        let next = replicator.pop_next_buffered_write();
        assert_eq!(
            next.unwrap().tx.get_expected_partition_sequence(),
            ExpectedVersion::Exact(12)
        );

        replicator.next_expected_seq += 1; // Now expecting 13
        let next = replicator.pop_next_buffered_write();
        assert_eq!(
            next.unwrap().tx.get_expected_partition_sequence(),
            ExpectedVersion::Exact(13)
        );

        replicator.next_expected_seq += 1; // Now expecting 14
        let next = replicator.pop_next_buffered_write();
        assert!(next.is_none()); // Gap at 14

        // Add 14
        let tx = create_test_transaction(ExpectedVersion::Exact(14));
        let reply_sender = new_reply_sender().await;
        replicator.buffer_write(tx, Some(reply_sender)).unwrap();

        replicator.next_expected_seq += 1; // Now expecting 15
        let next = replicator.pop_next_buffered_write();
        assert!(next.is_some());

        assert_eq!(replicator.buffered_writes.len(), 0);
    }

    #[tokio::test]
    async fn test_garbage_collection() {
        let (_temp_dir, db) = create_temp_db().await;
        let mut replicator = setup_partition_replicator(1, db, Some(5)).await;
        replicator.buffer_timeout = Duration::from_millis(1); // Very short timeout

        // Add a write
        let reply_sender = new_reply_sender().await;
        let tx = create_test_transaction(ExpectedVersion::Exact(7));
        replicator.buffer_write(tx, Some(reply_sender)).unwrap();
        assert_eq!(replicator.buffered_writes.len(), 1);

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(2));

        // Garbage collect
        replicator.garbage_collect();
        assert_eq!(replicator.buffered_writes.len(), 0);
    }

    #[tokio::test]
    async fn test_reply_sender_merging() {
        let (_temp_dir, db) = create_temp_db().await;
        let mut replicator = setup_partition_replicator(1, db, Some(5)).await;

        let tx = create_test_transaction(ExpectedVersion::Exact(6));

        // First write
        let reply_sender = new_reply_sender().await;
        replicator
            .buffer_write(tx.clone(), Some(reply_sender))
            .unwrap();

        // Duplicate with different reply sender
        let reply_sender = new_reply_sender().await;
        replicator.buffer_write(tx, Some(reply_sender)).unwrap();

        // Check that reply senders were merged
        let buffered = replicator.buffered_writes.get(&7).unwrap();
        dbg!(&buffered.reply_senders);
        assert_eq!(buffered.reply_senders.len(), 2);
    }

    // // ==========================================
    // // Property-Based Tests
    // // ==========================================

    #[tokio::test]
    async fn test_buffer_invariants() {
        let (_temp_dir, db) = create_temp_db().await;
        let mut replicator = setup_partition_replicator(1, db, Some(0)).await;
        replicator.buffer_size = 100;

        // Add 1000 random writes
        for i in 0..1000 {
            let seq = (i * 17 + 7) % 200; // Generate pseudo-random but deterministic sequences
            let tx = create_test_transaction(ExpectedVersion::Exact(seq));
            let reply_sender = new_reply_sender().await;
            let _ = replicator.buffer_write(tx.clone(), Some(reply_sender));
        }

        // Verify buffer size constraint
        assert!(replicator.buffered_writes.len() <= 100);

        // Verify all buffered sequences are >= next_expected_seq
        for &seq in replicator.buffered_writes.keys() {
            assert!(seq >= replicator.next_expected_seq);
        }

        // Verify no duplicates (BTreeMap guarantees this, but good to verify)
        let unique_sequences: HashSet<_> = replicator.buffered_writes.keys().collect();
        assert_eq!(unique_sequences.len(), replicator.buffered_writes.len());
    }
}
