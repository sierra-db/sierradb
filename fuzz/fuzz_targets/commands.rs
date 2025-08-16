#![no_main]

extern crate arbitrary;

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Duration;

use arbitrary::{Arbitrary, Unstructured, size_hint};
libfuzzer_sys::fuzz_target!(|commands: Commands| -> libfuzzer_sys::Corpus { run(commands) });

use sierradb::StreamId;
use sierradb::bucket::BucketId;
use sierradb::bucket::segment::EventRecord;
use sierradb::database::{
    Database, DatabaseBuilder, ExpectedVersion, NewEvent, PartitionLatestSequence,
    StreamLatestVersion, Transaction,
};
use sierradb::error::{StreamIdError, WriteError};
use sierradb::id::{NAMESPACE_PARTITION_KEY, uuid_to_partition_hash, uuid_v7_with_partition_hash};
use sierradb::writer_thread_pool::AppendResult;
use tempfile::TempDir;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct ModelEvent {
    event_id: Uuid,
    partition_id: u64,
    partition_key: Uuid,
    partition_sequence: u64,
    stream_id: StreamId,
    stream_version: u64,
    transaction_id: Uuid,
    event_name: String,
    timestamp: u64,
    metadata: Vec<u8>,
    payload: Vec<u8>,
    confirmation_count: u8,
    bucket_id: BucketId,
}

#[derive(Debug, Default)]
struct DatabaseModel {
    partitions: HashMap<u64, BTreeMap<u64, ModelEvent>>,
    streams: HashMap<(u64, StreamId), u64>,
    confirmations: HashMap<u64, HashMap<Vec<u64>, u8>>,
    transactions: HashMap<Uuid, Vec<ModelEvent>>,
    bucket_distribution: HashMap<BucketId, HashSet<u64>>,
    total_buckets: u16,
}

impl DatabaseModel {
    fn new(total_buckets: u16) -> Self {
        Self {
            total_buckets,
            ..Default::default()
        }
    }

    fn get_bucket_id(&self, partition_id: u64) -> BucketId {
        (partition_id % self.total_buckets as u64) as BucketId
    }

    fn append_events(&mut self, transaction: &Transaction, result: &AppendResult) {
        let partition_id = transaction.partition_id() as u64;
        let bucket_id = self.get_bucket_id(partition_id);
        let partition_entry = self.partitions.entry(partition_id).or_default();

        self.bucket_distribution
            .entry(bucket_id)
            .or_default()
            .insert(partition_id);

        let mut transaction_events = Vec::new();

        for (i, event) in transaction.events().iter().enumerate() {
            let partition_sequence = result.first_partition_sequence + i as u64;
            let stream_version = result.stream_versions[&event.stream_id];

            let model_event = ModelEvent {
                event_id: event.event_id,
                partition_id,
                partition_key: transaction.partition_key(),
                partition_sequence,
                stream_id: event.stream_id.clone(),
                stream_version,
                transaction_id: transaction.transaction_id(),
                event_name: event.event_name.clone(),
                timestamp: event.timestamp,
                metadata: event.metadata.clone(),
                payload: event.payload.clone(),
                confirmation_count: 0,
                bucket_id,
            };

            partition_entry.insert(partition_sequence, model_event.clone());
            self.streams
                .insert((partition_id, event.stream_id.clone()), stream_version);
            transaction_events.push(model_event);
        }

        self.transactions
            .insert(transaction.transaction_id(), transaction_events);
    }

    fn set_confirmations(&mut self, partition_id: u64, offsets: Vec<u64>, confirmation_count: u8) {
        self.confirmations
            .entry(partition_id)
            .or_default()
            .insert(offsets, confirmation_count);

        // Update confirmation counts in stored events
        if let Some(partition_events) = self.partitions.get_mut(&partition_id) {
            for event in partition_events.values_mut() {
                event.confirmation_count = confirmation_count;
            }
        }
    }

    fn get_event(&self, partition_id: u64, event_id: Uuid) -> Option<&ModelEvent> {
        self.partitions
            .get(&partition_id)?
            .values()
            .find(|event| event.event_id == event_id)
    }

    fn get_partition_sequence(&self, partition_id: u64) -> Option<u64> {
        self.partitions
            .get(&partition_id)
            .and_then(|events| events.keys().max().copied())
    }

    fn get_stream_version(&self, partition_id: u64, stream_id: &StreamId) -> Option<u64> {
        self.streams
            .get(&(partition_id, stream_id.clone()))
            .copied()
    }

    fn get_partition_events_from(&self, partition_id: u64, from_sequence: u64) -> Vec<&ModelEvent> {
        self.partitions
            .get(&partition_id)
            .map(|events| {
                events
                    .range(from_sequence..)
                    .map(|(_, event)| event)
                    .collect()
            })
            .unwrap_or_default()
    }

    fn get_stream_events_from(
        &self,
        partition_id: u64,
        stream_id: &StreamId,
        from_version: u64,
    ) -> Vec<&ModelEvent> {
        self.partitions
            .get(&partition_id)
            .map(|events| {
                events
                    .values()
                    .filter(|event| {
                        event.stream_id == *stream_id && event.stream_version >= from_version
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn get_transaction_events(&self, transaction_id: Uuid) -> Option<&Vec<ModelEvent>> {
        self.transactions.get(&transaction_id)
    }

    fn validate_bucket_assignment(&self, partition_id: u64, expected_bucket_id: BucketId) -> bool {
        let actual_bucket_id = self.get_bucket_id(partition_id);
        actual_bucket_id == expected_bucket_id
    }

    fn validate_partition_sequence_ordering(&self, partition_id: u64) -> Result<(), String> {
        if let Some(events) = self.partitions.get(&partition_id) {
            let mut prev_sequence: Option<u64> = None;
            for (&sequence, event) in events {
                if event.partition_sequence != sequence {
                    return Err(format!(
                        "Partition sequence mismatch: key={}, event.partition_sequence={}",
                        sequence, event.partition_sequence
                    ));
                }
                if let Some(prev) = prev_sequence
                    && sequence <= prev
                {
                    return Err(format!(
                        "Partition sequences not properly ordered: {sequence} <= {prev}"
                    ));
                }
                prev_sequence = Some(sequence);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, serde::Serialize)]
struct InitArgs {
    segment_size: usize,
    total_buckets: u16,
    writer_threads: u16,
    reader_threads: u16,
}

impl<'a> Arbitrary<'a> for InitArgs {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let segment_size = u.int_in_range(4096..=(32 * 1024))?; // Bias toward small segments (4KB-32KB) to force rollover
        let total_buckets = u.int_in_range(1..=64)?;
        let writer_threads = arbitrary_divisor(u, total_buckets)?;
        let reader_threads = u.int_in_range(1..=4)?;

        Ok(InitArgs {
            segment_size,
            total_buckets,
            writer_threads,
            reader_threads,
        })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        size_hint::and_all(&[
            usize::size_hint(depth), // segment_size
            u16::size_hint(depth),   // total_buckets
            usize::size_hint(depth), /* writer_threads (for arbitrary_divisor's internal
                                      * arbitrary calls) */
            u16::size_hint(depth), // reader_threads
        ])
    }
}

fn arbitrary_divisor(
    u: &mut arbitrary::Unstructured,
    total_buckets: u16,
) -> arbitrary::Result<u16> {
    if total_buckets == 0 {
        return Ok(1); // Handle edge case
    }

    // Find all divisors
    let mut divisors = Vec::new();
    for i in 1..=total_buckets {
        if total_buckets.is_multiple_of(i) {
            divisors.push(i);
        }
    }

    // Choose a random divisor
    let index = u.arbitrary::<usize>()? % divisors.len();
    Ok(divisors[index])
}

struct TestState {
    database: Database,
    temp_dir: TempDir,
    model: DatabaseModel,
    init_args: InitArgs,
}

impl TestState {
    async fn new(init_args: InitArgs) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let database = DatabaseBuilder::new()
            .segment_size(init_args.segment_size)
            .total_buckets(init_args.total_buckets)
            .bucket_ids_from_range(0..init_args.total_buckets)
            .writer_threads(init_args.writer_threads)
            .reader_threads(init_args.reader_threads)
            .flush_interval_duration(Duration::MAX)
            .flush_interval_events(1)
            .open(temp_dir.path())
            .unwrap();

        let model = DatabaseModel::new(init_args.total_buckets);

        Self {
            database,
            temp_dir,
            model,
            init_args,
        }
    }

    async fn reopen_database(&mut self) {
        self.database = DatabaseBuilder::new()
            .segment_size(self.init_args.segment_size)
            .total_buckets(self.init_args.total_buckets)
            .bucket_ids_from_range(0..self.init_args.total_buckets)
            .writer_threads(self.init_args.writer_threads)
            .reader_threads(self.init_args.reader_threads)
            .flush_interval_duration(Duration::MAX)
            .flush_interval_events(1)
            .open(self.temp_dir.path())
            .unwrap();
    }
}

#[derive(Debug, Clone, serde::Serialize)]
enum Command {
    AppendSingle(AppendEvent),
    AppendMultiple {
        events: Vec<AppendEvent>,
    },
    SetConfirmations {
        partition_id: Option<u64>,
        confirmation_count: u8,
    },
    ReadEvent {
        partition_id: Option<u64>,
        event_id: Option<Uuid>,
    },
    ReadTransaction {
        partition_id: Option<u64>,
        first_event_id: Option<Uuid>,
    },
    ReadPartition {
        partition_id: Option<u64>,
        from_sequence: Option<u64>,
    },
    GetPartitionSequence {
        partition_id: Option<u64>,
    },
    ReadStream {
        partition_id: Option<u64>,
        stream_id: Option<StreamId>,
        from_version: Option<u64>,
        reverse: bool,
    },
    GetStreamVersion {
        partition_id: Option<u64>,
        stream_id: Option<StreamId>,
    },
    ReopenDatabase,
}

// NOTE: This implementation is now replaced by the smart command generation in
// Commands::generate_smart_command The old uniform random approach led to
// repetitive sequences like multiple consecutive identical operations
/*
impl<'a> Arbitrary<'a> for Command {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        // ... old implementation removed for brevity ...
        // See Commands::generate_smart_command for the new intelligent approach
        todo!("Use Commands::generate_smart_command instead")
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        // Rough estimate for command size
        (1, Some(200))
    }
}
*/

fn arbitrary_partition_id<'a>(
    u: &mut Unstructured<'a>,
    total_buckets: u16,
) -> arbitrary::Result<u64> {
    // Generate realistic partition IDs that reflect real-world usage:
    // - Most partitions distributed across all buckets
    // - Some edge cases with very high partition IDs
    // - Bounded to reasonable limits for fuzzing
    let max_realistic = (total_buckets as u64 * 16).min(4096);
    u.int_in_range(0..=max_realistic)
}

fn arbitrary_sequence<'a>(u: &mut Unstructured<'a>) -> arbitrary::Result<u64> {
    u.int_in_range(0..=10_000)
}

fn arbitrary_version<'a>(u: &mut Unstructured<'a>) -> arbitrary::Result<u64> {
    u.int_in_range(0..=1_000)
}

fn arbitrary_confirmation_count<'a>(u: &mut Unstructured<'a>) -> arbitrary::Result<u8> {
    u.int_in_range(0..=5)
}

#[derive(Debug, Clone, serde::Serialize)]
struct ArbitraryStreamId(StreamId);

impl<'a> Arbitrary<'a> for ArbitraryStreamId {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let len = u.int_in_range(1..=64)?;
        let s: String = (0..len)
            .map(|_| u.arbitrary::<char>())
            .collect::<Result<String, _>>()?;
        Ok(ArbitraryStreamId(StreamId::new(s).map_err(
            |err| match err {
                StreamIdError::InvalidLength => arbitrary::Error::NotEnoughData,
                StreamIdError::ContainsNullByte => arbitrary::Error::IncorrectFormat,
            },
        )?))
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (0, Some(64 * 4 + std::mem::size_of::<usize>()))
    }
}

#[derive(Debug, Clone, serde::Serialize)]
struct AppendEvent {
    partition_id: u64,
    stream_id: ArbitraryStreamId,
    expected_version: ExpectedVersion,
    event_name: String,
    timestamp: u64,
    metadata: Vec<u8>,
    payload: Vec<u8>,
    partition_key_strategy: PartitionKeyStrategy,
}

#[derive(Debug, Clone, serde::Serialize)]
enum PartitionKeyStrategy {
    /// Use Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id.as_bytes())
    DeterministicFromStream,
    /// Use an arbitrary UUID (not generated at runtime)
    Arbitrary(Uuid),
}

impl<'a> Arbitrary<'a> for PartitionKeyStrategy {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        match u.int_in_range(0..=1)? {
            0 => Ok(PartitionKeyStrategy::DeterministicFromStream),
            1 => Ok(PartitionKeyStrategy::Arbitrary(Uuid::from_bytes(
                u.arbitrary()?,
            ))),
            _ => unreachable!(),
        }
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (1, Some(17)) // 1 byte for choice + 16 bytes for UUID
    }
}

impl AppendEvent {
    fn arbitrary_with_buckets<'a>(
        u: &mut Unstructured<'a>,
        total_buckets: u16,
    ) -> arbitrary::Result<Self> {
        Self::arbitrary_with_buckets_and_segment_size(u, total_buckets, 64 * 1024) // Default to 64KB
    }

    fn arbitrary_with_buckets_and_segment_size<'a>(
        u: &mut Unstructured<'a>,
        total_buckets: u16,
        segment_size: usize,
    ) -> arbitrary::Result<Self> {
        let expected_version = match u.int_in_range(0..=3)? {
            0 => ExpectedVersion::Any,
            1 => ExpectedVersion::Empty,
            2 => ExpectedVersion::Exists,
            3 => ExpectedVersion::Exact(arbitrary_version(u)?),
            _ => unreachable!(),
        };

        // Generate valid timestamp (must be < 2^63)
        let timestamp = u.int_in_range(0..=i64::MAX as u64)?;

        // Calculate maximum sizes based on segment size to avoid
        // EventsExceedSegmentSize
        let header_estimate = 256; // Conservative estimate for headers and overhead
        let available_size = segment_size.saturating_sub(header_estimate); // Leave room for headers

        // Generate reasonably sized event name
        let max_event_name_len = (available_size / 10).min(32); // Use up to 10% for event name, max 32
        let event_name_len = if max_event_name_len > 0 {
            u.int_in_range(1..=max_event_name_len)?
        } else {
            1
        };
        let event_name = (0..event_name_len)
            .map(|_| u.int_in_range(b'a'..=b'z').map(|b| b as char))
            .collect::<Result<String, _>>()?;

        let remaining_size = available_size.saturating_sub(event_name.len());

        // Split remaining size between metadata and payload
        let metadata_max = (remaining_size / 3).min(128); // Up to 33% for metadata, max 128
        let metadata = if metadata_max > 0 {
            let len = u.int_in_range(0..=metadata_max)?;
            (0..len)
                .map(|_| u.arbitrary())
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };

        let payload_max = remaining_size.saturating_sub(metadata.len()).min(512); // Remaining for payload, max 512
        let payload = if payload_max > 0 {
            let len = u.int_in_range(0..=payload_max)?;
            (0..len)
                .map(|_| u.arbitrary())
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };

        Ok(AppendEvent {
            partition_id: arbitrary_partition_id(u, total_buckets)?,
            stream_id: ArbitraryStreamId::arbitrary(u)?,
            expected_version,
            event_name,
            timestamp,
            metadata,
            payload,
            partition_key_strategy: PartitionKeyStrategy::arbitrary(u)?,
        })
    }
}

impl<'a> Arbitrary<'a> for AppendEvent {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Self::arbitrary_with_buckets(u, 64) // Default bucket count
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        (10, Some(2000)) // Rough estimate
    }
}

#[derive(Debug, Clone)]
enum CommandType {
    Write,       // AppendSingle, AppendMultiple
    Read,        // ReadEvent, ReadTransaction, ReadPartition, ReadStream
    Query,       // GetPartitionSequence, GetStreamVersion
    Maintenance, // SetConfirmations, ReopenDatabase
}

#[derive(Debug, Clone)]
struct CommandGenerationState {
    /// Recent command history to prevent repetition
    recent_commands: VecDeque<CommandType>,
    /// Known partition IDs from previous appends
    known_partitions: HashSet<u64>,
    /// Known stream IDs from previous appends
    known_streams: HashSet<StreamId>,
    /// Approximate event IDs from previous appends (for targeted reads)
    known_events: Vec<uuid::Uuid>,
    /// Total buckets from init args
    total_buckets: u16,
    /// Segment size from init args
    segment_size: usize,
    /// Commands generated so far
    commands_generated: usize,
    /// Estimated bytes written per bucket (for segment rollover tracking)
    estimated_bytes_per_bucket: HashMap<BucketId, usize>,
}

impl CommandGenerationState {
    fn new(total_buckets: u16, segment_size: usize) -> Self {
        Self {
            recent_commands: VecDeque::with_capacity(5),
            known_partitions: HashSet::new(),
            known_streams: HashSet::new(),
            known_events: Vec::new(),
            total_buckets,
            segment_size,
            commands_generated: 0,
            estimated_bytes_per_bucket: HashMap::new(),
        }
    }

    fn add_command(&mut self, cmd_type: CommandType) {
        if self.recent_commands.len() >= 5 {
            self.recent_commands.pop_front();
        }
        self.recent_commands.push_back(cmd_type);
        self.commands_generated += 1;
    }

    fn track_append(&mut self, events: &[AppendEvent]) {
        for event in events {
            self.known_partitions.insert(event.partition_id);
            self.known_streams.insert(event.stream_id.0.clone());
            // Generate a mock event ID for this event (in real fuzzing, we'd use the actual
            // one)
            self.known_events.push(uuid::Uuid::new_v4());

            // Track estimated bytes written to this bucket
            let bucket_id = self.get_bucket_id(event.partition_id);
            let estimated_size = self.estimate_event_size(event);
            let previous_bytes = self
                .estimated_bytes_per_bucket
                .get(&bucket_id)
                .copied()
                .unwrap_or(0);
            let new_bytes = previous_bytes + estimated_size;
            self.estimated_bytes_per_bucket.insert(bucket_id, new_bytes);

            // SEGMENT ROLLOVER LOGGING: Log when buckets get close to segment size
            let usage_percentage = (new_bytes as f64 / self.segment_size as f64 * 100.0) as u32;
            if usage_percentage >= 70
                && ((previous_bytes as f64 / self.segment_size as f64 * 100.0) as u32) < 70
                && std::env::var("FUZZ_DEBUG").is_ok()
            {
                println!(
                    "🚨 SEGMENT ROLLOVER: Bucket {} reached {}% full ({} / {} bytes)",
                    bucket_id, usage_percentage, new_bytes, self.segment_size
                );
            }
            if usage_percentage >= 95 && std::env::var("FUZZ_DEBUG").is_ok() {
                println!(
                    "💥 SEGMENT ROLLOVER IMMINENT: Bucket {} at {}% full ({} / {} bytes)",
                    bucket_id, usage_percentage, new_bytes, self.segment_size
                );
            }
        }
    }

    fn get_bucket_id(&self, partition_id: u64) -> BucketId {
        (partition_id % self.total_buckets as u64) as BucketId
    }

    /// Estimate the storage size of an event (headers + data)
    fn estimate_event_size(&self, event: &AppendEvent) -> usize {
        // Based on the segment writer format, rough estimation:
        // - Timestamp: 8 bytes
        // - Transaction ID: 16 bytes
        // - CRC32C: 4 bytes
        // - Confirmation count: 1 byte
        // - Confirmation CRC32C: 4 bytes
        // - Event ID: 16 bytes
        // - Partition key: 16 bytes
        // - Partition ID: 2 bytes
        // - Partition sequence: 8 bytes
        // - Stream version: 8 bytes
        // - Stream ID length: 1 byte
        // - Event name length: 1 byte
        // - Metadata length: 4 bytes
        // - Payload length: 4 bytes
        let header_size = 8 + 16 + 4 + 1 + 4 + 16 + 16 + 2 + 8 + 8 + 1 + 1 + 4 + 4;

        header_size
            + event.stream_id.0.len()
            + event.event_name.len()
            + event.metadata.len()
            + event.payload.len()
    }

    /// Get the usage percentage for a bucket (0.0 to 1.0+)
    fn get_bucket_usage_percentage(&self, bucket_id: BucketId) -> f64 {
        let used_bytes = self
            .estimated_bytes_per_bucket
            .get(&bucket_id)
            .copied()
            .unwrap_or(0);
        used_bytes as f64 / self.segment_size as f64
    }

    /// Get buckets that are nearly full (>= threshold percentage)
    fn get_nearly_full_buckets(&self, threshold: f64) -> Vec<BucketId> {
        (0..self.total_buckets)
            .filter(|&bucket_id| self.get_bucket_usage_percentage(bucket_id) >= threshold)
            .collect()
    }

    /// Get buckets that need filling (< threshold percentage)
    fn get_buckets_needing_fill(&self, threshold: f64) -> Vec<BucketId> {
        (0..self.total_buckets)
            .filter(|&bucket_id| self.get_bucket_usage_percentage(bucket_id) < threshold)
            .collect()
    }

    /// Get weights for different command types based on current state
    fn get_command_weights(&self) -> [u32; 4] {
        let mut weights: [u32; 4] = [30, 35, 20, 15]; // [Write, Read, Query, Maintenance]

        // SEGMENT ROLLOVER LOGIC: Heavily favor writes when buckets need filling
        let nearly_full_buckets = self.get_nearly_full_buckets(0.7); // 70% full
        let empty_buckets = self.get_buckets_needing_fill(0.1); // Less than 10% full

        if !empty_buckets.is_empty() {
            // Boost writes significantly when we have empty buckets to fill
            weights[0] += 40;
        }

        if !nearly_full_buckets.is_empty() {
            // SEGMENT ROLLOVER MODE: Massively boost writes to push buckets over the edge
            weights[0] += 100; // Increased from 60 to be more aggressive
            // Reduce reads/queries to focus on filling segments
            weights[1] = weights[1].saturating_sub(20);
            weights[2] = weights[2].saturating_sub(15);

            // For buckets that are extremely close to full (95%+), make writes even more
            // likely
            let extremely_full_buckets = self.get_nearly_full_buckets(0.95);
            if !extremely_full_buckets.is_empty() {
                weights[0] += 150; // Near-certain writes when buckets are 95%+ full
                weights[1] = weights[1].saturating_sub(30);
                weights[2] = weights[2].saturating_sub(20);
            }
        }

        // Reduce weight for recently used command types
        for recent_cmd in &self.recent_commands {
            match recent_cmd {
                CommandType::Write => weights[0] = weights[0].saturating_sub(8),
                CommandType::Read => weights[1] = weights[1].saturating_sub(8),
                CommandType::Query => weights[2] = weights[2].saturating_sub(8),
                CommandType::Maintenance => weights[3] = weights[3].saturating_sub(8),
            }
        }

        // Boost reads if we have data to read (but not during segment rollover mode)
        if !self.known_partitions.is_empty() && nearly_full_buckets.is_empty() {
            weights[1] += 10;
            weights[2] += 5;
        }

        // Reduce maintenance operations early in the sequence
        if self.commands_generated < 3 {
            weights[3] = weights[3].saturating_sub(10);
        }

        // Strongly discourage consecutive maintenance operations
        if let Some(CommandType::Maintenance) = self.recent_commands.back() {
            weights[3] = 1;
        }

        // Ensure minimum weights
        for weight in &mut weights {
            *weight = (*weight).max(1);
        }

        weights
    }

    /// Check if we should avoid generating a specific command type
    fn should_avoid_repetition(&self, cmd_type: &CommandType) -> bool {
        // Count recent occurrences of this command type
        let recent_count = self
            .recent_commands
            .iter()
            .rev()
            .take(3)
            .filter(|&t| std::mem::discriminant(t) == std::mem::discriminant(cmd_type))
            .count();

        recent_count >= 2
    }

    /// Generate a data-driven read command using known data
    fn generate_targeted_read_command<'a>(
        &self,
        u: &mut Unstructured<'a>,
    ) -> arbitrary::Result<Option<Command>> {
        if self.known_partitions.is_empty() {
            return Ok(None);
        }

        // Choose a known partition ID
        let partition_ids: Vec<_> = self.known_partitions.iter().copied().collect();
        let partition_id = partition_ids[u.arbitrary::<usize>()? % partition_ids.len()];

        match u.int_in_range(0..=3)? {
            0 => {
                // ReadEvent with known event ID (if available)
                let event_id = if !self.known_events.is_empty() {
                    Some(self.known_events[u.arbitrary::<usize>()? % self.known_events.len()])
                } else {
                    None
                };
                Ok(Some(Command::ReadEvent {
                    partition_id: Some(partition_id),
                    event_id,
                }))
            }
            1 => {
                // ReadPartition
                Ok(Some(Command::ReadPartition {
                    partition_id: Some(partition_id),
                    from_sequence: if u.arbitrary()? {
                        Some(u.int_in_range(0..=10)?)
                    } else {
                        None
                    },
                }))
            }
            2 => {
                // ReadStream with known stream ID (if available)
                let stream_id = if !self.known_streams.is_empty() {
                    let streams: Vec<_> = self.known_streams.iter().cloned().collect();
                    Some(streams[u.arbitrary::<usize>()? % streams.len()].clone())
                } else {
                    None
                };
                Ok(Some(Command::ReadStream {
                    partition_id: Some(partition_id),
                    stream_id,
                    from_version: if u.arbitrary()? {
                        Some(u.int_in_range(0..=5)?)
                    } else {
                        None
                    },
                    reverse: u.arbitrary()?,
                }))
            }
            3 => {
                // GetPartitionSequence
                Ok(Some(Command::GetPartitionSequence {
                    partition_id: Some(partition_id),
                }))
            }
            _ => unreachable!(),
        }
    }

    /// Generates an event specifically targeted at a bucket to help fill it up
    fn generate_bucket_targeted_event(
        &self,
        u: &mut Unstructured,
        target_bucket: BucketId,
    ) -> arbitrary::Result<AppendEvent> {
        // Calculate remaining space in this bucket for aggressive sizing
        let bytes_used = self
            .estimated_bytes_per_bucket
            .get(&target_bucket)
            .copied()
            .unwrap_or(0);
        let remaining_bytes = self.segment_size.saturating_sub(bytes_used);

        // Make events larger to fill buckets faster (use up to 80% of remaining space)
        let target_event_size = (remaining_bytes * 4 / 5).min(self.segment_size / 4);

        // Generate partition ID that maps to our target bucket
        let target_partition_id = self.generate_partition_for_bucket(u, target_bucket)?;

        let mut event = AppendEvent::arbitrary_with_buckets_and_segment_size(
            u,
            self.total_buckets,
            target_event_size.max(1024), // Minimum 1KB for substantial events
        )?;

        // Force the event to our target partition (and thus bucket)
        event.partition_id = target_partition_id;

        Ok(event)
    }

    /// Generates multiple events for a bucket to create burst writes
    fn generate_bucket_targeted_events(
        &self,
        u: &mut Unstructured,
        target_bucket: BucketId,
        count: usize,
    ) -> arbitrary::Result<Vec<AppendEvent>> {
        let mut events = Vec::with_capacity(count);

        // Use a common strategy for the batch to ensure consistency
        let common_strategy = PartitionKeyStrategy::arbitrary(u)?;
        let common_stream_id = if matches!(
            common_strategy,
            PartitionKeyStrategy::DeterministicFromStream
        ) {
            Some(ArbitraryStreamId::arbitrary(u)?)
        } else {
            None
        };

        // Calculate size per event to fill the bucket
        let bytes_used = self
            .estimated_bytes_per_bucket
            .get(&target_bucket)
            .copied()
            .unwrap_or(0);
        let remaining_bytes = self.segment_size.saturating_sub(bytes_used);
        let size_per_event = (remaining_bytes / count).max(512); // At least 512 bytes per event

        for _ in 0..count {
            let mut event = self.generate_bucket_targeted_event(u, target_bucket)?;

            // Apply common strategy and stream ID for transaction consistency
            event.partition_key_strategy = common_strategy.clone();
            if let Some(ref stream_id) = common_stream_id {
                event.stream_id = stream_id.clone();
            }

            // Resize the event to our calculated size
            let current_size = self.estimate_event_size(&event);
            if current_size < size_per_event {
                // Pad the payload to reach target size
                let additional_bytes = size_per_event - current_size;
                let mut new_payload = event.payload.clone();
                new_payload.extend(vec![0u8; additional_bytes]);
                event.payload = new_payload;
            }

            events.push(event);
        }

        Ok(events)
    }

    /// Generates a partition ID that maps to the specified bucket
    fn generate_partition_for_bucket(
        &self,
        u: &mut Unstructured,
        target_bucket: BucketId,
    ) -> arbitrary::Result<u64> {
        // Simple approach: generate partition IDs until we find one that maps to our
        // target bucket
        for _attempt in 0..100 {
            // Limit attempts to avoid infinite loops
            let partition_id = u.int_in_range(0..=10000)?;
            if partition_id % (self.total_buckets as u64) == target_bucket as u64 {
                return Ok(partition_id);
            }
        }

        // Fallback: directly construct a partition ID that maps to our bucket
        Ok(target_bucket as u64)
    }
}

#[derive(Debug, Clone, serde::Serialize)]
struct Commands {
    init: InitArgs,
    commands: Vec<Command>,
}

impl Commands {
    #[allow(dead_code)]
    fn print_commands_summary(&self) {
        println!("Executing {} commands:", self.commands.len());
        for cmd in &self.commands {
            match cmd {
                Command::AppendSingle(_) => println!("  - Append single event"),
                Command::AppendMultiple { events } => {
                    println!("  - Append {} events", events.len())
                }
                Command::SetConfirmations {
                    confirmation_count, ..
                } => println!("  - Set confirmation count to {confirmation_count}"),
                Command::ReadEvent { .. } => println!("  - Read event by ID"),
                Command::ReadTransaction { .. } => println!("  - Read transaction by ID"),
                Command::ReadPartition { .. } => println!("  - Read partition"),
                Command::GetPartitionSequence { .. } => {
                    println!("  - Get partition sequence")
                }
                Command::ReadStream { .. } => println!("  - Read stream"),
                Command::GetStreamVersion { .. } => println!("  - Get stream version"),
                Command::ReopenDatabase => println!("  - Reopen database"),
            }
        }
    }
}

/// Weighted selection helper function
fn weighted_choice<'a>(u: &mut Unstructured<'a>, weights: &[u32]) -> arbitrary::Result<usize> {
    let total: u32 = weights.iter().sum();
    if total == 0 {
        return Ok(0); // fallback
    }

    let mut choice = u.int_in_range(0..=total.saturating_sub(1))?;
    for (i, &weight) in weights.iter().enumerate() {
        if choice < weight {
            return Ok(i);
        }
        choice -= weight;
    }
    Ok(weights.len() - 1) // fallback
}

impl Commands {
    fn generate_smart_command(
        u: &mut Unstructured,
        state: &mut CommandGenerationState,
    ) -> arbitrary::Result<Command> {
        // Get weights based on current state
        let weights = state.get_command_weights();
        let command_type_idx = weighted_choice(u, &weights)?;

        let command_type = match command_type_idx {
            0 => CommandType::Write,
            1 => CommandType::Read,
            2 => CommandType::Query,
            3 => CommandType::Maintenance,
            _ => CommandType::Write, // fallback
        };

        // Check anti-repetition
        if state.should_avoid_repetition(&command_type) {
            // Try a different command type
            let alternative_weights = match command_type {
                CommandType::Write => [5, 40, 25, 30],
                CommandType::Read => [40, 5, 25, 30],
                CommandType::Query => [40, 30, 5, 25],
                CommandType::Maintenance => [40, 30, 25, 5],
            };
            let alt_idx = weighted_choice(u, &alternative_weights)?;
            let alt_command_type = match alt_idx {
                0 => CommandType::Write,
                1 => CommandType::Read,
                2 => CommandType::Query,
                3 => CommandType::Maintenance,
                _ => CommandType::Write,
            };
            return Self::generate_command_of_type(u, state, alt_command_type);
        }

        Self::generate_command_of_type(u, state, command_type)
    }

    fn generate_command_of_type(
        u: &mut Unstructured,
        state: &mut CommandGenerationState,
        cmd_type: CommandType,
    ) -> arbitrary::Result<Command> {
        state.add_command(cmd_type.clone());

        match cmd_type {
            CommandType::Write => {
                // SEGMENT ROLLOVER: Try to generate bucket-targeted writes when buckets need
                // filling
                let nearly_full_buckets = state.get_nearly_full_buckets(0.7); // 70% full

                if !nearly_full_buckets.is_empty() && u.arbitrary::<bool>()? {
                    // Generate events specifically for nearly-full buckets
                    if u.arbitrary::<bool>()? {
                        // AppendSingle to a specific bucket
                        let target_bucket = nearly_full_buckets
                            [u.arbitrary::<usize>()? % nearly_full_buckets.len()];
                        let event = state.generate_bucket_targeted_event(u, target_bucket)?;
                        state.track_append(std::slice::from_ref(&event));
                        Ok(Command::AppendSingle(event))
                    } else {
                        // AppendMultiple burst to fill bucket quickly
                        let target_bucket = nearly_full_buckets
                            [u.arbitrary::<usize>()? % nearly_full_buckets.len()];
                        let count = u.int_in_range(2..=5)?; // Larger batches for segment filling
                        let events =
                            state.generate_bucket_targeted_events(u, target_bucket, count)?;
                        state.track_append(&events);
                        Ok(Command::AppendMultiple { events })
                    }
                } else {
                    // Normal write generation
                    if u.arbitrary::<bool>()? {
                        // AppendSingle
                        let event = AppendEvent::arbitrary_with_buckets_and_segment_size(
                            u,
                            state.total_buckets,
                            state.segment_size,
                        )?;
                        state.track_append(std::slice::from_ref(&event));
                        Ok(Command::AppendSingle(event))
                    } else {
                        // AppendMultiple
                        let count = u.int_in_range(1..=2)?;
                        let mut events = Vec::with_capacity(count);

                        let common_strategy = PartitionKeyStrategy::arbitrary(u)?;
                        let common_stream_id = if matches!(
                            common_strategy,
                            PartitionKeyStrategy::DeterministicFromStream
                        ) {
                            Some(ArbitraryStreamId::arbitrary(u)?)
                        } else {
                            None
                        };

                        for _ in 0..count {
                            let mut event = AppendEvent::arbitrary_with_buckets_and_segment_size(
                                u,
                                state.total_buckets,
                                state.segment_size / count,
                            )?;
                            event.partition_key_strategy = common_strategy.clone();
                            if let Some(ref stream_id) = common_stream_id {
                                event.stream_id = stream_id.clone();
                            }
                            events.push(event);
                        }

                        state.track_append(&events);
                        Ok(Command::AppendMultiple { events })
                    }
                }
            }
            CommandType::Read => {
                // Try data-driven read first
                if let Some(targeted_cmd) = state.generate_targeted_read_command(u)? {
                    return Ok(targeted_cmd);
                }

                // Fallback to random read
                match u.int_in_range(0..=3)? {
                    0 => Ok(Command::ReadEvent {
                        partition_id: if u.arbitrary()? {
                            Some(arbitrary_partition_id(u, state.total_buckets)?)
                        } else {
                            None
                        },
                        event_id: if u.arbitrary()? {
                            Some(Uuid::from_bytes(u.arbitrary()?))
                        } else {
                            None
                        },
                    }),
                    1 => Ok(Command::ReadTransaction {
                        partition_id: if u.arbitrary()? {
                            Some(arbitrary_partition_id(u, state.total_buckets)?)
                        } else {
                            None
                        },
                        first_event_id: if u.arbitrary()? {
                            Some(Uuid::from_bytes(u.arbitrary()?))
                        } else {
                            None
                        },
                    }),
                    2 => Ok(Command::ReadPartition {
                        partition_id: if u.arbitrary()? {
                            Some(arbitrary_partition_id(u, state.total_buckets)?)
                        } else {
                            None
                        },
                        from_sequence: if u.arbitrary()? {
                            Some(arbitrary_sequence(u)?)
                        } else {
                            None
                        },
                    }),
                    3 => Ok(Command::ReadStream {
                        partition_id: if u.arbitrary()? {
                            Some(arbitrary_partition_id(u, state.total_buckets)?)
                        } else {
                            None
                        },
                        stream_id: if u.arbitrary()? {
                            Some(ArbitraryStreamId::arbitrary(u)?.0)
                        } else {
                            None
                        },
                        from_version: if u.arbitrary()? {
                            Some(arbitrary_version(u)?)
                        } else {
                            None
                        },
                        reverse: u.arbitrary()?,
                    }),
                    _ => unreachable!(),
                }
            }
            CommandType::Query => {
                if u.arbitrary::<bool>()? {
                    // GetPartitionSequence with known partition if available
                    let partition_id = if !state.known_partitions.is_empty()
                        && u.arbitrary::<bool>()?
                    {
                        let partitions: Vec<_> = state.known_partitions.iter().copied().collect();
                        Some(partitions[u.arbitrary::<usize>()? % partitions.len()])
                    } else if u.arbitrary()? {
                        Some(arbitrary_partition_id(u, state.total_buckets)?)
                    } else {
                        None
                    };

                    Ok(Command::GetPartitionSequence { partition_id })
                } else {
                    // GetStreamVersion with known stream if available
                    let (partition_id, stream_id) = if !state.known_streams.is_empty()
                        && u.arbitrary::<bool>()?
                    {
                        let partitions: Vec<_> = state.known_partitions.iter().copied().collect();
                        let streams: Vec<_> = state.known_streams.iter().cloned().collect();
                        let partition_id = if !partitions.is_empty() {
                            Some(partitions[u.arbitrary::<usize>()? % partitions.len()])
                        } else {
                            Some(arbitrary_partition_id(u, state.total_buckets)?)
                        };
                        let stream_id =
                            Some(streams[u.arbitrary::<usize>()? % streams.len()].clone());
                        (partition_id, stream_id)
                    } else {
                        (
                            if u.arbitrary()? {
                                Some(arbitrary_partition_id(u, state.total_buckets)?)
                            } else {
                                None
                            },
                            if u.arbitrary()? {
                                Some(ArbitraryStreamId::arbitrary(u)?.0)
                            } else {
                                None
                            },
                        )
                    };

                    Ok(Command::GetStreamVersion {
                        partition_id,
                        stream_id,
                    })
                }
            }
            CommandType::Maintenance => {
                if u.arbitrary::<bool>()? {
                    // SetConfirmations with known partition if available
                    let partition_id = if !state.known_partitions.is_empty()
                        && u.arbitrary::<bool>()?
                    {
                        let partitions: Vec<_> = state.known_partitions.iter().copied().collect();
                        Some(partitions[u.arbitrary::<usize>()? % partitions.len()])
                    } else if u.arbitrary()? {
                        Some(arbitrary_partition_id(u, state.total_buckets)?)
                    } else {
                        None
                    };

                    Ok(Command::SetConfirmations {
                        partition_id,
                        confirmation_count: arbitrary_confirmation_count(u)?,
                    })
                } else {
                    Ok(Command::ReopenDatabase)
                }
            }
        }
    }
}

impl<'a> Arbitrary<'a> for Commands {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let init = InitArgs::arbitrary(u)?;
        let command_count = u.int_in_range(100..=500)?; // More commands to fill segments
        let mut state = CommandGenerationState::new(init.total_buckets, init.segment_size);

        let mut commands = Vec::with_capacity(command_count);

        for _ in 0..command_count {
            let command = Commands::generate_smart_command(u, &mut state)?;
            commands.push(command);
        }

        Ok(Commands { init, commands })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        // InitArgs + up to 50 commands
        let init_hint = InitArgs::size_hint(depth);
        let commands_hint = (0, Some(50 * 200)); // Rough estimate

        (
            init_hint.0 + commands_hint.0,
            init_hint.1.and_then(|i| commands_hint.1.map(|c| i + c)),
        )
    }
}

fn create_transaction_from_events(
    events: &[AppendEvent],
    partition_key: Uuid,
) -> Result<Transaction, sierradb::error::EventValidationError> {
    if events.is_empty() {
        return Err(sierradb::error::EventValidationError::EmptyTransaction);
    }

    let partition_id = events[0].partition_id as u16;
    let partition_hash = uuid_to_partition_hash(partition_key);

    let new_events: smallvec::SmallVec<[NewEvent; 4]> = events
        .iter()
        .map(|event| {
            // Ensure timestamp is valid (< 2^63)
            let timestamp = if event.timestamp >= (1u64 << 63) {
                1u64 << 62 // Use a safe timestamp if the generated one is too large
            } else {
                event.timestamp
            };

            NewEvent {
                event_id: uuid_v7_with_partition_hash(partition_hash),
                stream_id: event.stream_id.0.clone(),
                stream_version: event.expected_version,
                event_name: event.event_name.clone(),
                timestamp,
                metadata: event.metadata.clone(),
                payload: event.payload.clone(),
            }
        })
        .collect();

    Transaction::new(partition_key, partition_id, new_events)
}

fn run(commands: Commands) -> libfuzzer_sys::Corpus {
    // Check if debug mode is enabled
    let debug_mode = std::env::var("FUZZ_DEBUG").unwrap_or_default() == "1";

    if debug_mode {
        eprintln!("=== FUZZ INPUT DEBUG ===");
        eprintln!("Init: {:?}", commands.init);
        eprintln!("Commands ({}):", commands.commands.len());
        for (i, cmd) in commands.commands.iter().enumerate() {
            eprintln!("  {}: {:?}", i + 1, cmd);
        }
        eprintln!("=========================");
    }

    // Validate input parameters
    if commands.commands.is_empty() {
        eprintln!("Warning: Empty command list provided to fuzzer");
        return libfuzzer_sys::Corpus::Reject;
    }

    if commands.init.total_buckets == 0 {
        eprintln!(
            "Error: Invalid total_buckets configuration: {}",
            commands.init.total_buckets
        );
        return libfuzzer_sys::Corpus::Reject;
    }

    if commands.init.writer_threads == 0 {
        eprintln!(
            "Error: Invalid writer_threads configuration: {}",
            commands.init.writer_threads
        );
        return libfuzzer_sys::Corpus::Reject;
    }

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|err| format!("Failed to create tokio runtime: {err}"))
            .unwrap();

        runtime.block_on(async {
            // commands.print_commands_summary();

            let mut state = TestState::new(commands.init).await;
            let total_commands = commands.commands.len();

            for (i, command) in commands.commands.into_iter().enumerate() {
                match execute_command(&mut state, command).await {
                    Ok(_) => {
                        // Validate model consistency periodically
                        if i.is_multiple_of(10) {
                            for &partition_id in state.model.partitions.keys() {
                                if let Err(err) = state
                                    .model
                                    .validate_partition_sequence_ordering(partition_id)
                                {
                                    eprintln!(
                                        "=== FATAL MODEL CONSISTENCY ERROR (PERIODIC CHECK) ==="
                                    );
                                    eprintln!(
                                        "Model consistency error at command {}/{}: {}",
                                        i + 1,
                                        total_commands,
                                        err
                                    );
                                    eprintln!(
                                        "Partition {}: {:?}",
                                        partition_id,
                                        state.model.partitions.get(&partition_id)
                                    );
                                    panic!("Periodic model consistency check failed: {err}");
                                }
                            }
                        }
                    }
                    Err(err) => {
                        panic!("Fatal error: {err}");
                    }
                }
            }

            // Final consistency check
            for &partition_id in state.model.partitions.keys() {
                if let Err(err) = state
                    .model
                    .validate_partition_sequence_ordering(partition_id)
                {
                    eprintln!("=== FATAL MODEL CONSISTENCY ERROR ===");
                    eprintln!("Partition sequence ordering violation: {err}");
                    eprintln!(
                        "Partition {}: {:?}",
                        partition_id,
                        state.model.partitions.get(&partition_id)
                    );
                    panic!("Final model consistency check failed: {err}");
                }
            }

            libfuzzer_sys::Corpus::Keep
        })
    }));

    match result {
        Ok(corpus) => corpus,
        Err(panic_info) => {
            // Panic occurred - this might indicate a bug
            if let Some(s) = panic_info.downcast_ref::<String>() {
                eprintln!("Panic occurred during fuzzing: {s}");
            } else if let Some(s) = panic_info.downcast_ref::<&str>() {
                eprintln!("Panic occurred during fuzzing: {s}");
            } else {
                eprintln!("Unknown panic occurred during fuzzing");
            }
            libfuzzer_sys::Corpus::Keep
        }
    }
}

async fn execute_command(
    state: &mut TestState,
    command: Command,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Command::AppendSingle(event) => execute_append_events(state, vec![event]).await,
        Command::AppendMultiple { events } => {
            if !events.is_empty() {
                execute_append_events(state, events).await
            } else {
                Ok(())
            }
        }
        Command::SetConfirmations {
            partition_id,
            confirmation_count,
        } => execute_set_confirmations(state, partition_id, confirmation_count).await,
        Command::ReadEvent {
            partition_id,
            event_id,
        } => execute_read_event(state, partition_id, event_id).await,
        Command::ReadTransaction {
            partition_id,
            first_event_id,
        } => execute_read_transaction(state, partition_id, first_event_id).await,
        Command::ReadPartition {
            partition_id,
            from_sequence,
        } => execute_read_partition(state, partition_id, from_sequence).await,
        Command::GetPartitionSequence { partition_id } => {
            execute_get_partition_sequence(state, partition_id).await
        }
        Command::ReadStream {
            partition_id,
            stream_id,
            from_version,
            reverse,
        } => execute_read_stream(state, partition_id, stream_id, from_version, reverse).await,
        Command::GetStreamVersion {
            partition_id,
            stream_id,
        } => execute_get_stream_version(state, partition_id, stream_id).await,
        Command::ReopenDatabase => {
            state.reopen_database().await;
            Ok(())
        }
    }
}

fn calculate_partition_key(events: &[AppendEvent]) -> Result<Uuid, Box<dyn std::error::Error>> {
    if events.is_empty() {
        return Err("Cannot determine partition key from empty events".into());
    }

    // All events in a transaction must use the same partition key strategy
    // Take the strategy from the first event and validate others match
    let primary_strategy = &events[0].partition_key_strategy;

    for (i, event) in events.iter().enumerate().skip(1) {
        match (primary_strategy, &event.partition_key_strategy) {
            (
                PartitionKeyStrategy::DeterministicFromStream,
                PartitionKeyStrategy::DeterministicFromStream,
            ) => {
                // Both deterministic - check they use the same stream_id
                if events[0].stream_id.0 != event.stream_id.0 {
                    return Err(format!(
                        "Deterministic partition key strategy requires all events to have the same stream_id: event 0 has '{}', event {i} has '{}'",
                        events[0].stream_id.0, event.stream_id.0
                    ).into());
                }
            }
            (PartitionKeyStrategy::Arbitrary(uuid1), PartitionKeyStrategy::Arbitrary(uuid2)) => {
                // Both arbitrary - they must be the same UUID
                if uuid1 != uuid2 {
                    return Err(format!(
                        "Arbitrary partition key strategy requires all events to have the same UUID: event 0 has {uuid1}, event {i} has {uuid2}"
                    ).into());
                }
            }
            _ => {
                return Err(format!(
                    "Mixed partition key strategies in transaction: event 0 uses {primary_strategy:?}, event {i} uses {:?}",
                    event.partition_key_strategy
                ).into());
            }
        }
    }

    // Calculate the partition key based on the primary strategy
    match primary_strategy {
        PartitionKeyStrategy::DeterministicFromStream => Ok(Uuid::new_v5(
            &NAMESPACE_PARTITION_KEY,
            events[0].stream_id.0.as_bytes(),
        )),
        PartitionKeyStrategy::Arbitrary(uuid) => Ok(*uuid),
    }
}

async fn execute_append_events(
    state: &mut TestState,
    events: Vec<AppendEvent>,
) -> Result<(), Box<dyn std::error::Error>> {
    let partition_key = match calculate_partition_key(&events) {
        Ok(key) => key,
        Err(_) => {
            // If we can't determine a valid partition key, skip this append
            return Ok(());
        }
    };
    let transaction = match create_transaction_from_events(&events, partition_key) {
        Ok(tx) => tx,
        Err(sierradb::error::EventValidationError::EmptyTransaction) => {
            // Empty transaction is a valid scenario for fuzzing
            return Ok(());
        }
        Err(sierradb::error::EventValidationError::InvalidEventId) => {
            // Invalid event ID is a valid scenario for fuzzing
            return Ok(());
        }
        Err(sierradb::error::EventValidationError::PartitionKeyMismatch { .. }) => {
            // Partition key mismatch is a valid scenario for fuzzing
            return Ok(());
        }
    };

    match state.database.append_events(transaction.clone()).await {
        Ok(result) => {
            // Update model with successful append
            state.model.append_events(&transaction, &result);

            // Validate the result matches our expectations
            validate_append_result(&state.model, &transaction, &result)?;
            Ok(())
        }
        Err(WriteError::WrongExpectedSequence { .. })
        | Err(WriteError::WrongExpectedVersion { .. }) => {
            // Version conflicts are expected with optimistic concurrency
            Ok(())
        }
        Err(WriteError::EventsExceedSegmentSize) => {
            // Events exceed segment size - this is a valid scenario for fuzzing
            // Don't update the model since the append failed
            Ok(())
        }
        Err(err) => Err(Box::new(err)),
    }
}

fn validate_append_result(
    model: &DatabaseModel,
    transaction: &Transaction,
    result: &AppendResult,
) -> Result<(), Box<dyn std::error::Error>> {
    let partition_id = transaction.partition_id() as u64;

    // Validate bucket assignment for the partition
    let expected_bucket_id = model.get_bucket_id(partition_id);
    if !model.validate_bucket_assignment(partition_id, expected_bucket_id) {
        return Err(format!(
            "Invalid bucket assignment for append: partition={partition_id}, expected_bucket={expected_bucket_id}"
        )
        .into());
    }

    // Validate that stream versions are correctly incremented
    for (stream_id, &version) in &result.stream_versions {
        let model_version = model.get_stream_version(partition_id, stream_id);
        if let Some(model_v) = model_version
            && model_v != version
        {
            return Err(format!(
                "Stream version mismatch for stream '{stream_id}' in partition {partition_id}: model={model_v}, result={version}"
            )
            .into());
        }

        // Note: Stream versions start from 0, so 0 is a valid version
    }

    // Validate partition sequence range
    if result.first_partition_sequence > result.last_partition_sequence {
        return Err(format!(
            "Invalid partition sequence range: first={} > last={}",
            result.first_partition_sequence, result.last_partition_sequence
        )
        .into());
    }

    // Note: Partition sequences start from 0, so first_partition_sequence of 0 is
    // valid

    let expected_events = transaction.events().len() as u64;
    let actual_range = result.last_partition_sequence - result.first_partition_sequence + 1;
    if actual_range != expected_events {
        return Err(format!(
            "Partition sequence range mismatch: expected {} events, got range {} (first={}, last={})", 
            expected_events, actual_range, result.first_partition_sequence, result.last_partition_sequence
        ).into());
    }

    // Validate that all events in the transaction have the same partition
    for event in transaction.events() {
        if event.stream_id.is_empty() {
            return Err("Event has empty stream_id".into());
        }
        if event.event_name.is_empty() {
            return Err("Event has empty event_name".into());
        }
    }

    Ok(())
}

async fn execute_set_confirmations(
    state: &mut TestState,
    partition_id: Option<u64>,
    confirmation_count: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let partition_id = match partition_id {
        Some(id) => id,
        None => {
            // Pick a random existing partition from the model
            if let Some(&id) = state.model.partitions.keys().next() {
                id
            } else {
                return Ok(()); // No partitions exist yet
            }
        }
    };

    // Validate bucket assignment for this partition
    let expected_bucket_id = state.model.get_bucket_id(partition_id);
    if !state
        .model
        .validate_bucket_assignment(partition_id, expected_bucket_id)
    {
        return Err(format!(
            "Invalid bucket assignment for confirmations: partition={partition_id}, expected_bucket={expected_bucket_id}"
        )
        .into());
    }

    // For fuzzing, we'll create dummy offsets
    let offsets = smallvec::SmallVec::from([0u64, 1, 2, 3]);
    let transaction_id = Uuid::new_v4();

    // Validate confirmation count is reasonable
    if confirmation_count > 10 {
        return Err(
            format!("Confirmation count too high: {confirmation_count} (should be <= 10)").into(),
        );
    }

    match state
        .database
        .set_confirmations(
            partition_id as u16,
            offsets.clone(),
            transaction_id,
            confirmation_count,
        )
        .await
    {
        Ok(_) => {
            state
                .model
                .set_confirmations(partition_id, offsets.to_vec(), confirmation_count);

            // Validate that the confirmation was properly stored in our model
            if let Some(confirmations) = state.model.confirmations.get(&partition_id)
                && let Some(&stored_count) = confirmations.get(&offsets.to_vec())
                && stored_count != confirmation_count
            {
                return Err(format!(
                            "Confirmation count not properly stored: expected={confirmation_count}, stored={stored_count}"
                        )
                        .into());
            }

            Ok(())
        }
        Err(err) => Err(Box::new(err)),
    }
}

async fn execute_read_event(
    state: &mut TestState,
    partition_id: Option<u64>,
    event_id: Option<Uuid>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (partition_id, event_id) = match (partition_id, event_id) {
        (Some(pid), Some(eid)) => (pid, eid),
        (partition_id, event_id) => {
            // Try to get existing data from model
            let pid = partition_id
                .or_else(|| state.model.partitions.keys().next().copied())
                .unwrap_or(0);
            let eid = event_id
                .or_else(|| {
                    state
                        .model
                        .partitions
                        .get(&pid)
                        .and_then(|events| events.values().next())
                        .map(|event| event.event_id)
                })
                .unwrap_or_else(Uuid::new_v4);
            (pid, eid)
        }
    };

    let db_result = state
        .database
        .read_event(partition_id as u16, event_id)
        .await?;
    let model_event = state.model.get_event(partition_id, event_id);

    match (db_result, model_event) {
        (Some(db_event), Some(model_event)) => {
            // Validate the event matches our model
            validate_event_record(state.init_args.total_buckets, &db_event, model_event)?;
        }
        (None, None) => {
            // Both agree the event doesn't exist - good
        }
        (Some(_), None) => {
            return Err("Database returned event that doesn't exist in model".into());
        }
        (None, Some(_)) => {
            return Err("Database didn't return event that exists in model".into());
        }
    }

    Ok(())
}

fn validate_event_record(
    total_buckets: u16,
    db_event: &EventRecord,
    model_event: &ModelEvent,
) -> Result<(), Box<dyn std::error::Error>> {
    // Validate basic event identity
    if db_event.event_id != model_event.event_id {
        return Err(format!(
            "Event ID mismatch: db={}, model={}",
            db_event.event_id, model_event.event_id
        )
        .into());
    }

    // Validate partition information
    if db_event.partition_id as u64 != model_event.partition_id {
        return Err(format!(
            "Partition ID mismatch: db={}, model={}",
            db_event.partition_id, model_event.partition_id
        )
        .into());
    }
    if db_event.partition_key != model_event.partition_key {
        return Err(format!(
            "Partition key mismatch: db={}, model={}",
            db_event.partition_key, model_event.partition_key
        )
        .into());
    }
    if db_event.partition_sequence != model_event.partition_sequence {
        return Err(format!(
            "Partition sequence mismatch: db={}, model={}",
            db_event.partition_sequence, model_event.partition_sequence
        )
        .into());
    }

    // Validate stream information
    if db_event.stream_version != model_event.stream_version {
        return Err(format!(
            "Stream version mismatch: db={}, model={}",
            db_event.stream_version, model_event.stream_version
        )
        .into());
    }
    if db_event.stream_id != model_event.stream_id {
        return Err(format!(
            "Stream ID mismatch: db={}, model={}",
            db_event.stream_id, model_event.stream_id
        )
        .into());
    }

    // Validate transaction information
    if db_event.transaction_id != model_event.transaction_id {
        return Err(format!(
            "Transaction ID mismatch: db={}, model={}",
            db_event.transaction_id, model_event.transaction_id
        )
        .into());
    }

    // Validate event metadata
    if db_event.timestamp != model_event.timestamp {
        return Err(format!(
            "Timestamp mismatch: db={}, model={}",
            db_event.timestamp, model_event.timestamp
        )
        .into());
    }

    if db_event.event_name != model_event.event_name {
        return Err(format!(
            "Event name mismatch: db='{}', model='{}'",
            db_event.event_name, model_event.event_name
        )
        .into());
    }
    if db_event.metadata != model_event.metadata {
        return Err(format!(
            "Metadata mismatch: db={:?}, model={:?}",
            db_event.metadata, model_event.metadata
        )
        .into());
    }
    if db_event.payload != model_event.payload {
        return Err(format!(
            "Payload mismatch: db={:?}, model={:?}",
            db_event.payload, model_event.payload
        )
        .into());
    }

    // Validate reasonable field values (sequences and versions start from 0)
    // Note: Both partition_sequence and stream_version are valid starting from 0
    if model_event.event_name.is_empty() {
        return Err("Event name cannot be empty".into());
    }
    if model_event.stream_id.is_empty() {
        return Err("Stream ID cannot be empty".into());
    }

    // Validate bucket assignment (assuming dynamic bucket calculation)
    let expected_bucket_id = (model_event.partition_id % total_buckets as u64) as BucketId; // Using 64 as fallback
    if model_event.bucket_id != expected_bucket_id {
        return Err(format!(
            "Bucket ID assignment incorrect: stored={}, expected={} (partition={})",
            model_event.bucket_id, expected_bucket_id, model_event.partition_id
        )
        .into());
    }

    Ok(())
}

async fn execute_read_transaction(
    state: &mut TestState,
    partition_id: Option<u64>,
    first_event_id: Option<Uuid>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (partition_id, first_event_id) = match (partition_id, first_event_id) {
        (Some(pid), Some(eid)) => (pid, eid),
        (partition_id, first_event_id) => {
            let pid = partition_id
                .or_else(|| state.model.partitions.keys().next().copied())
                .unwrap_or(0);
            let eid = first_event_id
                .or_else(|| {
                    state
                        .model
                        .partitions
                        .get(&pid)
                        .and_then(|events| events.values().next())
                        .map(|event| event.event_id)
                })
                .unwrap_or_else(Uuid::new_v4);
            (pid, eid)
        }
    };

    let db_result = state
        .database
        .read_transaction(partition_id as u16, first_event_id)
        .await?;

    // Find the corresponding transaction in our model
    let model_first_event = state.model.get_event(partition_id, first_event_id);

    match (db_result, model_first_event) {
        (Some(db_transaction), Some(model_event)) => {
            // Get the full transaction from our model using the transaction ID
            let model_transaction = state
                .model
                .get_transaction_events(model_event.transaction_id);

            if let Some(model_tx_events) = model_transaction {
                let db_events: Vec<_> = db_transaction.into_iter().collect();

                // Validate transaction event count
                if db_events.len() != model_tx_events.len() {
                    return Err(format!(
                        "Transaction event count mismatch: db={}, model={}",
                        db_events.len(),
                        model_tx_events.len()
                    )
                    .into());
                }

                // Validate each event in the transaction
                for (db_event, model_event) in db_events.iter().zip(model_tx_events.iter()) {
                    if db_event.transaction_id != model_event.transaction_id {
                        return Err(format!(
                            "Transaction ID mismatch in transaction: db={}, model={}",
                            db_event.transaction_id, model_event.transaction_id
                        )
                        .into());
                    }
                    validate_event_record(state.init_args.total_buckets, db_event, model_event)?;
                }

                // Validate that all events belong to the same transaction
                let transaction_id = model_event.transaction_id;
                for event in &db_events {
                    if event.transaction_id != transaction_id {
                        return Err(format!(
                            "Mixed transaction IDs in transaction read: expected={}, got={}",
                            transaction_id, event.transaction_id
                        )
                        .into());
                    }
                }
            }
        }
        (None, None) => {
            // Both agree the transaction doesn't exist - good
        }
        (Some(_), None) => {
            return Err("Database returned transaction that doesn't exist in model".into());
        }
        (None, Some(_)) => {
            return Err("Database didn't return transaction that exists in model".into());
        }
    }

    Ok(())
}

async fn execute_read_partition(
    state: &mut TestState,
    partition_id: Option<u64>,
    from_sequence: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let partition_id = partition_id
        .or_else(|| state.model.partitions.keys().next().copied())
        .unwrap_or(0);
    let from_sequence = from_sequence.unwrap_or(0);

    // First validate our model's internal consistency
    state
        .model
        .validate_partition_sequence_ordering(partition_id)?;

    let mut db_iter = state
        .database
        .read_partition(partition_id as u16, from_sequence)
        .await?;
    let model_events = state
        .model
        .get_partition_events_from(partition_id, from_sequence);

    let mut db_events = Vec::new();
    let mut all_db_event_records = Vec::new();

    while let Some(committed_events) = db_iter.next().await? {
        // Collect individual events for comparison
        let events_vec: Vec<EventRecord> = committed_events.clone().into_iter().collect();
        all_db_event_records.extend(events_vec);
        db_events.push(committed_events);
    }

    // Validate ordering of committed events
    for i in 1..db_events.len() {
        if let (Some(prev_seq), Some(curr_seq)) = (
            db_events[i - 1].first_partition_sequence(),
            db_events[i].first_partition_sequence(),
        ) && prev_seq >= curr_seq
        {
            return Err(
                format!("Partition events not properly ordered: {prev_seq} >= {curr_seq}").into(),
            );
        }
    }

    // Compare event counts when we have model data
    if !model_events.is_empty() && !all_db_event_records.is_empty() {
        // Check that we have reasonable event counts (model might have fewer due to
        // confirmations)
        if all_db_event_records.len() < model_events.len() {
            return Err(format!(
                "Database returned fewer events than model: db={}, model={}",
                all_db_event_records.len(),
                model_events.len()
            )
            .into());
        }

        // Validate that database events match model events where they exist
        for model_event in &model_events {
            let matching_db_event = all_db_event_records
                .iter()
                .find(|db_event| db_event.event_id == model_event.event_id);

            if let Some(db_event) = matching_db_event {
                validate_event_record(state.init_args.total_buckets, db_event, model_event)?;
            }
        }

        // Validate sequence ranges
        if let (Some(first_model), Some(last_model)) = (model_events.first(), model_events.last()) {
            let model_seq_range = (
                first_model.partition_sequence,
                last_model.partition_sequence,
            );

            if let (Some(first_db), Some(last_db)) =
                (all_db_event_records.first(), all_db_event_records.last())
            {
                let db_seq_range = (first_db.partition_sequence, last_db.partition_sequence);

                // Model sequence range should be within or equal to DB range
                if model_seq_range.0 < from_sequence {
                    return Err(format!(
                        "Model returned events before from_sequence: model_first={}, from_sequence={}", 
                        model_seq_range.0, from_sequence
                    ).into());
                }

                if model_seq_range.1 > db_seq_range.1 {
                    return Err(format!(
                        "Model has events beyond database range: model_last={}, db_last={}",
                        model_seq_range.1, db_seq_range.1
                    )
                    .into());
                }
            }
        }
    }

    // Validate bucket assignment for all events
    for event in &all_db_event_records {
        let expected_bucket_id = state.model.get_bucket_id(event.partition_id as u64);
        // Note: We can't directly validate bucket assignment from EventRecord
        // as it doesn't contain bucket_id, but we validate partition_id is correct
        if event.partition_id as u64 != partition_id {
            return Err(format!(
                "Event partition_id mismatch: expected={}, got={}",
                partition_id, event.partition_id
            )
            .into());
        }

        // Ensure the bucket assignment is mathematically correct
        if !state
            .model
            .validate_bucket_assignment(event.partition_id as u64, expected_bucket_id)
        {
            return Err(format!(
                "Invalid bucket assignment for partition {}: expected bucket {}",
                event.partition_id, expected_bucket_id
            )
            .into());
        }
    }

    Ok(())
}

async fn execute_get_partition_sequence(
    state: &mut TestState,
    partition_id: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let partition_id = partition_id
        .or_else(|| state.model.partitions.keys().next().copied())
        .unwrap_or(0);

    // Validate bucket assignment for this partition
    let expected_bucket_id = state.model.get_bucket_id(partition_id);
    if !state
        .model
        .validate_bucket_assignment(partition_id, expected_bucket_id)
    {
        return Err(format!(
            "Invalid bucket assignment for partition {partition_id}: expected bucket {expected_bucket_id}"
        )
        .into());
    }

    let db_result = state
        .database
        .get_partition_sequence(partition_id as u16)
        .await?;
    let model_sequence = state.model.get_partition_sequence(partition_id);

    match (db_result, model_sequence) {
        (
            Some(PartitionLatestSequence::LatestSequence {
                sequence: db_seq, ..
            }),
            Some(model_seq),
        ) => {
            if db_seq != model_seq {
                return Err(format!(
                    "Partition sequence mismatch for partition {partition_id}: db={db_seq}, model={model_seq}"
                )
                .into());
            }

            // Validate the sequence is within reasonable bounds
            // Note: Partition sequences start from 0, so 0 is valid
        }
        (Some(PartitionLatestSequence::ExternalBucket { .. }), _) => {
            // External bucket case - this indicates the partition is in a
            // different bucket For fuzzing purposes, we can accept
            // this as a valid state
        }
        (None, None) => {
            // Both agree no sequence exists - validate consistency
            if state.model.partitions.contains_key(&partition_id) {
                return Err(format!(
                    "Model has partition {partition_id} but no sequence, database also has no sequence"
                )
                .into());
            }
        }
        (
            Some(PartitionLatestSequence::LatestSequence {
                sequence: db_seq, ..
            }),
            None,
        ) => {
            return Err(format!(
                "Database returned sequence {db_seq} for partition {partition_id} that doesn't exist in model"
            )
            .into());
        }
        (None, Some(model_seq)) => {
            return Err(format!(
                "Model has sequence {model_seq} for partition {partition_id} but database returned none"
            )
            .into());
        }
    }

    Ok(())
}

async fn execute_read_stream(
    state: &mut TestState,
    partition_id: Option<u64>,
    stream_id: Option<StreamId>,
    from_version: Option<u64>,
    reverse: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let partition_id = partition_id
        .or_else(|| state.model.partitions.keys().next().copied())
        .unwrap_or(0);
    let stream_id = stream_id
        .or_else(|| {
            state
                .model
                .streams
                .keys()
                .find(|(pid, _)| *pid == partition_id)
                .map(|(_, sid)| sid.clone())
        })
        .unwrap_or_else(|| StreamId::new("test-stream").unwrap());
    let from_version = from_version.unwrap_or(0);

    let mut db_iter = state
        .database
        .read_stream(
            partition_id as u16,
            stream_id.clone(),
            from_version,
            reverse,
        )
        .await?;
    let model_events = state
        .model
        .get_stream_events_from(partition_id, &stream_id, from_version);

    let mut db_events = Vec::new();
    let mut all_db_event_records = Vec::new();

    while let Some(committed_events) = db_iter.next().await? {
        let events_vec: Vec<EventRecord> = committed_events.clone().into_iter().collect();
        all_db_event_records.extend(events_vec);
        db_events.push(committed_events);
    }

    // Validate stream consistency
    for event in &all_db_event_records {
        if event.stream_id != stream_id {
            return Err(format!(
                "Stream iterator returned event from wrong stream: expected='{}', got='{}'",
                stream_id, event.stream_id
            )
            .into());
        }
        if event.stream_version < from_version {
            return Err(format!(
                "Stream iterator returned event below requested version: event_version={}, from_version={}", 
                event.stream_version, from_version
            ).into());
        }
        if event.partition_id as u64 != partition_id {
            return Err(format!(
                "Stream iterator returned event from wrong partition: expected={}, got={}",
                partition_id, event.partition_id
            )
            .into());
        }
    }

    // Validate version ordering (accounting for reverse flag)
    if all_db_event_records.len() > 1 {
        for i in 1..all_db_event_records.len() {
            let prev_version = all_db_event_records[i - 1].stream_version;
            let curr_version = all_db_event_records[i].stream_version;

            if reverse {
                if prev_version < curr_version {
                    return Err(format!(
                        "Stream events not properly ordered (reverse): {prev_version} < {curr_version}"
                    )
                    .into());
                }
            } else if prev_version > curr_version {
                return Err(format!(
                    "Stream events not properly ordered (forward): {prev_version} > {curr_version}"
                )
                .into());
            }
        }
    }

    // Compare with model when we have data
    if !model_events.is_empty() && !all_db_event_records.is_empty() {
        // Verify that all model events are present in database results
        for model_event in &model_events {
            let matching_db_event = all_db_event_records
                .iter()
                .find(|db_event| db_event.event_id == model_event.event_id);

            if let Some(db_event) = matching_db_event {
                validate_event_record(state.init_args.total_buckets, db_event, model_event)?;

                // Additional stream-specific validation
                if db_event.stream_id != model_event.stream_id {
                    return Err(format!(
                        "Stream ID mismatch in stream read: db='{}', model='{}'",
                        db_event.stream_id, model_event.stream_id
                    )
                    .into());
                }
                if db_event.stream_version != model_event.stream_version {
                    return Err(format!(
                        "Stream version mismatch in stream read: db={}, model={}",
                        db_event.stream_version, model_event.stream_version
                    )
                    .into());
                }
            } else if model_event.stream_version >= from_version {
                // Only error if the model event should have been included
                return Err(format!(
                    "Model event missing from database stream read: event_id={}, stream_version={}",
                    model_event.event_id, model_event.stream_version
                )
                .into());
            }
        }

        // Validate stream version consistency with model
        let model_max_version = model_events
            .iter()
            .map(|e| e.stream_version)
            .max()
            .unwrap_or(0);

        let db_max_version = all_db_event_records
            .iter()
            .map(|e| e.stream_version)
            .max()
            .unwrap_or(0);

        if model_max_version > db_max_version {
            return Err(format!(
                "Model has higher stream version than database: model={model_max_version}, db={db_max_version}"
            )
            .into());
        }
    }

    // Validate bucket assignment for all events in the stream
    for event in &all_db_event_records {
        let expected_bucket_id = state.model.get_bucket_id(event.partition_id as u64);
        if !state
            .model
            .validate_bucket_assignment(event.partition_id as u64, expected_bucket_id)
        {
            return Err(format!(
                "Invalid bucket assignment for stream event: partition={}, expected_bucket={}",
                event.partition_id, expected_bucket_id
            )
            .into());
        }
    }

    Ok(())
}

async fn execute_get_stream_version(
    state: &mut TestState,
    partition_id: Option<u64>,
    stream_id: Option<StreamId>,
) -> Result<(), Box<dyn std::error::Error>> {
    let partition_id = partition_id
        .or_else(|| state.model.partitions.keys().next().copied())
        .unwrap_or(0);
    let stream_id = stream_id
        .or_else(|| {
            state
                .model
                .streams
                .keys()
                .find(|(pid, _)| *pid == partition_id)
                .map(|(_, sid)| sid.clone())
        })
        .unwrap_or_else(|| StreamId::new("test-stream").unwrap());

    // Validate bucket assignment for this partition
    let expected_bucket_id = state.model.get_bucket_id(partition_id);
    if !state
        .model
        .validate_bucket_assignment(partition_id, expected_bucket_id)
    {
        return Err(format!(
            "Invalid bucket assignment for partition {partition_id}: expected bucket {expected_bucket_id}"
        )
        .into());
    }

    let db_result = state
        .database
        .get_stream_version(partition_id as u16, &stream_id)
        .await?;
    let model_version = state.model.get_stream_version(partition_id, &stream_id);

    match (db_result, model_version) {
        (
            Some(StreamLatestVersion::LatestVersion {
                version: db_ver, ..
            }),
            Some(model_ver),
        ) => {
            if db_ver != model_ver {
                return Err(format!(
                    "Stream version mismatch for stream '{stream_id}' in partition {partition_id}: db={db_ver}, model={model_ver}"
                )
                .into());
            }

            // Validate the version is within reasonable bounds
            // Note: Stream versions start from 0, so 0 is valid
        }
        (Some(StreamLatestVersion::ExternalBucket { .. }), _) => {
            // External bucket case - this indicates the stream is in a
            // different bucket For fuzzing purposes, we can accept
            // this as a valid state
        }
        (None, None) => {
            // Both agree no version exists - this is fine for non-existent
            // streams
        }
        (
            Some(StreamLatestVersion::LatestVersion {
                version: db_ver, ..
            }),
            None,
        ) => {
            return Err(format!(
                "Database returned version {db_ver} for stream '{stream_id}' in partition {partition_id} that doesn't exist in model"
            ).into());
        }
        (None, Some(model_ver)) => {
            return Err(format!(
                "Model has version {model_ver} for stream '{stream_id}' in partition {partition_id} but database returned none"
            )
            .into());
        }
    }

    Ok(())
}
