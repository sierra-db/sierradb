#![no_main]

extern crate arbitrary;

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use arbitrary::{Arbitrary, Unstructured, size_hint};
use libfuzzer_sys::{Corpus, fuzz_target};
use sierradb::StreamId;
use sierradb::bucket::PartitionId;
use sierradb::bucket::segment::EventRecord;
use sierradb::database::{Database, DatabaseBuilder};
use sierradb::database::{ExpectedVersion, NewEvent, Transaction};
use sierradb::error::StreamIdError;
use sierradb::id::{NAMESPACE_PARTITION_KEY, uuid_to_partition_hash, uuid_v7_with_partition_hash};
use smallvec::smallvec;
use std::path::PathBuf;
use tokio::runtime::Runtime;
use uuid::Uuid;

#[derive(Debug)]
struct InitArgs {
    segment_size: usize,
    total_buckets: u16,
    writer_threads: u16,
    reader_threads: u16,
    flush_interval_duration: Duration,
    flush_interval_events: u32,
}

#[derive(Arbitrary)]
enum FlushInterval {
    Immediate,
    Value,
}

impl<'a> Arbitrary<'a> for InitArgs {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let segment_size = u.int_in_range(256..=(64 * 1024 * 1024))?;
        let total_buckets = u.int_in_range(1..=64)?;
        let writer_threads = arbitrary_divisor(u, total_buckets)?;
        let reader_threads = u.int_in_range(1..=4)?;
        let flush_interval_duration = match FlushInterval::arbitrary(u)? {
            FlushInterval::Immediate => Duration::MAX,
            FlushInterval::Value => Duration::from_millis(u.int_in_range(0..=2000)?),
        };
        let flush_interval_events = match FlushInterval::arbitrary(u)? {
            FlushInterval::Immediate => 1,
            FlushInterval::Value => u.int_in_range(2..=20)?,
        };

        Ok(InitArgs {
            segment_size,
            total_buckets,
            writer_threads,
            reader_threads,
            flush_interval_duration,
            flush_interval_events,
        })
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        let flush_duration_hint = size_hint::and(
            FlushInterval::size_hint(depth),
            size_hint::or((0, Some(0)), u64::size_hint(depth)), /* Either no extra bytes
                                                                 * (Immediate) or u64 bytes
                                                                 * (Value) */
        );

        let flush_events_hint = size_hint::and(
            FlushInterval::size_hint(depth),
            size_hint::or((0, Some(0)), u32::size_hint(depth)), /* Either no extra bytes
                                                                 * (Immediate) or u32 bytes
                                                                 * (Value) */
        );

        size_hint::and_all(&[
            usize::size_hint(depth), // segment_size
            u16::size_hint(depth),   // total_buckets
            usize::size_hint(depth), /* writer_threads (for arbitrary_divisor's internal
                                      * arbitrary calls) */
            u16::size_hint(depth), // reader_threads
            flush_duration_hint,
            flush_events_hint,
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
        if total_buckets % i == 0 {
            divisors.push(i);
        }
    }

    // Choose a random divisor
    let index = u.arbitrary::<usize>()? % divisors.len();
    Ok(divisors[index])
}

// Initialize the database and runtime once
fn init(args: InitArgs) -> (Runtime, Database) {
    // First initialize runtime if needed
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Use a unique directory for each fuzzing run to avoid conflicts
    let db_path = PathBuf::from("target/fuzz_db");
    // Remove any existing database files
    let _ = std::fs::remove_dir_all(&db_path);

    // Create the database using the runtime
    let db = runtime.block_on(async {
        DatabaseBuilder::new()
            .segment_size(args.segment_size)
            .total_buckets(args.total_buckets)
            .bucket_ids_from_range(0..args.total_buckets)
            .writer_threads(args.writer_threads)
            .reader_threads(args.reader_threads)
            .flush_interval_duration(args.flush_interval_duration)
            .flush_interval_events(args.flush_interval_events)
            .open(db_path)
            .unwrap()
    });

    (runtime, db)
}

#[derive(Arbitrary, Debug)]
enum Command {
    AppendSingle(AppendEvent),
    AppendMultiple(Vec<AppendEvent>),
    SetExistingConfirmations {
        confirmation_count: u8,
    },
    SetArbitraryConfirmations {
        #[arbitrary(with = arbitrary_partition_id)]
        partition_id: PartitionId,
        confirmation_count: u8,
    },
    ReadExistingEvent,
    ReadArbitraryEvent {
        #[arbitrary(with = arbitrary_partition_id)]
        partition_id: PartitionId,
        event_id: [u8; 16],
        header_only: bool,
    },
    ReadExistingTransaction,
    ReadArbitraryTransaction {
        #[arbitrary(with = arbitrary_partition_id)]
        partition_id: PartitionId,
        first_event_id: [u8; 16],
        header_only: bool,
    },
    ReadPartitionFromStart {
        #[arbitrary(with = arbitrary_partition_id)]
        partition_id: PartitionId,
    },
    ReadPartitionFromSequence {
        #[arbitrary(with = arbitrary_partition_id)]
        partition_id: PartitionId,
        #[arbitrary(with = arbitrary_sequence)]
        sequence: u64,
    },
    GetExistingPartitionSequence,
    GetArbitraryPartitionSequence {
        #[arbitrary(with = arbitrary_partition_id)]
        partition_id: PartitionId,
    },
    ReadStreamFromStart {
        #[arbitrary(with = arbitrary_partition_id)]
        partition_id: PartitionId,
    },
    ReadStreamFromVersion {
        #[arbitrary(with = arbitrary_partition_id)]
        partition_id: PartitionId,
        #[arbitrary(with = arbitrary_version)]
        version: u64,
    },
    GetExistingStreamVersion,
    GetArbitraryStreamVersion {
        #[arbitrary(with = arbitrary_partition_id)]
        partition_id: PartitionId,
        stream_id: ArbitraryStreamId,
    },
}

fn arbitrary_partition_id<'a>(u: &mut Unstructured<'a>) -> arbitrary::Result<PartitionId> {
    u.int_in_range(0..=1050)
}

fn arbitrary_sequence<'a>(u: &mut Unstructured<'a>) -> arbitrary::Result<u64> {
    u.int_in_range(0..=10_000)
}

fn arbitrary_version<'a>(u: &mut Unstructured<'a>) -> arbitrary::Result<u64> {
    u.int_in_range(0..=1_000)
}

#[derive(Debug)]
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

#[derive(Arbitrary, Debug)]
struct AppendEvent {
    #[arbitrary(with = arbitrary_partition_id)]
    partition_id: u16,
    stream_id: ArbitraryStreamId,
    event_name: String,
    timestamp: u64,
    metadata: Vec<u8>,
    payload: Vec<u8>,
}

#[derive(Arbitrary, Debug)]
struct Commands {
    init: InitArgs,
    commands: Vec<Command>,
}

fn run(commands: Commands) -> Corpus {
    let (runtime, db) = init(commands.init);

    let mut model: HashMap<PartitionId, BTreeMap<u64, EventRecord>> = HashMap::new();

    runtime.block_on(async move {
        for command in commands.commands {
            match command {
                Command::AppendSingle(event) => {}
                Command::AppendMultiple(events) => {}
                Command::SetExistingConfirmations { confirmation_count } => todo!(),
                Command::SetArbitraryConfirmations {
                    partition_id,
                    confirmation_count,
                } => todo!(),
                Command::ReadExistingEvent => todo!(),
                Command::ReadArbitraryEvent {
                    partition_id,
                    event_id,
                    header_only,
                } => todo!(),
                Command::ReadExistingTransaction => todo!(),
                Command::ReadArbitraryTransaction {
                    partition_id,
                    first_event_id,
                    header_only,
                } => todo!(),
                Command::ReadPartitionFromStart { partition_id } => todo!(),
                Command::ReadPartitionFromSequence {
                    partition_id,
                    sequence,
                } => todo!(),
                Command::GetExistingPartitionSequence => todo!(),
                Command::GetArbitraryPartitionSequence { partition_id } => todo!(),
                Command::ReadStreamFromStart { partition_id } => todo!(),
                Command::ReadStreamFromVersion {
                    partition_id,
                    version,
                } => todo!(),
                Command::GetExistingStreamVersion => todo!(),
                Command::GetArbitraryStreamVersion {
                    partition_id,
                    stream_id,
                } => todo!(),
            }
        }

        Corpus::Keep
    })
}

fuzz_target!(|commands: Commands| -> Corpus { run(commands) });
