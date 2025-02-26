use std::{collections::HashSet, fmt, io, path::Path, sync::Arc};

use rayon::{
    ThreadPool,
    iter::{IntoParallelIterator, ParallelIterator},
};
use uuid::Uuid;

use super::{
    BucketSegmentId,
    event_index::{ClosedEventIndex, OpenEventIndex},
    flusher::FlushSender,
    reader_thread_pool::ReaderThreadPool,
    segment::{
        AppendEvent, AppendEventBody, AppendEventHeader, BucketSegmentReader, BucketSegmentWriter,
        ClosedBucketSegment, CommittedEvents, EventRecord, OpenBucketSegment, Record,
    },
    stream_index::{ClosedStreamIndex, OpenStreamIndex},
};

pub struct OpenBucketSegmentSet {
    id: BucketSegmentId,
    pub segment: OpenBucketSegment,
    event_index: OpenEventIndex,
    stream_index: OpenStreamIndex,
    committed_event_offsets: HashSet<u64>,
}

impl OpenBucketSegmentSet {
    pub fn new(
        id: BucketSegmentId,
        segment: OpenBucketSegment,
        event_index: OpenEventIndex,
        stream_index: OpenStreamIndex,
    ) -> io::Result<Self> {
        let committed_event_offsets =
            Self::collect_committed_event_offsets(&mut segment.sequential_reader()?)?;

        Ok(OpenBucketSegmentSet {
            id,
            segment,
            event_index,
            stream_index,
            committed_event_offsets,
        })
    }

    pub fn create(
        id: BucketSegmentId,
        dir: impl AsRef<Path>,
        reader_pool: ReaderThreadPool,
        flush_tx: FlushSender,
    ) -> io::Result<OpenBucketSegmentSet> {
        let dir = dir.as_ref();
        let segment_path = dir.join(file_name(id, SegmentKind::Events));
        let event_index_path = dir.join(file_name(id, SegmentKind::EventIndex));
        let stream_index_path = dir.join(file_name(id, SegmentKind::StreamIndex));

        let writer = BucketSegmentWriter::create(&segment_path, id.bucket_id, flush_tx)?;
        let reader = BucketSegmentReader::open(segment_path, writer.flushed_offset())?;
        let segment = OpenBucketSegment::new(id, reader, reader_pool, writer);

        let event_index = OpenEventIndex::create(id, event_index_path)?;
        let stream_index = OpenStreamIndex::create(id, stream_index_path)?;

        Ok(OpenBucketSegmentSet {
            id,
            segment,
            event_index,
            stream_index,
            committed_event_offsets: HashSet::new(),
        })
    }

    pub fn open(
        id: BucketSegmentId,
        dir: impl AsRef<Path>,
        reader_pool: ReaderThreadPool,
        flush_tx: FlushSender,
    ) -> io::Result<OpenBucketSegmentSet> {
        let dir = dir.as_ref();
        let segment_path = dir.join(file_name(id, SegmentKind::Events));
        let event_index_path = dir.join(file_name(id, SegmentKind::EventIndex));
        let stream_index_path = dir.join(file_name(id, SegmentKind::StreamIndex));

        let writer = BucketSegmentWriter::open(&segment_path, flush_tx)?;
        let mut reader = BucketSegmentReader::open(segment_path, writer.flushed_offset())?;
        let committed_event_offsets = Self::collect_committed_event_offsets(&mut reader)?;

        let mut event_index = OpenEventIndex::open(id, event_index_path)?;
        let mut stream_index = OpenStreamIndex::open(id, stream_index_path)?;

        event_index.hydrate(&mut reader)?;
        stream_index.hydrate(&mut reader)?;

        let segment = OpenBucketSegment::new(id, reader, reader_pool, writer);

        Ok(OpenBucketSegmentSet {
            id,
            segment,
            event_index,
            stream_index,
            committed_event_offsets,
        })
    }

    pub fn close(self, pool: &ThreadPool) -> io::Result<ClosedBucketSegmentSet> {
        let committed_event_offsets = Arc::new(self.committed_event_offsets);
        let segment = self.segment.close();
        let event_index = self
            .event_index
            .close(pool, Arc::clone(&committed_event_offsets))?;
        let stream_index = self.stream_index.close(pool, committed_event_offsets)?;

        Ok(ClosedBucketSegmentSet {
            id: self.id,
            segment,
            event_index,
            stream_index,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append_event(
        &mut self,
        event_id: Uuid,
        partition_key: &Uuid,
        transaction_id: &Uuid,
        stream_version: u64,
        timestamp: u64,
        stream_id: impl Into<Arc<str>>,
        event_name: &str,
        metadata: &[u8],
        payload: &[u8],
    ) -> io::Result<(u64, usize)> {
        let stream_id = stream_id.into();
        let body = AppendEventBody::new(&stream_id, event_name, metadata, payload);
        let header = AppendEventHeader::new(
            &event_id,
            partition_key,
            transaction_id,
            stream_version,
            timestamp,
            body,
        )?;
        let (offset, len) = self
            .segment
            .writer()
            .append_event(AppendEvent::new(header, body))?;
        self.event_index.insert(event_id, offset);
        self.stream_index.insert(stream_id, offset);

        Ok((offset, len))
    }

    pub fn append_commit(
        &mut self,
        transaction_id: &Uuid,
        timestamp: u64,
        event_count: u32,
    ) -> io::Result<(u64, usize)> {
        self.segment
            .writer()
            .append_commit(transaction_id, timestamp, event_count)
    }

    pub fn read_event_by_id(&self, event_id: &Uuid) -> io::Result<Option<EventRecord<'static>>> {
        let Some(offset) = self.event_index.get(event_id) else {
            return Ok(None);
        };

        Ok(self
            .segment
            .read_record(offset)?
            .and_then(Record::into_event))
    }

    pub fn read_stream_events(&self, stream_id: &str) -> io::Result<Vec<EventRecord<'static>>> {
        let Some(offsets) = self.stream_index.get(stream_id) else {
            return Ok(vec![]);
        };

        if offsets.len() == 1 {
            let Some(event) = self
                .segment
                .read_record(offsets[0])?
                .and_then(Record::into_event)
            else {
                return Ok(vec![]);
            };
            return Ok(vec![event]);
        }

        let events = self
            .segment
            .reader_pool
            .install(move |with_reader| {
                offsets
                    .into_par_iter()
                    .map(|offset| {
                        with_reader(self.id, |reader| {
                            Ok(reader
                                .unwrap()
                                .read_committed_events(*offset, false)?
                                .map(|events| events.into_owned().into_iter()))
                        })
                    })
                    .collect::<io::Result<Vec<_>>>()
            })?
            .into_iter()
            .flatten()
            .flatten()
            .collect();

        Ok(events)
    }

    fn collect_committed_event_offsets(
        reader: &mut BucketSegmentReader,
    ) -> io::Result<HashSet<u64>> {
        let mut iter = reader.iter();
        let mut committed_transactions = HashSet::new();
        while let Some(events) = iter.next_committed_events()? {
            match events {
                CommittedEvents::None { .. } => {}
                CommittedEvents::Single(event) => {
                    committed_transactions.insert(event.offset);
                }
                CommittedEvents::Transaction { events, .. } => {
                    for event in events {
                        committed_transactions.insert(event.offset);
                    }
                }
            }
        }

        Ok(committed_transactions)
    }
}

pub struct ClosedBucketSegmentSet {
    pub id: BucketSegmentId,
    pub segment: ClosedBucketSegment,
    pub event_index: ClosedEventIndex,
    pub stream_index: ClosedStreamIndex,
}

impl ClosedBucketSegmentSet {
    pub fn new(
        id: BucketSegmentId,
        segment: ClosedBucketSegment,
        event_index: ClosedEventIndex,
        stream_index: ClosedStreamIndex,
    ) -> Self {
        ClosedBucketSegmentSet {
            id,
            segment,
            event_index,
            stream_index,
        }
    }
}
