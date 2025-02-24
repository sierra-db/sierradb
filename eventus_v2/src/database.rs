use std::{io, path::PathBuf, sync::Arc, time::Duration};

use tokio::sync::oneshot;
use tracing::error;
use uuid::Uuid;

use crate::{
    bucket::{
        reader_thread_pool::ReaderThreadPool,
        segment::{CommittedEvents, EventRecord},
        stream_index::EventStreamIter,
        writer_thread_pool::{AppendEventsBatch, WriterThreadPool},
    },
    id::{extract_stream_hash, stream_id_hash},
};

pub struct Database {
    reader_pool: ReaderThreadPool,
    writer_pool: WriterThreadPool,
    num_buckets: u16,
}

impl Database {
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<Self> {
        DatabaseBuilder::new(dir).open()
    }

    pub async fn append_events(&self, events: AppendEventsBatch) -> io::Result<()> {
        self.writer_pool
            .append_events(events)
            .await
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "writer thread panicked"))?
    }

    pub async fn read_event(&self, event_id: Uuid) -> io::Result<Option<EventRecord<'static>>> {
        let bucket_id = extract_stream_hash(event_id) % self.num_buckets;
        let segment_id_offset = self
            .writer_pool
            .with_event_index(bucket_id, |segment_id, event_index| {
                event_index
                    .get(&event_id)
                    .map(|offset| (segment_id, offset))
            })
            .await
            .flatten();

        let (reply_tx, reply_rx) = oneshot::channel();
        self.reader_pool.spawn(move |with_readers| {
            with_readers(move |readers| match segment_id_offset {
                Some((segment_id, offset)) => {
                    let Some(reader_set) = readers
                        .get_mut(&bucket_id)
                        .and_then(|segments| segments.get_mut(&segment_id))
                    else {
                        let _ = reply_tx.send(Ok(None));
                        return;
                    };

                    let res = reader_set
                        .reader
                        .read_committed_events(offset, false)
                        .map(|events| events.map(CommittedEvents::into_owned));
                    let _ = reply_tx.send(res);
                }
                None => {
                    let Some(segments) = readers.get_mut(&bucket_id) else {
                        let _ = reply_tx.send(Ok(None));
                        return;
                    };

                    for reader_set in segments.values_mut().rev() {
                        if let Some(event_index) = &mut reader_set.event_index {
                            match event_index.get(&event_id) {
                                Ok(Some(offset)) => {
                                    let res = reader_set
                                        .reader
                                        .read_committed_events(offset, false)
                                        .map(|events| events.map(CommittedEvents::into_owned));
                                    let _ = reply_tx.send(res);
                                    return;
                                }
                                Ok(None) => {}
                                Err(err) => {
                                    let _ = reply_tx.send(Err(err));
                                    return;
                                }
                            }
                        }
                    }

                    let _ = reply_tx.send(Ok(None));
                }
            })
        });

        match reply_rx.await {
            Ok(Ok(Some(events))) => Ok(events.into_iter().next()),
            Ok(Ok(None)) => Ok(None),
            Ok(Err(err)) => Err(err),
            Err(_) => {
                error!("no reply from reader pool");
                Ok(None)
            }
        }
    }

    pub async fn read_stream(&self, stream_id: impl Into<Arc<str>>) -> io::Result<EventStreamIter> {
        let stream_id = stream_id.into();
        let bucket_id = stream_id_hash(&stream_id) % self.num_buckets;
        EventStreamIter::new(
            stream_id,
            bucket_id,
            self.reader_pool.clone(),
            self.writer_pool.indexes(),
        )
        .await
    }
}

pub struct DatabaseBuilder {
    dir: PathBuf,
    segment_size: usize,
    num_buckets: u16,
    reader_pool_size: u16,
    writer_pool_size: u16,
    flush_interval: Duration,
}

impl DatabaseBuilder {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        let cores = num_cpus::get_physical() as u16;
        let reader_pool_size = cores.clamp(4, 32);
        let writer_pool_size = (cores * 2).clamp(4, 64);

        DatabaseBuilder {
            dir: dir.into(),
            segment_size: 256_000_000,
            num_buckets: 64,
            reader_pool_size,
            writer_pool_size,
            flush_interval: Duration::from_millis(100),
        }
    }

    pub fn open(&self) -> io::Result<Database> {
        let reader_pool = ReaderThreadPool::new(self.reader_pool_size as usize);
        let writer_pool = WriterThreadPool::new(
            &self.dir,
            self.segment_size,
            self.num_buckets,
            self.writer_pool_size,
            self.flush_interval,
            &reader_pool,
        )?;

        Ok(Database {
            reader_pool,
            writer_pool,
            num_buckets: self.num_buckets,
        })
    }

    pub fn num_buckets(&mut self, n: u16) -> &mut Self {
        self.num_buckets = n;
        self
    }

    pub fn reader_pool_size(&mut self, n: u16) -> &mut Self {
        self.reader_pool_size = n;
        self
    }

    pub fn writer_pool_size(&mut self, n: u16) -> &mut Self {
        self.writer_pool_size = n;
        self
    }

    pub fn flush_interval(&mut self, interval: Duration) -> &mut Self {
        self.flush_interval = interval;
        self
    }
}
