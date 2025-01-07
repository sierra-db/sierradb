use std::time::Duration;

use chrono::{DateTime, Utc};
use kameo::{
    actor::{ActorRef, WeakActorRef},
    error::{ActorStopReason, BoxError, PanicError},
    mailbox::bounded::BoundedMailbox,
    message::{Context, Message},
    request::MessageSend,
    Actor,
};
use tokio::{io, sync::broadcast};
use tracing::error;

// const FLUSH_FREQUENCY_MS: u64 = 100;

use crate::{
    cli::ARGS, AppendError, Event, EventLog, ExpectedVersion, NewEvent, OffsetRange, ReadError,
    ReadLimit,
};

impl Actor for EventLog {
    type Mailbox = BoundedMailbox<Self>;

    fn name() -> &'static str {
        "EventLog"
    }

    async fn on_start(&mut self, actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
        // tokio::task::Builder::new()
        //     .name("flusher")
        tokio::spawn(async move {
            let flush_interval = ARGS
                .get()
                .map(|args| args.flush_interval)
                .unwrap_or_else(|| Duration::from_millis(100));
            loop {
                tokio::time::sleep(flush_interval).await;
                if let Err(_) = actor_ref.tell(Flush).send().await {
                    return;
                }
            }
        });

        Ok(())
    }

    async fn on_panic(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        err: PanicError,
    ) -> Result<Option<ActorStopReason>, BoxError> {
        error!("event log actor panicked: {err}");
        return Ok(None); // Restart
    }

    async fn on_stop(
        self,
        _actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> Result<(), BoxError> {
        if !matches!(reason, ActorStopReason::Normal) {
            error!("event log actor stopping: {reason:?}");
        }
        Ok(())
    }
}

pub struct Flush;

impl Message<Flush> for EventLog {
    type Reply = io::Result<()>;

    async fn handle(&mut self, _msg: Flush, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        self.flush()
    }
}

pub struct AppendToStream {
    pub stream_id: String,
    pub expected_version: ExpectedVersion,
    pub events: Vec<NewEvent<'static>>,
    pub timestamp: Option<DateTime<Utc>>,
}

impl Message<AppendToStream> for EventLog {
    type Reply = Result<(OffsetRange, DateTime<Utc>), AppendError>;

    async fn handle(
        &mut self,
        msg: AppendToStream,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        let tx = self.next_tx();
        let timestamp = msg.timestamp.unwrap_or_else(|| Utc::now());
        self.append_to_stream(
            msg.stream_id,
            msg.expected_version,
            msg.events,
            timestamp,
            tx,
        )?;
        let offsets = self.commit(tx)?;

        Ok((offsets, timestamp))
    }
}

pub struct AppendToMultipleStreams {
    pub streams: Vec<NewStreamEvents<'static>>,
}

pub struct NewStreamEvents<'a> {
    pub stream_id: String,
    pub expected_version: ExpectedVersion,
    pub events: Vec<NewEvent<'a>>,
    pub timestamp: Option<DateTime<Utc>>,
}

impl Message<AppendToMultipleStreams> for EventLog {
    type Reply = Result<Vec<(OffsetRange, DateTime<Utc>)>, AppendError>;

    async fn handle(
        &mut self,
        msg: AppendToMultipleStreams,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        let tx = self.next_tx();

        let mut results = Vec::with_capacity(msg.streams.len());
        let now = Utc::now();
        let mut stream_offset = 0;
        for stream in msg.streams {
            if stream.events.is_empty() {
                continue;
            }
            let timestamp = stream.timestamp.unwrap_or(now);
            let events_len = stream.events.len();
            self.append_to_stream(
                stream.stream_id.clone(),
                stream.expected_version,
                stream.events,
                timestamp,
                tx,
            )?;

            results.push((stream_offset, events_len, timestamp));
            stream_offset += events_len;
        }
        let offsets = self.commit(tx)?;

        Ok(results
            .into_iter()
            .map(|(stream_offset, events_len, timestamp)| {
                (
                    OffsetRange::new(offsets.first() + stream_offset as u64, events_len),
                    timestamp,
                )
            })
            .collect())
    }
}

pub struct GetStreamEvents {
    pub stream_id: String,
    pub stream_version: u64,
    pub batch_size: usize,
}

impl Message<GetStreamEvents> for EventLog {
    type Reply = Result<Vec<Event<'static>>, ReadError>;

    async fn handle(
        &mut self,
        msg: GetStreamEvents,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        self.read_stream(&msg.stream_id, msg.stream_version)
            .take(msg.batch_size)
            .collect()
    }
}

pub struct GetLastEventID;

impl Message<GetLastEventID> for EventLog {
    type Reply = Option<u64>;

    async fn handle(
        &mut self,
        _msg: GetLastEventID,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        self.last_offset()
    }
}

pub struct Subscribe;

impl Message<Subscribe> for EventLog {
    type Reply = broadcast::Receiver<Vec<Event<'static>>>;

    async fn handle(
        &mut self,
        _msg: Subscribe,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        self.subscribe()
    }
}

pub struct LoadSubscription {
    pub subscriber_id: String,
}

impl Message<LoadSubscription> for EventLog {
    type Reply = rusqlite::Result<Option<u64>>;

    async fn handle(
        &mut self,
        msg: LoadSubscription,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        self.load_subscription(&msg.subscriber_id)
    }
}

pub struct UpdateSubscription {
    pub id: String,
    pub last_event_id: u64,
}

impl Message<UpdateSubscription> for EventLog {
    type Reply = rusqlite::Result<()>;

    async fn handle(
        &mut self,
        msg: UpdateSubscription,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        self.update_subscription(&msg.id, msg.last_event_id)
    }
}

pub struct ReadBatch {
    pub start_offset: u64,
    pub read_limit: ReadLimit,
    pub batch_size: usize,
}

impl Message<ReadBatch> for EventLog {
    type Reply = Result<Vec<Event<'static>>, ReadError>;

    async fn handle(
        &mut self,
        msg: ReadBatch,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        self.read(msg.start_offset, msg.read_limit, msg.batch_size)
    }
}
