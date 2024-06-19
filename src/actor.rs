use std::time::Duration;

use chrono::{DateTime, Utc};
use kameo::{
    actor::{ActorRef, BoundedMailbox},
    error::BoxError,
    message::{Context, Message},
    Actor,
};
use tokio::{io, sync::broadcast};

// const FLUSH_FREQUENCY_MS: u64 = 100;

use crate::{
    cli::ARGS, AppendError, CommitLog, Event, ExpectedVersion, NewEvent, OffsetRange, ReadError,
    ReadLimit,
};

impl Actor for CommitLog {
    type Mailbox = BoundedMailbox<Self>;

    fn name() -> &'static str {
        "CommitLog"
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
}

pub struct Flush;

impl Message<Flush> for CommitLog {
    type Reply = io::Result<()>;

    async fn handle(&mut self, _msg: Flush, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        self.flush()
    }
}

pub struct AppendToStream {
    pub stream_id: String,
    pub expected_version: ExpectedVersion,
    pub events: Vec<NewEvent<'static>>,
}

impl Message<AppendToStream> for CommitLog {
    type Reply = Result<(OffsetRange, DateTime<Utc>), AppendError>;

    async fn handle(
        &mut self,
        msg: AppendToStream,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        let timestamp = Utc::now();
        let offsets =
            self.append_to_stream(msg.stream_id, msg.expected_version, msg.events, timestamp)?;
        Ok((offsets, timestamp))
    }
}

pub struct GetStreamEvents {
    pub stream_id: String,
    pub stream_version: u64,
    pub batch_size: usize,
}

impl Message<GetStreamEvents> for CommitLog {
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

pub struct Subscribe;

impl Message<Subscribe> for CommitLog {
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

impl Message<LoadSubscription> for CommitLog {
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

impl Message<UpdateSubscription> for CommitLog {
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
}

impl Message<ReadBatch> for CommitLog {
    type Reply = Result<Vec<Event<'static>>, ReadError>;

    async fn handle(
        &mut self,
        msg: ReadBatch,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        self.read(msg.start_offset, msg.read_limit)
    }
}
