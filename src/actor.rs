use std::time::Duration;

use futures::Future;
use kameo::{
    actor::ActorRef,
    error::BoxError,
    message::{Context, Message},
    Actor,
};
use tokio::{io, sync::broadcast};

use crate::{
    AppendError, CommitLog, Event, ExpectedVersion, NewEvent, OffsetRange, ReadError, ReadLimit,
};

impl Actor for CommitLog {
    fn name() -> &'static str {
        "CommitLog"
    }

    async fn on_start(&mut self, actor_ref: ActorRef<Self>) -> Result<(), BoxError> {
        tokio::task::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                if let Err(_) = actor_ref.send(Flush).await {
                    let _ = actor_ref.stop_gracefully();
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
        self.flush().await
        // Ok(())
    }
}

pub struct AppendToStream {
    pub stream_id: String,
    pub expected_version: ExpectedVersion,
    pub events: Vec<NewEvent<'static>>,
}

impl Message<AppendToStream> for CommitLog {
    type Reply = Result<OffsetRange, AppendError>;

    async fn handle(
        &mut self,
        msg: AppendToStream,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> Self::Reply {
        let offsets = self
            .append_to_stream(msg.stream_id, msg.expected_version, msg.events)
            .await?;
        Ok(offsets)
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
        self.load_subscription(&msg.subscriber_id).await
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
        self.update_subscription(&msg.id, msg.last_event_id).await
    }
}

pub struct ReadBatch {
    pub start_offset: u64,
    pub read_limit: ReadLimit,
}

impl Message<ReadBatch> for CommitLog {
    type Reply = Result<Vec<Event<'static>>, ReadError>;

    fn handle(
        &mut self,
        msg: ReadBatch,
        _ctx: Context<'_, Self, Self::Reply>,
    ) -> impl Future<Output = Self::Reply> + Send {
        async move { self.read(msg.start_offset, msg.read_limit).await }
    }
}
