use async_stream::stream;
use chrono::Timelike;
use futures::stream::BoxStream;
use futures::StreamExt;
use kameo::actor::ActorRef;
use kameo::error::SendError;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use eventstore::event_store_server::EventStore;
use eventstore::{
    AcknowledgeRequest, AppendToStreamRequest, Event, GetStreamEventsRequest, SubscribeRequest,
};

use crate::actor::{
    AppendToStream, GetStreamEvents, LoadSubscription, ReadBatch, Subscribe, UpdateSubscription,
};
use crate::{AppendError, EventLog, ReadLimit};

use self::eventstore::subscribe_request::StartFrom;
use self::eventstore::{AppendToStreamResponse, EventBatch};

const BATCH_SIZE: usize = 65_536; // 65KB

pub mod eventstore {
    use std::{borrow::Cow, time::UNIX_EPOCH};

    use chrono::{DateTime, Utc};
    use prost_types::Timestamp;

    use self::expected_version::Version;

    tonic::include_proto!("eventstore");

    #[derive(Clone, Copy, Debug)]
    pub struct InvalidTimestamp;

    impl From<ExpectedVersion> for crate::ExpectedVersion {
        fn from(v: ExpectedVersion) -> Self {
            match v.version {
                Some(version) => match version {
                    Version::Any(_) => crate::ExpectedVersion::Any,
                    Version::StreamExists(_) => crate::ExpectedVersion::StreamExists,
                    Version::NoStream(_) => crate::ExpectedVersion::NoStream,
                    Version::Exact(version) => crate::ExpectedVersion::Exact(version),
                },
                None => crate::ExpectedVersion::Any,
            }
        }
    }

    impl From<crate::ExpectedVersion> for ExpectedVersion {
        fn from(v: crate::ExpectedVersion) -> Self {
            match v {
                crate::ExpectedVersion::Any => ExpectedVersion {
                    version: Some(Version::Any(())),
                },
                crate::ExpectedVersion::StreamExists => ExpectedVersion {
                    version: Some(Version::StreamExists(())),
                },
                crate::ExpectedVersion::NoStream => ExpectedVersion {
                    version: Some(Version::NoStream(())),
                },
                crate::ExpectedVersion::Exact(version) => ExpectedVersion {
                    version: Some(Version::Exact(version)),
                },
            }
        }
    }

    impl TryFrom<crate::Event<'static>> for Event {
        type Error = InvalidTimestamp;

        fn try_from(event: crate::Event<'static>) -> Result<Self, Self::Error> {
            let epoch = DateTime::<Utc>::from(UNIX_EPOCH);
            let duration_since_epoch = event.timestamp.signed_duration_since(epoch);
            let timestamp = duration_since_epoch
                .to_std()
                .map_err(|_| InvalidTimestamp)?;
            Ok(Event {
                id: event.id,
                stream_id: event.stream_id.into_owned(),
                stream_version: event.stream_version,
                event_name: event.event_name.into_owned(),
                event_data: event.event_data.into_owned(),
                metadata: event.metadata.into_owned(),
                timestamp: Some(Timestamp {
                    seconds: timestamp
                        .as_secs()
                        .try_into()
                        .map_err(|_| InvalidTimestamp)?,
                    nanos: timestamp
                        .subsec_nanos()
                        .try_into()
                        .map_err(|_| InvalidTimestamp)?,
                }),
            })
        }
    }

    impl TryFrom<Event> for crate::Event<'static> {
        type Error = InvalidTimestamp;

        fn try_from(event: Event) -> Result<Self, Self::Error> {
            Ok(crate::Event {
                id: event.id,
                stream_id: Cow::Owned(event.stream_id),
                stream_version: event.stream_version,
                event_name: Cow::Owned(event.event_name),
                event_data: Cow::Owned(event.event_data),
                metadata: Cow::Owned(event.metadata),
                timestamp: event
                    .timestamp
                    .and_then(|ts| {
                        Some(
                            DateTime::<Utc>::from(UNIX_EPOCH)
                                + chrono::Duration::from_std(std::time::Duration::new(
                                    ts.seconds.try_into().ok()?,
                                    ts.nanos.try_into().ok()?,
                                ))
                                .ok()?,
                        )
                    })
                    .ok_or(InvalidTimestamp)?,
            })
        }
    }

    impl From<NewEvent> for crate::NewEvent<'static> {
        fn from(event: NewEvent) -> Self {
            crate::NewEvent {
                event_name: Cow::Owned(event.event_name),
                event_data: Cow::Owned(event.event_data),
                metadata: Cow::Owned(event.metadata),
            }
        }
    }
}

#[derive(Debug)]
pub struct DefaultEventStoreServer {
    log: ActorRef<EventLog>,
}

impl DefaultEventStoreServer {
    pub fn new(log: ActorRef<EventLog>) -> Self {
        DefaultEventStoreServer { log }
    }
}

#[tonic::async_trait]
impl EventStore for DefaultEventStoreServer {
    type GetStreamEventsStream = BoxStream<'static, Result<EventBatch, Status>>;
    type SubscribeStream = BoxStream<'static, Result<EventBatch, Status>>;

    async fn append_to_stream(
        &self,
        request: Request<AppendToStreamRequest>,
    ) -> Result<Response<AppendToStreamResponse>, Status> {
        let req = request.into_inner();
        let res = self
            .log
            .ask(AppendToStream {
                stream_id: req.stream_id,
                expected_version: req
                    .expected_version
                    .map(crate::ExpectedVersion::from)
                    .unwrap_or(crate::ExpectedVersion::Any),
                events: req.events.into_iter().map(|event| event.into()).collect(),
            })
            .send()
            .await;
        match res {
            Ok((offset, timestamp)) => Ok(Response::new(AppendToStreamResponse {
                first_id: offset.first(),
                timestamp: Some(prost_types::Timestamp {
                    seconds: timestamp.timestamp(),
                    nanos: timestamp.nanosecond().try_into().unwrap(),
                }),
            })),
            Err(SendError::HandlerError(AppendError::MessageSizeExceeded)) => {
                Err(Status::failed_precondition("message size exceeded"))
            }
            Err(SendError::HandlerError(err @ AppendError::WrongExpectedVersion { .. })) => {
                Err(Status::failed_precondition(err.to_string()))
            }
            Err(err) => Err(Status::internal(err.to_string())),
        }
    }

    async fn get_stream_events(
        &self,
        request: Request<GetStreamEventsRequest>,
    ) -> Result<Response<Self::GetStreamEventsStream>, Status> {
        let req = request.into_inner();

        let log = self.log.clone();
        let mut stream_version = req.stream_version;
        let s = stream! {
            loop {
                let events = log
                    .ask(GetStreamEvents {
                        stream_id: req.stream_id.clone(),
                        stream_version,
                        batch_size: req.batch_size as usize,
                    })
                    .send()
                    .await
                    .map_err_internal()?;
                if events.is_empty() {
                    break;
                }

                stream_version = events.last().unwrap().stream_version + 1;

                let batch = events.into_iter().map(|event| {
                    Event::try_from(event).map_err(|_| Status::internal("invalid timestamp"))
                }).collect::<Result<_, _>>().map(|events| EventBatch { events });

                yield batch;
            }
        };

        Ok(Response::new(Box::pin(s)))
    }

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(64);

        let mut start_offset = match req.start_from {
            Some(StartFrom::SubscriberId(subscriber_id)) => self
                .log
                .ask(LoadSubscription { subscriber_id })
                .send()
                .await
                .map_err_internal()?
                .map(|last| last + 1)
                .unwrap_or(0),
            Some(StartFrom::EventId(event_id)) => event_id,
            None => 0,
        };

        let log = self.log.clone();
        tokio::spawn(async move {
            // Subscribe, and save to a buffer
            let (oneshot_tx, mut oneshot_rx) =
                oneshot::channel::<(Sender<Vec<crate::Event<'static>>>, u64)>();

            tokio::spawn({
                let log = log.clone();
                async move {
                    let mut buffer = Vec::new();
                    let mut subscription = log.ask(Subscribe).send().await.map_err_internal()?;

                    // Consume into buffer whilst serving historical events
                    loop {
                        tokio::select! {
                            res = subscription.recv() => {
                                buffer.push(res.map_err_internal()?);
                            }
                            res = &mut oneshot_rx => {
                                let (tx, last_offset) = res.map_err_internal()?;

                                // Serve buffer
                                for mut batch in buffer {
                                    batch.retain(|event| event.id > last_offset);
                                    tx.send(batch).await.map_err_internal()?;
                                }

                                // Serve from subscription
                                while let Ok(batch) = subscription.recv().await {
                                    tx.send(batch).await.map_err_internal()?;
                                }

                                break;
                            }
                        }
                    }

                    Ok::<_, Status>(())
                }
            });

            // Start streaming from history
            loop {
                let Ok(batch) = log
                    .ask(ReadBatch {
                        start_offset,
                        read_limit: ReadLimit(BATCH_SIZE),
                    })
                    .send()
                    .await
                else {
                    break;
                };

                if batch.is_empty() {
                    break;
                }

                start_offset = batch.last().map(|ev| ev.id).unwrap_or(start_offset) + 1;

                if tx.send(batch).await.is_err() {
                    break;
                }
            }

            // Switch from buffer to live events
            if let Err(_) = oneshot_tx.send((tx, start_offset)) {
                return;
            }
        });

        let stream = ReceiverStream::new(rx).map(|events| {
            let events = events
                .into_iter()
                .map(Event::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| Status::internal("invalid timestamp"))?;
            Ok(EventBatch { events })
        });

        Ok(Response::new(Box::pin(stream)))
    }

    async fn acknowledge(
        &self,
        request: Request<AcknowledgeRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        self.log
            .ask(UpdateSubscription {
                id: req.subscriber_id,
                last_event_id: req.last_event_id,
            })
            .send()
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        Ok(Response::new(()))
    }
}

trait MapInternalError<T> {
    fn map_err_internal(self) -> Result<T, Status>;
}

impl<T, E> MapInternalError<T> for Result<T, E>
where
    E: ToString,
{
    fn map_err_internal(self) -> Result<T, Status> {
        self.map_err(|err| Status::internal(err.to_string()))
    }
}
