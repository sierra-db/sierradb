use std::time::Duration;

use dialoguer::Input;
use eventus::{
    server::{
        eventstore::{
            event_store_client::EventStoreClient, EventBatch, GetEventsRequest,
            GetStreamEventsRequest,
        },
        ClientAuthInterceptor,
    },
    Event,
};
use futures::{StreamExt, TryStreamExt};
use tokio::{
    runtime::Runtime,
    sync::watch::{self, Sender},
};
use tonic::{transport::Channel, Request};

use crate::app::App;

mod app;
mod tui;

fn main() -> anyhow::Result<()> {
    let stream_id: String = Input::new()
        .with_prompt("Stream ID to view (* for all)")
        .interact_text()
        .unwrap();
    let (events_tx, events_rx) = watch::channel::<Vec<Event<'static>>>(vec![]);

    std::thread::spawn(move || {
        let res = Runtime::new()
            .unwrap()
            .block_on(async move { start_client(stream_id, events_tx).await });
        if let Err(err) = res {
            println!("client stopped with error: {err}");
        }

        std::process::exit(1);
    });

    let mut terminal = tui::init()?;
    let app_result = App::new(events_rx).run(&mut terminal);
    tui::restore()?;
    app_result
}

async fn start_client(
    stream_id: String,
    events_tx: Sender<Vec<Event<'static>>>,
) -> anyhow::Result<()> {
    let channel = Channel::builder("http://localhost:9220".parse()?)
        // let channel = Channel::builder("http://170.64.244.28:9220".parse()?)
        .connect()
        .await?;
    let mut client =
        EventStoreClient::with_interceptor(channel, ClientAuthInterceptor::new("localhost")?);
    // EventStoreClient::with_interceptor(channel, ClientAuthInterceptor::new("katm2V12UwkChWgDVhpZKFHUCVvOJYwqKfLLYlbrbVuoLqo20ZAZ6pesTurFGP1v")?);

    if stream_id.trim() == "*" {
        let request = GetEventsRequest {
            start_event_id: 0,
            batch_size: 100,
            limit: Some(1_000_000),
        };
        // let mut all_events = Vec::new();
        let mut is_first_iteration = true;
        let mut stream = client.get_events(request).await?.into_inner();
        while let Some(event_batch) = stream.next().await {
            match event_batch {
                Ok(EventBatch { events }) => {
                    let events = events
                        .into_iter()
                        .map(|event| Event::try_from(event).unwrap());
                    if is_first_iteration {
                        events_tx.send(events.collect())?;
                        is_first_iteration = false;
                    } else {
                        events_tx.send_modify(|all_events| all_events.extend(events));
                    }
                }
                Err(err) => {
                    panic!("Error receiving event batch: {err:?}");
                }
            }
        }
    } else {
        let events: Vec<_> = client
            .get_stream_events(GetStreamEventsRequest {
                stream_id,
                stream_version: 0,
                batch_size: 1000,
            })
            .await?
            .into_inner()
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flat_map(|batch| batch.events)
            .map(|event| Event::try_from(event).unwrap())
            .collect();

        events_tx.send(events)?;
    }

    tokio::time::sleep(Duration::from_secs(100000)).await;

    Ok(())
}
