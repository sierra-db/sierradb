use std::time::Duration;

use eventus::{
    server::eventstore::{event_store_client::EventStoreClient, EventBatch, GetEventsRequest},
    Event,
};
use futures::StreamExt;
use tokio::{
    runtime::Runtime,
    sync::watch::{self, Sender},
};
use tonic::Request;

use crate::app::App;

mod app;
mod tui;

fn main() -> anyhow::Result<()> {
    let (events_tx, events_rx) = watch::channel::<Vec<Event<'static>>>(vec![]);

    std::thread::spawn(move || {
        let res = Runtime::new()
            .unwrap()
            .block_on(async move { start_client(events_tx).await });
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

// enum ClientRequest {
//     GetEvents,
// }

async fn start_client(events_tx: Sender<Vec<Event<'static>>>) -> anyhow::Result<()> {
    let mut client = EventStoreClient::connect("http://[::1]:9220").await?;

    let request = Request::new(GetEventsRequest {
        start_event_id: 0,
        batch_size: 100,
        limit: 1_000_000,
    });
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
            Err(e) => {
                eprintln!("Error receiving event batch: {:?}", e);
                break;
            }
        }
    }

    tokio::time::sleep(Duration::from_secs(100000)).await;

    Ok(())
}
