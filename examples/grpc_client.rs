use std::time::Instant;

use commitlog::server::eventstore::{
    event_store_client::EventStoreClient, subscribe_request::StartFrom, AcknowledgeRequest,
    AppendToStreamRequest, EventBatch, NewEvent, SubscribeRequest,
};
use tokio_stream::StreamExt;
use tonic::Request;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a client connection to the EventStore service
    let mut client = EventStoreClient::connect("http://[::1]:50051").await?;

    let start = Instant::now();
    for _ in 0..10_000 {
        let request = Request::new(AppendToStreamRequest {
            stream_id: "remote".to_string(),
            expected_version: None,
            events: vec![
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
                NewEvent {
                    event_name: "OogaBooga".to_string(),
                    event_data: vec![],
                    metadata: vec![],
                },
            ],
        });
        client.append_to_stream(request).await?;
    }
    println!("{} ms", start.elapsed().as_millis());

    // Create a subscribe request
    let request = Request::new(SubscribeRequest {
        start_from: Some(StartFrom::SubscriberId("subscriber_1".to_string())),
    });

    // Send the subscribe request and get the response stream
    let mut stream = client.subscribe(request).await?.into_inner();

    // Process the stream of EventBatch messages
    while let Some(event_batch) = stream.next().await {
        match event_batch {
            Ok(EventBatch { events }) => {
                let first_event_id = events.first().map(|event| event.id);
                let last_event_id = events.last().map(|event| event.id);
                println!(
                    "{}-{}",
                    first_event_id.unwrap_or_default(),
                    last_event_id.unwrap_or_default()
                );

                if let Some(id) = last_event_id {
                    client
                        .acknowledge(Request::new(AcknowledgeRequest {
                            subscriber_id: "subscriber_1".to_string(),
                            last_event_id: id,
                        }))
                        .await?;
                }
            }
            Err(e) => {
                eprintln!("Error receiving event batch: {:?}", e);
                break;
            }
        }
    }

    Ok(())
}
