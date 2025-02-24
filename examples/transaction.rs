use chrono::Utc;
use eventus::{EventLog, ExpectedVersion, LogOptions, NewEvent, ReadLimit};
use std::{borrow::Cow, collections::HashMap};
use tracing::Instrument;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::builder()
        .filter_level("trace".parse().unwrap())
        .init();

    // Create a unique log file name based on the current time to avoid conflicts
    let log_file_name = format!(
        "../oddselite2/.log-temp",
        // SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()
    );
    let opts = LogOptions::new(log_file_name);

    // Initialize the EventLog
    let mut log = EventLog::new(opts).unwrap();

    // let event = log
    //     .read_last_stream_msg("game-sportsbet-8651443")
    //     .unwrap()
    //     .unwrap();
    // dbg!(
    //     event.id,
    //     event.stream_id,
    //     event.stream_version,
    //     event.event_name
    // );

    // // Define stream IDs
    // let stream_id_1 = "stream1";
    // let stream_id_2 = "stream2";

    // // Transaction 1: Will be committed
    // let tx1 = log.next_tx();
    // let expected_version = ExpectedVersion::Any;

    // let _offsets1 = log
    //     .append_to_stream(
    //         stream_id_1,
    //         expected_version,
    //         vec![NewEvent {
    //             event_name: "Event1".into(),
    //             event_data: Cow::Borrowed(&[]),
    //             metadata: Cow::Borrowed(&[]),
    //         }],
    //         Utc::now(),
    //         tx1,
    //     )
    //     .unwrap();

    // log.commit(tx1).unwrap();

    // // Transaction 2: Will not be committed
    // let tx2 = log.next_tx();

    // let _offsets2 = log.append_to_stream(
    //     stream_id_2,
    //     expected_version,
    //     vec![NewEvent {
    //         event_name: "Event2".into(),
    //         event_data: Cow::Borrowed(&[]),
    //         metadata: Cow::Borrowed(&[]),
    //     }],
    //     Utc::now(),
    //     tx2,
    // )?;

    // // Note: Not calling log.commit(tx2), so this transaction remains uncommitted
    // // log.commit(tx2).unwrap();

    // // Transaction 3: Multiple events, will be committed
    // let tx3 = log.next_tx();

    // let _offsets3 = log
    //     .append_to_stream(
    //         stream_id_1,
    //         expected_version,
    //         vec![NewEvent {
    //             event_name: "Event3".into(),
    //             event_data: Cow::Borrowed(&[]),
    //             metadata: Cow::Borrowed(&[]),
    //         }],
    //         Utc::now(),
    //         tx3,
    //     )
    //     .unwrap();

    // let _offsets4 = log
    //     .append_to_stream(
    //         stream_id_2,
    //         expected_version,
    //         vec![NewEvent {
    //             event_name: "Event4".into(),
    //             event_data: Cow::Borrowed(&[]),
    //             metadata: Cow::Borrowed(&[]),
    //         }],
    //         Utc::now(),
    //         tx3,
    //     )
    //     .unwrap();

    // log.commit(tx3).unwrap();
    // let tx4 = log.next_tx();
    // dbg!(tx4);

    // Now read back the events from position 0
    // let events = log
    //     .read(0, ReadLimit::max_bytes(1024 * 64 * 100), usize::MAX)
    // //     .unwrap();
    let events = log.read_stream("game-sportsbet-8651443", 0);

    // let mut expected_stream_versions = HashMap::new();
    for event in events {
        let event = event?;
        println!(
            "{} - {} {} {}",
            event.id, event.stream_id, event.stream_version, event.event_name
        );
        // let expected = expected_stream_versions
        //     .entry(event.stream_id.clone())
        //     .or_insert(0);
        // *expected += 1;
    }

    // Filter out events from the read result
    // dbg!(&events);
    // let event_names: Vec<_> = events.into_iter().map(|event| event.event_name).collect();

    // Expected event IDs are from committed transactions only: [1, 3, 4]
    // let expected_event_names = vec![
    //     Cow::Borrowed("Event1"),
    //     Cow::Borrowed("Event3"),
    //     Cow::Borrowed("Event4"),
    // ];

    // assert_eq!(event_names, expected_event_names);

    // log.flush().unwrap();

    Ok(())
}
