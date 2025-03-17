use crate::common::{fresh_stream_id, generate_events};
use chrono::{Datelike, Utc};
use futures::channel::oneshot;
use kurrentdb::{
    Acl, Client, ReadEvent, StreamAclBuilder, StreamMetadataBuilder, StreamMetadataResult,
    StreamName, StreamPosition, SubscriptionEvent,
};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, warn};

async fn test_write_events(client: &Client) -> kurrentdb::Result<()> {
    let stream_id = fresh_stream_id("write_events");
    let events = generate_events("write-events-test", 3);

    let result = client
        .append_to_stream(stream_id, &Default::default(), events)
        .await?;

    debug!("Write response: {:?}", result);
    assert_eq!(result.next_expected_version, 2);

    Ok(())
}
async fn test_tick_date_conversion(client: &Client) -> kurrentdb::Result<()> {
    let stream_id = fresh_stream_id("ticks_date");
    let events = generate_events("about_date_stuff", 1);

    client
        .append_to_stream(stream_id.as_str(), &Default::default(), events)
        .await?;

    let mut stream = client
        .read_stream(stream_id.as_str(), &Default::default())
        .await?;

    let event = stream.next().await?.unwrap();
    let now = Utc::now();
    let created = event.get_original_event().created;

    assert_eq!(now.day(), created.day());
    assert_eq!(now.year(), created.year());
    assert_eq!(now.month(), created.month());

    Ok(())
}

// We read all stream events by batch.
async fn test_read_all_stream_events(client: &Client) -> kurrentdb::Result<()> {
    // kurrent should always have "some" events in $all, since kurrent itself uses streams, ouroboros style.
    let result = client.read_all(&Default::default()).await?.next().await?;

    assert!(result.is_some());

    Ok(())
}

// We read stream events by batch. We also test if we can properly read a
// stream thoroughly.
async fn test_read_stream_events(client: &Client) -> kurrentdb::Result<()> {
    let stream_id = fresh_stream_id("read_stream_events");
    let events = generate_events("es6-read-stream-events-test", 10);

    let _ = client
        .append_to_stream(stream_id.clone(), &Default::default(), events)
        .await?;

    let mut pos = 0usize;
    let mut idx = 0i64;

    let mut stream = client.read_stream(stream_id, &Default::default()).await?;

    while let Some(event) = stream.next().await? {
        let event = event.get_original_event();
        let obj: HashMap<String, i64> = event.as_json().unwrap();
        let value = obj.get("event_index").unwrap();

        idx = *value;
        pos += 1;
    }

    assert_eq!(pos, 10);
    assert_eq!(idx, 10);

    Ok(())
}

async fn test_read_stream_events_with_position(client: &Client) -> kurrentdb::Result<()> {
    let stream_id = fresh_stream_id("read_position");
    let events = generate_events("read_position", 10);

    let _ = client
        .append_to_stream(stream_id.as_str(), &Default::default(), events)
        .await?;

    let options = kurrentdb::ReadStreamOptions::default()
        .forwards()
        .position(StreamPosition::Start);

    let mut stream = client.read_stream(stream_id, &options).await?;

    let mut last_stream_position = 0u64;
    while let Some(event) = stream.next_read_event().await? {
        if let ReadEvent::LastStreamPosition(pos) = event {
            last_stream_position = pos;
        }
    }

    assert_eq!(9, last_stream_position);

    Ok(())
}

async fn test_read_stream_populates_log_position(client: &Client) -> kurrentdb::Result<()> {
    let stream_id = fresh_stream_id("read_stream_populates_log_position");
    let events = generate_events("read_stream_populates_log_position", 1);

    let write_result = client
        .append_to_stream(stream_id.clone(), &Default::default(), events)
        .await?;

    assert_eq!(write_result.position.prepare, write_result.position.commit);

    let mut pos = 0usize;
    let mut stream = client.read_stream(stream_id, &Default::default()).await?;

    while let Some(event) = stream.next().await? {
        let event = event.get_original_event();
        assert_eq!(write_result.position, event.position);
        pos += 1;
    }

    assert_eq!(pos, 1);

    Ok(())
}

async fn test_metadata(client: &Client) -> kurrentdb::Result<()> {
    let stream_id = fresh_stream_id("metadata");
    let events = generate_events("metadata-test", 5);

    let _ = client
        .append_to_stream(stream_id.as_str(), &Default::default(), events)
        .await?;

    let expected = StreamMetadataBuilder::new()
        .max_age(std::time::Duration::from_secs(2))
        .acl(Acl::Stream(
            StreamAclBuilder::new().add_read_roles("admin").build(),
        ))
        .build();

    let _ = client
        .set_stream_metadata(stream_id.as_str(), &Default::default(), &expected)
        .await?;

    let actual = client
        .get_stream_metadata(stream_id.as_str(), &Default::default())
        .await?;

    assert!(actual.is_success());

    if let StreamMetadataResult::Success(actual) = actual {
        assert_eq!(&expected, actual.metadata());
    }

    Ok(())
}

async fn test_metadata_not_exist(client: &Client) -> kurrentdb::Result<()> {
    let stream_id = fresh_stream_id("metadata_not_exist");
    let events = generate_events("metadata-test-not-exist", 5);

    let _ = client
        .append_to_stream(stream_id.as_str(), &Default::default(), events)
        .await?;

    let actual = client
        .get_stream_metadata(stream_id.as_str(), &Default::default())
        .await?;

    assert!(actual.is_not_found());

    Ok(())
}

// We check to see the client can handle the correct GRPC proto response when
// a stream does not exist
async fn test_read_stream_events_non_existent(client: &Client) -> kurrentdb::Result<()> {
    let stream_id = fresh_stream_id("read_stream_events");

    let mut stream = client
        .read_stream(stream_id.as_str(), &Default::default())
        .await?;

    if let Err(kurrentdb::Error::ResourceNotFound) = stream.next().await {
        return Ok(());
    }

    panic!("We expected to have a stream not found result");
}

// We write an event into a stream then soft delete that stream.
async fn test_delete_stream(client: &Client) -> kurrentdb::Result<()> {
    let stream_id = fresh_stream_id("delete");
    let events = generate_events("delete-test", 1);

    let _ = client
        .append_to_stream(stream_id.clone(), &Default::default(), events)
        .await?;

    let result = client
        .delete_stream(stream_id.as_str(), &Default::default())
        .await?;

    debug!("Delete stream [{}] result: {:?}", stream_id, result);

    Ok(())
}

// We write an event into a stream then hard delete that stream.
async fn test_tombstone_stream(client: &Client) -> kurrentdb::Result<()> {
    let stream_id = fresh_stream_id("tombstone");
    let events = generate_events("tombstone-test", 1);

    let _ = client
        .append_to_stream(stream_id.clone(), &Default::default(), events)
        .await?;

    let result = client
        .tombstone_stream(stream_id.as_str(), &Default::default())
        .await?;

    debug!("Tombstone stream [{}] result: {:?}", stream_id, result);

    let result = client
        .read_stream(stream_id.as_str(), &Default::default())
        .await;

    if let Err(kurrentdb::Error::ResourceDeleted) = result {
        Ok(())
    } else {
        panic!("Expected stream deleted error");
    }
}

// We write events into a stream. Then, we issue a catchup subscription. After,
// we write another batch of events into the same stream. The goal is to make
// sure we receive events written prior and after our subscription request.
// To assess we received all the events we expected, we test our subscription
// internal state value.
async fn test_subscription(client: &Client) -> eyre::Result<()> {
    let stream_id = fresh_stream_id("catchup");
    let events_before = generate_events("catchup-test-before", 3);
    let events_after = generate_events("catchup-test-after", 3);

    let _ = client
        .append_to_stream(stream_id.as_str(), &Default::default(), events_before)
        .await?;

    let options =
        kurrentdb::SubscribeToStreamOptions::default().start_from(kurrentdb::StreamPosition::Start);

    let mut sub = client
        .subscribe_to_stream(stream_id.as_str(), &options)
        .await;

    let (tx, recv) = oneshot::channel();

    tokio::spawn(async move {
        let mut count = 0usize;
        let max = 6usize;

        loop {
            sub.next().await?;
            count += 1;

            if count == max {
                break;
            }
        }

        tx.send(count).unwrap();
        Ok(()) as kurrentdb::Result<()>
    });

    let _ = client
        .append_to_stream(stream_id, &Default::default(), events_after)
        .await?;

    match tokio::time::timeout(Duration::from_secs(60), recv).await {
        Ok(test_count) => {
            assert_eq!(
                test_count?, 6,
                "We are testing proper state after catchup subscription: got {} expected {}.",
                test_count?, 6
            );
        }

        Err(_) => panic!("test_subscription timed out!"),
    }

    Ok(())
}

async fn test_subscription_caughtup(client: &Client) -> kurrentdb::Result<()> {
    let info = client.server_info().await?;

    if info.version() < (23, 10) {
        warn!(
            "test_susbcription_caughtup is skipped because server {} doesn't support it",
            info.version()
        );
        return Ok(());
    }

    let stream_id = fresh_stream_id("catchup_live_detection").into_stream_name();
    let events = generate_events("catchup_live_detected", 10);

    let _ = client
        .append_to_stream(stream_id.clone(), &Default::default(), events)
        .await?;

    let options =
        kurrentdb::SubscribeToStreamOptions::default().start_from(kurrentdb::StreamPosition::Start);

    let mut sub = client
        .subscribe_to_stream(stream_id.clone(), &options)
        .await;

    let (tx, recv) = oneshot::channel();

    tokio::spawn(async move {
        loop {
            if let SubscriptionEvent::CaughtUp = sub.next_subscription_event().await? {
                break;
            }
        }

        let _ = tx.send(());
        Ok(()) as kurrentdb::Result<()>
    });

    if tokio::time::timeout(Duration::from_secs(60), recv)
        .await
        .is_err()
    {
        panic!("test_subscription_caughtup timed out!");
    }

    Ok(())
}

async fn test_subscription_all_filter(client: &Client) -> kurrentdb::Result<()> {
    let filter = kurrentdb::SubscriptionFilter::on_event_type().exclude_system_events();
    let options = kurrentdb::SubscribeToAllOptions::default()
        .position(kurrentdb::StreamPosition::Start)
        .filter(filter);

    let mut sub = client.subscribe_to_all(&options).await;

    match tokio::time::timeout(Duration::from_secs(60), async move {
        let event = sub.next().await?;

        assert!(!event.get_original_event().event_type.starts_with('$'));

        Ok(()) as kurrentdb::Result<()>
    })
    .await
    {
        Ok(result) => assert!(result.is_ok()),
        Err(_) => panic!("we are supposed to receive event from that subscription"),
    };

    Ok(())
}

async fn test_batch_append(client: &Client) -> kurrentdb::Result<()> {
    let batch_client = client.batch_append(&Default::default()).await?;

    for _ in 0..3 {
        let stream_id = fresh_stream_id("batch-append");
        let events = generate_events("batch-append-type", 3);
        let _ = batch_client
            .append_to_stream(stream_id.as_str(), kurrentdb::ExpectedRevision::Any, events)
            .await?;
        let options = kurrentdb::ReadStreamOptions::default()
            .forwards()
            .position(kurrentdb::StreamPosition::Start);
        let mut stream = client.read_stream(stream_id.as_str(), &options).await?;

        let mut cpt = 0usize;

        while (stream.next().await?).is_some() {
            cpt += 1;
        }

        assert_eq!(cpt, 3, "We expecting 3 events out of those streams");
    }

    Ok(())
}

// Tests that filtering works correctly when reading from $all stream
async fn test_read_all_filter(client: &Client) -> kurrentdb::Result<()> {
    // Create a unique prefix for our test events to make them identifiable
    let unique_prefix = format!("filter-test-{}", uuid::Uuid::new_v4());
    let stream_id_prefix = format!("filter-stream-{}", uuid::Uuid::new_v4());

    // Create two streams with different names for stream name filtering tests
    let stream_id1 = format!("{}-one", stream_id_prefix);
    let stream_id2 = format!("{}-two", stream_id_prefix);
    let other_stream_id = fresh_stream_id("read_all_filter_other");

    // Create events with different event types
    let filtered_type = format!("{}-include", unique_prefix);
    let unfiltered_type = format!("{}-exclude", unique_prefix);

    // Create event data directly without using serde_json::Result
    let events_to_append1 = vec![
        kurrentdb::EventData::json(
            &filtered_type,
            &serde_json::json!({"filtered": true, "index": 1}),
        )
        .unwrap(),
        kurrentdb::EventData::json(
            &unfiltered_type,
            &serde_json::json!({"filtered": false, "index": 2}),
        )
        .unwrap(),
        kurrentdb::EventData::json(
            &filtered_type,
            &serde_json::json!({"filtered": true, "index": 3}),
        )
        .unwrap(),
    ];

    // Append events to second stream
    let events_to_append2 = vec![
        kurrentdb::EventData::json(
            &filtered_type,
            &serde_json::json!({"filtered": true, "index": 4}),
        )
        .unwrap(),
        kurrentdb::EventData::json(
            &filtered_type,
            &serde_json::json!({"filtered": true, "index": 5}),
        )
        .unwrap(),
    ];

    // Append to other stream (should be excluded by stream prefix filter)
    let events_other = vec![
        kurrentdb::EventData::json(
            &filtered_type,
            &serde_json::json!({"filtered": true, "index": 99}),
        )
        .unwrap(),
    ];

    // Append our test events
    client
        .append_to_stream(stream_id1.clone(), &Default::default(), events_to_append1)
        .await?;
    client
        .append_to_stream(stream_id2.clone(), &Default::default(), events_to_append2)
        .await?;
    client
        .append_to_stream(other_stream_id.clone(), &Default::default(), events_other)
        .await?;

    debug!(
        "Created test streams: {}, {}, {}",
        stream_id1, stream_id2, other_stream_id
    );

    // TEST 1: Event type prefix filtering
    debug!("Testing event type prefix filtering");
    let filter1 = kurrentdb::SubscriptionFilter::on_event_type().add_prefix(&filtered_type);
    let options1 = kurrentdb::ReadAllOptions::default()
        .filter(filter1)
        .max_count(100);

    let mut stream = client.read_all(&options1).await?;
    let mut filtered_events = Vec::new();

    while let Some(event) = stream.next().await? {
        if event.get_original_stream_id() == stream_id1
            || event.get_original_stream_id() == stream_id2
        {
            filtered_events.push(event);
        }
    }

    // Should find exactly 4 filtered events (2 from stream_id1, 2 from stream_id2)
    assert_eq!(
        filtered_events.len(),
        4,
        "Expected exactly 4 events with filtered type"
    );

    // Verify all events have the expected type
    for event in filtered_events {
        assert_eq!(
            event.get_original_event().event_type,
            filtered_type,
            "Event should match our filtered type"
        );
    }

    // TEST 2: Stream name prefix filtering
    debug!("Testing stream name prefix filtering");
    let filter2 = kurrentdb::SubscriptionFilter::on_stream_name().add_prefix(&stream_id_prefix);
    let options2 = kurrentdb::ReadAllOptions::default()
        .filter(filter2)
        .max_count(100);

    let mut stream = client.read_all(&options2).await?;
    let mut stream_filtered_events = Vec::new();

    while let Some(event) = stream.next().await? {
        stream_filtered_events.push(event);
    }

    // Should find exactly 5 events (3 from stream_id1, 2 from stream_id2)
    assert_eq!(
        stream_filtered_events.len(),
        5,
        "Expected exactly 5 events from streams with prefix"
    );

    // Verify all events are from expected streams
    for event in &stream_filtered_events {
        let stream_name = event.get_original_stream_id();
        assert!(
            stream_name == stream_id1 || stream_name == stream_id2,
            "Event should be from one of our filtered streams"
        );
    }

    // TEST 3: Regular expression pattern matching
    debug!("Testing regex pattern matching");
    let regex_filter = kurrentdb::SubscriptionFilter::on_event_type()
        .regex(&format!("{}.*include", unique_prefix));
    let regex_options = kurrentdb::ReadAllOptions::default()
        .filter(regex_filter)
        .max_count(100);

    let mut stream = client.read_all(&regex_options).await?;
    let mut regex_filtered_events = Vec::new();

    while let Some(event) = stream.next().await? {
        if event.get_original_stream_id() == stream_id1
            || event.get_original_stream_id() == stream_id2
        {
            regex_filtered_events.push(event);
        }
    }

    // Should find exactly 4 filtered events that match regex
    assert_eq!(
        regex_filtered_events.len(),
        4,
        "Expected exactly 4 events matching regex"
    );

    // Verify all events have the expected type
    for event in regex_filtered_events {
        assert_eq!(
            event.get_original_event().event_type,
            filtered_type,
            "Event should match our filtered type via regex"
        );
    }

    Ok(())
}

pub async fn tests(client: Client) -> eyre::Result<()> {
    let info = client.server_info().await?;

    debug!("Before test_write_events…");
    test_write_events(&client).await?;
    debug!("Complete");
    debug!("Before test_tick_date_conversion…");
    test_tick_date_conversion(&client).await?;
    debug!("Complete");
    debug!("Before test_all_read_stream_events…");
    test_read_all_stream_events(&client).await?;
    debug!("Complete");
    debug!("Before test_read_stream_events…");
    test_read_stream_events(&client).await?;
    debug!("Complete");
    if info.version() >= (21, 10) {
        debug!("Before test_read_stream_events_with_position");
        test_read_stream_events_with_position(&client).await?;
        debug!("Complete");
    }
    if info.version() >= 22 {
        debug!("Before test_read_stream_populates_log_position");
        test_read_stream_populates_log_position(&client).await?;
    }
    debug!("Complete");
    debug!("Before test_read_stream_events_non_existent");
    test_read_stream_events_non_existent(&client).await?;
    debug!("Complete");
    debug!("Before test test_metadata");
    test_metadata(&client).await?;
    debug!("Complete");
    debug!("Before test test_metadata_not_exist");
    test_metadata_not_exist(&client).await?;
    debug!("Complete");
    debug!("Before test_delete_stream…");
    test_delete_stream(&client).await?;
    debug!("Complete");
    debug!("Before test_tombstone_stream…");
    test_tombstone_stream(&client).await?;
    debug!("Complete");
    debug!("Before test_subscription…");
    test_subscription(&client).await?;
    debug!("Complete");
    debug!("Before test_subscription_caughtup…");
    test_subscription_caughtup(&client).await?;
    debug!("Complete");
    debug!("Before test_subscription_all_filter…");
    test_subscription_all_filter(&client).await?;
    debug!("Complete");
    debug!("Before test_batch_append");
    if let Err(e) = test_batch_append(&client).await {
        if let kurrentdb::Error::UnsupportedFeature = e {
            warn!("batch_append is not supported on the server we are targeting");
            Ok(())
        } else {
            Err(e)
        }?;
    }
    debug!("Complete");
    debug!("Before test_read_all_filter…");
    test_read_all_filter(&client).await?;
    debug!("Complete");

    Ok(())
}
