use crate::common::{fresh_stream_id, generate_events};
use chrono::{Datelike, Utc};
use futures::channel::oneshot;
use kurrent::{
    Acl, Client, ReadEvent, StreamAclBuilder, StreamMetadataBuilder, StreamMetadataResult,
    StreamName, StreamPosition, SubscriptionEvent,
};
use std::collections::HashMap;
use std::time::Duration;

async fn test_write_events(client: &Client) -> kurrent::Result<()> {
    let stream_id = fresh_stream_id("write_events");
    let events = generate_events("write-events-test".to_string(), 3);

    let result = client
        .append_to_stream(stream_id, &Default::default(), events)
        .await?;

    debug!("Write response: {:?}", result);
    assert_eq!(result.next_expected_version, 2);

    Ok(())
}
async fn test_tick_date_conversion(client: &Client) -> kurrent::Result<()> {
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
async fn test_read_all_stream_events(client: &Client) -> kurrent::Result<()> {
    // kurrent should always have "some" events in $all, since kurrent itself uses streams, ouroboros style.
    let result = client.read_all(&Default::default()).await?.next().await?;

    assert!(result.is_some());

    Ok(())
}

// We read stream events by batch. We also test if we can properly read a
// stream thoroughly.
async fn test_read_stream_events(client: &Client) -> kurrent::Result<()> {
    let stream_id = fresh_stream_id("read_stream_events");
    let events = generate_events("es6-read-stream-events-test".to_string(), 10);

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

async fn test_read_stream_events_with_position(client: &Client) -> kurrent::Result<()> {
    let stream_id = fresh_stream_id("read_position");
    let events = generate_events("read_position", 10);

    let _ = client
        .append_to_stream(stream_id.as_str(), &Default::default(), events)
        .await?;

    let options = kurrent::ReadStreamOptions::default()
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

async fn test_read_stream_populates_log_position(client: &Client) -> kurrent::Result<()> {
    let stream_id = fresh_stream_id("read_stream_populates_log_position");
    let events = generate_events("read_stream_populates_log_position".to_string(), 1);

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

async fn test_metadata(client: &Client) -> kurrent::Result<()> {
    let stream_id = fresh_stream_id("metadata");
    let events = generate_events("metadata-test".to_string(), 5);

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

async fn test_metadata_not_exist(client: &Client) -> kurrent::Result<()> {
    let stream_id = fresh_stream_id("metadata_not_exist");
    let events = generate_events("metadata-test-not-exist".to_string(), 5);

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
async fn test_read_stream_events_non_existent(client: &Client) -> kurrent::Result<()> {
    let stream_id = fresh_stream_id("read_stream_events");

    let mut stream = client
        .read_stream(stream_id.as_str(), &Default::default())
        .await?;

    if let Err(kurrent::Error::ResourceNotFound) = stream.next().await {
        return Ok(());
    }

    panic!("We expected to have a stream not found result");
}

// We write an event into a stream then soft delete that stream.
async fn test_delete_stream(client: &Client) -> kurrent::Result<()> {
    let stream_id = fresh_stream_id("delete");
    let events = generate_events("delete-test".to_string(), 1);

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
async fn test_tombstone_stream(client: &Client) -> kurrent::Result<()> {
    let _ = pretty_env_logger::try_init();
    let stream_id = fresh_stream_id("tombstone");
    let events = generate_events("tombstone-test".to_string(), 1);

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

    if let Err(kurrent::Error::ResourceDeleted) = result {
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
    let events_before = generate_events("catchup-test-before".to_string(), 3);
    let events_after = generate_events("catchup-test-after".to_string(), 3);

    let _ = client
        .append_to_stream(stream_id.as_str(), &Default::default(), events_before)
        .await?;

    let options =
        kurrent::SubscribeToStreamOptions::default().start_from(kurrent::StreamPosition::Start);

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
        Ok(()) as kurrent::Result<()>
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

async fn test_subscription_caughtup(client: &Client) -> kurrent::Result<()> {
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
        kurrent::SubscribeToStreamOptions::default().start_from(kurrent::StreamPosition::Start);

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
        Ok(()) as kurrent::Result<()>
    });

    if tokio::time::timeout(Duration::from_secs(60), recv)
        .await
        .is_err()
    {
        panic!("test_subscription_caughtup timed out!");
    }

    Ok(())
}

async fn test_subscription_all_filter(client: &Client) -> kurrent::Result<()> {
    let filter = kurrent::SubscriptionFilter::on_event_type().exclude_system_events();
    let options = kurrent::SubscribeToAllOptions::default()
        .position(kurrent::StreamPosition::Start)
        .filter(filter);

    let mut sub = client.subscribe_to_all(&options).await;

    match tokio::time::timeout(Duration::from_secs(60), async move {
        let event = sub.next().await?;

        assert!(!event.get_original_event().event_type.starts_with('$'));

        Ok(()) as kurrent::Result<()>
    })
    .await
    {
        Ok(result) => assert!(result.is_ok()),
        Err(_) => panic!("we are supposed to receive event from that subscription"),
    };

    Ok(())
}

async fn test_batch_append(client: &Client) -> kurrent::Result<()> {
    let batch_client = client.batch_append(&Default::default()).await?;

    for _ in 0..3 {
        let stream_id = fresh_stream_id("batch-append");
        let events = generate_events("batch-append-type", 3);
        let _ = batch_client
            .append_to_stream(stream_id.as_str(), kurrent::ExpectedRevision::Any, events)
            .await?;
        let options = kurrent::ReadStreamOptions::default()
            .forwards()
            .position(kurrent::StreamPosition::Start);
        let mut stream = client.read_stream(stream_id.as_str(), &options).await?;

        let mut cpt = 0usize;

        while let Some(_) = stream.next().await? {
            cpt += 1;
        }

        assert_eq!(cpt, 3, "We expecting 3 events out of those streams");
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
        if let kurrent::Error::UnsupportedFeature = e {
            warn!("batch_append is not supported on the server we are targeting");
            Ok(())
        } else {
            Err(e)
        }?;
    }
    debug!("Complete");

    Ok(())
}
