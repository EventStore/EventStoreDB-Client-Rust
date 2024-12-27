use crate::common::{fresh_stream_id, generate_events};
use kurrent::{Client, StreamPosition};
use std::time::Duration;

async fn test_create_persistent_subscription(client: &Client) -> kurrent::Result<()> {
    let stream_id = fresh_stream_id("create_persistent_sub");

    let options =
        kurrent::PersistentSubscriptionOptions::default().deadline(Duration::from_secs(2));
    client
        .create_persistent_subscription(stream_id, "a_group_name", &options)
        .await?;

    Ok(())
}

async fn test_create_persistent_subscription_to_all(
    client: &Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    let group_name = names.next().unwrap();
    let options =
        kurrent::PersistentSubscriptionToAllOptions::default().deadline(Duration::from_secs(2));

    client
        .create_persistent_subscription_to_all(group_name, &options)
        .await?;

    Ok(())
}

// We test we can successfully update a persistent subscription.
async fn test_update_persistent_subscription(client: &Client) -> kurrent::Result<()> {
    let stream_id = fresh_stream_id("update_persistent_sub");

    let mut options =
        kurrent::PersistentSubscriptionOptions::default().deadline(Duration::from_secs(2));

    client
        .create_persistent_subscription(stream_id.as_str(), "a_group_name", &options)
        .await?;

    options.settings_mut().max_retry_count = 1_000;

    client
        .update_persistent_subscription(stream_id, "a_group_name", &options)
        .await?;

    Ok(())
}

async fn test_update_persistent_subscription_to_all(
    client: &Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    let group_name = names.next().unwrap();

    let mut options =
        kurrent::PersistentSubscriptionToAllOptions::default().deadline(Duration::from_secs(2));

    client
        .create_persistent_subscription_to_all(group_name.as_str(), &options)
        .await?;

    options.settings_mut().max_retry_count = 1_000;

    client
        .update_persistent_subscription_to_all(group_name, &options)
        .await?;

    Ok(())
}

// We test we can successfully delete a persistent subscription.
async fn test_delete_persistent_subscription(client: &Client) -> kurrent::Result<()> {
    let stream_id = fresh_stream_id("delete_persistent_sub");
    let options =
        kurrent::PersistentSubscriptionOptions::default().deadline(Duration::from_secs(2));

    client
        .create_persistent_subscription(stream_id.as_str(), "a_group_name", &options)
        .await?;

    let options =
        kurrent::DeletePersistentSubscriptionOptions::default().deadline(Duration::from_secs(2));

    client
        .delete_persistent_subscription(stream_id, "a_group_name", &options)
        .await?;

    Ok(())
}

async fn test_delete_persistent_subscription_to_all(
    client: &Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    let group_name = names.next().unwrap();
    let options =
        kurrent::PersistentSubscriptionToAllOptions::default().deadline(Duration::from_secs(2));

    client
        .create_persistent_subscription_to_all(group_name.as_str(), &options)
        .await?;

    let options =
        kurrent::DeletePersistentSubscriptionOptions::default().deadline(Duration::from_secs(2));

    client
        .delete_persistent_subscription_to_all(group_name.as_str(), &options)
        .await?;

    Ok(())
}

async fn test_persistent_subscription(client: &Client) -> kurrent::Result<()> {
    let stream_id = fresh_stream_id("persistent_subscription");
    let events = generate_events("persistent-subscription-test".to_string(), 5);

    client
        .create_persistent_subscription(stream_id.as_str(), "a_group_name", &Default::default())
        .await?;

    let _ = client
        .append_to_stream(stream_id.as_str(), &Default::default(), events)
        .await?;

    let mut sub = client
        .subscribe_to_persistent_subscription(
            stream_id.as_str(),
            "a_group_name",
            &Default::default(),
        )
        .await?;

    let max = 10usize;

    let handle = tokio::spawn(async move {
        let mut count = 0usize;
        loop {
            let event = sub.next().await?;
            sub.ack(event).await?;

            count += 1;

            if count == max {
                break;
            }
        }

        Ok::<usize, kurrent::Error>(count)
    });

    let events = generate_events("es6-persistent-subscription-test".to_string(), 5);
    let _ = client
        .append_to_stream(stream_id.as_str(), &Default::default(), events)
        .await?;

    let count = handle
        .await
        .map_err(|_| {
            kurrent::Error::IllegalStateError(
                "Error when joining the tokio thread handle".to_string(),
            )
        })
        .and_then(|r| r)?;

    assert_eq!(
        count, 10,
        "We are testing proper state after persistent subscription: got {} expected {}",
        count, 10
    );

    Ok(())
}

async fn test_persistent_subscription_to_all(
    client: &Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    let group_name = names.next().unwrap();

    let options = kurrent::PersistentSubscriptionToAllOptions::default()
        .start_from(kurrent::StreamPosition::Start);

    client
        .create_persistent_subscription_to_all(group_name.as_str(), &options)
        .await?;

    let mut sub = client
        .subscribe_to_persistent_subscription_to_all(group_name, &Default::default())
        .await?;

    let limit = 3;
    let outcome = tokio::time::timeout(Duration::from_secs(60), async move {
        let mut count = 0;
        for _ in 0..limit + 1 {
            let event = sub.next().await?;
            sub.ack(event).await?;

            count += 1;

            if count == limit {
                break;
            }
        }

        Ok::<_, kurrent::Error>(count)
    })
    .await;

    match outcome {
        Ok(count) => {
            assert_eq!(count?, limit)
        }
        Err(_) => panic!("persistent subscription to $all test timed out!"),
    };

    Ok(())
}

async fn test_list_persistent_subscriptions(
    client: &Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    let mut count = 0;
    let group_name = "group_name";
    let mut expected_set = std::collections::HashSet::new();

    while let Some(stream_name) = names.next() {
        count += 1;
        client
            .create_persistent_subscription(stream_name.as_str(), &group_name, &Default::default())
            .await?;

        expected_set.insert(stream_name);
        if count > 2 {
            break;
        }
    }

    let mut actual_set = std::collections::HashSet::new();

    let ps = client
        .list_all_persistent_subscriptions(&Default::default())
        .await?;

    for info in ps {
        if expected_set.contains(info.event_source.as_str()) {
            actual_set.insert(info.event_source);
        }
    }

    assert_eq!(expected_set, actual_set);

    Ok(())
}

async fn test_list_persistent_subscriptions_for_stream(
    client: &Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    let mut count = 0;
    let stream_name = names.next().unwrap();
    let mut expected_set = std::collections::HashSet::new();

    while let Some(group_name) = names.next() {
        count += 1;
        client
            .create_persistent_subscription(
                stream_name.as_str(),
                group_name.as_str(),
                &Default::default(),
            )
            .await?;

        expected_set.insert(group_name);
        if count > 2 {
            break;
        }
    }

    let mut actual_set = std::collections::HashSet::new();

    let ps = client
        .list_persistent_subscriptions_for_stream(stream_name.as_str(), &Default::default())
        .await?;

    for info in ps {
        if info.event_source != stream_name {
            continue;
        }

        if expected_set.contains(info.group_name.as_str()) {
            actual_set.insert(info.group_name);
        }
    }

    assert_eq!(expected_set, actual_set);

    Ok(())
}

async fn test_list_persistent_subscriptions_to_all(
    client: &Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    let mut count = 0;
    let mut expected_set = std::collections::HashSet::new();

    while let Some(group_name) = names.next() {
        count += 1;
        client
            .create_persistent_subscription_to_all(group_name.as_str(), &Default::default())
            .await?;

        expected_set.insert(group_name);
        if count > 2 {
            break;
        }
    }

    let mut actual_set = std::collections::HashSet::new();

    let ps = client
        .list_persistent_subscriptions_to_all(&Default::default())
        .await?;

    for info in ps {
        if expected_set.contains(info.group_name.as_str()) {
            actual_set.insert(info.group_name);
        }
    }

    assert_eq!(expected_set, actual_set);

    Ok(())
}

async fn test_get_persistent_subscription_info(
    client: &Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    let stream_name = names.next().unwrap();
    let group_name = names.next().unwrap();

    client
        .create_persistent_subscription(
            stream_name.as_str(),
            group_name.as_str(),
            &Default::default(),
        )
        .await?;

    let info = client
        .get_persistent_subscription_info(
            stream_name.as_str(),
            group_name.as_str(),
            &Default::default(),
        )
        .await?;

    assert_eq!(info.event_source, stream_name);
    assert_eq!(info.group_name, group_name);

    if let Some(setts) = info.settings {
        assert_eq!(setts.start_from, StreamPosition::End);
    }

    Ok(())
}

async fn test_get_persistent_subscription_info_to_all(
    client: &Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    let group_name = names.next().unwrap();

    client
        .create_persistent_subscription_to_all(group_name.as_str(), &Default::default())
        .await?;

    let info = client
        .get_persistent_subscription_info_to_all(group_name.as_str(), &Default::default())
        .await?;

    assert_eq!(info.group_name, group_name);

    if let Some(setts) = info.settings {
        assert_eq!(setts.start_from, StreamPosition::End);
    }

    Ok(())
}

async fn test_replay_parked_messages(
    client: &Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    let stream_name = names.next().unwrap();
    let group_name = names.next().unwrap();
    let event_count = 2;

    client
        .create_persistent_subscription(
            stream_name.as_str(),
            group_name.as_str(),
            &Default::default(),
        )
        .await?;

    let mut sub = client
        .subscribe_to_persistent_subscription(
            stream_name.as_str(),
            group_name.as_str(),
            &Default::default(),
        )
        .await?;

    let events = generate_events("foobar", 2);

    client
        .append_to_stream(stream_name.as_str(), &Default::default(), events)
        .await?;

    let outcome = tokio::time::timeout(Duration::from_secs(30), async move {
        for _ in 0..event_count {
            let event = sub.next().await?;

            sub.nack(event, kurrent::NakAction::Park, "because reasons")
                .await?;
        }

        debug!("We let the server the time to write those parked event in the stream...");
        tokio::time::sleep(Duration::from_secs(5)).await;
        debug!("done.");

        debug!("Before replaying parked messages...");
        client
            .replay_parked_messages(stream_name, group_name, &Default::default())
            .await?;
        debug!("done.");

        for _ in 0..event_count {
            debug!("Waiting on parked event to be replayed...");
            let event = sub.next().await?;
            debug!("done.");
            sub.ack(event).await?;
        }

        Ok::<(), kurrent::Error>(())
    })
    .await;

    match outcome {
        Err(_) => panic!("test_replay_parked_messages timed out!"),
        Ok(outcome) => {
            assert!(outcome.is_ok());

            Ok(())
        }
    }
}

async fn test_replay_parked_messages_to_all(
    client: &Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    let stream_name = names.next().unwrap();
    let group_name = names.next().unwrap();
    let event_count = 2;

    client
        .create_persistent_subscription_to_all(group_name.as_str(), &Default::default())
        .await?;

    let mut sub = client
        .subscribe_to_persistent_subscription_to_all(group_name.as_str(), &Default::default())
        .await?;

    let events = generate_events("foobar", 2);

    client
        .append_to_stream(stream_name.as_str(), &Default::default(), events)
        .await?;

    let outcome = tokio::time::timeout(Duration::from_secs(30), async move {
        let mut count = 0;
        loop {
            let event = sub.next().await?;

            if event.get_original_stream_id() == stream_name.as_str() {
                sub.nack(event, kurrent::NakAction::Park, "because reasons")
                    .await?;

                count += 1;
                if count == event_count {
                    break;
                }
            } else {
                sub.ack(event).await?;
            }
        }

        debug!("We let the server the time to write those parked event in the stream...");
        tokio::time::sleep(Duration::from_secs(5)).await;
        debug!("done.");

        debug!("Before replaying parked messages...");
        client
            .replay_parked_messages_to_all(group_name, &Default::default())
            .await?;
        debug!("done.");

        count = 0;
        loop {
            debug!("Waiting on parked event to be replayed...");
            let event = sub.next().await?;
            debug!("done.");

            let event_stream_id = event.event.as_ref().unwrap().stream_id.to_string();
            sub.ack(event).await?;

            if event_stream_id == stream_name.as_str() {
                count += 1;
                if count == event_count {
                    break;
                }
            }
        }

        Ok::<(), kurrent::Error>(())
    })
    .await;

    match outcome {
        Err(_) => panic!("test_replay_parked_messages_to_all timed out!"),
        Ok(outcome) => {
            assert!(outcome.is_ok());

            Ok(())
        }
    }
}

async fn test_restart_persistent_subscription_subsystem(client: &Client) -> kurrent::Result<()> {
    client
        .restart_persistent_subscription_subsystem(&Default::default())
        .await
}

async fn test_persistent_subscription_encoding(
    client: &Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    let stream_name = format!("/{}/foo", names.next().unwrap());
    let group_name = format!("/{}/foo", names.next().unwrap());

    debug!(
        "encoding - before create_persistent_subscription: '{}' :: '{}'",
        stream_name, group_name
    );
    client
        .create_persistent_subscription(
            stream_name.as_str(),
            group_name.as_str(),
            &Default::default(),
        )
        .await?;
    debug!("encoding - after create_persistent_subscription");

    debug!("encoding - before get_persistent_subscription_info");
    let info = client
        .get_persistent_subscription_info(
            stream_name.as_str(),
            group_name.as_str(),
            &Default::default(),
        )
        .await?;

    debug!("encoding - after get_persistent_subscription_info");
    assert_eq!(info.event_source, stream_name);
    assert_eq!(info.group_name, group_name);

    Ok(())
}

async fn test_persistent_subscription_info_with_connection_details(
    client: &Client,
    names: &mut names::Generator<'_>,
) -> kurrent::Result<()> {
    let stream_name = names.next().unwrap();
    let group_name = names.next().unwrap();

    client
        .create_persistent_subscription(
            stream_name.as_str(),
            group_name.as_str(),
            &Default::default(),
        )
        .await?;

    let mut sub = client
        .subscribe_to_persistent_subscription(
            stream_name.as_str(),
            group_name.as_str(),
            &Default::default(),
        )
        .await?;

    let client2 = client.clone();
    let stream_name_2 = stream_name.clone();
    let append_handle: tokio::task::JoinHandle<kurrent::Result<()>> = tokio::spawn(async move {
        loop {
            let event = generate_events("foobar", 1);
            let _ = client2
                .append_to_stream(stream_name_2.as_str(), &Default::default(), event)
                .await?;

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });

    let ack_handle: tokio::task::JoinHandle<kurrent::Result<()>> = tokio::spawn(async move {
        loop {
            let event = sub.next().await?;
            sub.ack(event).await?;
        }
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let info = client
        .get_persistent_subscription_info(
            stream_name.as_str(),
            group_name.as_str(),
            &Default::default(),
        )
        .await?;

    assert_eq!(info.event_source, stream_name);
    assert_eq!(info.group_name, group_name);
    assert_eq!(info.connections.is_empty(), false);

    append_handle.abort();
    ack_handle.abort();

    Ok(())
}

pub async fn tests(client: Client) -> eyre::Result<()> {
    let mut name_generator = names::Generator::default();

    debug!("Before test_create_persistent_subscription…");
    test_create_persistent_subscription(&client).await?;
    debug!("Complete");
    debug!("Before test_update_persistent_subscription…");
    test_update_persistent_subscription(&client).await?;
    debug!("Complete");
    debug!("Before test_create_persistent_subscription_to_all");
    if let Err(e) = test_create_persistent_subscription_to_all(&client, &mut name_generator).await {
        if let kurrent::Error::UnsupportedFeature = e {
            warn!(
                "Persistent subscription to $all is not supported on the server we are targeting"
            );
            Ok(())
        } else {
            Err(e)
        }?;
    }
    debug!("Complete");
    debug!("Before test_update_persistent_subscription_to_all");
    if let Err(e) = test_update_persistent_subscription_to_all(&client, &mut name_generator).await {
        if let kurrent::Error::UnsupportedFeature = e {
            warn!(
                "Persistent subscription to $all is not supported on the server we are targeting"
            );
            Ok(())
        } else {
            Err(e)
        }?;
    }
    debug!("Complete");
    debug!("Before test_delete_persistent_subscription…");
    test_delete_persistent_subscription(&client).await?;
    debug!("Complete");
    debug!("Before test_delete_persistent_subscription_to_all");
    if let Err(e) = test_delete_persistent_subscription_to_all(&client, &mut name_generator).await {
        if let kurrent::Error::UnsupportedFeature = e {
            warn!(
                "Persistent subscription to $all is not supported on the server we are targeting"
            );
            Ok(())
        } else {
            Err(e)
        }?;
    }
    debug!("Complete");
    debug!("Before test_persistent_subscription…");
    test_persistent_subscription(&client).await?;
    debug!("Complete");
    debug!("Before test_persistent_subscription_to_all");
    if let Err(e) = test_persistent_subscription_to_all(&client, &mut name_generator).await {
        if let kurrent::Error::UnsupportedFeature = e {
            warn!(
                "Persistent subscription to $all is not supported on the server we are targeting"
            );
            Ok(())
        } else {
            Err(e)
        }?;
    }
    debug!("Complete");
    debug!("Before test_list_persistent_subscriptions...");
    test_list_persistent_subscriptions(&client, &mut name_generator).await?;
    debug!("Complete");
    debug!("Before test_list_persistent_subscriptions_for_stream...");
    test_list_persistent_subscriptions_for_stream(&client, &mut name_generator).await?;
    debug!("Complete");
    debug!("Before test_list_persistent_subscriptions_to_all...");
    if let Err(e) = test_list_persistent_subscriptions_to_all(&client, &mut name_generator).await {
        if let kurrent::Error::UnsupportedFeature = e {
            warn!(
                "Persistent subscription to $all is not supported on the server we are targeting"
            );
            Ok(())
        } else {
            Err(e)
        }?;
    }
    debug!("Complete");
    debug!("Before test_get_persistent_subscription_info...");
    test_get_persistent_subscription_info(&client, &mut name_generator).await?;
    debug!("Complete");
    debug!("Before test_get_persistent_subscription_info_to_all...");
    if let Err(e) = test_get_persistent_subscription_info_to_all(&client, &mut name_generator).await
    {
        if let kurrent::Error::UnsupportedFeature = e {
            warn!(
                "Persistent subscription to $all is not supported on the server we are targeting"
            );
            Ok(())
        } else {
            Err(e)
        }?;
    }
    debug!("Complete");
    debug!("Before test_replay_parked_messages...");
    test_replay_parked_messages(&client, &mut name_generator).await?;
    debug!("Complete");
    debug!("Before test_replay_parked_messages_to_all...");
    if let Err(e) = test_replay_parked_messages_to_all(&client, &mut name_generator).await {
        if let kurrent::Error::UnsupportedFeature = e {
            warn!(
                "Persistent subscription to $all is not supported on the server we are targeting"
            );
            Ok(())
        } else {
            Err(e)
        }?;
    }
    debug!("Complete");
    debug!("Before test_persistent_subscription_encoding...");
    test_persistent_subscription_encoding(&client, &mut name_generator).await?;
    debug!("Complete");
    debug!("Before test_persistent_subscription_info_with_connection_details...");
    test_persistent_subscription_info_with_connection_details(&client, &mut name_generator).await?;
    debug!("Complete");
    debug!("Before test_restart_persistent_subscription_subsystem...");
    test_restart_persistent_subscription_subsystem(&client).await?;
    debug!("Complete");

    Ok(())
}
