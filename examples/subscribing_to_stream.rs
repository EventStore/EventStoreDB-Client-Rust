#![allow(unused_attributes)]
#![allow(unused_imports)]
#![allow(unused_results)]
#![allow(unused_variables)]
#![allow(unused_variables)]
#![allow(unreachable_code)]

use futures::TryStreamExt;
use kurrent::{
    Client, Credentials, EventData, ExpectedRevision, Position, RetryOptions, StreamPosition,
    SubscribeToAllOptions, SubscribeToStreamOptions, SubscriptionEvent, SubscriptionFilter,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use uuid::Uuid;

type Result<A> = std::result::Result<A, Box<dyn Error>>;

pub async fn subscribe_to_stream(client: &Client) -> Result<()> {
    // region subscribe-to-stream
    let mut stream = client
        .subscribe_to_stream("some-stream", &Default::default())
        .await;

    loop {
        let event = stream.next().await?;
        // Handles the event...
    }
    // endregion subscribe-to-stream

    // region subscribe-to-stream-from-position
    let options = SubscribeToStreamOptions::default().start_from(StreamPosition::Position(20));

    client.subscribe_to_stream("some-stream", &options).await;
    // endregion subscribe-to-stream-from-position

    // region subscribe-to-stream-live
    let options = SubscribeToStreamOptions::default().start_from(StreamPosition::End);
    client.subscribe_to_stream("some-stream", &options).await;
    // endregion subscribe-to-stream-live

    // region subscribe-to-stream-resolving-linktos
    let options = SubscribeToStreamOptions::default()
        .start_from(StreamPosition::Start)
        .resolve_link_tos();

    client
        .subscribe_to_stream("$et-myEventType", &options)
        .await;
    // endregion subscribe-to-stream-resolving-linktos

    // region subscribe-to-stream-subscription-dropped
    let retry = RetryOptions::default().retry_forever();
    let options = SubscribeToStreamOptions::default().retry_options(retry);
    let mut stream = client.subscribe_to_stream("some-stream", &options).await;

    loop {
        let event = stream.next().await?;
        // Handles the event...
    }
    // endregion subscribe-to-stream-subscription-dropped

    Ok(())
}

pub async fn subscribe_to_all(client: &Client) -> Result<()> {
    // region subscribe-to-all
    let mut stream = client.subscribe_to_all(&Default::default()).await;

    loop {
        let event = stream.next().await?;
        // Handles the event...
    }

    // endregion subscribe-to-all

    // region subscribe-to-all-from-position
    let options = SubscribeToAllOptions::default().position(StreamPosition::Position(Position {
        commit: 1_056,
        prepare: 1_056,
    }));

    client.subscribe_to_all(&options).await;
    // endregion subscribe-to-all-from-position

    // region subscribe-to-all-live
    let options = SubscribeToAllOptions::default().position(StreamPosition::End);
    client.subscribe_to_all(&options).await;
    // endregion subscribe-to-all-live

    // region subscribe-to-all-subscription-dropped
    let retry = RetryOptions::default().retry_forever();
    let options = SubscribeToAllOptions::default().retry_options(retry);
    let mut stream = client.subscribe_to_all(&options).await;

    loop {
        let event = stream.next().await?;
        // Handles the event...
    }

    // endregion subscribe-to-all-subscription-dropped

    Ok(())
}

pub async fn subscribe_to_all_filtered(client: &Client) -> Result<()> {
    // region stream-prefix-filtered-subscription
    let filter = SubscriptionFilter::on_stream_name().add_prefix("test-");
    let options = SubscribeToAllOptions::default().filter(filter);

    client.subscribe_to_all(&options).await;
    // endregion stream-prefix-filtered-subscription

    // region stream-regex-filtered-subscription
    let filter = SubscriptionFilter::on_stream_name().regex("/invoice-\\d\\d\\d/g");
    // endregion stream-regex-filtered-subscription

    Ok(())
}

pub async fn overriding_user_credentials(client: &Client) -> Result<()> {
    // region overriding-user-credentials
    let options =
        SubscribeToAllOptions::default().authenticated(Credentials::new("admin", "changeit"));
    client.subscribe_to_all(&options).await;
    // endregion overriding-user-credentials

    Ok(())
}
