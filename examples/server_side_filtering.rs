#![allow(unused_attributes)]
#![allow(unused_imports)]
#![allow(unused_results)]
#![allow(unused_variables)]
#![allow(unreachable_code)]

use futures::TryStreamExt;
use kurrent::{
    Client, Credentials, EventData, ExpectedRevision, SubscribeToAllOptions, SubscriptionEvent,
    SubscriptionFilter,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
struct TestEvent {
    pub id: String,
    pub important_data: String,
}

type Result<A> = std::result::Result<A, Box<dyn Error>>;

pub async fn exclude_system_events(client: &Client) -> Result<()> {
    // region exclude-system
    let filter = SubscriptionFilter::on_event_type().exclude_system_events();
    let options = SubscribeToAllOptions::default().filter(filter);

    let mut sub = client.subscribe_to_all(&options).await;

    loop {
        let event = sub.next().await?;
        let stream_id = event.get_original_stream_id();
        let revision = event.get_original_event().revision;

        println!("Received event {}@{}", revision, stream_id);
    }
    // endregion exclude-system

    Ok(())
}

pub async fn event_type_prefix(client: &Client) -> Result<()> {
    // region event-type-prefix
    let filter = SubscriptionFilter::on_event_type().add_prefix("customer-");
    let options = SubscribeToAllOptions::default().filter(filter);

    let mut sub = client.subscribe_to_all(&options).await;
    // endregion event-type-prefix

    loop {
        let event = sub.next().await?;
        let stream_id = event.get_original_stream_id();
        let revision = event.get_original_event().revision;

        println!("Received event {}@{}", revision, stream_id);
    }

    Ok(())
}

pub async fn event_type_regex(client: &Client) -> Result<()> {
    // region event-type-regex
    let filter = SubscriptionFilter::on_event_type().regex("^user|^company");
    let options = SubscribeToAllOptions::default().filter(filter);

    let mut sub = client.subscribe_to_all(&options).await;
    // endregion event-type-regex

    loop {
        let event = sub.next().await?;
        let stream_id = event.get_original_stream_id();
        let revision = event.get_original_event().revision;

        println!("Received event {}@{}", revision, stream_id);
    }

    Ok(())
}

pub async fn stream_prefix(client: &Client) -> Result<()> {
    // region stream-prefix
    let filter = SubscriptionFilter::on_stream_name().add_prefix("user-");
    let options = SubscribeToAllOptions::default().filter(filter);

    let mut sub = client.subscribe_to_all(&options).await;
    // endregion stream-prefix

    loop {
        let event = sub.next().await?;
        let stream_id = event.get_original_stream_id();
        let revision = event.get_original_event().revision;

        println!("Received event {}@{}", revision, stream_id);
    }

    Ok(())
}

pub async fn stream_regex(client: &Client) -> Result<()> {
    // region stream-regex
    let filter = SubscriptionFilter::on_event_type().regex("/^[^\\$].*/");
    let options = SubscribeToAllOptions::default().filter(filter);

    let mut sub = client.subscribe_to_all(&options).await;
    // endregion stream-regex

    loop {
        let event = sub.next().await?;
        let stream_id = event.get_original_stream_id();
        let revision = event.get_original_event().revision;

        println!("Received event {}@{}", revision, stream_id);
    }

    Ok(())
}

pub async fn checkpoint_callback_with_interval(client: &Client) -> Result<()> {
    // region checkpoint-with-interval
    let filter = SubscriptionFilter::on_event_type().regex("/^[^\\$].*/");
    let options = SubscribeToAllOptions::default().filter(filter);

    let mut sub = client.subscribe_to_all(&options).await;
    // endregion checkpoint-with-interval

    // region checkpoint
    loop {
        let event = sub.next_subscription_event().await?;
        match event {
            SubscriptionEvent::EventAppeared(event) => {
                let stream_id = event.get_original_stream_id();
                let revision = event.get_original_event().revision;

                println!("Received event {}@{}", revision, stream_id);
            }

            SubscriptionEvent::Checkpoint(position) => {
                println!("checkpoint taken at {}", position.prepare);
            }

            _ => {}
        }
    }
    // endregion checkpoint

    Ok(())
}
