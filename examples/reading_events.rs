#![allow(unused_attributes)]
#![allow(unused_imports)]
#![allow(unused_results)]
#![allow(unused_variables)]

use kurrent::{
    Client, Credentials, EventData, ExpectedRevision, Position, ReadAllOptions, ReadStreamOptions,
    StreamPosition,
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

pub async fn read_from_stream(client: &Client) -> Result<()> {
    // region read-from-stream
    let options = ReadStreamOptions::default()
        .position(StreamPosition::Start)
        .forwards();
    let mut stream = client.read_stream("some-stream", &options).await?;
    // endregion read-from-stream

    // region iterate-stream
    while let Some(event) = stream.next().await? {
        let test_event = event.get_original_event().as_json::<TestEvent>()?;

        println!("Event> {:?}", test_event);
    }
    // endregion iterate-stream

    Ok(())
}

pub async fn read_from_stream_position(client: &Client) -> Result<()> {
    // region read-from-stream-position
    let options = ReadStreamOptions::default()
        .position(StreamPosition::Position(10))
        .max_count(20);
    let mut stream = client.read_stream("some-stream", &options).await?;
    // endregion read-from-stream-position

    // region iterate-stream
    while let Some(event) = stream.next().await? {
        let test_event = event.get_original_event().as_json::<TestEvent>()?;

        println!("Event> {:?}", test_event);
    }
    // endregion iterate-stream

    Ok(())
}

pub async fn read_stream_overriding_user_credentials(client: &Client) -> Result<()> {
    // region overriding-user-credentials
    let options = ReadStreamOptions::default()
        .position(StreamPosition::Start)
        .authenticated(Credentials::new("admin", "changeit"));

    let stream = client.read_stream("some-stream", &options).await;
    // endregion overriding-user-credentials
    Ok(())
}

pub async fn read_from_stream_position_check(client: &Client) -> Result<()> {
    // region checking-for-stream-presence
    let options = ReadStreamOptions::default().position(StreamPosition::Position(10));

    let mut stream = client.read_stream("some-stream", &options).await?;

    while let Some(event) = stream.next().await? {
        let test_event = event.get_original_event().as_json::<TestEvent>()?;

        println!("Event> {:?}", test_event);
    }
    // endregion checking-for-stream-presence
    Ok(())
}

pub async fn read_stream_backwards(client: &Client) -> Result<()> {
    // region reading-backwards
    let options = ReadStreamOptions::default()
        .position(StreamPosition::End)
        .backwards();
    let mut stream = client.read_stream("some-stream", &options).await?;

    while let Some(event) = stream.next().await? {
        let test_event = event.get_original_event().as_json::<TestEvent>()?;

        println!("Event> {:?}", test_event);
    }
    // endregion reading-backwards

    Ok(())
}

pub async fn read_from_all_stream(client: &Client) -> Result<()> {
    // region read-from-all-stream
    let options = ReadAllOptions::default()
        .position(StreamPosition::Start)
        .forwards();
    let mut stream = client.read_all(&Default::default()).await?;
    // endregion read-from-all-stream

    // region read-from-all-stream-iterate
    while let Some(event) = stream.next().await? {
        println!("Event> {:?}", event.get_original_event());
    }
    // endregion read-from-all-stream-iterate

    Ok(())
}

pub async fn read_all_overriding_user_credentials(client: &Client) -> Result<()> {
    // region read-all-overriding-user-credentials
    let options = ReadAllOptions::default()
        .authenticated(Credentials::new("admin", "changeit"))
        .position(StreamPosition::Position(Position {
            commit: 1_110,
            prepare: 1_110,
        }));
    let stream = client.read_all(&options).await;
    // endregion read-all-overriding-user-credentials

    Ok(())
}

pub async fn ignore_system_events(client: &Client) -> Result<()> {
    // region ignore-system-events
    let mut stream = client.read_all(&Default::default()).await?;

    while let Some(event) = stream.next().await? {
        if event.get_original_event().event_type.starts_with("$") {
            continue;
        }

        println!("Event> {:?}", event.get_original_event());
    }
    // endregion ignore-system-events

    Ok(())
}

pub async fn read_from_all_stream_backwards(client: &Client) -> Result<()> {
    // region read-from-all-stream-backwards
    let options = ReadAllOptions::default().position(StreamPosition::End);

    let mut stream = client.read_all(&options).await?;
    // endregion read-from-all-stream-backwards

    // region read-from-all-stream-iterate
    while let Some(event) = stream.next().await? {
        println!("Event> {:?}", event.get_original_event());
    }
    // endregion read-from-all-stream-iterate

    Ok(())
}

pub async fn filtering_out_system_events(client: &Client) -> Result<()> {
    let mut stream = client.read_all(&Default::default()).await?;

    while let Some(event) = stream.next().await? {
        if !event.get_original_event().event_type.starts_with("$") {
            continue;
        }

        println!("Event> {:?}", event.get_original_event());
    }
    Ok(())
}

pub async fn read_from_stream_resolving_link_tos(client: &Client) -> Result<()> {
    // region read-from-all-stream-resolving-link-Tos
    let options = ReadAllOptions::default().resolve_link_tos();
    client.read_all(&options).await?;
    // endregion read-from-all-stream-resolving-link-Tos
    Ok(())
}
