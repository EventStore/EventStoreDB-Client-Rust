#![allow(unused_attributes)]
#![allow(unused_imports)]
#![allow(unused_results)]
#![allow(unused_variables)]

use futures::TryStreamExt;
use kurrent::{
    AppendToStreamOptions, Client, Credentials, EventData, ExpectedRevision, ReadStreamOptions,
    StreamPosition,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct TestEvent {
    pub id: String,
    pub important_data: String,
}

type Result<A> = std::result::Result<A, Box<dyn Error>>;

pub async fn append_to_stream(client: &Client) -> Result<()> {
    // region append-to-stream
    let data = TestEvent {
        id: "1".to_string(),
        important_data: "some value".to_string(),
    };

    let event = EventData::json("some-event", &data)?.id(Uuid::new_v4());
    let options = AppendToStreamOptions::default().expected_revision(ExpectedRevision::NoStream);

    let _ = client
        .append_to_stream("some-stream", &options, event)
        .await?;
    // endregion append-to-stream

    Ok(())
}

pub async fn append_with_same_id(client: &Client) -> Result<()> {
    // region append-duplicate-event
    let data = TestEvent {
        id: "1".to_string(),
        important_data: "some value".to_string(),
    };

    let event = EventData::json("some-event", &data)?.id(Uuid::new_v4());
    let options = AppendToStreamOptions::default();

    let _ = client
        .append_to_stream("same-event-stream", &options, event.clone())
        .await?;

    let _ = client
        .append_to_stream("same-event-stream", &options, event)
        .await?;
    // endregion append-duplicate-event

    Ok(())
}

pub async fn append_with_no_stream(client: &Client) -> Result<()> {
    // region append-with-no-stream
    let data = TestEvent {
        id: "1".to_string(),
        important_data: "some value".to_string(),
    };

    let event = EventData::json("some-event", &data)?.id(Uuid::new_v4());
    let options = AppendToStreamOptions::default().expected_revision(ExpectedRevision::NoStream);

    let _ = client
        .append_to_stream("same-event-stream", &options, event)
        .await?;

    let data = TestEvent {
        id: "2".to_string(),
        important_data: "some other value".to_string(),
    };

    let event = EventData::json("some-event", &data)?.id(Uuid::new_v4());

    let _ = client
        .append_to_stream("same-event-stream", &options, event)
        .await?;
    // endregion append-with-no-stream

    Ok(())
}

pub async fn append_with_concurrency_check(client: Client) -> Result<()> {
    // region append-with-concurrency-check
    let options = ReadStreamOptions::default().position(StreamPosition::End);

    let last_event = client
        .read_stream("concurrency-stream", &options)
        .await?
        .next()
        .await?
        .expect("the stream to at least exist.");

    let data = TestEvent {
        id: "1".to_string(),
        important_data: "clientOne".to_string(),
    };

    let event = EventData::json("some-event", &data)?.id(Uuid::new_v4());
    let options = AppendToStreamOptions::default().expected_revision(ExpectedRevision::Exact(
        last_event.get_original_event().revision,
    ));

    let _ = client
        .append_to_stream("concurrency-stream", &options, event)
        .await?;

    let data = TestEvent {
        id: "2".to_string(),
        important_data: "clientTwo".to_string(),
    };

    let event = EventData::json("some-event", &data)?.id(Uuid::new_v4());

    let _ = client
        .append_to_stream("concurrency-stream", &options, event)
        .await?;
    // endregion append-with-concurrency-check

    Ok(())
}

pub async fn append_overriding_user_credentials(client: &Client) -> Result<()> {
    let data = TestEvent {
        id: "1".to_string(),
        important_data: "clientOne".to_string(),
    };

    let event = EventData::json("some-event", &data)?.id(Uuid::new_v4());

    // region overriding-user-credentials
    let options =
        AppendToStreamOptions::default().authenticated(Credentials::new("admin", "changeit"));

    let _ = client
        .append_to_stream("some-stream", &options, event)
        .await?;
    // endregion overriding-user-credentials

    Ok(())
}
