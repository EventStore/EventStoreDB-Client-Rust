#![allow(unused_attributes)]
#![allow(unused_imports)]
#![allow(unused_results)]
#![allow(unused_variables)]
#![allow(unused_must_use)]

use kurrent::{
    AppendToStreamOptions, Client, Credentials, EventData, ExpectedRevision, ReadStreamOptions,
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

pub async fn run() -> Result<()> {
    // region createClient
    let settings = "{connectionString}".parse()?;
    let client = Client::new(settings)?;
    // endregion createClient

    // region createEvent
    let event = TestEvent {
        id: Uuid::new_v4().to_string(),
        important_data: "I wrote my first event!".to_string(),
    };

    let event_data = EventData::json("TestEvent", &event)?.id(Uuid::new_v4());
    // endregion createEvent

    let main_event_data = event_data.clone();

    // region appendEvents
    client
        .append_to_stream("some-stream", &Default::default(), event_data)
        .await?;
    // endregion appendEvents

    let event_data = main_event_data.clone();
    let options =
        AppendToStreamOptions::default().authenticated(Credentials::new("admin", "changeit"));

    // region overriding-user-credentials
    client
        .append_to_stream("some-stream", &options, event_data)
        .await?;
    // endregion overriding-user-credentials

    // region readStream
    let options = ReadStreamOptions::default().max_count(10);
    let mut stream = client.read_stream("some-stream", &options).await?;

    while let Some(event) = stream.next().await? {
        // Doing something productive with the events.
    }
    // endregion readStream

    Ok(())
}
