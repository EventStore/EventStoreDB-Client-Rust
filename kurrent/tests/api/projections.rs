use crate::common::generate_events;
use kurrent::{Client, ProjectionClient};
use serde::Deserialize;

// This is the state of the projection, see tests/fixtures/projection.js.
#[derive(Deserialize, Debug)]
struct State {
    #[serde(rename = "foo")]
    _foo: Foo,
}

#[derive(Deserialize, Debug)]
struct Foo {
    #[serde(rename = "baz")]
    _baz: Baz,
}

#[derive(Deserialize, Debug)]
struct Baz {
    #[serde(rename = "count")]
    _count: f64,
}

static PROJECTION_FILE: &'static str = include_str!("../fixtures/projection.js");
static PROJECTION_UPDATED_FILE: &'static str = include_str!("../fixtures/projection-updated.js");

async fn wait_until_projection_status_cc(
    client: &ProjectionClient,
    name: &str,
    last_status: &mut String,
    status: &str,
) -> eyre::Result<()> {
    loop {
        let result = client.get_status(name, &Default::default()).await?;

        if let Some(stats) = result {
            if stats.status.contains(status) {
                break;
            }

            *last_status = stats.status.clone();
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    Ok(())
}

async fn wait_until_projection_status_is(
    client: &ProjectionClient,
    name: &str,
    status: &str,
) -> eyre::Result<()> {
    let mut last_status = "".to_string();

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(FIVE_MINS_IN_SECS),
        wait_until_projection_status_cc(client, name, &mut last_status, status),
    )
    .await;

    if result.is_err() {
        error!(
            "Projection {} doesn't reach the expected status. Got {}, Expected {}",
            name, last_status, status
        );
    }

    result?
}

async fn wait_until_state_ready<A>(client: &ProjectionClient, name: &str) -> eyre::Result<A>
where
    A: serde::de::DeserializeOwned + Send,
{
    tokio::time::timeout(
        std::time::Duration::from_secs(FIVE_MINS_IN_SECS),
        async move {
            loop {
                let result: serde_json::Result<A> =
                    client.get_state(name, &Default::default()).await?;

                if let Ok(a) = result {
                    return Ok::<A, eyre::Report>(a);
                }
            }
        },
    )
    .await?
}

async fn wait_until_result_ready<A>(client: &ProjectionClient, name: &str) -> eyre::Result<A>
where
    A: serde::de::DeserializeOwned + Send,
{
    tokio::time::timeout(
        std::time::Duration::from_secs(FIVE_MINS_IN_SECS),
        async move {
            loop {
                let result: serde_json::Result<A> =
                    client.get_result(name, &Default::default()).await?;

                if let Ok(a) = result {
                    return Ok::<A, eyre::Report>(a);
                }
            }
        },
    )
    .await?
}

const FIVE_MINS_IN_SECS: u64 = 5 * 60;

async fn create_projection(
    client: &ProjectionClient,
    gen_name: &mut names::Generator<'_>,
) -> eyre::Result<()> {
    let name = gen_name.next().unwrap();
    client
        .create(
            name.as_str(),
            PROJECTION_FILE.to_string(),
            &Default::default(),
        )
        .await?;

    wait_until_projection_status_is(client, name.as_str(), "Running").await
}

// TODO - A projection must be stopped to be able to delete it. But Stop projection gRPC call doesn't exist yet.
async fn delete_projection(
    client: &ProjectionClient,
    gen_name: &mut names::Generator<'_>,
) -> eyre::Result<()> {
    let name = gen_name.next().unwrap();
    client
        .create(
            name.as_str(),
            PROJECTION_FILE.to_string(),
            &Default::default(),
        )
        .await?;

    wait_until_projection_status_is(client, name.as_str(), "Running").await?;

    debug!("delete_projection: create_projection succeeded: {}", name);

    client.abort(name.as_str(), &Default::default()).await?;

    wait_until_projection_status_is(client, name.as_str(), "Aborted").await?;

    debug!("delete_projection: reading newly-created projection statistic succeeded");

    let cloned_name = name.clone();
    // There is a race-condition in the projection manager: https://github.com/EventStore/EventStore/issues/2938
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async move {
        loop {
            let result = client
                .delete(cloned_name.as_str(), &Default::default())
                .await;

            if result.is_ok() {
                break;
            }

            warn!("projection deletion failed with: {:?}. Retrying...", result);
            let _ = tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    })
    .await;

    if result.is_err() {
        warn!("projection deletion didn't complete under test timeout. Not a big deal considering https://github.com/EventStore/EventStore/issues/2938");
    }

    Ok(())
}
async fn update_projection(
    client: &ProjectionClient,
    gen_name: &mut names::Generator<'_>,
) -> eyre::Result<()> {
    let name = gen_name.next().unwrap();

    client
        .create(
            name.as_str(),
            PROJECTION_FILE.to_string(),
            &Default::default(),
        )
        .await?;

    wait_until_projection_status_is(client, name.as_str(), "Running").await?;

    client
        .update(
            name.as_str(),
            PROJECTION_UPDATED_FILE.to_string(),
            &Default::default(),
        )
        .await?;

    let stats = client
        .get_status(name.as_str(), &Default::default())
        .await?;

    assert!(stats.is_some());

    let stats = stats.unwrap();

    assert_eq!(stats.name, name);
    assert_eq!(stats.version, 1);

    Ok(())
}

async fn enable_projection(
    client: &ProjectionClient,
    gen_name: &mut names::Generator<'_>,
) -> eyre::Result<()> {
    let name = gen_name.next().unwrap();
    client
        .create(
            name.as_str(),
            PROJECTION_FILE.to_string(),
            &Default::default(),
        )
        .await?;

    wait_until_projection_status_is(client, name.as_str(), "Running").await?;

    client.enable(name.as_str(), &Default::default()).await?;

    wait_until_projection_status_is(client, name.as_str(), "Running").await?;

    Ok(())
}

async fn disable_projection(
    client: &ProjectionClient,
    gen_name: &mut names::Generator<'_>,
) -> eyre::Result<()> {
    let name = gen_name.next().unwrap();
    client
        .create(
            name.as_str(),
            PROJECTION_FILE.to_string(),
            &Default::default(),
        )
        .await?;

    wait_until_projection_status_is(client, name.as_str(), "Running").await?;
    client.enable(name.as_str(), &Default::default()).await?;
    wait_until_projection_status_is(client, name.as_str(), "Running").await?;
    client.disable(name.as_str(), &Default::default()).await?;
    wait_until_projection_status_is(client, name.as_str(), "Stopped").await?;

    Ok(())
}

async fn reset_projection(
    client: &ProjectionClient,
    gen_name: &mut names::Generator<'_>,
) -> eyre::Result<()> {
    let name = gen_name.next().unwrap();
    client
        .create(
            name.as_str(),
            PROJECTION_FILE.to_string(),
            &Default::default(),
        )
        .await?;

    wait_until_projection_status_is(client, name.as_str(), "Running").await?;
    client.enable(name.as_str(), &Default::default()).await?;
    client.reset(name.as_str(), &Default::default()).await?;

    Ok(())
}

async fn projection_state(
    stream_client: &Client,
    client: &ProjectionClient,
    gen_name: &mut names::Generator<'_>,
) -> eyre::Result<()> {
    let events = generate_events("testing", 10);
    let stream_name = gen_name.next().unwrap();

    stream_client
        .append_to_stream(stream_name, &Default::default(), events)
        .await?;

    let name = gen_name.next().unwrap();
    client
        .create(
            name.as_str(),
            PROJECTION_FILE.to_string(),
            &Default::default(),
        )
        .await?;

    wait_until_projection_status_is(client, name.as_str(), "Running").await?;
    client.enable(name.as_str(), &Default::default()).await?;

    let state = wait_until_state_ready::<State>(client, name.as_str()).await?;

    debug!("{:?}", state);

    Ok(())
}

async fn projection_result(
    stream_client: &Client,
    client: &ProjectionClient,
    gen_name: &mut names::Generator<'_>,
) -> eyre::Result<()> {
    let events = generate_events("testing", 10);
    let stream_name = gen_name.next().unwrap();

    stream_client
        .append_to_stream(stream_name, &Default::default(), events)
        .await?;

    let name = gen_name.next().unwrap();
    client
        .create(
            name.as_str(),
            PROJECTION_FILE.to_string(),
            &Default::default(),
        )
        .await?;

    wait_until_projection_status_is(client, name.as_str(), "Running").await?;
    client.enable(name.as_str(), &Default::default()).await?;

    let result = wait_until_result_ready::<State>(client, name.as_str()).await?;

    debug!("{:?}", result);

    Ok(())
}

pub async fn tests(client: Client) -> eyre::Result<()> {
    let mut name_gen = names::Generator::default();
    let stream_client = client.clone();
    let client: ProjectionClient = client.into();

    debug!("before create_projection...");
    create_projection(&client, &mut name_gen).await?;
    debug!("passed");
    debug!("before delete_projection...");
    delete_projection(&client, &mut name_gen).await?;
    debug!("passed");
    debug!("before update_projection...");
    update_projection(&client, &mut name_gen).await?;
    debug!("passed");
    debug!("before enable_projection...");
    enable_projection(&client, &mut name_gen).await?;
    debug!("passed");
    debug!("before disable_projection...");
    disable_projection(&client, &mut name_gen).await?;
    debug!("passed");
    debug!("before reset_projection...");
    reset_projection(&client, &mut name_gen).await?;
    debug!("passed");
    debug!("before projection_state...");
    projection_state(&stream_client, &client, &mut name_gen).await?;
    debug!("passed");
    debug!("before projection_result...");
    projection_result(&stream_client, &client, &mut name_gen).await?;
    debug!("passed");

    Ok(())
}
