#[macro_use]
extern crate log;

mod api;
mod common;
mod images;

use crate::common::{fresh_stream_id, generate_events};
use futures::channel::oneshot;
use kurrent::{Client, ClientSettings};
use std::time::Duration;
use testcontainers::clients::Cli;
use testcontainers::core::RunnableImage;

type VolumeName = String;

fn create_unique_volume() -> eyre::Result<VolumeName> {
    let dir_name = uuid::Uuid::new_v4();
    let dir_name = format!("dir-{}", dir_name);

    std::process::Command::new("docker")
        .arg("volume")
        .arg("create")
        .arg(format!("--name {}", dir_name))
        .output()?;

    Ok(dir_name)
}

async fn wait_node_is_alive(setts: &kurrent::ClientSettings, port: u16) -> eyre::Result<()> {
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()?;

    let protocol = if setts.is_secure_mode_enabled() {
        "https"
    } else {
        "http"
    };

    match tokio::time::timeout(std::time::Duration::from_secs(60), async move {
        loop {
            match tokio::time::timeout(
                std::time::Duration::from_secs(1),
                client
                    .get(format!("{}://localhost:{}/health/live", protocol, port))
                    .send(),
            )
            .await
            {
                Err(_) => error!("Healthcheck timed out! retrying..."),

                Ok(resp) => match resp {
                    Err(e) => error!("Node localhost:{} is not up yet: {}", port, e),

                    Ok(resp) => {
                        if resp.status().is_success() {
                            break;
                        }

                        error!(
                            "Healthcheck response was not successful: {}, retrying...",
                            resp.status()
                        );
                    }
                },
            }

            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    })
    .await
    {
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "Docker container took too much time to start",
        )
        .into()),
        Ok(_) => {
            debug!(
                "Docker container was started successfully on localhost:{}",
                port
            );

            Ok(())
        }
    }
}

// This function assumes that we are using the admin credentials. It's possible during CI that
// the cluster hasn't created the admin user yet, leading to failing the tests.
async fn wait_for_admin_to_be_available(client: &Client) -> kurrent::Result<()> {
    fn can_retry(e: &kurrent::Error) -> bool {
        match e {
            kurrent::Error::AccessDenied
            | kurrent::Error::DeadlineExceeded
            | kurrent::Error::ServerError(_)
            | kurrent::Error::NotLeaderException(_)
            | kurrent::Error::ResourceNotFound => true,
            _ => false,
        }
    }
    let mut count = 0;

    while count < 50 {
        count += 1;

        debug!("Checking if admin user is available...{}/50", count);
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), async move {
            let mut stream = client.read_stream("$users", &Default::default()).await?;
            stream.next().await
        })
        .await;

        match result {
            Err(_) => {
                debug!("Request timed out, retrying...");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }

            Ok(result) => match result {
                Err(e) if can_retry(&e) => {
                    debug!("Not available: {:?}, retrying...", e);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }

                Err(e) => {
                    debug!("Fatal error, stop retrying. Cause: {:?}", e);
                    return Err(e);
                }

                Ok(opt) => {
                    if opt.is_some() {
                        debug!("Admin account is available!");
                        return Ok(());
                    }

                    debug!("$users stream seems to be empty, retrying...");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            },
        }
    }

    Err(kurrent::Error::ServerError(
        "Waiting for the admin user to be created took too much time".to_string(),
    ))
}

enum Tests {
    Streams,
    PersistentSubscriptions,
    Projections,
    Operations,
}

enum Topologies {
    SingleNode,
    Cluster,
}

async fn run_test(test: Tests, topology: Topologies) -> eyre::Result<()> {
    let docker = Cli::default();
    let mut target_container = None;

    let _ = pretty_env_logger::try_init();
    let client = match topology {
        Topologies::SingleNode => {
            let secure_mode = if let Some("true") = std::option_env!("SECURE") {
                true
            } else {
                false
            };

            let image = images::ESDB::default()
                .secure_mode(secure_mode)
                .enable_projections();
            let container = docker.run(image);

            let settings = if secure_mode {
                format!(
                    "esdb://admin:changeit@localhost:{}?defaultDeadline=60000&tlsVerifyCert=false",
                    container.get_host_port_ipv4(2_113),
                )
                .parse::<ClientSettings>()
            } else {
                format!(
                    "esdb://localhost:{}?tls=false&defaultDeadline=60000",
                    container.get_host_port_ipv4(2_113),
                )
                .parse::<ClientSettings>()
            }?;

            wait_node_is_alive(&settings, container.get_host_port_ipv4(2_113)).await?;
            target_container = Some(container);

            Client::new(settings.clone())?
        }

        Topologies::Cluster => {
            let settings = "esdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?tlsVerifyCert=false&nodePreference=leader&maxdiscoverattempts=50&defaultDeadline=60000"
                .parse::<ClientSettings>()?;

            let client = Client::new(settings.clone())?;

            // Those pre-checks are put in place to avoid test flakiness. In essence, those functions use
            // features we test later on.
            wait_for_admin_to_be_available(&client).await?;

            client
        }
    };

    let result = match test {
        Tests::Streams => api::streams::tests(client).await,
        Tests::PersistentSubscriptions => api::persistent_subscriptions::tests(client).await,
        Tests::Projections => api::projections::tests(client).await,
        Tests::Operations => api::operations::tests(client).await,
    };

    if let Some(container) = target_container {
        std::process::Command::new("docker")
            .arg("cp")
            .arg(format!("{}:/var/log/kurrent", container.id()))
            .arg("./esdb_logs")
            .output()?;
    }

    result?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn single_node_streams() -> eyre::Result<()> {
    run_test(Tests::Streams, Topologies::SingleNode).await
}

#[tokio::test(flavor = "multi_thread")]
async fn single_node_projections() -> eyre::Result<()> {
    run_test(Tests::Projections, Topologies::SingleNode).await
}

#[tokio::test(flavor = "multi_thread")]
async fn single_node_persistent_subscriptions() -> eyre::Result<()> {
    run_test(Tests::PersistentSubscriptions, Topologies::SingleNode).await
}

#[tokio::test(flavor = "multi_thread")]
async fn single_node_operations() -> eyre::Result<()> {
    run_test(Tests::Operations, Topologies::SingleNode).await
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_streams() -> eyre::Result<()> {
    run_test(Tests::Streams, Topologies::Cluster).await
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_projections() -> eyre::Result<()> {
    run_test(Tests::Projections, Topologies::Cluster).await
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_persistent_subscriptions() -> eyre::Result<()> {
    run_test(Tests::PersistentSubscriptions, Topologies::Cluster).await
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_operations() -> eyre::Result<()> {
    run_test(Tests::Operations, Topologies::Cluster).await
}

#[tokio::test(flavor = "multi_thread")]
async fn single_node_discover_error() -> eyre::Result<()> {
    let _ = pretty_env_logger::try_init();

    let settings = format!("esdb://noserver:{}", 2_113).parse()?;
    let client = Client::new(settings)?;
    let stream_id = fresh_stream_id("wont-be-created");
    let events = generate_events("wont-be-written".to_string(), 5);

    let result = client
        .append_to_stream(stream_id, &Default::default(), events)
        .await;

    if let Err(kurrent::Error::GrpcConnectionError(_)) = result {
        Ok(())
    } else {
        panic!("Expected gRPC connection error");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn single_node_auto_resub_on_connection_drop() -> eyre::Result<()> {
    let volume = create_unique_volume()?;
    let _ = pretty_env_logger::try_init();
    let docker = Cli::default();
    let image = images::ESDB::default()
        .insecure_mode()
        .attach_volume_to_db_directory(volume);
    let image_with_args = RunnableImage::from((image.clone(), ())).with_mapped_port((3_113, 2_113));
    let container = docker.run(image_with_args);

    let settings =
        format!("esdb://admin:changeit@localhost:{}?tls=false", 3_113).parse::<ClientSettings>()?;

    wait_node_is_alive(&settings, 3_113).await?;

    let cloned_setts = settings.clone();
    let client = Client::new(settings)?;
    let stream_name = fresh_stream_id("auto-reconnect");
    let retry = kurrent::RetryOptions::default().retry_forever();
    let options = kurrent::SubscribeToStreamOptions::default().retry_options(retry);
    let mut stream = client
        .subscribe_to_stream(stream_name.as_str(), &options)
        .await;
    let max = 6usize;
    let (tx, recv) = oneshot::channel();

    tokio::spawn(async move {
        let mut count = 0usize;

        loop {
            if let Err(e) = stream.next().await {
                error!("Subscription exited with: {}", e);
                break;
            }

            count += 1;

            if count == max {
                break;
            }
        }

        tx.send(count).unwrap();
    });

    let events = generate_events("reconnect".to_string(), 3);

    let _ = client
        .append_to_stream(stream_name.as_str(), &Default::default(), events)
        .await?;

    container.stop();
    debug!("Server is stopped");
    let image_with_args = RunnableImage::from((image, ())).with_mapped_port((3_113, 2_113));
    debug!("Server is restarting...");
    let _container = docker.run(image_with_args);

    wait_node_is_alive(&cloned_setts, 3_113).await?;
    debug!("Server is up again");

    let events = generate_events("reconnect".to_string(), 3);

    let _ = client
        .append_to_stream(stream_name.as_str(), &Default::default(), events)
        .await?;

    let test_count = tokio::time::timeout(std::time::Duration::from_secs(60), recv).await??;

    assert_eq!(
        test_count, 6,
        "We are testing proper state after subscription upon reconnection: got {} expected {}.",
        test_count, 6
    );

    Ok(())
}
