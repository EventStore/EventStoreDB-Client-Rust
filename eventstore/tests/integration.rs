mod api;
mod common;
mod images;
mod misc;
mod plugins;

use crate::common::{fresh_stream_id, generate_events};
use eventstore::{Client, ClientSettings};
use futures::channel::oneshot;
use std::time::Duration;
use testcontainers::{ImageExt, core::ContainerPort, runners::AsyncRunner};
use tracing::{debug, error};
use tracing_subscriber::EnvFilter;

fn configure_logging() {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(EnvFilter::new(
            "integration=debug,eventstore=debug,testcontainers=debug",
        ))
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        .init();
}

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

async fn wait_node_is_alive(setts: &eventstore::ClientSettings, port: u16) -> eyre::Result<()> {
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
async fn wait_for_admin_to_be_available(client: &Client) -> eventstore::Result<()> {
    fn can_retry(e: &eventstore::Error) -> bool {
        match e {
            eventstore::Error::AccessDenied
            | eventstore::Error::DeadlineExceeded
            | eventstore::Error::ServerError(_)
            | eventstore::Error::NotLeaderException(_)
            | eventstore::Error::ResourceNotFound => true,
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

    Err(eventstore::Error::ServerError(
        "Waiting for the admin user to be created took too much time".to_string(),
    ))
}

enum Tests {
    Api(ApiTests),
    Plugins(PluginTests),
    Misc(MiscTests),
}

impl Tests {
    fn is_user_certificates_related(&self) -> bool {
        matches!(self, Tests::Plugins(PluginTests::UserCertificates))
    }
}

enum ApiTests {
    Streams,
    PersistentSubscriptions,
    Projections,
    Operations,
}

impl From<ApiTests> for Tests {
    fn from(test: ApiTests) -> Self {
        Tests::Api(test)
    }
}

enum PluginTests {
    UserCertificates,
}

impl From<PluginTests> for Tests {
    fn from(test: PluginTests) -> Self {
        Tests::Plugins(test)
    }
}

enum MiscTests {
    RootCertificates,
}

impl From<MiscTests> for Tests {
    fn from(test: MiscTests) -> Self {
        Tests::Misc(test)
    }
}

enum Topologies {
    SingleNode,
    Cluster,
}

async fn run_test(test: impl Into<Tests>, topology: Topologies) -> eyre::Result<()> {
    configure_logging();
    let test = test.into();
    let mut container_port = 2_113;

    // we need to own the container otherwise RAII will drop it and you would lose hours figuring
    // out why the tests are not working anymore. It's because you totally forgot about that. So
    // with this long comment, I want to make sure it doesn't happen again.
    let mut _container = None;

    let predifined_client = match topology {
        Topologies::SingleNode => {
            let secure_mode = matches!(std::option_env!("SECURE"), Some("true"))
                || test.is_user_certificates_related();
            let temp = images::EventStoreDB::default()
                .secure_mode(secure_mode)
                .forward_eventstore_env_variables(test.is_user_certificates_related())
                .enable_projections()
                .start()
                .await?;

            container_port = temp.get_host_port_ipv4(2_113).await?;
            _container = Some(temp);
            let settings = if secure_mode {
                format!(
                    "esdb://admin:changeit@localhost:{}?defaultDeadline=60000&tlsVerifyCert=false",
                    container_port,
                )
                .parse::<ClientSettings>()
            } else {
                format!(
                    "esdb://localhost:{}?tls=false&defaultDeadline=60000",
                    container_port,
                )
                .parse::<ClientSettings>()
            }?;

            wait_node_is_alive(&settings, container_port).await?;
            Client::new(settings)?
        }

        Topologies::Cluster => {
            let settings = "esdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?tlsVerifyCert=false&nodePreference=leader&maxdiscoverattempts=50&defaultDeadline=60000"
                .parse::<ClientSettings>()?;

            let client = Client::new(settings)?;

            // Those pre-checks are put in place to avoid test flakiness. In essence, those functions use
            // features we test later on.
            wait_for_admin_to_be_available(&client).await?;

            client
        }
    };

    let result = match test {
        Tests::Api(test) => match test {
            ApiTests::Streams => api::streams::tests(predifined_client).await,
            ApiTests::PersistentSubscriptions => {
                api::persistent_subscriptions::tests(predifined_client).await
            }
            ApiTests::Projections => api::projections::tests(predifined_client).await,
            ApiTests::Operations => api::operations::tests(predifined_client).await,
        },

        Tests::Plugins(test) => match test {
            PluginTests::UserCertificates => {
                plugins::user_certificates::tests(container_port).await
            }
        },

        Tests::Misc(test) => match test {
            MiscTests::RootCertificates => misc::root_certificates::tests(container_port).await,
        },
    };

    result?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn single_node_streams() -> eyre::Result<()> {
    run_test(ApiTests::Streams, Topologies::SingleNode).await
}

#[tokio::test(flavor = "multi_thread")]
async fn single_node_projections() -> eyre::Result<()> {
    run_test(ApiTests::Projections, Topologies::SingleNode).await
}

#[tokio::test(flavor = "multi_thread")]
async fn single_node_persistent_subscriptions() -> eyre::Result<()> {
    run_test(ApiTests::PersistentSubscriptions, Topologies::SingleNode).await
}

#[tokio::test(flavor = "multi_thread")]
async fn single_node_operations() -> eyre::Result<()> {
    run_test(ApiTests::Operations, Topologies::SingleNode).await
}

#[tokio::test(flavor = "multi_thread")]
async fn plugin_usercertificates() -> eyre::Result<()> {
    run_test(PluginTests::UserCertificates, Topologies::SingleNode).await
}

#[tokio::test(flavor = "multi_thread")]
async fn root_certificates() -> eyre::Result<()> {
    run_test(MiscTests::RootCertificates, Topologies::SingleNode).await
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_streams() -> eyre::Result<()> {
    run_test(ApiTests::Streams, Topologies::Cluster).await
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_projections() -> eyre::Result<()> {
    run_test(ApiTests::Projections, Topologies::Cluster).await
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_persistent_subscriptions() -> eyre::Result<()> {
    run_test(ApiTests::PersistentSubscriptions, Topologies::Cluster).await
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_operations() -> eyre::Result<()> {
    run_test(ApiTests::Operations, Topologies::Cluster).await
}

#[tokio::test(flavor = "multi_thread")]
async fn single_node_discover_error() -> eyre::Result<()> {
    let settings = format!("esdb://noserver:{}", 2_113).parse()?;
    let client = Client::new(settings)?;
    let stream_id = fresh_stream_id("wont-be-created");
    let events = generate_events("wont-be-written", 5);

    let result = client
        .append_to_stream(stream_id, &Default::default(), events)
        .await;

    if let Err(eventstore::Error::GrpcConnectionError(_)) = result {
        Ok(())
    } else {
        panic!("Expected gRPC connection error");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn single_node_auto_resub_on_connection_drop() -> eyre::Result<()> {
    let volume = create_unique_volume()?;
    let image = images::EventStoreDB::default()
        .insecure_mode()
        .attach_volume_to_db_directory(volume);

    let container = image
        .clone()
        .with_mapped_port(3_113, ContainerPort::Tcp(2_113))
        .start()
        .await?;

    let settings =
        format!("esdb://admin:changeit@localhost:{}?tls=false", 3_113).parse::<ClientSettings>()?;

    wait_node_is_alive(&settings, 3_113).await?;

    let cloned_setts = settings.clone();
    let client = Client::new(settings)?;
    let stream_name = fresh_stream_id("auto-reconnect");
    let retry = eventstore::RetryOptions::default().retry_forever();
    let options = eventstore::SubscribeToStreamOptions::default().retry_options(retry);
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

    let events = generate_events("reconnect", 3);

    let _ = client
        .append_to_stream(stream_name.as_str(), &Default::default(), events)
        .await?;

    container.stop().await?;
    debug!("Server is stopped, restarting...");
    let _container = image
        .with_mapped_port(3_113, ContainerPort::Tcp(2_113))
        .start()
        .await?;

    wait_node_is_alive(&cloned_setts, 3_113).await?;
    debug!("Server is up again");

    let events = generate_events("reconnect", 3);

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
