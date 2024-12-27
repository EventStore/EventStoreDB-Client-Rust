use eventstore_macros::{options, streaming};
use futures::stream::TryStreamExt;
use std::time::SystemTime;
use std::{collections::HashMap, time::Duration};

use crate::event_store::generated::monitoring;
use crate::event_store::generated::operations;
use crate::event_store::generated::users;
use crate::{ClientSettings, Endpoint};

pub(crate) mod gossip;

pub use crate::server_features::{Features, ServerInfo, ServerVersion};
pub use gossip::{MemberInfo, VNodeState};

#[derive(Clone)]
pub struct Client {
    inner: crate::grpc::GrpcClient,
}

options! {
    #[derive(Default)]
    pub struct OperationalOptions {}
}

options! {
    #[streaming]
    pub struct StatsOptions {
        pub(crate) refresh_time: Duration,
    }
}

impl Default for StatsOptions {
    fn default() -> StatsOptions {
        StatsOptions {
            common_operation_options: Default::default(),
            refresh_time: Duration::from_secs(1),
        }
    }
}

impl StatsOptions {
    pub fn refresh_time(self, value: Duration) -> Self {
        Self {
            refresh_time: value,
            ..self
        }
    }
}

impl Client {
    pub fn new(setts: ClientSettings) -> Self {
        let inner = crate::grpc::GrpcClient::create(tokio::runtime::Handle::current(), setts);
        Self { inner }
    }

    pub async fn current_selected_node(&self) -> crate::Result<Endpoint> {
        let handle = self.inner.current_selected_node().await?;

        Ok(handle.endpoint)
    }

    pub async fn server_version(&self) -> crate::Result<ServerInfo> {
        let handle = self.inner.current_selected_node().await?;

        Ok(handle.server_info())
    }

    pub fn settings(&self) -> &ClientSettings {
        self.inner.connection_settings()
    }

    pub async fn read_gossip(&self) -> crate::Result<Vec<gossip::MemberInfo>> {
        let handle = self.inner.current_selected_node().await?;

        // We currently use the http endpoint instead of the gRPC one because at that time
        // 04-25-2022, the public gRPC endpoint doesn't return all the gossip info like current
        // epoch and other checkpoints.
        gossip::http_read(self.inner.connection_settings(), handle)
            .await
            .map_err(|e| crate::Error::IllegalStateError(e.to_string()))
    }

    pub async fn stats(&self, options: &StatsOptions) -> crate::Result<Stats> {
        let handle = self.inner.current_selected_node().await?;

        let req = monitoring::StatsReq {
            // Using the metadata messes the parsing for no benefit. It only provides value in the UI, it's
            // better to have all the metrics flattened with only strings.
            use_metadata: false,
            refresh_time_period_in_ms: options.refresh_time.as_millis() as u64,
        };

        let req = crate::commands::new_request(self.inner.connection_settings(), options, req);
        let mut client =
            monitoring::monitoring_client::MonitoringClient::with_origin(handle.client, handle.uri);

        let inner = client
            .stats(req)
            .await
            .map_err(crate::Error::from_grpc)?
            .into_inner();

        Ok(Stats { inner })
    }

    pub async fn start_scavenge(
        &self,
        thread_count: usize,
        start_from_chunk: usize,
        options: &OperationalOptions,
    ) -> crate::Result<ScavengeResult> {
        let handle = self.inner.current_selected_node().await?;

        let req = operations::StartScavengeReq {
            options: Some(operations::start_scavenge_req::Options {
                thread_count: thread_count as i32,
                start_from_chunk: start_from_chunk as i32,
            }),
        };

        let req = crate::commands::new_request(self.inner.connection_settings(), options, req);

        let mut client =
            operations::operations_client::OperationsClient::with_origin(handle.client, handle.uri);

        let resp = client
            .start_scavenge(req)
            .await
            .map_err(crate::Error::from_grpc)?
            .into_inner();

        let status = match resp.scavenge_result() {
            operations::scavenge_resp::ScavengeResult::Started => ScavengeStatus::Started,
            operations::scavenge_resp::ScavengeResult::InProgress => ScavengeStatus::InProgress,
            operations::scavenge_resp::ScavengeResult::Stopped => ScavengeStatus::Stopped,
        };
        let id = resp.scavenge_id;

        Ok(ScavengeResult { id, status })
    }

    pub async fn stop_scavenge(
        &self,
        scavenge_id: impl AsRef<str>,
        options: &OperationalOptions,
    ) -> crate::Result<ScavengeResult> {
        let handle = self.inner.current_selected_node().await?;

        let req = operations::StopScavengeReq {
            options: Some(operations::stop_scavenge_req::Options {
                scavenge_id: scavenge_id.as_ref().to_string(),
            }),
        };

        let req = crate::commands::new_request(self.inner.connection_settings(), options, req);

        let mut client =
            operations::operations_client::OperationsClient::with_origin(handle.client, handle.uri);

        let resp = client
            .stop_scavenge(req)
            .await
            .map_err(crate::Error::from_grpc)?
            .into_inner();

        let status = match resp.scavenge_result() {
            operations::scavenge_resp::ScavengeResult::Started => ScavengeStatus::Started,
            operations::scavenge_resp::ScavengeResult::InProgress => ScavengeStatus::InProgress,
            operations::scavenge_resp::ScavengeResult::Stopped => ScavengeStatus::Stopped,
        };
        let id = resp.scavenge_id;

        Ok(ScavengeResult { id, status })
    }

    pub async fn shutdown(&self, options: &OperationalOptions) -> crate::Result<()> {
        let handle = self.inner.current_selected_node().await?;

        let mut client =
            operations::operations_client::OperationsClient::with_origin(handle.client, handle.uri);
        let req = crate::commands::new_request(self.inner.connection_settings(), options, ());
        client
            .shutdown(req)
            .await
            .map_err(crate::Error::from_grpc)
            .map(|_| ())
    }

    pub async fn merge_indexes(&self, options: &OperationalOptions) -> crate::Result<()> {
        let handle = self.inner.current_selected_node().await?;
        let mut client =
            operations::operations_client::OperationsClient::with_origin(handle.client, handle.uri);
        let req = crate::commands::new_request(self.inner.connection_settings(), options, ());

        client
            .merge_indexes(req)
            .await
            .map_err(crate::Error::from_grpc)
            .map(|_| ())
    }

    pub async fn resign_node(&self, options: &OperationalOptions) -> crate::Result<()> {
        let handle = self.inner.current_selected_node().await?;
        let mut client =
            operations::operations_client::OperationsClient::with_origin(handle.client, handle.uri);
        let req = crate::commands::new_request(self.inner.connection_settings(), options, ());

        client
            .resign_node(req)
            .await
            .map_err(crate::Error::from_grpc)
            .map(|_| ())
    }

    pub async fn set_node_priority(
        &self,
        priority: usize,
        options: &OperationalOptions,
    ) -> crate::Result<()> {
        let handle = self.inner.current_selected_node().await?;
        let mut client =
            operations::operations_client::OperationsClient::with_origin(handle.client, handle.uri);

        let req = operations::SetNodePriorityReq {
            priority: priority as i32,
        };

        let req = crate::commands::new_request(self.inner.connection_settings(), options, req);

        client
            .set_node_priority(req)
            .await
            .map_err(crate::Error::from_grpc)
            .map(|_| ())
    }

    pub async fn restart_persistent_subscriptions(
        &self,
        options: &OperationalOptions,
    ) -> crate::Result<()> {
        let handle = self.inner.current_selected_node().await?;
        let mut client =
            operations::operations_client::OperationsClient::with_origin(handle.client, handle.uri);
        let req = crate::commands::new_request(self.inner.connection_settings(), options, ());

        client
            .restart_persistent_subscriptions(req)
            .await
            .map_err(crate::Error::from_grpc)
            .map(|_| ())
    }

    pub async fn create_user(
        &self,
        login: impl AsRef<str>,
        password: impl AsRef<str>,
        full_name: impl AsRef<str>,
        groups: Vec<String>,
        options: &OperationalOptions,
    ) -> crate::Result<()> {
        let handle = self.inner.current_selected_node().await?;
        let mut client = users::users_client::UsersClient::with_origin(handle.client, handle.uri);

        let req = users::CreateReq {
            options: Some(users::create_req::Options {
                login_name: login.as_ref().to_string(),
                password: password.as_ref().to_string(),
                full_name: full_name.as_ref().to_string(),
                groups,
            }),
        };

        let req = crate::commands::new_request(self.inner.connection_settings(), options, req);

        client
            .create(req)
            .await
            .map_err(crate::Error::from_grpc)
            .map(|_| ())
    }

    pub async fn update_user(
        &self,
        login: impl AsRef<str>,
        password: impl AsRef<str>,
        full_name: impl AsRef<str>,
        groups: Vec<String>,
        options: &OperationalOptions,
    ) -> crate::Result<()> {
        let handle = self.inner.current_selected_node().await?;
        let mut client = users::users_client::UsersClient::with_origin(handle.client, handle.uri);

        let req = users::UpdateReq {
            options: Some(users::update_req::Options {
                login_name: login.as_ref().to_string(),
                password: password.as_ref().to_string(),
                full_name: full_name.as_ref().to_string(),
                groups,
            }),
        };

        let req = crate::commands::new_request(self.inner.connection_settings(), options, req);

        client
            .update(req)
            .await
            .map_err(crate::Error::from_grpc)
            .map(|_| ())
    }

    pub async fn delete_user(
        &self,
        login: impl AsRef<str>,
        options: &OperationalOptions,
    ) -> crate::Result<()> {
        let handle = self.inner.current_selected_node().await?;
        let mut client = users::users_client::UsersClient::with_origin(handle.client, handle.uri);

        let req = users::DeleteReq {
            options: Some(users::delete_req::Options {
                login_name: login.as_ref().to_string(),
            }),
        };

        let req = crate::commands::new_request(self.inner.connection_settings(), options, req);

        client
            .delete(req)
            .await
            .map_err(crate::Error::from_grpc)
            .map(|_| ())
    }

    pub async fn enable_user(
        &self,
        login: impl AsRef<str>,
        options: &OperationalOptions,
    ) -> crate::Result<()> {
        let handle = self.inner.current_selected_node().await?;
        let mut client = users::users_client::UsersClient::with_origin(handle.client, handle.uri);

        let req = users::EnableReq {
            options: Some(users::enable_req::Options {
                login_name: login.as_ref().to_string(),
            }),
        };

        let req = crate::commands::new_request(self.inner.connection_settings(), options, req);

        client
            .enable(req)
            .await
            .map_err(crate::Error::from_grpc)
            .map(|_| ())
    }

    pub async fn disable_user(
        &self,
        login: impl AsRef<str>,
        options: &OperationalOptions,
    ) -> crate::Result<()> {
        let handle = self.inner.current_selected_node().await?;
        let mut client = users::users_client::UsersClient::with_origin(handle.client, handle.uri);

        let req = users::DisableReq {
            options: Some(users::disable_req::Options {
                login_name: login.as_ref().to_string(),
            }),
        };

        let req = crate::commands::new_request(self.inner.connection_settings(), options, req);

        client
            .disable(req)
            .await
            .map_err(crate::Error::from_grpc)
            .map(|_| ())
    }

    pub async fn user_details(
        &self,
        login: impl AsRef<str>,
        options: &OperationalOptions,
    ) -> crate::Result<UserDetailsStream> {
        let handle = self.inner.current_selected_node().await?;
        let mut client = users::users_client::UsersClient::with_origin(handle.client, handle.uri);

        let req = users::DetailsReq {
            options: Some(users::details_req::Options {
                login_name: login.as_ref().to_string(),
            }),
        };

        let req = crate::commands::new_request(self.inner.connection_settings(), options, req);

        let inner = client
            .details(req)
            .await
            .map_err(crate::Error::from_grpc)?
            .into_inner();

        Ok(UserDetailsStream { inner })
    }

    pub async fn change_user_password(
        &self,
        login: impl AsRef<str>,
        current_password: impl AsRef<str>,
        new_password: impl AsRef<str>,
        options: &OperationalOptions,
    ) -> crate::Result<()> {
        let handle = self.inner.current_selected_node().await?;
        let mut client = users::users_client::UsersClient::with_origin(handle.client, handle.uri);

        let req = users::ChangePasswordReq {
            options: Some(users::change_password_req::Options {
                login_name: login.as_ref().to_string(),
                current_password: current_password.as_ref().to_string(),
                new_password: new_password.as_ref().to_string(),
            }),
        };

        let req = crate::commands::new_request(self.inner.connection_settings(), options, req);

        client
            .change_password(req)
            .await
            .map_err(crate::Error::from_grpc)
            .map(|_| ())
    }

    pub async fn reset_user_password(
        &self,
        login: impl AsRef<str>,
        new_password: impl AsRef<str>,
        options: &OperationalOptions,
    ) -> crate::Result<()> {
        let handle = self.inner.current_selected_node().await?;
        let mut client = users::users_client::UsersClient::with_origin(handle.client, handle.uri);

        let req = users::ResetPasswordReq {
            options: Some(users::reset_password_req::Options {
                login_name: login.as_ref().to_string(),
                new_password: new_password.as_ref().to_string(),
            }),
        };

        let req = crate::commands::new_request(self.inner.connection_settings(), options, req);

        client
            .reset_password(req)
            .await
            .map_err(crate::Error::from_grpc)
            .map(|_| ())
    }
}

impl From<crate::Client> for Client {
    fn from(src: crate::Client) -> Self {
        Self { inner: src.client }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RawStatistics(pub HashMap<String, String>);

pub struct Stats {
    inner: tonic::Streaming<monitoring::StatsResp>,
}

impl From<HashMap<String, String>> for RawStatistics {
    fn from(value: HashMap<String, String>) -> Self {
        RawStatistics(value)
    }
}

impl Stats {
    pub async fn next(&mut self) -> crate::Result<RawStatistics> {
        let result = self
            .inner
            .try_next()
            .await
            .map_err(crate::Error::from_grpc)?;

        if let Some(resp) = result {
            Ok(resp.stats.into())
        } else {
            Err(crate::Error::ResourceNotFound)
        }
    }
}

pub struct UserDetailsStream {
    inner: tonic::Streaming<users::DetailsResp>,
}

impl UserDetailsStream {
    pub async fn next(&mut self) -> crate::Result<Option<UserDetails>> {
        let result = self
            .inner
            .try_next()
            .await
            .map_err(crate::Error::from_grpc)?;

        Ok(result.and_then(|resp| {
            let details = resp.user_details?;
            let last_updated = if let Some(datetime) = details.last_updated {
                SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(
                    datetime.ticks_since_epoch as u64 / 10_000_000,
                ))
            } else {
                None
            };

            Some(UserDetails {
                login: details.login_name,
                full_name: details.full_name,
                groups: details.groups,
                disabled: details.disabled,
                last_updated,
            })
        }))
    }
}

#[derive(Clone, Debug)]
pub struct UserDetails {
    pub login: String,
    pub full_name: String,
    pub groups: Vec<String>,
    pub disabled: bool,
    pub last_updated: Option<SystemTime>,
}

#[derive(Debug, Clone)]
pub struct ScavengeResult {
    id: String,
    status: ScavengeStatus,
}

impl ScavengeResult {
    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    pub fn status(&self) -> ScavengeStatus {
        self.status
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScavengeStatus {
    Started,
    InProgress,
    Stopped,
}
