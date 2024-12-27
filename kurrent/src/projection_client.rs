use crate::event_store::client::projections;
use crate::grpc::{ClientSettings, GrpcClient};
use crate::options::projections::{
    CreateProjectionOptions, DeleteProjectionOptions, GenericProjectionOptions,
    GetResultProjectionOptions, GetStateProjectionOptions, UpdateProjectionOptions,
};
use futures::{stream::BoxStream, TryStreamExt};
use serde::de::DeserializeOwned;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) enum StatsFor {
    Name(String),
    AllProjections,
    AllContinuous,
}

#[derive(Clone, Debug)]
pub struct ProjectionStatus {
    pub core_processing_time: i64,
    pub version: i64,
    pub epoch: i64,
    pub effective_name: String,
    pub writes_in_progress: i32,
    pub reads_in_progress: i32,
    pub partitions_cached: i32,
    pub status: String,
    pub state_reason: String,
    pub name: String,
    pub mode: String,
    pub position: String,
    pub progress: f32,
    pub last_checkpoint: String,
    pub events_processed_after_restart: i64,
    pub checkpoint_status: String,
    pub buffered_events: i64,
    pub write_pending_events_before_checkpoint: i32,
    pub write_pending_events_after_checkpoint: i32,
}

#[derive(Clone)]
pub struct ProjectionClient {
    client: GrpcClient,
}

impl ProjectionClient {
    pub fn new(settings: ClientSettings) -> Self {
        ProjectionClient::with_runtime_handle(tokio::runtime::Handle::current(), settings)
    }

    pub fn with_runtime_handle(handle: tokio::runtime::Handle, settings: ClientSettings) -> Self {
        let client = GrpcClient::create(handle, settings);

        ProjectionClient { client }
    }

    pub fn settings(&self) -> &ClientSettings {
        self.client.connection_settings()
    }

    pub async fn create<Name>(
        &self,
        name: Name,
        query: String,
        options: &CreateProjectionOptions,
    ) -> crate::Result<()>
    where
        Name: AsRef<str>,
    {
        self.create_projection_internal(
            options,
            projections::create_req::Options {
                query: query.clone(),
                mode: Some(projections::create_req::options::Mode::Continuous(
                    projections::create_req::options::Continuous {
                        name: name.as_ref().to_string(),
                        track_emitted_streams: options.track_emitted_streams,
                    },
                )),
            },
        )
        .await?;

        // TODO - create projection RPC call needs to be fixed upstream where the emit options
        // will be added to the API. Right now, do an extra RPC call to implement it.
        if options.emit {
            let upd_options = UpdateProjectionOptions::default().emit(true);

            self.update(name.as_ref(), query, &upd_options).await?;
        }

        Ok(())
    }

    async fn create_projection_internal<Opts>(
        &self,
        create_opts: &Opts,
        options: projections::create_req::Options,
    ) -> crate::Result<()>
    where
        Opts: crate::options::Options,
    {
        let req = projections::CreateReq {
            options: Some(options),
        };

        let req = crate::commands::new_request(self.client.connection_settings(), create_opts, req);

        self.client
            .execute(|handle| async move {
                let mut client = projections::projections_client::ProjectionsClient::with_origin(
                    handle.client,
                    handle.uri,
                );
                let _ = client.create(req).await?;

                Ok(())
            })
            .await
    }

    pub async fn update<Name>(
        &self,
        name: Name,
        query: String,
        options: &UpdateProjectionOptions,
    ) -> crate::Result<()>
    where
        Name: AsRef<str>,
    {
        let req_options = projections::update_req::Options {
            name: name.as_ref().to_string(),
            emit_option: options
                .emit
                .as_ref()
                .copied()
                .map(projections::update_req::options::EmitOption::EmitEnabled)
                .or(Some(
                    projections::update_req::options::EmitOption::NoEmitOptions(()),
                )),
            query,
        };

        let req = projections::UpdateReq {
            options: Some(req_options),
        };

        let req = crate::commands::new_request(self.client.connection_settings(), options, req);

        self.client
            .execute(|handle| async move {
                let mut client = projections::projections_client::ProjectionsClient::with_origin(
                    handle.client.clone(),
                    handle.uri.clone(),
                );

                let _ = client.update(req).await?;

                Ok(())
            })
            .await
    }

    pub async fn delete<Name>(
        &self,
        name: Name,
        options: &DeleteProjectionOptions,
    ) -> crate::Result<()>
    where
        Name: AsRef<str>,
    {
        let req_options = projections::delete_req::Options {
            name: name.as_ref().to_string(),
            delete_emitted_streams: options.delete_emitted_streams,
            delete_state_stream: options.delete_state_stream,
            delete_checkpoint_stream: options.delete_checkpoint_stream,
        };

        let req = projections::DeleteReq {
            options: Some(req_options),
        };

        let req = crate::commands::new_request(self.client.connection_settings(), options, req);

        self.client
            .execute(|handle| async move {
                let mut client = projections::projections_client::ProjectionsClient::with_origin(
                    handle.client.clone(),
                    handle.uri.clone(),
                );

                let _ = client.delete(req).await?;

                Ok(())
            })
            .await
    }

    pub async fn get_status<Name>(
        &self,
        name: Name,
        options: &GenericProjectionOptions,
    ) -> crate::Result<Option<ProjectionStatus>>
    where
        Name: AsRef<str>,
    {
        self.statistics(StatsFor::Name(name.as_ref().to_string()), options)
            .await?
            .try_next()
            .await
    }

    pub async fn list(
        &self,
        options: &GenericProjectionOptions,
    ) -> crate::Result<BoxStream<'_, crate::Result<ProjectionStatus>>> {
        self.statistics(StatsFor::AllContinuous, options).await
    }

    async fn statistics(
        &self,
        stats_for: StatsFor,
        options: &GenericProjectionOptions,
    ) -> crate::Result<BoxStream<'_, crate::Result<ProjectionStatus>>> {
        let mode = match stats_for {
            StatsFor::Name(name) => projections::statistics_req::options::Mode::Name(name),
            StatsFor::AllProjections => projections::statistics_req::options::Mode::All(()),
            StatsFor::AllContinuous => projections::statistics_req::options::Mode::Continuous(()),
        };

        let stats_options = projections::statistics_req::Options { mode: Some(mode) };

        let req = projections::StatisticsReq {
            options: Some(stats_options),
        };

        let req = crate::commands::new_request(self.client.connection_settings(), options, req);

        self.client
            .execute(|handle| async move {
                let mut client =
                    projections::projections_client::ProjectionsClient::with_origin(handle.client.clone(), handle.uri.clone());

                let mut stream = client.statistics(req).await?.into_inner();

                let stream = async_stream::stream! {
                    loop {
                        match stream.try_next().await {
                            Err(e) => {
                                let e = crate::Error::from_grpc(e);

                                handle.report_error(&e);
                                yield Err(e);
                                break;
                            }

                            Ok(resp) => {
                                if let Some(resp) = resp {
                                    let details = resp.details.expect("to be defined");
                                    let details = ProjectionStatus {
                                        core_processing_time: details.core_processing_time,
                                        version: details.version,
                                        epoch: details.epoch,
                                        effective_name: details.effective_name,
                                        writes_in_progress: details.writes_in_progress,
                                        reads_in_progress: details.reads_in_progress,
                                        partitions_cached: details.partitions_cached,
                                        status: details.status,
                                        state_reason: details.state_reason,
                                        name: details.name,
                                        mode: details.mode,
                                        position: details.position,
                                        progress: details.progress,
                                        last_checkpoint: details.last_checkpoint,
                                        events_processed_after_restart: details.events_processed_after_restart,
                                        checkpoint_status: details.checkpoint_status,
                                        buffered_events: details.buffered_events,
                                        write_pending_events_after_checkpoint: details.write_pending_events_after_checkpoint,
                                        write_pending_events_before_checkpoint: details.write_pending_events_before_checkpoint,
                                    };

                                    yield Ok(details);
                                    continue;
                                }

                                break;
                            }
                        }
                    }
                };

                let stream: BoxStream<crate::Result<ProjectionStatus>> = Box::pin(stream);

                Ok(stream)
            })
            .await
    }

    pub async fn enable<Name>(
        &self,
        name: Name,
        options: &GenericProjectionOptions,
    ) -> crate::Result<()>
    where
        Name: AsRef<str>,
    {
        let req_options = projections::enable_req::Options {
            name: name.as_ref().to_string(),
        };

        let req = projections::EnableReq {
            options: Some(req_options),
        };

        let req = crate::commands::new_request(self.client.connection_settings(), options, req);

        self.client
            .execute(|handle| async move {
                let mut client = projections::projections_client::ProjectionsClient::with_origin(
                    handle.client.clone(),
                    handle.uri.clone(),
                );

                let _ = client.enable(req).await?;

                Ok(())
            })
            .await
    }

    pub async fn reset<Name>(
        &self,
        name: Name,
        options: &GenericProjectionOptions,
    ) -> crate::Result<()>
    where
        Name: AsRef<str>,
    {
        let req_options = projections::reset_req::Options {
            name: name.as_ref().to_string(),
            write_checkpoint: false,
        };

        let req = projections::ResetReq {
            options: Some(req_options),
        };

        let req = crate::commands::new_request(self.client.connection_settings(), options, req);

        self.client
            .execute(|handle| async move {
                let mut client = projections::projections_client::ProjectionsClient::with_origin(
                    handle.client.clone(),
                    handle.uri.clone(),
                );

                let _ = client.reset(req).await?;

                Ok(())
            })
            .await
    }

    pub async fn disable<Name>(
        &self,
        name: Name,
        options: &GenericProjectionOptions,
    ) -> crate::Result<()>
    where
        Name: AsRef<str>,
    {
        self.disable_projection_internal(name, true, options).await
    }

    pub async fn abort<Name>(
        &self,
        name: Name,
        options: &GenericProjectionOptions,
    ) -> crate::Result<()>
    where
        Name: AsRef<str>,
    {
        self.disable_projection_internal(name, false, options).await
    }

    async fn disable_projection_internal<Name>(
        &self,
        name: Name,
        write_checkpoint: bool,
        options: &GenericProjectionOptions,
    ) -> crate::Result<()>
    where
        Name: AsRef<str>,
    {
        let req_options = projections::disable_req::Options {
            name: name.as_ref().to_string(),
            write_checkpoint,
        };

        let req = projections::DisableReq {
            options: Some(req_options),
        };

        let req = crate::commands::new_request(self.client.connection_settings(), options, req);

        self.client
            .execute(|handle| async move {
                let mut client = projections::projections_client::ProjectionsClient::with_origin(
                    handle.client.clone(),
                    handle.uri.clone(),
                );

                let _ = client.disable(req).await?;

                Ok(())
            })
            .await
    }

    pub async fn get_state<Name, A>(
        &self,
        name: Name,
        options: &GetStateProjectionOptions,
    ) -> crate::Result<serde_json::Result<A>>
    where
        Name: AsRef<str>,
        A: DeserializeOwned + Send,
    {
        let req_options = projections::state_req::Options {
            name: name.as_ref().to_string(),
            partition: options.partition.clone(),
        };

        let req = projections::StateReq {
            options: Some(req_options),
        };

        let req = crate::commands::new_request(self.client.connection_settings(), options, req);

        self.client
            .execute(|handle| async move {
                let mut client = projections::projections_client::ProjectionsClient::with_origin(
                    handle.client.clone(),
                    handle.uri.clone(),
                );

                let resp = client.state(req).await?.into_inner();
                let value = resp
                    .state
                    .map(parse_value)
                    .unwrap_or(serde_json::Value::Null);

                Ok(serde_json::from_value(value))
            })
            .await
    }

    pub async fn get_result<Name, A>(
        &self,
        name: Name,
        options: &GetResultProjectionOptions,
    ) -> crate::Result<serde_json::Result<A>>
    where
        Name: AsRef<str>,
        A: DeserializeOwned + Send,
    {
        let req_options = projections::result_req::Options {
            name: name.as_ref().to_string(),
            partition: options.partition.clone(),
        };

        let req = projections::ResultReq {
            options: Some(req_options),
        };

        let req = crate::commands::new_request(self.client.connection_settings(), options, req);

        self.client
            .execute(|handle| async move {
                let mut client = projections::projections_client::ProjectionsClient::with_origin(
                    handle.client.clone(),
                    handle.uri.clone(),
                );

                let resp = client.result(req).await?.into_inner();
                let value = resp
                    .result
                    .map(parse_value)
                    .unwrap_or(serde_json::Value::Null);

                Ok(serde_json::from_value(value))
            })
            .await
    }

    pub async fn restart_subsystem(&self, options: &GenericProjectionOptions) -> crate::Result<()> {
        let req = crate::commands::new_request(self.client.connection_settings(), options, ());

        self.client
            .execute(|handle| async {
                let mut client = projections::projections_client::ProjectionsClient::with_origin(
                    handle.client,
                    handle.uri,
                );
                let _ = client.restart_subsystem(req).await?;

                Ok(())
            })
            .await
    }
}

fn parse_value(value: prost_types::Value) -> serde_json::Value {
    enum Stack {
        List(
            std::vec::IntoIter<prost_types::Value>,
            Vec<serde_json::Value>,
        ),
        Map(
            std::collections::btree_map::IntoIter<String, prost_types::Value>,
            String,
            serde_json::Map<String, serde_json::Value>,
        ),
        Value(serde_json::Value),
        Proto(prost_types::Value),
    }

    let mut stack = vec![Stack::Proto(value)];
    let mut param: Option<serde_json::Value> = None;

    while let Some(elem) = stack.pop() {
        match elem {
            Stack::Value(value) => {
                param = Some(value);
            }

            Stack::Map(mut iter, scope, mut acc) => {
                let value = param.take().expect("to be defined");
                acc.insert(scope, value);

                if let Some((scope, value)) = iter.next() {
                    stack.push(Stack::Map(iter, scope, acc));
                    stack.push(Stack::Proto(value));
                } else {
                    stack.push(Stack::Value(serde_json::Value::Object(acc)));
                }
            }

            Stack::List(mut iter, mut acc) => {
                let value = param.take().expect("to be defined");
                acc.push(value);

                if let Some(value) = iter.next() {
                    stack.push(Stack::List(iter, acc));
                    stack.push(Stack::Proto(value));
                } else {
                    stack.push(Stack::Value(serde_json::Value::Array(acc)));
                }
            }

            Stack::Proto(proto_value) => {
                if let Some(kind) = proto_value.kind {
                    match kind {
                        prost_types::value::Kind::NullValue(_) => {
                            stack.push(Stack::Value(serde_json::Value::Null));
                        }

                        prost_types::value::Kind::NumberValue(value) => {
                            stack.push(Stack::Value(serde_json::Value::Number(
                                serde_json::Number::from_f64(value).expect("to be valid"),
                            )));
                        }

                        prost_types::value::Kind::StringValue(value) => {
                            stack.push(Stack::Value(serde_json::Value::String(value)));
                        }

                        prost_types::value::Kind::BoolValue(value) => {
                            stack.push(Stack::Value(serde_json::Value::Bool(value)));
                        }

                        prost_types::value::Kind::StructValue(obj) => {
                            let mut iter = obj.fields.into_iter();

                            if let Some((key, value)) = iter.next() {
                                stack.push(Stack::Map(iter, key, serde_json::Map::new()));
                                stack.push(Stack::Proto(value));
                            } else {
                                stack.push(Stack::Value(serde_json::Value::Object(
                                    serde_json::Map::new(),
                                )));
                            }
                        }

                        prost_types::value::Kind::ListValue(list) => {
                            let mut iter = list.values.into_iter();

                            if let Some(value) = iter.next() {
                                stack.push(Stack::List(iter, Vec::new()));
                                stack.push(Stack::Proto(value));
                            } else {
                                stack.push(Stack::Value(serde_json::Value::Array(Vec::new())));
                            }
                        }
                    }
                } else {
                    stack.push(Stack::Value(serde_json::Value::Null));
                }
            }
        }
    }

    param.expect("not empty")
}

impl From<crate::Client> for ProjectionClient {
    fn from(src: crate::Client) -> Self {
        Self { client: src.client }
    }
}
