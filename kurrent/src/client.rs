use crate::batch::BatchAppendClient;
use crate::grpc::{ClientSettings, GrpcClient};
use crate::options::batch_append::BatchAppendOptions;
use crate::options::persistent_subscription::PersistentSubscriptionOptions;
use crate::options::read_all::ReadAllOptions;
use crate::options::read_stream::ReadStreamOptions;
use crate::options::subscribe_to_stream::SubscribeToStreamOptions;
use crate::server_features::ServerInfo;
use crate::{
    commands, DeletePersistentSubscriptionOptions, DeleteStreamOptions,
    GetPersistentSubscriptionInfoOptions, ListPersistentSubscriptionsOptions, MetadataStreamName,
    PersistentSubscription, PersistentSubscriptionInfo, PersistentSubscriptionToAllOptions,
    Position, ReadStream, ReplayParkedMessagesOptions, RestartPersistentSubscriptionSubsystem,
    RevisionOrPosition, StreamMetadata, StreamMetadataResult, StreamName, SubscribeToAllOptions,
    SubscribeToPersistentSubscriptionOptions, Subscription, TombstoneStreamOptions,
    VersionedMetadata, WriteResult,
};
use crate::{
    options::append_to_stream::{AppendToStreamOptions, ToEvents},
    EventData,
};

/// Represents a client to a single node. `Client` maintains a full duplex
/// communication to KurrentDB.
///
/// Many threads can use an KurrentDB client at the same time
/// or a single thread can make many asynchronous requests.
#[derive(Clone)]
pub struct Client {
    pub(crate) http_client: reqwest::Client,
    pub(crate) client: GrpcClient,
}

impl Client {
    /// Creates a gRPC client to an KurrentDB database.
    pub fn new(settings: ClientSettings) -> crate::Result<Self> {
        Client::with_runtime_handle(tokio::runtime::Handle::current(), settings)
    }

    /// Creates a gRPC client to an KurrentDB database using an existing tokio runtime.
    pub fn with_runtime_handle(
        handle: tokio::runtime::Handle,
        settings: ClientSettings,
    ) -> crate::Result<Self> {
        let client = GrpcClient::create(handle, settings.clone());

        let http_client = reqwest::Client::builder()
            .danger_accept_invalid_certs(!settings.is_tls_certificate_verification_enabled())
            .https_only(settings.is_secure_mode_enabled())
            .build()
            .map_err(|e| crate::Error::InitializationError(e.to_string()))?;

        Ok(Client {
            http_client,
            client,
        })
    }

    pub fn settings(&self) -> &ClientSettings {
        self.client.connection_settings()
    }

    /// Returns the server information the client is connected to. If `None`, means you are dealing
    /// with a server older than 21.6 version.
    pub async fn server_info(&self) -> crate::Result<ServerInfo> {
        let handle = self.client.current_selected_node().await?;

        Ok(handle.server_info())
    }

    /// Sends events to a given stream.
    pub async fn append_to_stream<Events>(
        &self,
        stream_name: impl StreamName,
        options: &AppendToStreamOptions,
        events: Events,
    ) -> crate::Result<WriteResult>
    where
        Events: ToEvents,
    {
        commands::append_to_stream(&self.client, stream_name, options, events.into_events()).await
    }

    // Sets a stream metadata.
    pub async fn set_stream_metadata(
        &self,
        name: impl MetadataStreamName,
        options: &AppendToStreamOptions,
        metadata: &StreamMetadata,
    ) -> crate::Result<WriteResult> {
        let event = EventData::json("$metadata", metadata)
            .map_err(|e| crate::Error::InternalParsingError(e.to_string()))?;

        self.append_to_stream(name.into_metadata_stream_name(), options, event)
            .await
    }

    // Creates a batch-append client.
    pub async fn batch_append(
        &self,
        options: &BatchAppendOptions,
    ) -> crate::Result<BatchAppendClient> {
        commands::batch_append(&self.client, options).await
    }

    /// Reads events from a given stream. The reading can be done forward and
    /// backward.
    pub async fn read_stream(
        &self,
        stream_name: impl StreamName,
        options: &ReadStreamOptions,
    ) -> crate::Result<ReadStream> {
        commands::read_stream(
            self.client.clone(),
            options,
            stream_name,
            options.max_count as u64,
        )
        .await
    }

    /// Reads events for the system stream `$all`. The reading can be done
    /// forward and backward.
    pub async fn read_all(&self, options: &ReadAllOptions) -> crate::Result<ReadStream> {
        commands::read_all(self.client.clone(), options, options.max_count as u64).await
    }

    /// Reads a stream metadata.
    pub async fn get_stream_metadata(
        &self,
        name: impl MetadataStreamName,
        options: &ReadStreamOptions,
    ) -> crate::Result<StreamMetadataResult> {
        let mut stream = self
            .read_stream(name.into_metadata_stream_name(), options)
            .await?;

        match stream.next().await {
            Ok(event) => {
                let event = event.expect("to be defined");
                let metadata = event
                    .get_original_event()
                    .as_json::<StreamMetadata>()
                    .map_err(|e| crate::Error::InternalParsingError(e.to_string()))?;

                let metadata = VersionedMetadata {
                    stream: event.get_original_stream_id().to_string(),
                    version: event.get_original_event().revision,
                    metadata,
                };

                Ok(StreamMetadataResult::Success(Box::new(metadata)))
            }
            Err(e) => match e {
                crate::Error::ResourceNotFound => Ok(StreamMetadataResult::NotFound),
                crate::Error::ResourceDeleted => Ok(StreamMetadataResult::Deleted),
                other => Err(other),
            },
        }
    }

    /// Soft deletes a given stream.
    /// Makes use of Truncate before. When a stream is deleted, its Truncate
    /// before is set to the streams current last event number. When a soft
    /// deleted stream is read, the read will return a StreamNotFound. After
    /// deleting the stream, you are able to write to it again, continuing from
    /// where it left off.
    pub async fn delete_stream(
        &self,
        stream_name: impl StreamName,
        options: &DeleteStreamOptions,
    ) -> crate::Result<Option<Position>> {
        commands::delete_stream(&self.client, stream_name, options).await
    }

    /// Hard deletes a given stream.
    /// A hard delete writes a tombstone event to the stream, permanently
    /// deleting it. The stream cannot be recreated or written to again.
    /// Tombstone events are written with the event type '$streamDeleted'. When
    /// a hard deleted stream is read, the read will return a StreamDeleted.
    pub async fn tombstone_stream(
        &self,
        stream_name: impl StreamName,
        options: &TombstoneStreamOptions,
    ) -> crate::Result<Option<Position>> {
        commands::tombstone_stream(&self.client, stream_name, options).await
    }

    /// Subscribes to a given stream. This kind of subscription specifies a
    /// starting point (by default, the beginning of a stream). For a regular
    /// stream, that starting point will be an event number. For the system
    /// stream `$all`, it will be a position in the transaction file
    /// (see [`subscribe_to_all`]). This subscription will fetch every event
    /// until the end of the stream, then will dispatch subsequently written
    /// events.
    ///
    /// For example, if a starting point of 50 is specified when a stream has
    /// 100 events in it, the subscriber can expect to see events 51 through
    /// 100, and then any events subsequently written events until such time
    /// as the subscription is dropped or closed.
    ///
    /// [`subscribe_to_all`]: #method.subscribe_to_all_from
    pub async fn subscribe_to_stream(
        &self,
        stream_name: impl StreamName,
        options: &SubscribeToStreamOptions,
    ) -> Subscription {
        commands::subscribe_to_stream(self.client.clone(), stream_name, options)
    }

    /// Like [`subscribe_to_stream`] but specific to system `$all` stream.
    ///
    /// [`subscribe_to_stream`]: #method.subscribe_to_stream
    pub async fn subscribe_to_all(&self, options: &SubscribeToAllOptions) -> Subscription {
        commands::subscribe_to_all(self.client.clone(), options)
    }

    /// Creates a persistent subscription group on a stream.
    ///
    /// Persistent subscriptions are special kind of subscription where the
    /// server remembers the state of the subscription. This allows for many
    /// different modes of operations compared to a regular or catchup
    /// subscription where the client holds the subscription state.
    pub async fn create_persistent_subscription(
        &self,
        stream_name: impl StreamName,
        group_name: impl AsRef<str>,
        options: &PersistentSubscriptionOptions,
    ) -> crate::Result<()> {
        commands::create_persistent_subscription(
            &self.client,
            stream_name,
            group_name.as_ref(),
            options,
        )
        .await
    }

    /// Creates a persistent subscription group on a the $all stream.
    pub async fn create_persistent_subscription_to_all(
        &self,
        group_name: impl AsRef<str>,
        options: &PersistentSubscriptionToAllOptions,
    ) -> crate::Result<()> {
        commands::create_persistent_subscription(&self.client, "", group_name.as_ref(), options)
            .await
    }

    /// Updates a persistent subscription group on a stream.
    pub async fn update_persistent_subscription(
        &self,
        stream_name: impl StreamName,
        group_name: impl AsRef<str>,
        options: &PersistentSubscriptionOptions,
    ) -> crate::Result<()> {
        commands::update_persistent_subscription(
            &self.client,
            stream_name,
            group_name.as_ref(),
            options,
        )
        .await
    }

    /// Updates a persistent subscription group to $all.
    pub async fn update_persistent_subscription_to_all(
        &self,
        group_name: impl AsRef<str>,
        options: &PersistentSubscriptionToAllOptions,
    ) -> crate::Result<()> {
        commands::update_persistent_subscription(&self.client, "", group_name.as_ref(), options)
            .await
    }

    /// Deletes a persistent subscription group on a stream.
    pub async fn delete_persistent_subscription(
        &self,
        stream_name: impl StreamName,
        group_name: impl AsRef<str>,
        options: &DeletePersistentSubscriptionOptions,
    ) -> crate::Result<()> {
        commands::delete_persistent_subscription(
            &self.client,
            stream_name,
            group_name.as_ref(),
            options,
            false,
        )
        .await
    }

    /// Deletes a persistent subscription group on the $all stream.
    pub async fn delete_persistent_subscription_to_all(
        &self,
        group_name: impl AsRef<str>,
        options: &DeletePersistentSubscriptionOptions,
    ) -> crate::Result<()> {
        commands::delete_persistent_subscription(
            &self.client,
            "",
            group_name.as_ref(),
            options,
            true,
        )
        .await
    }

    /// Connects to a persistent subscription group on a stream.
    pub async fn subscribe_to_persistent_subscription(
        &self,
        stream_name: impl StreamName,
        group_name: impl AsRef<str>,
        options: &SubscribeToPersistentSubscriptionOptions,
    ) -> crate::Result<PersistentSubscription> {
        commands::subscribe_to_persistent_subscription(
            &self.client,
            stream_name,
            group_name.as_ref(),
            options,
            false,
        )
        .await
    }

    /// Connects to a persistent subscription group to $all stream.
    pub async fn subscribe_to_persistent_subscription_to_all(
        &self,
        group_name: impl AsRef<str>,
        options: &SubscribeToPersistentSubscriptionOptions,
    ) -> crate::Result<PersistentSubscription> {
        commands::subscribe_to_persistent_subscription(
            &self.client,
            "",
            group_name.as_ref(),
            options,
            true,
        )
        .await
    }

    /// Replays a persistent subscriptions parked events.
    pub async fn replay_parked_messages(
        &self,
        stream_name: impl AsRef<str>,
        group_name: impl AsRef<str>,
        options: &ReplayParkedMessagesOptions,
    ) -> crate::Result<()> {
        commands::replay_parked_messages(
            &self.client,
            &self.http_client,
            commands::RegularStream(stream_name.as_ref().to_string()),
            group_name,
            options,
        )
        .await
    }

    /// Replays a persistent subscriptions to $all parked events.
    pub async fn replay_parked_messages_to_all(
        &self,
        group_name: impl AsRef<str>,
        options: &ReplayParkedMessagesOptions,
    ) -> crate::Result<()> {
        commands::replay_parked_messages(
            &self.client,
            &self.http_client,
            commands::AllStream,
            group_name,
            options,
        )
        .await
    }

    /// Lists all persistent subscriptions to date.
    pub async fn list_all_persistent_subscriptions(
        &self,
        options: &ListPersistentSubscriptionsOptions,
    ) -> crate::Result<Vec<PersistentSubscriptionInfo<RevisionOrPosition>>> {
        commands::list_all_persistent_subscriptions(&self.client, &self.http_client, options).await
    }

    /// List all persistent subscriptions of a specific stream.
    pub async fn list_persistent_subscriptions_for_stream(
        &self,
        stream_name: impl AsRef<str>,
        options: &ListPersistentSubscriptionsOptions,
    ) -> crate::Result<Vec<PersistentSubscriptionInfo<u64>>> {
        commands::list_persistent_subscriptions_for_stream(
            &self.client,
            &self.http_client,
            commands::RegularStream(stream_name.as_ref().to_string()),
            options,
        )
        .await
    }

    /// List all persistent subscriptions of the $all stream.
    pub async fn list_persistent_subscriptions_to_all(
        &self,
        options: &ListPersistentSubscriptionsOptions,
    ) -> crate::Result<Vec<PersistentSubscriptionInfo<Position>>> {
        commands::list_persistent_subscriptions_for_stream(
            &self.client,
            &self.http_client,
            commands::AllStream,
            options,
        )
        .await
    }

    // Gets a specific persistent subscription info.
    pub async fn get_persistent_subscription_info(
        &self,
        stream_name: impl AsRef<str>,
        group_name: impl AsRef<str>,
        options: &GetPersistentSubscriptionInfoOptions,
    ) -> crate::Result<PersistentSubscriptionInfo<u64>> {
        commands::get_persistent_subscription_info(
            &self.client,
            &self.http_client,
            commands::RegularStream(stream_name.as_ref().to_string()),
            group_name,
            options,
        )
        .await
    }

    // Gets a specific persistent subscription info to $all.
    pub async fn get_persistent_subscription_info_to_all(
        &self,
        group_name: impl AsRef<str>,
        options: &GetPersistentSubscriptionInfoOptions,
    ) -> crate::Result<PersistentSubscriptionInfo<Position>> {
        commands::get_persistent_subscription_info(
            &self.client,
            &self.http_client,
            commands::AllStream,
            group_name,
            options,
        )
        .await
    }

    // Restarts the server persistent subscription subsystem.
    pub async fn restart_persistent_subscription_subsystem(
        &self,
        options: &RestartPersistentSubscriptionSubsystem,
    ) -> crate::Result<()> {
        commands::restart_persistent_subscription_subsystem(
            &self.client,
            &self.http_client,
            options,
        )
        .await
    }
}
