#![allow(clippy::large_enum_variant)]
//! Common types used across the library.
use std::cmp::Ordering;
use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::Stream;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;
use tonic::Status;
use uuid::Uuid;

/// Represents a reconnection strategy when a connection has dropped or is
/// about to be created.
#[derive(Copy, Clone, Debug)]
pub enum Retry {
    Indefinitely,
    Only(usize),
}

/// Holds login and password information.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Credentials {
    #[serde(
        serialize_with = "serialize_creds_bytes",
        deserialize_with = "deserialize_creds_bytes"
    )]
    pub(crate) login: Bytes,

    #[serde(
        serialize_with = "serialize_creds_bytes",
        deserialize_with = "deserialize_creds_bytes"
    )]
    pub(crate) password: Bytes,
}

impl Credentials {
    /// Creates a new `Credentials` instance.
    pub fn new<S>(login: S, password: S) -> Credentials
    where
        S: Into<Bytes>,
    {
        Credentials {
            login: login.into(),
            password: password.into(),
        }
    }
}

struct CredsVisitor;

impl<'de> Visitor<'de> for CredsVisitor {
    type Value = Bytes;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ASCII string")
    }

    fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(v.to_string().into())
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(v.to_string().into())
    }

    fn visit_string<E>(self, v: String) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(v.into())
    }
}

fn serialize_creds_bytes<S>(value: &Bytes, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_bytes(value.as_ref())
}

fn deserialize_creds_bytes<'de, D>(deserializer: D) -> std::result::Result<Bytes, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(CredsVisitor)
}

/// Constants used for expected version control.
/// The use of expected version can be a bit tricky especially when discussing
/// assurances given by the GetEventStore server.
///
/// The GetEventStore server will assure idempotency for all operations using
/// any value in `ExpectedRevision` except `ExpectedRevision::Any`. When using
/// `ExpectedRevision::Any`, the GetEventStore server will do its best to assure
/// idempotency but will not guarantee idempotency.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ExpectedRevision {
    /// This write should not conflict with anything and should always succeed.
    Any,

    /// The stream should exist. If it or a metadata stream does not exist,
    /// treats that as a concurrency problem.
    StreamExists,

    /// The stream being written to should not yet exist. If it does exist,
    /// treats that as a concurrency problem.
    NoStream,

    /// States that the last event written to the stream should have an event
    /// number matching your expected value.
    Exact(u64),
}

/// A structure referring to a potential logical record position in the
/// EventStoreDB transaction file.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Position {
    /// Commit position of the record.
    pub commit: u64,

    /// Prepare position of the record.
    pub prepare: u64,
}

impl Position {
    /// Points to the begin of the transaction file.
    pub fn start() -> Self {
        Position {
            commit: 0,
            prepare: 0,
        }
    }

    /// Points to the end of the transaction file.
    pub fn end() -> Self {
        Position {
            commit: u64::MAX,
            prepare: u64::MAX,
        }
    }
}

impl PartialOrd for Position {
    fn partial_cmp(&self, other: &Position) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Position {
    fn cmp(&self, other: &Position) -> Ordering {
        self.commit
            .cmp(&other.commit)
            .then(self.prepare.cmp(&other.prepare))
    }
}

/// Returned after writing to a stream.
#[derive(Debug)]
pub struct WriteResult {
    /// Next expected version of the stream.
    pub next_expected_version: u64,

    /// `Position` of the write.
    pub position: Position,
}

#[derive(Debug, Clone, Copy)]
pub enum StreamPosition<A> {
    Start,
    End,
    Point(A),
}

/// Enumeration detailing the possible outcomes of reading a stream.
#[derive(Debug)]
pub enum ReadEventStatus<A> {
    NotFound,
    NoStream,
    Deleted,
    Success(A),
}

/// Represents the result of looking up a specific event number from a stream.
#[derive(Debug)]
pub struct ReadEventResult {
    /// Stream where the event orignates from.
    pub stream_id: String,

    /// Sequence number of the event.
    pub event_number: i64,

    /// Event data.
    pub event: ResolvedEvent,
}

/// Represents a previously written event.
#[derive(Debug)]
pub struct RecordedEvent {
    /// The event stream that events belongs to.
    pub stream_id: String,

    /// Unique identifier representing this event.
    pub id: Uuid,

    /// Number of this event in the stream.
    pub revision: u64,

    /// Type of this event.
    pub event_type: String,

    /// Payload of this event.
    pub data: Bytes,

    /// Representing the metadata associated with this event.
    pub metadata: HashMap<String, String>,

    /// Representing the user-defined metadata associated with this event.
    pub custom_metadata: Bytes,

    /// Indicates wheter the content is internally marked as JSON.
    pub is_json: bool,

    /// An event position in the $all stream.
    pub position: Position,
}

impl RecordedEvent {
    /// Tries to decode this event payload as a JSON object.
    pub fn as_json<'a, T>(&'a self) -> serde_json::Result<T>
    where
        T: Deserialize<'a>,
    {
        serde_json::from_slice(&self.data[..])
    }
}

/// A structure representing a single event or an resolved link event.
#[derive(Debug)]
pub struct ResolvedEvent {
    /// The event, or the resolved link event if this `ResolvedEvent` is a link
    /// event.
    pub event: Option<RecordedEvent>,

    /// The link event if this `ResolvedEvent` is a link event.
    pub link: Option<RecordedEvent>,

    pub commit_position: Option<u64>,
}

impl ResolvedEvent {
    /// If it's a link event with its associated resolved event.
    pub fn is_resolved(&self) -> bool {
        self.event.is_some() && self.link.is_some()
    }

    /// Returns the event that was read or which triggered the subscription.
    /// If this `ResolvedEvent` represents a link event, the link will be the
    /// original event, otherwise it will be the event.
    ///
    pub fn get_original_event(&self) -> &RecordedEvent {
        self.link.as_ref().unwrap_or_else(|| {
            self.event
                .as_ref()
                .expect("[get_original_event] Not supposed to happen!")
        })
    }

    /// Returns the stream id of the original event.
    pub fn get_original_stream_id(&self) -> &str {
        &self.get_original_event().stream_id
    }
}

/// Represents stream metadata as a series of properties for system data and
/// user-defined metadata.
#[derive(Debug, Clone)]
pub enum StreamMetadataResult {
    Deleted { stream: String },
    NotFound { stream: String },
    Success(Box<VersionedMetadata>),
}

/// Represents a stream metadata.
#[derive(Debug, Clone)]
pub struct VersionedMetadata {
    /// Metadata's stream.
    pub stream: String,

    /// Metadata's version.
    pub version: i64,

    /// Metadata properties.
    pub metadata: StreamMetadata,
}

/// Represents the direction of read operation (both from '$all' and a regular
/// stream).
#[derive(Copy, Clone, Debug)]
pub(crate) enum ReadDirection {
    Forward,
    Backward,
}

/// Represents the errors that can arise when reading a stream.
#[derive(Debug, Clone)]
pub enum ReadStreamError {
    NoStream(String),
    StreamDeleted(String),
    NotModified(String),
    Error(String),
    AccessDenied(String),
}

/// Represents the result of reading a stream.
#[derive(Debug, Clone)]
pub enum ReadStreamStatus<A> {
    Success(A),
    Error(ReadStreamError),
}

/// Holds data of event about to be sent to the server.
#[derive(Clone)]
pub struct EventData {
    pub(crate) payload: Bytes,
    pub(crate) id_opt: Option<Uuid>,
    pub(crate) metadata: HashMap<String, String>,
    pub(crate) custom_metadata: Option<Bytes>,
}

impl EventData {
    /// Creates an event with a JSON payload.
    pub fn json<S, P>(event_type: S, payload: P) -> serde_json::Result<EventData>
    where
        P: Serialize,
        S: AsRef<str>,
    {
        let payload = Bytes::from(serde_json::to_vec(&payload)?);
        let mut metadata = HashMap::new();
        metadata.insert("type".to_owned(), event_type.as_ref().to_owned());
        metadata.insert("content-type".to_owned(), "application/json".to_owned());

        Ok(EventData {
            payload,
            id_opt: None,
            metadata,
            custom_metadata: None,
        })
    }

    /// Creates an event with a raw binary payload.
    pub fn binary<S>(event_type: S, payload: Bytes) -> Self
    where
        S: AsRef<str>,
    {
        let mut metadata = HashMap::new();
        metadata.insert("type".to_owned(), event_type.as_ref().to_owned());
        metadata.insert(
            "content-type".to_owned(),
            "application/octet-stream".to_owned(),
        );

        EventData {
            payload,
            id_opt: None,
            metadata,
            custom_metadata: None,
        }
    }

    /// Set an id to this event. By default, the id will be generated
    pub fn id(self, value: Uuid) -> Self {
        EventData {
            id_opt: Some(value),
            ..self
        }
    }

    /// Assigns a JSON metadata to this event.
    pub fn metadata_as_json<P>(self, payload: P) -> serde_json::Result<EventData>
    where
        P: Serialize,
    {
        let custom_metadata = Some(Bytes::from(serde_json::to_vec(&payload)?));
        Ok(EventData {
            custom_metadata,
            ..self
        })
    }

    /// Assigns a raw binary metadata to this event.
    pub fn metadata(self, payload: Bytes) -> EventData {
        EventData {
            custom_metadata: Some(payload),
            ..self
        }
    }
}

/// Used to facilitate the creation of a stream's metadata.
#[derive(Default)]
pub struct StreamMetadataBuilder {
    max_count: Option<u64>,
    max_age: Option<Duration>,
    truncate_before: Option<u64>,
    cache_control: Option<Duration>,
    acl: Option<StreamAcl>,
    properties: HashMap<String, serde_json::Value>,
}

impl StreamMetadataBuilder {
    /// Creates a `StreamMetadata` initialized with default values.
    pub fn new() -> StreamMetadataBuilder {
        Default::default()
    }

    /// Sets a sliding window based on the number of items in the stream.
    /// When data reaches a certain length it disappears automatically
    /// from the stream and is considered eligible for scavenging.
    pub fn max_count(self, value: u64) -> StreamMetadataBuilder {
        StreamMetadataBuilder {
            max_count: Some(value),
            ..self
        }
    }

    /// Sets a sliding window based on dates. When data reaches a certain age
    /// it disappears automatically from the stream and is considered
    /// eligible for scavenging.
    pub fn max_age(self, value: Duration) -> StreamMetadataBuilder {
        StreamMetadataBuilder {
            max_age: Some(value),
            ..self
        }
    }

    /// Sets the event number from which previous events can be scavenged.
    pub fn truncate_before(self, value: u64) -> StreamMetadataBuilder {
        StreamMetadataBuilder {
            truncate_before: Some(value),
            ..self
        }
    }

    /// This controls the cache of the head of a stream. Most URIs in a stream
    /// are infinitely cacheable but the head by default will not cache. It
    /// may be preferable in some situations to set a small amount of caching
    /// on the head to allow intermediaries to handle polls (say 10 seconds).
    pub fn cache_control(self, value: Duration) -> StreamMetadataBuilder {
        StreamMetadataBuilder {
            cache_control: Some(value),
            ..self
        }
    }

    /// Sets the ACL of a stream.
    pub fn acl(self, value: StreamAcl) -> StreamMetadataBuilder {
        StreamMetadataBuilder {
            acl: Some(value),
            ..self
        }
    }

    /// Adds user-defined property in the stream metadata.
    pub fn insert_custom_property<V>(mut self, key: String, value: V) -> StreamMetadataBuilder
    where
        V: Serialize,
    {
        let serialized = serde_json::to_value(value).unwrap();
        let _ = self.properties.insert(key, serialized);

        self
    }

    /// Returns a properly configured `StreamMetaData`.
    pub fn build(self) -> StreamMetadata {
        StreamMetadata {
            max_count: self.max_count,
            max_age: self.max_age,
            truncate_before: self.truncate_before,
            cache_control: self.cache_control,
            acl: self.acl.unwrap_or_default(),
            custom_properties: self.properties,
        }
    }
}

/// Represents stream metadata with strongly types properties for system values
/// and a dictionary-like interface for custom values.
#[derive(Debug, Default, Clone)]
pub struct StreamMetadata {
    /// A sliding window based on the number of items in the stream. When data reaches
    /// a certain length it disappears automatically from the stream and is considered
    /// eligible for scavenging.
    pub max_count: Option<u64>,

    /// A sliding window based on dates. When data reaches a certain age it disappears
    /// automatically from the stream and is considered eligible for scavenging.
    pub max_age: Option<Duration>,

    /// The event number from which previous events can be scavenged. This is
    /// used to implement soft-deletion of streams.
    pub truncate_before: Option<u64>,

    /// Controls the cache of the head of a stream. Most URIs in a stream are infinitely
    /// cacheable but the head by default will not cache. It may be preferable
    /// in some situations to set a small amount of caching on the head to allow
    /// intermediaries to handle polls (say 10 seconds).
    pub cache_control: Option<Duration>,

    /// The access control list for the stream.
    pub acl: StreamAcl,

    /// An enumerable of key-value pairs of keys to JSON value for
    /// user-provided metadata.
    pub custom_properties: HashMap<String, serde_json::Value>,
}

impl StreamMetadata {
    /// Initializes a fresh stream metadata builder.
    pub fn builder() -> StreamMetadataBuilder {
        StreamMetadataBuilder::new()
    }
}

/// Represents an access control list for a stream.
#[derive(Default, Debug, Clone)]
pub struct StreamAcl {
    /// Roles and users permitted to read the stream.
    pub read_roles: Option<Vec<String>>,

    /// Roles and users permitted to write to the stream.
    pub write_roles: Option<Vec<String>>,

    /// Roles and users permitted to delete to the stream.
    pub delete_roles: Option<Vec<String>>,

    /// Roles and users permitted to read stream metadata.
    pub meta_read_roles: Option<Vec<String>>,

    /// Roles and users permitted to write stream metadata.
    pub meta_write_roles: Option<Vec<String>>,
}

/// Read part of a persistent subscription, isomorphic to a stream of events.
pub struct PersistentSubRead {
    pub(crate) inner: Box<dyn Stream<Item = PersistentSubEvent> + Send + Unpin>,
}

impl PersistentSubRead {
    pub fn into_inner(self) -> Box<dyn Stream<Item = PersistentSubEvent> + Send + Unpin> {
        self.inner
    }

    pub async fn read_next(&mut self) -> Option<PersistentSubEvent> {
        use futures::stream::StreamExt;

        self.inner.next().await
    }
}

/// Events related to a subscription.
#[derive(Debug)]
pub enum SubEvent {
    /// Indicates the subscription has been confirmed by the server. The String value represents
    /// the subscription id.
    Confirmed(String),

    /// An event notification from the server.
    EventAppeared(ResolvedEvent),

    /// Indicates a checkpoint has been created. Related to subscription to $all when
    /// filters are used.
    Checkpoint(Position),
}

#[derive(Debug)]
pub struct PersistentSubEvent {
    pub inner: ResolvedEvent,
    pub retry_count: usize,
}

/// Gathers every possible Nak actions.
#[derive(Debug, PartialEq, Eq)]
pub enum NakAction {
    /// Client unknown on action. Let server decide.
    Unknown,

    /// Park message do not resend. Put on poison queue.
    Park,

    /// Explicity retry the message.
    Retry,

    /// Skip this message do not resend do not put in poison queue.
    Skip,

    /// Stop the subscription.
    Stop,
}

/// System supported consumer strategies for use with persistent subscriptions.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum SystemConsumerStrategy {
    /// Distributes events to a single client until the bufferSize is reached.
    /// After which the next client is selected in a round robin style,
    /// and the process is repeated.
    DispatchToSingle,

    /// Distributes events to all clients evenly. If the client buffer-size
    /// is reached the client is ignored until events are
    /// acknowledged/not acknowledged.
    RoundRobin,

    /// For use with an indexing projection such as the system $by_category
    /// projection. Event Store inspects event for its source stream id,
    /// hashing the id to one of 1024 buckets assigned to individual clients.
    /// When a client disconnects it's buckets are assigned to other clients.
    /// When a client connects, it is assigned some of the existing buckets.
    /// This naively attempts to maintain a balanced workload.
    /// The main aim of this strategy is to decrease the likelihood of
    /// concurrency and ordering issues while maintaining load balancing.
    /// This is not a guarantee, and you should handle the usual ordering
    /// and concurrency issues.
    Pinned,
}

/// Gathers every persistent subscription property.
#[derive(Debug, Clone, Copy)]
pub struct PersistentSubscriptionSettings {
    /// Whether or not the persistent subscription should resolve link
    /// events to their linked events.
    pub resolve_link_tos: bool,

    /// Where the subscription should start from (event number).
    pub revision: u64,

    /// Whether or not in depth latency statistics should be tracked on this
    /// subscription.
    pub extra_stats: bool,

    /// The amount of time after which a message should be considered to be
    /// timeout and retried.
    pub message_timeout: Duration,

    /// The maximum number of retries (due to timeout) before a message get
    /// considered to be parked.
    pub max_retry_count: i32,

    /// The size of the buffer listenning to live messages as they happen.
    pub live_buffer_size: i32,

    /// The number of events read at a time when paging in history.
    pub read_batch_size: i32,

    /// The number of events to cache when paging through history.
    pub history_buffer_size: i32,

    /// The amount of time to try checkpoint after.
    pub checkpoint_after: Duration,

    /// The minimum number of messages to checkpoint.
    pub min_checkpoint_count: i32,

    /// The maximum number of messages to checkpoint. If this number is reached
    /// , a checkpoint will be forced.
    pub max_checkpoint_count: i32,

    /// The maximum number of subscribers allowed.
    pub max_subscriber_count: i32,

    /// The strategy to use for distributing events to client consumers.
    pub named_consumer_strategy: SystemConsumerStrategy,
}

impl PersistentSubscriptionSettings {
    pub fn default() -> PersistentSubscriptionSettings {
        PersistentSubscriptionSettings {
            resolve_link_tos: false,
            revision: 0,
            extra_stats: false,
            message_timeout: Duration::from_secs(30),
            max_retry_count: 10,
            live_buffer_size: 500,
            read_batch_size: 20,
            history_buffer_size: 500,
            checkpoint_after: Duration::from_secs(2),
            min_checkpoint_count: 10,
            max_checkpoint_count: 1_000,
            max_subscriber_count: 0, // Means their is no limit.
            named_consumer_strategy: SystemConsumerStrategy::RoundRobin,
        }
    }
}

impl Default for PersistentSubscriptionSettings {
    fn default() -> PersistentSubscriptionSettings {
        PersistentSubscriptionSettings::default()
    }
}

/// Represents the different scenarios that could happen when performing
/// a persistent subscription.
#[derive(Debug, Eq, PartialEq)]
pub enum PersistActionResult {
    Success,
    Failure(PersistActionError),
}

impl PersistActionResult {
    /// Checks if the persistent action succeeded.
    pub fn is_success(&self) -> bool {
        matches!(*self, PersistActionResult::Success)
    }

    /// Checks if the persistent action failed.
    pub fn is_failure(&self) -> bool {
        !self.is_success()
    }
}

/// Enumerates all persistent action exceptions.
#[derive(Debug, Eq, PartialEq)]
pub enum PersistActionError {
    /// The action failed.
    Fail,

    /// Happens when creating a persistent subscription on a stream with a
    /// group name already taken.
    AlreadyExists,

    /// An operation tried to do something on a persistent subscription or a
    /// stream that don't exist.
    DoesNotExist,

    /// The current user is not allowed to operate on the supplied stream or
    /// persistent subscription.
    AccessDenied,
}

/// Indicates which order of preferred nodes for connecting to.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum NodePreference {
    /// When attempting connection, prefers leader nodes.
    Leader,

    /// When attempting connection, prefers follower nodes.
    Follower,

    /// When attempting connection, has no node preference.
    Random,

    /// When attempting connection, prefers read-replica nodes.
    ReadOnlyReplica,
}

impl Default for NodePreference {
    fn default() -> Self {
        NodePreference::Random
    }
}

impl std::fmt::Display for NodePreference {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use self::NodePreference::*;

        match self {
            Leader => write!(f, "Leader"),
            Follower => write!(f, "Follower"),
            Random => write!(f, "Random"),
            ReadOnlyReplica => write!(f, "ReadOnlyReplica"),
        }
    }
}

#[derive(Debug)]
pub(crate) enum Either<A, B> {
    Left(A),
    Right(B),
}

impl<A, B> Either<A, B> {
    pub(crate) fn as_ref(&self) -> Either<&A, &B> {
        match self {
            Either::Left(a) => Either::Left(&a),
            Either::Right(b) => Either::Right(&b),
        }
    }
}

#[derive(Debug)]
pub(crate) struct DnsClusterSettings {
    pub(crate) endpoint: Endpoint,
}

#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, Clone, Copy)]
pub(crate) enum LookupType {
    #[serde(rename = "a")]
    LookupA,
    #[serde(rename = "srv")]
    LookupSRV,
}

impl Default for LookupType {
    fn default() -> Self {
        LookupType::LookupA
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
/// Actual revision of a stream.
pub enum CurrentRevision {
    /// The last event's number.
    Current(u64),

    /// The stream doesn't exist.
    NoStream,
}

#[derive(Clone, Debug, Copy, Eq, PartialEq)]
pub struct WrongExpectedVersion {
    pub current: CurrentRevision,
    pub expected: ExpectedRevision,
}

impl std::fmt::Display for WrongExpectedVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "WrongExpectedVersion: expected: {:?}, got: {:?}",
            self.expected, self.current
        )
    }
}

impl std::error::Error for WrongExpectedVersion {}

#[derive(Debug, Clone, Eq, Ord, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct Endpoint {
    pub host: String,
    pub port: u32,
}

#[derive(Error, Debug)]
/// EventStoreDB command error.
pub enum Error {
    #[error("Server-side error.")]
    ServerError,
    #[error("You tried to execute a command that requires a leader node on a follower node. New leader: ")]
    NotLeaderException(Endpoint),
    #[error("Connection is closed.")]
    ConnectionClosed,
    #[error("Unmapped gRPC error: {0}.")]
    Grpc(Status),
    #[error("gRPC connection error: {0}")]
    GrpcConnectionError(GrpcConnectionError),
}

impl Error {
    pub fn from_grpc(status: Status) -> Self {
        match status.code() {
            tonic::Code::Unavailable => Error::ServerError,
            _ => {
                let metadata = status.metadata();
                if let Some("not-leader") = metadata.get("exception").and_then(|e| e.to_str().ok())
                {
                    let endpoint = metadata
                        .get("leader-endpoint-host")
                        .zip(metadata.get("leader-endpoint-port"))
                        .and_then(|(host, port)| {
                            let host = host.to_str().ok()?;
                            let port = port.to_str().ok()?;
                            let host = host.to_string();
                            let port = port.parse().ok()?;

                            Some(Endpoint { host, port })
                        });

                    if let Some(leader) = endpoint {
                        return Error::NotLeaderException(leader);
                    }
                }

                Error::Grpc(status)
            }
        }
    }
}

#[derive(Error, Debug)]
/// EventStoreDB command error.
pub enum GrpcConnectionError {
    #[error("Max discovery attempt count reached. count: {0}")]
    MaxDiscoveryAttemptReached(usize),
    #[error("Unmapped gRPC connection error: {0}.")]
    Grpc(Status),
}

pub type Result<A> = std::result::Result<A, Error>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadResult<A> {
    Ok(A),
    StreamNotFound(String),
}

impl<A> ReadResult<A> {
    /// Maps an `ReadResult<A>` to `ReadResult<B>` by applying a function to a contained value.
    pub fn map<B, F>(self, f: F) -> ReadResult<B>
    where
        F: FnOnce(A) -> B,
    {
        match self {
            ReadResult::Ok(a) => ReadResult::Ok(f(a)),
            ReadResult::StreamNotFound(s) => ReadResult::StreamNotFound(s),
        }
    }

    /// Converts from `ReadResult<A>` to [`Option<A>`].
    ///
    /// Converts `self` into an [`Option<A>`], consuming `self`,
    /// and discarding the success value, if any.
    ///
    /// [`Option<A>`]: Option
    pub fn ok(self) -> Option<A> {
        match self {
            ReadResult::Ok(a) => Some(a),
            ReadResult::StreamNotFound(_) => None,
        }
    }

    /// Returns `true` if the result is [`ReadResult::Ok`].
    pub const fn is_ok(&self) -> bool {
        matches!(*self, ReadResult::Ok(_))
    }

    /// Returns `true` if the result is [`ReadResult::StreamNotFound`].
    pub const fn is_not_found(&self) -> bool {
        !self.is_ok()
    }

    /// Returns the contained [`Ok`] value, consuming the `self` value.
    #[inline]
    #[track_caller]
    pub fn unwrap(self) -> A {
        match self {
            ReadResult::Ok(a) => a,
            ReadResult::StreamNotFound(s) => panic!(
                "called `ReadResult::unwrap()` on an `StreamNotFound({})` value",
                &s
            ),
        }
    }
}

#[async_trait]
pub trait ToCount<'a> {
    type Selection;
    fn to_count(&self) -> usize;
    async fn select(
        self,
        stream: BoxStream<'a, crate::Result<ResolvedEvent>>,
    ) -> crate::Result<Self::Selection>;
}

#[async_trait]
impl<'a> ToCount<'a> for usize {
    type Selection = BoxStream<'a, crate::Result<ResolvedEvent>>;

    fn to_count(&self) -> usize {
        *self
    }

    async fn select(
        self,
        stream: BoxStream<'a, crate::Result<ResolvedEvent>>,
    ) -> crate::Result<Self::Selection> {
        Ok(stream)
    }
}

/// Get all the stream's events.
pub struct All;

#[async_trait]
impl<'a> ToCount<'a> for All {
    type Selection = BoxStream<'a, crate::Result<ResolvedEvent>>;

    fn to_count(&self) -> usize {
        usize::MAX
    }

    async fn select(
        self,
        stream: BoxStream<'a, crate::Result<ResolvedEvent>>,
    ) -> crate::Result<Self::Selection> {
        Ok(stream)
    }
}

/// Get only one stream's event.
pub struct Single;

#[async_trait]
impl<'a> ToCount<'a> for Single {
    type Selection = Option<ResolvedEvent>;

    fn to_count(&self) -> usize {
        1
    }

    async fn select(
        self,
        mut stream: BoxStream<'a, crate::Result<ResolvedEvent>>,
    ) -> crate::Result<Self::Selection> {
        use futures::stream::TryStreamExt;

        stream.try_next().await
    }
}

#[derive(Debug, Clone)]
pub struct SubscriptionFilter {
    pub(crate) based_on_stream: bool,
    pub(crate) max: Option<u32>,
    pub(crate) regex: Option<String>,
    pub(crate) prefixes: Vec<String>,
}

impl SubscriptionFilter {
    pub fn on_stream_name() -> Self {
        SubscriptionFilter {
            based_on_stream: true,
            max: None,
            regex: None,
            prefixes: Vec::new(),
        }
    }

    pub fn on_event_type() -> Self {
        let mut temp = SubscriptionFilter::on_stream_name();
        temp.based_on_stream = false;

        temp
    }

    pub fn max(self, max: u32) -> Self {
        SubscriptionFilter {
            max: Some(max),
            ..self
        }
    }

    pub fn regex<A: AsRef<str>>(self, regex: A) -> Self {
        SubscriptionFilter {
            regex: Some(regex.as_ref().to_string()),
            ..self
        }
    }

    pub fn add_prefix<A: AsRef<str>>(mut self, prefix: A) -> Self {
        self.prefixes.push(prefix.as_ref().to_string());
        self
    }
}
