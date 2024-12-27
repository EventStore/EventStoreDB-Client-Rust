#![allow(clippy::large_enum_variant)]
//! Common types used across the library.
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::time::Duration;

use crate::event_store;
use crate::operations::gossip::VNodeState;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::de::SeqAccess;
use serde::{de::Visitor, ser::SerializeSeq};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;
use tonic::{Code, Status};
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

impl std::fmt::Display for ExpectedRevision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// A structure referring to a potential logical record position in the
/// KurrentDB transaction file.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Position {
    /// Commit position of the record.
    pub commit: u64,

    /// Prepare position of the record.
    pub prepare: u64,
}

impl std::fmt::Display for Position {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "C:{}/P:{}", self.commit, self.prepare)
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamPosition<A> {
    Start,
    End,
    Position(A),
}

impl<A> StreamPosition<A> {
    pub fn map<F, B>(self, f: F) -> StreamPosition<B>
    where
        F: FnOnce(A) -> B,
    {
        match self {
            StreamPosition::Start => StreamPosition::Start,
            StreamPosition::End => StreamPosition::End,
            StreamPosition::Position(a) => StreamPosition::Position(f(a)),
        }
    }
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

    /// When the event was created in the database.
    pub created: DateTime<Utc>,
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
    Deleted,
    NotFound,
    Success(Box<VersionedMetadata>),
}

impl StreamMetadataResult {
    pub fn is_deleted(&self) -> bool {
        if let StreamMetadataResult::Deleted = self {
            return true;
        }

        false
    }

    pub fn is_not_found(&self) -> bool {
        if let StreamMetadataResult::NotFound = self {
            return true;
        }

        false
    }

    pub fn is_success(&self) -> bool {
        if let StreamMetadataResult::Success(_) = self {
            return true;
        }

        false
    }
}

/// Represents a stream metadata.
#[derive(Debug, Clone)]
pub struct VersionedMetadata {
    pub(crate) stream: String,
    pub(crate) version: u64,
    pub(crate) metadata: StreamMetadata,
}

impl VersionedMetadata {
    /// Metadata's stream.
    pub fn stream_name(&self) -> &str {
        self.stream.as_str()
    }

    /// Metadata's version.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Metadata properties.
    pub fn metadata(&self) -> &StreamMetadata {
        &self.metadata
    }
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
#[derive(Clone, Debug)]
pub struct EventData {
    pub(crate) payload: Bytes,
    pub(crate) id_opt: Option<Uuid>,
    pub(crate) metadata: HashMap<String, String>,
    pub(crate) custom_metadata: Option<Bytes>,
}

impl EventData {
    /// Creates an event with a JSON payload.
    pub fn json<S, P>(event_type: S, payload: &P) -> serde_json::Result<EventData>
    where
        P: Serialize,
        S: AsRef<str>,
    {
        let payload = Bytes::from(serde_json::to_vec(payload)?);
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
    pub fn metadata_as_json<P>(self, payload: &P) -> serde_json::Result<EventData>
    where
        P: Serialize,
    {
        let custom_metadata = Some(Bytes::from(serde_json::to_vec(payload)?));
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
    acl: Option<Acl>,
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
    pub fn acl(self, value: Acl) -> StreamMetadataBuilder {
        StreamMetadataBuilder {
            acl: Some(value),
            ..self
        }
    }

    /// Adds user-defined property in the stream metadata.
    pub fn insert_custom_property<V>(
        mut self,
        key: impl AsRef<str>,
        value: V,
    ) -> StreamMetadataBuilder
    where
        V: Serialize,
    {
        let serialized = serde_json::to_value(value).unwrap();
        let _ = self.properties.insert(key.as_ref().to_string(), serialized);

        self
    }

    /// Returns a properly configured `StreamMetaData`.
    pub fn build(self) -> StreamMetadata {
        StreamMetadata {
            max_count: self.max_count,
            max_age: self.max_age,
            truncate_before: self.truncate_before,
            cache_control: self.cache_control,
            acl: self.acl,
            custom_properties: self.properties,
        }
    }
}

/// Represents stream metadata with strongly types properties for system values
/// and a dictionary-like interface for custom values.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamMetadata {
    /// A sliding window based on the number of items in the stream. When data reaches
    /// a certain length it disappears automatically from the stream and is considered
    /// eligible for scavenging.
    #[serde(rename = "$maxCount", skip_serializing_if = "Option::is_none", default)]
    pub max_count: Option<u64>,

    /// A sliding window based on dates. When data reaches a certain age it disappears
    /// automatically from the stream and is considered eligible for scavenging.
    #[serde(
        rename = "$maxAge",
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration",
        default
    )]
    pub max_age: Option<Duration>,

    /// The event number from which previous events can be scavenged. This is
    /// used to implement soft-deletion of streams.
    #[serde(rename = "$tb", skip_serializing_if = "Option::is_none", default)]
    pub truncate_before: Option<u64>,

    /// Controls the cache of the head of a stream. Most URIs in a stream are infinitely
    /// cacheable but the head by default will not cache. It may be preferable
    /// in some situations to set a small amount of caching on the head to allow
    /// intermediaries to handle polls (say 10 seconds).
    #[serde(
        rename = "$cacheControl",
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration",
        default
    )]
    pub cache_control: Option<Duration>,

    /// The access control list for the stream.
    #[serde(
        rename = "$acl",
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_acl",
        serialize_with = "serialize_acl",
        default
    )]
    pub acl: Option<Acl>,

    /// An enumerable of key-value pairs of keys to JSON value for
    /// user-provided metadata.
    #[serde(flatten, skip_serializing_if = "HashMap::is_empty", default)]
    pub custom_properties: HashMap<String, serde_json::Value>,
}

fn serialize_duration<S>(
    src: &Option<Duration>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if let Some(duration) = src.as_ref() {
        serializer.serialize_u64(duration.as_millis() as u64)
    } else {
        serializer.serialize_none()
    }
}

fn deserialize_duration<'de, D>(deserializer: D) -> std::result::Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_u64(DurationVisitor)
}

fn serialize_acl<S>(src: &Option<Acl>, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if let Some(ref acl) = src.as_ref() {
        match acl {
            Acl::UserStream => serializer.serialize_str("$userStreamAcl"),
            Acl::SystemStream => serializer.serialize_str("$systemStreamAcl"),
            Acl::Stream(acl) => serializer.serialize_some(acl),
        }
    } else {
        serializer.serialize_none()
    }
}

fn deserialize_acl<'de, D>(deserializer: D) -> std::result::Result<Option<Acl>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(AclVisitor)
}

struct DurationVisitor;

impl Visitor<'_> for DurationVisitor {
    type Value = Option<Duration>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a time duration in milliseconds")
    }

    fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E> {
        Ok(Some(Duration::from_millis(value)))
    }

    fn visit_none<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(None)
    }
}

struct AclVisitor;

impl<'de> Visitor<'de> for AclVisitor {
    type Value = Option<Acl>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a KurrentDB ACL")
    }

    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match value {
            "$userStreamAcl" => Ok(Some(Acl::UserStream)),
            "$systemStreamAcl" => Ok(Some(Acl::SystemStream)),
            unknown => Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(unknown),
                &self,
            )),
        }
    }

    fn visit_none<E>(self) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(None)
    }

    fn visit_map<M>(self, map: M) -> std::result::Result<Self::Value, M::Error>
    where
        M: serde::de::MapAccess<'de>,
    {
        let stream_acl =
            Deserialize::deserialize(serde::de::value::MapAccessDeserializer::new(map))?;

        Ok(Some(Acl::Stream(stream_acl)))
    }
}

impl StreamMetadata {
    pub fn new() -> Self {
        StreamMetadata::default()
    }

    /// Initializes a fresh stream metadata builder.
    pub fn builder() -> StreamMetadataBuilder {
        StreamMetadataBuilder::new()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Acl {
    UserStream,
    SystemStream,
    Stream(StreamAcl),
}

#[derive(Default)]
pub struct StreamAclBuilder {
    read_roles: Option<Vec<String>>,
    write_roles: Option<Vec<String>>,
    delete_roles: Option<Vec<String>>,
    meta_read_roles: Option<Vec<String>>,
    meta_write_roles: Option<Vec<String>>,
}

impl StreamAclBuilder {
    pub fn new() -> Self {
        StreamAclBuilder::default()
    }

    pub fn add_read_roles(mut self, role: impl AsRef<str>) -> Self {
        self.read_roles
            .get_or_insert_with(Vec::new)
            .push(role.as_ref().to_string());

        self
    }

    pub fn add_write_roles(mut self, role: impl AsRef<str>) -> Self {
        self.write_roles
            .get_or_insert_with(Vec::new)
            .push(role.as_ref().to_string());

        self
    }

    pub fn add_delete_roles(mut self, role: impl AsRef<str>) -> Self {
        self.delete_roles
            .get_or_insert_with(Vec::new)
            .push(role.as_ref().to_string());

        self
    }

    pub fn add_meta_read_roles(mut self, role: impl AsRef<str>) -> Self {
        self.meta_read_roles
            .get_or_insert_with(Vec::new)
            .push(role.as_ref().to_string());

        self
    }

    pub fn add_meta_write_roles(mut self, role: impl AsRef<str>) -> Self {
        self.meta_write_roles
            .get_or_insert_with(Vec::new)
            .push(role.as_ref().to_string());

        self
    }

    pub fn build(self) -> StreamAcl {
        StreamAcl {
            read_roles: self.read_roles,
            write_roles: self.write_roles,
            delete_roles: self.delete_roles,
            meta_read_roles: self.meta_read_roles,
            meta_write_roles: self.meta_write_roles,
        }
    }
}

/// Represents an access control list for a stream.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamAcl {
    /// Roles and users permitted to read the stream.
    #[serde(
        rename = "$r",
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_roles",
        serialize_with = "serialize_roles",
        default
    )]
    pub read_roles: Option<Vec<String>>,

    /// Roles and users permitted to write to the stream.
    #[serde(
        rename = "$w",
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_roles",
        serialize_with = "serialize_roles",
        default
    )]
    pub write_roles: Option<Vec<String>>,

    /// Roles and users permitted to delete to the stream.
    #[serde(
        rename = "$d",
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_roles",
        serialize_with = "serialize_roles",
        default
    )]
    pub delete_roles: Option<Vec<String>>,

    /// Roles and users permitted to read stream metadata.
    #[serde(
        rename = "$mr",
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_roles",
        serialize_with = "serialize_roles",
        default
    )]
    pub meta_read_roles: Option<Vec<String>>,

    /// Roles and users permitted to write stream metadata.
    #[serde(
        rename = "$mw",
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_roles",
        serialize_with = "serialize_roles",
        default
    )]
    pub meta_write_roles: Option<Vec<String>>,
}

fn serialize_roles<S>(
    src: &Option<Vec<String>>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if let Some(roles) = src.as_ref() {
        if roles.len() == 1 {
            serializer.serialize_str(roles.first().unwrap().as_str())
        } else {
            let mut seq = serializer.serialize_seq(Some(roles.len()))?;

            for role in roles.iter() {
                seq.serialize_element(role.as_str())?;
            }

            seq.end()
        }
    } else {
        serializer.serialize_none()
    }
}

struct RolesVisitor;

impl<'de> Visitor<'de> for RolesVisitor {
    type Value = Option<Vec<String>>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a KurrentDB role or role list")
    }

    fn visit_none<E>(self) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(None)
    }

    fn visit_str<E>(self, value: &str) -> std::result::Result<Option<Vec<String>>, E>
    where
        E: serde::de::Error,
    {
        Ok(Some(vec![value.to_string()]))
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Option<Vec<String>>, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut roles = Vec::new();

        while let Some(role) = seq.next_element::<String>()? {
            roles.push(role);
        }

        Ok(Some(roles))
    }
}

fn deserialize_roles<'de, D>(deserializer: D) -> std::result::Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(RolesVisitor)
}

#[cfg(test)]
mod metadata_tests {
    use std::time::Duration;

    use super::{Acl, StreamAclBuilder, StreamMetadata, StreamMetadataBuilder};

    #[test]
    fn isomorphic_1() -> Result<(), Box<dyn std::error::Error>> {
        let acl = StreamAclBuilder::new()
            .add_read_roles("admin")
            .add_write_roles("admin")
            .add_delete_roles("admin")
            .add_meta_read_roles("admin")
            .add_meta_write_roles("admin")
            .build();

        let expected = StreamMetadataBuilder::new()
            .max_age(Duration::from_secs(2))
            .cache_control(Duration::from_secs(15))
            .truncate_before(1)
            .max_count(12)
            .acl(Acl::Stream(acl))
            .insert_custom_property("foo", "bar")
            .build();

        let actual: StreamMetadata =
            serde_json::from_slice(serde_json::to_vec(&expected)?.as_slice())?;

        assert_eq!(expected, actual);

        Ok(())
    }

    #[test]
    fn isomorphic_2() -> Result<(), Box<dyn std::error::Error>> {
        let expected = StreamMetadataBuilder::new()
            .max_age(Duration::from_secs(2))
            .cache_control(Duration::from_secs(15))
            .truncate_before(1)
            .max_count(12)
            .acl(Acl::UserStream)
            .insert_custom_property("foo", "bar")
            .build();

        let actual: StreamMetadata =
            serde_json::from_slice(serde_json::to_vec(&expected)?.as_slice())?;

        assert_eq!(expected, actual);

        Ok(())
    }

    #[test]
    fn isomorphic_3() -> Result<(), Box<dyn std::error::Error>> {
        let expected = StreamMetadataBuilder::new()
            .max_age(Duration::from_secs(2))
            .cache_control(Duration::from_secs(15))
            .truncate_before(1)
            .max_count(12)
            .acl(Acl::SystemStream)
            .insert_custom_property("foo", "bar")
            .build();

        let actual: StreamMetadata =
            serde_json::from_slice(serde_json::to_vec(&expected)?.as_slice())?;

        assert_eq!(expected, actual);

        Ok(())
    }

    #[test]
    fn metadata_spec() -> Result<(), Box<dyn std::error::Error>> {
        let content = r#"
        {
            "$maxCount": 12,
            "$maxAge": 2000,
            "$tb": 1,
            "$cacheControl": 15000,
            "$acl": {
                "$r": "admin",
                "$w": "admin",
                "$d": "admin",
                "$mr": "admin",
                "$mw": "admin"
            },
            "foo": "bar"
        }
        "#;

        let acl = StreamAclBuilder::new()
            .add_read_roles("admin")
            .add_write_roles("admin")
            .add_delete_roles("admin")
            .add_meta_read_roles("admin")
            .add_meta_write_roles("admin")
            .build();

        let expected = StreamMetadataBuilder::new()
            .max_age(Duration::from_secs(2))
            .cache_control(Duration::from_secs(15))
            .truncate_before(1)
            .max_count(12)
            .acl(Acl::Stream(acl))
            .insert_custom_property("foo", "bar")
            .build();

        let actual = serde_json::from_str(content)?;

        assert_eq!(expected, actual);

        Ok(())
    }
}

/// Events related to a subscription.
#[derive(Debug)]
pub enum SubscriptionEvent {
    /// Indicates the subscription has been confirmed by the server. The String value represents
    /// the subscription id.
    Confirmed(String),

    /// An event notification from the server.
    EventAppeared(ResolvedEvent),

    /// Indicates a checkpoint has been created. Related to subscription to $all when
    /// filters are used.
    Checkpoint(Position),

    /// If running a regular subscription, indicates the first's event position.
    FirstStreamPosition(u64),

    /// If running a regular subscription, indicates the last's event position.
    LastStreamPosition(u64),

    /// If running a subscription to $all, indicates the position of the $all last's event.
    LastAllPosition(Position),

    /// Indicates the subscription has reached the head of the stream.
    CaughtUp,

    /// Indicates the subscription has fell behind, meaning it's no longer keeping up with the
    /// stream's pace.
    FellBehind,
}

#[derive(Debug)]
pub enum PersistentSubscriptionEvent {
    EventAppeared {
        retry_count: usize,
        event: ResolvedEvent,
    },
    Confirmed(String),
}

/// Gathers every possible Nak actions.
#[derive(Debug, PartialEq, Eq)]
pub enum NakAction {
    /// Client unknown on action. Let server decide.
    Unknown,

    /// Park message do not resend. Put on poison queue.
    Park,

    /// Explicit retry the message.
    Retry,

    /// Skip this message do not resend do not put in poison queue.
    Skip,

    /// Stop the subscription.
    Stop,
}

/// System supported consumer strategies for use with persistent subscriptions.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
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

    PinnedByCorrelation,

    Custom(String),
}

impl SystemConsumerStrategy {
    pub(crate) fn from_string(str: String) -> Self {
        match str.as_str() {
            "DispatchToSingle" => SystemConsumerStrategy::DispatchToSingle,
            "RoundRobin" => SystemConsumerStrategy::RoundRobin,
            "Pinned" => SystemConsumerStrategy::Pinned,
            "PinnedByCorrelation" => SystemConsumerStrategy::PinnedByCorrelation,
            _ => SystemConsumerStrategy::Custom(str),
        }
    }
}

impl std::fmt::Display for SystemConsumerStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            SystemConsumerStrategy::DispatchToSingle => "DispatchToSingle",
            SystemConsumerStrategy::RoundRobin => "RoundRobin",
            SystemConsumerStrategy::Pinned => "Pinned",
            SystemConsumerStrategy::PinnedByCorrelation => "PinnedByCorrelation",
            SystemConsumerStrategy::Custom(value) => value.as_str(),
        };

        write!(f, "{}", value)
    }
}

/// Gathers every persistent subscription property.
#[derive(Debug, Clone)]
pub struct PersistentSubscriptionSettings<A> {
    /// Whether or not the persistent subscription should resolve link
    /// events to their linked events.
    pub resolve_link_tos: bool,

    /// Where the subscription should start from (event number).
    pub start_from: StreamPosition<A>,

    /// Whether or not in depth latency statistics should be tracked on this
    /// subscription.
    pub extra_statistics: bool,

    /// The amount of time after which a message should be considered to be
    /// timeout and retried.
    pub message_timeout: Duration,

    /// The maximum number of retries (due to timeout) before a message get
    /// considered to be parked.
    pub max_retry_count: i32,

    /// The size of the buffer listening to live messages as they happen.
    pub live_buffer_size: i32,

    /// The number of events read at a time when paging in history.
    pub read_batch_size: i32,

    /// The number of events to cache when paging through history.
    pub history_buffer_size: i32,

    /// The amount of time to try checkpoint after.
    pub checkpoint_after: Duration,

    /// The minimum number of messages to checkpoint.
    pub checkpoint_lower_bound: i32,

    /// The maximum number of messages to checkpoint. If this number is reached
    /// , a checkpoint will be forced.
    pub checkpoint_upper_bound: i32,

    /// The maximum number of subscribers allowed.
    pub max_subscriber_count: i32,

    /// The strategy to use for distributing events to client consumers.
    pub consumer_strategy_name: SystemConsumerStrategy,
}

impl<A> PersistentSubscriptionSettings<A> {
    pub(crate) fn named_consumer_strategy_as_i32(&self) -> Result<i32> {
        match self.consumer_strategy_name {
            SystemConsumerStrategy::DispatchToSingle => Ok(0),
            SystemConsumerStrategy::RoundRobin => Ok(1),
            SystemConsumerStrategy::Pinned => Ok(2),
            SystemConsumerStrategy::Custom(_) | SystemConsumerStrategy::PinnedByCorrelation => {
                // Currently unsupported by all released servers.
                Err(Error::UnsupportedFeature)
            }
        }
    }

    pub(crate) fn map<F, B>(self, f: F) -> PersistentSubscriptionSettings<B>
    where
        F: FnOnce(A) -> B,
    {
        PersistentSubscriptionSettings {
            resolve_link_tos: self.resolve_link_tos,
            start_from: self.start_from.map(f),
            extra_statistics: self.extra_statistics,
            message_timeout: self.message_timeout,
            max_retry_count: self.max_retry_count,
            live_buffer_size: self.live_buffer_size,
            read_batch_size: self.read_batch_size,
            history_buffer_size: self.history_buffer_size,
            checkpoint_after: self.checkpoint_after,
            checkpoint_lower_bound: self.checkpoint_lower_bound,
            checkpoint_upper_bound: self.checkpoint_upper_bound,
            max_subscriber_count: self.max_subscriber_count,
            consumer_strategy_name: self.consumer_strategy_name,
        }
    }
}

impl<A> Default for PersistentSubscriptionSettings<A> {
    fn default() -> PersistentSubscriptionSettings<A> {
        PersistentSubscriptionSettings {
            resolve_link_tos: false,
            start_from: StreamPosition::End,
            extra_statistics: false,
            message_timeout: Duration::from_secs(30),
            max_retry_count: 10,
            live_buffer_size: 500,
            read_batch_size: 20,
            history_buffer_size: 500,
            checkpoint_after: Duration::from_secs(2),
            checkpoint_lower_bound: 10,
            checkpoint_upper_bound: 1_000,
            max_subscriber_count: 0, // Means their is no limit.
            consumer_strategy_name: SystemConsumerStrategy::RoundRobin,
        }
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
#[derive(Default, Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum NodePreference {
    /// When attempting connection, prefers leader nodes.
    #[default]
    Leader,

    /// When attempting connection, prefers follower nodes.
    Follower,

    /// When attempting connection, has no node preference.
    Random,

    /// When attempting connection, prefers read-replica nodes.
    ReadOnlyReplica,
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

impl NodePreference {
    pub(crate) fn match_preference(&self, state: &VNodeState) -> bool {
        matches!(
            (self, state),
            (NodePreference::Leader, VNodeState::Leader)
                | (NodePreference::Follower, VNodeState::Follower)
                | (
                    NodePreference::ReadOnlyReplica,
                    VNodeState::ReadOnlyReplica
                        | VNodeState::ReadOnlyLeaderLess
                        | VNodeState::PreReadOnlyReplica,
                )
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DnsClusterSettings {
    pub(crate) endpoint: Endpoint,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
/// Actual revision of a stream.
pub enum CurrentRevision {
    /// The last event's number.
    Current(u64),

    /// The stream doesn't exist.
    NoStream,
}

impl std::fmt::Display for CurrentRevision {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
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

#[derive(Error, Debug, Clone)]
/// KurrentDB command error.
pub enum Error {
    #[error("Server-side error: {0}")]
    ServerError(String),
    #[error("You tried to execute a command that requires a leader node on a follower node. New leader: ")]
    NotLeaderException(Endpoint),
    #[error("Connection is closed.")]
    ConnectionClosed,
    #[error("Unmapped gRPC error: code: {code}, message: {message}.")]
    Grpc { code: tonic::Code, message: String },
    #[error("gRPC connection error: {0}")]
    GrpcConnectionError(GrpcConnectionError),
    #[error("Internal parsing error: {0}")]
    InternalParsingError(String),
    #[error("Access denied error")]
    AccessDenied,
    #[error("The resource you tried to create already exists")]
    ResourceAlreadyExists,
    #[error("The resource you asked for doesn't exist")]
    ResourceNotFound,
    #[error("The resource you asked for was deleted")]
    ResourceDeleted,
    #[error("The operation is unsupported by the server")]
    UnsupportedFeature,
    #[error("Unexpected internal client error. Please fill an issue on GitHub")]
    InternalClientError,
    #[error("Deadline exceeded")]
    DeadlineExceeded,
    #[error("Initialization error: {0}")]
    InitializationError(String),
    #[error("Illegal state error: {0}")]
    IllegalStateError(String),
    #[error("Wrong expected version: expected '{expected}' but got '{current}'")]
    WrongExpectedVersion {
        expected: ExpectedRevision,
        current: CurrentRevision,
    },
}

impl Error {
    pub fn from_grpc(status: Status) -> Self {
        let metadata = status.metadata();
        if let Some("not-leader") = metadata.get("exception").and_then(|e| e.to_str().ok()) {
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

        if let Some("stream-deleted") = status
            .metadata()
            .get("exception")
            .and_then(|e| e.to_str().ok())
        {
            return Error::ResourceDeleted;
        }

        if status.code() == Code::Cancelled && status.message() == "Timeout expired"
            || status.code() == Code::DeadlineExceeded
        {
            return Error::DeadlineExceeded;
        }

        if status.code() == Code::DeadlineExceeded {
            return Error::DeadlineExceeded;
        }

        if status.code() == Code::Unauthenticated || status.code() == Code::PermissionDenied {
            return Error::AccessDenied;
        }

        if status.code() == Code::AlreadyExists {
            return Error::ResourceAlreadyExists;
        }

        if status.code() == Code::NotFound {
            return Error::ResourceNotFound;
        }

        if status.code() == Code::Unavailable
            || status.code() == Code::Internal
            || status.code() == Code::DataLoss
            || status.code() == Code::Unknown
        {
            return Error::ServerError(status.to_string());
        }

        if status.code() == Code::Unimplemented {
            return Error::UnsupportedFeature;
        }

        Error::Grpc {
            code: status.code(),
            message: status.message().to_string(),
        }
    }

    pub fn is_access_denied(&self) -> bool {
        if let Error::AccessDenied = self {
            return true;
        }

        false
    }

    pub fn is_unsupported_feature(&self) -> bool {
        if let Error::UnsupportedFeature = self {
            return true;
        }

        false
    }
}

#[derive(Error, Debug, Clone)]
/// KurrentDB command error.
pub enum GrpcConnectionError {
    #[error("Max discovery attempt count reached. count: {0}")]
    MaxDiscoveryAttemptReached(usize),
    #[error("Unmapped gRPC connection error: {0}.")]
    Grpc(String),
}

pub type Result<A> = std::result::Result<A, Error>;

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

    pub fn exclude_system_events(self) -> Self {
        Self {
            regex: Some("^[^\\$].*".to_string()),
            ..self
        }
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PersistentSubscriptionInfoHttpJson {
    pub event_stream_id: String,
    pub group_name: String,
    pub status: String,
    pub average_items_per_second: f64,
    pub total_items_processed: usize,
    pub last_processed_event_number: i64,
    pub last_checkpointed_event_position: Option<String>,
    pub last_known_event_position: Option<String>,
    pub last_known_event_number: i64,
    #[serde(default)]
    pub connection_count: usize,
    pub total_in_flight_messages: usize,
    pub config: Option<PersistentSubscriptionConfig>,
    #[serde(default)]
    pub read_buffer_count: usize,
    #[serde(default)]
    pub live_buffer_count: usize,
    #[serde(default)]
    pub retry_buffer_count: usize,
    #[serde(default)]
    pub outstanding_messages_count: usize,
    #[serde(default)]
    pub parked_message_count: usize,
    #[serde(default)]
    pub count_since_last_measurement: usize,
    #[serde(default)]
    pub connections: Vec<PersistentSubscriptionConnectionInfo>,
}

#[derive(Debug, Clone)]
pub struct PersistentSubscriptionInfo<A> {
    pub event_source: String,
    pub group_name: String,
    pub status: String,
    pub connections: Vec<PersistentSubscriptionConnectionInfo>,
    pub settings: Option<PersistentSubscriptionSettings<A>>,
    pub stats: PersistentSubscriptionStats,
}

impl<A> PersistentSubscriptionInfo<A> {
    pub(crate) fn map<F, B>(self, f: F) -> PersistentSubscriptionInfo<B>
    where
        F: FnOnce(A) -> B,
    {
        PersistentSubscriptionInfo {
            event_source: self.event_source,
            group_name: self.group_name,
            status: self.status,
            connections: self.connections,
            settings: self.settings.map(|setts| setts.map(f)),
            stats: self.stats,
        }
    }
}

#[derive(Debug, Clone)]
pub enum RevisionOrPosition {
    Revision(u64),
    Position(Position),
}

impl serde::Serialize for RevisionOrPosition {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            RevisionOrPosition::Revision(rev) => serializer.serialize_u64(*rev),
            RevisionOrPosition::Position(pos) => {
                serializer.serialize_str(format!("C:{}/P:{}", pos.commit, pos.prepare).as_str())
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistentSubscriptionStats {
    pub average_per_second: f64,
    pub total_items: usize,
    pub count_since_last_measurement: usize,
    pub last_checkpointed_event_revision: Option<u64>,
    pub last_known_event_revision: Option<u64>,
    pub last_checkpointed_position: Option<Position>,
    pub last_known_position: Option<Position>,
    pub read_buffer_count: usize,
    pub live_buffer_count: usize,
    pub retry_buffer_count: usize,
    pub total_in_flight_messages: usize,
    pub outstanding_messages_count: usize,
    pub parked_message_count: usize,
}

impl Default for PersistentSubscriptionStats {
    fn default() -> Self {
        Self {
            average_per_second: 0.0,
            total_items: 0,
            count_since_last_measurement: 0,
            last_checkpointed_event_revision: None,
            last_known_event_revision: None,
            last_checkpointed_position: None,
            last_known_position: None,
            read_buffer_count: 0,
            live_buffer_count: 0,
            retry_buffer_count: 0,
            total_in_flight_messages: 0,
            outstanding_messages_count: 0,
            parked_message_count: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PersistentSubscriptionConfig {
    pub resolve_linktos: bool,
    pub start_from: StreamPosition<RevisionOrPosition>,
    #[serde(default)]
    pub start_position: String,
    pub message_timeout_milliseconds: i64,
    pub extra_statistics: bool,
    pub max_retry_count: i64,
    pub live_buffer_size: i64,
    pub buffer_size: i64,
    pub read_batch_size: i64,
    pub prefer_round_robin: bool,
    #[serde(rename = "checkPointAfterMilliseconds")]
    pub checkpoint_after_milliseconds: i64,
    #[serde(rename = "minCheckPointCount")]
    pub min_checkpoint_count: i64,
    #[serde(rename = "maxCheckPointCount")]
    pub max_checkpoint_count: i64,
    pub max_subscriber_count: i64,
    pub named_consumer_strategy: SystemConsumerStrategy,
}

impl Serialize for StreamPosition<RevisionOrPosition> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            StreamPosition::Start => serializer.serialize_i64(0),
            StreamPosition::Position(or) => match or {
                RevisionOrPosition::Revision(rev) => serializer.serialize_u64(*rev),
                RevisionOrPosition::Position(p) => serializer.serialize_str(p.to_string().as_str()),
            },
            StreamPosition::End => serializer.serialize_i64(-1),
        }
    }
}

struct PositionVisitor;

impl Visitor<'_> for PositionVisitor {
    type Value = Position;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(formatter, "Expected a transaction log position")
    }

    fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if let Ok(v) = event_store::generated::parse_position(v) {
            Ok(v)
        } else {
            Err(serde::de::Error::custom(format!(
                "can't parse a transaction log position out of this: '{}'",
                v,
            )))
        }
    }
}

impl<'de> Deserialize<'de> for Position {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(PositionVisitor)
    }
}

impl Serialize for Position {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

impl<'de> Deserialize<'de> for StreamPosition<RevisionOrPosition> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(StreamRevOrPosVisitor)
    }
}

struct StreamRevOrPosVisitor;

impl Visitor<'_> for StreamRevOrPosVisitor {
    type Value = StreamPosition<RevisionOrPosition>;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(
            formatter,
            "Expecting a revision number or a transaction log position"
        )
    }

    fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v == -1 {
            return Ok(StreamPosition::End);
        } else if v == 0 {
            return Ok(StreamPosition::Start);
        }

        // This usecase should not happen.
        Ok(StreamPosition::Position(RevisionOrPosition::Revision(
            v as u64,
        )))
    }

    fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v == 0 {
            return Ok(StreamPosition::Start);
        }

        Ok(StreamPosition::Position(RevisionOrPosition::Revision(v)))
    }

    fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if let Ok(v) = event_store::generated::parse_stream_position(v) {
            Ok(v)
        } else {
            Err(serde::de::Error::custom(format!(
                "can't parse a revision or a position out of this: '{}'",
                v,
            )))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistentSubscriptionConnectionInfo {
    pub from: String,
    pub username: String,
    pub average_items_per_second: f64,
    #[serde(rename = "totalItemsProcessed")]
    pub total_items: usize,
    pub count_since_last_measurement: usize,
    pub available_slots: usize,
    pub in_flight_messages: usize,
    pub connection_name: String,
    #[serde(
        default,
        deserialize_with = "deserialize_measurements",
        skip_serializing
    )]
    pub extra_statistics: PersistentSubscriptionMeasurements,
}

fn deserialize_measurements<'de, D>(
    deserializer: D,
) -> std::result::Result<PersistentSubscriptionMeasurements, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_seq(Measurements)
}

#[derive(Debug, Clone, Default)]
pub struct PersistentSubscriptionMeasurements(pub(crate) HashMap<String, i64>);

impl PersistentSubscriptionMeasurements {
    pub fn entries(self) -> impl Iterator<Item = (String, i64)> {
        self.0.into_iter()
    }

    pub fn get(&self, key: impl AsRef<str>) -> Option<i64> {
        self.0.get(key.as_ref()).copied()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct KeyValue {
    key: String,
    value: i64,
}

struct Measurements;

impl<'de> serde::de::Visitor<'de> for Measurements {
    type Value = PersistentSubscriptionMeasurements;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(formatter, "a list of key-values")
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut map = HashMap::new();
        while let Some(obj) = seq.next_element::<KeyValue>()? {
            map.insert(obj.key, obj.value);
        }

        Ok(PersistentSubscriptionMeasurements(map))
    }
}

pub trait StreamName {
    fn into_stream_name(self) -> Bytes;
}

impl StreamName for Bytes {
    fn into_stream_name(self) -> Bytes {
        self
    }
}

impl StreamName for &str {
    fn into_stream_name(self) -> Bytes {
        self.to_string().into()
    }
}

impl StreamName for String {
    fn into_stream_name(self) -> Bytes {
        self.into()
    }
}

pub trait MetadataStreamName {
    fn into_metadata_stream_name(self) -> Bytes;
}

impl MetadataStreamName for Bytes {
    fn into_metadata_stream_name(self) -> Bytes {
        self
    }
}

impl MetadataStreamName for &str {
    fn into_metadata_stream_name(self) -> Bytes {
        format!("$${}", self).to_string().into()
    }
}

impl MetadataStreamName for String {
    fn into_metadata_stream_name(self) -> Bytes {
        self.as_str().into_metadata_stream_name()
    }
}
