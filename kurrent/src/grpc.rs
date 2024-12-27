use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::str::FromStr;
use std::time::Duration;

use futures::Future;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use nom::lib::std::fmt::Formatter;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::{RngCore, SeedableRng};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use serde::de::{Error, Visitor};
use serde::{Deserialize, Serialize};
use serde::{Deserializer, Serializer};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tonic::{Code, Status};
use url::Url;
use uuid::Uuid;

use crate::operations::gossip::{self, MemberInfo, VNodeState};
use crate::server_features::{Features, ServerInfo};
use crate::types::{Endpoint, GrpcConnectionError};
use crate::{Credentials, DnsClusterSettings, NodePreference};

#[derive(Debug)]
struct NoVerification;

impl rustls::client::danger::ServerCertVerifier for NoVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA1,
            SignatureScheme::ECDSA_SHA1_Legacy,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}

#[test]
fn test_connection_string() {
    pretty_env_logger::init();

    #[derive(Debug, Serialize, Deserialize)]
    struct Mockup {
        string: String,
        #[serde(default)]
        expect_failure: bool,
        expected: ClientSettings,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct Mockups {
        mockups: Vec<Mockup>,
    }

    let mockups = include_str!("../tests/fixtures/connection_string/mockups.toml");
    let fixtures: Mockups = toml::from_str(mockups).unwrap();

    for mockup in fixtures.mockups {
        match mockup.string.as_str().parse::<ClientSettings>() {
            Ok(current) => assert_eq!(
                current, mockup.expected,
                "Failed parsing [{}]",
                mockup.string
            ),

            Err(e) => {
                if !mockup.expect_failure {
                    panic!("Failed parsing [{}]: {:?}", mockup.string, e);
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClientSettingsParseError {
    message: String,
    error: Option<url::ParseError>,
}

impl ClientSettingsParseError {
    pub fn message(&self) -> &str {
        self.message.as_str()
    }

    pub fn error(&self) -> Option<&url::ParseError> {
        self.error.as_ref()
    }
}

impl Display for ClientSettingsParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ClientSettings parsing error with '{}': {:?}",
            self.message, self.error
        )
    }
}

impl std::error::Error for ClientSettingsParseError {}

struct DurationVisitor;

impl Visitor<'_> for DurationVisitor {
    type Value = Duration;

    fn expecting(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "duration in milliseconds")
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v == -1 {
            return Ok(Duration::from_millis(u64::MAX));
        }

        Ok(Duration::from_millis(v as u64))
    }
}

struct OptionalDurationVisitor;

impl<'de> Visitor<'de> for OptionalDurationVisitor {
    type Value = Option<Duration>;

    fn expecting(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "duration in milliseconds")
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(None)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Some(deserializer.deserialize_i64(DurationVisitor)?))
    }
}

fn serialize_duration<S>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_u64(value.as_millis() as u64)
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(DurationVisitor)
}

fn serialize_optional_duration<S>(
    value: &Option<Duration>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if let Some(value) = value.as_ref() {
        serialize_duration(value, serializer)
    } else {
        serializer.serialize_none()
    }
}

fn deserialize_optional_duration<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_option(OptionalDurationVisitor)
}

fn default_max_discover_attempts() -> usize {
    ClientSettings::default().max_discover_attempts
}

fn default_discovery_interval() -> Duration {
    ClientSettings::default().discovery_interval
}

fn default_gossip_timeout() -> Duration {
    ClientSettings::default().gossip_timeout
}

fn default_preference() -> NodePreference {
    ClientSettings::default().preference
}

fn default_secure() -> bool {
    ClientSettings::default().secure
}

fn default_tls_verify_cert() -> bool {
    ClientSettings::default().tls_verify_cert
}

fn default_keep_alive_interval() -> Duration {
    ClientSettings::default().keep_alive_interval
}

fn default_keep_alive_timeout() -> Duration {
    ClientSettings::default().keep_alive_timeout
}

/// Gathers all the settings related to a gRPC client with an KurrentDB database.
/// `ClientSettings` can only be created when parsing a connection string.
///
/// ```
/// # use eventstore::ClientSettings;
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let setts = "esdb://localhost:1234?tls=false".parse::<ClientSettings>()?;
/// # Ok(())
/// # }
/// ```
///
/// You can declare a single-node or a cluster-mode client while only using a connection string.
/// For example, you can define a cluster-mode client based on a fixed set of gossip seeds:
///
/// ```
/// # use eventstore::ClientSettings;
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let setts = "esdb://localhost:1111,localhost:2222,localhost:3333".parse::<ClientSettings>()?;
/// # Ok(())
/// # }
/// ```
///
/// Same example except we are using DNS discovery this time. The client will perform SRV queries
/// to resolve all the node associated to that domain:
/// ```
/// # use eventstore::ClientSettings;
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let setts = "esdb+discover://mydomain:1234".parse::<ClientSettings>()?;
/// # Ok(())
/// # }
/// ```
///
/// `ClientSettings` supports a wide range of settings. If a setting is not mentioned in the
/// connection string, that setting default value is used.
///
/// * `maxDiscoverAttempts`: default `3`. Maximum number of DNS discovery attempts before the
///    connection gives up.
///
/// * `discoveryInterval`: default `500ms`. Waiting period between discovery attempts.
///
/// * `gossipTimeout`: default `3s`: Waiting period before a gossip request timeout.
///    __*TODO - Current behavior doesn't timeout at all.*__
///
/// * `tls`: default `true`. Use a secure connection.
///
/// * `tlsVerifyCert`: default `true`. When using a secure connection, perform a certification
///    verification.
///
/// * `nodePreference`: default `random`. When in a cluster connection, indicates what type of node
///    a connection should pick. Keep in mind that's best effort. Supported values are:
///    * `leader`
///    * `random`
///    * `follower`
///    * `readOnlyReplica`
///
/// * `keepAliveInterval`: default `10s`
/// * `keepAliveTimeout`: default `10s`
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientSettings {
    #[serde(default)]
    pub(crate) dns_discover: bool,
    #[serde(default)]
    pub(crate) hosts: Vec<Endpoint>,
    #[serde(default = "default_max_discover_attempts")]
    pub(crate) max_discover_attempts: usize,
    #[serde(
        default = "default_discovery_interval",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub(crate) discovery_interval: Duration,
    #[serde(
        default = "default_gossip_timeout",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub(crate) gossip_timeout: Duration,
    #[serde(default = "default_preference")]
    pub(crate) preference: NodePreference,
    #[serde(default = "default_secure")]
    pub(crate) secure: bool,
    #[serde(default = "default_tls_verify_cert")]
    pub(crate) tls_verify_cert: bool,
    #[serde(default)]
    pub(crate) default_user_name: Option<Credentials>,
    #[serde(
        default = "default_keep_alive_interval",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub(crate) keep_alive_interval: Duration,
    #[serde(
        default = "default_keep_alive_timeout",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub(crate) keep_alive_timeout: Duration,
    #[serde(
        default,
        serialize_with = "serialize_optional_duration",
        deserialize_with = "deserialize_optional_duration"
    )]
    pub(crate) default_deadline: Option<Duration>,
    pub(crate) connection_name: Option<String>,
}

impl ClientSettings {
    pub fn is_dns_discovery_enabled(&self) -> bool {
        self.dns_discover
    }

    pub fn hosts(&self) -> &Vec<Endpoint> {
        &self.hosts
    }

    pub fn max_discover_attempts(&self) -> usize {
        self.max_discover_attempts
    }

    pub fn discovery_interval(&self) -> Duration {
        self.discovery_interval
    }

    pub fn gossip_timeout(&self) -> Duration {
        self.gossip_timeout
    }

    pub fn node_preference(&self) -> NodePreference {
        self.preference
    }

    pub fn is_secure_mode_enabled(&self) -> bool {
        self.secure
    }

    pub fn is_tls_certificate_verification_enabled(&self) -> bool {
        self.tls_verify_cert
    }

    pub fn default_authenticated_user(&self) -> &Option<Credentials> {
        &self.default_user_name
    }

    pub fn to_uri(&self, endpoint: &Endpoint) -> http::Uri {
        let scheme = if self.secure { "https" } else { "http" };

        format!("{}://{}:{}", scheme, endpoint.host, endpoint.port)
            .parse()
            .unwrap()
    }

    pub(crate) fn to_hyper_uri(&self, endpoint: &Endpoint) -> hyper::Uri {
        let scheme = if self.secure { "https" } else { "http" };

        hyper::Uri::from_maybe_shared(format!("{}://{}:{}", scheme, endpoint.host, endpoint.port))
            .unwrap()
    }
}

fn parse_param<A>(
    param_name: impl AsRef<str>,
    value: impl AsRef<str>,
) -> Result<A, ClientSettingsParseError>
where
    A: FromStr,
    <A as FromStr>::Err: Display,
{
    match value.as_ref().parse::<A>() {
        Err(e) => Err(ClientSettingsParseError {
            message: format!(
                "Invalid format for param '{}'. value = '{}': {}",
                param_name.as_ref(),
                value.as_ref(),
                e
            ),
            error: None,
        }),

        Ok(a) => Ok(a),
    }
}

fn parse_from_url(
    mut result: ClientSettings,
    url: Url,
) -> Result<ClientSettings, ClientSettingsParseError> {
    if url.scheme() != "esdb" && url.scheme() != "esdb+discover" {
        return Err(ClientSettingsParseError {
            message: format!("Unknown URL scheme: {}", url.scheme()),
            error: None,
        });
    }

    result.dns_discover = url.scheme() == "esdb+discover";

    if !url.username().is_empty() {
        result.default_user_name = Some(Credentials::new(
            url.username().to_string(),
            url.password().unwrap_or_default().to_string(),
        ));
    }

    if result.hosts.is_empty() && !url.path().is_empty() && url.path() != "/" {
        return Err(ClientSettingsParseError {
            message: format!("Unsupported URL path: {}", url.path()),
            error: None,
        });
    }

    if result.hosts.is_empty() && !url.has_host() {
        return Err(ClientSettingsParseError {
            message: "Connection string doesn't have an host".to_string(),
            error: None,
        });
    }

    // If not empty, it means we are dealing with pre-populated connection settings, from a
    // desugared connection string for example.
    if result.hosts.is_empty() {
        let host = url.host_str().unwrap_or_default();

        if !host.contains(',') {
            result.hosts.push(Endpoint {
                host: host.to_string(),
                port: url.port().unwrap_or(2_113) as u32,
            });
        } else {
            for host_part in host.split(',').collect::<Vec<_>>() {
                parse_gossip_seed(&mut result, host_part)?;
            }
        }
    }

    for (param, value) in url.query_pairs() {
        let name = param.to_lowercase();

        match param.to_lowercase().as_str() {
            "maxdiscoverattempts" => {
                result.max_discover_attempts = parse_param(name, value)?;
            }

            "discoveryinterval" => {
                result.discovery_interval = Duration::from_millis(parse_param(name, value)?);
            }

            "gossiptimeout" => {
                result.gossip_timeout = Duration::from_millis(parse_param(name, value)?);
            }

            "tls" => {
                result.secure = parse_param(name, value)?;
            }

            "tlsverifycert" => {
                result.tls_verify_cert = parse_param(name, value)?;
            }

            "nodepreference" => match value.to_lowercase().as_str() {
                "follower" => {
                    result.preference = NodePreference::Follower;
                }

                "random" => {
                    result.preference = NodePreference::Random;
                }

                "leader" => {
                    result.preference = NodePreference::Leader;
                }

                "readonlyreplica" => {
                    result.preference = NodePreference::ReadOnlyReplica;
                }

                unknown => {
                    return Err(ClientSettingsParseError {
                        message: format!("Unknown node preference value '{}'", unknown),
                        error: None,
                    });
                }
            },

            "keepaliveinterval" => {
                let value = parse_param::<i64>(name, value)?;

                if value >= 0 && value < self::defaults::KEEP_ALIVE_INTERVAL_IN_MS as i64 {
                    warn!(
                        "Specified keepAliveInterval of {} is less than recommended {}",
                        value,
                        self::defaults::KEEP_ALIVE_INTERVAL_IN_MS
                    );
                    continue;
                }

                if value == -1 {
                    result.keep_alive_interval = Duration::from_millis(u64::MAX);
                    continue;
                }

                if value < -1 {
                    return Err(ClientSettingsParseError {
                        message: format!("Invalid keepAliveInterval of {}. Please provide a positive integer, or -1 to disable", value),
                        error: None,
                    });
                }

                result.keep_alive_interval = Duration::from_millis(value as u64);
            }

            "keepalivetimeout" => {
                let value = parse_param::<i64>(name, value)?;

                if value >= 0 && value < self::defaults::KEEP_ALIVE_TIMEOUT_IN_MS as i64 {
                    warn!(
                        "Specified keepAliveTimeout of {} is less than recommended {}",
                        value,
                        self::defaults::KEEP_ALIVE_TIMEOUT_IN_MS
                    );
                    continue;
                }

                if value == -1 {
                    result.keep_alive_timeout = Duration::from_millis(u64::MAX);
                    continue;
                }

                if value < -1 {
                    return Err(ClientSettingsParseError {
                        message: format!("Invalid keepAliveTimeout of {}. Please provide a positive integer, or -1 to disable", value),
                        error: None,
                    });
                }

                result.keep_alive_timeout = Duration::from_millis(value as u64);
            }

            "defaultdeadline" => {
                let value = parse_param::<i64>(name, value)?;

                if value == -1 {
                    result.default_deadline = Some(Duration::from_millis(u64::MAX));
                    continue;
                }

                if value < -1 {
                    return Err(ClientSettingsParseError {
                        message: format!("Invalid defaultDeadline of {}. Please provide a positive integer, or -1 to disable", value),
                        error: None,
                    });
                }

                result.default_deadline = Some(Duration::from_millis(value as u64));
            }

            "connectionname" => {
                result.connection_name = Some(value.to_string());
            }

            ignored => {
                warn!("Ignored connection string parameter: {}", ignored);
                continue;
            }
        }
    }

    Ok(result)
}

fn parse_gossip_seed(
    result: &mut ClientSettings,
    host: &str,
) -> Result<(), ClientSettingsParseError> {
    let host_parts: Vec<&str> = host.split(':').collect();

    match host_parts.len() {
        1 => result.hosts.push(Endpoint {
            host: host.to_string(),
            port: 2_113,
        }),

        2 => {
            if let Ok(port) = host_parts.as_slice()[1].parse::<u16>() {
                result.hosts.push(Endpoint {
                    host: host_parts.as_slice()[0].to_string(),
                    port: port as u32,
                });
            } else {
                return Err(ClientSettingsParseError {
                    message: format!("Invalid port number: {}", host_parts.as_slice()[1]),
                    error: None,
                });
            }
        }

        _ => {
            return Err(ClientSettingsParseError {
                message: format!("Invalid host part: '{}'", host),
                error: None,
            })
        }
    }

    Ok(())
}

impl FromStr for ClientSettings {
    type Err = ClientSettingsParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.parse::<Url>() {
            Err(e) => {
                // The way we support gossip seeds in cluster configuration is not supported by
                // the URL standard. When it happens, we rewrite the connection string to parse
                // those seeds properly. It happens when facing connection string like the following:
                //
                // esdb://host1:1234,host2:4321,host3:3231
                if e == url::ParseError::InvalidPort && s.contains(',') {
                    // We replace ',' by '/' and handle remaining gossip seeds as path segments.
                    match s.replace(',', "/").parse::<Url>() {
                        // In this case it should mean the connection is truly invalid so we return
                        // the previous error.
                        Err(_) => {
                            return Err(ClientSettingsParseError {
                                message: s.to_string(),
                                error: Some(e),
                            })
                        }

                        Ok(url) => {
                            let mut setts = ClientSettings::default();

                            if url.host_str().is_none() {
                                return Err(ClientSettingsParseError {
                                    message: s.to_string(),
                                    error: Some(e),
                                });
                            }

                            setts.hosts.push(Endpoint {
                                host: url.host_str().unwrap().to_string(),
                                port: url.port().unwrap_or(2_113) as u32,
                            });

                            if let Some(segments) = url.path_segments() {
                                for segment in segments {
                                    parse_gossip_seed(&mut setts, segment)?;
                                }
                            }

                            return parse_from_url(setts, url);
                        }
                    }
                }

                Err(ClientSettingsParseError {
                    message: s.to_string(),
                    error: Some(e),
                })
            }

            Ok(url) => parse_from_url(ClientSettings::default(), url),
        }
    }
}

impl Default for ClientSettings {
    fn default() -> Self {
        ClientSettings {
            dns_discover: false,
            hosts: Vec::new(),
            max_discover_attempts: 3,
            discovery_interval: Duration::from_millis(500),
            gossip_timeout: Duration::from_secs(3),
            preference: Default::default(),
            secure: true,
            tls_verify_cert: true,
            default_user_name: None,
            keep_alive_interval: Duration::from_millis(self::defaults::KEEP_ALIVE_INTERVAL_IN_MS),
            keep_alive_timeout: Duration::from_millis(self::defaults::KEEP_ALIVE_TIMEOUT_IN_MS),
            default_deadline: None,
            connection_name: None,
        }
    }
}

pub(crate) mod defaults {
    pub const KEEP_ALIVE_INTERVAL_IN_MS: u64 = 10_000;
    pub const KEEP_ALIVE_TIMEOUT_IN_MS: u64 = 10_000;
}

pub(crate) type HyperClient =
    hyper_util::client::legacy::Client<HttpsConnector<HttpConnector>, tonic::body::BoxBody>;

struct NodeConnection {
    id: Uuid,
    client: HyperClient,
    handle: Option<HandleInfo>,
    settings: ClientSettings,
    cluster_mode: Option<ClusterMode>,
    rng: SmallRng,
    previous_candidates: Option<Vec<Member>>,
}

#[derive(Clone)]
enum ClusterMode {
    Dns(DnsClusterSettings),
    Seeds(Vec<Endpoint>),
}

struct NodeRequest {
    correlation: Uuid,
    endpoint: Endpoint,
}

#[derive(Clone)]
pub(crate) struct HandleInfo {
    id: Uuid,
    pub(crate) client: HyperClient,
    pub(crate) uri: hyper::Uri,
    pub(crate) endpoint: Endpoint,
    pub(crate) secure: bool,
    pub(crate) server_info: ServerInfo,
}

impl NodeConnection {
    fn new(settings: ClientSettings) -> Self {
        let mut roots = rustls::RootCertStore::empty();

        for cert in rustls_native_certs::load_native_certs().expect("could not load platform certs")
        {
            roots.add(cert).unwrap();
        }

        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("failed to install rustls crypto provider");

        let mut tls = tokio_rustls::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();

        if !settings.tls_verify_cert && settings.secure {
            tls.dangerous()
                .set_certificate_verifier(std::sync::Arc::new(NoVerification));
        }

        let mut http = HttpConnector::new();
        http.enforce_http(false);

        let connector = tower::ServiceBuilder::new()
            .layer_fn(move |s| {
                hyper_rustls::HttpsConnectorBuilder::new()
                    .with_tls_config(tls.clone())
                    .https_or_http()
                    .enable_http2()
                    .wrap_connector(s)
            })
            .service(http);

        let client =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .timer(hyper_util::rt::tokio::TokioTimer::new())
                .http2_only(true)
                .http2_keep_alive_interval(settings.keep_alive_interval)
                .http2_keep_alive_timeout(settings.keep_alive_timeout)
                .build::<_, tonic::body::BoxBody>(connector);

        let cluster_mode = if settings.dns_discover || settings.hosts().len() > 1 {
            let mode = if settings.dns_discover {
                let endpoint = settings.hosts()[0].clone();
                ClusterMode::Dns(DnsClusterSettings { endpoint })
            } else {
                ClusterMode::Seeds(settings.hosts().clone())
            };

            Some(mode)
        } else {
            None
        };

        Self {
            id: Uuid::nil(),
            client,
            handle: None,
            settings,
            cluster_mode,
            rng: SmallRng::from_entropy(),
            previous_candidates: None,
        }
    }

    async fn next(
        &mut self,
        mut request: Option<NodeRequest>,
    ) -> Result<HandleInfo, GrpcConnectionError> {
        let mut selected_node = None;
        let mut failed_endpoint = None;

        loop {
            if let Some(request) = request.take() {
                if self.id != request.correlation {
                    if let Some(handle) = self.handle.clone() {
                        return Ok(handle);
                    }
                }

                failed_endpoint = self.handle.take().map(|h| h.endpoint);
                selected_node = Some(request.endpoint);

                continue;
            } else if let Some(handle) = self.handle.clone() {
                return Ok(handle);
            }

            let mut attempts = 1usize;
            loop {
                if let Some(selected_node) = selected_node.take() {
                    let uri = self.settings.to_hyper_uri(&selected_node);
                    debug!("Before calling server features endpoint...");
                    let server_info = match tokio::time::timeout(
                        self.settings.gossip_timeout(),
                        crate::server_features::supported_methods(&self.client, uri.clone()),
                    )
                    .await
                    {
                        Ok(outcome) => match outcome {
                            Ok(fs) => {
                                debug!("Successfully received server features");
                                fs
                            }

                            Err(status) => {
                                debug!("Error when calling server features endpoint: {}", status);
                                if status.code() == Code::NotFound
                                    || status.code() == Code::Unimplemented
                                {
                                    ServerInfo::default()
                                } else {
                                    error!(
                                        "Unexpected error when fetching server features: {}",
                                        status
                                    );
                                    return Err(GrpcConnectionError::Grpc(status.to_string()));
                                }
                            }
                        },

                        Err(_) => {
                            error!(
                                "Timeout when fetching server features on {:?}",
                                selected_node
                            );

                            continue;
                        }
                    };
                    self.id = Uuid::new_v4();
                    let handle = HandleInfo {
                        id: self.id,
                        endpoint: selected_node,
                        secure: self.settings.secure,
                        client: self.client.clone(),
                        uri,
                        server_info,
                    };

                    debug!("Successfully connected to node {:?}", handle.endpoint);
                    self.handle = Some(handle.clone());

                    return Ok(handle);
                } else if let Some(mode) = self.cluster_mode.as_ref() {
                    debug!("Before cluster node selection");
                    let node = node_selection(
                        &self.settings,
                        mode,
                        &self.client,
                        &failed_endpoint,
                        &mut self.rng,
                        &mut self.previous_candidates,
                    )
                    .await;

                    debug!("Cluster node selection completed: {:?}", node);
                    if node.is_some() {
                        selected_node = node;
                    }
                } else {
                    selected_node = self.settings.hosts().first().cloned();
                }

                attempts += 1;

                if attempts <= self.settings.max_discover_attempts() {
                    tokio::time::sleep(self.settings.discovery_interval()).await;
                    debug!("Starting new connection attempt");
                    continue;
                }

                debug!("Reached maximum discovery attempt count");
                return Err(GrpcConnectionError::MaxDiscoveryAttemptReached(
                    self.settings.max_discover_attempts(),
                ));
            }
        }
    }
}

fn connection_state_machine(
    handle: tokio::runtime::Handle,
    settings: ClientSettings,
) -> UnboundedSender<Msg> {
    let (sender, mut consumer) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let dup_sender = sender.clone();

    handle.spawn(async move {
        let mut connection = NodeConnection::new(settings);
        let mut handle_opt: Option<Handle> = None;

        while let Some(msg) = consumer.recv().await {
            match msg {
                Msg::GetChannel(resp) => {
                    if let Some(handle) = handle_opt.as_ref() {
                        debug!("Re-using active connection");
                        let _ = resp.send(Ok(handle.clone()));
                        continue;
                    }

                    debug!("Asking for a channel but we don't have an active connection. Connecting...");
                    match connection.next(None).await {
                        Err(e) => {
                            error!("gRPC connection error: {}", e);
                            let _ = resp.send(Err(e));
                            break;
                        }
                        Ok(info) => {
                            debug!(
                                "Successfully connected to {}:{}",
                                info.endpoint.host, info.endpoint.port
                            );

                            let handle = Handle {
                                id: info.id,
                                client: info.client,
                                uri: info.uri,
                                endpoint: info.endpoint,
                                secure: info.secure,
                                sender: sender.clone(),
                                server_info: info.server_info,
                            };

                            handle_opt = Some(handle.clone());

                            let _ = resp.send(Ok(handle));
                        }
                    }
                }
                Msg::CreateChannel(id, seed_opt) => {
                    let request = seed_opt.map(|endpoint| NodeRequest {
                        correlation: id,
                        endpoint,
                    });

                    debug!("Creating a new connection...");
                    match connection.next(request).await {
                        Err(e) => {
                            error!("gRPC connection error: {}", e);
                            break;
                        }
                        Ok(info) => {
                            debug!(
                                "Successfully connected to {}:{}",
                                info.endpoint.host, info.endpoint.port
                            );

                            let handle = Handle {
                                id: info.id,
                                client: info.client,
                                uri: info.uri,
                                endpoint: info.endpoint,
                                secure: info.secure,
                                sender: sender.clone(),
                                server_info: info.server_info,
                            };

                            handle_opt = Some(handle);
                        }
                    }
                }
            }
        }
    });

    dup_sender
}

#[derive(Clone, Debug)]
pub(crate) struct Handle {
    id: Uuid,
    pub(crate) client: HyperClient,
    pub(crate) uri: hyper::Uri,
    pub(crate) endpoint: Endpoint,
    pub(crate) secure: bool,
    pub(crate) server_info: ServerInfo,
    sender: tokio::sync::mpsc::UnboundedSender<Msg>,
}

impl Handle {
    pub(crate) fn report_error(self, e: &crate::Error) {
        error!("Error occurred during operation execution: {:?}", e);
        let _ = self.sender.send(Msg::CreateChannel(self.id, None));
    }

    pub(crate) fn id(&self) -> Uuid {
        self.id
    }

    pub(crate) fn sender(&self) -> &tokio::sync::mpsc::UnboundedSender<Msg> {
        &self.sender
    }

    pub(crate) fn url(&self) -> String {
        let protocol = if self.secure { "https" } else { "http" };

        format!(
            "{}://{}:{}",
            protocol, self.endpoint.host, self.endpoint.port
        )
    }

    pub(crate) fn supports_feature(&self, feats: Features) -> bool {
        self.server_info.contains_features(feats)
    }

    pub(crate) fn server_info(&self) -> ServerInfo {
        self.server_info
    }
}

pub(crate) enum Msg {
    GetChannel(oneshot::Sender<Result<Handle, GrpcConnectionError>>),
    CreateChannel(Uuid, Option<Endpoint>),
}

impl std::fmt::Debug for Msg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Msg::GetChannel(_) => write!(f, "Msg::GetChannel"),
            Msg::CreateChannel(id, seed_opt) => {
                write!(f, "Msg::CreateChannel({:?}, {:?})", id, seed_opt)
            }
        }
    }
}

#[derive(Clone)]
pub struct GrpcClient {
    pub(crate) sender: tokio::sync::mpsc::UnboundedSender<Msg>,
    connection_settings: ClientSettings,
}

impl GrpcClient {
    pub fn create(handle: tokio::runtime::Handle, connection_settings: ClientSettings) -> Self {
        let sender = connection_state_machine(handle, connection_settings.clone());

        GrpcClient {
            sender,
            connection_settings,
        }
    }

    pub(crate) async fn execute<F, Fut, A>(&self, action: F) -> crate::Result<A>
    where
        F: FnOnce(Handle) -> Fut + Send,
        Fut: Future<Output = Result<A, Status>> + Send,
        A: Send,
    {
        debug!("Sending channel handle request...");
        let handle = self.current_selected_node().await?;
        debug!("Handle received!");

        let id = handle.id;
        action(handle).await.map_err(|status| {
            let e = crate::Error::from_grpc(status);
            handle_error(&self.sender, id, &e);
            e
        })
    }

    pub(crate) async fn current_selected_node(&self) -> crate::Result<Handle> {
        let (sender, consumer) = tokio::sync::oneshot::channel();

        if self.sender.send(Msg::GetChannel(sender)).is_err() {
            return Err(crate::Error::ConnectionClosed);
        }

        match consumer.await {
            Ok(handle) => handle.map_err(crate::Error::GrpcConnectionError),
            Err(_) => Err(crate::Error::ConnectionClosed),
        }
    }

    pub fn connection_settings(&self) -> &ClientSettings {
        &self.connection_settings
    }
}

pub(crate) fn handle_error(sender: &UnboundedSender<Msg>, connection_id: Uuid, err: &crate::Error) {
    if let crate::Error::ServerError(ref status) = err {
        error!(
            "Current selected KurrentDB node gone unavailable. Starting node selection process: {}",
            status
        );

        let _ = sender.send(Msg::CreateChannel(connection_id, None));
    } else if let crate::Error::NotLeaderException(ref leader) = err {
        let _ = sender.send(Msg::CreateChannel(connection_id, Some(leader.clone())));

        warn!(
            "NotLeaderException found. Start reconnection process on: {:?}",
            leader
        );
    } else if let crate::Error::Grpc {
        ref code,
        ref message,
    } = err
    {
        debug!(
            "Operation unexpected error: code: {}, message: {}",
            code, message
        );
    }
}

struct Member {
    endpoint: Endpoint,
    state: VNodeState,
}

async fn node_selection(
    conn_setts: &ClientSettings,
    mode: &ClusterMode,
    client: &HyperClient,
    failed_endpoint: &Option<Endpoint>,
    rng: &mut SmallRng,
    previous_candidates: &mut Option<Vec<Member>>,
) -> Option<Endpoint> {
    let candidates = match previous_candidates.take() {
        Some(old_candidates) => {
            let mut new_candidates = candidates_from_old_gossip(failed_endpoint, old_candidates);

            // Use case: when the cluster is only comprised of a single node and that node
            // previously failed. This can only happen if the user used a fixed set of seeds.
            if new_candidates.is_empty() {
                new_candidates.clone_from(&conn_setts.hosts);
            }

            new_candidates
        }

        None => {
            let mut seeds = match mode {
                ClusterMode::Seeds(ref seeds) => seeds.clone(),
                ClusterMode::Dns(ref dns) => vec![dns.endpoint.clone()],
            };

            seeds.shuffle(rng);
            seeds
        }
    };

    debug!("List of candidates: {:?}", candidates);

    for candidate in candidates {
        let uri = conn_setts.to_hyper_uri(&candidate);
        debug!("Calling gossip endpoint on: {:?}", candidate);
        if let Ok(result) = tokio::time::timeout(
            conn_setts.gossip_timeout,
            gossip::read(conn_setts, client, uri),
        )
        .await
        {
            match result {
                Ok(members_info) => {
                    debug!("Candidate {:?} gossip info: {:?}", candidate, members_info);
                    let selected_node =
                        determine_best_node(rng, conn_setts.preference, members_info.as_slice());

                    if let Some(selected_node) = selected_node {
                        return Some(selected_node);
                    }
                }
                Err(err) => {
                    debug!(
                        "Failed to retrieve gossip information from candidate {:?}: {}",
                        &candidate, err
                    );
                }
            }
        } else {
            warn!("Gossip request timeout for candidate: {:?}", candidate);
        }
    }

    None
}

struct Candidates {
    nodes: Vec<Member>,
    managers: Vec<Member>,
}

impl Candidates {
    fn new() -> Candidates {
        Candidates {
            nodes: vec![],
            managers: vec![],
        }
    }

    fn push(&mut self, member: Member) {
        if let VNodeState::Manager = member.state {
            self.managers.push(member);
        } else {
            self.nodes.push(member);
        }
    }

    fn shuffle(&mut self) {
        let mut rng = rand::thread_rng();

        self.nodes.shuffle(&mut rng);
        self.managers.shuffle(&mut rng);
    }

    fn endpoints(mut self) -> Vec<Endpoint> {
        self.nodes.extend(self.managers);

        self.nodes.into_iter().map(|m| m.endpoint).collect()
    }
}

fn candidates_from_old_gossip(
    failed_endpoint: &Option<Endpoint>,
    old_candidates: Vec<Member>,
) -> Vec<Endpoint> {
    let candidates = match failed_endpoint {
        Some(endpoint) => old_candidates
            .into_iter()
            .filter(|member| member.endpoint != *endpoint)
            .collect(),

        None => old_candidates,
    };

    arrange_gossip_candidates(candidates)
}

fn arrange_gossip_candidates(candidates: Vec<Member>) -> Vec<Endpoint> {
    let mut arranged_candidates = Candidates::new();

    for member in candidates {
        arranged_candidates.push(member);
    }

    arranged_candidates.shuffle();
    arranged_candidates.endpoints()
}

fn determine_best_node(
    rng: &mut SmallRng,
    preference: NodePreference,
    members: &[MemberInfo],
) -> Option<Endpoint> {
    fn allowed_states(state: VNodeState) -> bool {
        !matches!(
            state,
            VNodeState::Manager | VNodeState::ShuttingDown | VNodeState::Shutdown
        )
    }

    let members = members
        .iter()
        .filter(|member| member.is_alive)
        .filter(|member| allowed_states(member.state));

    let member_opt = members.min_by(|a, b| {
        if let NodePreference::Random = preference {
            if rng.next_u32() % 2 == 0 {
                return Ordering::Greater;
            }

            return Ordering::Less;
        }

        if preference.match_preference(&a.state) && preference.match_preference(&b.state) {
            if rng.next_u32() % 2 == 0 {
                return Ordering::Less;
            } else {
                return Ordering::Greater;
            }
        }

        if preference.match_preference(&a.state) && !preference.match_preference(&b.state) {
            return Ordering::Less;
        }

        if !preference.match_preference(&a.state) && preference.match_preference(&b.state) {
            return Ordering::Greater;
        }

        Ordering::Greater
    });

    member_opt.map(|member| {
        info!(
            "Discovering: found best choice {}:{} ({:?})",
            member.http_end_point.host, member.http_end_point.port, member.state
        );

        member.http_end_point.clone()
    })
}

#[cfg(test)]
mod node_selection_tests {
    use rand::{rngs::SmallRng, RngCore, SeedableRng};

    use crate::{
        operations::gossip::{MemberInfo, VNodeState},
        Endpoint, NodePreference,
    };

    // Make sure matching preference nodes are still sorted randomly.

    #[test]
    fn test_determine_best_node_leader() {
        generate_test_case(NodePreference::Leader);
    }

    #[test]
    fn test_determine_best_node_follower() {
        generate_test_case(NodePreference::Follower);
    }

    #[test]
    fn test_determine_best_node_replica() {
        generate_test_case(NodePreference::ReadOnlyReplica);
    }

    #[test]
    fn test_determine_best_node_random() {
        generate_test_case(NodePreference::Random);
    }

    fn generate_test_case(pref: NodePreference) {
        let mut members = Vec::new();
        let mut rng = SmallRng::from_entropy();

        members.push(MemberInfo {
            instance_id: uuid::Uuid::new_v4(),
            time_stamp: rng.next_u32() as i64,
            state: VNodeState::Leader,
            is_alive: true,
            http_end_point: Endpoint {
                host: "localhost".to_string(),
                port: rng.next_u32(),
            },

            last_commit_position: 0,
            writer_checkpoint: 0,
            chaser_checkpoint: 0,
            epoch_position: 0,
            epoch_number: 0,
            epoch_id: Default::default(),
            node_priority: 0,
        });

        members.push(MemberInfo {
            instance_id: uuid::Uuid::new_v4(),
            time_stamp: rng.next_u32() as i64,
            state: VNodeState::Follower,
            is_alive: true,
            http_end_point: Endpoint {
                host: "localhost".to_string(),
                port: rng.next_u32(),
            },
            last_commit_position: 0,
            writer_checkpoint: 0,
            chaser_checkpoint: 0,
            epoch_position: 0,
            epoch_number: 0,
            epoch_id: Default::default(),
            node_priority: 0,
        });

        members.push(MemberInfo {
            instance_id: uuid::Uuid::new_v4(),
            time_stamp: rng.next_u32() as i64,
            state: VNodeState::Follower,
            is_alive: true,
            http_end_point: Endpoint {
                host: "localhost".to_string(),
                port: rng.next_u32(),
            },
            last_commit_position: 0,
            writer_checkpoint: 0,
            chaser_checkpoint: 0,
            epoch_position: 0,
            epoch_number: 0,
            epoch_id: Default::default(),
            node_priority: 0,
        });

        members.push(MemberInfo {
            instance_id: uuid::Uuid::new_v4(),
            time_stamp: rng.next_u32() as i64,
            state: VNodeState::ReadOnlyReplica,
            is_alive: true,
            http_end_point: Endpoint {
                host: "localhost".to_string(),
                port: rng.next_u32(),
            },
            last_commit_position: 0,
            writer_checkpoint: 0,
            chaser_checkpoint: 0,
            epoch_position: 0,
            epoch_number: 0,
            epoch_id: Default::default(),
            node_priority: 0,
        });

        members.push(MemberInfo {
            instance_id: uuid::Uuid::new_v4(),
            time_stamp: rng.next_u32() as i64,
            state: VNodeState::ReadOnlyReplica,
            is_alive: true,
            http_end_point: Endpoint {
                host: "localhost".to_string(),
                port: rng.next_u32(),
            },
            last_commit_position: 0,
            writer_checkpoint: 0,
            chaser_checkpoint: 0,
            epoch_position: 0,
            epoch_number: 0,
            epoch_id: Default::default(),
            node_priority: 0,
        });

        let opt1 = super::determine_best_node(&mut rng, pref, members.as_slice());
        let mut opt2 = super::determine_best_node(&mut rng, pref, members.as_slice());

        assert!(opt1.is_some());
        assert!(opt2.is_some());

        if pref != NodePreference::Random {
            // We make sure that the selected node matches the preference.
            assert!(
                members
                    .iter()
                    .any(|m| m.http_end_point == opt1.as_ref().unwrap().clone()
                        && pref.match_preference(&m.state)),
                "Someone broke the node selection implementation!"
            );
        }

        // In case of the leader, we make sure that we are always returning the same node.
        if let NodePreference::Leader = pref {
            assert_eq!(opt1, opt2);
        } else {
            // When not asking for a leader, we want to still introduce some randomness
            // while still meeting the node preference.
            if opt1 != opt2 {
                if pref != NodePreference::Random {
                    // We make sure that the selected node matches the preference.
                    assert!(
                        members
                            .iter()
                            .any(|m| m.http_end_point == opt2.as_ref().unwrap().clone()
                                && pref.match_preference(&m.state)),
                        "Someone broke the node selection implementation!"
                    );
                }

                return;
            }

            // It's still possible to have the same selected node two times in a row.
            // We re-run the node selection process many times to ensure chaos is running
            // its course.
            for _ in 0..100 {
                opt2 = super::determine_best_node(&mut rng, pref, members.as_slice());
                if opt2.is_some() && opt1 != opt2 {
                    if pref != NodePreference::Random {
                        // We make sure that the selected node matches the preference.
                        assert!(
                            members
                                .iter()
                                .any(|m| m.http_end_point == opt2.as_ref().unwrap().clone()
                                    && pref.match_preference(&m.state)),
                            "Someone broke the node selection implementation!"
                        );
                    }

                    return;
                }
            }

            // If after that we keep having the same selected node, it probably means
            // the implementation is wrong.
            panic!("Not random enough, someone broke the node selection implementation!");
        }
    }
}
