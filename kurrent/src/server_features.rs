use std::{cmp::Ordering, fmt::Display};

use crate::event_store::generated::server_features::server_features_client::ServerFeaturesClient;
use crate::grpc::HyperClient;
use bitflags::bitflags;
use tonic::{Code, Request, Status};

bitflags! {
    #[derive(Clone, Copy, Default, Debug)]
    pub struct Features: u32 {
        const NOTHING = 0;
        const BATCH_APPEND = 1;
        const PERSISTENT_SUBSCRIPTION_LIST = 2;
        const PERSISTENT_SUBSCRIPTION_REPLAY = 4;
        const PERSISTENT_SUBSCRIPTION_RESTART_SUBSYSTEM = 8;
        const PERSISTENT_SUBSCRIPTION_GET_INFO = 16;
        const PERSISTENT_SUBSCRIPTION_MANAGEMENT = Self::PERSISTENT_SUBSCRIPTION_LIST.bits() | Self::PERSISTENT_SUBSCRIPTION_REPLAY.bits() | Self::PERSISTENT_SUBSCRIPTION_RESTART_SUBSYSTEM.bits() | Self::PERSISTENT_SUBSCRIPTION_GET_INFO.bits();
        const PERSISTENT_SUBSCRIPITON_TO_ALL = 32;
    }
}

#[derive(Clone, Copy, Default, Debug)]
pub struct ServerVersion {
    pub(crate) major: usize,
    pub(crate) minor: usize,
    pub(crate) patch: usize,
}

impl ServerVersion {
    pub fn major(&self) -> usize {
        self.major
    }

    pub fn minor(&self) -> usize {
        self.minor
    }

    pub fn patch(&self) -> usize {
        self.patch
    }
}

impl Display for ServerVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major(), self.minor(), self.patch())
    }
}

impl PartialEq<usize> for ServerVersion {
    fn eq(&self, other: &usize) -> bool {
        self.major() == *other
    }
}

impl PartialEq<(usize, usize)> for ServerVersion {
    fn eq(&self, other: &(usize, usize)) -> bool {
        let (maj, min) = other;
        self.eq(maj) && self.minor() == *min
    }
}

impl PartialEq<(usize, usize, usize)> for ServerVersion {
    fn eq(&self, other: &(usize, usize, usize)) -> bool {
        let (maj, min, patch) = other;

        self.eq(&(*maj, *min)) && self.patch() == *patch
    }
}

impl PartialOrd<usize> for ServerVersion {
    fn partial_cmp(&self, other: &usize) -> Option<Ordering> {
        self.major().partial_cmp(other)
    }
}

impl PartialOrd<(usize, usize)> for ServerVersion {
    fn partial_cmp(&self, other: &(usize, usize)) -> Option<Ordering> {
        let (maj, min) = other;
        match self.partial_cmp(maj)? {
            Ordering::Equal => self.minor().partial_cmp(min),
            other => Some(other),
        }
    }
}

impl PartialOrd<(usize, usize, usize)> for ServerVersion {
    fn partial_cmp(&self, other: &(usize, usize, usize)) -> Option<Ordering> {
        let (maj, min, patch) = other;

        match self.partial_cmp(&(*maj, *min))? {
            Ordering::Equal => self.patch().partial_cmp(patch),
            other => Some(other),
        }
    }
}

#[cfg(test)]
mod server_version_tests {
    use super::ServerVersion;

    #[test]
    fn equality_checks() {
        let version = ServerVersion {
            major: 23,
            minor: 10,
            patch: 42,
        };

        assert_eq!(version, 23);
        assert_eq!(version, (23, 10));
        assert_eq!(version, (23, 10, 42));

        assert!(version <= 23);
        assert!(version <= (23, 10));
        assert!(version <= (23, 10, 42));
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ServerInfo {
    pub(crate) version: ServerVersion,
    pub(crate) features: Features,
}

impl ServerInfo {
    pub fn version(&self) -> ServerVersion {
        self.version
    }

    pub fn contains_features(&self, feats: Features) -> bool {
        self.features.contains(feats)
    }
}

pub(crate) async fn supported_methods(
    client: &HyperClient,
    uri: hyper::Uri,
) -> Result<ServerInfo, Status> {
    let mut client = ServerFeaturesClient::with_origin(client, uri);
    let methods = client
        .get_supported_methods(Request::new(()))
        .await?
        .into_inner();

    let mut version = ServerVersion::default();
    let semver: String = methods
        .event_store_server_version
        .chars()
        .filter(|&c| c.is_ascii_digit() || c == '.')
        .collect();

    // Server version uses sem versioning.
    for (idx, ver) in semver.split('.').enumerate() {
        if idx > 2 {
            break;
        }

        match ver.parse::<usize>() {
            Err(_) => return Err(Status::new(Code::InvalidArgument, "invalid semver format")),
            Ok(v) => match idx {
                0 => version.major = v,
                1 => version.minor = v,
                2 => version.patch = v,
                _ => unreachable!(),
            },
        }
    }

    let mut features = Features::NOTHING;

    for method in methods.methods {
        let feats = method.features;
        match (method.method_name.as_str(), method.service_name.as_str()) {
            ("batchappend", "event_store.client.streams.streams") => {
                features |= Features::BATCH_APPEND
            }

            (method, "event_store.client.persistent_subscriptions.persistentsubscriptions") => {
                match method {
                    "create" => {
                        for feat in feats {
                            if feat == "all" {
                                features |= Features::PERSISTENT_SUBSCRIPITON_TO_ALL;
                            }
                        }
                    }
                    "getinfo" => features |= Features::PERSISTENT_SUBSCRIPTION_GET_INFO,
                    "replayparked" => features |= Features::PERSISTENT_SUBSCRIPTION_REPLAY,
                    "list" => features |= Features::PERSISTENT_SUBSCRIPTION_LIST,
                    "restartsubsystem" => {
                        features |= Features::PERSISTENT_SUBSCRIPTION_RESTART_SUBSYSTEM
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    Ok(ServerInfo { version, features })
}
