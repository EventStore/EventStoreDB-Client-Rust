#![allow(dead_code)]

use std::{borrow::Cow, collections::HashMap};
use testcontainers::{
    core::{ContainerPort, Mount, WaitFor},
    Image,
};

const DEFAULT_REGISTRY: &str = "docker.io";
const DEFAULT_REPO: &str = "eventstore";
const DEFAULT_CONTAINER: &str = "eventstore";
const DEFAULT_TAG: &str = "latest";

#[derive(Debug, Clone)]
pub struct EventStoreDB {
    name: String,
    tag: String,
    env_vars: HashMap<String, String>,
    mounts: Vec<Mount>,
}

impl EventStoreDB {
    pub fn insecure_mode(mut self) -> Self {
        self.env_vars
            .insert("EVENTSTORE_INSECURE".to_string(), "true".to_string());
        self.env_vars.insert(
            "EVENTSTORE_ENABLE_ATOM_PUB_OVER_HTTP".to_string(),
            "true".to_string(),
        );

        self
    }

    pub fn secure_mode(mut self, is_secure: bool) -> Self {
        if is_secure {
            self.verify_certificates_exist().unwrap();

            self.env_vars.insert(
                "EVENTSTORE_CERTIFICATE_FILE".to_string(),
                "/etc/eventstore/certs/node/node.crt".to_string(),
            );

            self.env_vars.insert(
                "EVENTSTORE_CERTIFICATE_PRIVATE_KEY_FILE".to_string(),
                "/etc/eventstore/certs/node/node.key".to_string(),
            );

            self.env_vars.insert(
                "EVENTSTORE_TRUSTED_ROOT_CERTIFICATES_PATH".to_string(),
                "/etc/eventstore/certs/ca".to_string(),
            );

            let mut certs = std::env::current_dir().unwrap();
            certs.push("certs");

            self.mounts.push(Mount::bind_mount(
                certs.as_path().display().to_string(),
                "/etc/eventstore/certs".to_string(),
            ));

            self
        } else {
            self.insecure_mode()
        }
    }

    pub fn enable_projections(mut self) -> Self {
        self.env_vars
            .insert("EVENTSTORE_RUN_PROJECTIONS".to_string(), "all".to_string());
        self.env_vars.insert(
            "EVENTSTORE_START_STANDARD_PROJECTIONS".to_string(),
            "true".to_string(),
        );

        self
    }

    pub fn attach_volume_to_db_directory(mut self, volume: String) -> Self {
        self.mounts
            .push(Mount::bind_mount(volume, "/var/lib/eventstore".to_string()));

        self
    }

    pub fn forward_eventstore_env_variables(mut self, forward: bool) -> Self {
        if !forward {
            return self;
        }

        for (key, value) in std::env::vars_os() {
            if let Some((key, value)) = key.to_str().zip(value.to_str()) {
                if !key.to_lowercase().starts_with("eventstore") {
                    continue;
                }

                self.env_vars.insert(key.to_string(), value.to_string());
            }
        }

        self
    }

    fn verify_certificates_exist(&self) -> std::io::Result<()> {
        let mut root_dir = std::env::current_dir()?;
        let certs = &[
            ["ca", "ca.crt"],
            ["ca", "ca.key"],
            ["node", "node.crt"],
            ["node", "node.key"],
        ];

        root_dir.push("certs");

        for paths in certs {
            let mut tmp = root_dir.clone();

            for path in paths {
                tmp.push(path);
            }

            if !tmp.as_path().exists() {
                panic!("certificates directory is not configured properly, please run 'docker-compose --file configure-tls-for-tests.yml up'");
            }
        }

        Ok(())
    }
}

impl Image for EventStoreDB {
    fn name(&self) -> &str {
        &self.name
    }

    fn tag(&self) -> &str {
        &self.tag
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stdout("SPARTA!")]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        self.env_vars.iter()
    }

    fn mounts(&self) -> impl IntoIterator<Item = &Mount> {
        self.mounts.iter()
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[ContainerPort::Tcp(2_113)]
    }
}

impl Default for EventStoreDB {
    fn default() -> Self {
        let registry = option_env!("ESDB_DOCKER_REGISTRY").unwrap_or(DEFAULT_REGISTRY);
        let tag = option_env!("ESDB_DOCKER_CONTAINER_VERSION").unwrap_or(DEFAULT_TAG);
        let repo = option_env!("ESDB_DOCKER_REPO").unwrap_or(DEFAULT_REPO);
        let container = option_env!("ESDB_DOCKER_CONTAINER").unwrap_or(DEFAULT_CONTAINER);
        let mut env_vars = HashMap::new();

        EventStoreDB {
            name: format!("{}/{}/{}", registry, repo, container),
            tag: tag.to_string(),
            env_vars,
            mounts: vec![],
        }
    }
}
