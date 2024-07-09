#![allow(dead_code)]

use std::collections::HashMap;
use testcontainers::{core::WaitFor, Image};

const REGISTRY: &str = "docker.eventstore.com";
const DEFAULT_REPO: &str = "eventstore-ce";
const DEFAULT_CONTAINER: &str = "eventstoredb-ce";
const DEFAULT_TAG: &str = "latest";

#[derive(Debug, Clone)]
pub struct ESDB {
    tag: String,
    repo: String,
    container: String,
    env_vars: HashMap<String, String>,
    vol_vars: HashMap<String, String>,
}

impl ESDB {
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

            self.vol_vars.insert(
                certs.as_path().display().to_string(),
                "/etc/eventstore/certs".to_string(),
            );

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
        self.vol_vars
            .insert(volume, "/var/lib/eventstore".to_string());

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

impl Image for ESDB {
    type Args = ();

    fn name(&self) -> String {
        format!("{}/{}/{}", REGISTRY, self.repo, self.container)
    }

    fn tag(&self) -> String {
        self.tag.clone()
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stdout("SPARTA!")]
    }

    fn env_vars(&self) -> Box<dyn Iterator<Item = (&String, &String)> + '_> {
        Box::new(self.env_vars.iter())
    }

    fn volumes(&self) -> Box<dyn Iterator<Item = (&String, &String)> + '_> {
        Box::new(self.vol_vars.iter())
    }

    fn expose_ports(&self) -> Vec<u16> {
        vec![2_113]
    }
}

impl Default for ESDB {
    fn default() -> Self {
        let tag = option_env!("ESDB_DOCKER_CONTAINER_VERSION").unwrap_or(DEFAULT_TAG);
        let repo = option_env!("ESDB_DOCKER_REPO").unwrap_or(DEFAULT_REPO);
        let container = option_env!("ESDB_DOCKER_CONTAINER").unwrap_or(DEFAULT_CONTAINER);
        let mut env_vars = HashMap::new();

        env_vars.insert(
            "EVENTSTORE_GOSSIP_ON_SINGLE_NODE".to_string(),
            "true".to_string(),
        );

        ESDB {
            tag: tag.to_string(),
            repo: repo.to_string(),
            container: container.to_string(),
            env_vars,
            vol_vars: HashMap::new(),
        }
    }
}
