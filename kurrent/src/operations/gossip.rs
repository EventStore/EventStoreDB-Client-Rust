use crate::event_store::client::gossip as wire;
use crate::grpc::HyperClient;
use crate::http::http_configure_auth;
use crate::request::build_request_metadata;
use crate::types::Endpoint;
use crate::{grpc, ClientSettings};
use serde::{Deserialize, Serialize};
use tonic::{Request, Status};
use uuid::Uuid;

pub async fn read(
    settings: &ClientSettings,
    client: &HyperClient,
    uri: hyper::Uri,
) -> Result<Vec<MemberInfo>, Status> {
    let inner = wire::gossip_client::GossipClient::with_origin(client, uri);
    let mut req = Request::new(());

    *req.metadata_mut() = build_request_metadata(settings, &Default::default());

    let wire_members = inner.clone().read(req).await?.into_inner().members;

    let mut members = Vec::with_capacity(wire_members.capacity());
    for wire_member in wire_members {
        let state = VNodeState::from_i32(wire_member.state)?;

        let instance_id = if let Some(wire_uuid) = wire_member.instance_id {
            wire_uuid.try_into().unwrap()
        } else {
            Uuid::nil()
        };

        let http_end_point = if let Some(endpoint) = wire_member.http_end_point {
            let endpoint = Endpoint {
                host: endpoint.address,
                port: endpoint.port,
            };

            Ok(endpoint)
        } else {
            Err(Status::failed_precondition(
                "MemberInfo endpoint must be defined",
            ))
        }?;

        let member = MemberInfo {
            instance_id,
            state,
            is_alive: wire_member.is_alive,
            time_stamp: wire_member.time_stamp,
            http_end_point,
            last_commit_position: 0,
            writer_checkpoint: 0,
            chaser_checkpoint: 0,
            epoch_position: 0,
            epoch_number: 0,
            epoch_id: Default::default(),
            node_priority: 0,
        };
        members.push(member);
    }

    Ok(members)
}

pub(crate) async fn http_read(
    setts: &ClientSettings,
    handle: grpc::Handle,
) -> Result<Vec<MemberInfo>, Box<dyn std::error::Error>> {
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(!setts.tls_verify_cert)
        .build()?;

    let resp = http_configure_auth(
        client.get(format!("{}/gossip", handle.url())),
        setts.default_user_name.as_ref(),
    )
    .send()
    .await?;

    let gossip = resp.json::<Gossip>().await?;

    Ok(gossip
        .members
        .into_iter()
        .map(|i| MemberInfo {
            instance_id: i.instance_id,
            time_stamp: i.time_stamp.timestamp(),
            state: i.state,
            is_alive: i.is_alive,
            http_end_point: Endpoint {
                host: i.external_http_ip,
                port: i.external_http_port as u32,
            },
            last_commit_position: i.last_commit_position,
            writer_checkpoint: i.writer_checkpoint,
            chaser_checkpoint: i.chaser_checkpoint,
            epoch_position: i.epoch_position,
            epoch_number: i.epoch_number,
            epoch_id: i.epoch_id,
            node_priority: i.node_priority,
        })
        .collect())
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Gossip {
    members: Vec<HttpMemberInfo>,
}

#[derive(Debug, Clone)]
pub struct MemberInfo {
    pub instance_id: Uuid,
    pub time_stamp: i64,
    pub state: VNodeState,
    pub is_alive: bool,
    pub http_end_point: Endpoint,
    pub last_commit_position: i64,
    pub writer_checkpoint: i64,
    pub chaser_checkpoint: i64,
    pub epoch_position: i64,
    pub epoch_number: i64,
    pub epoch_id: Uuid,
    pub node_priority: i64,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HttpMemberInfo {
    pub instance_id: Uuid,
    pub time_stamp: chrono::DateTime<chrono::Utc>,
    pub state: VNodeState,
    pub is_alive: bool,
    pub internal_tcp_ip: String,
    pub internal_tcp_port: u16,
    pub internal_secure_tcp_port: u16,
    pub external_tcp_ip: String,
    pub external_secure_tcp_port: u16,
    #[serde(rename = "httpEndPointIp")]
    pub external_http_ip: String,
    #[serde(rename = "httpEndPointPort")]
    pub external_http_port: u16,
    pub last_commit_position: i64,
    pub writer_checkpoint: i64,
    pub chaser_checkpoint: i64,
    pub epoch_position: i64,
    pub epoch_number: i64,
    pub epoch_id: Uuid,
    pub node_priority: i64,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum VNodeState {
    Initializing,
    DiscoverLeader,
    Unknown,
    PreReplica,
    CatchingUp,
    Clone,
    Follower,
    PreLeader,
    Leader,
    Manager,
    ShuttingDown,
    Shutdown,
    ReadOnlyLeaderLess,
    PreReadOnlyReplica,
    ReadOnlyReplica,
    ResigningLeader,
}

impl VNodeState {
    pub fn from_i32(value: i32) -> Result<Self, Status> {
        match value {
            0 => Ok(VNodeState::Initializing),
            1 => Ok(VNodeState::DiscoverLeader),
            2 => Ok(VNodeState::Unknown),
            3 => Ok(VNodeState::PreReplica),
            4 => Ok(VNodeState::CatchingUp),
            5 => Ok(VNodeState::Clone),
            6 => Ok(VNodeState::Follower),
            7 => Ok(VNodeState::PreLeader),
            8 => Ok(VNodeState::Leader),
            9 => Ok(VNodeState::Manager),
            10 => Ok(VNodeState::ShuttingDown),
            11 => Ok(VNodeState::Shutdown),
            12 => Ok(VNodeState::ReadOnlyLeaderLess),
            13 => Ok(VNodeState::PreReadOnlyReplica),
            14 => Ok(VNodeState::ReadOnlyReplica),
            15 => Ok(VNodeState::ResigningLeader),
            _ => Err(Status::out_of_range(format!(
                "Unknown VNodeState value: {}",
                value
            ))),
        }
    }
}
