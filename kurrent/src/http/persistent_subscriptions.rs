use crate::commands::{BothTypeOfStream, StreamKind, StreamPositionTypeSelector};
use crate::event_store::generated::{parse_position, parse_stream_position};
use crate::grpc::Handle;
use crate::{
    ClientSettings, GetPersistentSubscriptionInfoOptions, ListPersistentSubscriptionsOptions,
    PersistentSubscriptionInfo, PersistentSubscriptionInfoHttpJson, PersistentSubscriptionSettings,
    PersistentSubscriptionStats, ReplayParkedMessagesOptions,
    RestartPersistentSubscriptionSubsystem, RevisionOrPosition,
};
use std::time::Duration;

/// Replays a persistent subscriptions parked events.
pub(crate) async fn replay_parked_messages(
    handle: &Handle,
    http_client: &reqwest::Client,
    settings: &ClientSettings,
    stream_name: impl AsRef<str>,
    group_name: impl AsRef<str>,
    options: &ReplayParkedMessagesOptions,
) -> crate::Result<()> {
    let mut builder = http_client
        .post(format!(
            "{}/subscriptions/{}/{}/replayParked",
            handle.url(),
            urlencoding::encode(stream_name.as_ref()),
            urlencoding::encode(group_name.as_ref()),
        ))
        .header("content-type", "application/json")
        .header("content-length", "0");

    if let Some(stop_at) = options.stop_at {
        builder = builder.query(&[("stopAt", stop_at.to_string().as_str())])
    }

    builder = super::http_configure_auth(
        builder,
        options
            .common_operation_options
            .credentials
            .as_ref()
            .or_else(|| settings.default_authenticated_user().as_ref()),
    );

    super::http_execute_request(builder).await?;

    Ok(())
}

fn from_http_info<Selector>(
    selector: &Selector,
    info: PersistentSubscriptionInfoHttpJson,
) -> crate::Result<PersistentSubscriptionInfo<<Selector as StreamPositionTypeSelector>::Value>>
where
    Selector: StreamPositionTypeSelector,
{
    let settings = if let Some(c) = info.config {
        let mut settings = PersistentSubscriptionSettings {
            resolve_link_tos: c.resolve_linktos,
            extra_statistics: c.extra_statistics,
            message_timeout: Duration::from_millis(c.message_timeout_milliseconds as u64),
            max_retry_count: c.max_retry_count as i32,
            live_buffer_size: c.live_buffer_size as i32,
            read_batch_size: c.read_batch_size as i32,
            checkpoint_after: Duration::from_millis(c.checkpoint_after_milliseconds as u64),
            checkpoint_lower_bound: c.min_checkpoint_count as i32,
            checkpoint_upper_bound: c.max_checkpoint_count as i32,
            max_subscriber_count: c.max_subscriber_count as i32,
            consumer_strategy_name: c.named_consumer_strategy,
            history_buffer_size: c.buffer_size as i32,
            ..Default::default()
        };

        if info.event_stream_id == "$all" {
            settings.start_from =
                parse_stream_position(c.start_position.as_str())?.map(|p| selector.select(p));
        } else {
            settings.start_from = c.start_from.map(|p| selector.select(p));
        };

        Some(settings)
    } else {
        None
    };

    let mut stats = PersistentSubscriptionStats {
        average_per_second: info.average_items_per_second,
        total_items: info.total_items_processed,
        count_since_last_measurement: info.count_since_last_measurement,
        last_checkpointed_event_revision: None,
        last_known_event_revision: None,
        last_checkpointed_position: None,
        last_known_position: None,
        read_buffer_count: info.read_buffer_count,
        live_buffer_count: info.live_buffer_count,
        retry_buffer_count: info.retry_buffer_count,
        total_in_flight_messages: info.total_in_flight_messages,
        outstanding_messages_count: info.outstanding_messages_count,
        parked_message_count: info.parked_message_count,
    };

    if info.event_stream_id == "$all" {
        if let Some(pos) = info.last_checkpointed_event_position {
            stats.last_checkpointed_position = Some(parse_position(pos.as_str())?);
        }

        if let Some(pos) = info.last_known_event_position {
            stats.last_known_position = Some(parse_position(pos.as_str())?);
        }
    } else {
        stats.last_checkpointed_event_revision = Some(info.last_processed_event_number as u64);
        stats.last_known_event_revision = Some(info.last_known_event_number as u64);
    };
    Ok(PersistentSubscriptionInfo {
        event_source: info.event_stream_id,
        group_name: info.group_name,
        status: info.status,
        connections: info.connections,
        settings,
        stats,
    })
}

/// Lists all persistent subscriptions to date.
pub(crate) async fn list_all_persistent_subscriptions(
    handle: &Handle,
    client: &reqwest::Client,
    settings: &ClientSettings,
    options: &ListPersistentSubscriptionsOptions,
) -> crate::Result<Vec<PersistentSubscriptionInfo<RevisionOrPosition>>> {
    let mut builder = client
        .get(format!("{}/subscriptions", handle.url()))
        .header("content-type", "application/json");

    builder = super::http_configure_auth(
        builder,
        options
            .common_operation_options
            .credentials
            .as_ref()
            .or_else(|| settings.default_authenticated_user().as_ref()),
    );

    let resp = super::http_execute_request(builder).await?;

    let infos = resp
        .json::<Vec<PersistentSubscriptionInfoHttpJson>>()
        .await
        .map_err(|e| {
            error!("Error when listing persistent subscriptions: {}", e);
            crate::Error::InternalParsingError(e.to_string())
        })?;

    let mut result = Vec::with_capacity(infos.capacity());

    for info in infos {
        result.push(from_http_info(&BothTypeOfStream, info)?);
    }

    Ok(result)
}

/// List all persistent subscriptions of a specific stream.
pub(crate) async fn list_persistent_subscriptions_for_stream<StreamName>(
    handle: &Handle,
    http_client: &reqwest::Client,
    settings: &ClientSettings,
    stream_name: StreamName,
    options: &ListPersistentSubscriptionsOptions,
) -> crate::Result<Vec<PersistentSubscriptionInfo<<StreamName as StreamPositionTypeSelector>::Value>>>
where
    StreamName: StreamKind + StreamPositionTypeSelector,
{
    let mut builder = http_client
        .get(format!(
            "{}/subscriptions/{}",
            handle.url(),
            urlencoding::encode(stream_name.name())
        ))
        .header("content-type", "application/json");

    builder = super::http_configure_auth(
        builder,
        options
            .common_operation_options
            .credentials
            .as_ref()
            .or_else(|| settings.default_authenticated_user().as_ref()),
    );

    let resp = super::http_execute_request(builder).await?;

    let infos = resp
        .json::<Vec<PersistentSubscriptionInfoHttpJson>>()
        .await
        .map_err(|e| {
            error!("Error when listing persistent subscriptions: {}", e);
            crate::Error::InternalParsingError(e.to_string())
        })?;

    let mut result = Vec::with_capacity(infos.capacity());

    for info in infos {
        result.push(from_http_info(&stream_name, info)?);
    }

    Ok(result)
}

// Gets a specific persistent subscription info.
pub(crate) async fn get_persistent_subscription_info<StreamName>(
    handle: &Handle,
    http_client: &reqwest::Client,
    settings: &ClientSettings,
    stream_name: StreamName,
    group_name: impl AsRef<str>,
    options: &GetPersistentSubscriptionInfoOptions,
) -> crate::Result<PersistentSubscriptionInfo<<StreamName as StreamPositionTypeSelector>::Value>>
where
    StreamName: StreamKind + StreamPositionTypeSelector,
{
    let mut builder = http_client
        .get(format!(
            "{}/subscriptions/{}/{}/info",
            handle.url(),
            urlencoding::encode(stream_name.name()),
            urlencoding::encode(group_name.as_ref()),
        ))
        .header("content-type", "application/json");

    builder = super::http_configure_auth(
        builder,
        options
            .common_operation_options
            .credentials
            .as_ref()
            .or_else(|| settings.default_authenticated_user().as_ref()),
    );

    let resp = super::http_execute_request(builder).await?;

    let info = resp.json().await.map_err(|e| {
        error!("Error when listing persistent subscriptions: {}", e);
        crate::Error::InternalParsingError(e.to_string())
    })?;

    from_http_info(&stream_name, info)
}

pub(crate) async fn restart_persistent_subscription_subsystem(
    handle: &Handle,
    http_client: &reqwest::Client,
    settings: &ClientSettings,
    options: &RestartPersistentSubscriptionSubsystem,
) -> crate::Result<()> {
    let mut builder = http_client
        .post(format!("{}/subscriptions/restart", handle.url(),))
        .header("content-type", "application/json")
        .header("content-length", "0");

    builder = super::http_configure_auth(
        builder,
        options
            .common_operation_options
            .credentials
            .as_ref()
            .or_else(|| settings.default_authenticated_user().as_ref()),
    );

    super::http_execute_request(builder).await?;

    Ok(())
}
