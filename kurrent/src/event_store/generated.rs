use crate::{
    CurrentRevision, EventData, ExpectedRevision, PersistentSubscriptionConnectionInfo,
    PersistentSubscriptionEvent, PersistentSubscriptionInfo, PersistentSubscriptionMeasurements,
    PersistentSubscriptionSettings, PersistentSubscriptionStats, Position, RecordedEvent,
    ResolvedEvent, RevisionOrPosition, StreamPosition, SystemConsumerStrategy, WriteResult,
};
use chrono::{DateTime, Utc};
use nom::AsBytes;
use std::ops::Add;
use std::time::{Duration, SystemTime};

pub mod common;
pub mod google_rpc;
pub mod gossip;
pub mod monitoring;
pub mod operations;
pub mod persistent;
pub mod projections;
pub mod server_features;
pub mod streams;
pub mod users;

impl TryFrom<common::Uuid> for uuid::Uuid {
    type Error = uuid::Error;

    fn try_from(value: common::Uuid) -> Result<Self, Self::Error> {
        use byteorder::{BigEndian, ByteOrder};

        if let Some(value) = value.value {
            match value {
                common::uuid::Value::Structured(s) => {
                    let mut buf = [0u8; 16];

                    BigEndian::write_i64(&mut buf, s.most_significant_bits);
                    BigEndian::write_i64(&mut buf[8..16], s.least_significant_bits);

                    Ok(uuid::Uuid::from_bytes(buf))
                }

                common::uuid::Value::String(s) => s.parse(),
            }
        } else {
            Ok(uuid::Uuid::nil())
        }
    }
}

impl From<uuid::Uuid> for common::Uuid {
    fn from(value: uuid::Uuid) -> Self {
        let integer = value.as_u128();
        let most_significant = (integer >> 64) as u64;
        let least_significant = integer as u64;

        common::Uuid {
            value: Some(common::uuid::Value::Structured(common::uuid::Structured {
                most_significant_bits: most_significant as i64,
                least_significant_bits: least_significant as i64,
            })),
        }
    }
}

impl From<EventData> for streams::AppendReq {
    fn from(value: EventData) -> Self {
        let id = value.id_opt.unwrap_or_else(uuid::Uuid::new_v4).into();
        let custom_metadata = value.custom_metadata.unwrap_or_default();

        let msg = streams::append_req::ProposedMessage {
            id: Some(id),
            metadata: value.metadata,
            custom_metadata,
            data: value.payload,
        };

        let content = streams::append_req::Content::ProposedMessage(msg);

        streams::AppendReq {
            content: Some(content),
        }
    }
}

impl From<streams::append_resp::Success> for WriteResult {
    fn from(value: streams::append_resp::Success) -> Self {
        let next_expected_version = match value.current_revision_option.unwrap() {
            streams::append_resp::success::CurrentRevisionOption::CurrentRevision(rev) => rev,
            streams::append_resp::success::CurrentRevisionOption::NoStream(_) => 0,
        };

        let position = match value.position_option.unwrap() {
            streams::append_resp::success::PositionOption::Position(pos) => Position {
                commit: pos.commit_position,
                prepare: pos.prepare_position,
            },

            streams::append_resp::success::PositionOption::NoPosition(_) => Position::start(),
        };

        WriteResult {
            next_expected_version,
            position,
        }
    }
}

impl From<streams::append_resp::WrongExpectedVersion> for crate::Error {
    fn from(value: streams::append_resp::WrongExpectedVersion) -> Self {
        let current = match value.current_revision_option.unwrap() {
            streams::append_resp::wrong_expected_version::CurrentRevisionOption::CurrentRevision(rev) => CurrentRevision::Current(rev),
            streams::append_resp::wrong_expected_version::CurrentRevisionOption::CurrentNoStream(_) => CurrentRevision::NoStream,
        };

        let expected = match value.expected_revision_option.unwrap() {
            streams::append_resp::wrong_expected_version::ExpectedRevisionOption::ExpectedRevision(rev) => ExpectedRevision::Exact(rev),
            streams::append_resp::wrong_expected_version::ExpectedRevisionOption::ExpectedAny(_) => ExpectedRevision::Any,
            streams::append_resp::wrong_expected_version::ExpectedRevisionOption::ExpectedStreamExists(_) => ExpectedRevision::StreamExists,
            streams::append_resp::wrong_expected_version::ExpectedRevisionOption::ExpectedNoStream(_) => ExpectedRevision::NoStream,
        };

        crate::Error::WrongExpectedVersion { current, expected }
    }
}

impl From<streams::read_resp::read_event::RecordedEvent> for RecordedEvent {
    fn from(value: streams::read_resp::read_event::RecordedEvent) -> Self {
        let id = value.id.unwrap().try_into().unwrap();

        let position = Position {
            commit: value.commit_position,
            prepare: value.prepare_position,
        };

        let event_type = value.metadata.get("type").cloned().unwrap_or_default();

        let created: DateTime<Utc> = if let Some(ticks_since_epoch) = value.metadata.get("created")
        {
            let ticks_since_epoch = ticks_since_epoch.parse::<u64>().unwrap_or(0);
            let secs_since_epoch = ticks_since_epoch / 10_000_000;

            SystemTime::UNIX_EPOCH
                .add(Duration::from_secs(secs_since_epoch))
                .into()
        } else {
            SystemTime::UNIX_EPOCH.into()
        };

        let is_json = if let Some(content_type) = value.metadata.get("content-type") {
            matches!(content_type.as_str(), "application/json")
        } else {
            false
        };

        let stream_id = String::from_utf8_lossy(
            value
                .stream_identifier
                .expect("stream_identifier is always defined")
                .stream_name
                .as_bytes(),
        )
        .to_string();

        RecordedEvent {
            id,
            stream_id,
            revision: value.stream_revision,
            position,
            event_type,
            is_json,
            created,
            metadata: value.metadata,
            custom_metadata: value.custom_metadata,
            data: value.data,
        }
    }
}

impl From<persistent::read_resp::read_event::RecordedEvent>
    for streams::read_resp::read_event::RecordedEvent
{
    fn from(value: persistent::read_resp::read_event::RecordedEvent) -> Self {
        streams::read_resp::read_event::RecordedEvent {
            id: value.id,
            stream_identifier: value.stream_identifier,
            stream_revision: value.stream_revision,
            prepare_position: value.prepare_position,
            commit_position: value.commit_position,
            metadata: value.metadata,
            custom_metadata: value.custom_metadata,
            data: value.data,
        }
    }
}

impl From<persistent::read_resp::read_event::RecordedEvent> for RecordedEvent {
    fn from(value: persistent::read_resp::read_event::RecordedEvent) -> Self {
        let value: streams::read_resp::read_event::RecordedEvent = value.into();
        value.into()
    }
}

impl From<streams::read_resp::ReadEvent> for ResolvedEvent {
    fn from(value: streams::read_resp::ReadEvent) -> Self {
        let commit_position = if let Some(pos_alt) = value.position {
            match pos_alt {
                streams::read_resp::read_event::Position::CommitPosition(pos) => Some(pos),
                streams::read_resp::read_event::Position::NoPosition(_) => None,
            }
        } else {
            None
        };

        ResolvedEvent {
            event: value.event.map(|e| e.into()),
            link: value.link.map(|e| e.into()),
            commit_position,
        }
    }
}

impl<'a, A> TryFrom<&'a PersistentSubscriptionSettings<A>> for persistent::create_req::Settings {
    type Error = crate::Error;

    fn try_from(value: &'a PersistentSubscriptionSettings<A>) -> Result<Self, Self::Error> {
        let named_consumer_strategy = value.named_consumer_strategy_as_i32()?;
        let consumer_strategy = value.consumer_strategy_name.to_string();

        #[allow(deprecated)]
        let settings = persistent::create_req::Settings {
            resolve_links: value.resolve_link_tos,
            revision: 0,
            extra_statistics: value.extra_statistics,
            message_timeout: Some(
                persistent::create_req::settings::MessageTimeout::MessageTimeoutMs(
                    value.message_timeout.as_millis() as i32,
                ),
            ),
            max_retry_count: value.max_retry_count,
            checkpoint_after: Some(
                persistent::create_req::settings::CheckpointAfter::CheckpointAfterMs(
                    value.checkpoint_after.as_millis() as i32,
                ),
            ),
            min_checkpoint_count: value.checkpoint_lower_bound,
            max_checkpoint_count: value.checkpoint_upper_bound,
            max_subscriber_count: value.max_subscriber_count,
            live_buffer_size: value.live_buffer_size,
            read_batch_size: value.read_batch_size,
            history_buffer_size: value.history_buffer_size,
            named_consumer_strategy,
            consumer_strategy,
        };

        Ok(settings)
    }
}

impl<'a, A> TryFrom<&'a PersistentSubscriptionSettings<A>> for persistent::update_req::Settings {
    type Error = crate::Error;

    fn try_from(value: &'a PersistentSubscriptionSettings<A>) -> Result<Self, Self::Error> {
        let named_consumer_strategy = value.named_consumer_strategy_as_i32()?;

        #[allow(deprecated)]
        let settings = persistent::update_req::Settings {
            resolve_links: value.resolve_link_tos,
            revision: 0,
            extra_statistics: value.extra_statistics,
            message_timeout: Some(
                persistent::update_req::settings::MessageTimeout::MessageTimeoutMs(
                    value.message_timeout.as_millis() as i32,
                ),
            ),
            max_retry_count: value.max_retry_count,
            checkpoint_after: Some(
                persistent::update_req::settings::CheckpointAfter::CheckpointAfterMs(
                    value.checkpoint_after.as_millis() as i32,
                ),
            ),
            min_checkpoint_count: value.checkpoint_lower_bound,
            max_checkpoint_count: value.checkpoint_upper_bound,
            max_subscriber_count: value.max_subscriber_count,
            live_buffer_size: value.live_buffer_size,
            read_batch_size: value.read_batch_size,
            history_buffer_size: value.history_buffer_size,
            named_consumer_strategy,
        };

        Ok(settings)
    }
}

impl From<persistent::read_resp::ReadEvent> for PersistentSubscriptionEvent {
    fn from(value: persistent::read_resp::ReadEvent) -> Self {
        use persistent::read_resp::read_event::Count;
        let commit_position = if let Some(pos_alt) = value.position {
            match pos_alt {
                persistent::read_resp::read_event::Position::CommitPosition(pos) => Some(pos),
                persistent::read_resp::read_event::Position::NoPosition(_) => None,
            }
        } else {
            None
        };

        let resolved = ResolvedEvent {
            event: value.event.map(|e| e.into()),
            link: value.link.map(|e| e.into()),
            commit_position,
        };

        let retry_count = value.count.map_or(0usize, |repr| match repr {
            Count::RetryCount(count) => count as usize,
            Count::NoRetryCount(_) => 0,
        });

        PersistentSubscriptionEvent::EventAppeared {
            retry_count,
            event: resolved,
        }
    }
}

impl From<persistent::read_resp::Content> for PersistentSubscriptionEvent {
    fn from(value: persistent::read_resp::Content) -> Self {
        match value {
            persistent::read_resp::Content::Event(e) => e.into(),
            persistent::read_resp::Content::SubscriptionConfirmation(c) => {
                PersistentSubscriptionEvent::Confirmed(c.subscription_id)
            }
        }
    }
}

impl From<persistent::subscription_info::ConnectionInfo> for PersistentSubscriptionConnectionInfo {
    fn from(value: persistent::subscription_info::ConnectionInfo) -> Self {
        PersistentSubscriptionConnectionInfo {
            from: value.from,
            username: value.username,
            average_items_per_second: value.average_items_per_second as f64,
            total_items: value.total_items as usize,
            count_since_last_measurement: value.count_since_last_measurement as usize,
            available_slots: value.available_slots as usize,
            in_flight_messages: value.in_flight_messages as usize,
            connection_name: value.connection_name,
            extra_statistics: PersistentSubscriptionMeasurements(
                value
                    .observed_measurements
                    .into_iter()
                    .map(|m| (m.key, m.value))
                    .collect(),
            ),
        }
    }
}

impl TryFrom<persistent::SubscriptionInfo> for PersistentSubscriptionInfo<RevisionOrPosition> {
    type Error = crate::Error;

    fn try_from(value: persistent::SubscriptionInfo) -> Result<Self, Self::Error> {
        let named_consumer_strategy =
            SystemConsumerStrategy::from_string(value.named_consumer_strategy);
        let connections = value
            .connections
            .into_iter()
            .map(|conn| conn.into())
            .collect();
        let status = value.status;
        let mut last_known_position = None;
        let mut last_known_event_number = None;
        let mut last_processed_event_number = None;
        let mut last_processed_position = None;

        if let Ok(value) = parse_revision_or_position(value.last_known_event_position.as_str()) {
            match value {
                RevisionOrPosition::Position(p) => last_known_position = Some(p),
                RevisionOrPosition::Revision(r) => last_known_event_number = Some(r),
            }
        }

        if let Ok(value) =
            parse_revision_or_position(value.last_checkpointed_event_position.as_str())
        {
            match value {
                RevisionOrPosition::Position(p) => last_processed_position = Some(p),
                RevisionOrPosition::Revision(r) => last_processed_event_number = Some(r),
            }
        }
        let start_from = parse_stream_position(value.start_from.as_str())?;
        let stats = PersistentSubscriptionStats {
            average_per_second: value.average_per_second as f64,
            total_items: value.total_items as usize,
            count_since_last_measurement: value.count_since_last_measurement as usize,
            last_checkpointed_event_revision: last_processed_event_number,
            last_known_event_revision: last_known_event_number,
            last_checkpointed_position: last_processed_position,
            last_known_position,
            read_buffer_count: value.read_buffer_count as usize,
            live_buffer_count: value.live_buffer_count as usize,
            retry_buffer_count: value.retry_buffer_count as usize,
            total_in_flight_messages: value.total_in_flight_messages as usize,
            outstanding_messages_count: value.outstanding_messages_count as usize,
            parked_message_count: value.parked_message_count as usize,
        };

        Ok(PersistentSubscriptionInfo {
            event_source: value.event_source,
            group_name: value.group_name,
            status,
            connections,
            settings: Some(PersistentSubscriptionSettings {
                resolve_link_tos: value.resolve_link_tos,
                start_from,
                extra_statistics: value.extra_statistics,
                message_timeout: Duration::from_millis(value.message_timeout_milliseconds as u64),
                max_retry_count: value.max_retry_count,
                live_buffer_size: value.live_buffer_size,
                read_batch_size: value.read_batch_size,
                history_buffer_size: value.buffer_size,
                checkpoint_after: Duration::from_millis(
                    value.check_point_after_milliseconds as u64,
                ),
                checkpoint_lower_bound: value.min_check_point_count,
                checkpoint_upper_bound: value.max_check_point_count,
                max_subscriber_count: value.max_subscriber_count,
                consumer_strategy_name: named_consumer_strategy,
            }),
            stats,
        })
    }
}

pub(crate) fn parse_revision_or_position(input: &str) -> crate::Result<RevisionOrPosition> {
    if let Ok(v) = input.parse::<u64>() {
        Ok(RevisionOrPosition::Revision(v))
    } else if let Ok(pos) = parse_position(input) {
        Ok(RevisionOrPosition::Position(pos))
    } else {
        Err(crate::Error::InternalParsingError(format!(
            "Failed to parse either a stream revision or a transaction log position: '{}'",
            input
        )))
    }
}

pub(crate) fn parse_position(input: &str) -> crate::Result<Position> {
    if let Ok((_, pos)) = nom::combinator::complete(parse_position_start_from)(input) {
        Ok(pos)
    } else {
        Err(crate::Error::InternalParsingError(format!(
            "Failed to parse a transaction log position: '{}'",
            input
        )))
    }
}

pub(crate) fn parse_stream_position(
    input: &str,
) -> crate::Result<StreamPosition<RevisionOrPosition>> {
    if input == "-1" || input == "C:-1/P:-1" {
        return Ok(StreamPosition::End);
    } else if input == "0" || input == "C:0/P:0" {
        return Ok(StreamPosition::Start);
    }

    Ok(StreamPosition::Position(parse_revision_or_position(input)?))
}

pub(crate) fn parse_position_start_from(input: &str) -> nom::IResult<&str, Position> {
    use nom::bytes::complete::tag;

    let (input, _) = tag("C:")(input)?;
    let (input, commit) = nom::character::complete::u64(input)?;
    let (input, _) = tag("/P:")(input)?;
    let (input, prepare) = nom::character::complete::u64(input)?;

    Ok((input, Position { commit, prepare }))
}

#[test]
fn test_uuid_conversion() {
    let id = uuid::Uuid::new_v4();
    let wire: common::Uuid = id.into();

    assert_eq!(id, wire.try_into().unwrap());
}
