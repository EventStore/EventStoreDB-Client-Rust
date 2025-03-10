use crate::event_store::client::streams::append_req::options::ExpectedStreamRevision;
use crate::private::Sealed;
use crate::{EventData, StreamState};
use eventstore_macros::options;

options! {
    #[derive(Clone)]
    /// Options of the append to stream command.
    pub struct AppendToStreamOptions {
        pub(crate) version: ExpectedStreamRevision,
    }
}

impl Default for AppendToStreamOptions {
    fn default() -> Self {
        Self {
            version: ExpectedStreamRevision::Any(()),
            common_operation_options: Default::default(),
        }
    }
}

impl AppendToStreamOptions {
    /// Asks the server to check that the stream receiving the event is at
    /// the given stream state. Default: `StreamState::Any`.
    pub fn stream_state(self, version: StreamState) -> Self {
        let version = match version {
            StreamState::Any => ExpectedStreamRevision::Any(()),
            StreamState::StreamExists => ExpectedStreamRevision::StreamExists(()),
            StreamState::NoStream => ExpectedStreamRevision::NoStream(()),
            StreamState::StreamRevision(version) => ExpectedStreamRevision::Revision(version),
        };

        Self { version, ..self }
    }
}

pub struct Streaming<I>(pub I);

pub trait ToEvents: Sealed {
    type Events: Iterator<Item = EventData> + Send + 'static;
    fn into_events(self) -> Self::Events;
}

impl ToEvents for EventData {
    type Events = std::option::IntoIter<EventData>;

    fn into_events(self) -> Self::Events {
        Some(self).into_iter()
    }
}

impl ToEvents for Vec<EventData> {
    type Events = std::vec::IntoIter<EventData>;

    fn into_events(self) -> Self::Events {
        self.into_iter()
    }
}

impl<I> ToEvents for Streaming<I>
where
    I: Iterator<Item = EventData> + Send + 'static,
{
    type Events = I;

    fn into_events(self) -> Self::Events {
        self.0
    }
}
