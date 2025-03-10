use crate::StreamState;
use eventstore_macros::options;

options! {
    #[derive(Clone)]
    /// Options of the delete stream command.
    pub struct DeleteStreamOptions {
        pub(crate) version: StreamState,
    }
}

impl Default for DeleteStreamOptions {
    fn default() -> Self {
        Self {
            version: StreamState::Any,
            common_operation_options: Default::default(),
        }
    }
}

impl DeleteStreamOptions {
    /// Asks the server to check that the stream receiving the event is at
    /// the given expected version. Default: `ExpectedVersion::Any`.
    pub fn stream_state(self, version: StreamState) -> Self {
        Self { version, ..self }
    }
}
