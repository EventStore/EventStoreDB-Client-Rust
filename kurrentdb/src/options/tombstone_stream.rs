use crate::StreamState;
use eventstore_macros::options;

options! {
    #[derive(Clone)]
    /// Options of the tombstone stream command.
    pub struct TombstoneStreamOptions {
        pub(crate) stream_state: StreamState,
    }
}

impl Default for TombstoneStreamOptions {
    fn default() -> Self {
        Self {
            stream_state: StreamState::Any,
            common_operation_options: Default::default(),
        }
    }
}

impl TombstoneStreamOptions {
    /// Asks the server to check that the stream receiving the event is at
    /// the given expected version. Default: `StreamState::Any`.
    pub fn stream_state(self, stream_state: StreamState) -> Self {
        Self {
            stream_state,
            ..self
        }
    }
}
