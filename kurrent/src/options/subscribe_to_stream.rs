use crate::options::retry::RetryOptions;
use crate::StreamPosition;
use eventstore_macros::{options, streaming};

options! {
    #[derive(Clone)]
    #[streaming]
    pub struct SubscribeToStreamOptions {
        pub(crate) position: StreamPosition<u64>,
        pub(crate) resolve_link_tos: bool,
        pub(crate) retry: Option<RetryOptions>,
    }
}

impl Default for SubscribeToStreamOptions {
    fn default() -> Self {
        Self {
            position: StreamPosition::End,
            resolve_link_tos: false,
            retry: None,
            common_operation_options: Default::default(),
        }
    }
}

impl SubscribeToStreamOptions {
    /// For example, if a starting point of 50 is specified when a stream has
    /// 100 events in it, the subscriber can expect to see events 51 through
    /// 100, and then any events subsequently written events until such time
    /// as the subscription is dropped or closed.
    ///
    /// By default, it's `StreamPosition::End`
    pub fn start_from(self, position: StreamPosition<u64>) -> Self {
        Self { position, ..self }
    }

    /// When using projections, you can have links placed into another stream.
    /// If you set `true`, the server will resolve those links and will return
    /// the event that the link points to. Default: [NoResolution](../types/enum.LinkTos.html).
    pub fn resolve_link_tos(self) -> Self {
        Self {
            resolve_link_tos: true,
            ..self
        }
    }

    /// When a disconnection happens, automatically resubscribe to stream changes. When enabled,
    /// The client will keep track of the current subscription offset.
    pub fn retry_options(self, options: RetryOptions) -> Self {
        Self {
            retry: Some(options),
            ..self
        }
    }
}
