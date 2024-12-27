use std::time::Duration;

use crate::Credentials;

pub mod append_to_stream;
pub mod batch_append;
pub mod delete_stream;
pub mod persistent_subscription;
pub mod projections;
pub mod read_all;
pub mod read_stream;
pub mod retry;
pub mod subscribe_to_all;
pub mod subscribe_to_stream;
pub mod tombstone_stream;

pub(crate) trait Options {
    fn common_operation_options(&self) -> &CommonOperationOptions;
    fn kind(&self) -> OperationKind;
}

#[derive(Clone, Default)]
pub(crate) struct CommonOperationOptions {
    pub(crate) credentials: Option<Credentials>,
    pub(crate) requires_leader: bool,
    pub(crate) deadline: Option<Duration>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum OperationKind {
    Regular,
    Streaming,
}
