pub const MAX_RECEIVE_MESSAGE_SIZE: usize = 17 * 1024 * 1024;

pub mod gossip {
    pub use super::super::generated::gossip::*;
}

pub mod persistent {
    pub use super::super::generated::persistent::*;
}

pub mod projections {
    pub use super::super::generated::projections::*;
}

pub mod streams {
    pub use super::super::generated::streams::*;
}
