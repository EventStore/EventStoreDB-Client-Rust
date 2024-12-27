//! Official Rust [KurrentDB] gRPC Client.
//!
//! [KurrentDB] is an open-source database built from the ground up for Event Sourcing, with Complex Event Processing in Javascript.
//!
//! ## KurrentDB Server Compatibility
//! This client is compatible with version `20.6.1` upwards and works on Linux, MacOS and Windows.
//!
//!
//! Server setup instructions can be found here [KurrentDB Docs], follow the docker setup for the simplest configuration.
//!
//! # Example
//!
//! ```no_run
//! use eventstore::{ Client, EventData };
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize, Debug)]
//! struct Foo {
//!     is_rust_a_nice_language: bool,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!
//!     // Creates a client settings for a single node configuration.
//!     let settings = "esdb://admin:changeit@localhost:2113".parse()?;
//!     let client = Client::new(settings)?;
//!
//!     let payload = Foo {
//!         is_rust_a_nice_language: true,
//!     };
//!
//!     // It is not mandatory to use JSON as a data format however KurrentDB
//!     // provides great additional value if you do so.
//!     let evt = EventData::json("language-poll", &payload)?;
//!
//!     let _ = client
//!         .append_to_stream("language-stream", &Default::default(), evt)
//!         .await?;
//!
//!     let mut stream = client
//!         .read_stream("language-stream", &Default::default())
//!         .await?;
//!
//!     while let Some(event) = stream.next().await? {
//!         let event = event.get_original_event()
//!             .as_json::<Foo>()?;
//!
//!         // Do something productive with the result.
//!         println!("{:?}", event);
//!     }
//!
//!     Ok(())
//! }
//! ```
//! [KurrentDB]: https://eventstore.com/
//! [eventstoredb docs]: https://developers.eventstore.com/server/20.6/server/installation/
#[macro_use]
extern crate log;

mod batch;
mod client;
mod commands;
mod event_store;
mod grpc;
mod http;
pub mod operations;
mod options;
mod private;
mod projection_client;
pub(crate) mod request;
mod server_features;
mod types;

pub(crate) mod google {
    pub mod rpc {
        pub use super::super::event_store::generated::google_rpc::*;
    }
}

pub use batch::*;
pub use client::Client;
pub use commands::{PersistentSubscription, ReadEvent, ReadStream, Subscription};
pub use grpc::{ClientSettings, ClientSettingsParseError};
pub use options::append_to_stream::*;
pub use options::batch_append::*;
pub use options::delete_stream::*;
pub use options::persistent_subscription::*;
pub use options::projections::*;
pub use options::read_all::*;
pub use options::read_stream::*;
pub use options::retry::*;
pub use options::subscribe_to_all::*;
pub use options::subscribe_to_stream::*;
pub use options::tombstone_stream::*;
pub use projection_client::*;
pub use types::*;

pub mod prelude {
    pub use crate::batch::*;
    pub use crate::client::Client;
    pub use crate::commands::{PersistentSubscription, ReadEvent, ReadStream, Subscription};
    pub use crate::grpc::{ClientSettings, ClientSettingsParseError};
    pub use crate::options::append_to_stream::*;
    pub use crate::options::batch_append::*;
    pub use crate::options::delete_stream::*;
    pub use crate::options::persistent_subscription::*;
    pub use crate::options::projections::*;
    pub use crate::options::read_all::*;
    pub use crate::options::read_stream::*;
    pub use crate::options::retry::*;
    pub use crate::options::subscribe_to_all::*;
    pub use crate::options::subscribe_to_stream::*;
    pub use crate::options::tombstone_stream::*;
    pub use crate::projection_client::*;
    pub use crate::types::*;
}
