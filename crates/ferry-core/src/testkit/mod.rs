//! Test support shared by every connector crate: generic contract suites any
//! [`Source`](crate::Source) or [`Sink`](crate::Sink) must pass, and
//! in-memory connectors for testing without a database.
//!
//! Suites return every violation they find instead of panicking on the
//! first, so a test can assert that a deliberately broken connector is caught
//! for the right reason.

mod mem_sink;
mod mem_source;
mod sink_contract;
mod source_contract;

pub use mem_sink::{MemSink, MemSinkFaults};
pub use mem_source::MemSource;
pub use sink_contract::{SinkFixture, SinkProbe, check_sink};
pub use source_contract::check_source;
