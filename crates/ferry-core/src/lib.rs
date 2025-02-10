//! Core types and contracts shared by every 0xferry crate.

pub mod checkpoint;
pub mod entity;
pub mod error;
pub mod lease;
pub mod missing;
pub mod partition;
pub mod sink;
pub mod source;
pub mod target;
pub mod types;

pub use checkpoint::Checkpoint;
pub use entity::Entity;
pub use error::{FerryError, Result};
pub use lease::{Epoch, Lease};
pub use partition::{KeyRange, KeyValue, Partition, PartitionHint, PartitionId, PartitionScope};
pub use sink::{IdempotencyStrategy, Sink, SinkCaps, SinkTxn, WriteMode, WriteSummary};
pub use source::{BatchStream, Source, SourceCaps};
pub use target::{ForeignKey, TargetSchema, TargetTable};
