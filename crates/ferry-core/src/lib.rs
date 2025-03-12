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
#[cfg(feature = "testkit")]
pub mod testkit;
pub mod transform;
pub mod types;

pub use checkpoint::Checkpoint;
pub use entity::Entity;
pub use error::{FerryError, Result};
pub use lease::{Epoch, Lease};
pub use partition::{
    CoPartition, KeyRange, KeyValue, Partition, PartitionHint, PartitionId, PartitionScope, align,
};
pub use sink::{IdempotencyStrategy, Sink, SinkCaps, SinkTxn, WriteMode, WriteSummary};
pub use source::{BatchStream, Source, SourceCaps};
pub use target::{ForeignKey, TargetSchema, TargetTable};
pub use transform::{
    IdentityPlan, PartitionInputs, RejectedRecord, SourceRecordId, TargetBatch, TransformOutput,
    TransformPlan, TransformStream,
};
