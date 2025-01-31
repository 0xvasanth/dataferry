//! Core types and contracts shared by every 0xferry crate.

pub mod checkpoint;
pub mod entity;
pub mod error;
pub mod missing;
pub mod partition;
pub mod types;

pub use checkpoint::Checkpoint;
pub use entity::Entity;
pub use error::{FerryError, Result};
pub use partition::{KeyRange, KeyValue, Partition, PartitionId, PartitionScope};
