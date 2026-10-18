//! The contract every source connector implements.

use arrow_array::RecordBatch;
use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::entity::Entity;
use crate::error::Result;
use crate::partition::{Partition, PartitionHint};

pub type BatchStream = BoxStream<'static, Result<RecordBatch>>;

/// What a source can do, checked after connecting because it can depend on
/// the server (version, replica set, permissions).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SourceCaps {
    /// Can read a key range efficiently (an index on the partition key).
    pub range_scan: bool,
    /// Can evaluate filters server-side.
    pub predicate_pushdown: bool,
    /// Can read every partition from one consistent snapshot.
    pub consistent_snapshot: bool,
}

#[async_trait]
pub trait Source: Send + Sync {
    /// Entities with their schema and keys. For schemaless sources the schema
    /// is inferred; rows that don't fit it surface at read time as errors.
    async fn discover(&self) -> Result<Vec<Entity>>;

    /// Splits an entity into partitions that together cover every row exactly
    /// once. Called once per run; the result is stored, never re-planned.
    async fn plan_partitions(
        &self,
        entity: &Entity,
        hint: &PartitionHint,
    ) -> Result<Vec<Partition>>;

    /// Streams the rows of one partition as batches matching
    /// [`Entity::arrow_schema`]. Every source must also accept
    /// [`PartitionScope::Whole`](crate::partition::PartitionScope::Whole).
    async fn read(&self, entity: &Entity, partition: &Partition) -> Result<BatchStream>;

    async fn capabilities(&self) -> Result<SourceCaps>;
}
