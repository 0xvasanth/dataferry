use serde::{Deserialize, Serialize};

use crate::partition::PartitionId;

/// Fencing token for a partition lease. The state store issues a strictly
/// higher epoch every time a partition is (re-)leased, so a worker whose lease
/// expired holds a lower epoch than the current owner and its commit can be
/// rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Epoch(pub u64);

/// Permission to process one partition, valid until it expires or another
/// worker is given a higher epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub partition: PartitionId,
    pub epoch: Epoch,
}
