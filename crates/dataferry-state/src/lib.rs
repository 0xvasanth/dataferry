//! The state store (A4): runs, their planned partitions, epoch-fenced
//! leases and done-records, behind [`dataferry_registry::StateStore`].
//!
//! - [`FileStateStore`] keeps one JSON file per run in a local directory,
//!   for a single process (`backend: file`).
//! - `PgStateBackend` (feature `postgres`) keeps state in tables in the
//!   state connection's database, and gives the sink a [`Ledger`] so a
//!   partition's rows and its done-record commit in one transaction
//!   (`TxnWithState`, 0004 §8).
//!
//! A run is pinned to its config hash and engine version when it is
//! created; [`resume`] refuses to continue it if either changed. Partitions
//! are planned once and stored, and resume reads them back.
//!
//! Partition ids are unique within a run, across its flows: `is_done` and
//! the ledger name a partition without its flow.
//!
//! [`Ledger`]: dataferry_registry::Ledger

mod file;
#[cfg(feature = "postgres")]
mod postgres;
mod resume;
#[cfg(feature = "testkit")]
pub mod testkit;

use dataferry_core::{Epoch, FerryError, Lease, PartitionId};
use dataferry_registry::state::RunId;

pub use file::FileStateStore;
#[cfg(feature = "postgres")]
pub use postgres::{PgLedger, PgStateBackend, PgStateStore};
pub use resume::{ResumeError, resume};

/// Checks `lease` against the partition's current epoch: a newer lease makes
/// it stale, and an epoch the store never issued is refused.
fn fence(lease: &Lease, current: Epoch, backend: &str) -> dataferry_core::Result<()> {
    match current.cmp(&lease.epoch) {
        std::cmp::Ordering::Equal => Ok(()),
        std::cmp::Ordering::Greater => Err(FerryError::StaleLease {
            partition: lease.partition.0.clone(),
            epoch: lease.epoch.0,
            current: current.0,
        }),
        std::cmp::Ordering::Less => Err(store_error(
            backend,
            format!(
                "partition `{}` was never leased at epoch {}",
                lease.partition.0, lease.epoch.0
            ),
        )),
    }
}

fn store_error(backend: &str, message: impl Into<String>) -> FerryError {
    FerryError::Connector {
        connector: format!("state:{backend}"),
        message: message.into(),
    }
}

/// A fresh run id. Random rather than sequential, so two runs never share
/// an id even across state directories or databases.
fn new_run_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

fn already_planned(backend: &str, run: &RunId, flow: &str) -> FerryError {
    store_error(
        backend,
        format!(
            "flow `{flow}` of run `{}` was already planned with different partitions; \
             partitions are planned once per run",
            run.0
        ),
    )
}

fn duplicate_id(backend: &str, flow: &str, id: &PartitionId) -> FerryError {
    store_error(
        backend,
        format!("flow `{flow}` plans partition `{}` twice", id.0),
    )
}

fn id_in_other_flow(backend: &str, flow: &str, id: &PartitionId, other: &str) -> FerryError {
    store_error(
        backend,
        format!(
            "flow `{flow}` plans partition `{}`, which flow `{other}` already has; \
             partition ids must be unique within a run",
            id.0
        ),
    )
}
