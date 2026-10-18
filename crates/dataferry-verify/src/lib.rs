//! The verifier (A8): proves that what a run computed is what landed.
//!
//! - **Counts** come from what the run read, recorded at read time: the
//!   state store's per-partition rows (A4) and the write log's record
//!   counts. The source is never queried again, so a source that changed
//!   after the run doesn't change the result. Every driver row read must be
//!   a record that was written, filtered or dead-lettered; a split's or a
//!   merge's extra rows don't count against it.
//! - **Checksums** ([`checksum`]): an order-independent 128-bit multiset
//!   hash, the sum of row hashes modulo 2^128 (never XOR, which cancels
//!   duplicates), over normalized mapped columns. [`ChecksumObserver`]
//!   computes it at write time inside the runner and records it per
//!   partition in a [`WriteLog`]; [`Verifier`] computes it again over a
//!   read-back of the target and compares the two per table.
//! - **Constraints** on the target after the load: not-null columns,
//!   primary-key uniqueness, and foreign-key orphans, plus whatever the
//!   sink's finalize couldn't restore (A6), from the run report.
//! - **Dead letters**: the dead-letter table's rows for the run, by
//!   reason, against the state store's count (A7).
//! - **Assertions**: `expect:` entries compiled by B8 plug in as
//!   [`CompiledAssertion`]s.
//! - **The run report** ([`Report`]): `report.json` and `report.md`.
//!
//! What this proves, and what it doesn't, is spelled out in
//! [`report::PROVES`] and [`report::DOES_NOT_PROVE`].

pub mod assertion;
pub mod checksum;
mod log;
mod observer;
mod postgres;
pub mod report;
mod target;
mod verify;

use dataferry_core::FerryError;

pub use assertion::{AssertionContext, AssertionOutcome, Comparison, CompiledAssertion};
pub use checksum::{Checksum, ChecksumSpec};
pub use log::{FileWriteLog, MemoryWriteLog, PartitionRecord, RecordCounts, TargetWrite, WriteLog};
pub use observer::ChecksumObserver;
pub use postgres::PgTarget;
pub use report::{Report, RunTiming};
pub use target::{Aggregate, DeadLetters, Scalar, Target};
pub use verify::{
    AssertionCheck, Constraint, ConstraintCheck, Counts, DeadLetterCheck, FinalizeCheck,
    FlowCounts, Status, TableCheck, Verification, Verifier,
};

pub(crate) fn verifier_error(message: impl Into<String>) -> FerryError {
    FerryError::Connector {
        connector: "verifier".into(),
        message: message.into(),
    }
}
