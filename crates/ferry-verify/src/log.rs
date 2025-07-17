//! What each partition wrote, recorded at write time and kept until the
//! run is verified.
//!
//! The observer records one [`PartitionRecord`] per attempt, before the
//! commit, the same way the runner records the rows a partition read in
//! the state store. A later attempt at the same partition (a re-run after
//! a crash, or `retry --failed`) holds a higher lease epoch and replaces
//! it; an attempt whose epoch is lower than the recorded one is refused
//! with `StaleLease`, like a superseded lease. The verifier sums only the
//! records of partitions the state store says are done, so an attempt that
//! recorded but never committed doesn't count.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_trait::async_trait;
use ferry_core::{FerryError, Result};
use ferry_registry::state::RunId;
use serde::{Deserialize, Serialize};

use crate::checksum::Checksum;
use crate::verifier_error;

/// One attempt at one partition, as the observer saw it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionRecord {
    pub flow: String,
    /// The partition id as the state store keeps it.
    pub partition: String,
    pub epoch: u64,
    /// Driver rows read, as the runner recorded them in the state store.
    pub read: u64,
    pub records: RecordCounts,
    /// By target table, every target of the flow, even with no rows.
    pub targets: BTreeMap<String, TargetWrite>,
}

/// What became of the driver records a partition's transform accounted
/// for. A record is written if it has a row in any target.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordCounts {
    pub written: u64,
    /// Accounted for with no rows in any target: filtered out by `where:`.
    pub filtered: u64,
    /// Rejected by the transform: dead-lettered under `on_error:
    /// dead_letter` (A7).
    pub rejected: u64,
}

impl RecordCounts {
    pub fn add(&mut self, other: &RecordCounts) {
        self.written += other.written;
        self.filtered += other.filtered;
        self.rejected += other.rejected;
    }

    pub fn total(&self) -> u64 {
        self.written + self.filtered + self.rejected
    }
}

/// The rows one partition wrote to one target table.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetWrite {
    pub checksum: Checksum,
    /// Timestamp columns hashed at fewer than 6 fractional digits, because
    /// the target keeps fewer. The read-back must be hashed the same way.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub timestamp_digits: BTreeMap<String, u8>,
}

/// Where partition records are kept.
#[async_trait]
pub trait WriteLog: Send + Sync {
    /// Keeps `record` for its partition, replacing a record from an
    /// earlier or the same epoch. Fails with `StaleLease` if a record from
    /// a later epoch exists.
    async fn record(&self, run: &RunId, record: &PartitionRecord) -> Result<()>;

    /// Every partition's latest record.
    async fn load(&self, run: &RunId) -> Result<Vec<PartitionRecord>>;
}

fn stale(record: &PartitionRecord, current: u64) -> FerryError {
    FerryError::StaleLease {
        partition: record.partition.clone(),
        epoch: record.epoch,
        current,
    }
}

/// Records in memory: for tests, and for runs that are verified by the
/// process that ran them from start to finish.
#[derive(Default)]
pub struct MemoryWriteLog {
    records: Mutex<HashMap<(RunId, String), PartitionRecord>>,
}

impl MemoryWriteLog {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl WriteLog for MemoryWriteLog {
    async fn record(&self, run: &RunId, record: &PartitionRecord) -> Result<()> {
        let mut records = self.records.lock().expect("not poisoned");
        let key = (run.clone(), record.partition.clone());
        if let Some(kept) = records.get(&key)
            && kept.epoch > record.epoch
        {
            return Err(stale(record, kept.epoch));
        }
        records.insert(key, record.clone());
        Ok(())
    }

    async fn load(&self, run: &RunId) -> Result<Vec<PartitionRecord>> {
        let records = self.records.lock().expect("not poisoned");
        let mut out: Vec<PartitionRecord> = records
            .iter()
            .filter(|((r, _), _)| r == run)
            .map(|(_, record)| record.clone())
            .collect();
        out.sort_by(|a, b| a.partition.cmp(&b.partition));
        Ok(out)
    }
}

/// One JSON file per partition under `<dir>/<run>/`, written to a
/// temporary file and renamed into place, so a crash leaves the previous
/// record or the new one, never half of one. Survives the process, so a
/// resumed run is verified with the records of every attempt that ran
/// before it. One process per directory: the epoch check is not atomic
/// across processes.
pub struct FileWriteLog {
    dir: PathBuf,
    /// Serializes the read-compare-write of a record within the process.
    lock: Mutex<()>,
}

impl FileWriteLog {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            lock: Mutex::new(()),
        }
    }

    fn run_dir(&self, run: &RunId) -> PathBuf {
        self.dir.join(hex(&run.0))
    }

    fn read(path: &Path) -> Result<Option<PartitionRecord>> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_error(path, e)),
        };
        serde_json::from_str(&text)
            .map(Some)
            .map_err(|e| verifier_error(format!("{}: malformed record: {e}", path.display())))
    }
}

/// File names are the hex of the id: partition ids contain `/`.
fn hex(id: &str) -> String {
    id.bytes().map(|b| format!("{b:02x}")).collect()
}

fn io_error(path: &Path, e: std::io::Error) -> FerryError {
    verifier_error(format!("{}: {e}", path.display()))
}

#[async_trait]
impl WriteLog for FileWriteLog {
    async fn record(&self, run: &RunId, record: &PartitionRecord) -> Result<()> {
        let _guard = self.lock.lock().expect("not poisoned");
        let dir = self.run_dir(run);
        std::fs::create_dir_all(&dir).map_err(|e| io_error(&dir, e))?;
        let path = dir.join(format!("{}.json", hex(&record.partition)));
        if let Some(kept) = Self::read(&path)?
            && kept.epoch > record.epoch
        {
            return Err(stale(record, kept.epoch));
        }
        let text = serde_json::to_string_pretty(record)
            .map_err(|e| verifier_error(format!("serializing a record: {e}")))?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, text).map_err(|e| io_error(&tmp, e))?;
        std::fs::rename(&tmp, &path).map_err(|e| io_error(&path, e))
    }

    async fn load(&self, run: &RunId) -> Result<Vec<PartitionRecord>> {
        let dir = self.run_dir(run);
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(io_error(&dir, e)),
        };
        let mut out = Vec::new();
        for entry in entries {
            let path = entry.map_err(|e| io_error(&dir, e))?.path();
            if path.extension().is_some_and(|x| x == "json")
                && let Some(record) = Self::read(&path)?
            {
                out.push(record);
            }
        }
        out.sort_by(|a, b| a.partition.cmp(&b.partition));
        Ok(out)
    }
}
