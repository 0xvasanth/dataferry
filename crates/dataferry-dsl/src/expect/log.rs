//! Source-side aggregates per partition attempt, kept until the run is
//! verified. The same rules as the verifier's `WriteLog` (A8): one record
//! per attempt, written before the commit; a later attempt (higher lease
//! epoch) replaces it; an attempt with a lower epoch than the one recorded
//! is refused with `StaleLease`. An assertion only sums the records of
//! the attempts that committed (`AssertionContext::partitions`).

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_trait::async_trait;
use dataferry_core::{FerryError, Result};
use dataferry_registry::state::RunId;
use serde::{Deserialize, Serialize};

use super::value::Accumulated;

/// One attempt at one partition: each source-side aggregate over the
/// driver records it wrote.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceRecord {
    pub flow: String,
    /// The partition id as the state store keeps it.
    pub partition: String,
    pub epoch: u64,
    /// By [`SourceTerm::key`](super::SourceTerm::key).
    pub aggregates: BTreeMap<String, Accumulated>,
}

/// Where source records are kept.
#[async_trait]
pub trait SourceLog: Send + Sync {
    /// Keeps `record` for its partition, replacing a record from an
    /// earlier or the same epoch. Fails with `StaleLease` if a record from
    /// a later epoch exists.
    async fn record(&self, run: &RunId, record: &SourceRecord) -> Result<()>;

    /// Every partition's latest record.
    async fn load(&self, run: &RunId) -> Result<Vec<SourceRecord>>;
}

fn stale(record: &SourceRecord, current: u64) -> FerryError {
    FerryError::StaleLease {
        partition: record.partition.clone(),
        epoch: record.epoch,
        current,
    }
}

pub(crate) fn expect_error(message: impl Into<String>) -> FerryError {
    FerryError::Connector {
        connector: "expect".into(),
        message: message.into(),
    }
}

/// Records in memory: for tests, and for runs verified by the process
/// that ran them from start to finish.
#[derive(Default)]
pub struct MemorySourceLog {
    records: Mutex<HashMap<(RunId, String), SourceRecord>>,
}

impl MemorySourceLog {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl SourceLog for MemorySourceLog {
    async fn record(&self, run: &RunId, record: &SourceRecord) -> Result<()> {
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

    async fn load(&self, run: &RunId) -> Result<Vec<SourceRecord>> {
        let records = self.records.lock().expect("not poisoned");
        let mut out: Vec<SourceRecord> = records
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
/// record or the new one. Survives the process, so a resumed run is
/// verified with the records of the attempts before it. One process per
/// directory: the epoch check is not atomic across processes.
pub struct FileSourceLog {
    dir: PathBuf,
    /// Serializes the read-compare-write of a record within the process.
    lock: Mutex<()>,
}

impl FileSourceLog {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            lock: Mutex::new(()),
        }
    }

    fn run_dir(&self, run: &RunId) -> PathBuf {
        self.dir.join(hex(&run.0))
    }

    fn read(path: &Path) -> Result<Option<SourceRecord>> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_error(path, e)),
        };
        serde_json::from_str(&text)
            .map(Some)
            .map_err(|e| expect_error(format!("{}: malformed record: {e}", path.display())))
    }
}

/// File names are the hex of the id: partition ids contain `/`.
fn hex(id: &str) -> String {
    id.bytes().map(|b| format!("{b:02x}")).collect()
}

fn io_error(path: &Path, e: std::io::Error) -> FerryError {
    expect_error(format!("{}: {e}", path.display()))
}

#[async_trait]
impl SourceLog for FileSourceLog {
    async fn record(&self, run: &RunId, record: &SourceRecord) -> Result<()> {
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
            .map_err(|e| expect_error(format!("serializing a record: {e}")))?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, text).map_err(|e| io_error(&tmp, e))?;
        std::fs::rename(&tmp, &path).map_err(|e| io_error(&path, e))
    }

    async fn load(&self, run: &RunId) -> Result<Vec<SourceRecord>> {
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
