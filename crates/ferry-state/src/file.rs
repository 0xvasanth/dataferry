//! `backend: file`: one JSON file per run in a local directory.
//!
//! For a single process. Every change rewrites the run's file (to a temp
//! file, then renamed over it), so a crash leaves either the old state or
//! the new one. Runs are cached in memory after their first use; the cache
//! is the source of truth while the process runs, which is why two
//! processes must not share a directory.
//!
//! The file I/O (and its fsyncs) is blocking, so every call runs on tokio's
//! blocking pool: the async thread that asked, and its heartbeats, carry on
//! meanwhile. A call runs to the end once started, even if the future that
//! asked is dropped, so the cache and the file never disagree.

use std::collections::BTreeMap;
use std::fs;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
#[cfg(feature = "testkit")]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use ferry_core::{Epoch, Lease, Partition, PartitionId, Result, WriteSummary};
use ferry_registry::StateStore;
use ferry_registry::state::{NewRun, PartitionCounts, PartitionRows, RunId, RunRecord, RunTotals};
use serde::{Deserialize, Serialize};

use crate::{already_planned, duplicate_id, fence, id_in_other_flow, new_run_id, store_error};

const BACKEND: &str = "file";

/// Bumped when the file layout changes incompatibly.
const FORMAT: u32 = 1;

pub struct FileStateStore {
    inner: Arc<Inner>,
}

/// Called after a run's new file is written and synced, before it replaces
/// the old one.
#[cfg(feature = "testkit")]
type WriteHook = Arc<dyn Fn() + Send + Sync>;

struct Inner {
    dir: PathBuf,
    runs: Mutex<BTreeMap<String, RunFile>>,
    #[cfg(feature = "testkit")]
    before_rename: Mutex<Option<WriteHook>>,
    /// Calls started and not yet finished, whether or not anyone still
    /// waits for them.
    #[cfg(feature = "testkit")]
    in_flight: AtomicUsize,
    #[cfg(feature = "testkit")]
    settled: tokio::sync::Notify,
}

/// Counts a call as in flight until it returns or panics.
#[cfg(feature = "testkit")]
struct InFlight(Arc<Inner>);

#[cfg(feature = "testkit")]
impl Drop for InFlight {
    fn drop(&mut self) {
        if self.0.in_flight.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.settled.notify_waiters();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunFile {
    format: u32,
    id: String,
    config_hash: String,
    engine_version: String,
    connections: Vec<(String, String)>,
    /// The last epoch issued to any partition of the run.
    last_epoch: u64,
    /// Every flow's partitions, in planned order.
    partitions: Vec<Entry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entry {
    flow: String,
    partition: Partition,
    /// 0 until first leased.
    epoch: u64,
    status: Status,
    read: u64,
    dead_lettered: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Status {
    Pending,
    Leased { expires_at_ms: u64 },
    Done { written: BTreeMap<String, u64> },
    Failed { error: String },
}

impl FileStateStore {
    /// No I/O: the directory is created with the first run.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            inner: Arc::new(Inner {
                dir: dir.into(),
                runs: Mutex::new(BTreeMap::new()),
                #[cfg(feature = "testkit")]
                before_rename: Mutex::new(None),
                #[cfg(feature = "testkit")]
                in_flight: AtomicUsize::new(0),
                #[cfg(feature = "testkit")]
                settled: tokio::sync::Notify::new(),
            }),
        }
    }

    /// Runs `hook` inside every write, after the new file is synced and
    /// before it replaces the old one: a test can make the filesystem slow.
    #[cfg(feature = "testkit")]
    pub fn before_rename(&self, hook: impl Fn() + Send + Sync + 'static) {
        *self.inner.before_rename.lock().unwrap() = Some(Arc::new(hook));
    }

    /// Returns once every call started so far has finished, including
    /// those whose futures were dropped: a test that drops a runner to
    /// simulate a crash waits for its last writes before reopening the
    /// directory.
    #[cfg(feature = "testkit")]
    pub async fn settled(&self) {
        loop {
            let mut settled = std::pin::pin!(self.inner.settled.notified());
            settled.as_mut().enable();
            if self.inner.in_flight.load(Ordering::SeqCst) == 0 {
                return;
            }
            settled.await;
        }
    }

    /// Runs `f` on the blocking pool. Dropping the returned future doesn't
    /// stop `f`.
    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Inner) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let inner = Arc::clone(&self.inner);
        #[cfg(feature = "testkit")]
        let in_flight = {
            inner.in_flight.fetch_add(1, Ordering::SeqCst);
            InFlight(Arc::clone(&inner))
        };
        tokio::task::spawn_blocking(move || {
            #[cfg(feature = "testkit")]
            let _in_flight = in_flight;
            f(&inner)
        })
        .await
        .map_err(|e| store_error(BACKEND, format!("state store call failed: {e}")))?
    }
}

impl Inner {
    fn path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}.json"))
    }

    /// Reads a run into the cache. `None` if no such run exists.
    fn load<'a>(
        &self,
        runs: &'a mut BTreeMap<String, RunFile>,
        run: &RunId,
    ) -> Result<Option<&'a mut RunFile>> {
        // Run ids come from the command line (`ferry resume <run-id>`), so
        // one that could leave the directory names no run.
        let valid = !run.0.is_empty()
            && run
                .0
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
        if !valid {
            return Ok(None);
        }
        if !runs.contains_key(&run.0) {
            let path = self.path(&run.0);
            let text = match fs::read_to_string(&path) {
                Ok(text) => text,
                Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
                Err(e) => {
                    return Err(store_error(
                        BACKEND,
                        format!("reading {}: {e}", path.display()),
                    ));
                }
            };
            let file: RunFile = serde_json::from_str(&text).map_err(|e| {
                store_error(
                    BACKEND,
                    format!("{} is not a run file: {e}", path.display()),
                )
            })?;
            if file.format != FORMAT || file.id != run.0 {
                return Err(store_error(
                    BACKEND,
                    format!(
                        "{} has format {} for run `{}`, expected format {FORMAT} for `{}`",
                        path.display(),
                        file.format,
                        file.id,
                        run.0
                    ),
                ));
            }
            runs.insert(run.0.clone(), file);
        }
        Ok(runs.get_mut(&run.0))
    }

    /// Reads `run` without changing it.
    fn read<T>(&self, run: &RunId, f: impl FnOnce(&RunFile) -> T) -> Result<T> {
        let mut runs = self.runs.lock().unwrap();
        let file = self.load(&mut runs, run)?.ok_or_else(|| no_run(run))?;
        Ok(f(file))
    }

    /// Applies `f` to a copy of `run` and writes it out; the cache only
    /// changes once the file did.
    fn update<T>(&self, run: &RunId, f: impl FnOnce(&mut RunFile) -> Result<T>) -> Result<T> {
        let mut runs = self.runs.lock().unwrap();
        let file = self.load(&mut runs, run)?.ok_or_else(|| no_run(run))?;
        let mut next = file.clone();
        let out = f(&mut next)?;
        self.write(&next)?;
        *file = next;
        self.sync_dir()?;
        Ok(out)
    }

    /// Updates the partition `lease` is for, if the lease is current.
    fn fenced<T>(
        &self,
        run: &RunId,
        lease: &Lease,
        f: impl FnOnce(&mut Entry) -> Result<T>,
    ) -> Result<T> {
        self.update(run, |file| {
            let entry = file
                .partitions
                .iter_mut()
                .find(|e| e.partition.id == lease.partition)
                .ok_or_else(|| unknown_partition(run, &lease.partition))?;
            fence(lease, Epoch(entry.epoch), BACKEND)?;
            f(entry)
        })
    }

    fn write(&self, file: &RunFile) -> Result<()> {
        let io = |what: &str, e: std::io::Error| store_error(BACKEND, format!("{what}: {e}"));
        fs::create_dir_all(&self.dir).map_err(|e| io(&self.dir.display().to_string(), e))?;
        let path = self.path(&file.id);
        let tmp = path.with_extension("json.tmp");
        let text = serde_json::to_vec_pretty(file).expect("a run file serializes");
        let mut out = fs::File::create(&tmp).map_err(|e| io(&tmp.display().to_string(), e))?;
        out.write_all(&text)
            .and_then(|()| out.sync_all())
            .map_err(|e| io(&tmp.display().to_string(), e))?;
        #[cfg(feature = "testkit")]
        if let Some(hook) = self.before_rename.lock().unwrap().clone() {
            hook();
        }
        fs::rename(&tmp, &path).map_err(|e| io(&path.display().to_string(), e))
    }

    /// Makes the last rename durable. It runs after the cache took the
    /// renamed file's state, so a failure here can't leave the two apart.
    fn sync_dir(&self) -> Result<()> {
        sync_dir(&self.dir)
            .map_err(|e| store_error(BACKEND, format!("syncing {}: {e}", self.dir.display())))
    }
}

/// Windows can't open a directory to sync it, and needs no such step for a
/// rename.
fn sync_dir(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    fs::File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

fn now_ms() -> u64 {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
}

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

fn count<'a>(entries: impl Iterator<Item = &'a Entry>) -> PartitionCounts {
    let now = now_ms();
    let mut counts = PartitionCounts::default();
    for e in entries {
        match e.status {
            Status::Pending => counts.pending += 1,
            Status::Leased { expires_at_ms } if expires_at_ms <= now => counts.pending += 1,
            Status::Leased { .. } => counts.leased += 1,
            Status::Done { .. } => counts.done += 1,
            Status::Failed { .. } => counts.failed += 1,
        }
    }
    counts
}

fn no_run(run: &RunId) -> ferry_core::FerryError {
    store_error(BACKEND, format!("no run `{}`", run.0))
}

fn unknown_partition(run: &RunId, partition: &PartitionId) -> ferry_core::FerryError {
    store_error(
        BACKEND,
        format!("run `{}` has no partition `{}`", run.0, partition.0),
    )
}

fn not_leased(partition: &PartitionId, status: &Status) -> ferry_core::FerryError {
    let status = match status {
        Status::Pending => "pending",
        Status::Leased { .. } => "leased",
        Status::Done { .. } => "done",
        Status::Failed { .. } => "failed",
    };
    store_error(
        BACKEND,
        format!("partition `{}` is {status}, not leased", partition.0),
    )
}

#[async_trait]
impl StateStore for FileStateStore {
    async fn create_run(&self, run: NewRun) -> Result<RunId> {
        self.blocking(move |s| {
            let file = RunFile {
                format: FORMAT,
                id: new_run_id(),
                config_hash: run.config_hash,
                engine_version: run.engine_version,
                connections: run.connections,
                last_epoch: 0,
                partitions: Vec::new(),
            };
            s.write(&file)?;
            let id = RunId(file.id.clone());
            s.runs.lock().unwrap().insert(file.id.clone(), file);
            s.sync_dir()?;
            Ok(id)
        })
        .await
    }

    async fn load_run(&self, run: &RunId) -> Result<Option<RunRecord>> {
        let run = run.clone();
        self.blocking(move |s| {
            let mut runs = s.runs.lock().unwrap();
            Ok(s.load(&mut runs, &run)?.map(|f| RunRecord {
                id: run.clone(),
                config_hash: f.config_hash.clone(),
                engine_version: f.engine_version.clone(),
                connections: f.connections.clone(),
            }))
        })
        .await
    }

    async fn store_partitions(
        &self,
        run: &RunId,
        flow: &str,
        partitions: &[Partition],
    ) -> Result<()> {
        let (run, flow, partitions) = (run.clone(), flow.to_owned(), partitions.to_vec());
        self.blocking(move |s| {
            s.update(&run, |file| {
                let planned: Vec<&Partition> = file
                    .partitions
                    .iter()
                    .filter(|e| e.flow == flow)
                    .map(|e| &e.partition)
                    .collect();
                if !planned.is_empty() {
                    return if planned.iter().copied().eq(&partitions) {
                        Ok(())
                    } else {
                        Err(already_planned(BACKEND, &run, &flow))
                    };
                }
                let mut seen = std::collections::BTreeSet::new();
                for p in &partitions {
                    if !seen.insert(&p.id) {
                        return Err(duplicate_id(BACKEND, &flow, &p.id));
                    }
                    if let Some(other) = file.partitions.iter().find(|e| e.partition.id == p.id) {
                        return Err(id_in_other_flow(BACKEND, &flow, &p.id, &other.flow));
                    }
                }
                file.partitions.extend(partitions.iter().map(|p| Entry {
                    flow: flow.clone(),
                    partition: p.clone(),
                    epoch: 0,
                    status: Status::Pending,
                    read: 0,
                    dead_lettered: 0,
                }));
                Ok(())
            })
        })
        .await
    }

    async fn load_partitions(&self, run: &RunId, flow: &str) -> Result<Vec<Partition>> {
        let (run, flow) = (run.clone(), flow.to_owned());
        self.blocking(move |s| {
            s.read(&run, |file| {
                file.partitions
                    .iter()
                    .filter(|e| e.flow == flow)
                    .map(|e| e.partition.clone())
                    .collect()
            })
        })
        .await
    }

    async fn lease_next(&self, run: &RunId, flow: &str, ttl: Duration) -> Result<Option<Lease>> {
        let (run, flow) = (run.clone(), flow.to_owned());
        self.blocking(move |s| {
            let now = now_ms();
            let available = |e: &Entry| {
                e.flow == flow
                    && match e.status {
                        Status::Pending => true,
                        Status::Leased { expires_at_ms } => expires_at_ms <= now,
                        Status::Done { .. } | Status::Failed { .. } => false,
                    }
            };
            if !s.read(&run, |file| file.partitions.iter().any(available))? {
                return Ok(None);
            }
            s.update(&run, |file| {
                let epoch = file.last_epoch + 1;
                let Some(entry) = file.partitions.iter_mut().find(|e| available(e)) else {
                    return Ok(None);
                };
                entry.epoch = epoch;
                entry.status = Status::Leased {
                    expires_at_ms: now.saturating_add(ms(ttl)),
                };
                let lease = Lease {
                    partition: entry.partition.id.clone(),
                    epoch: Epoch(epoch),
                };
                file.last_epoch = epoch;
                Ok(Some(lease))
            })
        })
        .await
    }

    async fn renew(&self, run: &RunId, lease: &Lease, ttl: Duration) -> Result<()> {
        let (run, lease) = (run.clone(), lease.clone());
        self.blocking(move |s| {
            s.fenced(&run, &lease, |entry| match entry.status {
                Status::Leased { .. } => {
                    entry.status = Status::Leased {
                        expires_at_ms: now_ms().saturating_add(ms(ttl)),
                    };
                    Ok(())
                }
                ref other => Err(not_leased(&lease.partition, other)),
            })
        })
        .await
    }

    async fn mark_done(&self, run: &RunId, lease: &Lease, summary: &WriteSummary) -> Result<()> {
        let (run, lease, written) = (run.clone(), lease.clone(), summary.rows_written.clone());
        self.blocking(move |s| {
            s.fenced(&run, &lease, |entry| match entry.status {
                Status::Leased { .. } | Status::Done { .. } => {
                    entry.status = Status::Done { written };
                    Ok(())
                }
                ref other => Err(not_leased(&lease.partition, other)),
            })
        })
        .await
    }

    async fn mark_failed(&self, run: &RunId, lease: &Lease, error: &str) -> Result<()> {
        let (run, lease, error) = (run.clone(), lease.clone(), error.to_owned());
        self.blocking(move |s| {
            s.fenced(&run, &lease, |entry| match entry.status {
                Status::Leased { .. } => {
                    entry.status = Status::Failed { error };
                    Ok(())
                }
                ref other => Err(not_leased(&lease.partition, other)),
            })
        })
        .await
    }

    async fn record_rows(&self, run: &RunId, lease: &Lease, rows: &PartitionRows) -> Result<()> {
        let (run, lease, rows) = (run.clone(), lease.clone(), *rows);
        self.blocking(move |s| {
            s.fenced(&run, &lease, |entry| match entry.status {
                Status::Leased { .. } => {
                    entry.read = rows.read;
                    entry.dead_lettered = rows.dead_lettered;
                    Ok(())
                }
                ref other => Err(not_leased(&lease.partition, other)),
            })
        })
        .await
    }

    async fn is_done(&self, run: &RunId, partition: &PartitionId) -> Result<bool> {
        let (run, partition) = (run.clone(), partition.clone());
        self.blocking(move |s| {
            s.read(&run, |file| {
                file.partitions
                    .iter()
                    .any(|e| e.partition.id == partition && matches!(e.status, Status::Done { .. }))
            })
        })
        .await
    }

    async fn counts(&self, run: &RunId) -> Result<PartitionCounts> {
        let run = run.clone();
        self.blocking(move |s| s.read(&run, |file| count(file.partitions.iter())))
            .await
    }

    async fn flow_counts(&self, run: &RunId, flow: &str) -> Result<PartitionCounts> {
        let (run, flow) = (run.clone(), flow.to_owned());
        self.blocking(move |s| {
            s.read(&run, |file| {
                count(file.partitions.iter().filter(|e| e.flow == flow))
            })
        })
        .await
    }

    async fn reset_failed(&self, run: &RunId) -> Result<u64> {
        let run = run.clone();
        self.blocking(move |s| {
            let failed = |e: &Entry| matches!(e.status, Status::Failed { .. });
            if !s.read(&run, |file| file.partitions.iter().any(failed))? {
                return Ok(0);
            }
            s.update(&run, |file| {
                let mut reset = 0;
                for entry in file.partitions.iter_mut().filter(|e| failed(e)) {
                    entry.status = Status::Pending;
                    reset += 1;
                }
                Ok(reset)
            })
        })
        .await
    }

    async fn totals(&self, run: &RunId) -> Result<RunTotals> {
        let run = run.clone();
        self.blocking(move |s| {
            s.read(&run, |file| {
                let mut totals = RunTotals::default();
                for e in &file.partitions {
                    if let Status::Done { written } = &e.status {
                        totals.read += e.read;
                        totals.dead_lettered += e.dead_lettered;
                        for (target, n) in written {
                            *totals.written.entry(target.clone()).or_default() += n;
                        }
                    }
                }
                totals
            })
        })
        .await
    }
}
