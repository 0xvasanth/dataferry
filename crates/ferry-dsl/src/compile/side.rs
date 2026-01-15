//! A joined entity, sorted by its join key: what a driver batch looks its
//! joined rows up in (B6).
//!
//! Both strategies (0002 step 5) end up here:
//!
//! - **Co-partitioned**, a partition's rows of the joined entity, held in
//!   memory while the partition is processed ([`Sorted::in_memory`]).
//! - **Staged**, the whole joined entity, read once before the first
//!   partition and kept on disk ([`Sorted::stage`]): an external merge sort
//!   into Arrow IPC files under a byte budget, then a file of sorted blocks
//!   with the first and last key of each in memory, and a bounded cache of
//!   blocks. A lookup reads only the blocks whose key range holds a key it
//!   needs, so a driver read in key order (the usual `many` join on the
//!   driver's key) reads the stage about once overall. A lookup whose keys
//!   are spread over the whole stage (a `one` lookup on another column)
//!   reads every block it needs, from the cache when the stage fits there.
//!
//! Sorting, spilling and reading the stage's files block, so they run on
//! tokio's blocking pool ([`blocking`]), never on the async thread that
//! drives the partition: a stage being sorted doesn't stall the runner's
//! heartbeats or other partitions on a current-thread runtime.
//!
//! Rows are sorted by (join key, primary key), so a key's rows are
//! contiguous and in primary-key order: the order `unnest:` gives them, and
//! the last tie-breaker of a `nest:` (0003 §5.8). Keys are compared as
//! arrow-row bytes of the key columns cast to the join's key types, so a
//! composite key is one byte string and equal values compare equal from
//! either side. A row whose key has a null never matches (it is a left
//! join on equality), so it isn't kept.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow_array::{Array, ArrayRef, BinaryArray, BooleanArray, RecordBatch, UInt32Array};
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;
use arrow_row::{RowConverter, Rows, SortField};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use ferry_core::{Entity, FerryError, PartitionHint, Source};
use futures::StreamExt;

/// The key bytes of each row, in the stage's files.
const KEY_COLUMN: &str = "_ferry_stage_key";
/// The (key, primary key) bytes of each row, in the sorted runs.
const SORT_COLUMN: &str = "_ferry_stage_sort";
/// Sorted runs merged at once.
const FAN_IN: usize = 64;
/// The partition size a staged entity is read in. Staging reads every
/// partition in turn, so this only sizes the source's queries.
const STAGE_PARTITION_ROWS: u64 = 100_000;

/// The bytes an array's values take. Not `get_array_memory_size`: the
/// buffers of a batch read from an IPC file all slice one allocation, which
/// that counts again for every buffer.
fn array_bytes(array: &dyn Array) -> usize {
    array
        .to_data()
        .get_slice_memory_size()
        .unwrap_or_else(|_| array.get_array_memory_size())
}

fn batch_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|c| array_bytes(c.as_ref()))
        .sum()
}

/// Runs `f` on tokio's blocking pool. Once started it runs to the end,
/// even if the caller's future is dropped; what it returns is then
/// dropped on that thread (a dropped stage removes its files there).
pub(crate) async fn blocking<T: Send + 'static>(
    context: &str,
    f: impl FnOnce() -> ferry_core::Result<T> + Send + 'static,
) -> ferry_core::Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| error(context, e))?
}

/// Blocking pauses a test injects into the stage's file I/O, by the
/// joined entity's reference, to tell whether it runs on the async thread.
#[cfg(test)]
pub(crate) mod pause {
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::thread::ThreadId;
    use std::time::Duration;

    /// The pause, and every point reached with the thread it ran on.
    type Pauses = BTreeMap<String, (Duration, Vec<(&'static str, ThreadId)>)>;
    static PAUSES: Mutex<Pauses> = Mutex::new(BTreeMap::new());

    /// Pauses the first time each point is reached for `reference`.
    pub(crate) fn set(reference: &str, pause: Duration) {
        PAUSES
            .lock()
            .unwrap()
            .insert(reference.to_owned(), (pause, Vec::new()));
    }

    /// The points reached for `reference`, in order, each with its thread.
    pub(crate) fn hits(reference: &str) -> Vec<(&'static str, ThreadId)> {
        PAUSES
            .lock()
            .unwrap()
            .get(reference)
            .map(|(_, hits)| hits.clone())
            .unwrap_or_default()
    }

    pub(crate) fn at(reference: &str, point: &'static str) {
        let pause = PAUSES
            .lock()
            .unwrap()
            .get_mut(reference)
            .and_then(|(d, hits)| {
                let first = hits.iter().all(|(p, _)| *p != point);
                hits.push((point, std::thread::current().id()));
                first.then_some(*d)
            });
        if let Some(pause) = pause {
            std::thread::sleep(pause);
        }
    }
}

/// What fails here is mostly I/O on the stage's files (a full disk), so it
/// is reported like a connector's failure, not as a broken plan.
fn error(context: &str, e: impl std::fmt::Display) -> FerryError {
    FerryError::Connector {
        connector: "stage".into(),
        message: format!("{context}: {e}"),
    }
}

/// How a joined entity's rows are found by key.
pub(crate) struct Keyed {
    entity: Entity,
    /// What the messages call the entity: `app.public.customers`.
    reference: String,
    /// This entry's key columns, in `keys:` order.
    columns: Vec<String>,
    /// The type each key pair is compared as: the wider of the two sides.
    types: Vec<DataType>,
    keys: RowConverter,
    /// The key columns, then the primary key: the sort order.
    order: RowConverter,
}

/// The key bytes of a batch's rows: `None` for a row with a null key part.
pub(crate) struct KeyRows {
    rows: Rows,
    valid: Vec<bool>,
}

impl KeyRows {
    pub(crate) fn len(&self) -> usize {
        self.valid.len()
    }

    pub(crate) fn get(&self, row: usize) -> Option<&[u8]> {
        self.valid[row].then(|| self.rows.row(row).data())
    }
}

/// Rows with a full key, with their key and sort bytes.
struct Prepared {
    rows: RecordBatch,
    keys: BinaryArray,
    sort: BinaryArray,
}

impl Prepared {
    fn bytes(&self) -> usize {
        batch_bytes(&self.rows) + array_bytes(&self.keys) + array_bytes(&self.sort)
    }
}

impl Keyed {
    pub(crate) fn new(
        entity: Entity,
        reference: String,
        columns: Vec<String>,
        types: Vec<DataType>,
    ) -> ferry_core::Result<Self> {
        let pk_types = entity
            .primary_key()
            .iter()
            .map(|c| {
                entity
                    .field(c)
                    .map(|f| f.ty.to_arrow())
                    .ok_or_else(|| error(&reference, format!("no primary-key field `{c}`")))
            })
            .collect::<ferry_core::Result<Vec<_>>>()?;
        let keys = RowConverter::new(types.iter().cloned().map(SortField::new).collect())
            .map_err(|e| error(&reference, e))?;
        let order = RowConverter::new(
            types
                .iter()
                .chain(&pk_types)
                .cloned()
                .map(SortField::new)
                .collect(),
        )
        .map_err(|e| error(&reference, e))?;
        Ok(Self {
            entity,
            reference,
            columns,
            types,
            keys,
            order,
        })
    }

    /// Key values (one array per key pair, castable to the key types) as
    /// key bytes.
    pub(crate) fn encode(&self, columns: &[ArrayRef]) -> ferry_core::Result<KeyRows> {
        let columns = self.cast(columns)?;
        let rows = columns.first().map_or(0, |c| c.len());
        let valid = (0..rows)
            .map(|r| columns.iter().all(|c| c.is_valid(r)))
            .collect();
        let rows = self
            .keys
            .convert_columns(&columns)
            .map_err(|e| error(&self.reference, e))?;
        Ok(KeyRows { rows, valid })
    }

    fn cast(&self, columns: &[ArrayRef]) -> ferry_core::Result<Vec<ArrayRef>> {
        columns
            .iter()
            .zip(&self.types)
            .map(|(c, ty)| {
                if c.data_type() == ty {
                    return Ok(Arc::clone(c));
                }
                // A value the key type can't hold can't equal any key of
                // that type: it becomes null, which matches nothing.
                let options = arrow_cast::CastOptions {
                    safe: true,
                    ..Default::default()
                };
                arrow_cast::cast_with_options(c, ty, &options)
                    .map_err(|e| error(&self.reference, e))
            })
            .collect()
    }

    /// The rows of `batch` (laid out as the entity) that have a full key.
    fn prepare(&self, batch: &RecordBatch) -> ferry_core::Result<Prepared> {
        if batch.schema().fields() != self.entity.arrow_schema().fields() {
            return Err(error(
                &self.reference,
                "a batch doesn't have the entity's columns",
            ));
        }
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .cloned()
                .ok_or_else(|| error(&self.reference, format!("no column `{name}`")))
        };
        let keys = self.cast(
            &self
                .columns
                .iter()
                .map(|c| column(c))
                .collect::<ferry_core::Result<Vec<_>>>()?,
        )?;
        let pk = self
            .entity
            .primary_key()
            .iter()
            .map(|c| column(c))
            .collect::<ferry_core::Result<Vec<_>>>()?;
        let full: BooleanArray = (0..batch.num_rows())
            .map(|r| Some(keys.iter().all(|k| k.is_valid(r))))
            .collect();
        let (rows, keys, pk) = if full.true_count() == batch.num_rows() {
            (batch.clone(), keys, pk)
        } else {
            let filter = |a: &ArrayRef| {
                arrow_select::filter::filter(a, &full).map_err(|e| error(&self.reference, e))
            };
            (
                arrow_select::filter::filter_record_batch(batch, &full)
                    .map_err(|e| error(&self.reference, e))?,
                keys.iter().map(filter).collect::<ferry_core::Result<_>>()?,
                pk.iter().map(filter).collect::<ferry_core::Result<_>>()?,
            )
        };
        let binary = |converter: &RowConverter, columns: &[ArrayRef]| {
            converter
                .convert_columns(columns)
                .and_then(Rows::try_into_binary)
                .map_err(|e| error(&self.reference, e))
        };
        let sort = binary(&self.order, &[keys.clone(), pk].concat())?;
        let keys = binary(&self.keys, &keys)?;
        Ok(Prepared { rows, keys, sort })
    }

    /// `parts`, as one batch sorted by (key, primary key).
    fn sort(&self, parts: Vec<Prepared>) -> ferry_core::Result<Prepared> {
        let schema = self.entity.arrow_schema();
        if parts.is_empty() {
            // A partition with no rows of the joined entity.
            let empty = || BinaryArray::from(Vec::<&[u8]>::new());
            return Ok(Prepared {
                rows: RecordBatch::new_empty(schema),
                keys: empty(),
                sort: empty(),
            });
        }
        let concat_binary = |arrays: Vec<&BinaryArray>| -> ferry_core::Result<BinaryArray> {
            let refs: Vec<&dyn Array> = arrays.into_iter().map(|a| a as &dyn Array).collect();
            let all = arrow_select::concat::concat(&refs).map_err(|e| error(&self.reference, e))?;
            Ok(all
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("binary in, binary out")
                .clone())
        };
        let rows = arrow_select::concat::concat_batches(&schema, parts.iter().map(|p| &p.rows))
            .map_err(|e| error(&self.reference, e))?;
        let keys = concat_binary(parts.iter().map(|p| &p.keys).collect())?;
        let sort = concat_binary(parts.iter().map(|p| &p.sort).collect())?;
        drop(parts);
        let mut order: Vec<u32> = (0..rows.num_rows() as u32).collect();
        order.sort_by(|&a, &b| sort.value(a as usize).cmp(sort.value(b as usize)));
        let order = UInt32Array::from(order);
        let take = |a: &dyn Array| {
            arrow_select::take::take(a, &order, None).map_err(|e| error(&self.reference, e))
        };
        let binary = |a: ArrayRef| {
            a.as_any()
                .downcast_ref::<BinaryArray>()
                .expect("binary in, binary out")
                .clone()
        };
        Ok(Prepared {
            rows: arrow_select::take::take_record_batch(&rows, &order)
                .map_err(|e| error(&self.reference, e))?,
            keys: binary(take(&keys)?),
            sort: binary(take(&sort)?),
        })
    }
}

/// A joined entity's rows, sorted by key, ready for lookups.
pub(crate) struct Sorted {
    keyed: Arc<Keyed>,
    index: Vec<Meta>,
    store: Store,
}

/// The first and last key of a block.
struct Meta {
    first: Vec<u8>,
    last: Vec<u8>,
}

struct Block {
    rows: RecordBatch,
    keys: BinaryArray,
}

impl Block {
    fn bytes(&self) -> usize {
        batch_bytes(&self.rows) + array_bytes(&self.keys)
    }

    /// The rows whose key is `key`.
    fn range(&self, key: &[u8]) -> Range<usize> {
        let bound = |inclusive: bool| {
            let (mut lo, mut hi) = (0, self.keys.len());
            while lo < hi {
                let mid = (lo + hi) / 2;
                let below = match self.keys.value(mid).cmp(key) {
                    std::cmp::Ordering::Less => true,
                    std::cmp::Ordering::Equal => inclusive,
                    std::cmp::Ordering::Greater => false,
                };
                if below {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            lo
        };
        bound(false)..bound(true)
    }
}

enum Store {
    Memory(Vec<Arc<Block>>),
    Disk(Box<Disk>),
}

/// The stage's file of sorted blocks, and the blocks recently read.
struct Disk {
    reader: Mutex<FileReader<BufReader<File>>>,
    cache: Mutex<Cache>,
    /// Removed, with every file in it, when the stage is dropped.
    _dir: tempfile::TempDir,
}

/// Blocks by index, evicting the least recently used over `limit` bytes.
struct Cache {
    blocks: HashMap<usize, (Arc<Block>, u64)>,
    bytes: usize,
    limit: usize,
    tick: u64,
}

impl Cache {
    fn get(&mut self, b: usize) -> Option<Arc<Block>> {
        self.tick += 1;
        let tick = self.tick;
        self.blocks.get_mut(&b).map(|(block, used)| {
            *used = tick;
            Arc::clone(block)
        })
    }

    fn insert(&mut self, b: usize, block: Arc<Block>) {
        // Two partitions that missed the same block both read it.
        if let Some((replaced, _)) = self.blocks.remove(&b) {
            self.bytes -= replaced.bytes();
        }
        let bytes = block.bytes();
        if bytes > self.limit {
            return;
        }
        while self.bytes + bytes > self.limit {
            let Some((&oldest, _)) = self.blocks.iter().min_by_key(|(_, (_, used))| *used) else {
                break;
            };
            let (evicted, _) = self.blocks.remove(&oldest).expect("just found");
            self.bytes -= evicted.bytes();
        }
        self.tick += 1;
        self.bytes += bytes;
        self.blocks.insert(b, (block, self.tick));
    }
}

/// What a lookup found: for driver row `r`, its joined rows are
/// `rows[offsets[r]..offsets[r + 1]]`, in primary-key order.
pub(crate) struct Matched {
    pub rows: RecordBatch,
    offsets: Vec<usize>,
}

impl Matched {
    pub(crate) fn range(&self, row: usize) -> Range<usize> {
        self.offsets[row]..self.offsets[row + 1]
    }
}

/// How a stage spends its memory: a quarter to sort runs (a run is about
/// three times its buffer while it is sorted), a quarter to merge them, and
/// half for the block cache. A stage that fits in the cache budget is never
/// written to disk.
#[derive(Clone, Copy, Debug)]
struct Limits {
    run_bytes: usize,
    merge_bytes: usize,
    cache_bytes: usize,
}

impl Limits {
    fn new(memory: u64) -> Self {
        let memory = usize::try_from(memory).unwrap_or(usize::MAX);
        Self {
            run_bytes: (memory / 12).max(1),
            merge_bytes: (memory / 4).max(1),
            cache_bytes: (memory / 2).max(1),
        }
    }
}

impl Sorted {
    /// A co-partitioned input's rows for one partition.
    pub(crate) fn in_memory(
        keyed: Arc<Keyed>,
        batches: &[RecordBatch],
    ) -> ferry_core::Result<Self> {
        let parts = batches
            .iter()
            .map(|b| keyed.prepare(b))
            .collect::<ferry_core::Result<Vec<_>>>()?;
        Self::from_sorted(Arc::clone(&keyed), keyed.sort(parts)?)
    }

    fn from_sorted(keyed: Arc<Keyed>, sorted: Prepared) -> ferry_core::Result<Self> {
        let mut index = Vec::new();
        let mut blocks = Vec::new();
        if sorted.keys.len() > 0 {
            index.push(Meta {
                first: sorted.keys.value(0).to_vec(),
                last: sorted.keys.value(sorted.keys.len() - 1).to_vec(),
            });
            blocks.push(Arc::new(Block {
                rows: sorted.rows,
                keys: sorted.keys,
            }));
        }
        Ok(Self {
            keyed,
            index,
            store: Store::Memory(blocks),
        })
    }

    /// Reads every partition of the joined entity from `source` and sorts
    /// it, in `memory` bytes, spilling to a new directory under `dir`.
    /// The source is read on the caller's task; everything that sorts or
    /// touches the files runs on the blocking pool, one batch at a time.
    pub(crate) async fn stage(
        keyed: Arc<Keyed>,
        source: &dyn Source,
        memory: u64,
        dir: &Path,
    ) -> ferry_core::Result<Self> {
        let context = format!("staging `{}`", keyed.reference);
        let entity = keyed.entity.clone();
        let mut spill = {
            let (keyed, dir) = (Arc::clone(&keyed), dir.to_path_buf());
            blocking(&context, move || {
                Spill::new(keyed, Limits::new(memory), &dir)
            })
            .await?
        };
        let partitions = source
            .plan_partitions(&entity, &PartitionHint::rows(STAGE_PARTITION_ROWS))
            .await?;
        for partition in &partitions {
            let mut batches = source.read(&entity, partition).await?;
            while let Some(batch) = batches.next().await {
                let (batch, keyed, at) = (batch?, Arc::clone(&keyed), context.clone());
                spill = blocking(&context, move || {
                    let prepared = keyed.prepare(&batch)?;
                    spill.push(prepared).map_err(|e| error(&at, e))?;
                    Ok(spill)
                })
                .await?;
            }
        }
        blocking(&context, move || spill.finish()).await
    }

    /// The joined rows of each row of `keys`.
    pub(crate) fn lookup(&self, keys: &KeyRows) -> ferry_core::Result<Matched> {
        let context = || format!("looking up `{}`", self.keyed.reference);
        let n = keys.len();
        let mut wanted: Vec<(&[u8], usize)> =
            (0..n).filter_map(|r| keys.get(r).map(|k| (k, r))).collect();
        wanted.sort_unstable();
        // Each distinct key once, and each row's distinct key.
        let mut distinct: Vec<&[u8]> = Vec::new();
        let mut key_of_row: Vec<Option<usize>> = vec![None; n];
        for (key, row) in wanted {
            if distinct.last() != Some(&key) {
                distinct.push(key);
            }
            key_of_row[row] = Some(distinct.len() - 1);
        }
        // The blocks each key may be in: those whose range holds it.
        let mut per_block: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for (d, key) in distinct.iter().enumerate() {
            let start = self.index.partition_point(|m| m.last.as_slice() < *key);
            for (b, meta) in self.index.iter().enumerate().skip(start) {
                if meta.first.as_slice() > *key {
                    break;
                }
                per_block.entry(b).or_default().push(d);
            }
        }
        // One block at a time: only what matched is kept.
        let mut parts: Vec<RecordBatch> = Vec::new();
        let mut found: Vec<Vec<(usize, usize, usize)>> = vec![Vec::new(); distinct.len()];
        for (b, ds) in per_block {
            let block = self.block(b)?;
            let mut take = Vec::new();
            for d in ds {
                let range = block.range(distinct[d]);
                if !range.is_empty() {
                    found[d].push((parts.len(), take.len(), range.len()));
                    take.extend(range.map(|i| i as u32));
                }
            }
            if !take.is_empty() {
                parts.push(
                    arrow_select::take::take_record_batch(&block.rows, &UInt32Array::from(take))
                        .map_err(|e| error(&context(), e))?,
                );
            }
        }
        let schema = self.keyed.entity.arrow_schema();
        let all = arrow_select::concat::concat_batches(&schema, &parts)
            .map_err(|e| error(&context(), e))?;
        let mut bases = Vec::with_capacity(parts.len());
        let mut base = 0;
        for p in &parts {
            bases.push(base);
            base += p.num_rows();
        }
        drop(parts);
        let mut indices: Vec<u32> = Vec::new();
        let mut offsets = Vec::with_capacity(n + 1);
        offsets.push(0);
        for d in key_of_row {
            for &(p, start, len) in d.map(|d| found[d].as_slice()).unwrap_or_default() {
                let from = bases[p] + start;
                indices.extend((from..from + len).map(|i| i as u32));
            }
            offsets.push(indices.len());
        }
        let rows = arrow_select::take::take_record_batch(&all, &UInt32Array::from(indices))
            .map_err(|e| error(&context(), e))?;
        Ok(Matched { rows, offsets })
    }

    fn block(&self, b: usize) -> ferry_core::Result<Arc<Block>> {
        let disk = match &self.store {
            Store::Memory(blocks) => return Ok(Arc::clone(&blocks[b])),
            Store::Disk(disk) => disk,
        };
        if let Some(block) = disk.cache.lock().expect("cache lock").get(b) {
            return Ok(block);
        }
        let context = format!("reading the stage of `{}`", self.keyed.reference);
        #[cfg(test)]
        pause::at(&self.keyed.reference, "read");
        let batch = {
            let mut reader = disk.reader.lock().expect("reader lock");
            reader.set_index(b).map_err(|e| error(&context, e))?;
            reader
                .next()
                .ok_or_else(|| error(&context, format!("block {b} is missing")))?
                .map_err(|e| error(&context, e))?
        };
        let block = Arc::new(split_keys(batch, &context)?);
        disk.cache
            .lock()
            .expect("cache lock")
            .insert(b, Arc::clone(&block));
        Ok(block)
    }

    #[cfg(test)]
    fn blocks(&self) -> usize {
        self.index.len()
    }
}

/// A stage file's batch: the entity's columns, then the key column.
fn split_keys(batch: RecordBatch, context: &str) -> ferry_core::Result<Block> {
    let last = batch.num_columns() - 1;
    let keys = batch
        .column(last)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| error(context, "the key column isn't binary"))?
        .clone();
    let rows = batch
        .project(&(0..last).collect::<Vec<_>>())
        .map_err(|e| error(context, e))?;
    Ok(Block { rows, keys })
}

/// The external sort: sorted runs written as they fill, merged at the end.
struct Spill {
    keyed: Arc<Keyed>,
    limits: Limits,
    dir: tempfile::TempDir,
    buffer: Vec<Prepared>,
    buffered: usize,
    runs: Vec<PathBuf>,
    /// Rows and bytes seen, for sizing batches and blocks.
    rows: usize,
    bytes: usize,
    files: usize,
}

type IoResult<T> = Result<T, String>;

fn io(e: impl std::fmt::Display) -> String {
    e.to_string()
}

impl Spill {
    fn new(keyed: Arc<Keyed>, limits: Limits, dir: &Path) -> ferry_core::Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| error("creating the staging directory", e))?;
        let dir = tempfile::Builder::new()
            .prefix("ferry-stage-")
            .tempdir_in(dir)
            .map_err(|e| error("creating the staging directory", e))?;
        Ok(Self {
            keyed,
            limits,
            dir,
            buffer: Vec::new(),
            buffered: 0,
            runs: Vec::new(),
            rows: 0,
            bytes: 0,
            files: 0,
        })
    }

    fn push(&mut self, prepared: Prepared) -> IoResult<()> {
        let bytes = prepared.bytes();
        self.rows += prepared.rows.num_rows();
        self.bytes += bytes;
        self.buffered += bytes;
        self.buffer.push(prepared);
        if self.buffered >= self.limits.run_bytes {
            self.spill_run()?;
        }
        Ok(())
    }

    fn path(&mut self) -> PathBuf {
        self.files += 1;
        self.dir.path().join(format!("{}.arrow", self.files))
    }

    /// Rows per batch so `budget` bytes hold `batches` of them.
    fn rows_for(&self, budget: usize, batches: usize) -> usize {
        let row = (self.bytes / self.rows.max(1)).max(1);
        (budget / batches.max(1) / row).clamp(16, 8192)
    }

    fn file_schema(&self, sort: bool) -> SchemaRef {
        let mut fields: Vec<Field> = self
            .keyed
            .entity
            .arrow_schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new(KEY_COLUMN, DataType::Binary, false));
        if sort {
            fields.push(Field::new(SORT_COLUMN, DataType::Binary, false));
        }
        Arc::new(Schema::new(fields))
    }

    fn spill_run(&mut self) -> IoResult<()> {
        #[cfg(test)]
        pause::at(&self.keyed.reference, "spill");
        let parts = std::mem::take(&mut self.buffer);
        self.buffered = 0;
        let sorted = self.keyed.sort(parts).map_err(io)?;
        // Merging reads one batch of every run of a merge at once.
        let batch_rows = self.rows_for(self.limits.merge_bytes, 2 * FAN_IN);
        let schema = self.file_schema(true);
        let mut columns = sorted.rows.columns().to_vec();
        columns.push(Arc::new(sorted.keys));
        columns.push(Arc::new(sorted.sort));
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).map_err(io)?;
        let path = self.path();
        let mut writer =
            FileWriter::try_new(BufWriter::new(File::create(&path).map_err(io)?), &schema)
                .map_err(io)?;
        let mut offset = 0;
        while offset < batch.num_rows() {
            let len = batch_rows.min(batch.num_rows() - offset);
            writer.write(&batch.slice(offset, len)).map_err(io)?;
            offset += len;
        }
        writer.finish().map_err(io)?;
        self.runs.push(path);
        Ok(())
    }

    fn finish(mut self) -> ferry_core::Result<Sorted> {
        let context = format!("staging `{}`", self.keyed.reference);
        if self.runs.is_empty() && self.buffered <= self.limits.cache_bytes {
            let parts = std::mem::take(&mut self.buffer);
            let sorted = self.keyed.sort(parts)?;
            return Sorted::from_sorted(Arc::clone(&self.keyed), sorted);
        }
        if !self.buffer.is_empty() {
            self.spill_run().map_err(|e| error(&context, e))?;
        }
        let batch_rows = self.rows_for(self.limits.merge_bytes, 2 * FAN_IN);
        while self.runs.len() > FAN_IN {
            let runs = std::mem::take(&mut self.runs);
            for group in runs.chunks(FAN_IN) {
                let path = self.path();
                let schema = self.file_schema(true);
                merge(group, &path, &schema, batch_rows, true).map_err(|e| error(&context, e))?;
                for run in group {
                    let _ = std::fs::remove_file(run);
                }
                self.runs.push(path);
            }
        }
        #[cfg(test)]
        pause::at(&self.keyed.reference, "merge");
        // A cached block is a sixteenth of the cache.
        let block_rows = self.rows_for(self.limits.cache_bytes, 16);
        let path = self.path();
        let schema = self.file_schema(false);
        let runs = std::mem::take(&mut self.runs);
        let index =
            merge(&runs, &path, &schema, block_rows, false).map_err(|e| error(&context, e))?;
        for run in &runs {
            let _ = std::fs::remove_file(run);
        }
        let file = File::open(&path).map_err(|e| error(&context, e))?;
        let reader =
            FileReader::try_new(BufReader::new(file), None).map_err(|e| error(&context, e))?;
        Ok(Sorted {
            keyed: Arc::clone(&self.keyed),
            index,
            store: Store::Disk(Box::new(Disk {
                reader: Mutex::new(reader),
                cache: Mutex::new(Cache {
                    blocks: HashMap::new(),
                    bytes: 0,
                    limit: self.limits.cache_bytes,
                    tick: 0,
                }),
                _dir: self.dir,
            })),
        })
    }
}

/// A sorted run being merged: its current batch and position.
struct Cursor {
    reader: FileReader<BufReader<File>>,
    /// Index of its current batch in the merge's `slots`.
    slot: usize,
    sort: BinaryArray,
    row: usize,
}

impl Cursor {
    fn next_batch(&mut self, slots: &mut Vec<RecordBatch>) -> IoResult<bool> {
        loop {
            let Some(batch) = self.reader.next() else {
                return Ok(false);
            };
            let batch = batch.map_err(io)?;
            if batch.num_rows() == 0 {
                continue;
            }
            self.sort = batch
                .column_by_name(SORT_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<BinaryArray>())
                .ok_or("a run has no sort column")?
                .clone();
            self.row = 0;
            slots.push(batch);
            self.slot = slots.len() - 1;
            return Ok(true);
        }
    }
}

/// Merges sorted runs into `out`, `block_rows` rows per batch, with the
/// sort column kept (`sort`, for another merge) or dropped (the stage).
/// Returns each written batch's first and last key.
fn merge(
    runs: &[PathBuf],
    out: &Path,
    schema: &SchemaRef,
    block_rows: usize,
    sort: bool,
) -> IoResult<Vec<Meta>> {
    let mut slots: Vec<RecordBatch> = Vec::new();
    // By run; `None` once a run is done.
    let mut cursors: Vec<Option<Cursor>> = Vec::new();
    for run in runs {
        let reader =
            FileReader::try_new(BufReader::new(File::open(run).map_err(io)?), None).map_err(io)?;
        let mut cursor = Cursor {
            reader,
            slot: 0,
            sort: BinaryArray::from(Vec::<&[u8]>::new()),
            row: 0,
        };
        let live = cursor.next_batch(&mut slots)?;
        cursors.push(live.then_some(cursor));
    }
    let mut writer =
        FileWriter::try_new(BufWriter::new(File::create(out).map_err(io)?), schema).map_err(io)?;
    let mut index = Vec::new();
    let mut pending: Vec<(usize, usize)> = Vec::with_capacity(block_rows);
    let flush = |pending: &mut Vec<(usize, usize)>,
                 slots: &mut Vec<RecordBatch>,
                 cursors: &mut [Option<Cursor>],
                 writer: &mut FileWriter<BufWriter<File>>,
                 index: &mut Vec<Meta>|
     -> IoResult<()> {
        if pending.is_empty() {
            return Ok(());
        }
        let refs: Vec<&RecordBatch> = slots.iter().collect();
        let mut batch =
            arrow_select::interleave::interleave_record_batch(&refs, pending).map_err(io)?;
        pending.clear();
        if !sort {
            let keep: Vec<usize> = (0..batch.num_columns())
                .filter(|&i| batch.schema().field(i).name() != SORT_COLUMN)
                .collect();
            batch = batch.project(&keep).map_err(io)?;
        }
        let keys = batch
            .column_by_name(KEY_COLUMN)
            .and_then(|c| c.as_any().downcast_ref::<BinaryArray>())
            .ok_or("a run has no key column")?;
        index.push(Meta {
            first: keys.value(0).to_vec(),
            last: keys.value(keys.len() - 1).to_vec(),
        });
        writer.write(&batch).map_err(io)?;
        // Only the cursors' current batches are still needed.
        let mut current = Vec::with_capacity(cursors.len());
        for c in cursors.iter_mut().flatten() {
            current.push(slots[c.slot].clone());
            c.slot = current.len() - 1;
        }
        *slots = current;
        Ok(())
    };
    // The smallest current row of every run; ties go to the earlier run.
    let head = |c: &Cursor, run: usize| Reverse((c.sort.value(c.row).to_vec(), run));
    let mut heap: BinaryHeap<Reverse<(Vec<u8>, usize)>> = cursors
        .iter()
        .enumerate()
        .filter_map(|(run, c)| c.as_ref().map(|c| head(c, run)))
        .collect();
    while let Some(Reverse((_, run))) = heap.pop() {
        let cursor = cursors[run].as_mut().expect("a run in the heap is live");
        pending.push((cursor.slot, cursor.row));
        cursor.row += 1;
        if cursor.row < cursor.sort.len() || cursor.next_batch(&mut slots)? {
            heap.push(head(cursor, run));
        } else {
            // The run is done; its batches stay in `slots` until the flush.
            cursors[run] = None;
        }
        if pending.len() == block_rows {
            flush(
                &mut pending,
                &mut slots,
                &mut cursors,
                &mut writer,
                &mut index,
            )?;
        }
    }
    flush(
        &mut pending,
        &mut slots,
        &mut cursors,
        &mut writer,
        &mut index,
    )?;
    writer.finish().map_err(io)?;
    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use ferry_core::testkit::MemSource;
    use ferry_core::types::{CanonicalField, CanonicalType};

    fn entity() -> Entity {
        Entity::new(
            "items",
            vec![
                CanonicalField::new("id", CanonicalType::Int64, false),
                CanonicalField::new("order_id", CanonicalType::Int64, true),
                CanonicalField::new("sku", CanonicalType::Utf8, false),
            ],
            vec!["id".into()],
        )
        .unwrap()
    }

    fn keyed() -> Arc<Keyed> {
        Arc::new(
            Keyed::new(
                entity(),
                "shop.items".into(),
                vec!["order_id".into()],
                vec![DataType::Int64],
            )
            .unwrap(),
        )
    }

    /// `n` items, `order_id = id % groups` (null when that is 0), read in
    /// a scrambled order.
    fn items(n: i64, groups: i64) -> RecordBatch {
        let ids: Vec<i64> = (0..n).map(|i| (i * 7919) % n).collect();
        RecordBatch::try_new(
            entity().arrow_schema(),
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(Int64Array::from(
                    ids.iter()
                        .map(|i| (i % groups != 0).then_some(i % groups))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    ids.iter().map(|i| format!("sku-{i}")).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn lookup(sorted: &Sorted, keys: &[Option<i64>]) -> Vec<Vec<i64>> {
        let keys = sorted
            .keyed
            .encode(&[Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef])
            .unwrap();
        let matched = sorted.lookup(&keys).unwrap();
        let ids = matched
            .rows
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        (0..keys.len())
            .map(|r| matched.range(r).map(|i| ids.value(i)).collect())
            .collect()
    }

    /// The ids with `order_id == key`, ascending.
    fn expected(n: i64, groups: i64, key: Option<i64>) -> Vec<i64> {
        match key {
            Some(k) if k != 0 => (0..n).filter(|i| i % groups == k).collect(),
            _ => Vec::new(),
        }
    }

    async fn staged(n: i64, groups: i64, memory: u64, dir: &Path) -> Sorted {
        let source = MemSource::new()
            .with_entity(entity(), items(n, groups))
            .unwrap()
            .with_batch_rows(97);
        Sorted::stage(keyed(), &source, memory, dir).await.unwrap()
    }

    #[tokio::test]
    async fn a_spilled_stage_finds_every_key_across_blocks_and_runs() {
        let dir = tempfile::tempdir().unwrap();
        // 20 000 rows in 50 groups of 400, in 64 KiB: many runs, several
        // merge passes' worth of fan-in, and groups spanning blocks.
        let (n, groups) = (20_000, 50);
        let sorted = staged(n, groups, 64 << 10, dir.path()).await;
        assert!(matches!(sorted.store, Store::Disk(_)));
        assert!(
            sorted.blocks() > groups as usize,
            "{} blocks",
            sorted.blocks()
        );
        let keys: Vec<Option<i64>> = vec![Some(7), None, Some(0), Some(49), Some(7), Some(50)];
        let found = lookup(&sorted, &keys);
        for (key, rows) in keys.iter().zip(&found) {
            assert_eq!(rows, &expected(n, groups, *key), "key {key:?}");
        }
        // Every key, in one lookup, in reverse order.
        let keys: Vec<Option<i64>> = (0..groups).rev().map(Some).collect();
        for (key, rows) in keys.iter().zip(lookup(&sorted, &keys)) {
            assert_eq!(rows, expected(n, groups, *key), "key {key:?}");
        }
        // The cache never holds more than its share of the budget.
        let Store::Disk(disk) = &sorted.store else {
            unreachable!()
        };
        let cache = disk.cache.lock().unwrap();
        assert!(
            cache.bytes <= cache.limit,
            "{} > {}",
            cache.bytes,
            cache.limit
        );
    }

    #[tokio::test]
    async fn a_spilled_stage_that_fits_the_cache_is_read_from_disk_once() {
        let dir = tempfile::tempdir().unwrap();
        // About 1 MiB of rows: over a run's share of 4 MiB, so it spills,
        // and well under the cache's half.
        let (n, groups) = (20_000, 50);
        let sorted = staged(n, groups, 4 << 20, dir.path()).await;
        assert!(matches!(sorted.store, Store::Disk(_)));
        let keys: Vec<Option<i64>> = (0..groups).map(Some).collect();
        for _ in 0..2 {
            for (key, rows) in keys.iter().zip(lookup(&sorted, &keys)) {
                assert_eq!(rows, expected(n, groups, *key), "key {key:?}");
            }
        }
        let Store::Disk(disk) = &sorted.store else {
            unreachable!()
        };
        let cache = disk.cache.lock().unwrap();
        assert_eq!(
            cache.blocks.len(),
            sorted.blocks(),
            "every block stays cached"
        );
        assert!(
            cache.bytes <= cache.limit,
            "{} > {}",
            cache.bytes,
            cache.limit
        );
    }

    #[test]
    fn a_block_cached_twice_is_counted_once() {
        let sorted = Sorted::in_memory(keyed(), &[items(100, 10)]).unwrap();
        let Store::Memory(blocks) = &sorted.store else {
            unreachable!()
        };
        let mut cache = Cache {
            blocks: HashMap::new(),
            bytes: 0,
            limit: usize::MAX,
            tick: 0,
        };
        cache.insert(0, Arc::clone(&blocks[0]));
        cache.insert(0, Arc::clone(&blocks[0]));
        assert_eq!(cache.bytes, blocks[0].bytes());
    }

    #[tokio::test]
    async fn a_small_stage_stays_in_memory_and_the_directory_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        let sorted = staged(1000, 10, 64 << 20, dir.path()).await;
        assert!(matches!(sorted.store, Store::Memory(_)));
        let found = lookup(&sorted, &[Some(3), Some(11)]);
        assert_eq!(found, [expected(1000, 10, Some(3)), Vec::new()]);
        drop(sorted);

        let spilled = staged(20_000, 50, 64 << 10, dir.path()).await;
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
        drop(spilled);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn keys_of_a_narrower_type_are_compared_by_value() {
        let sorted = Sorted::in_memory(keyed(), &[items(100, 10)]).unwrap();
        let keys = sorted
            .keyed
            .encode(&[Arc::new(arrow_array::Int32Array::from(vec![Some(4), None])) as ArrayRef])
            .unwrap();
        let matched = sorted.lookup(&keys).unwrap();
        assert_eq!(matched.range(0).len(), 10);
        assert!(matched.range(1).is_empty());
    }
}
