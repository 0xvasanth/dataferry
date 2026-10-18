//! How `memory_budget` is spent, and the byte-bounded channel each input
//! stream reads ahead through.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use dataferry_core::Result;
use futures::Stream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

/// Bytes of row data `batch` refers to. Unlike
/// `RecordBatch::get_array_memory_size`, a slice counts only its own rows,
/// not the whole buffers it shares with the batch it was cut from (sources
/// often hand out slices of one big read).
pub(crate) fn batch_size(batch: &RecordBatch) -> u64 {
    batch
        .columns()
        .iter()
        .map(|c| {
            c.to_data()
                .get_slice_memory_size()
                .unwrap_or_else(|_| c.get_array_memory_size()) as u64
        })
        .sum()
}

/// Accounting unit for read-ahead permits, so a budget of terabytes still
/// fits the semaphore's `u32` acquisitions.
const UNIT: u64 = 1024;

/// The smallest write batch the runner aims for, so a tiny budget doesn't
/// turn every row into its own write.
const MIN_BATCH_BYTES: u64 = 64 * 1024;

/// How `memory_budget` is split. A quarter goes to read-ahead, shared
/// equally by every input stream of every worker; a stream's share covers
/// the batches queued for the transform and the one it is working on. A
/// quarter goes to write batches: each worker coalesces transformed rows
/// up to [`batch_bytes`] per write, and holds at most two of those (the
/// buffer and the batch cut from it). The other half is headroom for what
/// the runner doesn't count batch by batch: the batch a reader has in hand
/// while it waits for room, what a plan allocates per row (record ids,
/// lineage), the sink's own buffers, and the runtime.
///
/// A source batch is never split on read, so a source whose batches are
/// bigger than a stream's share still flows, one batch at a time, and can
/// take the process past the budget by that batch.
///
/// [`batch_bytes`]: Budget::batch_bytes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    total: u64,
    workers: u64,
}

impl Budget {
    pub fn new(memory_budget: u64, workers: usize) -> Self {
        Self {
            total: memory_budget,
            workers: workers.max(1) as u64,
        }
    }

    /// Target bytes per `SinkTxn::write`.
    pub fn batch_bytes(&self) -> u64 {
        (self.total / (8 * self.workers)).max(MIN_BATCH_BYTES)
    }

    /// Read-ahead bytes for one of the `inputs` streams of a partition.
    pub fn read_ahead(&self, inputs: usize) -> u64 {
        self.total / 4 / (self.workers * inputs.max(1) as u64)
    }
}

/// A batch read ahead, holding its share of the stream's read-ahead until
/// the consumer moves past it.
pub(crate) struct Ahead {
    pub batch: RecordBatch,
    permit: OwnedSemaphorePermit,
}

/// The sending half of one input stream's read-ahead: at most `bytes` of
/// batches are queued at once, but a lone batch bigger than that is let
/// through on its own.
pub(crate) struct AheadSender {
    tx: mpsc::Sender<Result<Ahead>>,
    permits: Arc<Semaphore>,
    capacity: u32,
}

pub(crate) fn read_ahead(bytes: u64) -> (AheadSender, AheadStream) {
    let capacity = u32::try_from((bytes / UNIT).max(1)).unwrap_or(u32::MAX);
    let permits = Arc::new(Semaphore::new(capacity as usize));
    // The byte limit is the real bound; the slot count only has to be large
    // enough never to bind before it, and small batches add up slowly.
    let (tx, rx) = mpsc::channel(1024);
    (
        AheadSender {
            tx,
            permits,
            capacity,
        },
        AheadStream { rx, current: None },
    )
}

impl AheadSender {
    /// Waits for room, then queues `batch`. `false` if the consumer is gone.
    pub async fn send(&self, batch: RecordBatch) -> bool {
        let units = batch_size(&batch).div_ceil(UNIT);
        let units = u32::try_from(units)
            .unwrap_or(u32::MAX)
            .clamp(1, self.capacity);
        let Ok(permit) = Arc::clone(&self.permits).acquire_many_owned(units).await else {
            return false;
        };
        self.tx.send(Ok(Ahead { batch, permit })).await.is_ok()
    }

    pub async fn fail(&self, error: dataferry_core::FerryError) {
        let _ = self.tx.send(Err(error)).await;
    }
}

/// The consuming half, as the stream a `TransformPlan` reads. The batch it
/// last yielded keeps its permit until the next poll, so the batch being
/// transformed and written still counts against the read-ahead.
pub(crate) struct AheadStream {
    rx: mpsc::Receiver<Result<Ahead>>,
    current: Option<OwnedSemaphorePermit>,
}

impl Stream for AheadStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.current = None;
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(Ok(ahead))) => {
                self.current = Some(ahead.permit);
                Poll::Ready(Some(Ok(ahead.batch)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_budget_splits_into_read_ahead_and_write_batches() {
        let budget = Budget::new(64 << 20, 4);
        assert_eq!(budget.batch_bytes(), 2 << 20);
        assert_eq!(budget.read_ahead(1), 4 << 20);
        assert_eq!(budget.read_ahead(2), 2 << 20);
        // Every stream's read-ahead plus every worker's two write batches
        // stay within half the budget.
        let workers = 4;
        let used = workers * budget.read_ahead(1) + workers * 2 * budget.batch_bytes();
        assert_eq!(used, (64 << 20) / 2);
        // A tiny budget still writes batches of a useful size.
        assert_eq!(Budget::new(1 << 20, 64).batch_bytes(), MIN_BATCH_BYTES);
    }

    #[test]
    fn a_slice_counts_only_its_own_rows() {
        let id: arrow_array::ArrayRef =
            Arc::new(arrow_array::Int64Array::from_iter_values(0..1000));
        let batch = RecordBatch::try_from_iter([("id", id)]).unwrap();
        assert_eq!(batch_size(&batch), 8000);
        assert_eq!(batch_size(&batch.slice(100, 10)), 80);
    }
}
