//! The run-wide source throttles (0003 §6): open read streams and rows per
//! second, shared by every flow in the process.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

#[derive(Clone)]
pub(crate) struct Throttle {
    connections: Option<(Arc<Semaphore>, u32)>,
    rate: Option<Arc<Pacer>>,
}

impl Throttle {
    pub fn new(max_connections: Option<usize>, rows_per_sec: Option<u64>) -> Self {
        Self {
            connections: max_connections.map(|n| {
                let n = u32::try_from(n).unwrap_or(u32::MAX);
                (Arc::new(Semaphore::new(n as usize)), n)
            }),
            rate: rows_per_sec.map(|r| Arc::new(Pacer::new(r))),
        }
    }

    /// Waits until `streams` read streams may open at once: all of a
    /// partition's inputs are taken together, so two merges can't each hold
    /// half of what they need. The runner refuses flows with more inputs
    /// than the limit; should one get here, it takes the whole limit rather
    /// than wait forever.
    pub async fn connections(&self, streams: usize) -> Option<OwnedSemaphorePermit> {
        let (semaphore, limit) = self.connections.as_ref()?;
        let n = u32::try_from(streams).unwrap_or(u32::MAX).clamp(1, *limit);
        Some(
            Arc::clone(semaphore)
                .acquire_many_owned(n)
                .await
                .expect("the semaphore is never closed"),
        )
    }

    /// Waits until `rows` more rows may be read.
    pub async fn rows(&self, rows: usize) {
        if let Some(pacer) = &self.rate {
            pacer.take(rows as u64).await;
        }
    }
}

/// How far the pacer lets readers catch up after a stall: a moment when
/// every reader was busy elsewhere (say, committing) doesn't lose the rate
/// for good, but can't turn into a burst either. Over any window of length
/// `w`, reads stay within `rate * (w + CATCH_UP)`.
const CATCH_UP: Duration = Duration::from_millis(50);

/// Spaces reads out evenly: each batch gets the next `rows / rate` slice
/// of time, and the reader waits until its slice is over. The cost of a
/// batch is paid before the next read, so the read rate never runs ahead
/// by more than one batch, and an idle stretch earns at most [`CATCH_UP`].
struct Pacer {
    rows_per_sec: u64,
    next: Mutex<Option<Instant>>,
}

impl Pacer {
    fn new(rows_per_sec: u64) -> Self {
        Self {
            rows_per_sec,
            next: Mutex::new(None),
        }
    }

    async fn take(&self, rows: u64) {
        let cost = Duration::from_secs_f64(rows as f64 / self.rows_per_sec as f64);
        let until = {
            let mut next = self.next.lock().unwrap();
            let now = Instant::now();
            let start = match *next {
                None => now,
                Some(n) => n.max(now.checked_sub(CATCH_UP).unwrap_or(now)),
            };
            let until = start + cost;
            *next = Some(until);
            until
        };
        tokio::time::sleep_until(until).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn readers_share_one_rate() {
        let throttle = Throttle::new(None, Some(1000));
        let start = Instant::now();
        let a = throttle.clone();
        let b = throttle.clone();
        let reader = |t: Throttle| async move {
            for _ in 0..5 {
                t.rows(100).await;
            }
        };
        tokio::join!(reader(a), reader(b));
        // 1000 rows at 1000 rows/s, whoever read them.
        assert_eq!(start.elapsed(), Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn an_idle_stretch_earns_only_a_short_catch_up() {
        let throttle = Throttle::new(None, Some(100));
        throttle.rows(100).await;
        tokio::time::sleep(Duration::from_secs(10)).await;
        let start = Instant::now();
        throttle.rows(100).await;
        throttle.rows(100).await;
        // Two seconds of rows, less the catch-up, not 10 seconds' credit.
        assert_eq!(start.elapsed(), Duration::from_secs(2) - CATCH_UP);
    }

    #[tokio::test]
    async fn a_partition_takes_all_its_connections_at_once() {
        let throttle = Throttle::new(Some(3), None);
        let merge = throttle.connections(2).await.unwrap();
        // One left: a second two-input partition has to wait for both.
        let waiting = tokio::time::timeout(Duration::from_millis(50), throttle.connections(2));
        assert!(waiting.await.is_err());
        let single = throttle.connections(1).await.unwrap();
        drop((merge, single));
        // More inputs than the limit takes the whole limit, not forever.
        let wide = throttle.connections(5).await.unwrap();
        assert_eq!(wide.num_permits(), 3);
        assert!(Throttle::new(None, None).connections(9).await.is_none());
    }
}
