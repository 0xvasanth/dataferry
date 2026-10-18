//! Write logs: a later attempt replaces an earlier one, an earlier one
//! can't overwrite a later one, and the file log survives the process.

use std::collections::BTreeMap;

use dataferry_core::FerryError;
use dataferry_registry::state::RunId;
use dataferry_verify::{
    Checksum, FileWriteLog, MemoryWriteLog, PartitionRecord, RecordCounts, TargetWrite, WriteLog,
};

fn record(partition: &str, epoch: u64, rows: u64) -> PartitionRecord {
    PartitionRecord {
        flow: "orders".into(),
        partition: partition.into(),
        epoch,
        read: rows,
        records: RecordCounts {
            written: rows,
            filtered: 0,
            rejected: 0,
        },
        targets: BTreeMap::from([(
            "orders".to_string(),
            TargetWrite {
                checksum: Checksum {
                    rows,
                    sum: u128::from(rows) << 100,
                },
                timestamp_digits: BTreeMap::from([("placed_at".to_string(), 3)]),
            },
        )]),
    }
}

async fn epochs_are_fenced(log: &dyn WriteLog) {
    let run = RunId("run-1".into());
    // Partition ids contain `/`: the flow namespaces them.
    log.record(&run, &record("orders/orders-0", 1, 10))
        .await
        .unwrap();
    // A retry at a higher epoch replaces the first attempt.
    log.record(&run, &record("orders/orders-0", 3, 12))
        .await
        .unwrap();
    // A superseded worker's record is refused, like its lease.
    let stale = log
        .record(&run, &record("orders/orders-0", 2, 99))
        .await
        .unwrap_err();
    assert_eq!(
        stale,
        FerryError::StaleLease {
            partition: "orders/orders-0".into(),
            epoch: 2,
            current: 3
        }
    );
    // The same epoch again (a repeated finish) replaces.
    log.record(&run, &record("orders/orders-0", 3, 13))
        .await
        .unwrap();
    log.record(&run, &record("orders/orders-1", 1, 5))
        .await
        .unwrap();
    log.record(&RunId("run-2".into()), &record("orders/orders-0", 9, 1))
        .await
        .unwrap();

    let kept = log.load(&run).await.unwrap();
    assert_eq!(
        kept,
        vec![
            record("orders/orders-0", 3, 13),
            record("orders/orders-1", 1, 5)
        ]
    );
    assert!(log.load(&RunId("none".into())).await.unwrap().is_empty());
}

#[tokio::test]
async fn the_memory_log_fences_epochs() {
    epochs_are_fenced(&MemoryWriteLog::new()).await;
}

#[tokio::test]
async fn the_file_log_fences_epochs_and_survives_the_process() {
    let dir = tempfile::tempdir().unwrap();
    epochs_are_fenced(&FileWriteLog::new(dir.path())).await;

    // A new process sees the same records, and the same fence.
    let reopened = FileWriteLog::new(dir.path());
    let run = RunId("run-1".into());
    assert_eq!(reopened.load(&run).await.unwrap().len(), 2);
    assert!(
        reopened
            .record(&run, &record("orders/orders-0", 1, 1))
            .await
            .is_err()
    );
    // No temporary files are left behind.
    let leftovers: Vec<_> = walk(dir.path())
        .into_iter()
        .filter(|p| p.extension().is_some_and(|x| x == "tmp"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

#[tokio::test]
async fn a_corrupt_record_is_an_error_not_a_missing_partition() {
    let dir = tempfile::tempdir().unwrap();
    let log = FileWriteLog::new(dir.path());
    let run = RunId("run-1".into());
    log.record(&run, &record("p", 1, 1)).await.unwrap();
    let file = walk(dir.path())
        .into_iter()
        .find(|p| p.extension().is_some_and(|x| x == "json"))
        .unwrap();
    std::fs::write(&file, "{ not json").unwrap();
    let err = log.load(&run).await.unwrap_err();
    assert!(err.to_string().contains("malformed record"), "{err}");
}

fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(walk(&path));
        } else {
            out.push(path);
        }
    }
    out
}
