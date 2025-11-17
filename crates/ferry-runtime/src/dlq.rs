//! The dead-letter queue (A7): rejected source records, written to
//! [`DLQ_TABLE`] on the flow's sink through the partition's own
//! [`SinkTxn`], so dead letters commit or roll back with the partition's
//! rows.
//!
//! The table's key is the flow and the record's id. A partition that runs
//! again (its done-record was lost, it was retried, or a later run loads
//! the same source) replaces its dead letters under every idempotency
//! strategy that replaces rows by key, exactly as it replaces its target
//! rows, so a record is dead-lettered once however often it is read.
//!
//! The raw record is the driver row the source read, as JSON. Under
//! `on_error: dead_letter` the runner keeps each driver batch until every
//! record in it is accounted for by a transform output, which is what
//! [`Tap`] does.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use ferry_core::types::{CanonicalField, CanonicalType};
use ferry_core::{FerryError, KeyValue, Result, SinkTxn, SourceRecordId, TargetTable};
use ferry_registry::rejected::RejectedRow;

/// The dead-letter table, created on each dead-lettering flow's sink.
pub const DLQ_TABLE: &str = "_ferry_dlq";

/// Dead letters are written once this many have built up, and at the end
/// of the partition.
const FLUSH_ROWS: usize = 1024;

/// The dead-letter table: one row per rejected source record.
///
/// - `flow`, `source_id`: the key. `source_id` is the record's primary-key
///   values as a JSON array (`["650000000000000000000006"]`, `[7]`).
/// - `run`, `partition`: the run and the (stored) partition that rejected
///   it last.
/// - `target`: the target whose row failed, if the failure was one
///   target's; `error`: why.
/// - `record`: the record as the source read it, as JSON.
pub fn dlq_table() -> TargetTable {
    let text = |name: &str, nullable| CanonicalField::new(name, CanonicalType::Utf8, nullable);
    TargetTable {
        name: DLQ_TABLE.into(),
        fields: vec![
            text("flow", false),
            text("source_id", false),
            text("run", false),
            text("partition", false),
            text("target", true),
            text("error", false),
            CanonicalField::new("record", CanonicalType::Json, true),
        ],
        primary_key: vec!["flow".into(), "source_id".into()],
        foreign_keys: Vec::new(),
    }
}

/// A record id as a JSON array of its key values. Bytes (an ObjectId read
/// as binary) are lowercase hex.
pub(crate) fn source_id_json(id: &SourceRecordId) -> String {
    let values: Vec<serde_json::Value> =
        id.0.iter()
            .map(|v| match v {
                KeyValue::Int(i) => serde_json::Value::from(*i),
                KeyValue::Str(s) => serde_json::Value::from(s.as_str()),
                KeyValue::Bytes(b) => serde_json::Value::from(
                    b.iter().map(|b| format!("{b:02x}")).collect::<String>(),
                ),
            })
            .collect();
    serde_json::Value::Array(values).to_string()
}

/// One row of `batch` as a JSON object, nulls included.
fn row_json(batch: &RecordBatch, row: usize) -> std::result::Result<String, String> {
    let mut writer = arrow_json::WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, arrow_json::writer::JsonArray>(Vec::new());
    writer
        .write(&batch.slice(row, 1))
        .and_then(|()| writer.finish())
        .map_err(|e| e.to_string())?;
    let rows: serde_json::Value =
        serde_json::from_slice(&writer.into_inner()).map_err(|e| e.to_string())?;
    rows.as_array()
        .and_then(|rows| rows.first())
        .map(ToString::to_string)
        .ok_or_else(|| "no row".to_owned())
}

/// The driver batches a partition read whose records aren't all accounted
/// for yet, and the records the source itself rejected.
#[derive(Default)]
pub(crate) struct Tap {
    batches: VecDeque<Buffered>,
    rejected: Vec<RejectedRow>,
}

struct Buffered {
    batch: RecordBatch,
    /// The row of each record not yet accounted for.
    open: HashMap<SourceRecordId, usize>,
}

impl Tap {
    pub fn buffer(&mut self, batch: RecordBatch, ids: Vec<SourceRecordId>) {
        if ids.is_empty() {
            return;
        }
        let open = ids
            .into_iter()
            .enumerate()
            .map(|(row, id)| (id, row))
            .collect();
        self.batches.push_back(Buffered { batch, open });
    }

    pub fn reject(&mut self, rows: Vec<RejectedRow>) {
        self.rejected.extend(rows);
    }

    pub fn take_rejected(&mut self) -> Vec<RejectedRow> {
        std::mem::take(&mut self.rejected)
    }

    /// The record as JSON, or why there is none: its batch isn't held (a
    /// plan that reported a record twice), or it couldn't be rendered.
    pub fn record(&self, id: &SourceRecordId) -> std::result::Result<String, String> {
        let (b, row) = self
            .batches
            .iter()
            .find_map(|b| b.open.get(id).map(|&row| (b, row)))
            .ok_or_else(|| "the record was no longer held".to_owned())?;
        row_json(&b.batch, row)
    }

    /// Lets go of `records`, and of every batch with none left.
    pub fn account(&mut self, records: &[SourceRecordId]) {
        for id in records {
            if let Some(b) = self.batches.iter_mut().find(|b| b.open.contains_key(id)) {
                b.open.remove(id);
            }
        }
        self.batches.retain(|b| !b.open.is_empty());
    }
}

/// One partition's dead letters, written in the partition's transaction.
pub(crate) struct DeadLetters {
    flow: String,
    run: String,
    partition: String,
    rows: Vec<Letter>,
}

struct Letter {
    source_id: String,
    target: Option<String>,
    error: String,
    record: Option<String>,
}

impl DeadLetters {
    pub fn new(flow: &str, run: &str, partition: &str) -> Self {
        Self {
            flow: flow.to_owned(),
            run: run.to_owned(),
            partition: partition.to_owned(),
            rows: Vec::new(),
        }
    }

    pub fn push(
        &mut self,
        id: &SourceRecordId,
        target: Option<&str>,
        error: &str,
        record: Option<String>,
    ) {
        self.rows.push(Letter {
            source_id: source_id_json(id),
            target: target.map(str::to_owned),
            error: error.to_owned(),
            record,
        });
    }

    /// Writes what has built up, once there is enough of it or `all`.
    pub async fn flush(&mut self, txn: &mut dyn SinkTxn, all: bool) -> Result<()> {
        if self.rows.is_empty() || (!all && self.rows.len() < FLUSH_ROWS) {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.rows);
        let n = rows.len();
        let constant = |v: &str| -> ArrayRef { Arc::new(StringArray::from(vec![v; n])) };
        let column = |f: fn(&Letter) -> Option<&str>| -> ArrayRef {
            Arc::new(StringArray::from(rows.iter().map(f).collect::<Vec<_>>()))
        };
        let table = dlq_table();
        let schema = Arc::new(arrow_schema_of(&table));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                constant(&self.flow),
                column(|l| Some(&l.source_id)),
                constant(&self.run),
                constant(&self.partition),
                column(|l| l.target.as_deref()),
                column(|l| Some(&l.error)),
                column(|l| l.record.as_deref()),
            ],
        )
        .map_err(|e| FerryError::Connector {
            connector: "runner".into(),
            message: format!("building dead letters: {e}"),
        })?;
        txn.write(DLQ_TABLE, batch).await
    }
}

fn arrow_schema_of(table: &TargetTable) -> arrow_schema::Schema {
    arrow_schema::Schema::new(
        table
            .fields
            .iter()
            .map(CanonicalField::to_arrow)
            .collect::<Vec<_>>(),
    )
}

#[cfg(test)]
mod tests {
    use arrow_array::Int64Array;

    use super::*;

    fn id(i: i64) -> SourceRecordId {
        SourceRecordId(vec![KeyValue::Int(i)])
    }

    #[test]
    fn source_ids_are_json_arrays_of_their_key_values() {
        let composite = SourceRecordId(vec![
            KeyValue::Str("o-1".into()),
            KeyValue::Int(-3),
            KeyValue::Bytes(vec![0x65, 0x00, 0xff]),
        ]);
        assert_eq!(source_id_json(&composite), r#"["o-1",-3,"6500ff"]"#);
    }

    #[test]
    fn the_tap_keeps_a_batch_until_every_record_in_it_is_accounted_for() {
        let batch = |ids: Vec<i64>| {
            let id: ArrayRef = Arc::new(Int64Array::from(ids.clone()));
            let qty: ArrayRef = Arc::new(StringArray::from(
                ids.iter()
                    .map(|i| (i % 2 == 0).then(|| format!("{i}")))
                    .collect::<Vec<_>>(),
            ));
            RecordBatch::try_from_iter([("id", id), ("qty", qty)]).unwrap()
        };
        let mut tap = Tap::default();
        tap.buffer(batch(vec![1, 2]), vec![id(1), id(2)]);
        tap.buffer(batch(vec![3, 4]), vec![id(3), id(4)]);
        // Nulls are kept, so a missing value reads as null, not absent.
        assert_eq!(tap.record(&id(3)).unwrap(), r#"{"id":3,"qty":null}"#);
        assert_eq!(tap.record(&id(2)).unwrap(), r#"{"id":2,"qty":"2"}"#);

        // Accounting across batches frees only the batch that's done.
        tap.account(&[id(1), id(2), id(3)]);
        assert_eq!(tap.batches.len(), 1);
        assert!(tap.record(&id(1)).is_err());
        assert_eq!(tap.record(&id(4)).unwrap(), r#"{"id":4,"qty":"4"}"#);
        tap.account(&[id(4)]);
        assert!(tap.batches.is_empty());
    }

    #[test]
    fn records_with_a_named_time_zone_render() {
        // The Mongo source's timestamps are in "UTC", by name.
        let at: ArrayRef = Arc::new(
            arrow_array::TimestampMicrosecondArray::from(vec![1_772_791_200_000_000])
                .with_timezone("UTC"),
        );
        let batch = RecordBatch::try_from_iter([("created_at", at)]).unwrap();
        assert_eq!(
            row_json(&batch, 0).unwrap(),
            r#"{"created_at":"2026-03-06T10:00:00Z"}"#
        );
    }

    #[test]
    fn the_dead_letter_table_is_keyed_by_flow_and_record() {
        let t = dlq_table();
        assert_eq!(t.primary_key, vec!["flow", "source_id"]);
        // A valid target table on its own.
        ferry_core::TargetSchema::new(vec![t]).unwrap();
    }
}
