//! Verifying a run after it finished: counts from state, write-time
//! checksums against a read-back of the target, constraints and
//! assertions.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::SystemTime;

use ferry_core::{ForeignKey, Result, TargetSchema, TargetTable};
use ferry_registry::state::{PartitionCounts, RunId};
use ferry_registry::{FinalizeProblem, StateStore};
use ferry_runtime::{Flow, RunReport};
use futures::StreamExt;
use serde::Serialize;

use crate::assertion::{AssertionContext, AssertionOutcome, CompiledAssertion};
use crate::checksum::{Checksum, ChecksumSpec};
use crate::log::{PartitionRecord, RecordCounts, WriteLog};
use crate::observer::table_specs;
use crate::target::{DeadLetters, Target};
use crate::verifier_error;

/// Verifies runs of a set of flows against one target.
///
/// Nothing here reads the source: counts come from the state store and
/// the write log, which the runner filled at read and write time.
pub struct Verifier {
    store: Arc<dyn StateStore>,
    log: Arc<dyn WriteLog>,
    target: Arc<dyn Target>,
    flows: Vec<(String, TargetSchema)>,
    assertions: Vec<(String, Arc<dyn CompiledAssertion>)>,
    finalized: Vec<(String, FinalizeProblem)>,
}

/// The overall outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// Every check passed on a complete run.
    Passed,
    /// A check failed, or couldn't be evaluated.
    Failed,
    /// The run has partitions that aren't done (or flows never planned),
    /// so the target can't match yet. Checks were still run and reported.
    Incomplete,
}

/// Everything verification found, in the shape the report prints.
#[derive(Debug, Clone, Serialize)]
pub struct Verification {
    pub status: Status,
    pub run: RunInfo,
    /// `Target::describe`.
    pub target: String,
    pub partitions: Partitions,
    pub counts: Counts,
    pub tables: Vec<TableCheck>,
    pub constraints: Vec<ConstraintCheck>,
    pub assertions: Vec<AssertionCheck>,
    /// What the sinks' finalize couldn't restore (A6). Each one fails
    /// verification.
    pub finalize: Vec<FinalizeCheck>,
    pub dead_letters: DeadLetterCheck,
    /// Every failed check, as one line each.
    pub problems: Vec<String>,
    #[serde(skip)]
    pub started: SystemTime,
    #[serde(skip)]
    pub finished: SystemTime,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunInfo {
    pub id: String,
    pub config_hash: String,
    pub engine_version: String,
    /// `(name, type)` of every connection, never its fields.
    pub connections: Vec<Connection>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Connection {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: String,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Partitions {
    pub pending: u64,
    pub leased: u64,
    pub done: u64,
    pub failed: u64,
}

impl From<PartitionCounts> for Partitions {
    fn from(c: PartitionCounts) -> Self {
        Self {
            pending: c.pending,
            leased: c.leased,
            done: c.done,
            failed: c.failed,
        }
    }
}

/// Row counts over the run's done partitions, as recorded at read time.
#[derive(Debug, Clone, Serialize)]
pub struct Counts {
    /// Driver rows read, from the state store.
    pub read: u64,
    /// From the state store.
    pub dead_lettered: u64,
    /// Rows the sink reported written, per table, from the state store.
    pub written: BTreeMap<String, u64>,
    pub flows: Vec<FlowCounts>,
}

/// One flow's driver rows and what became of them. A split's or unnest's
/// many rows per record and a merge's joined rows don't change these:
/// they count driver records.
#[derive(Debug, Clone, Serialize)]
pub struct FlowCounts {
    pub flow: String,
    pub read: u64,
    pub records: RecordCounts,
    /// `read` minus every record accounted for: must be 0.
    pub unaccounted: i128,
}

/// One target table: rows counted three ways, and the checksum computed at
/// write time against the one computed over the read-back.
#[derive(Debug, Clone, Serialize)]
pub struct TableCheck {
    pub table: String,
    pub flow: String,
    /// From the state store: what the sink reported committing.
    pub written: u64,
    pub at_write: Checksum,
    pub read_back: Option<Checksum>,
    /// Timestamp columns compared at fewer than 6 fractional digits.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub timestamp_digits: BTreeMap<String, u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub passed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Constraint {
    NotNull {
        column: String,
    },
    Unique {
        columns: Vec<String>,
    },
    ForeignKey {
        columns: Vec<String>,
        references_table: String,
        references_columns: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct ConstraintCheck {
    pub table: String,
    #[serde(flatten)]
    pub constraint: Constraint,
    /// Null rows, duplicated keys, or orphaned rows.
    pub violations: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub passed: bool,
}

/// An index or constraint finalize couldn't restore as it was, such as a
/// foreign key the loaded rows break.
#[derive(Debug, Clone, Serialize)]
pub struct FinalizeCheck {
    pub flow: String,
    pub table: String,
    /// The index or constraint.
    pub object: String,
    pub message: String,
    /// Rows that reference no parent, for a broken foreign key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub orphans: Option<u64>,
    /// The keys of some of them.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub orphan_keys: Vec<String>,
}

/// The run's dead letters as the dead-letter table holds them, by reason.
#[derive(Debug, Clone, Default, Serialize)]
pub struct DeadLetterCheck {
    /// Rows for the run in the table.
    pub rows: u64,
    pub reasons: Vec<DeadLetters>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// The table holds exactly the state store's count.
    pub passed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct AssertionCheck {
    pub flow: String,
    pub name: String,
    pub expression: String,
    pub outcome: Option<AssertionOutcome>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub passed: bool,
}

impl Verifier {
    /// The target tables and names of `flows`, as the runner ran them.
    pub fn new(
        store: Arc<dyn StateStore>,
        log: Arc<dyn WriteLog>,
        target: Arc<dyn Target>,
        flows: &[Flow],
    ) -> Self {
        Self {
            store,
            log,
            target,
            flows: flows
                .iter()
                .map(|f| (f.name.clone(), f.target.clone()))
                .collect(),
            assertions: Vec::new(),
            finalized: Vec::new(),
        }
    }

    /// Fails verification on what the run's finalize couldn't restore
    /// (A6): a foreign key the loaded rows break, an index that couldn't
    /// be recreated. The verifier's own constraint checks cover the keys a
    /// target schema declares; this covers every key the sink dropped.
    pub fn with_finalized(mut self, report: &RunReport) -> Self {
        self.finalized = report
            .finalize_problems()
            .map(|(flow, p)| (flow.to_owned(), p.clone()))
            .collect();
        self
    }

    /// Evaluates `assertion`, one of `flow`'s `expect:` entries.
    pub fn with_assertion(mut self, flow: &str, assertion: Arc<dyn CompiledAssertion>) -> Self {
        self.assertions.push((flow.to_owned(), assertion));
        self
    }

    /// Verifies `run`. An `Err` means the state store or write log
    /// couldn't be read; a failing or unevaluable check is in the result.
    pub async fn verify(&self, run: &RunId) -> Result<Verification> {
        let started = SystemTime::now();
        let record = self
            .store
            .load_run(run)
            .await?
            .ok_or_else(|| verifier_error(format!("no run `{}` in the state store", run.0)))?;
        let partitions = self.store.counts(run).await?;
        let totals = self.store.totals(run).await?;
        let mut problems = Vec::new();
        let mut complete =
            partitions.pending == 0 && partitions.leased == 0 && partitions.failed == 0;

        // The latest record of every done partition, by flow.
        let mut records: BTreeMap<String, PartitionRecord> = self
            .log
            .load(run)
            .await?
            .into_iter()
            .map(|r| (r.partition.clone(), r))
            .collect();
        let mut done: BTreeMap<&str, Vec<PartitionRecord>> = BTreeMap::new();
        for (flow, _) in &self.flows {
            let planned = self.store.load_partitions(run, flow).await?;
            if planned.is_empty() {
                complete = false;
                problems.push(format!("flow `{flow}` was never planned"));
            }
            let mut missing = 0;
            for p in planned {
                if !self.store.is_done(run, &p.id).await? {
                    continue;
                }
                match records.remove(&p.id.0) {
                    Some(r) if r.flow == *flow => done.entry(flow).or_default().push(r),
                    _ => missing += 1,
                }
            }
            if missing > 0 {
                problems.push(format!(
                    "flow `{flow}`: {missing} done partitions have no write-time record, \
                     so their rows can't be checked"
                ));
            }
        }

        let counts = self.counts(&totals, &done, &mut problems);
        let tables = self.tables(&totals, &done, &mut problems).await;
        let constraints = self.constraints(&mut problems).await;
        let assertions = self.assertions(run, &done, &mut problems).await;
        let finalize = self.finalize(&mut problems);
        let dead_letters = self.dead_letters(run, &totals, &mut problems).await;

        let status = if !complete {
            Status::Incomplete
        } else if problems.is_empty() {
            Status::Passed
        } else {
            Status::Failed
        };
        Ok(Verification {
            status,
            run: RunInfo {
                id: record.id.0,
                config_hash: record.config_hash,
                engine_version: record.engine_version,
                connections: record
                    .connections
                    .into_iter()
                    .map(|(name, kind)| Connection { name, kind })
                    .collect(),
            },
            target: self.target.describe(),
            partitions: partitions.into(),
            counts,
            tables,
            constraints,
            assertions,
            finalize,
            dead_letters,
            problems,
            started,
            finished: SystemTime::now(),
        })
    }

    /// Read-time accounting: every driver row read is a record written,
    /// filtered or dead-lettered, and the write log agrees with the state
    /// store.
    fn counts(
        &self,
        totals: &ferry_registry::state::RunTotals,
        done: &BTreeMap<&str, Vec<PartitionRecord>>,
        problems: &mut Vec<String>,
    ) -> Counts {
        let mut flows = Vec::new();
        let (mut read, mut rejected) = (0, 0);
        for (flow, _) in &self.flows {
            let mut fc = FlowCounts {
                flow: flow.clone(),
                read: 0,
                records: RecordCounts::default(),
                unaccounted: 0,
            };
            for r in done.get(flow.as_str()).into_iter().flatten() {
                fc.read += r.read;
                fc.records.add(&r.records);
            }
            fc.unaccounted = i128::from(fc.read) - i128::from(fc.records.total());
            if fc.unaccounted != 0 {
                problems.push(format!(
                    "flow `{flow}`: {} driver rows read but {} records written, filtered or \
                     dead-lettered",
                    fc.read,
                    fc.records.total()
                ));
            }
            read += fc.read;
            rejected += fc.records.rejected;
            flows.push(fc);
        }
        if read != totals.read {
            problems.push(format!(
                "the state store recorded {} rows read, the write log {read}",
                totals.read
            ));
        }
        if rejected != totals.dead_lettered {
            problems.push(format!(
                "the state store recorded {} rows dead-lettered, but {rejected} records were \
                 rejected",
                totals.dead_lettered
            ));
        }
        Counts {
            read: totals.read,
            dead_lettered: totals.dead_lettered,
            written: totals.written.clone(),
            flows,
        }
    }

    async fn tables(
        &self,
        totals: &ferry_registry::state::RunTotals,
        done: &BTreeMap<&str, Vec<PartitionRecord>>,
        problems: &mut Vec<String>,
    ) -> Vec<TableCheck> {
        let mut checks = Vec::new();
        for (flow, schema) in &self.flows {
            let records = done.get(flow.as_str()).map(Vec::as_slice).unwrap_or(&[]);
            for table in schema.tables() {
                let mut check = TableCheck {
                    table: table.name.clone(),
                    flow: flow.clone(),
                    written: totals.written.get(&table.name).copied().unwrap_or(0),
                    at_write: Checksum::default(),
                    read_back: None,
                    timestamp_digits: BTreeMap::new(),
                    error: None,
                    passed: false,
                };
                let mut digits = BTreeSet::new();
                for w in records.iter().filter_map(|r| r.targets.get(&table.name)) {
                    check.at_write.merge(&w.checksum);
                    digits.insert(w.timestamp_digits.clone());
                }
                if digits.len() > 1 {
                    problems.push(format!(
                        "`{}`: partitions were checksummed at different timestamp precisions \
                         {digits:?}; the target's columns changed during the run",
                        table.name
                    ));
                }
                let spec = match digits.into_iter().next() {
                    Some(digits) => digits
                        .into_iter()
                        .try_fold(ChecksumSpec::new(table), |spec, (column, d)| {
                            spec.with_timestamp_digits(&column, d)
                        }),
                    None => table_specs(self.target.as_ref(), std::slice::from_ref(table))
                        .await
                        .map(|mut s| s.remove(&table.name).expect("one spec per table")),
                };
                let read_back = match spec {
                    Ok(spec) => {
                        check.timestamp_digits = spec
                            .columns()
                            .iter()
                            .filter(|c| c.timestamp_digits < crate::checksum::MICROS)
                            .map(|c| (c.name.clone(), c.timestamp_digits))
                            .collect();
                        self.read_back(table, &spec).await
                    }
                    Err(e) => Err(e),
                };
                match read_back {
                    Ok(sum) => check.read_back = Some(sum),
                    Err(e) => check.error = Some(e.to_string()),
                }
                check.passed = self.compare(&check, problems);
                checks.push(check);
            }
        }
        checks
    }

    async fn read_back(&self, table: &TargetTable, spec: &ChecksumSpec) -> Result<Checksum> {
        let mut stream = self.target.read_back(table).await?;
        let mut sum = Checksum::default();
        while let Some(batch) = stream.next().await {
            sum.add_batch(spec, &batch?)?;
        }
        Ok(sum)
    }

    fn compare(&self, check: &TableCheck, problems: &mut Vec<String>) -> bool {
        let t = &check.table;
        let Some(read_back) = check.read_back else {
            problems.push(format!(
                "`{t}`: couldn't read the target back: {}",
                check.error.as_deref().unwrap_or("unknown error")
            ));
            return false;
        };
        let mut passed = true;
        if check.written != check.at_write.rows {
            problems.push(format!(
                "`{t}`: the sink reported {} rows committed, but {} were handed to it",
                check.written, check.at_write.rows
            ));
            passed = false;
        }
        if read_back.rows != check.at_write.rows {
            problems.push(format!(
                "`{t}`: {} rows were written, the target has {}",
                check.at_write.rows, read_back.rows
            ));
            passed = false;
        }
        if read_back.sum != check.at_write.sum {
            problems.push(format!(
                "`{t}`: checksum mismatch: written {:032x}, read back {:032x}; rows in the \
                 target differ from the rows the run wrote",
                check.at_write.sum, read_back.sum
            ));
            passed = false;
        }
        passed
    }

    async fn constraints(&self, problems: &mut Vec<String>) -> Vec<ConstraintCheck> {
        let mut checks = Vec::new();
        for (_, schema) in &self.flows {
            for table in schema.tables() {
                for field in table.fields.iter().filter(|f| !f.nullable) {
                    let n = self.target.null_count(&table.name, &field.name).await;
                    let constraint = Constraint::NotNull {
                        column: field.name.clone(),
                    };
                    checks.push(constraint_check(table, constraint, n, problems));
                }
                if !table.primary_key.is_empty() {
                    let n = self
                        .target
                        .duplicate_keys(&table.name, &table.primary_key)
                        .await;
                    let constraint = Constraint::Unique {
                        columns: table.primary_key.clone(),
                    };
                    checks.push(constraint_check(table, constraint, n, problems));
                }
                for fk in &table.foreign_keys {
                    let n = self.target.orphans(&table.name, fk).await;
                    let ForeignKey {
                        columns,
                        references_table,
                        references_columns,
                    } = fk.clone();
                    let constraint = Constraint::ForeignKey {
                        columns,
                        references_table,
                        references_columns,
                    };
                    checks.push(constraint_check(table, constraint, n, problems));
                }
            }
        }
        checks
    }

    fn finalize(&self, problems: &mut Vec<String>) -> Vec<FinalizeCheck> {
        self.finalized
            .iter()
            .map(|(flow, p)| {
                problems.push(format!("flow `{flow}`: finalize: {p}"));
                FinalizeCheck {
                    flow: flow.clone(),
                    table: p.table.clone(),
                    object: p.object.clone(),
                    message: p.message.clone(),
                    orphans: p.orphans.as_ref().map(|o| o.count),
                    orphan_keys: p
                        .orphans
                        .as_ref()
                        .map(|o| o.keys.clone())
                        .unwrap_or_default(),
                }
            })
            .collect()
    }

    /// Every record the state store counts as dead-lettered has its row in
    /// the dead-letter table, and no other row is there for the run.
    async fn dead_letters(
        &self,
        run: &RunId,
        totals: &ferry_registry::state::RunTotals,
        problems: &mut Vec<String>,
    ) -> DeadLetterCheck {
        let mut check = DeadLetterCheck::default();
        match self.target.dead_letters(&run.0).await {
            Ok(reasons) => {
                check.rows = reasons.iter().map(|r| r.count).sum();
                check.reasons = reasons;
                check.passed = check.rows == totals.dead_lettered;
                if !check.passed {
                    problems.push(format!(
                        "the state store recorded {} records dead-lettered, but the dead-letter \
                         table holds {} rows for the run",
                        totals.dead_lettered, check.rows
                    ));
                }
            }
            Err(e) => {
                problems.push(format!("couldn't read the dead letters: {e}"));
                check.error = Some(e.to_string());
            }
        }
        check
    }

    async fn assertions(
        &self,
        run: &RunId,
        done: &BTreeMap<&str, Vec<PartitionRecord>>,
        problems: &mut Vec<String>,
    ) -> Vec<AssertionCheck> {
        let mut checks = Vec::new();
        for (flow, assertion) in &self.assertions {
            let partitions = done.get(flow.as_str()).map(Vec::as_slice).unwrap_or(&[]);
            let mut records = RecordCounts::default();
            for r in partitions {
                records.add(&r.records);
            }
            let ctx = AssertionContext {
                run,
                flow,
                partitions,
                target: self.target.as_ref(),
                records,
            };
            let mut check = AssertionCheck {
                flow: flow.clone(),
                name: assertion.name().to_owned(),
                expression: assertion.expression().to_owned(),
                outcome: None,
                error: None,
                passed: false,
            };
            match assertion.evaluate(&ctx).await {
                Ok(outcome) => {
                    check.passed = outcome.passed;
                    if !outcome.passed {
                        problems.push(format!(
                            "flow `{flow}`: assertion `{}` failed: {} {} {} is false",
                            check.name, outcome.left, outcome.op, outcome.right
                        ));
                    }
                    check.outcome = Some(outcome);
                }
                Err(e) => {
                    problems.push(format!(
                        "flow `{flow}`: assertion `{}` couldn't be evaluated: {e}",
                        check.name
                    ));
                    check.error = Some(e.to_string());
                }
            }
            checks.push(check);
        }
        checks
    }
}

fn constraint_check(
    table: &TargetTable,
    constraint: Constraint,
    violations: Result<u64>,
    problems: &mut Vec<String>,
) -> ConstraintCheck {
    let t = &table.name;
    let what = match &constraint {
        Constraint::NotNull { column } => format!("{column} is null in"),
        Constraint::Unique { columns } => {
            format!("duplicated values of ({}) in", columns.join(", "))
        }
        Constraint::ForeignKey {
            columns,
            references_table,
            ..
        } => format!(
            "({}) reference no `{references_table}` row in",
            columns.join(", ")
        ),
    };
    let (violations, error) = match violations {
        Ok(n) => (Some(n), None),
        Err(e) => (None, Some(e.to_string())),
    };
    match (violations, &error) {
        (Some(0), _) => {}
        (Some(n), _) => problems.push(format!("`{t}`: {what} {n} rows")),
        (None, Some(e)) => problems.push(format!("`{t}`: couldn't check {constraint:?}: {e}")),
        (None, None) => unreachable!("an error or a count"),
    }
    ConstraintCheck {
        table: t.clone(),
        constraint,
        passed: violations == Some(0),
        violations,
        error,
    }
}
