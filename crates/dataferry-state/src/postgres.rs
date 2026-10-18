//! `backend: postgres`: state in tables of the state connection's database
//! (0004 §8).
//!
//! The store takes the connection's pool, built by the Postgres connector
//! with its TLS policy, and creates its tables on first use. Workers lease
//! with `FOR UPDATE SKIP LOCKED`, so concurrent workers never get the same
//! partition, and expiry is judged by the database's clock.
//!
//! Epochs come from one sequence for the whole database, so an epoch names
//! a single lease. That is how [`PgLedger::record_done`], which gets no run
//! id, finds the lease's run: through `_ferry_leases`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dataferry_core::{Epoch, FerryError, Lease, Partition, PartitionId, Result, WriteSummary};
use dataferry_registry::state::{
    NewRun, PartitionCounts, PartitionRows, RunId, RunRecord, RunTotals,
};
use dataferry_registry::{Ledger, StateBackend, StateStore};
use deadpool_postgres::Pool;
use tokio::sync::OnceCell;
use tokio_postgres::{Client, GenericClient};

use crate::{fence, new_run_id, store_error};

const BACKEND: &str = "postgres";

/// Serializes concurrent first uses creating the tables, across processes.
const SCHEMA_LOCK: i64 = 0x0f_e2_27_57_a7_e0;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS _ferry_runs (
    run_id         text PRIMARY KEY,
    config_hash    text NOT NULL,
    engine_version text NOT NULL,
    connections    jsonb NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE SEQUENCE IF NOT EXISTS _ferry_epochs;
CREATE TABLE IF NOT EXISTS _ferry_partitions (
    run_id        text NOT NULL REFERENCES _ferry_runs,
    partition_id  text NOT NULL,
    flow          text NOT NULL,
    ordinal       bigint NOT NULL,
    spec          jsonb NOT NULL,
    status        text NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'leased', 'done', 'failed')),
    epoch         bigint NOT NULL DEFAULT 0,
    expires_at    timestamptz,
    rows_read     bigint NOT NULL DEFAULT 0,
    dead_lettered bigint NOT NULL DEFAULT 0,
    rows_written  jsonb,
    error         text,
    PRIMARY KEY (run_id, partition_id)
);
CREATE INDEX IF NOT EXISTS _ferry_partitions_by_flow
    ON _ferry_partitions (run_id, flow, ordinal);
CREATE TABLE IF NOT EXISTS _ferry_leases (
    epoch        bigint PRIMARY KEY,
    run_id       text NOT NULL,
    partition_id text NOT NULL,
    FOREIGN KEY (run_id, partition_id) REFERENCES _ferry_partitions
);
";

/// Registered with the Postgres connector through `register_state_capable`.
/// The pool and connection types are the ones A3's `StateCapable` names.
#[derive(Debug, Clone, Copy, Default)]
pub struct PgStateBackend;

impl StateBackend for PgStateBackend {
    type Pool = Pool;
    type Conn = Client;

    fn open(&self, pool: Pool) -> (Arc<dyn StateStore>, Arc<dyn Ledger<Client>>) {
        (Arc::new(PgStateStore::new(pool)), Arc::new(PgLedger))
    }
}

pub struct PgStateStore {
    pool: Pool,
    schema: OnceCell<()>,
}

/// Writes done-records on the sink's connection, inside its transaction.
/// It needs no pool: the store created the tables before it issued the lease
/// the sink holds.
#[derive(Debug, Clone, Copy, Default)]
pub struct PgLedger;

fn pg_error(context: &str, e: tokio_postgres::Error) -> FerryError {
    let message = match e.as_db_error() {
        Some(db) => db.to_string(),
        None => {
            let mut message = e.to_string();
            let mut source = std::error::Error::source(&e);
            while let Some(cause) = source {
                message += &format!(": {cause}");
                source = cause.source();
            }
            message
        }
    };
    store_error(BACKEND, format!("{context}: {message}"))
}

fn to_i64(n: u64, what: &str) -> Result<i64> {
    i64::try_from(n).map_err(|_| store_error(BACKEND, format!("{what} {n} is out of range")))
}

fn to_u64(n: i64) -> u64 {
    u64::try_from(n).unwrap_or(0)
}

impl PgStateStore {
    /// No I/O, like [`StateBackend::open`].
    pub fn new(pool: Pool) -> Self {
        Self {
            pool,
            schema: OnceCell::new(),
        }
    }

    /// A pooled connection, with the tables created.
    async fn client(&self) -> Result<deadpool_postgres::Object> {
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|e| store_error(BACKEND, format!("connecting: {e}")))?;
        self.schema
            .get_or_try_init(|| async {
                let tx = Client::transaction(&mut client)
                    .await
                    .map_err(|e| pg_error("creating the state tables", e))?;
                tx.execute("SELECT pg_advisory_xact_lock($1)", &[&SCHEMA_LOCK])
                    .await
                    .map_err(|e| pg_error("creating the state tables", e))?;
                tx.batch_execute(SCHEMA)
                    .await
                    .map_err(|e| pg_error("creating the state tables", e))?;
                tx.commit()
                    .await
                    .map_err(|e| pg_error("creating the state tables", e))
            })
            .await?;
        Ok(client)
    }

    /// Runs `f` on the partition `lease` is for, in a transaction holding
    /// its row lock, if the lease is current.
    async fn fenced(
        &self,
        run: &RunId,
        lease: &Lease,
        context: &str,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        allowed: &[&str],
    ) -> Result<()> {
        let mut client = self.client().await?;
        let tx = Client::transaction(&mut client)
            .await
            .map_err(|e| pg_error(context, e))?;
        let status = lock_current(&tx, &run.0, lease, context).await?;
        if !allowed.contains(&status.as_str()) {
            return Err(not_leased(&lease.partition, &status));
        }
        let mut all: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            vec![&run.0, &lease.partition.0];
        all.extend_from_slice(params);
        tx.execute(sql, &all)
            .await
            .map_err(|e| pg_error(context, e))?;
        tx.commit().await.map_err(|e| pg_error(context, e))
    }
}

impl PgStateStore {
    /// Counts the partitions of `run`, or of one of its flows.
    async fn count(&self, run: &RunId, flow: Option<&str>) -> Result<PartitionCounts> {
        let client = self.client().await?;
        let rows = client
            .query(
                "SELECT CASE WHEN status = 'leased' AND expires_at <= now() THEN 'pending'
                             ELSE status END AS state,
                        count(*)
                   FROM _ferry_partitions
                  WHERE run_id = $1 AND ($2::text IS NULL OR flow = $2)
                  GROUP BY 1",
                &[&run.0, &flow],
            )
            .await
            .map_err(|e| pg_error("counting partitions", e))?;
        let mut counts = PartitionCounts::default();
        for row in rows {
            let n = to_u64(row.get(1));
            match row.get::<_, &str>(0) {
                "pending" => counts.pending = n,
                "leased" => counts.leased = n,
                "done" => counts.done = n,
                _ => counts.failed = n,
            }
        }
        Ok(counts)
    }
}

/// Locks the partition's row and checks `lease` is its current lease.
/// Returns the partition's status.
async fn lock_current(
    client: &impl GenericClient,
    run: &str,
    lease: &Lease,
    context: &str,
) -> Result<String> {
    let row = client
        .query_opt(
            "SELECT epoch, status FROM _ferry_partitions \
             WHERE run_id = $1 AND partition_id = $2 FOR UPDATE",
            &[&run, &lease.partition.0],
        )
        .await
        .map_err(|e| pg_error(context, e))?
        .ok_or_else(|| {
            store_error(
                BACKEND,
                format!("run `{run}` has no partition `{}`", lease.partition.0),
            )
        })?;
    fence(lease, Epoch(to_u64(row.get(0))), BACKEND)?;
    Ok(row.get(1))
}

fn not_leased(partition: &PartitionId, status: &str) -> FerryError {
    store_error(
        BACKEND,
        format!("partition `{}` is {status}, not leased", partition.0),
    )
}

fn written_json(summary: &WriteSummary) -> serde_json::Value {
    serde_json::to_value(&summary.rows_written).expect("row counts serialize")
}

#[async_trait]
impl StateStore for PgStateStore {
    async fn create_run(&self, run: NewRun) -> Result<RunId> {
        let client = self.client().await?;
        let id = new_run_id();
        let connections =
            serde_json::to_value(&run.connections).expect("connection names serialize");
        client
            .execute(
                "INSERT INTO _ferry_runs (run_id, config_hash, engine_version, connections) \
                 VALUES ($1, $2, $3, $4)",
                &[&id, &run.config_hash, &run.engine_version, &connections],
            )
            .await
            .map_err(|e| pg_error("creating a run", e))?;
        Ok(RunId(id))
    }

    async fn load_run(&self, run: &RunId) -> Result<Option<RunRecord>> {
        let client = self.client().await?;
        let row = client
            .query_opt(
                "SELECT config_hash, engine_version, connections FROM _ferry_runs \
                 WHERE run_id = $1",
                &[&run.0],
            )
            .await
            .map_err(|e| pg_error("loading a run", e))?;
        row.map(|row| {
            let connections = serde_json::from_value(row.get(2)).map_err(|e| {
                store_error(
                    BACKEND,
                    format!("run `{}` has malformed connections: {e}", run.0),
                )
            })?;
            Ok(RunRecord {
                id: run.clone(),
                config_hash: row.get(0),
                engine_version: row.get(1),
                connections,
            })
        })
        .transpose()
    }

    async fn store_partitions(
        &self,
        run: &RunId,
        flow: &str,
        partitions: &[Partition],
    ) -> Result<()> {
        const CONTEXT: &str = "storing partitions";
        let mut client = self.client().await?;
        let tx = Client::transaction(&mut client)
            .await
            .map_err(|e| pg_error(CONTEXT, e))?;
        // One planner per run at a time, so two can't interleave their
        // checks and inserts.
        tx.execute("SELECT pg_advisory_xact_lock(hashtext($1))", &[&run.0])
            .await
            .map_err(|e| pg_error(CONTEXT, e))?;
        let exists = tx
            .query_opt("SELECT 1 FROM _ferry_runs WHERE run_id = $1", &[&run.0])
            .await
            .map_err(|e| pg_error(CONTEXT, e))?;
        if exists.is_none() {
            return Err(store_error(BACKEND, format!("no run `{}`", run.0)));
        }
        let planned = load_specs(&tx, &run.0, flow).await?;
        if !planned.is_empty() {
            return if planned == partitions {
                Ok(())
            } else {
                Err(crate::already_planned(BACKEND, run, flow))
            };
        }
        let mut seen = std::collections::BTreeSet::new();
        for p in partitions {
            if !seen.insert(&p.id) {
                return Err(crate::duplicate_id(BACKEND, flow, &p.id));
            }
        }
        let ids: Vec<&str> = partitions.iter().map(|p| p.id.0.as_str()).collect();
        let taken = tx
            .query_opt(
                "SELECT partition_id, flow FROM _ferry_partitions \
                 WHERE run_id = $1 AND partition_id = ANY($2) LIMIT 1",
                &[&run.0, &ids],
            )
            .await
            .map_err(|e| pg_error(CONTEXT, e))?;
        if let Some(row) = taken {
            let other: String = row.get(1);
            return Err(crate::id_in_other_flow(
                BACKEND,
                flow,
                &PartitionId(row.get(0)),
                &other,
            ));
        }
        let insert = tx
            .prepare(
                "INSERT INTO _ferry_partitions (run_id, partition_id, flow, ordinal, spec) \
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .await
            .map_err(|e| pg_error(CONTEXT, e))?;
        for (ordinal, p) in partitions.iter().enumerate() {
            let spec = serde_json::to_value(p).expect("a partition serializes");
            let ordinal = to_i64(ordinal as u64, "partition ordinal")?;
            tx.execute(&insert, &[&run.0, &p.id.0, &flow, &ordinal, &spec])
                .await
                .map_err(|e| pg_error(CONTEXT, e))?;
        }
        tx.commit().await.map_err(|e| pg_error(CONTEXT, e))
    }

    async fn load_partitions(&self, run: &RunId, flow: &str) -> Result<Vec<Partition>> {
        let client = self.client().await?;
        load_specs(&**client, &run.0, flow).await
    }

    async fn lease_next(&self, run: &RunId, flow: &str, ttl: Duration) -> Result<Option<Lease>> {
        let client = self.client().await?;
        // One statement, so it commits as a unit: pick, bump the epoch,
        // record the lease.
        let row = client
            .query_opt(
                "WITH next AS (
                     SELECT partition_id FROM _ferry_partitions
                      WHERE run_id = $1 AND flow = $2
                        AND (status = 'pending'
                             OR (status = 'leased' AND expires_at <= now()))
                      ORDER BY ordinal
                      LIMIT 1
                      FOR UPDATE SKIP LOCKED
                 ), leased AS (
                     UPDATE _ferry_partitions p
                        SET status = 'leased',
                            epoch = nextval('_ferry_epochs'),
                            expires_at = now() + make_interval(secs => $3)
                       FROM next
                      WHERE p.run_id = $1 AND p.partition_id = next.partition_id
                  RETURNING p.partition_id, p.epoch
                 )
                 INSERT INTO _ferry_leases (epoch, run_id, partition_id)
                 SELECT epoch, $1, partition_id FROM leased
                 RETURNING partition_id, epoch",
                &[&run.0, &flow, &ttl.as_secs_f64()],
            )
            .await
            .map_err(|e| pg_error("leasing a partition", e))?;
        Ok(row.map(|row| Lease {
            partition: PartitionId(row.get(0)),
            epoch: Epoch(to_u64(row.get(1))),
        }))
    }

    async fn renew(&self, run: &RunId, lease: &Lease, ttl: Duration) -> Result<()> {
        self.fenced(
            run,
            lease,
            "renewing a lease",
            "UPDATE _ferry_partitions SET expires_at = now() + make_interval(secs => $3) \
             WHERE run_id = $1 AND partition_id = $2",
            &[&ttl.as_secs_f64()],
            &["leased"],
        )
        .await
    }

    async fn mark_done(&self, run: &RunId, lease: &Lease, summary: &WriteSummary) -> Result<()> {
        self.fenced(
            run,
            lease,
            "recording a partition done",
            "UPDATE _ferry_partitions \
             SET status = 'done', expires_at = NULL, rows_written = $3 \
             WHERE run_id = $1 AND partition_id = $2",
            &[&written_json(summary)],
            &["leased", "done"],
        )
        .await
    }

    async fn mark_failed(&self, run: &RunId, lease: &Lease, error: &str) -> Result<()> {
        self.fenced(
            run,
            lease,
            "recording a partition failed",
            "UPDATE _ferry_partitions SET status = 'failed', expires_at = NULL, error = $3 \
             WHERE run_id = $1 AND partition_id = $2",
            &[&error],
            &["leased"],
        )
        .await
    }

    async fn record_rows(&self, run: &RunId, lease: &Lease, rows: &PartitionRows) -> Result<()> {
        let read = to_i64(rows.read, "rows read")?;
        let dead = to_i64(rows.dead_lettered, "rows dead-lettered")?;
        self.fenced(
            run,
            lease,
            "recording partition rows",
            "UPDATE _ferry_partitions SET rows_read = $3, dead_lettered = $4 \
             WHERE run_id = $1 AND partition_id = $2",
            &[&read, &dead],
            &["leased"],
        )
        .await
    }

    async fn is_done(&self, run: &RunId, partition: &PartitionId) -> Result<bool> {
        let client = self.client().await?;
        let row = client
            .query_opt(
                "SELECT 1 FROM _ferry_partitions \
                 WHERE run_id = $1 AND partition_id = $2 AND status = 'done'",
                &[&run.0, &partition.0],
            )
            .await
            .map_err(|e| pg_error("checking a partition", e))?;
        Ok(row.is_some())
    }

    async fn counts(&self, run: &RunId) -> Result<PartitionCounts> {
        self.count(run, None).await
    }

    async fn flow_counts(&self, run: &RunId, flow: &str) -> Result<PartitionCounts> {
        self.count(run, Some(flow)).await
    }

    async fn reset_failed(&self, run: &RunId) -> Result<u64> {
        const CONTEXT: &str = "resetting failed partitions";
        let client = self.client().await?;
        let exists = client
            .query_opt("SELECT 1 FROM _ferry_runs WHERE run_id = $1", &[&run.0])
            .await
            .map_err(|e| pg_error(CONTEXT, e))?;
        if exists.is_none() {
            return Err(store_error(BACKEND, format!("no run `{}`", run.0)));
        }
        client
            .execute(
                "UPDATE _ferry_partitions SET status = 'pending', error = NULL \
                 WHERE run_id = $1 AND status = 'failed'",
                &[&run.0],
            )
            .await
            .map_err(|e| pg_error(CONTEXT, e))
    }

    async fn totals(&self, run: &RunId) -> Result<RunTotals> {
        let client = self.client().await?;
        let rows = client
            .query(
                "SELECT rows_read, dead_lettered, rows_written FROM _ferry_partitions \
                 WHERE run_id = $1 AND status = 'done'",
                &[&run.0],
            )
            .await
            .map_err(|e| pg_error("totalling rows", e))?;
        let mut totals = RunTotals::default();
        for row in rows {
            totals.read += to_u64(row.get(0));
            totals.dead_lettered += to_u64(row.get(1));
            let written: Option<serde_json::Value> = row.get(2);
            let written: BTreeMap<String, u64> = written
                .map(serde_json::from_value)
                .transpose()
                .map_err(|e| store_error(BACKEND, format!("malformed row counts: {e}")))?
                .unwrap_or_default();
            for (target, n) in written {
                *totals.written.entry(target).or_default() += n;
            }
        }
        Ok(totals)
    }
}

async fn load_specs(client: &impl GenericClient, run: &str, flow: &str) -> Result<Vec<Partition>> {
    let rows = client
        .query(
            "SELECT spec FROM _ferry_partitions WHERE run_id = $1 AND flow = $2 ORDER BY ordinal",
            &[&run, &flow],
        )
        .await
        .map_err(|e| pg_error("loading partitions", e))?;
    rows.into_iter()
        .map(|row| {
            serde_json::from_value(row.get(0))
                .map_err(|e| store_error(BACKEND, format!("malformed partition: {e}")))
        })
        .collect()
}

#[async_trait]
impl Ledger<Client> for PgLedger {
    /// Runs inside the sink's open transaction: the row lock it takes is
    /// held until the sink's COMMIT or ROLLBACK, so a newer lease can't be
    /// granted between the check and the commit.
    async fn record_done(
        &self,
        conn: &mut Client,
        lease: &Lease,
        summary: &WriteSummary,
    ) -> Result<()> {
        const CONTEXT: &str = "recording a partition done";
        let epoch = to_i64(lease.epoch.0, "epoch")?;
        let row = conn
            .query_opt(
                "SELECT run_id FROM _ferry_leases WHERE epoch = $1 AND partition_id = $2",
                &[&epoch, &lease.partition.0],
            )
            .await
            .map_err(|e| pg_error(CONTEXT, e))?
            .ok_or_else(|| {
                store_error(
                    BACKEND,
                    format!(
                        "partition `{}` was never leased at epoch {}",
                        lease.partition.0, lease.epoch.0
                    ),
                )
            })?;
        let run: String = row.get(0);
        let status = lock_current(&*conn, &run, lease, CONTEXT).await?;
        if status != "leased" && status != "done" {
            return Err(not_leased(&lease.partition, &status));
        }
        conn.execute(
            "UPDATE _ferry_partitions \
             SET status = 'done', expires_at = NULL, rows_written = $3 \
             WHERE run_id = $1 AND partition_id = $2",
            &[&run, &lease.partition.0, &written_json(summary)],
        )
        .await
        .map_err(|e| pg_error(CONTEXT, e))?;
        Ok(())
    }
}
