//! The conformance suite (S3, `dataferry_runtime::testkit::conformance`) on
//! the built-in connectors, through the real `dataferry` binary: Mongo →
//! Postgres on `examples/golden/ferry`, and Postgres → Postgres on
//! `examples/golden-pg/ferry` with the same golden data loaded as rows.
//! Each pair gets an uninterrupted golden run, then 50 runs killed with
//! SIGKILL and resumed with `dataferry resume <run-id>`, every one ending with
//! the expected rows, a green report, and the uninterrupted run's
//! checksums.
//!
//! The source holds [`COPIES`] copies of the golden data, each with its
//! own ids (`65` + the copy number + the rest of the original id), so a
//! run has a few dozen partitions for kills to land between. Copy 0 is
//! the golden data itself.

mod common;

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use common::{SECRET, Shop, Warehouse, copy_dir, fixture, golden_documents, repo};
use dataferry_core::KeyValue;
use dataferry_core::transform::SourceRecordId;
use dataferry_dsl::compile::key::{Step, child_key};
use dataferry_registry::state::RunId;
use dataferry_runtime::testkit::chaos::Progress;
use dataferry_runtime::testkit::conformance::{self, Pair};
use mongodb::bson::{Bson, oid::ObjectId};
use serde_json::Value;
use tokio::process::Command;
use tokio_postgres::{Client, NoTls};

/// Copies of the golden data in the source: 80 orders in partitions of
/// four. Eight dead letters, within the config's `max_errors: 10`.
const COPIES: usize = 8;

/// The `application_name` of `dataferry`'s Postgres connections.
const FERRY_APP: &str = "ferry_conformance";

/// How long ending a killed child's sessions may take.
const SESSIONS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The sessions named `app`, for an error: what each is doing.
async fn sessions(client: &Client, app: &str) -> String {
    let sql = "SELECT concat_ws(' ', pid, state, wait_event_type, wait_event, \
                      'for ' || (now() - state_change)::text, \
                      'blocked by ' || pg_blocking_pids(pid)::text, \
                      'from ' || client_addr || ':' || client_port, left(query, 120)) \
               FROM pg_stat_activity WHERE application_name = $1";
    match client.query(sql, &[&app]).await {
        Ok(rows) => rows
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join("; "),
        Err(e) => format!("(couldn't list them: {e})"),
    }
}

/// `url` with [`FERRY_APP`] as its `application_name`.
fn with_app(url: &str) -> String {
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{url}{sep}application_name={FERRY_APP}")
}

/// `id` as copy `copy` has it.
fn copy_id(id: &str, copy: usize) -> String {
    assert!(id.starts_with("650000") && id.len() == 24, "{id}");
    format!("65{copy:04x}{}", &id[6..])
}

/// The golden rows of `fixtures/golden/expected/<table>.json` for every
/// copy, sorted as `Warehouse::json_rows` returns them.
fn expected(table: &str, with_item_ids: bool) -> Vec<Value> {
    let rows = fixture(&format!("expected/{table}.json"));
    let mut out = Vec::new();
    for copy in 0..COPIES {
        for row in rows.as_array().unwrap() {
            let mut row = row.clone();
            if table == "orders" {
                row["id"] = copy_id(row["id"].as_str().unwrap(), copy).into();
            } else {
                let order = copy_id(row["order_id"].as_str().unwrap(), copy);
                if with_item_ids {
                    // `$key` over the copy's `_id` and `items[line_no]`.
                    let record = SourceRecordId(vec![KeyValue::Str(order.clone())]);
                    let step = Step {
                        path: "items".into(),
                        index: row["line_no"].as_u64().unwrap(),
                    };
                    let key = child_key(&record, &[step]).to_string();
                    if copy == 0 {
                        assert_eq!(row["id"], key.as_str(), "the fixture's key is computed so");
                    }
                    row["id"] = key.into();
                } else {
                    row.as_object_mut().unwrap().remove("id");
                }
                row["order_id"] = order.into();
            }
            out.push(row);
        }
    }
    let key = |r: &Value| match table {
        "orders" => r["id"].as_str().unwrap().to_owned(),
        _ => format!("{}/{}", r["order_id"].as_str().unwrap(), r["line_no"]),
    };
    out.sort_by_key(key);
    out
}

/// Where the golden data comes from.
enum Source {
    Mongo(Box<Shop>),
    /// The `shop` database next to the warehouse.
    Postgres {
        url: String,
    },
}

struct Golden {
    source: Source,
    warehouse: Warehouse,
    /// The config's copy, so the test never writes into `examples/`.
    config: tempfile::TempDir,
}

impl Golden {
    /// Mongo → Postgres: `examples/golden/ferry`, the golden documents in
    /// Mongo.
    async fn mongo() -> Self {
        let (shop, warehouse) = tokio::join!(Shop::golden(), Warehouse::start());
        let mut copies = Vec::new();
        for copy in 1..COPIES {
            for mut doc in golden_documents() {
                let id = doc.get_object_id("_id").unwrap().to_hex();
                let id = ObjectId::parse_str(copy_id(&id, copy)).unwrap();
                doc.insert("_id", Bson::ObjectId(id));
                copies.push(doc);
            }
        }
        shop.orders().insert_many(copies).await.unwrap();
        Self {
            source: Source::Mongo(Box::new(shop)),
            warehouse,
            config: config("examples/golden/ferry"),
        }
    }

    /// Postgres → Postgres: `examples/golden-pg/ferry`, its `source.sql`
    /// in a `shop` database on the warehouse's server.
    async fn postgres() -> Self {
        let warehouse = Warehouse::start().await;
        warehouse
            .client
            .batch_execute("CREATE DATABASE shop")
            .await
            .unwrap();
        let url = format!(
            "postgres://postgres:{SECRET}@127.0.0.1:{}/shop",
            warehouse.port
        );
        let shop = connect(&url).await.unwrap();
        let sql = std::fs::read_to_string(repo("examples/golden-pg/source.sql")).unwrap();
        shop.batch_execute(&sql).await.unwrap();
        // The copies: the same rows under each copy's ids.
        let id = |column: &str| format!("'65' || lpad(to_hex(c), 4, '0') || substr({column}, 7)");
        shop.batch_execute(&format!(
            "INSERT INTO orders SELECT {}, status, customer_id, total, created_at, deleted_at \
               FROM orders, generate_series(1, {last}) c; \
             INSERT INTO order_items SELECT {}, line_no, sku, qty, price \
               FROM order_items, generate_series(1, {last}) c \
               WHERE order_items.order_id LIKE '650000%';",
            id("id"),
            id("order_id"),
            last = COPIES - 1,
        ))
        .await
        .unwrap();
        Self {
            source: Source::Postgres { url },
            warehouse,
            config: config("examples/golden-pg/ferry"),
        }
    }
}

/// A copy of the example config at `path`.
fn config(path: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    copy_dir(&repo(path), &dir.path().join("ferry"));
    dir
}

async fn connect(url: &str) -> Result<Client, String> {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .map_err(|e| e.to_string())?;
    tokio::spawn(connection);
    Ok(client)
}

#[async_trait]
impl Pair for Golden {
    fn name(&self) -> String {
        match self.source {
            Source::Mongo(_) => "mongo → postgres".into(),
            Source::Postgres { .. } => "postgres → postgres".into(),
        }
    }

    fn command(&self) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_dataferry"));
        cmd.current_dir(self.config.path())
            .env_clear()
            .env("FERRY_GOLDEN_PG_URL", with_app(&self.warehouse.url));
        match &self.source {
            Source::Mongo(shop) => cmd.env("FERRY_GOLDEN_MONGO_URL", &shop.url),
            Source::Postgres { url } => cmd.env("FERRY_GOLDEN_PG_SOURCE_URL", with_app(url)),
        };
        cmd
    }

    /// Ends the killed `dataferry`'s sessions on the warehouse's server (both
    /// databases of Postgres → Postgres are on it), which Docker's port
    /// forwarding can keep open with their transactions and locks. One
    /// still running a statement is left to finish first, as the server
    /// would on a closed connection; one waiting on the dead client (the
    /// rest of a COPY) is ended at once.
    async fn killed(&self) -> Result<(), String> {
        let client = &self.warehouse.client;
        let started = std::time::Instant::now();
        loop {
            client
                .execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                     WHERE application_name = $1 \
                 AND (state <> 'active' OR wait_event_type = 'Client')",
                    &[&FERRY_APP],
                )
                .await
                .map_err(|e| e.to_string())?;
            let left: i64 = client
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1",
                    &[&FERRY_APP],
                )
                .await
                .map_err(|e| e.to_string())?
                .get(0);
            if left == 0 {
                return Ok(());
            }
            if started.elapsed() > SESSIONS_TIMEOUT {
                return Err(format!(
                    "sessions of a killed child didn't end: {}",
                    sessions(client, FERRY_APP).await
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    /// Every schema in the warehouse, so the target tables, `_ferry_dlq`
    /// and the state store's tables all go.
    async fn reset(&self) -> Result<(), String> {
        self.killed().await?;
        let schemas = self
            .warehouse
            .client
            .query(
                "SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg\\_%' \
                 AND nspname <> 'information_schema'",
                &[],
            )
            .await
            .map_err(|e| e.to_string())?;
        let mut sql = String::new();
        for row in schemas {
            let schema: String = row.get(0);
            sql.push_str(&format!("DROP SCHEMA \"{schema}\" CASCADE; "));
        }
        sql.push_str("CREATE SCHEMA public;");
        self.warehouse
            .client
            .batch_execute(&sql)
            .await
            .map_err(|e| e.to_string())
    }

    /// From the warehouse's `_ferry_partitions`, where the config keeps
    /// its state (`backend: postgres`, `connection: warehouse`).
    async fn progress(&self, run: &RunId, _: &Path) -> Result<Progress, String> {
        // As the store counts them: a lease past its expiry is pending.
        let row = self
            .warehouse
            .client
            .query_one(
                "SELECT count(*) FILTER (WHERE status = 'done'), \
                        count(*) FILTER (WHERE status = 'leased' AND expires_at > now()), \
                        count(*) \
                 FROM _ferry_partitions WHERE run_id = $1",
                &[&run.0],
            )
            .await
            .map_err(|e| e.to_string())?;
        let count = |i: usize| row.get::<_, i64>(i) as u64;
        Ok(Progress {
            started: true,
            done: count(0),
            leased: count(1),
            total: count(2),
        })
    }

    async fn verify(&self) -> Result<(), String> {
        let with_item_ids = matches!(self.source, Source::Mongo(_));
        let mut problems = Vec::new();
        for (table, order_by) in [("orders", "id"), ("order_items", "order_id, line_no")] {
            let actual = self
                .warehouse
                .json_rows(&format!("public.{table}"), order_by)
                .await;
            let expected = expected(table, with_item_ids);
            if actual != expected {
                let missing = expected.iter().filter(|r| !actual.contains(r)).count();
                let extra = actual.iter().filter(|r| !expected.contains(r)).count();
                problems.push(format!(
                    "{table}: {} rows, expected {}: {missing} missing, {extra} unexpected",
                    actual.len(),
                    expected.len()
                ));
            }
        }
        // One dead letter per copy: …0006, as a whole.
        let letters = self
            .warehouse
            .strings("SELECT source_id::text FROM public._ferry_dlq ORDER BY 1")
            .await;
        let want: Vec<String> = (0..COPIES)
            .map(|c| format!("[\"{}\"]", copy_id("650000000000000000000006", c)))
            .collect();
        if letters != want {
            problems.push(format!("dead letters {letters:?}, expected {want:?}"));
        }
        match problems.is_empty() {
            true => Ok(()),
            false => Err(problems.join("; ")),
        }
    }
}

/// The config's `workers`: at most that many partitions are in flight.
fn workers(config: &Path) -> u64 {
    let runtime = std::fs::read_to_string(config.join("ferry/runtime.yaml")).unwrap();
    runtime
        .lines()
        .find_map(|l| l.trim().strip_prefix("workers: "))
        .and_then(|n| n.parse().ok())
        .expect("runtime.yaml sets workers")
}

async fn certify(pair: Golden, assertions: &[&str]) {
    let work = tempfile::tempdir().unwrap();
    let config = conformance::Config::new(workers(pair.config.path()));
    let certificate = conformance::certify(&pair, &config, work.path())
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    eprintln!("{}: {certificate:?}", pair.name());
    let golden = &certificate.golden;
    assert_eq!(
        golden.tables.keys().collect::<Vec<_>>(),
        ["order_items", "orders"]
    );
    assert_eq!(golden.tables["orders"].0, 8 * COPIES as u64);
    assert_eq!(golden.tables["order_items"].0, 7 * COPIES as u64);
    assert_eq!(golden.assertions, assertions);
    let chaos = &certificate.chaos;
    assert_eq!(chaos.runs, 50);
    assert!(chaos.kills_mid_partition >= 50, "{chaos:?}");
    assert_no_secret(work.path());
}

/// No file the runs wrote (reports, write logs, each attempt's output)
/// holds the warehouse password.
fn assert_no_secret(dir: &Path) {
    let mut stack: Vec<PathBuf> = vec![dir.to_owned()];
    let mut files = 0;
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                let bytes = std::fs::read(&path).unwrap();
                assert!(
                    !String::from_utf8_lossy(&bytes).contains(SECRET),
                    "{}",
                    path.display()
                );
                files += 1;
            }
        }
    }
    assert!(files > 100, "only {files} files written");
}

#[tokio::test(flavor = "multi_thread")]
async fn mongo_to_postgres_is_certified() {
    let assertions = ["order_count", "order_total", "item_count"];
    certify(Golden::mongo().await, &assertions).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_to_postgres_is_certified() {
    certify(Golden::postgres().await, &["order_count", "order_total"]).await;
}

/// What Postgres → Postgres runs on, checked on its own: the source, with
/// its copies, is the golden documents as rows.
#[tokio::test(flavor = "multi_thread")]
async fn the_postgres_source_holds_the_golden_documents_as_rows() {
    let pair = Golden::postgres().await;
    let Source::Postgres { url } = &pair.source else {
        unreachable!()
    };
    let shop = connect(url).await.unwrap();
    shop.batch_execute("SET TIME ZONE 'UTC'").await.unwrap();
    let strings = |sql: &'static str| {
        let shop = &shop;
        async move {
            let rows = shop.query(sql, &[]).await.unwrap();
            // Sorted here, not by the server's collation.
            let mut rows: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
            rows.sort();
            rows
        }
    };
    let orders = strings(
        "SELECT concat_ws('|', id, status, customer_id, total::float8, \
                to_char(created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), \
                to_char(deleted_at, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')) \
         FROM orders ORDER BY id",
    )
    .await;
    let items = strings(
        "SELECT concat_ws('|', order_id, line_no, sku, qty, price::float8) \
         FROM order_items ORDER BY order_id, line_no",
    )
    .await;

    // The same fields of each document, `customer.id` and `items[]`
    // flattened; null and missing both give nothing.
    let text = |v: &Value| match v {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Object(o) if o.contains_key("$oid") => Some(o["$oid"].as_str()?.to_owned()),
        Value::Object(o) if o.contains_key("$date") => Some(o["$date"].as_str()?.to_owned()),
        Value::Object(o) if o.contains_key("$numberDecimal") => {
            let n: f64 = o["$numberDecimal"].as_str()?.parse().ok()?;
            Some(n.to_string())
        }
        other => Some(other.to_string()),
    };
    let join = |fields: Vec<Option<String>>| -> String {
        fields.into_iter().flatten().collect::<Vec<_>>().join("|")
    };
    let (mut want_orders, mut want_items) = (Vec::new(), Vec::new());
    for copy in 0..COPIES {
        for doc in fixture("source/orders.json").as_array().unwrap() {
            let id = copy_id(&text(&doc["_id"]).unwrap(), copy);
            want_orders.push(join(vec![
                Some(id.clone()),
                text(&doc["status"]),
                text(&doc["customer"]["id"]),
                text(&doc["total"]),
                text(&doc["created_at"]),
                text(&doc["deleted_at"]),
            ]));
            for (line, item) in doc["items"].as_array().into_iter().flatten().enumerate() {
                want_items.push(join(vec![
                    Some(id.clone()),
                    Some(line.to_string()),
                    text(&item["sku"]),
                    text(&item["qty"]),
                    text(&item["price"]),
                ]));
            }
        }
    }
    want_orders.sort();
    want_items.sort();
    assert_eq!(orders, want_orders);
    assert_eq!(items, want_items);
    assert_eq!(orders.len(), 10 * COPIES);
}
