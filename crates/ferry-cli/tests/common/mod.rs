//! Real databases for the `ferry` binary: the golden documents in a Mongo
//! `shop`, and an empty Postgres `warehouse`, both on testcontainers, and
//! the golden config pointed at them through its `${ENV}` variables.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use assert_cmd::Command;
use mongodb::bson::{Bson, Document};
use serde_json::Value;
use testcontainers_modules::mongo::Mongo;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_postgres::{Client, NoTls};

/// The warehouse password. Nothing `ferry` prints or writes may contain it.
pub const SECRET: &str = "w4rehouse-pa55-never-printed";

pub fn repo(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

pub fn fixture(path: &str) -> Value {
    let text = std::fs::read_to_string(repo(&format!("fixtures/golden/{path}"))).unwrap();
    serde_json::from_str(&text).unwrap()
}

/// The ten golden documents, from Extended JSON.
pub fn golden_documents() -> Vec<Document> {
    fixture("source/orders.json")
        .as_array()
        .unwrap()
        .iter()
        .map(|d| match Bson::try_from(d.clone()).unwrap() {
            Bson::Document(doc) => doc,
            other => panic!("not a document: {other:?}"),
        })
        .collect()
}

pub struct Shop {
    _container: ContainerAsync<Mongo>,
    pub url: String,
    pub client: mongodb::Client,
}

impl Shop {
    /// A Mongo whose `shop.orders` holds the golden documents.
    pub async fn golden() -> Self {
        let container = Mongo::default().start().await.expect("start mongo");
        let port = container.get_host_port_ipv4(27017).await.unwrap();
        let url = format!("mongodb://127.0.0.1:{port}");
        let client = mongodb::Client::with_uri_str(&url).await.unwrap();
        client
            .database("shop")
            .collection::<Document>("orders")
            .insert_many(golden_documents())
            .await
            .unwrap();
        Self {
            _container: container,
            url,
            client,
        }
    }

    pub fn orders(&self) -> mongodb::Collection<Document> {
        self.client.database("shop").collection("orders")
    }
}

pub struct Warehouse {
    _container: ContainerAsync<Postgres>,
    pub port: u16,
    pub url: String,
    pub client: Client,
}

impl Warehouse {
    /// An empty Postgres, its password [`SECRET`].
    pub async fn start() -> Self {
        let container = Postgres::default()
            .with_password(SECRET)
            .start()
            .await
            .expect("start postgres");
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:{SECRET}@127.0.0.1:{port}/postgres");
        let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(connection);
        Self {
            _container: container,
            port,
            url,
            client,
        }
    }

    pub async fn strings(&self, sql: &str) -> Vec<String> {
        self.client
            .query(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .iter()
            .map(|r| r.get(0))
            .collect()
    }

    pub async fn count(&self, table: &str) -> i64 {
        self.client
            .query_one(&format!("SELECT count(*) FROM {table}"), &[])
            .await
            .unwrap_or_else(|e| panic!("{table}: {e}"))
            .get(0)
    }

    /// Every row of `table` as JSON, ordered by `order_by`: what
    /// `fixtures/golden/expected` holds, in the same encoding (timestamps
    /// in UTC to the microsecond, as `2026-03-01T10:00:00.000000Z`).
    pub async fn json_rows(&self, table: &str, order_by: &str) -> Vec<Value> {
        let sql = format!(
            "SELECT row_to_json(t)::text FROM (SELECT * FROM {table} ORDER BY {order_by}) t"
        );
        self.client
            .batch_execute("SET TIME ZONE 'UTC'")
            .await
            .unwrap();
        self.strings(&sql)
            .await
            .iter()
            .map(|row| {
                let mut row: Value = serde_json::from_str(row).unwrap();
                for value in row.as_object_mut().unwrap().values_mut() {
                    if let Some(ts) = value.as_str().and_then(timestamp) {
                        *value = Value::String(ts);
                    }
                }
                row
            })
            .collect()
    }

    /// Relations, columns and constraints of every schema that isn't the
    /// system's, and the schemas themselves: what DDL would change.
    pub async fn catalog(&self) -> Vec<String> {
        let mut out = self
            .strings(
                "SELECT 'schema ' || nspname FROM pg_namespace \
                 WHERE nspname NOT LIKE 'pg\\_%' AND nspname <> 'information_schema' \
                 ORDER BY 1",
            )
            .await;
        out.extend(
            self.strings(
                "SELECT c.relkind::text || ' ' || n.nspname || '.' || c.relname || ' ' || \
                        coalesce((SELECT string_agg(a.attname || ':' || \
                                         format_type(a.atttypid, a.atttypmod), ',' ORDER BY a.attnum) \
                                  FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0 \
                                    AND NOT a.attisdropped), '') AS r \
                 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname NOT LIKE 'pg\\_%' AND n.nspname <> 'information_schema' \
                 ORDER BY r",
            )
            .await,
        );
        out.extend(
            self.strings(
                "SELECT conrelid::regclass::text || ' ' || conname || ' ' || \
                        pg_get_constraintdef(oid) \
                 FROM pg_constraint WHERE connamespace NOT IN \
                   (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg\\_%' \
                    OR nspname = 'information_schema') ORDER BY 1",
            )
            .await,
        );
        out
    }
}

/// `2026-03-01 10:00:00+00` (or with fractions) as the fixtures write it.
fn timestamp(text: &str) -> Option<String> {
    let (date, rest) = text.split_once('T').or_else(|| text.split_once(' '))?;
    if date.len() != 10 || !date.as_bytes()[4].eq(&b'-') {
        return None;
    }
    let rest = rest
        .strip_suffix("+00:00")
        .or_else(|| rest.strip_suffix("+00"))
        .or_else(|| rest.strip_suffix('Z'))?;
    let (time, fraction) = rest.split_once('.').unwrap_or((rest, ""));
    if time.len() != 8 {
        return None;
    }
    Some(format!("{date}T{time}.{fraction:0<6}Z"))
}

/// The `ferry` binary, run from `cwd` with only the environment given.
pub fn ferry(cwd: &Path) -> Command {
    let mut cmd = Command::cargo_bin("ferry").unwrap();
    cmd.current_dir(cwd)
        .env_clear()
        .timeout(Duration::from_secs(180));
    cmd
}

/// `ferry` with the golden config's variables pointing at `shop` and
/// `warehouse`.
pub fn golden_ferry(cwd: &Path, shop: &Shop, warehouse: &Warehouse) -> Command {
    let mut cmd = ferry(cwd);
    cmd.env("FERRY_GOLDEN_MONGO_URL", &shop.url)
        .env("FERRY_GOLDEN_PG_URL", &warehouse.url);
    cmd
}

/// A copy of `examples/golden/ferry`, to edit.
pub fn golden_copy() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    copy_dir(&repo("examples/golden/ferry"), &dir.path().join("ferry"));
    dir
}

pub fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// Replaces `from` with `to` in `path`, which must contain it.
pub fn edit(path: &Path, from: &str, to: &str) {
    let text = std::fs::read_to_string(path).unwrap();
    assert!(text.contains(from), "{} has no {from:?}", path.display());
    std::fs::write(path, text.replacen(from, to, 1)).unwrap();
}

pub struct Output {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
}

impl Output {
    /// Fails the test with both streams if the exit code isn't `code`.
    pub fn expect(self, code: i32) -> Self {
        assert_eq!(
            self.code, code,
            "exit {}\n--- stdout\n{}\n--- stderr\n{}",
            self.code, self.stdout, self.stderr
        );
        self
    }

    /// Neither stream contains the warehouse password.
    pub fn no_secret(self) -> Self {
        assert!(!self.stdout.contains(SECRET), "{}", self.stdout);
        assert!(!self.stderr.contains(SECRET), "{}", self.stderr);
        self
    }
}

pub fn output(cmd: &mut Command) -> Output {
    let out = cmd.output().unwrap();
    Output {
        code: out.status.code().unwrap_or(-1),
        stdout: String::from_utf8(out.stdout).unwrap(),
        stderr: String::from_utf8(out.stderr).unwrap(),
    }
}
