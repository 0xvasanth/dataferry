//! The connector contract suite (0004 Consequences): what every connector
//! registered through this crate must guarantee, runnable by built-in and
//! third-party connectors alike.
//!
//! Like dataferry-core's suites, these return every violation they find instead
//! of panicking on the first, so a test can assert that a deliberately
//! broken connector is caught for the right reason.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use dataferry_core::testkit::{SinkFixture, SinkProbe};
use dataferry_core::{FerryError, IdempotencyStrategy, Lease, Result, Sink, WriteSummary};
use serde_json::Value;

use crate::connector::{BuildError, Connector, RawConnection, Roles};
use crate::registry::ConnectorRegistry;
use crate::state::{Ledger, NewRun, StateBackend, StateCapable, StateStore};

/// A key no connector should accept.
const UNKNOWN_FIELD: &str = "ferry_contract_unknown_field";

/// The flow name the state-capable suite plans its partition under.
const FLOW: &str = "contract";

/// A valid connection for the connector under test.
pub struct ConnectorFixture {
    /// The type-specific fields of a valid entry (no `name` or `type`).
    /// Building them must not do I/O, so any address works.
    pub options: Value,
    /// A secret `options` holds, in a `Secret` field or as a
    /// [`derived_secret`](Connector::derived_secrets).
    pub secret: String,
}

/// Checks the guarantees every [`Connector`] makes:
///
/// - it registers: its type is not empty and its options use no reserved key;
/// - its options schema generates and is closed (`deny_unknown_fields`);
/// - an unknown field is rejected as an options error at that field's path;
/// - `build` returns exactly the roles asked for, for every role it declares,
///   and a role it doesn't declare is refused;
/// - the fixture's secret is in the redaction set, and never appears in
///   `describe()` or in the `Debug`/`Display` of what `build` returns,
///   successful or not.
pub fn check_connector<C: Connector>(connector: C, fixture: &ConnectorFixture) -> Vec<String> {
    let mut v = Violations {
        secret: &fixture.secret,
        found: Vec::new(),
    };
    let Value::Object(fields) = &fixture.options else {
        return vec!["the fixture's options are not an object".into()];
    };
    let raw = |fields: serde_json::Map<String, Value>| RawConnection {
        name: "contract".into(),
        r#type: C::TYPE.into(),
        fields,
    };

    // Straight from the connector, not through the registry, which would
    // redact it: a connector must not rely on that backstop.
    match serde_json::from_value::<C::Options>(fixture.options.clone()) {
        Ok(options) => v.leak("describe()", &connector.describe(&options)),
        Err(e) => v.push(format!("the fixture's options don't deserialize: {e}")),
    }

    let mut registry = ConnectorRegistry::new();
    if let Err(e) = registry.try_register(connector) {
        v.push(format!("does not register: {e}"));
        return v.found;
    }

    let schema = registry.connections_file_schema();
    let branch = schema
        .pointer("/properties/connections/items/oneOf")
        .and_then(Value::as_array)
        .and_then(|branches| {
            branches
                .iter()
                .find(|b| b.pointer("/properties/type/const") == Some(&Value::from(C::TYPE)))
        });
    match branch {
        None => v.push("the connections schema has no branch for it".into()),
        Some(b) if b.get("additionalProperties") != Some(&Value::Bool(false)) => {
            v.push("its options schema is open: use #[serde(deny_unknown_fields)]".into())
        }
        Some(_) => {}
    }

    let conn = raw(fields.clone());
    for wants in [Roles::SOURCE, Roles::SINK, Roles::BOTH] {
        match (C::ROLES.contains(wants), registry.build(&conn, wants)) {
            (true, Ok(built)) => {
                v.leak(&format!("the {wants} build"), &format!("{built:?}"));
                if !built.secrets.iter().any(|s| s.contains(&fixture.secret)) {
                    v.push(format!(
                        "the secret is missing from the {wants} build's redaction set: \
                         mark its field `Secret` or report it from derived_secrets()"
                    ));
                }
            }
            (true, Err(e)) => {
                v.leak_error(&format!("the {wants} build error"), &e);
                v.push(format!("building as {wants} failed: {e}"));
            }
            (false, Ok(_)) => v.push(format!(
                "built as {wants} although it declares only {}",
                C::ROLES
            )),
            (false, Err(e)) => v.leak_error(&format!("the refused {wants} build error"), &e),
        }
    }

    let mut unknown = fields.clone();
    unknown.insert(UNKNOWN_FIELD.into(), Value::Bool(true));
    let wants = if C::ROLES.source {
        Roles::SOURCE
    } else {
        Roles::SINK
    };
    match registry.build(&raw(unknown), wants) {
        Err(e @ BuildError::Options(_)) => {
            v.leak_error("the unknown-field error", &e);
            if let BuildError::Options(errors) = &e
                && !errors.iter().any(|o| o.path == UNKNOWN_FIELD)
            {
                v.push(format!(
                    "an unknown field is rejected without its path: {errors:?}"
                ));
            }
        }
        Err(e) => v.push(format!(
            "an unknown field fails as {e:?}, not as an options error"
        )),
        Ok(_) => v.push(format!("the unknown field `{UNKNOWN_FIELD}` was accepted")),
    }
    v.found
}

struct Violations<'a> {
    secret: &'a str,
    found: Vec<String>,
}

impl Violations<'_> {
    fn push(&mut self, violation: String) {
        self.found.push(violation);
    }

    fn leak(&mut self, what: &str, text: &str) {
        if !self.secret.is_empty() && text.contains(self.secret) {
            self.found.push(format!("the secret appears in {what}"));
        }
    }

    fn leak_error(&mut self, what: &str, e: &BuildError) {
        self.leak(what, &format!("{e} {e:?}"));
    }
}

/// How the suite's ledger fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Crash {
    None,
    /// After the sink's writes, instead of writing the done-record.
    BeforeRecord,
    /// After the done-record was written on the sink's connection, before
    /// COMMIT.
    AfterRecord,
}

/// Wraps the backend under test so its ledger can simulate a crash.
struct CrashingBackend<B> {
    inner: B,
    crash: Arc<Mutex<Crash>>,
}

impl<B: StateBackend> StateBackend for CrashingBackend<B> {
    type Pool = B::Pool;
    type Conn = B::Conn;

    fn open(&self, pool: B::Pool) -> (Arc<dyn StateStore>, Arc<dyn Ledger<B::Conn>>) {
        let (store, ledger) = self.inner.open(pool);
        let ledger = Arc::new(CrashingLedger {
            inner: ledger,
            crash: Arc::clone(&self.crash),
        });
        (store, ledger)
    }
}

struct CrashingLedger<Conn> {
    inner: Arc<dyn Ledger<Conn>>,
    crash: Arc<Mutex<Crash>>,
}

fn crashed(when: &str) -> FerryError {
    FerryError::Connector {
        connector: "contract".into(),
        message: format!("simulated crash {when}"),
    }
}

#[async_trait]
impl<Conn: Send + 'static> Ledger<Conn> for CrashingLedger<Conn> {
    async fn record_done(
        &self,
        conn: &mut Conn,
        lease: &Lease,
        summary: &WriteSummary,
    ) -> Result<()> {
        let crash = *self.crash.lock().unwrap();
        match crash {
            Crash::BeforeRecord => Err(crashed("before record_done")),
            Crash::AfterRecord => {
                self.inner.record_done(conn, lease, summary).await?;
                Err(crashed("after record_done, before COMMIT"))
            }
            Crash::None => self.inner.record_done(conn, lease, summary).await,
        }
    }
}

/// Checks what a [`StateCapable`] connector and its [`StateBackend`]
/// guarantee together (0004 §8), starting from empty targets and an empty
/// state store:
///
/// - a sink built with plain `build` doesn't list `TxnWithState`, and one
///   built with the state store does;
/// - a crash after the data writes, before or after `record_done` but before
///   COMMIT, commits neither the rows nor the done-record;
/// - a clean commit makes both durable, and the store sees the done-record
///   the sink's transaction wrote.
///
/// `probe` must observe the database the fixture connects to.
pub async fn check_state_capable<C, B>(
    connector: C,
    backend: B,
    fixture: &ConnectorFixture,
    sink_fixture: &SinkFixture,
    probe: &dyn SinkProbe,
) -> Vec<String>
where
    C: StateCapable,
    B: StateBackend<Pool = C::Pool, Conn = C::Conn>,
{
    let crash = Arc::new(Mutex::new(Crash::None));
    let mut registry = ConnectorRegistry::new();
    let backend = CrashingBackend {
        inner: backend,
        crash: Arc::clone(&crash),
    };
    if let Err(e) = registry.try_register_state_capable(connector, backend) {
        return vec![format!("does not register: {e}")];
    }
    let Value::Object(fields) = &fixture.options else {
        return vec!["the fixture's options are not an object".into()];
    };
    let conn = RawConnection {
        name: "contract".into(),
        r#type: C::TYPE.into(),
        fields: fields.clone(),
    };
    match run_state(&registry, &conn, &crash, sink_fixture, probe).await {
        Ok(violations) => violations,
        Err(e) => vec![format!("operation failed: {e}")],
    }
}

async fn run_state(
    registry: &ConnectorRegistry,
    conn: &RawConnection,
    crash: &Mutex<Crash>,
    f: &SinkFixture,
    probe: &dyn SinkProbe,
) -> std::result::Result<Vec<String>, String> {
    let mut violations = Vec::new();
    let plain = registry
        .build(conn, Roles::SINK)
        .map_err(|e| format!("plain build: {e}"))?;
    let plain_sink = plain.endpoints.sink.ok_or("plain build returned no sink")?;
    if lists_txn_with_state(plain_sink.as_ref()).await? {
        violations.push("a sink built without the state store lists TxnWithState".into());
    }

    let (built, store) = registry
        .build_with_state(conn, Roles::SINK)
        .map_err(|e| format!("build_with_state: {e}"))?;
    let sink = built
        .endpoints
        .sink
        .ok_or("build_with_state returned no sink")?;
    if !lists_txn_with_state(sink.as_ref()).await? {
        violations.push("a sink built with the state store doesn't list TxnWithState".into());
        return Ok(violations);
    }
    let fail = |e: FerryError| e.to_string();
    sink.prepare(&f.schema, f.mode).await.map_err(fail)?;
    let run = store
        .create_run(NewRun {
            config_hash: "sha256:contract".into(),
            engine_version: "contract".into(),
            connections: vec![("contract".into(), conn.r#type.clone())],
        })
        .await
        .map_err(fail)?;
    store
        .store_partitions(&run, FLOW, std::slice::from_ref(&f.partition))
        .await
        .map_err(fail)?;
    let mut expected: BTreeMap<String, u64> = BTreeMap::new();
    for (target, batch) in &f.batches {
        *expected.entry(target.clone()).or_default() += batch.num_rows() as u64;
    }

    for when in [Crash::BeforeRecord, Crash::AfterRecord] {
        *crash.lock().unwrap() = when;
        // Expires at once, so the next step can lease the partition again.
        let Some(lease) = store
            .lease_next(&run, FLOW, Duration::ZERO)
            .await
            .map_err(fail)?
        else {
            violations.push(format!(
                "before a crash {when:?}, the partition could not be leased: it is no longer pending"
            ));
            return Ok(violations);
        };
        match write_partition(sink.as_ref(), f, &lease).await {
            Ok(_) => violations.push(format!("commit succeeded through a crash {when:?}")),
            Err(e) if !e.to_string().contains("simulated crash") => violations.push(format!(
                "a crash {when:?} surfaced as an unrelated error: {e}"
            )),
            Err(_) => {}
        }
        for target in expected.keys() {
            let n = probe.row_count(target).await.map_err(fail)?;
            if n != 0 {
                violations.push(format!(
                    "after a crash {when:?}, `{target}` has {n} rows instead of 0"
                ));
            }
        }
        if store.is_done(&run, &f.partition.id).await.map_err(fail)? {
            violations.push(format!(
                "after a crash {when:?}, the partition is recorded done"
            ));
        }
    }

    *crash.lock().unwrap() = Crash::None;
    let Some(lease) = store
        .lease_next(&run, FLOW, Duration::from_secs(60))
        .await
        .map_err(fail)?
    else {
        violations.push("after the crashes, the partition is no longer pending".into());
        return Ok(violations);
    };
    match write_partition(sink.as_ref(), f, &lease).await {
        Ok(_) => {
            for (target, want) in &expected {
                let n = probe.row_count(target).await.map_err(fail)?;
                if n != *want {
                    violations.push(format!(
                        "after commit, `{target}` has {n} rows instead of {want}"
                    ));
                }
            }
            if !store.is_done(&run, &f.partition.id).await.map_err(fail)? {
                violations.push("after commit, the store doesn't see the done-record".into());
            }
        }
        Err(e) => violations.push(format!("a clean commit failed: {e}")),
    }
    Ok(violations)
}

async fn lists_txn_with_state(sink: &dyn Sink) -> std::result::Result<bool, String> {
    Ok(sink
        .capabilities()
        .await
        .map_err(|e| e.to_string())?
        .strategies
        .contains(&IdempotencyStrategy::TxnWithState))
}

async fn write_partition(sink: &dyn Sink, f: &SinkFixture, lease: &Lease) -> Result<WriteSummary> {
    let mut txn = sink
        .begin(&f.partition, lease, IdempotencyStrategy::TxnWithState)
        .await?;
    for (target, batch) in &f.batches {
        txn.write(target, batch.clone()).await?;
    }
    txn.commit().await
}
