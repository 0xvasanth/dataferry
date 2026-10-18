//! A sink this binary can't read back is reported unverified (W0701),
//! while every other sink of the same run is still verified (S3). The
//! `scratch` type is registered in-process through `Ferry`, the way a
//! third-party binary adds a connector, and keeps rows in memory.

mod common;

use std::ffi::OsString;
use std::sync::Arc;

use common::{Shop, Warehouse, edit, golden_copy};
use dataferry::Ferry;
use dataferry::registry::{BuildContext, BuildError, Connector, Endpoints, Roles};
use dataferry_core::IdempotencyStrategy;
use dataferry_core::testkit::MemSink;
use serde_json::Value;

/// A sink in memory: no verifier is registered for it.
struct Scratch;

#[derive(serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct ScratchOptions {}

impl Connector for Scratch {
    const TYPE: &'static str = "scratch";
    const ROLES: Roles = Roles::SINK;
    type Options = ScratchOptions;

    fn build(
        &self,
        _: &BuildContext<'_>,
        _: ScratchOptions,
        wants: Roles,
    ) -> Result<Endpoints, BuildError> {
        Ok(Endpoints {
            sink: wants.sink.then(|| {
                Arc::new(MemSink::new(vec![
                    IdempotencyStrategy::DeletePartitionThenInsert,
                ])) as Arc<dyn dataferry_core::Sink>
            }),
            ..Endpoints::default()
        })
    }

    fn describe(&self, _: &ScratchOptions) -> String {
        "rows in memory".into()
    }
}

#[test]
fn only_the_sink_with_no_verifier_is_reported_unverified() {
    let containers = tokio::runtime::Runtime::new().unwrap();
    let (shop, warehouse) =
        containers.block_on(async { tokio::join!(Shop::golden(), Warehouse::start()) });
    let dir = golden_copy();
    let ferry_dir = dir.path().join("ferry");
    // `run_with` reads the process environment, which a test can't set
    // safely, so the URLs are written in. The binary warns about the
    // literal secret; that is not under test.
    let connections = ferry_dir.join("connections.yaml");
    edit(&connections, "${FERRY_GOLDEN_MONGO_URL}", &shop.url);
    edit(&connections, "${FERRY_GOLDEN_PG_URL}", &warehouse.url);
    let text = std::fs::read_to_string(&connections).unwrap();
    std::fs::write(
        &connections,
        format!("{text}\n  - name: scratch\n    type: scratch\n"),
    )
    .unwrap();
    std::fs::write(
        ferry_dir.join("flows/scratch.yaml"),
        "flows:\n  - name: statuses\n    from: shop.orders\n    sink: scratch\n    to:\n      order_status:\n        key: [id]\n        columns:\n          id: string(_id)\n          status: lower(status)\n",
    )
    .unwrap();

    let args: Vec<OsString> = ["ferry", "--config"]
        .into_iter()
        .map(OsString::from)
        .chain([ferry_dir.clone().into_os_string(), "run".into()])
        .collect();
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let code = Ferry::with_builtins()
        .register(Scratch)
        .run_with(args, &mut out, &mut err);
    let (out, err) = (
        String::from_utf8(out).unwrap(),
        String::from_utf8(err).unwrap(),
    );
    assert_eq!(code, std::process::ExitCode::SUCCESS, "{out}\n{err}");
    assert!(
        err.contains(
            "warning[W0701]: not verified: this binary can't read `scratch` connections back, \
             so `scratch` isn't verified"
        ),
        "{err}"
    );
    assert_eq!(err.matches("W0701").count(), 1, "{err}");
    assert!(
        out.contains("verification of `warehouse` passed; report: "),
        "{out}"
    );
    let run = out
        .lines()
        .next()
        .and_then(|l| l.strip_prefix("run "))
        .unwrap()
        .to_owned();
    // Two sinks, so the warehouse's report is named for it, and the
    // unverified sink has none.
    let reports = dir.path().join(".ferry/runs").join(&run);
    let report: Value = serde_json::from_str(
        &std::fs::read_to_string(reports.join("report-warehouse.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(report["status"], "passed", "{report:#}");
    assert_eq!(report["counts"]["read"], 10);
    assert!(!reports.join("report-scratch.json").exists());

    containers.block_on(async move {
        drop(shop);
        drop(warehouse);
    });
}
