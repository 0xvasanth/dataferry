//! Compiling configs for the plan tests, and reading what plans write back
//! as JSON rows to compare with `fixtures/golden/expected`.

use std::path::Path;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type, TimestampMicrosecondType};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use ferry_core::Entity;
use ferry_core::types::{CanonicalField, CanonicalType};
use ferry_dsl::compile::{CompiledFlow, Transformed, compile};
use ferry_dsl::config::{ConfigErrors, load};
use ferry_dsl::typecheck::{Catalog, check};
use serde_json::{Map, Value};

use super::{golden, repo};

/// A `ferry/` directory with one flow file, `flows/f.yaml`.
pub fn project(flows: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let write = |path: &str, text: &str| {
        let path = dir.path().join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    };
    write("ferry.yaml", "version: 1\nname: t\n");
    write(
        "connections.yaml",
        "connections:\n  - name: shop\n    type: mongo\n    url: x\n  - name: warehouse\n    type: postgres\n    url: y\n",
    );
    write("flows/f.yaml", flows);
    dir
}

pub fn golden_catalog() -> Catalog {
    Catalog::new().with("shop", golden::entity())
}

/// The entities `examples/pg-merge` reads, as Postgres discovers them.
pub fn pg_merge_catalog() -> Catalog {
    let utc = CanonicalType::Timestamp {
        tz: Some("UTC".into()),
    };
    let f = |n: &str, t: CanonicalType, null: bool| CanonicalField::new(n, t, null);
    let customers = Entity::new(
        "public.customers",
        vec![
            f("id", CanonicalType::Int64, false),
            f("name", CanonicalType::Utf8, false),
            f("email", CanonicalType::Utf8, true),
        ],
        vec!["id".into()],
    )
    .unwrap();
    let orders = Entity::new(
        "public.orders",
        vec![
            f("id", CanonicalType::Int64, false),
            f("customer_id", CanonicalType::Int64, true),
            f("status", CanonicalType::Utf8, false),
            f("placed_at", utc, false),
            f("total_cents", CanonicalType::Int64, false),
        ],
        vec!["id".into()],
    )
    .unwrap();
    let items = Entity::new(
        "public.order_items",
        vec![
            f("order_id", CanonicalType::Int64, false),
            f("line_no", CanonicalType::Int32, false),
            f("sku", CanonicalType::Utf8, false),
            f("qty", CanonicalType::Int32, false),
            f("unit_cents", CanonicalType::Int64, false),
        ],
        vec!["order_id".into(), "line_no".into()],
    )
    .unwrap();
    Catalog::new()
        .with("app", customers)
        .with("app", orders)
        .with("app", items)
}

/// Loads, type-checks and compiles every flow under `dir`. Load and type
/// errors panic: these tests are about the compiler.
pub fn compile_dir(dir: &Path, catalog: &Catalog) -> Result<Vec<CompiledFlow>, ConfigErrors> {
    let config = load(dir).unwrap_or_else(|e| panic!("{e}"));
    let checked = check(&config, catalog).unwrap_or_else(|e| panic!("{e}"));
    let mut out = Vec::new();
    let mut errors = Vec::new();
    for (flow, checked) in config.flows.iter().zip(&checked) {
        match compile(flow, checked, catalog) {
            Ok(c) => out.push(c),
            Err(e) => errors.extend(e.0),
        }
    }
    if errors.is_empty() {
        Ok(out)
    } else {
        Err(ConfigErrors(errors))
    }
}

/// The one flow of `flows`, compiled against `catalog`.
pub fn compile_one(flows: &str, catalog: &Catalog) -> CompiledFlow {
    let dir = project(flows);
    let mut flows = compile_dir(dir.path(), catalog).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(flows.len(), 1);
    flows.remove(0)
}

/// The golden flow, compiled from `examples/golden`.
pub fn golden_flow() -> CompiledFlow {
    let mut flows = compile_dir(&repo("examples/golden/ferry"), &golden_catalog())
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(flows.len(), 1);
    flows.remove(0)
}

/// The rows a transform wrote to `target`, as JSON.
pub fn target_rows(out: &Transformed, target: &str) -> Vec<Value> {
    out.output
        .targets
        .iter()
        .filter(|t| t.target == target)
        .flat_map(|t| json_rows(&t.batch))
        .collect()
}

/// `fixtures/golden/expected/<name>.json`.
pub fn expected(name: &str) -> Vec<Value> {
    let path = repo(&format!("fixtures/golden/expected/{name}.json"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// `rows` sorted by the text of their `keys` columns.
pub fn sorted(mut rows: Vec<Value>, keys: &[&str]) -> Vec<Value> {
    rows.sort_by_key(|r| keys.iter().map(|k| r[k].to_string()).collect::<Vec<_>>());
    rows
}

/// Every row of `batch` as a JSON object, normalized the way the expected
/// files are written: timestamps as RFC 3339 UTC with microseconds, UUIDs
/// hyphenated.
pub fn json_rows(batch: &RecordBatch) -> Vec<Value> {
    let schema = batch.schema();
    (0..batch.num_rows())
        .map(|row| {
            let mut object = Map::new();
            for (field, column) in schema.fields().iter().zip(batch.columns()) {
                object.insert(field.name().clone(), json_value(column.as_ref(), row));
            }
            Value::Object(object)
        })
        .collect()
}

fn json_value(array: &dyn Array, row: usize) -> Value {
    if array.is_null(row) {
        return Value::Null;
    }
    match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().value(row).into(),
        DataType::Int64 => array.as_primitive::<Int64Type>().value(row).into(),
        DataType::Int32 => array.as_primitive::<Int32Type>().value(row).into(),
        DataType::Boolean => array.as_boolean().value(row).into(),
        DataType::FixedSizeBinary(16) => {
            let bytes = array.as_fixed_size_binary().value(row);
            uuid::Uuid::from_slice(bytes).unwrap().to_string().into()
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            rfc3339(array.as_primitive::<TimestampMicrosecondType>().value(row)).into()
        }
        DataType::List(_) => {
            let items = array.as_list::<i32>().value(row);
            Value::Array((0..items.len()).map(|i| json_value(&items, i)).collect())
        }
        DataType::Struct(fields) => {
            let structs = array.as_struct();
            let mut object = Map::new();
            for (field, column) in fields.iter().zip(structs.columns()) {
                object.insert(field.name().clone(), json_value(column.as_ref(), row));
            }
            Value::Object(object)
        }
        other => panic!("no JSON form for {other} in these tests"),
    }
}

/// `2026-03-01T10:00:00.000000Z`.
fn rfc3339(micros: i64) -> String {
    let secs = micros.div_euclid(1_000_000);
    let frac = micros.rem_euclid(1_000_000);
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    // Civil from days (Howard Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{frac:06}Z",
        tod / 3600,
        tod % 3600 / 60,
        tod % 60
    )
}

/// A batch of `entity` from JSON rows, with `missing[i]` the paths row `i`
/// omits (for an entity that tracks them).
pub fn batch_of(entity: &Entity, rows: &[Value], missing: &[&[&str]]) -> RecordBatch {
    use arrow_schema::Schema;
    use ferry_core::missing::MissingPathsBuilder;
    use std::sync::Arc;

    let fields: Vec<_> = entity.fields().iter().map(|f| f.to_arrow()).collect();
    let text = rows
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    let mut reader = arrow_json::ReaderBuilder::new(Arc::new(Schema::new(fields)))
        .with_batch_size(rows.len().max(1))
        .build(text.as_bytes())
        .unwrap();
    let values = reader.next().unwrap().unwrap();
    let mut columns = values.columns().to_vec();
    if entity.tracks_missing() {
        let mut builder = MissingPathsBuilder::new();
        for i in 0..rows.len() {
            builder.append_row(missing.get(i).copied().unwrap_or(&[]));
        }
        columns.push(builder.finish());
    }
    RecordBatch::try_new(entity.arrow_schema(), columns).unwrap()
}

pub fn list_of(item: CanonicalType) -> CanonicalType {
    CanonicalType::List(Box::new(CanonicalField::new("item", item, true)))
}

/// A compiled flow as ferry-runtime runs it, wired the way the planner
/// (B7) does: the driver and every co-partitioned join as inputs (in the
/// plan's input order), and every staged join's source given through
/// `stage_with`. `source` is the source of a connection.
pub fn runtime_flow(
    compiled: &CompiledFlow,
    source: impl Fn(&str) -> std::sync::Arc<dyn ferry_core::Source>,
    driver_connection: &str,
    sink: std::sync::Arc<dyn ferry_core::Sink>,
    staging_memory: u64,
) -> ferry_runtime::Flow {
    use ferry_dsl::compile::{Staging, Strategy};
    use ferry_runtime::Input;

    let plan = &compiled.plan;
    let mut inputs = vec![Input {
        source: source(driver_connection),
        entity: plan.entity().clone(),
        co_partition: None,
    }];
    let mut staged = std::collections::BTreeMap::new();
    for join in plan.joins() {
        match &join.strategy {
            Strategy::CoPartition { column } => inputs.push(Input {
                source: source(&join.connection),
                entity: join.entity.clone(),
                co_partition: Some(column.clone()),
            }),
            Strategy::Stage => {
                staged.insert(join.alias.clone(), source(&join.connection));
            }
        }
    }
    plan.stage_with(Staging {
        sources: staged,
        memory: staging_memory,
        dir: None,
    })
    .unwrap();
    ferry_runtime::Flow {
        name: compiled.name.clone(),
        inputs,
        plan: std::sync::Arc::new(plan.clone()),
        sink,
        target: compiled.target.clone(),
        finalizer: None,
        mode: compiled.mode,
        depends_on: Vec::new(),
    }
}
