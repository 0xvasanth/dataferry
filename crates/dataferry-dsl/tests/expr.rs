//! The expression language (B2, 0XT-255): parsing, scopes and sigils,
//! null/missing semantics, decimal rounding, strict conversions.

mod common;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray, StructArray,
};
use arrow_schema::Field;
use common::golden;
use dataferry_core::types::{CanonicalField, CanonicalType};
use dataferry_dsl::config::{ErrorKind, FieldPath, FileKind, Location, SourceFile, Spanned};
use dataferry_dsl::expr::{Compiled, ExprError, Expression, INDEX_COLUMN, PARENT_COLUMN, Scope};

fn at(text: &str) -> Spanned<String> {
    Spanned::new(text.to_owned(), Location::new("flows/orders.yaml", 12, 10))
}

fn parse(text: &str) -> Result<Expression, ExprError> {
    Expression::parse(&at(text))
}

fn golden_scope() -> Scope {
    Scope::entity("entity `shop.orders`", &golden::entity())
}

fn compile(text: &str, scope: &Scope) -> Result<Compiled, ExprError> {
    parse(text)?.compile(scope)
}

fn eval_golden(text: &str) -> ArrayRef {
    compile(text, &golden_scope())
        .unwrap_or_else(|e| panic!("{e}"))
        .evaluate(&golden::batch())
        .unwrap()
}

fn bools(array: &ArrayRef) -> Vec<Option<bool>> {
    array.as_boolean().iter().collect()
}

fn ids_where(mask: &ArrayRef) -> Vec<String> {
    let docs = golden::documents();
    bools(mask)
        .iter()
        .zip(&docs)
        .filter(|(keep, _)| **keep == Some(true))
        .map(|(_, d)| golden::id(d))
        .collect()
}

fn expected_ids(file: &str, key: &str) -> Vec<String> {
    let text = std::fs::read_to_string(common::repo(file)).unwrap();
    let rows: Vec<serde_json::Value> = serde_json::from_str(&text).unwrap();
    rows.iter()
        .map(|r| r[key].as_str().unwrap().to_owned())
        .collect()
}

// ---------------------------------------------------------------- null and missing

#[test]
fn equals_null_keeps_null_and_missing_rows_of_the_golden_dataset() {
    let kept = ids_where(&eval_golden("deleted_at == null"));
    let filtered = expected_ids("fixtures/golden/expected/filtered.json", "_id");
    let all: Vec<String> = golden::documents().iter().map(golden::id).collect();
    let expected: Vec<String> = all
        .into_iter()
        .filter(|id| !filtered.contains(id))
        .collect();
    assert_eq!(kept, expected);
    // Not an empty filter (the `= NULL` trap) and both kinds of null kept:
    // …0001 has no `deleted_at`, …0005 has `deleted_at: null`.
    assert_eq!(kept.len(), 9);
    assert!(kept.contains(&"650000000000000000000001".to_owned()));
    assert!(kept.contains(&"650000000000000000000005".to_owned()));
}

#[test]
fn sql_style_equals_null_means_the_same() {
    assert_eq!(
        bools(&eval_golden("deleted_at = null")),
        bools(&eval_golden("deleted_at == null"))
    );
    assert_eq!(
        bools(&eval_golden("null == deleted_at")),
        bools(&eval_golden("deleted_at == null"))
    );
}

#[test]
fn not_equals_null_is_present_and_not_null() {
    assert_eq!(
        ids_where(&eval_golden("deleted_at != null")),
        ["650000000000000000000004"]
    );
    assert_eq!(
        bools(&eval_golden("deleted_at <> null")),
        bools(&eval_golden("deleted_at != null"))
    );
}

#[test]
fn missing_tells_an_absent_field_from_an_explicit_null() {
    assert_eq!(
        ids_where(&eval_golden(
            "deleted_at == null and not missing(deleted_at)"
        )),
        ["650000000000000000000005"]
    );
    assert_eq!(
        ids_where(&eval_golden("missing(items)")),
        ["650000000000000000000003"]
    );
    // …0008's customer has no email; …000a's customer is null, which is
    // not the same as its email being missing.
    assert_eq!(
        ids_where(&eval_golden("missing(customer.email)")),
        ["650000000000000000000008"]
    );
    assert_eq!(
        ids_where(&eval_golden("customer.email == null")),
        ["650000000000000000000008", "65000000000000000000000a"]
    );
}

#[test]
fn missing_is_false_for_a_source_that_cannot_omit_fields() {
    let fields = vec![CanonicalField::new("a", CanonicalType::Int64, true)];
    let scope = Scope::record("table `t`", fields);
    let batch = RecordBatch::try_new(
        scope.input_schema(),
        vec![Arc::new(Int64Array::from(vec![None, Some(1)]))],
    )
    .unwrap();
    let out = compile("missing(a)", &scope)
        .unwrap()
        .evaluate(&batch)
        .unwrap();
    assert_eq!(bools(&out), [Some(false), Some(false)]);
}

#[test]
fn missing_checks_the_path_exists() {
    let err = compile("missing(delted_at)", &golden_scope()).unwrap_err();
    assert!(err.message.contains("no field `delted_at`"), "{err}");
    let err = parse("missing(lower(status))").unwrap_err();
    assert!(err.message.contains("takes a path"), "{err}");
}

#[test]
fn ordering_against_the_null_literal_is_an_error() {
    let err = parse("deleted_at > null").unwrap_err();
    assert!(err.message.contains("never true"), "{err}");
    assert!(err.help.as_deref().unwrap().contains("== null"));
}

#[test]
fn a_comparison_with_a_null_value_is_null_and_filters_out() {
    // …000a has customer null: `customer.id != 'c1'` is null there, not true.
    let out = eval_golden("customer.id != 'c1'");
    let ids = ids_where(&out);
    assert!(!ids.contains(&"65000000000000000000000a".to_owned()));
    assert_eq!(bools(&out)[9], None);
}

// ---------------------------------------------------------------- decimals

fn decimal_values(array: &ArrayRef) -> (Vec<Option<i128>>, i8) {
    let decimals = array.as_primitive::<Decimal128Type>();
    (decimals.iter().collect(), decimals.scale())
}

#[test]
fn round_of_a_decimal_literal_is_half_up() {
    let one_row = golden::batch().slice(0, 1);
    let eval = |text: &str| {
        compile(text, &golden_scope())
            .unwrap_or_else(|e| panic!("{e}"))
            .evaluate(&one_row)
            .unwrap()
    };
    assert_eq!(
        decimal_values(&eval("round(1.005 * 100)")),
        (vec![Some(101)], 0)
    );
    assert_eq!(
        decimal_values(&eval("round(-1.005 * 100)")),
        (vec![Some(-101)], 0)
    );
    assert_eq!(decimal_values(&eval("round(2.5)")), (vec![Some(3)], 0));
    assert_eq!(
        decimal_values(&eval("round(1.005, 2)")),
        (vec![Some(101)], 2)
    );
    // The float route is exactly what decimals avoid: 1.005 as a float is
    // 1.00499999…, so it rounds down.
    let float = eval("round(float(1.005) * 100)");
    assert_eq!(
        float
            .as_primitive::<arrow_array::types::Float64Type>()
            .value(0),
        100.0
    );
}

#[test]
fn round_of_a_decimal_column_matches_the_golden_cents() {
    let cents = eval_golden("int(round(total * 100))");
    let cents = cents.as_primitive::<Int64Type>();
    let expected_text =
        std::fs::read_to_string(common::repo("fixtures/golden/expected/orders.json")).unwrap();
    let expected: Vec<serde_json::Value> = serde_json::from_str(&expected_text).unwrap();
    let docs = golden::documents();
    for row in &expected {
        let i = docs
            .iter()
            .position(|d| golden::id(d) == row["id"].as_str().unwrap())
            .unwrap();
        assert_eq!(
            cents.value(i),
            row["total_cents"].as_i64().unwrap(),
            "{}",
            row["id"]
        );
    }
    // …0007 is the 1.005 case.
    assert_eq!(cents.value(6), 101);
}

#[test]
fn decimal_arithmetic_stays_decimal() {
    let compiled = compile("total * 100", &golden_scope()).unwrap();
    assert!(matches!(
        compiled.output_type(),
        Some(CanonicalType::Decimal(_))
    ));
    let compiled = compile("1.5 + 1", &golden_scope()).unwrap();
    assert!(matches!(
        compiled.output_type(),
        Some(CanonicalType::Decimal(_))
    ));
}

// ---------------------------------------------------------------- strict conversions

#[test]
fn a_failed_conversion_fails_its_own_row_only() {
    let scope = Scope::record(
        "element of `items`",
        vec![CanonicalField::new("qty", CanonicalType::Utf8, true)],
    );
    let batch = RecordBatch::try_new(
        scope.input_schema(),
        vec![Arc::new(StringArray::from(vec![
            Some("1"),
            Some("three"),
            None,
        ]))],
    )
    .unwrap();
    let compiled = compile("int(qty)", &scope).unwrap();
    let err = compiled.evaluate(&batch).unwrap_err();
    assert!(err.0.contains("`three` is not an integer"), "{err}");
    let (values, errors) = compiled.evaluate_rows(&batch);
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].row, 1);
    let values = values.as_primitive::<Int64Type>();
    assert_eq!(values.iter().collect::<Vec<_>>(), [Some(1), None, None]);
}

#[test]
fn int_of_a_fraction_is_an_error_not_a_truncation() {
    let one_row = golden::batch().slice(6, 1);
    let compiled = compile("int(total * 100)", &golden_scope()).unwrap();
    let err = compiled.evaluate(&one_row).unwrap_err();
    assert!(err.0.contains("not a whole number"), "{err}");
}

// ---------------------------------------------------------------- reserved names and sigils

#[test]
fn a_reserved_name_without_its_sigil_is_a_parse_error() {
    for (text, sigil) in [
        ("index", "$index"),
        ("index + 1", "$index"),
        ("parent.id", "$parent"),
        ("item", "$item"),
        ("lower(key)", "$key"),
    ] {
        let err = parse(text).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Expr, "{text}");
        assert!(err.message.contains("reserved"), "{text}: {err}");
        let help = err.help.as_deref().unwrap();
        assert!(help.contains(sigil), "{text}: {help}");
        assert!(help.contains("for a field named"), "{text}: {help}");
    }
}

#[test]
fn a_quoted_reserved_word_is_a_field() {
    let scope = Scope::record(
        "table `t`",
        vec![CanonicalField::new("index", CanonicalType::Int64, true)],
    );
    let batch = RecordBatch::try_new(
        scope.input_schema(),
        vec![Arc::new(Int64Array::from(vec![7]))],
    )
    .unwrap();
    let out = compile("\"index\" + 1", &scope)
        .unwrap()
        .evaluate(&batch)
        .unwrap();
    assert_eq!(out.as_primitive::<Int64Type>().value(0), 8);
    // Only the head of a path is reserved: a struct field may be `key`.
    assert!(parse("customer.key").is_ok());
}

#[test]
fn sigils_read_the_engine_columns() {
    let parent = vec![CanonicalField::new("id", CanonicalType::Utf8, false)];
    let element =
        CanonicalType::Struct(vec![CanonicalField::new("sku", CanonicalType::Utf8, true)]);
    let scope = Scope::element("element of `items`", element).with_parent(parent);
    let schema = scope.input_schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        [
            "sku",
            PARENT_COLUMN,
            INDEX_COLUMN,
            "_ferry_item",
            "_ferry_key"
        ]
    );

    let sku: ArrayRef = Arc::new(StringArray::from(vec!["A", "B"]));
    let parent_struct = StructArray::from(vec![(
        Arc::new(Field::new("id", arrow_schema::DataType::Utf8, false)),
        Arc::new(StringArray::from(vec!["o1", "o1"])) as ArrayRef,
    )]);
    let key = arrow_array::FixedSizeBinaryArray::try_from_iter([[1u8; 16], [2u8; 16]].into_iter())
        .unwrap();
    let item = StructArray::from(vec![(
        Arc::new(Field::new("sku", arrow_schema::DataType::Utf8, true)),
        sku.clone(),
    )]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            sku,
            Arc::new(parent_struct),
            Arc::new(Int64Array::from(vec![0, 1])),
            Arc::new(item),
            Arc::new(key),
        ],
    )
    .unwrap();
    let eval = |text: &str| compile(text, &scope).unwrap().evaluate(&batch).unwrap();
    let ids = eval("$parent.id || '-' || string($index)");
    assert_eq!(ids.as_string::<i32>().value(1), "o1-1");
    let item_sku = eval("$item.sku");
    assert_eq!(item_sku.as_string::<i32>().value(0), "A");
    assert_eq!(
        eval("string($key)").as_string::<i32>().value(0),
        "01010101-0101-0101-0101-010101010101"
    );
}

#[test]
fn a_sigil_outside_its_target_is_an_error() {
    let err = compile("$index", &golden_scope()).unwrap_err();
    assert!(
        err.message.contains("only available in `unnest:` targets"),
        "{err}"
    );
    let err = compile("$parent.id", &golden_scope()).unwrap_err();
    assert!(err.message.contains("targets with `parent:`"), "{err}");
    let err = parse("$idx + 1").unwrap_err();
    assert!(err.message.contains("`$idx` is not a sigil"), "{err}");
    assert!(err.help.unwrap().contains("$parent, $index, $item, $key"));
}

#[test]
fn engine_reserved_names_cannot_be_read_directly() {
    let err = parse("_ferry_missing").unwrap_err();
    assert!(err.message.contains("reserved for the engine"), "{err}");
    assert!(err.help.unwrap().contains("missing("));
}

// ---------------------------------------------------------------- paths

#[test]
fn paths_reach_into_structs_and_arrays() {
    let ids = eval_golden("customer.id");
    assert_eq!(ids.as_string::<i32>().value(0), "c1");
    assert!(ids.is_null(9));
    let skus = eval_golden("items[*].sku");
    let first = skus.as_list::<i32>().value(0);
    assert_eq!(
        first.as_string::<i32>().iter().collect::<Vec<_>>(),
        [Some("SKU-1"), Some("SKU-2")]
    );
    assert!(skus.is_null(2), "…0003 has no items");
    // The golden config's item count: a missing array counts as none.
    let counts = eval_golden("int(coalesce(cardinality(items[*].sku), 0))");
    assert_eq!(
        counts.as_primitive::<Int64Type>().values().to_vec(),
        [2, 0, 0, 1, 1, 2, 1, 1, 1, 1]
    );
}

#[test]
fn a_misspelled_path_suggests_the_nearest_field() {
    let err = compile("lower(stauts)", &golden_scope()).unwrap_err();
    assert_eq!(err.message, "no field `stauts` in entity `shop.orders`");
    assert_eq!(err.help.as_deref(), Some("did you mean `status`?"));
    assert_eq!(err.span, Some((6, 12)));

    let err = compile("customer.emial", &golden_scope()).unwrap_err();
    assert_eq!(err.message, "`customer` has no field `emial`");
    assert_eq!(err.help.as_deref(), Some("did you mean `customer.email`?"));

    let err = compile("items.sku", &golden_scope()).unwrap_err();
    assert!(err.help.unwrap().contains("items[*].sku"));
    let err = compile("status.x", &golden_scope()).unwrap_err();
    assert_eq!(err.message, "`status` is utf8, it has no fields");
    let err = compile("status[*]", &golden_scope()).unwrap_err();
    assert_eq!(err.message, "`status` is utf8, not an array");
    let err = parse("items[0].sku").unwrap_err();
    assert!(err.message.contains("indexing"), "{err}");
}

#[test]
fn merge_scopes_resolve_aliases() {
    let customers = vec![
        CanonicalField::new("id", CanonicalType::Int64, false),
        CanonicalField::new("name", CanonicalType::Utf8, true),
    ];
    let orders = vec![
        CanonicalField::new("id", CanonicalType::Int64, false),
        CanonicalField::new("status", CanonicalType::Utf8, true),
    ];
    let scope = Scope::record("entity `app.public.orders`", orders)
        .with_driver_alias("o")
        .with_join("c", customers)
        .with_many("i");
    let schema = scope.input_schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["id", "status", "_ferry_join_c"]);

    let joined = StructArray::new(
        match schema.field(2).data_type() {
            arrow_schema::DataType::Struct(f) => f.clone(),
            _ => unreachable!(),
        },
        vec![
            Arc::new(Int64Array::from(vec![1, 0])),
            Arc::new(StringArray::from(vec![Some("Ana"), None])),
        ],
        Some(vec![true, false].into()),
    );
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![10, 13])),
            Arc::new(StringArray::from(vec!["paid", "pending"])),
            Arc::new(joined),
        ],
    )
    .unwrap();
    let eval = |text: &str| compile(text, &scope).unwrap().evaluate(&batch).unwrap();
    let names = eval("c.name");
    assert_eq!(names.as_string::<i32>().value(0), "Ana");
    assert!(names.is_null(1), "no customer matched");
    assert_eq!(bools(&eval("o.status == status")), [Some(true), Some(true)]);

    let err = compile("i.qty", &scope).unwrap_err();
    assert!(err.message.contains("`many` join"), "{err}");
    let err = compile("c", &scope).unwrap_err();
    assert!(err.message.contains("alias, not a value"), "{err}");
    let err = compile("c.nme", &scope).unwrap_err();
    assert_eq!(err.help.as_deref(), Some("did you mean `c.name`?"));
}

// ---------------------------------------------------------------- what isn't the language

#[test]
fn impure_and_non_row_constructs_are_rejected() {
    for (text, says) in [
        ("now()", "pure"),
        ("random() > 0.5", "pure"),
        ("sum(total)", "aggregates"),
        ("(select 1)", "subquery"),
        ("lowr(status)", "did you mean `lower`?"),
        ("count(*)", "aggregates"),
    ] {
        let err = parse(text).unwrap_err();
        let shown = format!("{} {}", err.message, err.help.clone().unwrap_or_default());
        assert!(shown.contains(says), "{text}: {shown}");
    }
}

#[test]
fn parse_errors_point_at_the_config_file() {
    let source = SourceFile {
        path: "flows/orders.yaml".into(),
        absolute: "/x/ferry/flows/orders.yaml".into(),
        kind: FileKind::Flows,
        text: format!("{}          status: lower(status\n", "\n".repeat(11)),
    };
    let err = Expression::parse(&Spanned::new(
        "lower(status".into(),
        Location::new("flows/orders.yaml", 12, 19),
    ))
    .unwrap_err();
    assert!(err.message.starts_with("syntax error: expected"), "{err}");
    let config_error = err.to_config_error(
        &FieldPath::root().key("flows").item(0, Some("orders")),
        std::slice::from_ref(&source),
    );
    let location = config_error.location.unwrap();
    assert_eq!((location.line, location.column), (12, 31));
    assert_eq!(config_error.path, "flows[orders]");

    let err = parse("status status").unwrap_err();
    assert!(err.message.contains("unexpected"), "{err}");
    assert_eq!(err.span, Some((7, 13)));
}

#[test]
fn a_batch_with_other_columns_is_refused() {
    let compiled = compile("status", &golden_scope()).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "status",
            arrow_schema::DataType::Utf8,
            true,
        )])),
        vec![Arc::new(StringArray::from(vec!["x"]))],
    )
    .unwrap();
    assert!(compiled.evaluate(&batch).is_err());
}

#[test]
fn boolean_results_are_boolean_arrays() {
    let out = eval_golden("status in ('PAID', 'PENDING') and total between 1 and 20");
    let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
    // Totals 12.00, 9.00, 7.25, 8.00, 1.005, 3.00 and 4.00 are in range;
    // the 3.00 one is `Shipped`.
    assert_eq!(out.true_count(), 6);
}

#[test]
fn sql_forms_that_plan_to_functions_work() {
    let first = |text: &str| eval_golden(text).as_string::<i32>().value(0).to_owned();
    assert_eq!(first("substring(status from 2 for 2)"), "AI");
    assert_eq!(first("substr(status, 1, 2)"), "PA");
    assert_eq!(first("trim('  ' || status || ' ')"), "PAID");
    assert_eq!(
        first("CASE WHEN position('I' in status) = 3 THEN 'yes' ELSE 'no' END"),
        "yes"
    );
    assert_eq!(first("lower(status) || '/' || customer.id"), "paid/c1");
}

#[test]
fn like_and_ilike_honour_the_backslash_escape() {
    let docs = golden::documents();
    let status =
        |d: &serde_json::Value| d.get("status").and_then(|s| s.as_str()).map(str::to_owned);
    let ids = |keep: &dyn Fn(&str) -> bool| -> Vec<String> {
        docs.iter()
            .filter(|d| status(d).is_some_and(|s| keep(&s)))
            .map(golden::id)
            .collect()
    };
    // `\%` is a literal percent sign: only `PAID` followed by `%` matches,
    // not every status that starts with `PAID`.
    assert_eq!(
        ids_where(&eval_golden(r"(status || '%') like 'PAID\%' escape '\'")),
        ids(&|s| s == "PAID")
    );
    assert_eq!(
        ids_where(&eval_golden(r"(status || 'x') like 'PAID\%' escape '\'")),
        Vec::<String>::new()
    );
    // Backslash is also the implicit escape, so the clause can be left out.
    assert_eq!(
        ids_where(&eval_golden(r"(status || '%') like 'PAID\%'")),
        ids(&|s| s == "PAID")
    );
    // An unescaped `%` is the wildcard.
    assert_eq!(
        ids_where(&eval_golden("(status || 'x') like 'PAID%'")),
        ids(&|s| s == "PAID")
    );
    assert_eq!(
        ids_where(&eval_golden(r"status ilike 'p\_%' escape '\'")),
        Vec::<String>::new()
    );
    assert_eq!(
        ids_where(&eval_golden("status ilike 'p%'")),
        ids(&|s| s.to_lowercase().starts_with('p'))
    );
    // DataFusion only escapes with a backslash; any other character is
    // refused when the expression compiles, not at run time.
    let err = compile("status like 'PAID!%' escape '!'", &golden_scope()).unwrap_err();
    assert!(err.to_string().contains("escape"), "{err}");
    // The pattern is an expression and resolves like one: a misspelled
    // field in it is still an error.
    assert!(compile("status like custmer.id", &golden_scope()).is_err());
    assert_eq!(
        ids_where(&eval_golden("lower(status) like lower(status)")),
        ids(&|_| true)
    );
}
