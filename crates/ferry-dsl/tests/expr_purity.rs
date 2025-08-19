//! Purity (B2, 0XT-255): the same expression on the same input gives the
//! same output, whether it is compiled again, evaluated again, evaluated a
//! row at a time (any partitioning) or on the rows in another order.
//! Expressions and batches are random.

use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, Decimal128Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_schema::DataType;
use ferry_core::missing::MissingPathsBuilder;
use ferry_core::types::{CanonicalField, CanonicalType, DecimalSpec};
use ferry_dsl::config::{Location, Spanned};
use ferry_dsl::expr::{Compiled, Expression, Scope};
use proptest::prelude::*;

fn scope() -> Scope {
    let decimal = CanonicalType::Decimal(DecimalSpec::new(12, 2).unwrap());
    Scope::record(
        "entity `t`",
        vec![
            CanonicalField::new("a", CanonicalType::Int64, true),
            CanonicalField::new("b", CanonicalType::Int64, true),
            CanonicalField::new("d", decimal, true),
            CanonicalField::new("s", CanonicalType::Utf8, true),
            CanonicalField::new("t", CanonicalType::Utf8, true),
            CanonicalField::new("f", CanonicalType::Bool, true),
        ],
    )
    .tracking_missing(true)
}

/// One row: each value is present, null, or (for `s`) missing.
#[derive(Clone, Debug)]
struct Row {
    a: Option<i64>,
    b: Option<i64>,
    d: Option<i64>,
    s: Option<Option<String>>,
    t: Option<String>,
    f: Option<bool>,
}

fn text() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(String::new()),
        "[a-cA-C ]{1,6}",
        Just("abc".to_owned()),
        Just("Ab".to_owned()),
    ]
}

fn row() -> impl Strategy<Value = Row> {
    (
        proptest::option::of(-1000i64..1000),
        proptest::option::of(-1000i64..1000),
        proptest::option::of(-100_000i64..100_000),
        proptest::option::of(proptest::option::of(text())),
        proptest::option::of(text()),
        proptest::option::of(any::<bool>()),
    )
        .prop_map(|(a, b, d, s, t, f)| Row { a, b, d, s, t, f })
}

fn batch(rows: &[Row]) -> RecordBatch {
    let mut a = Int64Builder::new();
    let mut b = Int64Builder::new();
    let mut d = Decimal128Builder::new().with_data_type(DataType::Decimal128(12, 2));
    let mut s = StringBuilder::new();
    let mut t = StringBuilder::new();
    let mut f = BooleanBuilder::new();
    let mut missing = MissingPathsBuilder::new();
    for row in rows {
        a.append_option(row.a);
        b.append_option(row.b);
        d.append_option(row.d.map(i128::from));
        // `None` is a missing `s`, `Some(None)` an explicit null.
        s.append_option(row.s.clone().flatten());
        t.append_option(row.t.clone());
        f.append_option(row.f);
        let absent: &[&str] = if row.s.is_none() { &["s"] } else { &[] };
        missing.append_row(absent);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(a.finish()),
        Arc::new(b.finish()),
        Arc::new(d.finish()),
        Arc::new(s.finish()),
        Arc::new(t.finish()),
        Arc::new(f.finish()),
        missing.finish(),
    ];
    RecordBatch::try_new(scope().input_schema(), columns).unwrap()
}

fn num() -> impl Strategy<Value = String> {
    let leaf = prop_oneof![
        Just("a".to_owned()),
        Just("b".to_owned()),
        Just("d".to_owned()),
        (0i64..1000).prop_map(|n| n.to_string()),
        (0i64..10_000).prop_map(|n| format!("{}.{:03}", n / 1000, n % 1000)),
    ];
    leaf.prop_recursive(3, 16, 2, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(x, y)| format!("({x} + {y})")),
            (inner.clone(), inner.clone()).prop_map(|(x, y)| format!("({x} - {y})")),
            (inner.clone(), inner.clone()).prop_map(|(x, y)| format!("({x} * {y})")),
            inner.clone().prop_map(|x| format!("abs({x})")),
            inner.clone().prop_map(|x| format!("round({x})")),
            inner.clone().prop_map(|x| format!("round({x}, 1)")),
            (inner.clone(), inner.clone()).prop_map(|(x, y)| format!("coalesce({x}, {y})")),
            (inner.clone(), inner.clone()).prop_map(|(x, y)| format!("greatest({x}, {y})")),
            (inner.clone(), inner)
                .prop_map(|(x, y)| format!("CASE WHEN {x} > {y} THEN {x} ELSE {y} END")),
            Just("length(s)".to_owned()),
        ]
    })
}

fn string() -> impl Strategy<Value = String> {
    let leaf = prop_oneof![
        Just("s".to_owned()),
        Just("t".to_owned()),
        Just("'x'".to_owned()),
        num().prop_map(|n| format!("string({n})")),
    ];
    leaf.prop_recursive(2, 8, 2, |inner| {
        prop_oneof![
            inner.clone().prop_map(|x| format!("lower({x})")),
            inner.clone().prop_map(|x| format!("upper({x})")),
            inner.clone().prop_map(|x| format!("trim({x})")),
            inner.clone().prop_map(|x| format!("substr({x}, 2, 3)")),
            (inner.clone(), inner.clone()).prop_map(|(x, y)| format!("({x} || {y})")),
            (inner.clone(), inner).prop_map(|(x, y)| format!("concat({x}, {y})")),
        ]
    })
}

fn boolean() -> impl Strategy<Value = String> {
    let leaf = prop_oneof![
        (num(), num()).prop_map(|(x, y)| format!("{x} > {y}")),
        (num(), num()).prop_map(|(x, y)| format!("{x} == {y}")),
        num().prop_map(|x| format!("{x} == null")),
        string().prop_map(|x| format!("{x} != null")),
        Just("missing(s)".to_owned()),
        Just("f".to_owned()),
        (string(), string()).prop_map(|(x, y)| format!("{x} = {y}")),
        string().prop_map(|x| format!("{x} like 'a%'")),
        string().prop_map(|x| format!("{x} in ('abc', 'Ab', '')")),
        (num(), num(), num()).prop_map(|(x, l, h)| format!("{x} between {l} and {h}")),
    ];
    leaf.prop_recursive(2, 8, 2, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(x, y)| format!("({x} and {y})")),
            (inner.clone(), inner.clone()).prop_map(|(x, y)| format!("({x} or {y})")),
            inner.prop_map(|x| format!("not ({x})")),
        ]
    })
}

fn expression() -> impl Strategy<Value = String> {
    prop_oneof![num(), string(), boolean()]
}

fn compile(text: &str) -> Compiled {
    let source = Spanned::new(text.to_owned(), Location::new("flows/f.yaml", 1, 1));
    Expression::parse(&source)
        .and_then(|e| e.compile(&scope()))
        .unwrap_or_else(|e| panic!("generated expression doesn't compile: {e}"))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn same_expression_same_input_same_output(
        text in expression(),
        rows in proptest::collection::vec(row(), 1..24),
    ) {
        let input = batch(&rows);
        let first = compile(&text).evaluate(&input);
        let again = compile(&text).evaluate(&input);
        match (&first, &again) {
            (Ok(x), Ok(y)) => prop_assert_eq!(x.to_data(), y.to_data(), "{}", text),
            (Err(x), Err(y)) => prop_assert_eq!(x, y),
            _ => prop_assert!(false, "{text}: one run failed, the other didn't"),
        }

        let Ok(whole) = first else { return Ok(()); };
        let compiled = compile(&text);
        // Any partitioning: a row alone gives the value it had in the batch.
        for i in 0..input.num_rows() {
            let alone = compiled.evaluate(&input.slice(i, 1)).unwrap();
            prop_assert_eq!(alone.to_data(), whole.slice(i, 1).to_data(), "{} row {}", text, i);
        }
        // Row order doesn't matter either.
        let reversed: Vec<u32> = (0..input.num_rows() as u32).rev().collect();
        let reversed = UInt32Array::from(reversed);
        let shuffled = arrow_select::take::take_record_batch(&input, &reversed).unwrap();
        let out = compiled.evaluate(&shuffled).unwrap();
        let back = arrow_select::take::take(&out, &reversed, None).unwrap();
        prop_assert_eq!(back.to_data(), whole.to_data(), "{}", text);
    }
}
