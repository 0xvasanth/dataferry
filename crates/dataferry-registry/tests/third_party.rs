//! A connector defined outside the crate, the way a third party would write
//! one: it registers through the public API only and builds working
//! endpoints (dataferry-core's in-memory source and sink stand in for a
//! database).

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use dataferry_core::testkit::{MemSink, MemSource};
use dataferry_core::types::{CanonicalField, CanonicalType};
use dataferry_core::{
    Entity, Epoch, FerryError, IdempotencyStrategy, Lease, PartitionHint, TargetSchema,
    TargetTable, WriteMode,
};
use dataferry_registry::{
    BuildContext, BuildError, Connector, ConnectorRegistry, Endpoints, Finalized, Finalizer,
    RawConnection, RegisterError, RenderDdl, Roles, Secret,
};
use futures::TryStreamExt;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

fn orders_entity() -> Entity {
    Entity::new(
        "orders",
        vec![
            CanonicalField::new("id", CanonicalType::Int64, false),
            CanonicalField::new("status", CanonicalType::Utf8, true),
        ],
        vec!["id".into()],
    )
    .unwrap()
}

fn orders_rows(n: i64) -> RecordBatch {
    let ids: Vec<i64> = (1..=n).collect();
    let id: ArrayRef = Arc::new(Int64Array::from(ids));
    let status: ArrayRef = Arc::new(StringArray::from(vec!["paid"; n as usize]));
    RecordBatch::try_new(orders_entity().arrow_schema(), vec![id, status]).unwrap()
}

/// An inventory service: reads `orders`, and can be written to.
struct Inventory;

/// An inventory service.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct InventoryOptions {
    /// Service endpoint.
    url: Secret,
    /// How many seeded orders the source holds.
    #[serde(default = "default_orders")]
    orders: i64,
}

fn default_orders() -> i64 {
    3
}

impl Connector for Inventory {
    const TYPE: &'static str = "inventory";
    const ROLES: Roles = Roles::BOTH;
    type Options = InventoryOptions;

    fn build(
        &self,
        _: &BuildContext<'_>,
        options: InventoryOptions,
        wants: Roles,
    ) -> Result<Endpoints, BuildError> {
        if !options.url.expose().starts_with("https://") {
            return Err(BuildError::option("url", "must be an https:// URL"));
        }
        if options.orders < 0 {
            return Err(BuildError::option("orders", "must not be negative"));
        }
        let mut endpoints = Endpoints::default();
        if wants.source {
            let source = MemSource::new()
                .with_entity(orders_entity(), orders_rows(options.orders))
                .map_err(BuildError::Other)?;
            endpoints.source = Some(Arc::new(source));
        }
        if wants.sink {
            endpoints.sink = Some(Arc::new(MemSink::new(vec![
                IdempotencyStrategy::DeletePartitionThenInsert,
            ])));
        }
        Ok(endpoints)
    }

    fn describe(&self, options: &InventoryOptions) -> String {
        format!("inventory with {} orders", options.orders)
    }
}

/// Read-only, like Mongo before S3.
struct Archive;

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArchiveOptions {}

impl Connector for Archive {
    const TYPE: &'static str = "archive";
    const ROLES: Roles = Roles::SOURCE;
    type Options = ArchiveOptions;

    fn build(
        &self,
        _: &BuildContext<'_>,
        _: ArchiveOptions,
        _: Roles,
    ) -> Result<Endpoints, BuildError> {
        Ok(Endpoints {
            source: Some(Arc::new(MemSource::new())),
            sink: None,
            finalizer: None,
            ddl: None,
        })
    }

    fn describe(&self, _: &ArchiveOptions) -> String {
        "archive".into()
    }
}

/// Ignores `wants` and always returns both halves.
struct Greedy;

impl Connector for Greedy {
    const TYPE: &'static str = "greedy";
    const ROLES: Roles = Roles::BOTH;
    type Options = ArchiveOptions;

    fn build(
        &self,
        _: &BuildContext<'_>,
        _: ArchiveOptions,
        _: Roles,
    ) -> Result<Endpoints, BuildError> {
        Ok(Endpoints {
            source: Some(Arc::new(MemSource::new())),
            sink: Some(Arc::new(MemSink::new(Vec::new()))),
            finalizer: None,
            ddl: None,
        })
    }

    fn describe(&self, _: &ArchiveOptions) -> String {
        "greedy".into()
    }
}

/// A source-only connector that also returns a finalizer, which can only
/// restore what a sink's `prepare` changed.
struct StrayFinalizer;

/// A source-only connector that also says how it would render DDL.
struct StrayDdl;

struct Nothing;

impl RenderDdl for Nothing {
    fn render_ddl(&self, _: &TargetSchema, _: WriteMode) -> Vec<String> {
        Vec::new()
    }
}

impl Connector for StrayDdl {
    const TYPE: &'static str = "stray_ddl";
    const ROLES: Roles = Roles::SOURCE;
    type Options = ArchiveOptions;

    fn build(
        &self,
        _: &BuildContext<'_>,
        _: ArchiveOptions,
        _: Roles,
    ) -> Result<Endpoints, BuildError> {
        Ok(Endpoints {
            source: Some(Arc::new(MemSource::new())),
            ddl: Some(Arc::new(Nothing)),
            ..Endpoints::default()
        })
    }

    fn describe(&self, _: &ArchiveOptions) -> String {
        "stray".into()
    }
}

struct NoOp;

#[async_trait::async_trait]
impl Finalizer for NoOp {
    async fn finalize(&self, _: &TargetSchema) -> dataferry_core::Result<Finalized> {
        Ok(Finalized::default())
    }
}

impl Connector for StrayFinalizer {
    const TYPE: &'static str = "stray";
    const ROLES: Roles = Roles::SOURCE;
    type Options = ArchiveOptions;

    fn build(
        &self,
        _: &BuildContext<'_>,
        _: ArchiveOptions,
        _: Roles,
    ) -> Result<Endpoints, BuildError> {
        Ok(Endpoints {
            source: Some(Arc::new(MemSource::new())),
            sink: None,
            finalizer: Some(Arc::new(NoOp)),
            ddl: None,
        })
    }

    fn describe(&self, _: &ArchiveOptions) -> String {
        "stray".into()
    }
}

fn conn(name: &str, r#type: &str, fields: serde_json::Value) -> RawConnection {
    let serde_json::Value::Object(fields) = fields else {
        panic!("fields must be an object")
    };
    RawConnection {
        name: name.into(),
        r#type: r#type.into(),
        fields,
    }
}

fn registry() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();
    registry.register(Inventory).register(Archive);
    registry
}

#[tokio::test]
async fn third_party_connector_builds_a_working_source_and_sink() {
    let registry = registry();
    let raw = conn(
        "stock",
        "inventory",
        json!({"url": "https://inv.example.com", "orders": 5}),
    );

    let built = registry.build(&raw, Roles::BOTH).unwrap();
    assert_eq!(built.description, "inventory with 5 orders");
    let source = built.endpoints.source.expect("asked for a source");
    let sink = built.endpoints.sink.expect("asked for a sink");

    // Read everything from the source and write it to the sink, the way the
    // runner will, through the trait objects only.
    let entity = source.discover().await.unwrap().remove(0);
    assert_eq!(entity.name(), "orders");
    let target = TargetSchema::new(vec![TargetTable {
        name: "orders".into(),
        fields: entity.fields().to_vec(),
        primary_key: vec!["id".into()],
        foreign_keys: Vec::new(),
    }])
    .unwrap();
    sink.prepare(&target, WriteMode::Insert).await.unwrap();
    let strategy = sink
        .capabilities()
        .await
        .unwrap()
        .strategy_for(WriteMode::Insert)
        .unwrap();
    let mut written = 0;
    for partition in source
        .plan_partitions(&entity, &PartitionHint::rows(2))
        .await
        .unwrap()
    {
        let lease = Lease {
            partition: partition.id.clone(),
            epoch: Epoch(1),
        };
        let mut txn = sink.begin(&partition, &lease, strategy).await.unwrap();
        let batches: Vec<_> = source
            .read(&entity, &partition)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        for batch in batches {
            txn.write("orders", batch).await.unwrap();
        }
        written += txn.commit().await.unwrap().rows_written["orders"];
    }
    assert_eq!(written, 5);
}

#[test]
fn build_returns_only_the_roles_asked_for() {
    let registry = registry();
    let raw = conn(
        "stock",
        "inventory",
        json!({"url": "https://inv.example.com"}),
    );

    let source_only = registry.build(&raw, Roles::SOURCE).unwrap();
    assert!(source_only.endpoints.source.is_some());
    assert!(source_only.endpoints.sink.is_none());

    let sink_only = registry.build(&raw, Roles::SINK).unwrap();
    assert!(sink_only.endpoints.source.is_none());
    assert!(sink_only.endpoints.sink.is_some());
}

#[test]
fn a_read_only_type_used_as_a_sink_is_refused_before_building() {
    let registry = registry();
    let err = registry
        .build(&conn("old", "archive", json!({})), Roles::SINK)
        .unwrap_err();
    let BuildError::Other(FerryError::Connector { connector, message }) = err else {
        panic!("expected a connector error, got {err:?}")
    };
    assert_eq!(connector, "archive");
    assert!(
        message.contains("connection `old` is used as a sink"),
        "{message}"
    );
    assert!(message.contains("only a source"), "{message}");
}

#[test]
fn a_connector_that_returns_unasked_endpoints_is_reported_as_a_bug() {
    let mut registry = ConnectorRegistry::new();
    registry.register(Greedy);
    let err = registry
        .build(&conn("g", "greedy", json!({})), Roles::SOURCE)
        .unwrap_err();
    assert!(
        err.to_string().contains("connector bug") && err.to_string().contains("asked for source"),
        "{err}"
    );
}

#[test]
fn a_finalizer_without_a_sink_is_reported_as_a_bug() {
    let mut registry = ConnectorRegistry::new();
    registry.register(StrayFinalizer);
    let err = registry
        .build(&conn("s", "stray", json!({})), Roles::SOURCE)
        .unwrap_err();
    assert!(
        err.to_string().contains("connector bug")
            && err.to_string().contains("a finalizer without a sink"),
        "{err}"
    );
}

#[test]
fn a_ddl_renderer_without_a_sink_is_reported_as_a_bug() {
    let mut registry = ConnectorRegistry::new();
    registry.register(StrayDdl);
    let err = registry
        .build(&conn("s", "stray_ddl", json!({})), Roles::SOURCE)
        .unwrap_err();
    assert!(
        err.to_string().contains("connector bug")
            && err.to_string().contains("a DDL renderer without a sink"),
        "{err}"
    );
}

#[test]
fn an_unknown_type_is_an_options_error_listing_the_registered_types() {
    let err = registry()
        .build(&conn("x", "fake", json!({})), Roles::SOURCE)
        .unwrap_err();
    let BuildError::Options(errors) = err else {
        panic!("expected an options error, got {err:?}")
    };
    assert_eq!(errors[0].path, "type");
    assert_eq!(
        errors[0].message,
        "unknown connection type `fake`; registered types: `archive`, `inventory`"
    );
}

#[test]
fn bad_options_come_back_with_the_path_of_the_bad_field() {
    let registry = registry();

    let typo = registry
        .build(
            &conn(
                "s",
                "inventory",
                json!({"url": "https://inv.example.com", "ordrs": 1}),
            ),
            Roles::SOURCE,
        )
        .unwrap_err();
    let BuildError::Options(errors) = typo else {
        panic!("{typo:?}")
    };
    assert!(
        errors[0].message.contains("unknown field `ordrs`"),
        "{errors:?}"
    );

    let wrong_type = registry
        .build(
            &conn(
                "s",
                "inventory",
                json!({"url": "https://inv.example.com", "orders": "many"}),
            ),
            Roles::SOURCE,
        )
        .unwrap_err();
    let BuildError::Options(errors) = wrong_type else {
        panic!("{wrong_type:?}")
    };
    assert_eq!(errors[0].path, "orders");

    let missing = registry
        .build(&conn("s", "inventory", json!({})), Roles::SOURCE)
        .unwrap_err();
    assert!(
        missing.to_string().contains("missing field `url`"),
        "{missing}"
    );

    // Options errors raised by the connector's own `build` keep their path.
    let negative = registry
        .build(
            &conn(
                "s",
                "inventory",
                json!({"url": "https://inv.example.com", "orders": -1}),
            ),
            Roles::SOURCE,
        )
        .unwrap_err();
    assert_eq!(
        negative,
        BuildError::option("orders", "must not be negative")
    );
}

#[test]
fn types_lists_every_registered_type_with_its_roles() {
    let types: Vec<_> = registry().types().map(|(t, r)| (t.to_owned(), r)).collect();
    assert_eq!(
        types,
        vec![
            ("archive".to_owned(), Roles::SOURCE),
            ("inventory".to_owned(), Roles::BOTH)
        ]
    );
}

// ---- (b) registration is rejected for wiring bugs ----

#[test]
fn registering_a_type_twice_is_rejected() {
    let mut registry = registry();
    assert_eq!(
        registry.try_register(Inventory).err(),
        Some(RegisterError::DuplicateType {
            type_name: "inventory".into()
        })
    );
    // The original registration is untouched.
    assert!(
        registry
            .build(
                &conn("s", "inventory", json!({"url": "https://inv.example.com"})),
                Roles::SINK
            )
            .is_ok()
    );
}

#[test]
#[should_panic(expected = "connector type `inventory` is already registered")]
fn register_panics_on_a_duplicate_type() {
    registry().register(Inventory);
}

/// Two different Rust types claiming the same `type:` string collide too.
#[test]
fn a_different_connector_with_the_same_type_name_is_rejected() {
    struct Impostor;
    impl Connector for Impostor {
        const TYPE: &'static str = "archive";
        const ROLES: Roles = Roles::SOURCE;
        type Options = ArchiveOptions;
        fn build(
            &self,
            _: &BuildContext<'_>,
            _: ArchiveOptions,
            _: Roles,
        ) -> Result<Endpoints, BuildError> {
            unreachable!()
        }
        fn describe(&self, _: &ArchiveOptions) -> String {
            unreachable!()
        }
    }
    assert!(matches!(
        registry().try_register(Impostor),
        Err(RegisterError::DuplicateType { .. })
    ));
}

#[test]
fn replace_swaps_a_registered_type_on_purpose() {
    struct PatchedArchive;
    impl Connector for PatchedArchive {
        const TYPE: &'static str = "archive";
        const ROLES: Roles = Roles::SOURCE;
        type Options = ArchiveOptions;
        fn build(
            &self,
            _: &BuildContext<'_>,
            _: ArchiveOptions,
            _: Roles,
        ) -> Result<Endpoints, BuildError> {
            Ok(Endpoints {
                source: Some(Arc::new(MemSource::new())),
                sink: None,
                finalizer: None,
                ddl: None,
            })
        }
        fn describe(&self, _: &ArchiveOptions) -> String {
            "patched archive".into()
        }
    }
    let mut registry = registry();
    registry.replace(PatchedArchive);
    let built = registry
        .build(&conn("a", "archive", json!({})), Roles::SOURCE)
        .unwrap();
    assert_eq!(built.description, "patched archive");
}

#[test]
#[should_panic(expected = "connector type `archive` is not registered")]
fn replace_refuses_a_type_that_was_never_registered() {
    ConnectorRegistry::new().replace(Archive);
}

mod reserved {
    use super::*;

    macro_rules! connector_with {
        ($name:ident, $options:ty) => {
            struct $name;
            impl Connector for $name {
                const TYPE: &'static str = stringify!($name);
                const ROLES: Roles = Roles::SOURCE;
                type Options = $options;
                fn build(
                    &self,
                    _: &BuildContext<'_>,
                    _: $options,
                    _: Roles,
                ) -> Result<Endpoints, BuildError> {
                    unreachable!()
                }
                fn describe(&self, _: &$options) -> String {
                    unreachable!()
                }
            }
        };
    }

    #[derive(Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    #[allow(dead_code)]
    struct WithDescription {
        url: String,
        description: String,
    }
    connector_with!(Described, WithDescription);

    /// The Rust field is innocent; the YAML key it reads is not.
    #[derive(Deserialize, JsonSchema)]
    #[allow(dead_code)]
    struct RenamedToType {
        #[serde(rename = "type")]
        kind: String,
    }
    connector_with!(Renamed, RenamedToType);

    /// A reserved key hidden in a flattened struct is still a top-level key.
    #[derive(Deserialize, JsonSchema)]
    #[allow(dead_code)]
    struct Common {
        name: Option<String>,
    }
    #[derive(Deserialize, JsonSchema)]
    #[allow(dead_code)]
    struct Flattened {
        url: String,
        #[serde(flatten)]
        common: Common,
    }
    connector_with!(Flat, Flattened);

    /// Nested keys are the connector's own business.
    #[derive(Deserialize, JsonSchema)]
    #[allow(dead_code)]
    struct Tls {
        name: String,
    }
    #[derive(Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    #[allow(dead_code)]
    struct NestedName {
        tls: Tls,
    }
    connector_with!(Nested, NestedName);

    #[test]
    fn options_using_a_reserved_key_are_rejected() {
        let mut registry = ConnectorRegistry::new();
        for (result, key) in [
            (registry.try_register(Described).map(|_| ()), "description"),
            (registry.try_register(Renamed).map(|_| ()), "type"),
            (registry.try_register(Flat).map(|_| ()), "name"),
        ] {
            let Err(RegisterError::ReservedKey { key: got, .. }) = result else {
                panic!("expected `{key}` to be rejected, got {result:?}")
            };
            assert_eq!(got, key);
        }
        assert_eq!(registry.types().count(), 0, "nothing was registered");
    }

    #[test]
    fn a_reserved_word_below_the_top_level_is_allowed() {
        let mut registry = ConnectorRegistry::new();
        registry.register(Nested);
        assert_eq!(registry.types().count(), 1);
    }

    #[test]
    #[should_panic(expected = "option `description` uses a reserved connection key")]
    fn register_panics_on_a_reserved_key() {
        ConnectorRegistry::new().register(Described);
    }
}

/// 0004 §3's opt-out: a connector that takes any fields.
struct FreeForm;

impl Connector for FreeForm {
    const TYPE: &'static str = "freeform";
    const ROLES: Roles = Roles::SOURCE;
    type Options = serde_json::Map<String, serde_json::Value>;

    fn build(
        &self,
        _: &BuildContext<'_>,
        _: Self::Options,
        _: Roles,
    ) -> Result<Endpoints, BuildError> {
        Ok(Endpoints {
            source: Some(Arc::new(MemSource::new())),
            sink: None,
            finalizer: None,
            ddl: None,
        })
    }

    fn describe(&self, options: &Self::Options) -> String {
        format!("{} fields", options.len())
    }
}

#[test]
fn reserved_keys_in_an_entry_never_reach_a_free_form_connector() {
    let mut registry = ConnectorRegistry::new();
    registry.register(FreeForm);
    let built = registry
        .build(
            &conn("f", "freeform", json!({"anything": 1})),
            Roles::SOURCE,
        )
        .unwrap();
    assert_eq!(built.description, "1 fields");

    for key in ["description", "name", "type"] {
        let err = registry
            .build(&conn("f", "freeform", json!({ key: "x" })), Roles::SOURCE)
            .unwrap_err();
        let BuildError::Options(errors) = err else {
            panic!("{key}: {err:?}")
        };
        assert_eq!(errors[0].path, key);
        assert!(errors[0].message.contains("reserved"), "{errors:?}");
    }
}
