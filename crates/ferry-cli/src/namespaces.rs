//! How a flow may qualify an entity with the namespace its connection
//! reads (S3): `app.public.orders` for the table `orders` that a Postgres
//! connection on schema `public` discovers.
//!
//! An entity reference is `<connection>.<entity>`, the entity named as
//! `discover()` reports it (0003 §3). A Postgres source reads one schema
//! and reports its tables unqualified, while configs (the pg-merge example
//! among them) write them schema-qualified, as `psql` would. The binary
//! keeps one function per connection type that has such a namespace, set
//! where its connectors are registered (`builtins`), like the verifier's
//! targets: no connector type is named in `plan` or `run`.

use std::collections::BTreeMap;

use ferry_registry::RawConnection;

/// The namespace a connection's entities may be qualified with, from its
/// substituted entry. `None` when the type has none or the entry can't be
/// read (validate reports that).
pub(crate) type NamespaceFn = fn(&RawConnection) -> Option<String>;

#[derive(Default)]
pub(crate) struct Namespaces {
    by_type: BTreeMap<&'static str, NamespaceFn>,
}

impl Namespaces {
    pub fn add(&mut self, type_name: &'static str, namespace: NamespaceFn) {
        self.by_type.insert(type_name, namespace);
    }

    pub fn of(&self, connection: &RawConnection) -> Option<String> {
        self.by_type
            .get(connection.r#type.as_str())
            .and_then(|f| f(connection))
    }
}

/// The built-in `postgres` namespace: the connection's `schema:`.
#[cfg(feature = "postgres")]
pub(crate) fn postgres(conn: &RawConnection) -> Option<String> {
    let options: ferry_connector_postgres::PostgresOptions =
        serde_json::from_value(serde_json::Value::Object(conn.fields.clone())).ok()?;
    Some(options.schema)
}
