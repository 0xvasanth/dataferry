//! Reading Mongo collections as 0xferry entities.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use ferry_core::types::{CanonicalField, CanonicalType};
use ferry_core::{
    BatchStream, Entity, FerryError, KeyRange, KeyValue, Partition, PartitionHint, PartitionId,
    PartitionScope, Result, Source, SourceCaps,
};
use futures::{StreamExt, TryStreamExt, stream};
use mongodb::bson::oid::ObjectId;
use mongodb::bson::{Bson, Document, doc};
use mongodb::options::{ClientOptions, Collation, ConnectionString};
use mongodb::results::CollectionType;
use mongodb::{Client, Collection, Cursor};
use tokio::sync::OnceCell;

use crate::convert::to_batches;
use crate::infer::{Shape, TextKind, TextKinds, text_kinds};
use crate::tls;

/// Reads the collections of one Mongo database.
///
/// Each collection's schema is inferred from a sample of its documents (all
/// of them when the collection is no bigger than the sample). Fields of
/// conflicting types become JSON variants, and missing fields are told apart
/// from nulls through `_ferry_missing`. A document read later that doesn't
/// fit the inferred schema is never cast or dropped: it is read as a row of
/// a rejected batch ([`ferry_registry::rejected`]) naming its `_id` and the
/// path, with the document as relaxed Extended JSON, which the runner
/// dead-letters or fails on per `on_error`. A document whose `_id` doesn't
/// fit fails the read.
///
/// Collections whose `_id`s are all ObjectIds, strings or integers are split
/// into `_id` ranges; others are read as one partition. Strings compare by
/// bytes (the `simple` collation) whatever the collection's default.
///
/// Views are skipped: they hold no data of their own. Time-series
/// collections are not supported yet and fail discovery by name.
pub struct MongoSource {
    conn: ConnectionString,
    database: String,
    sample_size: u64,
    batch_rows: usize,
    client: OnceCell<Client>,
    /// Which UTF-8 columns of each collection hold ObjectIds, learnt at
    /// discovery or, for an entity discovered elsewhere, from a new sample.
    text_kinds: Mutex<HashMap<String, Arc<TextKinds>>>,
}

impl MongoSource {
    pub const DEFAULT_SAMPLE_SIZE: u64 = 1000;
    pub const DEFAULT_BATCH_ROWS: usize = 8192;

    /// `url` is a `mongodb://` or `mongodb+srv://` connection string; TLS
    /// follows the policy in [`ferry_connector_tls`]. Does no I/O: the first trait
    /// call connects.
    pub fn new(url: &str, database: impl Into<String>) -> Result<Self> {
        Ok(Self {
            conn: tls::parse(url)?,
            database: database.into(),
            sample_size: Self::DEFAULT_SAMPLE_SIZE,
            batch_rows: Self::DEFAULT_BATCH_ROWS,
            client: OnceCell::new(),
            text_kinds: Mutex::new(HashMap::new()),
        })
    }

    /// How many documents per collection schema inference looks at.
    pub fn with_sample_size(mut self, n: u64) -> Self {
        self.sample_size = n.max(1);
        self
    }

    /// Maximum rows per emitted batch.
    pub fn with_batch_rows(mut self, n: usize) -> Self {
        self.batch_rows = n.max(1);
        self
    }

    async fn client(&self) -> Result<&Client> {
        self.client
            .get_or_try_init(|| async {
                let options = ClientOptions::parse(self.conn.clone())
                    .await
                    .map_err(mongo_error)?;
                Client::with_options(options).map_err(mongo_error)
            })
            .await
    }

    async fn collection(&self, name: &str) -> Result<Collection<Document>> {
        Ok(self
            .client()
            .await?
            .database(&self.database)
            .collection(name))
    }

    /// The documents schema inference looks at, in `_id` order.
    async fn sample(&self, coll: &Collection<Document>) -> Result<Vec<Document>> {
        let count = coll.estimated_document_count().await.map_err(mongo_error)?;
        let cursor = if count <= self.sample_size {
            coll.find(doc! {})
                .sort(doc! { "_id": 1 })
                .limit(self.sample_size as i64)
                .await
        } else {
            coll.aggregate([
                doc! { "$sample": { "size": self.sample_size as i64 } },
                doc! { "$sort": { "_id": 1 } },
            ])
            .await
        };
        cursor
            .map_err(mongo_error)?
            .try_collect()
            .await
            .map_err(mongo_error)
    }

    fn entity_from(&self, name: &str, docs: &[Document]) -> Result<Entity> {
        let mut shapes = merged_fields(docs);
        // `_id` first, and never null.
        let id = match shapes.iter().position(|(n, _)| n == "_id") {
            Some(i) => shapes.remove(i).1,
            None => Shape::Unknown,
        };
        let mut fields = vec![CanonicalField::new("_id", id.to_canonical(), false)];
        let mut all = vec![("_id".to_owned(), id)];
        all.extend(shapes.iter().cloned());
        self.text_kinds
            .lock()
            .expect("text kinds lock")
            .insert(name.to_owned(), Arc::new(text_kinds(&all)));
        fields.extend(
            shapes
                .iter()
                .map(|(n, s)| CanonicalField::new(n, s.to_canonical(), true)),
        );
        Ok(Entity::new(name, fields, vec!["_id".into()])?.with_missing_tracking())
    }

    /// Which UTF-8 columns of `entity` hold ObjectIds.
    async fn text_kinds_of(&self, entity: &Entity) -> Result<Arc<TextKinds>> {
        if let Some(kinds) = self
            .text_kinds
            .lock()
            .expect("text kinds lock")
            .get(entity.name())
        {
            return Ok(Arc::clone(kinds));
        }
        let docs = self.sample(&self.collection(entity.name()).await?).await?;
        let kinds = Arc::new(text_kinds(&merged_fields(&docs)));
        self.text_kinds
            .lock()
            .expect("text kinds lock")
            .insert(entity.name().to_owned(), Arc::clone(&kinds));
        Ok(kinds)
    }

    /// How values of a key column are stored, or `None` if it can't be
    /// range-partitioned.
    async fn key_kind(&self, entity: &Entity, column: &str) -> Result<Option<KeyKind>> {
        let Some(field) = entity.field(column) else {
            return Ok(None);
        };
        match field.ty {
            CanonicalType::Int32 | CanonicalType::Int64 => Ok(Some(KeyKind::Int)),
            CanonicalType::Utf8 => Ok(Some(match self.text_kinds_of(entity).await?.get(column) {
                Some(TextKind::ObjectId) => KeyKind::ObjectId,
                Some(TextKind::String) => KeyKind::String,
                // Only nulls in the sample: ask whether there are ObjectIds.
                None => match self
                    .collection(entity.name())
                    .await?
                    .find_one(doc! { column: { "$type": "objectId" } })
                    .projection(doc! { "_id": 1 })
                    .await
                    .map_err(mongo_error)?
                {
                    Some(_) => KeyKind::ObjectId,
                    None => KeyKind::String,
                },
            })),
            _ => Ok(None),
        }
    }
}

/// The merged shape of every top-level field of `docs`.
fn merged_fields(docs: &[Document]) -> Vec<(String, Shape)> {
    match docs
        .iter()
        .map(Shape::of_document)
        .fold(Shape::Struct(Vec::new()), Shape::merge)
    {
        Shape::Struct(fields) => fields,
        _ => unreachable!("documents always merge into a struct"),
    }
}

/// Byte-order string comparison, matching [`KeyValue`]'s, whatever the
/// collection's default collation.
fn simple_collation() -> Collation {
    Collation::builder().locale("simple").build()
}

pub(crate) fn connector_error(message: impl Into<String>) -> FerryError {
    FerryError::Connector {
        connector: "mongo".into(),
        message: message.into(),
    }
}

fn mongo_error(e: mongodb::error::Error) -> FerryError {
    connector_error(e.to_string())
}

/// The kinds of keys that can be range-partitioned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyKind {
    /// int32 / int64 (`KeyValue::Int`).
    Int,
    /// Strings (`KeyValue::Str`), compared by bytes as Mongo does without a
    /// collation.
    String,
    /// ObjectIds, as their hex strings (`KeyValue::Str`), which sort the
    /// same way.
    ObjectId,
}

impl KeyKind {
    /// The `$type` of every value a range of this kind can match. Mongo
    /// compares all numbers with each other, so an int range also takes
    /// doubles and decimals.
    fn bracket(self) -> &'static str {
        match self {
            Self::Int => "number",
            Self::String => "string",
            Self::ObjectId => "objectId",
        }
    }

    /// The `$type`s that partition boundaries are taken from.
    fn exact_types(self) -> Bson {
        match self {
            Self::Int => Bson::Array(vec!["int".into(), "long".into()]),
            Self::String => "string".into(),
            Self::ObjectId => "objectId".into(),
        }
    }

    fn accepts(self, value: &KeyValue) -> bool {
        match (self, value) {
            (Self::Int, KeyValue::Int(_)) | (Self::String, KeyValue::Str(_)) => true,
            (Self::ObjectId, KeyValue::Str(s)) => ObjectId::parse_str(s).is_ok(),
            _ => false,
        }
    }

    fn to_bson(self, value: &KeyValue) -> Result<Bson> {
        match (self, value) {
            (Self::Int, KeyValue::Int(v)) => Ok(Bson::Int64(*v)),
            (Self::String, KeyValue::Str(s)) => Ok(Bson::String(s.clone())),
            (Self::ObjectId, KeyValue::Str(s)) => ObjectId::parse_str(s)
                .map(Bson::ObjectId)
                .map_err(|_| connector_error(format!("key bound {s:?} is not an ObjectId"))),
            _ => Err(connector_error(format!(
                "key bound {value:?} does not match a {self:?} key"
            ))),
        }
    }

    fn key_value(self, value: &Bson) -> Option<KeyValue> {
        match (self, value) {
            (Self::Int, Bson::Int32(v)) => Some(KeyValue::Int(i64::from(*v))),
            (Self::Int, Bson::Int64(v)) => Some(KeyValue::Int(*v)),
            (Self::String, Bson::String(s)) => Some(KeyValue::Str(s.clone())),
            (Self::ObjectId, Bson::ObjectId(o)) => Some(KeyValue::Str(o.to_hex())),
            _ => None,
        }
    }

    /// The filter selecting `range`. The first range also takes every
    /// document whose key is of another type, null or missing: a range
    /// filter alone would match none of them, and they would silently fall
    /// into no partition. Reading them then fails, loudly, on the type.
    fn filter(self, range: &KeyRange) -> Result<Document> {
        let column = range.column();
        let mut bounds = Document::new();
        if let Some(lo) = range.lower() {
            bounds.insert("$gte", self.to_bson(lo)?);
        }
        if let Some(hi) = range.upper() {
            bounds.insert("$lt", self.to_bson(hi)?);
        }
        Ok(match (range.lower(), range.upper()) {
            (None, None) => doc! {},
            (None, Some(_)) => doc! { "$or": [
                { column: bounds },
                { column: { "$not": { "$type": self.bracket() } } },
            ] },
            (Some(_), _) => doc! { column: bounds },
        })
    }
}

fn partitions_for(entity: &Entity, ranges: Vec<KeyRange>) -> Vec<Partition> {
    ranges
        .into_iter()
        .enumerate()
        .map(|(i, range)| Partition {
            id: PartitionId(format!("{}-{i}", entity.name())),
            entity: entity.name().to_owned(),
            scope: PartitionScope::KeyRange(range),
        })
        .collect()
}

#[async_trait]
impl Source for MongoSource {
    async fn discover(&self) -> Result<Vec<Entity>> {
        let db = self.client().await?.database(&self.database);
        let specs: Vec<_> = db
            .list_collections()
            .await
            .map_err(mongo_error)?
            .try_collect()
            .await
            .map_err(mongo_error)?;
        let mut entities = Vec::new();
        let mut problems = Vec::new();
        let mut names = Vec::new();
        for spec in specs {
            match spec.collection_type {
                _ if spec.name.starts_with("system.") => {}
                CollectionType::Collection => names.push(spec.name),
                CollectionType::Timeseries => problems.push(format!(
                    "{}: time-series collections are not supported yet",
                    spec.name
                )),
                // Views hold no data of their own.
                _ => {}
            }
        }
        names.sort();
        problems.sort();
        for name in names {
            let docs = self.sample(&db.collection(&name)).await?;
            match self.entity_from(&name, &docs) {
                Ok(e) => entities.push(e),
                Err(e) => problems.push(format!("{name}: {e}")),
            }
        }
        if !problems.is_empty() {
            return Err(connector_error(format!(
                "database `{}` has collections that can't be read:\n  - {}",
                self.database,
                problems.join("\n  - ")
            )));
        }
        Ok(entities)
    }

    async fn plan_partitions(
        &self,
        entity: &Entity,
        hint: &PartitionHint,
    ) -> Result<Vec<Partition>> {
        if let Some(cp) = &hint.co_partition {
            let unsupported = |reason: String| FerryError::CoPartitionUnsupported {
                column: cp.column().to_owned(),
                reason,
            };
            let field = entity
                .field(cp.column())
                .ok_or_else(|| unsupported(format!("`{}` has no such field", entity.name())))?;
            let kind = self.key_kind(entity, cp.column()).await?.ok_or_else(|| {
                unsupported(format!("{:?} fields can't be range-partitioned", field.ty))
            })?;
            if let Some(bad) = cp.boundaries().iter().find(|b| !kind.accepts(b)) {
                return Err(unsupported(format!(
                    "boundary {bad:?} can't be compared with a {kind:?} field"
                )));
            }
            return Ok(partitions_for(entity, cp.ranges()));
        }

        let whole = vec![Partition {
            id: PartitionId(format!("{}-0", entity.name())),
            entity: entity.name().to_owned(),
            scope: PartitionScope::Whole,
        }];
        let Some(kind) = self.key_kind(entity, "_id").await? else {
            return Ok(whole);
        };
        let target = hint.target_rows.max(1);
        let mut ids = self
            .collection(entity.name())
            .await?
            .aggregate([
                doc! { "$match": { "_id": { "$type": kind.exact_types() } } },
                doc! { "$sort": { "_id": 1 } },
                doc! { "$project": { "_id": 1 } },
            ])
            .collation(simple_collation())
            .await
            .map_err(mongo_error)?;
        let mut bounds = Vec::new();
        let mut n: u64 = 0;
        while let Some(doc) = ids.try_next().await.map_err(mongo_error)? {
            if n > 0
                && n.is_multiple_of(target)
                && let Some(key) = doc.get("_id").and_then(|id| kind.key_value(id))
            {
                bounds.push(key);
            }
            n += 1;
        }

        let lowers = std::iter::once(None).chain(bounds.iter().cloned().map(Some));
        let uppers = bounds
            .iter()
            .cloned()
            .map(Some)
            .chain(std::iter::once(None));
        let ranges = lowers
            .zip(uppers)
            .map(|(lo, hi)| KeyRange::new("_id", lo, hi))
            .collect::<Result<Vec<_>>>()?;
        Ok(partitions_for(entity, ranges))
    }

    async fn read(&self, entity: &Entity, partition: &Partition) -> Result<BatchStream> {
        let filter = match &partition.scope {
            PartitionScope::Whole => doc! {},
            PartitionScope::KeyRange(range) => {
                let kind = self
                    .key_kind(entity, range.column())
                    .await?
                    .ok_or_else(|| {
                        connector_error(format!(
                            "`{}` can't be range-partitioned on `{}`",
                            entity.name(),
                            range.column()
                        ))
                    })?;
                kind.filter(range)?
            }
        };
        let cursor = self
            .collection(entity.name())
            .await?
            .find(filter)
            .sort(doc! { "_id": 1 })
            .collation(simple_collation())
            .batch_size(self.batch_rows.min(u32::MAX as usize) as u32)
            .await
            .map_err(mongo_error)?;

        struct State {
            cursor: Cursor<Document>,
            done: bool,
        }
        let kinds = self.text_kinds_of(entity).await?;
        let schema = entity.arrow_schema();
        let entity = entity.clone();
        let batch_rows = self.batch_rows;
        let state = State {
            cursor,
            done: false,
        };
        Ok(stream::try_unfold(state, move |mut st| {
            let schema = schema.clone();
            let entity = entity.clone();
            let kinds = Arc::clone(&kinds);
            async move {
                if st.done {
                    return Ok(None);
                }
                let mut docs = Vec::new();
                while docs.len() < batch_rows {
                    let Some(doc) = st.cursor.try_next().await.map_err(mongo_error)? else {
                        st.done = true;
                        break;
                    };
                    docs.push(doc);
                }
                if docs.is_empty() {
                    return Ok(None);
                }
                let batches = to_batches(&entity, schema, &docs, &kinds)
                    .map_err(|e| connector_error(format!("{}: {e}", entity.name())))?;
                Ok(Some((stream::iter(batches.into_iter().map(Ok)), st)))
            }
        })
        .try_flatten()
        .boxed())
    }

    async fn capabilities(&self) -> Result<SourceCaps> {
        Ok(SourceCaps {
            range_scan: true,
            predicate_pushdown: false,
            consistent_snapshot: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(lo: Option<KeyValue>, hi: Option<KeyValue>) -> KeyRange {
        KeyRange::new("_id", lo, hi).unwrap()
    }

    #[test]
    fn the_first_range_also_takes_keys_of_other_types_and_none_at_all() {
        let f = KeyKind::Int
            .filter(&range(None, Some(KeyValue::Int(10))))
            .unwrap();
        assert_eq!(
            f,
            doc! { "$or": [
                { "_id": { "$lt": 10_i64 } },
                { "_id": { "$not": { "$type": "number" } } },
            ] }
        );
        let f = KeyKind::Int
            .filter(&range(Some(KeyValue::Int(10)), Some(KeyValue::Int(20))))
            .unwrap();
        assert_eq!(f, doc! { "_id": { "$gte": 10_i64, "$lt": 20_i64 } });
        let f = KeyKind::String
            .filter(&range(Some(KeyValue::Str("m".into())), None))
            .unwrap();
        assert_eq!(f, doc! { "_id": { "$gte": "m" } });
        assert_eq!(KeyKind::Int.filter(&range(None, None)).unwrap(), doc! {});
    }

    #[test]
    fn object_id_bounds_are_compared_as_object_ids() {
        let hex = "650000000000000000000005";
        let f = KeyKind::ObjectId
            .filter(&range(Some(KeyValue::Str(hex.into())), None))
            .unwrap();
        assert_eq!(
            f,
            doc! { "_id": { "$gte": ObjectId::parse_str(hex).unwrap() } }
        );
        let err = KeyKind::ObjectId
            .filter(&range(Some(KeyValue::Str("c1".into())), None))
            .unwrap_err();
        assert!(err.to_string().contains("not an ObjectId"), "{err}");
        assert!(!KeyKind::ObjectId.accepts(&KeyValue::Str("c1".into())));
        assert!(KeyKind::ObjectId.accepts(&KeyValue::Str(hex.into())));
    }
}
