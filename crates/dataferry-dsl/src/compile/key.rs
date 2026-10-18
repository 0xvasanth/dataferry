//! `$key`: the deterministic child key of an unnested row (B5, 0003 §5.4).
//!
//! A key is a UUIDv5 in [`NAMESPACE`] over three things, so the same input
//! gives the same key on any machine, in any partitioning, on every re-run:
//!
//! 1. the driver record's id (its primary-key values),
//! 2. the unnest path from the driver record down to the row, with the
//!    index taken at every level above it (`items[1].options`),
//! 3. `$index`, the row's position in the innermost array.
//!
//! The name hashed is a length-prefixed byte string, so two different
//! inputs never encode to the same name (`["ab"]` vs `["a", "b"]`, or
//! `items.0` vs `items0`). All integers are big-endian:
//!
//! ```text
//! name   = record path
//! record = u32 count, then per key value: tag u8, payload
//!          tag 0 (int):   i64
//!          tag 1 (text):  u32 length, UTF-8 bytes
//!          tag 2 (bytes): u32 length, bytes
//! path   = u32 levels, then per level: u32 length, path text, u64 index
//! ```
//!
//! The last level's index is `$index`. A UUID has 122 random bits, so
//! keys don't collide in practice (about 10^-19 at a billion rows), which a
//! 64-bit hash couldn't promise (about 2.7%).

use dataferry_core::partition::KeyValue;
use dataferry_core::transform::SourceRecordId;
use uuid::Uuid;

/// The namespace of every `$key`: UUIDv5 of the URL
/// `https://github.com/0xvasanth/ferry/keys/unnest/v1` in the standard URL
/// namespace. Changing it changes every generated key, so it never changes
/// within a major version.
pub const NAMESPACE: Uuid = Uuid::from_u128(0xbd7f_d989_838e_5717_97ef_0ae7_8367_6388);

/// The URL [`NAMESPACE`] is derived from.
pub const NAMESPACE_URL: &str = "https://github.com/0xvasanth/ferry/keys/unnest/v1";

/// One level of an unnest path: the path text as written in `unnest:`
/// and the index of the element taken there.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Step {
    pub path: String,
    pub index: u64,
}

/// The `$key` of the row reached from `record` through `steps`; the last
/// step's index is the row's `$index`.
pub fn child_key(record: &SourceRecordId, steps: &[Step]) -> Uuid {
    Uuid::new_v5(&NAMESPACE, &name(record, steps))
}

/// The bytes hashed (see the module docs).
pub fn name(record: &SourceRecordId, steps: &[Step]) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    let len = |out: &mut Vec<u8>, n: usize| {
        let n = u32::try_from(n).expect("a key part is shorter than 4 GiB");
        out.extend_from_slice(&n.to_be_bytes());
    };
    len(&mut out, record.0.len());
    for value in &record.0 {
        match value {
            KeyValue::Int(v) => {
                out.push(0);
                out.extend_from_slice(&v.to_be_bytes());
            }
            KeyValue::Str(s) => {
                out.push(1);
                len(&mut out, s.len());
                out.extend_from_slice(s.as_bytes());
            }
            KeyValue::Bytes(b) => {
                out.push(2);
                len(&mut out, b.len());
                out.extend_from_slice(b);
            }
        }
    }
    len(&mut out, steps.len());
    for step in steps {
        len(&mut out, step.path.len());
        out.extend_from_slice(step.path.as_bytes());
        out.extend_from_slice(&step.index.to_be_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_namespace_is_derived_from_its_url() {
        assert_eq!(
            Uuid::new_v5(&Uuid::NAMESPACE_URL, NAMESPACE_URL.as_bytes()),
            NAMESPACE
        );
    }
}
