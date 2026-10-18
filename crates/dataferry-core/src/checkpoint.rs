use serde::{Deserialize, Serialize};

/// Where a read can resume within a partition. The bytes are owned and
/// interpreted only by the connector that produced them; the engine stores
/// and returns them unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint(Vec<u8>);

impl Checkpoint {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
