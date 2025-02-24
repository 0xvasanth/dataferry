use thiserror::Error;

pub type Result<T, E = FerryError> = std::result::Result<T, E>;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FerryError {
    #[error("invalid decimal({precision}, {scale}): {reason}")]
    InvalidDecimal {
        precision: u8,
        scale: i8,
        reason: &'static str,
    },

    #[error("field `{field}`: arrow type {arrow_type} has no canonical equivalent")]
    UnsupportedArrowType { field: String, arrow_type: String },

    #[error("entity `{entity}`: {reason}")]
    InvalidEntity { entity: String, reason: String },

    #[error("invalid key range on `{column}`: {reason}")]
    InvalidKeyRange {
        column: String,
        reason: &'static str,
    },

    #[error("target table `{table}`: {reason}")]
    InvalidTargetSchema { table: String, reason: String },

    #[error("lease on partition `{partition}` at epoch {epoch} was superseded by epoch {current}")]
    StaleLease {
        partition: String,
        epoch: u64,
        current: u64,
    },

    #[error("key column: {reason}")]
    InvalidKey { reason: String },

    #[error("no input stream for entity `{entity}`")]
    MissingInput { entity: String },

    #[error("transform output broke its contract: {reason}")]
    InvalidTransformOutput { reason: String },

    #[error("{connector}: {message}")]
    Connector { connector: String, message: String },
}
