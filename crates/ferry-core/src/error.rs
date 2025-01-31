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
}
