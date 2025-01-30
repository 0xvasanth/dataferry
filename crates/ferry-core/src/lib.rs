//! Core types and contracts shared by every 0xferry crate.

pub mod entity;
pub mod error;
pub mod missing;
pub mod types;

pub use entity::Entity;
pub use error::{FerryError, Result};
