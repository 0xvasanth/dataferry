//! The target schema the planner asks a sink to create or validate. It is
//! dialect-neutral; each sink renders its own DDL from it.

use crate::error::{FerryError, Result};
use crate::types::CanonicalField;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKey {
    pub columns: Vec<String>,
    pub references_table: String,
    pub references_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetTable {
    pub name: String,
    pub fields: Vec<CanonicalField>,
    pub primary_key: Vec<String>,
    pub foreign_keys: Vec<ForeignKey>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetSchema {
    tables: Vec<TargetTable>,
}

impl TargetSchema {
    /// Validates that keys name real columns and every foreign key points at
    /// an existing table and columns with matching arity.
    pub fn new(tables: Vec<TargetTable>) -> Result<Self> {
        for (i, t) in tables.iter().enumerate() {
            let invalid = |reason: String| FerryError::InvalidTargetSchema {
                table: t.name.clone(),
                reason,
            };
            if tables[..i].iter().any(|o| o.name == t.name) {
                return Err(invalid("table is declared twice".into()));
            }
            if let Some(c) = missing_column(t, &t.primary_key) {
                return Err(invalid(format!("primary key column `{c}` is not a field")));
            }
            for fk in &t.foreign_keys {
                if let Some(c) = missing_column(t, &fk.columns) {
                    return Err(invalid(format!("foreign key column `{c}` is not a field")));
                }
                let Some(parent) = tables.iter().find(|p| p.name == fk.references_table) else {
                    return Err(invalid(format!(
                        "foreign key references unknown table `{}`",
                        fk.references_table
                    )));
                };
                if let Some(c) = missing_column(parent, &fk.references_columns) {
                    return Err(invalid(format!(
                        "foreign key references unknown column `{c}` on `{}`",
                        parent.name
                    )));
                }
                if fk.columns.len() != fk.references_columns.len() {
                    return Err(invalid(format!(
                        "foreign key has {} columns but references {} columns",
                        fk.columns.len(),
                        fk.references_columns.len()
                    )));
                }
            }
        }
        Ok(Self { tables })
    }

    pub fn tables(&self) -> &[TargetTable] {
        &self.tables
    }

    pub fn table(&self, name: &str) -> Option<&TargetTable> {
        self.tables.iter().find(|t| t.name == name)
    }
}

fn missing_column<'a>(table: &TargetTable, columns: &'a [String]) -> Option<&'a String> {
    columns
        .iter()
        .find(|c| !table.fields.iter().any(|f| &f.name == *c))
}
