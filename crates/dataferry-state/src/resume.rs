//! Resuming a run: the config hash and engine version it was pinned to
//! must not have changed (0003 §10).

use dataferry_core::FerryError;
use dataferry_registry::StateStore;
use dataferry_registry::state::{RunId, RunRecord};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ResumeError {
    #[error("no run `{0}` in the state store")]
    NotFound(String),

    /// The config changed in a way that can change what the run writes.
    /// Tuning settings and secret values are not in the hash, so changing
    /// only those never lands here.
    #[error(
        "run `{run}` was started with config hash {pinned}, but the config now hashes to \
         {current}: the config changed since the run started. Start a new run, or restore \
         the config the run was started with"
    )]
    ConfigChanged {
        run: String,
        pinned: String,
        current: String,
    },

    #[error(
        "run `{run}` was started by dataferry {pinned}, and this is dataferry {current}: resume it \
         with dataferry {pinned}, or start a new run"
    )]
    EngineChanged {
        run: String,
        pinned: String,
        current: String,
    },

    #[error(transparent)]
    Store(#[from] FerryError),
}

/// Loads `run` for resuming, if it is pinned to `config_hash` and
/// `engine_version`. Both are compared as opaque strings.
pub async fn resume(
    store: &dyn StateStore,
    run: &RunId,
    config_hash: &str,
    engine_version: &str,
) -> Result<RunRecord, ResumeError> {
    let record = store
        .load_run(run)
        .await?
        .ok_or_else(|| ResumeError::NotFound(run.0.clone()))?;
    if record.config_hash != config_hash {
        return Err(ResumeError::ConfigChanged {
            run: run.0.clone(),
            pinned: record.config_hash,
            current: config_hash.to_owned(),
        });
    }
    if record.engine_version != engine_version {
        return Err(ResumeError::EngineChanged {
            run: run.0.clone(),
            pinned: record.engine_version,
            current: engine_version.to_owned(),
        });
    }
    Ok(record)
}
