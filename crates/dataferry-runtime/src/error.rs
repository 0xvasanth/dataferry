use dataferry_core::FerryError;
use dataferry_state::ResumeError;
use thiserror::Error;

/// Why the runner couldn't run (or resume) a run at all. A partition that
/// fails is not one of these: it is marked failed, and shows up in the
/// [`RunReport`](crate::RunReport).
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RunError {
    /// The flows or settings handed to the runner are inconsistent.
    #[error("{0}")]
    Invalid(String),

    /// `resume` or `retry`: no such run, or it is pinned to another config
    /// or engine version.
    #[error(transparent)]
    Resume(#[from] ResumeError),

    /// The flow's sink offers no idempotency strategy for its write mode
    /// (0001): re-running a partition could duplicate or lose rows.
    #[error(
        "flow `{flow}`: the sink offers no idempotency strategy for {mode:?} \
         (it supports {strategies})"
    )]
    NoStrategy {
        flow: String,
        mode: dataferry_core::WriteMode,
        strategies: String,
    },

    /// Planning a flow, preparing its sink, or a state store call outside
    /// any one partition failed.
    #[error("flow `{flow}`: {source}")]
    Flow {
        flow: String,
        #[source]
        source: FerryError,
    },

    /// A worker panicked. Its partition stays leased until the lease
    /// expires; resuming the run picks it up again.
    #[error("a worker panicked: {0}")]
    Worker(String),

    #[error("state store: {0}")]
    Store(#[source] FerryError),
}
