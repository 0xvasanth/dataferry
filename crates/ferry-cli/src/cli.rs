//! Arguments (0004 §6). Global flags go before or after the subcommand.

use std::path::PathBuf;

use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use ferry_dsl::config::DEFAULT_DIR;

use crate::diagnostic::Format;

#[derive(Debug, Parser)]
#[command(about = "Move data between databases, one committed partition at a time.")]
pub(crate) struct Cli {
    /// The `ferry/` config directory.
    #[arg(short, long, global = true, env = "FERRY_CONFIG", default_value = DEFAULT_DIR, value_name = "DIR")]
    pub config: PathBuf,

    /// Colour diagnostics: `auto` colours a terminal unless NO_COLOR is set.
    #[arg(long, global = true, value_enum, default_value_t = Color::Auto, value_name = "WHEN")]
    pub color: Color,

    /// `json` prints diagnostics as JSON lines, for CI and editors.
    #[arg(long, global = true, value_enum, default_value_t = Format::Text)]
    pub format: Format,

    /// More detail, e.g. how each connection was understood.
    #[arg(short, long, global = true, action = ArgAction::Count, conflicts_with = "quiet")]
    pub verbose: u8,

    /// Only errors: no warnings, no summary.
    #[arg(short, long, global = true)]
    pub quiet: bool,

    /// Where `run` keeps write-time records, reports and file-backed
    /// state. Default: `.ferry` next to the config directory.
    #[arg(long, global = true, env = "FERRY_WORK_DIR", value_name = "DIR")]
    pub work_dir: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum Color {
    Auto,
    Always,
    Never,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Check the config without connecting to anything.
    Validate,
    /// Print what a run would do: discovered entities, types, DDL and
    /// strategies. Reads from sources, writes nothing.
    Plan,
    /// Plan, then run every flow. Prints the run id first.
    Run,
    /// Continue an unfinished run.
    Resume {
        /// The run to continue.
        run_id: String,
    },
    /// Re-run the failed partitions of a finished run.
    Retry {
        /// The run whose failed partitions to re-run.
        #[arg(long = "failed", value_name = "RUN_ID", required = true)]
        failed: String,
    },
    /// Print the JSON Schemas for this binary's config files, including the
    /// connection types it has registered.
    Schema {
        /// Write one `<file>.schema.json` per config file into DIR instead.
        #[arg(long, value_name = "DIR")]
        out: Option<PathBuf>,
    },
}
