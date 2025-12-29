//! The exit-code table (0004 §6). Part of the CLI's contract for scripts and
//! CI: a code never changes meaning once released. `README.md` documents the
//! same table, and a test keeps the two in step.

use std::process::ExitCode;

/// Why `ferry` exited.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Exit {
    /// Success. Warnings, and dead-lettered rows under `max_errors`, still
    /// exit 0.
    Success,
    /// Internal error or not implemented: a bug, or a miswired binary.
    Internal,
    /// Bad flags, or `resume`/`retry` without a run id or with one the
    /// state store doesn't have.
    Usage,
    /// Load, schema, options, missing `${ENV}`, wrong role, or a state
    /// connection that isn't state-capable.
    ConfigInvalid,
    /// Type errors, no idempotency strategy, a dependency cycle, unknown
    /// entity (B7).
    PlanRefused,
    /// Could not connect or authenticate.
    Connect,
    /// The run finished with failed partitions or exceeded `max_errors`.
    RunFailed,
    /// Verification (`expect:`) failed after data was written.
    VerifyFailed,
    /// Resume refused: the config hash or engine version changed (A4).
    ResumeRefused,
    /// Interrupted (Ctrl-C) during `run`, `resume` or `retry`.
    Interrupted,
}

impl Exit {
    /// Every exit, in table order.
    pub const ALL: [Exit; 10] = [
        Exit::Success,
        Exit::Internal,
        Exit::Usage,
        Exit::ConfigInvalid,
        Exit::PlanRefused,
        Exit::Connect,
        Exit::RunFailed,
        Exit::VerifyFailed,
        Exit::ResumeRefused,
        Exit::Interrupted,
    ];

    pub const fn code(self) -> u8 {
        match self {
            Exit::Success => 0,
            Exit::Internal => 1,
            Exit::Usage => 2,
            Exit::ConfigInvalid => 3,
            Exit::PlanRefused => 4,
            Exit::Connect => 5,
            Exit::RunFailed => 6,
            Exit::VerifyFailed => 7,
            Exit::ResumeRefused => 8,
            Exit::Interrupted => 130,
        }
    }
}

impl From<Exit> for ExitCode {
    fn from(exit: Exit) -> Self {
        ExitCode::from(exit.code())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The codes in README.md's exit-code table, in order.
    fn documented() -> Vec<u8> {
        include_str!("../README.md")
            .lines()
            .skip_while(|l| !l.starts_with("| Code |"))
            .skip(2)
            .take_while(|l| l.starts_with('|'))
            .map(|l| {
                let cell = l.split('|').nth(1).unwrap().trim();
                cell.parse()
                    .unwrap_or_else(|_| panic!("not an exit code: {cell}"))
            })
            .collect()
    }

    #[test]
    fn the_documented_table_is_the_implemented_one() {
        let implemented: Vec<u8> = Exit::ALL.iter().map(|e| e.code()).collect();
        assert_eq!(documented(), implemented);
        assert_eq!(implemented, [0, 1, 2, 3, 4, 5, 6, 7, 8, 130]);
    }
}
