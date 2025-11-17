//! The run report: `report.json` for tools, `report.md` for people.
//!
//! The report holds the config hash, which is computed before `${ENV}`
//! substitution (B1c), connection names and types, and counts and
//! checksums. It never holds a connection's fields, so no substituted value
//! can reach it through the model; error messages from the target pass
//! through the CLI's [`Redactor`] before anything is written, as a
//! backstop for drivers that echo a connection string.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ferry_core::Result;
use ferry_registry::Redactor;
use serde_json::json;

use crate::verifier_error;
use crate::verify::{Constraint, Status, Verification};

/// What verification proves, printed at the top of every report.
pub const PROVES: &str = "Verification proves that what the run computed is what \
landed: every row the runner handed the sink for a done partition is in the target, \
exactly once, with the same values after normalization, and the target holds no rows \
the run didn't write. Counts are what the run read, recorded in the state store at \
read time, so changes to the source after it was read don't affect them.";

/// What it does not prove.
pub const DOES_NOT_PROVE: &str = "It does not prove the mapping is correct: a wrong \
expression computes wrong values, and they land faithfully. Only `expect:` assertions \
check the mapping. Differences below a target column's timestamp precision, between \
equal numbers of different width or scale, and in JSON key order or whitespace are \
normalized away. The checksum covers whole tables, so it assumes a full load into \
empty targets: rows already in a table count as rows the run didn't write.";

/// The report's format, for tools reading `report.json`.
pub const FORMAT: &str = "ferry-report/1";

/// When the run itself started and finished, as the caller timed it.
#[derive(Debug, Clone, Copy)]
pub struct RunTiming {
    pub started: SystemTime,
    pub finished: SystemTime,
}

pub struct Report<'a> {
    verification: &'a Verification,
    run: Option<RunTiming>,
}

impl<'a> Report<'a> {
    pub fn new(verification: &'a Verification) -> Self {
        Self {
            verification,
            run: None,
        }
    }

    pub fn with_run_timing(mut self, timing: RunTiming) -> Self {
        self.run = Some(timing);
        self
    }

    /// `report.json`.
    pub fn to_json(&self) -> String {
        let v = self.verification;
        let timing = |started: SystemTime, finished: SystemTime| {
            json!({
                "started": rfc3339(started),
                "finished": rfc3339(finished),
                "seconds": seconds(started, finished),
            })
        };
        let report = json!({
            "format": FORMAT,
            "status": v.status,
            "proves": PROVES,
            "does_not_prove": DOES_NOT_PROVE,
            "run": v.run,
            "versions": {
                "engine": v.run.engine_version,
                "ferry_verify": env!("CARGO_PKG_VERSION"),
            },
            "target": v.target,
            "timings": {
                "run": self.run.map(|t| timing(t.started, t.finished)),
                "verification": timing(v.started, v.finished),
            },
            "partitions": v.partitions,
            "counts": v.counts,
            "tables": v.tables,
            "constraints": v.constraints,
            "assertions": v.assertions,
            "finalize": v.finalize,
            "dead_letter": dead_letter(v),
            "problems": v.problems,
        });
        serde_json::to_string_pretty(&report).expect("a JSON value serializes")
    }

    /// `report.md`.
    pub fn to_markdown(&self) -> String {
        let v = self.verification;
        let mut md = String::new();
        let status = match v.status {
            Status::Passed => "PASSED",
            Status::Failed => "FAILED",
            Status::Incomplete => "INCOMPLETE",
        };
        let _ = writeln!(md, "# Run {}: verification {status}\n", v.run.id);
        let _ = writeln!(md, "{PROVES}\n\n{DOES_NOT_PROVE}\n");

        let _ = writeln!(md, "## Run\n");
        let _ = writeln!(md, "- Config hash: `{}`", v.run.config_hash);
        let _ = writeln!(md, "- Engine version: {}", v.run.engine_version);
        let _ = writeln!(md, "- Verifier version: {}", env!("CARGO_PKG_VERSION"));
        let _ = writeln!(md, "- Target: {}", v.target);
        for c in &v.run.connections {
            let _ = writeln!(md, "- Connection `{}`: {}", c.name, c.kind);
        }
        if let Some(t) = self.run {
            let _ = writeln!(
                md,
                "- Run: {} to {} ({:.1}s)",
                rfc3339(t.started),
                rfc3339(t.finished),
                seconds(t.started, t.finished)
            );
        }
        let _ = writeln!(
            md,
            "- Verification: {} to {} ({:.1}s)",
            rfc3339(v.started),
            rfc3339(v.finished),
            seconds(v.started, v.finished)
        );
        let p = &v.partitions;
        let _ = writeln!(
            md,
            "- Partitions: {} done, {} failed, {} pending, {} leased\n",
            p.done, p.failed, p.pending, p.leased
        );

        if !v.problems.is_empty() {
            let _ = writeln!(md, "## Problems\n");
            for problem in &v.problems {
                let _ = writeln!(md, "- {problem}");
            }
            md.push('\n');
        }

        let _ = writeln!(md, "## Counts\n");
        let _ = writeln!(
            md,
            "{} driver rows read, {} dead-lettered (from the state store).\n",
            v.counts.read, v.counts.dead_lettered
        );
        let _ = writeln!(
            md,
            "| Flow | Read | Written | Filtered | Dead-lettered | Unaccounted |"
        );
        let _ = writeln!(md, "|---|---:|---:|---:|---:|---:|");
        for f in &v.counts.flows {
            let _ = writeln!(
                md,
                "| {} | {} | {} | {} | {} | {} |",
                f.flow,
                f.read,
                f.records.written,
                f.records.filtered,
                f.records.rejected,
                f.unaccounted
            );
        }

        let _ = writeln!(md, "\n## Tables\n");
        let _ = writeln!(
            md,
            "| Table | Committed | Written | Read back | Checksum | Result |"
        );
        let _ = writeln!(md, "|---|---:|---:|---:|---|---|");
        for t in &v.tables {
            let read_back = t
                .read_back
                .map_or_else(|| "error".to_owned(), |c| c.rows.to_string());
            let matches = match t.read_back {
                Some(c) if c.sum == t.at_write.sum => "matches",
                Some(_) => "**differs**",
                None => "n/a",
            };
            let _ = writeln!(
                md,
                "| {} | {} | {} | {read_back} | {matches} | {} |",
                t.table,
                t.written,
                t.at_write.rows,
                pass(t.passed)
            );
        }
        for t in v.tables.iter().filter(|t| !t.timestamp_digits.is_empty()) {
            let columns: Vec<String> = t
                .timestamp_digits
                .iter()
                .map(|(c, d)| format!("`{c}` at {d} digits"))
                .collect();
            let _ = writeln!(
                md,
                "\n`{}`: timestamps compared as the target stores them: {}.",
                t.table,
                columns.join(", ")
            );
        }

        let _ = writeln!(md, "\n## Constraints\n");
        if v.constraints.is_empty() {
            let _ = writeln!(md, "No constraints declared.");
        } else {
            let _ = writeln!(md, "| Table | Constraint | Violations | Result |");
            let _ = writeln!(md, "|---|---|---:|---|");
            for c in &v.constraints {
                let what = match &c.constraint {
                    Constraint::NotNull { column } => format!("`{column}` not null"),
                    Constraint::Unique { columns } => format!("unique ({})", columns.join(", ")),
                    Constraint::ForeignKey {
                        columns,
                        references_table,
                        references_columns,
                    } => format!(
                        "({}) references {references_table} ({})",
                        columns.join(", "),
                        references_columns.join(", ")
                    ),
                };
                let violations = c
                    .violations
                    .map_or_else(|| "error".to_owned(), |n| n.to_string());
                let _ = writeln!(
                    md,
                    "| {} | {what} | {violations} | {} |",
                    c.table,
                    pass(c.passed)
                );
            }
        }

        let _ = writeln!(md, "\n## Assertions\n");
        if v.assertions.is_empty() {
            let _ = writeln!(md, "No `expect:` assertions.");
        } else {
            let _ = writeln!(md, "| Flow | Name | Assertion | Values | Result |");
            let _ = writeln!(md, "|---|---|---|---|---|");
            for a in &v.assertions {
                let values = match (&a.outcome, &a.error) {
                    (Some(o), _) => format!("{} {} {}", o.left, o.op, o.right),
                    (None, Some(e)) => format!("error: {e}"),
                    (None, None) => String::new(),
                };
                let _ = writeln!(
                    md,
                    "| {} | {} | `{}` | {values} | {} |",
                    a.flow,
                    a.name,
                    a.expression,
                    pass(a.passed)
                );
            }
        }

        if !v.finalize.is_empty() {
            let _ = writeln!(md, "\n## Finalize\n");
            let _ = writeln!(
                md,
                "Indexes and constraints the load dropped or the target declares, that \
                 couldn't be restored as they were.\n"
            );
            let _ = writeln!(md, "| Flow | Table | Object | Problem | Orphaned rows |");
            let _ = writeln!(md, "|---|---|---|---|---|");
            for f in &v.finalize {
                let orphans = match f.orphans {
                    Some(n) => format!("{n}: {}", f.orphan_keys.join("; ")),
                    None => String::new(),
                };
                let _ = writeln!(
                    md,
                    "| {} | {} | {} | {} | {orphans} |",
                    f.flow, f.table, f.object, f.message
                );
            }
        }

        let d = &v.dead_letters;
        let _ = writeln!(md, "\n## Dead letters\n");
        let _ = writeln!(
            md,
            "{} records dead-lettered (from the state store); `{}` holds {} rows for this \
             run: {}.",
            v.counts.dead_lettered,
            ferry_runtime::DLQ_TABLE,
            d.rows,
            pass(d.passed)
        );
        if let Some(e) = &d.error {
            let _ = writeln!(md, "\nCouldn't read them: {e}");
        }
        if !d.reasons.is_empty() {
            let _ = writeln!(
                md,
                "\n| Flow | Target | Error | Records | Example `source_id` |"
            );
            let _ = writeln!(md, "|---|---|---|---|---|");
            for r in &d.reasons {
                let _ = writeln!(
                    md,
                    "| {} | {} | {} | {} | `{}` |",
                    r.flow,
                    r.target.as_deref().unwrap_or(""),
                    r.error,
                    r.count,
                    r.example
                );
            }
        }
        md
    }

    /// Writes `report.json` and `report.md` into `dir`, creating it, with
    /// every secret `redactor` knows replaced by `***`.
    pub fn write(&self, dir: &Path, redactor: &Redactor) -> Result<(PathBuf, PathBuf)> {
        std::fs::create_dir_all(dir)
            .map_err(|e| verifier_error(format!("{}: {e}", dir.display())))?;
        let json = dir.join("report.json");
        let md = dir.join("report.md");
        for (path, text) in [(&json, self.to_json()), (&md, self.to_markdown())] {
            std::fs::write(path, redactor.redact(&text).as_bytes())
                .map_err(|e| verifier_error(format!("{}: {e}", path.display())))?;
        }
        Ok((json, md))
    }
}

fn pass(passed: bool) -> &'static str {
    if passed { "pass" } else { "**fail**" }
}

fn dead_letter(v: &Verification) -> serde_json::Value {
    let d = &v.dead_letters;
    json!({
        "table": ferry_runtime::DLQ_TABLE,
        "dead_lettered": v.counts.dead_lettered,
        "rows": d.rows,
        "reasons": d.reasons,
        "error": d.error,
        "passed": d.passed,
    })
}

fn seconds(started: SystemTime, finished: SystemTime) -> f64 {
    finished
        .duration_since(started)
        .unwrap_or(Duration::ZERO)
        .as_secs_f64()
}

/// UTC, to the millisecond: `2026-10-17T03:04:27.462Z`.
pub(crate) fn rfc3339(t: SystemTime) -> String {
    let since = t.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let secs = since.as_secs();
    let (days, rem) = (secs / 86_400, secs % 86_400);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3_600,
        rem % 3_600 / 60,
        rem % 60,
        since.subsec_millis()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_utc_timestamps() {
        let at = |secs: u64, millis: u64| {
            rfc3339(UNIX_EPOCH + Duration::from_secs(secs) + Duration::from_millis(millis))
        };
        assert_eq!(at(0, 0), "1970-01-01T00:00:00.000Z");
        assert_eq!(at(951_782_400, 5), "2000-02-29T00:00:00.005Z");
        assert_eq!(at(1_792_206_267, 462), "2026-10-17T03:04:27.462Z");
    }
}
