//! Diagnostics and how they are printed (0004 §6): a severity, a stable code,
//! a message, the original file and line, the field path and an optional
//! help line. As text for people, or as JSON lines (`--format json`) for CI
//! and editors. Every byte goes through the redaction pass last (0004 §4).

use std::io::{self, Write};
use std::path::Path;

use clap::ValueEnum;
use ferry_dsl::config::{ConfigError, ErrorKind, Location, mask_urls};
use ferry_registry::Redactor;
use ferry_registry::secret::url_password;
use serde_json::json;

/// Stable diagnostic codes. Docs and CI refer to them, so a code keeps its
/// meaning once released; new checks get new codes.
pub(crate) mod code {
    /// `--config` names nothing, or a file instead of the `ferry/` directory.
    pub const CONFIG_DIR: &str = "F0001";
    /// The binary registered its connectors wrongly (a bug in the binary).
    pub const MISWIRED: &str = "F0002";
    /// The command exists but its owning ticket hasn't landed. No command
    /// uses it any more; it stays reserved so it never means anything else.
    #[allow(dead_code)]
    pub const NOT_IMPLEMENTED: &str = "F0003";
    /// Output (e.g. `ferry schema --out`) could not be written.
    pub const OUTPUT: &str = "F0004";

    // F01xx: the config loader (ferry-dsl), one code per error kind.
    pub const LOAD_IO: &str = "F0101";
    pub const LOAD_YAML: &str = "F0102";
    pub const LOAD_LAYOUT: &str = "F0103";
    pub const LOAD_INCLUDE: &str = "F0104";
    pub const LOAD_MODEL: &str = "F0105";
    pub const LOAD_DUPLICATE: &str = "F0106";
    pub const LOAD_INVALID: &str = "F0107";
    pub const LOAD_ENV: &str = "F0108";
    pub const LOAD_DEPENDS_ON: &str = "F0109";
    /// An expression doesn't parse or names nothing in scope (B2).
    pub const EXPR: &str = "F0110";
    /// An expression's type doesn't fit (B3), found by `plan`.
    pub const TYPE: &str = "F0111";
    /// The planner refused the config (B7): a cycle between flows, no
    /// idempotency strategy, a per-flow setting the runner can't apply.
    pub const PLAN: &str = "F0112";
    /// A loader error kind this binary doesn't know yet.
    pub const LOAD_OTHER: &str = "F0100";

    // F03xx: connections, checked through the registry (0004).
    pub const OPTIONS: &str = "F0301";
    pub const UNKNOWN_TYPE: &str = "F0302";
    pub const ROLE: &str = "F0303";
    pub const STATE_TYPE: &str = "F0304";
    pub const STATE_NOT_CAPABLE: &str = "F0305";
    pub const BUILD: &str = "F0306";

    /// A secret field holds a literal value instead of `${ENV}`.
    pub const SECRET_LITERAL: &str = "W0301";

    /// Something the plan points out that doesn't stop a run.
    pub const PLAN_WARNING: &str = "W0401";

    // F05xx: reaching the databases.
    /// A source couldn't be discovered or a sink asked for its
    /// capabilities, or the state store couldn't be reached.
    pub const CONNECT: &str = "F0501";

    // F06xx: running.
    /// The runner couldn't run the flows at all.
    pub const RUN: &str = "F0601";
    /// A partition failed, or a flow was blocked by one that did.
    pub const PARTITION_FAILED: &str = "F0602";
    /// Ctrl-C.
    pub const INTERRUPTED: &str = "F0603";
    /// `FERRY_LEASE_TTL_MS` overrides the lease: for tests only.
    pub const LEASE_OVERRIDE: &str = "W0601";

    /// Verification found a problem, or couldn't run (A8).
    pub const VERIFY_FAILED: &str = "F0701";
    /// A sink this binary can't read back: its flows are not verified.
    pub const UNVERIFIED: &str = "W0701";

    /// `resume`/`retry`: no such run, or it is pinned to another config or
    /// engine version (A4).
    pub const RESUME_REFUSED: &str = "F0801";
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Severity {
    Error,
    Warning,
}

impl Severity {
    fn as_str(self) -> &'static str {
        match self {
            Severity::Error => "error",
            Severity::Warning => "warning",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Diagnostic {
    pub severity: Severity,
    pub code: &'static str,
    pub message: String,
    /// Relative to the config directory, as the loader reports it.
    pub location: Option<Location>,
    /// The field path, e.g. `connections[warehouse].type`; may be empty.
    pub path: String,
    pub help: Option<String>,
}

impl Diagnostic {
    pub fn error(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Error,
            code,
            message: message.into(),
            location: None,
            path: String::new(),
            help: None,
        }
    }

    pub fn warning(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Warning,
            ..Self::error(code, message)
        }
    }

    pub fn at(mut self, location: Option<&Location>) -> Self {
        self.location = location.cloned();
        self
    }

    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    pub fn help(mut self, help: impl Into<String>) -> Self {
        self.help = Some(help.into());
        self
    }

    pub fn is_error(&self) -> bool {
        self.severity == Severity::Error
    }
}

impl From<ConfigError> for Diagnostic {
    fn from(error: ConfigError) -> Self {
        let code = match error.kind {
            ErrorKind::Io => code::LOAD_IO,
            ErrorKind::Yaml => code::LOAD_YAML,
            ErrorKind::Layout => code::LOAD_LAYOUT,
            ErrorKind::Include => code::LOAD_INCLUDE,
            ErrorKind::Model => code::LOAD_MODEL,
            ErrorKind::Duplicate => code::LOAD_DUPLICATE,
            ErrorKind::Invalid => code::LOAD_INVALID,
            ErrorKind::Env => code::LOAD_ENV,
            ErrorKind::DependsOn => code::LOAD_DEPENDS_ON,
            ErrorKind::Expr => code::EXPR,
            ErrorKind::Type => code::TYPE,
            ErrorKind::Plan => code::PLAN,
            _ => code::LOAD_OTHER,
        };
        Self {
            severity: Severity::Error,
            code,
            message: error.message,
            location: error.location,
            path: error.path,
            help: error.help,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum Format {
    #[default]
    Text,
    /// One JSON object per line.
    Json,
}

/// Every value that must never be printed, and the pass that removes them.
#[derive(Clone, Debug, Default)]
pub(crate) struct Redaction {
    secrets: Redactor,
}

impl Redaction {
    /// A value substituted from the environment or known to be a secret, and
    /// the password of any URL it is.
    pub fn add(&mut self, value: &str) {
        self.secrets.extend(url_password(value));
        self.secrets.add(value);
    }

    /// The last step before any text is written: known secrets, then any
    /// password still left in a URL.
    pub fn apply(&self, text: &str) -> String {
        mask_urls(&self.secrets.redact(text))
    }
}

/// Writes command output to `out` and diagnostics to `err`, redacted.
pub(crate) struct Printer<'a> {
    pub out: &'a mut dyn Write,
    pub err: &'a mut dyn Write,
    pub format: Format,
    pub color: bool,
    pub redaction: Redaction,
}

impl Printer<'_> {
    /// Command output (stdout), redacted.
    pub fn output(&mut self, text: &str) -> io::Result<()> {
        let text = self.redaction.apply(text);
        self.out.write_all(text.as_bytes())?;
        self.out.flush()
    }

    /// A line for people on stderr, redacted.
    pub fn note(&mut self, text: &str) -> io::Result<()> {
        let text = self.redaction.apply(text);
        self.err.write_all(text.as_bytes())?;
        self.err.flush()
    }

    /// Diagnostics (stderr), redacted. `config_dir` turns locations into
    /// paths the user can open, and is where source lines are read from.
    pub fn diagnostics(&mut self, diagnostics: &[Diagnostic], config_dir: &Path) -> io::Result<()> {
        let mut text = String::new();
        for d in diagnostics {
            match self.format {
                Format::Text => render_text(d, config_dir, self.color, &mut text),
                Format::Json => {
                    text.push_str(&render_json(d, config_dir).to_string());
                    text.push('\n');
                }
            }
        }
        let text = self.redaction.apply(&text);
        self.err.write_all(text.as_bytes())?;
        self.err.flush()
    }
}

fn display_file(config_dir: &Path, location: &Location) -> String {
    config_dir.join(&*location.file).display().to_string()
}

fn render_json(d: &Diagnostic, config_dir: &Path) -> serde_json::Value {
    json!({
        "severity": d.severity.as_str(),
        "code": d.code,
        "message": d.message,
        "file": d.location.as_ref().map(|l| display_file(config_dir, l)),
        "line": d.location.as_ref().map(|l| l.line),
        "column": d.location.as_ref().map(|l| l.column),
        "path": (!d.path.is_empty()).then_some(&d.path),
        "help": d.help,
    })
}

/// ```text
/// error[F0302]: connections[warehouse].type: unknown connection type `postgresql`
///   --> ferry/connections.yaml:10:11
///    |
/// 10 |     type: postgresql
///    |           ^^^^^^^^^^
///    = help: registered types: `mongo`, `postgres`
/// ```
fn render_text(d: &Diagnostic, config_dir: &Path, color: bool, out: &mut String) {
    let paint = |style: &str, text: &str| match color {
        true => format!("\x1b[{style}m{text}\x1b[0m"),
        false => text.to_owned(),
    };
    let label = match d.severity {
        Severity::Error => paint("1;31", &format!("error[{}]", d.code)),
        Severity::Warning => paint("1;33", &format!("warning[{}]", d.code)),
    };
    out.push_str(&label);
    out.push_str(": ");
    if !d.path.is_empty() {
        out.push_str(&d.path);
        out.push_str(": ");
    }
    out.push_str(&d.message);
    out.push('\n');

    let mut gutter = 2;
    if let Some(location) = &d.location {
        let line = source_line(config_dir, location);
        gutter = line
            .as_ref()
            .map_or(1, |_| location.line.to_string().len())
            .max(1);
        let pad = " ".repeat(gutter);
        out.push_str(&format!(
            "{pad}{} {}:{}:{}\n",
            paint("1;34", "-->"),
            display_file(config_dir, location),
            location.line,
            location.column
        ));
        if let Some(text) = line {
            let bar = paint("1;34", "|");
            let start = (location.column as usize).saturating_sub(1);
            let width = text
                .chars()
                .skip(start)
                .take_while(|c| !c.is_whitespace())
                .count()
                .max(1);
            out.push_str(&format!("{pad} {bar}\n"));
            out.push_str(&format!(
                "{} {bar} {text}\n",
                paint("1;34", &location.line.to_string())
            ));
            out.push_str(&format!(
                "{pad} {bar} {}{}\n",
                " ".repeat(start),
                paint("1;31", &"^".repeat(width))
            ));
        }
    }
    if let Some(help) = &d.help {
        out.push_str(&format!(
            "{} {} help: {help}\n",
            " ".repeat(gutter),
            paint("1;34", "=")
        ));
    }
}

/// The text of the line `location` points at, if the file can be read.
fn source_line(config_dir: &Path, location: &Location) -> Option<String> {
    let text = std::fs::read_to_string(config_dir.join(&*location.file)).ok()?;
    let line = text.lines().nth((location.line as usize).checked_sub(1)?)?;
    Some(line.replace('\t', " "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_points_at_the_line_and_underlines_the_token() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("connections.yaml"),
            "connections:\n  - name: warehouse\n    type: postgresql\n",
        )
        .unwrap();
        let d = Diagnostic::error(code::UNKNOWN_TYPE, "unknown connection type `postgresql`")
            .at(Some(&Location::new("connections.yaml", 3, 11)))
            .path("connections[warehouse].type")
            .help("registered types: `postgres`");
        let mut text = String::new();
        render_text(&d, dir.path(), false, &mut text);
        let file = dir.path().join("connections.yaml").display().to_string();
        assert_eq!(
            text,
            format!(
                "error[F0302]: connections[warehouse].type: unknown connection type `postgresql`\n \
                 --> {file}:3:11\n  \
                 |\n\
                 3 |     type: postgresql\n  \
                 |           ^^^^^^^^^^\n  \
                 = help: registered types: `postgres`\n"
            )
        );
    }

    #[test]
    fn text_without_a_readable_file_still_names_file_and_line() {
        let d = Diagnostic::error(code::LOAD_YAML, "bad indentation").at(Some(&Location::new(
            "flows/orders.yaml",
            4,
            2,
        )));
        let mut text = String::new();
        render_text(&d, Path::new("/nonexistent/ferry"), false, &mut text);
        assert_eq!(
            text,
            "error[F0102]: bad indentation\n --> /nonexistent/ferry/flows/orders.yaml:4:2\n"
        );
    }

    #[test]
    fn json_has_every_field_and_nulls_for_missing_ones() {
        let d = Diagnostic::warning(code::SECRET_LITERAL, "literal secret");
        assert_eq!(
            render_json(&d, Path::new("ferry")),
            json!({
                "severity": "warning", "code": "W0301", "message": "literal secret",
                "file": null, "line": null, "column": null, "path": null, "help": null,
            })
        );
    }

    #[test]
    fn redaction_masks_values_their_url_passwords_and_any_other_url_password() {
        let mut r = Redaction::default();
        r.add("postgres://app:hunter2@db/shop");
        assert_eq!(
            r.apply("url postgres://app:hunter2@db/shop, password hunter2"),
            "url ***, password ***"
        );
        assert_eq!(
            r.apply("other mongodb://u:pw@host/x"),
            "other mongodb://u:***@host/x"
        );
        // As a JSON string, a quote in a secret is escaped; still masked.
        r.add(r#"pa"ss"#);
        assert_eq!(
            r.apply(&json!({"m": r#"pa"ss"#}).to_string()),
            r#"{"m":"***"}"#
        );
    }
}
