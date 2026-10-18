//! The `dataferry` command line as a library (0004 §5), so a third party can
//! ship its own binary with extra connectors:
//!
//! ```no_run
//! use std::process::ExitCode;
//!
//! use dataferry::Ferry;
//! # struct ClickHouse;
//! # impl dataferry::registry::Connector for ClickHouse {
//! #     const TYPE: &'static str = "clickhouse";
//! #     const ROLES: dataferry::registry::Roles = dataferry::registry::Roles::SINK;
//! #     type Options = serde_json::Map<String, serde_json::Value>;
//! #     fn build(&self, _: &dataferry::registry::BuildContext<'_>, _: Self::Options, _: dataferry::registry::Roles)
//! #         -> Result<dataferry::registry::Endpoints, dataferry::registry::BuildError> { unimplemented!() }
//! #     fn describe(&self, _: &Self::Options) -> String { String::new() }
//! # }
//!
//! fn main() -> ExitCode {
//!     Ferry::with_builtins()
//!         .register(ClickHouse)
//!         .name("acme-dataferry")
//!         .main()
//! }
//! ```
//!
//! The builder and the subcommands are the whole stable surface in v0.1.
//! The command functions stay private until the planner and runner types
//! have run on real data.

mod builtins;
mod cli;
mod diagnostic;
mod exit;
mod leases;
mod namespaces;
mod plan;
mod run;
mod schema;
mod targets;
mod validate;

use std::ffi::OsString;
use std::io::{IsTerminal, Write};
use std::path::Path;
use std::process::ExitCode;

use clap::error::ErrorKind as ClapErrorKind;
use clap::{CommandFactory, FromArgMatches};
pub use dataferry_registry as registry;
use registry::{Connector, ConnectorRegistry, RegisterError, StateBackend, StateCapable};

use crate::cli::{Cli, Color, Command};
use crate::diagnostic::{Diagnostic, Format, Printer, Redaction, code};
pub use crate::exit::Exit;

/// A `dataferry` command line: a connector registry, a name and a version.
pub struct Ferry {
    registry: ConnectorRegistry,
    /// How `run` verifies each sink type it can read back (A8).
    targets: targets::Targets,
    /// The namespace each connection type's entities may be qualified
    /// with in a flow (`app.public.orders`).
    namespaces: namespaces::Namespaces,
    name: &'static str,
    version: &'static str,
    /// Registration mistakes, reported when the binary starts.
    miswired: Vec<RegisterError>,
}

impl Default for Ferry {
    fn default() -> Self {
        Self::new()
    }
}

impl Ferry {
    /// No connectors registered.
    pub fn new() -> Self {
        Self {
            registry: ConnectorRegistry::new(),
            targets: targets::Targets::default(),
            namespaces: namespaces::Namespaces::default(),
            name: "dataferry",
            version: env!("CARGO_PKG_VERSION"),
            miswired: Vec::new(),
        }
    }

    /// The built-in `postgres` and `mongo` connectors, each behind a
    /// default-on cargo feature of the same name.
    pub fn with_builtins() -> Self {
        builtins::register(Self::new())
    }

    /// Adds a connector type. Registering a type twice, or one whose options
    /// use a reserved key, is a bug in the binary: every command then fails
    /// at startup, naming it, with exit code 1.
    pub fn register<C: Connector>(mut self, connector: C) -> Self {
        if let Err(e) = self.registry.try_register(connector) {
            self.miswired.push(e);
        }
        self
    }

    /// Adds a connector that can hold the state store, with its backend
    /// (0004 §8). Same rules as [`register`](Self::register).
    pub fn register_state_capable<C, B>(mut self, connector: C, backend: B) -> Self
    where
        C: StateCapable,
        B: StateBackend<Pool = C::Pool, Conn = C::Conn>,
    {
        if let Err(e) = self.registry.try_register_state_capable(connector, backend) {
            self.miswired.push(e);
        }
        self
    }

    /// Replaces a registered type on purpose, e.g. a patched postgres. The
    /// type must already be registered, and must not be state-capable (use
    /// [`replace_state_capable`](Self::replace_state_capable)).
    pub fn replace<C: Connector>(mut self, connector: C) -> Self {
        if let Err(e) = self.registry.try_replace(connector) {
            self.miswired.push(e);
        }
        self
    }

    /// [`replace`](Self::replace) for a state-capable type.
    pub fn replace_state_capable<C, B>(mut self, connector: C, backend: B) -> Self
    where
        C: StateCapable,
        B: StateBackend<Pool = C::Pool, Conn = C::Conn>,
    {
        if let Err(e) = self.registry.try_replace_state_capable(connector, backend) {
            self.miswired.push(e);
        }
        self
    }

    /// The name used in help text and error messages; defaults to "dataferry".
    pub fn name(mut self, name: &'static str) -> Self {
        self.name = name;
        self
    }

    /// The version `--version` prints; defaults to dataferry's own.
    pub fn version(mut self, version: &'static str) -> Self {
        self.version = version;
        self
    }

    /// Parses `std::env::args`, runs the command and renders errors. Never
    /// panics on user error.
    pub fn main(self) -> ExitCode {
        let terminal = std::io::stderr().is_terminal();
        let mut out = std::io::stdout().lock();
        let mut err = std::io::stderr().lock();
        self.run(std::env::args_os(), &mut out, &mut err, terminal)
            .into()
    }

    /// Same as [`main`](Self::main), with explicit arguments (the first is
    /// the program name) and output, for tests.
    pub fn run_with(
        self,
        args: impl IntoIterator<Item = OsString>,
        out: &mut dyn Write,
        err: &mut dyn Write,
    ) -> ExitCode {
        self.run(args, out, err, false).into()
    }

    fn run(
        self,
        args: impl IntoIterator<Item = OsString>,
        out: &mut dyn Write,
        err: &mut dyn Write,
        terminal: bool,
    ) -> Exit {
        if !self.miswired.is_empty() {
            let diagnostics: Vec<Diagnostic> = self
                .miswired
                .iter()
                .map(|e| {
                    Diagnostic::error(code::MISWIRED, format!("this binary is miswired: {e}"))
                        .help("a bug in how the binary registers its connectors; report it to its authors")
                })
                .collect();
            let mut printer = Printer {
                out,
                err,
                format: Format::Text,
                color: false,
                redaction: Redaction::default(),
            };
            let _ = printer.diagnostics(&diagnostics, Path::new("."));
            return Exit::Internal;
        }

        let command = Cli::command()
            .name(self.name)
            .bin_name(self.name)
            .version(self.version);
        let cli = match command
            .try_get_matches_from(args)
            .and_then(|m| Cli::from_arg_matches(&m))
        {
            Ok(cli) => cli,
            Err(e) => {
                let text = e.render().to_string();
                return match e.kind() {
                    ClapErrorKind::DisplayHelp | ClapErrorKind::DisplayVersion => {
                        let _ = out.write_all(text.as_bytes());
                        Exit::Success
                    }
                    _ => {
                        let _ = err.write_all(text.as_bytes());
                        Exit::Usage
                    }
                };
            }
        };

        let color = match cli.color {
            Color::Always => true,
            Color::Never => false,
            Color::Auto => terminal && std::env::var_os("NO_COLOR").is_none_or(|v| v.is_empty()),
        };
        let mut printer = Printer {
            out,
            err,
            format: cli.format,
            color,
            redaction: Redaction::default(),
        };
        // Connectors may need a runtime even to build (a driver client that
        // spawns its background tasks lazily), and plan/run are async.
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(e) => {
                let d = Diagnostic::error(
                    code::OUTPUT,
                    format!("could not start the async runtime: {e}"),
                );
                let _ = printer.diagnostics(&[d], &cli.config);
                return Exit::Internal;
            }
        };
        let exit = {
            let _guard = runtime.enter();
            commands::run(&self, &cli, &mut printer)
        };
        // A blocking task still running (a merge's stage being sorted when
        // Ctrl-C stopped the run) would otherwise hold the exit until it
        // ends. A few seconds lets a state store write in flight finish.
        runtime.shutdown_timeout(std::time::Duration::from_secs(5));
        exit
    }
}

mod commands {
    use super::*;

    pub(super) fn run(ferry: &Ferry, cli: &Cli, printer: &mut Printer<'_>) -> Exit {
        match &cli.command {
            Command::Validate => validate(ferry, cli, printer),
            Command::Schema { out } => schema(ferry, cli, out.as_deref(), printer),
            Command::Plan => plan(ferry, cli, printer),
            Command::Run => block_on(run::run(ferry, cli, printer, run::Start::New)),
            Command::Resume { run_id } => block_on(run::run(
                ferry,
                cli,
                printer,
                run::Start::Resume(run_id.clone()),
            )),
            Command::Retry { failed } => block_on(run::run(
                ferry,
                cli,
                printer,
                run::Start::Retry(failed.clone()),
            )),
        }
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Handle::current().block_on(future)
    }

    fn plan(ferry: &Ferry, cli: &Cli, printer: &mut Printer<'_>) -> Exit {
        let planned = block_on(plan::plan(
            ferry,
            &cli.config,
            &|name| std::env::var(name).ok(),
            &mut printer.redaction,
        ));
        match planned {
            Ok((planned, warnings)) => {
                let shown: Vec<_> = warnings.into_iter().filter(|_| !cli.quiet).collect();
                let _ = printer.diagnostics(&shown, &cli.config);
                let _ = printer.output(&plan::render(&planned));
                Exit::Success
            }
            Err(refused) => {
                let _ = printer.diagnostics(&refused.diagnostics, &cli.config);
                refused.exit
            }
        }
    }

    fn validate(ferry: &Ferry, cli: &Cli, printer: &mut Printer<'_>) -> Exit {
        let report = validate::validate(
            &ferry.registry,
            &cli.config,
            &|name| std::env::var(name).ok(),
            &mut printer.redaction,
        );
        let shown: Vec<_> = report
            .diagnostics
            .iter()
            .filter(|d| d.is_error() || !cli.quiet)
            .cloned()
            .collect();
        let _ = printer.diagnostics(&shown, &cli.config);
        let errors = report.diagnostics.iter().filter(|d| d.is_error()).count();
        let warnings = report.diagnostics.len() - errors;
        let exit = report.exit();
        if cli.quiet {
            return exit;
        }
        let text = match (printer.format, &report.valid) {
            (Format::Json, valid) => {
                let mut summary = serde_json::json!({
                    "valid": valid.is_some(),
                    "errors": errors,
                    "warnings": warnings,
                });
                if let Some(s) = valid {
                    summary["config"] = s.name.clone().into();
                    summary["flows"] = s.flows.into();
                    summary["connections"] = s
                        .connections
                        .iter()
                        .map(|(name, kind, roles, description)| {
                            serde_json::json!({
                                "name": name, "type": kind,
                                "used_as": roles.to_string(), "description": description,
                            })
                        })
                        .collect();
                }
                format!("{summary}\n")
            }
            (Format::Text, Some(s)) => {
                let mut text = format!(
                    "`{}` is valid: {} connection{}, {} flow{}{}\n",
                    s.name,
                    s.connections.len(),
                    plural(s.connections.len()),
                    s.flows,
                    plural(s.flows),
                    match warnings {
                        0 => String::new(),
                        n => format!(", {n} warning{}", plural(n)),
                    }
                );
                if cli.verbose > 0 {
                    for (name, kind, roles, description) in &s.connections {
                        text.push_str(&format!(
                            "  {name} ({kind}, used as {roles}): {description}\n"
                        ));
                    }
                }
                text
            }
            (Format::Text, None) => {
                let _ = printer.note(&format!(
                    "config is invalid: {errors} error{}\n",
                    plural(errors)
                ));
                return exit;
            }
        };
        let _ = printer.output(&text);
        exit
    }

    fn schema(ferry: &Ferry, cli: &Cli, out: Option<&Path>, printer: &mut Printer<'_>) -> Exit {
        let documents = schema::documents(&ferry.registry);
        let Some(dir) = out else {
            let _ = printer.output(&schema::render_all(&documents));
            return Exit::Success;
        };
        let write = || -> std::io::Result<Vec<String>> {
            std::fs::create_dir_all(dir)?;
            documents
                .iter()
                .map(|(name, doc)| {
                    let path = dir.join(name);
                    std::fs::write(&path, schema::render(doc))?;
                    Ok(path.display().to_string())
                })
                .collect()
        };
        match write() {
            Ok(paths) => {
                if !cli.quiet {
                    let text: String = paths.iter().map(|p| format!("wrote {p}\n")).collect();
                    let _ = printer.output(&text);
                }
                Exit::Success
            }
            Err(e) => {
                let d = Diagnostic::error(
                    code::OUTPUT,
                    format!("could not write the schemas to `{}`: {e}", dir.display()),
                );
                let _ = printer.diagnostics(&[d], &cli.config);
                Exit::Internal
            }
        }
    }

    fn plural(n: usize) -> &'static str {
        if n == 1 { "" } else { "s" }
    }
}
