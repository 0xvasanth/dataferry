//! The top-level README's quickstart, run as written: its `mongoimport`
//! line loads the golden documents into a Mongo container, then every
//! `dataferry` line of its `sh` blocks runs, in order, with the variables it
//! exports. A quickstart that no longer works fails here.

mod common;

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use common::{Warehouse, ferry, output, repo};
use serde_json::Value;
use testcontainers_modules::mongo::Mongo;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

fn readme() -> String {
    std::fs::read_to_string(repo("README.md")).unwrap()
}

/// The text of the `## <title>` section, up to the next `## ` heading.
fn section<'a>(text: &'a str, title: &str) -> &'a str {
    let heading = format!("\n## {title}\n");
    let start = text
        .find(&heading)
        .unwrap_or_else(|| panic!("README.md has no `## {title}` section"))
        + heading.len();
    let rest = &text[start..];
    &rest[..rest.find("\n## ").unwrap_or(rest.len())]
}

/// Every line of the `sh` blocks in `text`, with `\` continuations joined.
fn shell_lines(text: &str) -> Vec<String> {
    let mut lines = Vec::new();
    let mut in_block = false;
    let mut pending = String::new();
    for line in text.lines() {
        match line.trim_end() {
            "```sh" => in_block = true,
            "```" if in_block => in_block = false,
            line if in_block => {
                let line = line.trim();
                if let Some(head) = line.strip_suffix('\\') {
                    pending.push_str(head);
                    continue;
                }
                pending.push_str(line);
                if !pending.is_empty() && !pending.starts_with('#') {
                    lines.push(std::mem::take(&mut pending));
                }
                pending.clear();
            }
            _ => {}
        }
    }
    assert!(!in_block, "an unclosed sh block");
    lines
}

struct Quickstart {
    /// `export NAME=value` lines, in order.
    exports: Vec<(String, String)>,
    /// The arguments of every `dataferry` line, in order.
    commands: Vec<Vec<String>>,
    /// Every other line (docker, mongoimport).
    other: Vec<String>,
}

fn quickstart() -> Quickstart {
    let text = readme();
    let mut quickstart = Quickstart {
        exports: Vec::new(),
        commands: Vec::new(),
        other: Vec::new(),
    };
    for line in shell_lines(section(&text, "Quickstart")) {
        if let Some(export) = line.strip_prefix("export ") {
            let (name, value) = export.split_once('=').expect("export NAME=value");
            quickstart.exports.push((name.into(), value.into()));
        } else if let Some(args) = line.strip_prefix("dataferry ") {
            assert!(
                !args.contains(['"', '\'', '$', '|', ';', '&']),
                "the test splits on spaces, so keep quickstart commands plain: {line}"
            );
            quickstart
                .commands
                .push(args.split_whitespace().map(str::to_owned).collect());
        } else {
            quickstart.other.push(line);
        }
    }
    quickstart
}

/// The subcommand: the first argument that isn't a flag or a flag's value.
fn subcommand(args: &[String]) -> &str {
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-c" | "--config" | "--format" | "--color" | "--work-dir" => {
                args.next();
            }
            flag if flag.starts_with('-') => {}
            command => return command,
        }
    }
    panic!("no subcommand in {args:?}")
}

/// A `docker run` line of the quickstart: its `--name`, its `-p` port
/// mappings (host → container) and its `-e` variables.
struct DockerRun {
    name: String,
    ports: Vec<(u16, u16)>,
    env: Vec<(String, String)>,
}

fn docker_run(quickstart: &Quickstart, image: &str) -> DockerRun {
    let line = quickstart
        .other
        .iter()
        .find(|l| {
            l.starts_with("docker run ") && l.split_whitespace().any(|w| w.starts_with(image))
        })
        .unwrap_or_else(|| panic!("no `docker run` of {image} in the quickstart"));
    let words: Vec<&str> = line.split_whitespace().collect();
    let mut run = DockerRun {
        name: String::new(),
        ports: Vec::new(),
        env: Vec::new(),
    };
    for pair in words.windows(2) {
        match pair[0] {
            "--name" => run.name = pair[1].to_owned(),
            "-p" => {
                let (host, container) = pair[1].split_once(':').expect("-p host:container");
                run.ports
                    .push((host.parse().unwrap(), container.parse().unwrap()));
            }
            "-e" => {
                let (name, value) = pair[1].split_once('=').expect("-e NAME=value");
                run.env.push((name.into(), value.into()));
            }
            _ => {}
        }
    }
    assert!(!run.name.is_empty(), "{line}: no --name");
    run
}

/// The quickstart's URLs reach the containers it starts: the published
/// port, and for Postgres the password it sets.
#[test]
fn the_quickstart_urls_match_the_containers_it_starts() {
    let quickstart = quickstart();
    let url = |name: &str| {
        quickstart
            .exports
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| panic!("no export of {name}"))
    };
    let mongo = docker_run(&quickstart, "mongo:");
    assert!(mongo.ports.contains(&(27017, 27017)), "{:?}", mongo.ports);
    assert!(url("FERRY_GOLDEN_MONGO_URL").starts_with("mongodb://localhost:27017"));

    let postgres = docker_run(&quickstart, "postgres:");
    assert!(
        postgres.ports.contains(&(5432, 5432)),
        "{:?}",
        postgres.ports
    );
    let password = postgres
        .env
        .iter()
        .find(|(n, _)| n == "POSTGRES_PASSWORD")
        .map(|(_, v)| v.as_str())
        .expect("-e POSTGRES_PASSWORD");
    assert!(
        url("FERRY_GOLDEN_PG_URL")
            .starts_with(&format!("postgres://postgres:{password}@localhost:5432/")),
        "{}",
        url("FERRY_GOLDEN_PG_URL")
    );
    // `dataferry` redacts the password everywhere it prints: one that is a
    // common word would blank it out of paths and messages.
    assert!(!"dataferry golden orders warehouse".contains(password));
}

/// Runs the quickstart's `docker exec <mongo> mongoimport ... < <file>` line
/// against `container` instead of the container the README starts.
fn import(quickstart: &Quickstart, container: &str) {
    let mongo = docker_run(quickstart, "mongo:");
    let line = quickstart
        .other
        .iter()
        .find(|l| l.contains("mongoimport"))
        .expect("a mongoimport line");
    let (command, input) = line.split_once(" < ").expect("mongoimport ... < <file>");
    let words: Vec<&str> = command.split_whitespace().collect();
    assert_eq!(
        words[..4],
        ["docker", "exec", "-i", mongo.name.as_str()],
        "{line}"
    );
    let file = std::fs::File::open(repo(input.trim())).unwrap();
    let status = std::process::Command::new("docker")
        .args(["exec", "-i", container])
        .args(&words[4..])
        .stdin(file)
        .status()
        .expect("run docker");
    assert!(status.success(), "{line}: {status}");
}

/// What the golden config's variables are set to in a test: the names the
/// README exports, with the values the test chooses. The README's values
/// must at least be URLs of the right kind.
fn golden_env(quickstart: &Quickstart, mongo: &str, postgres: &str) -> Vec<(String, String)> {
    let names: BTreeSet<&str> = quickstart.exports.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names,
        BTreeSet::from(["FERRY_GOLDEN_MONGO_URL", "FERRY_GOLDEN_PG_URL"]),
        "the quickstart exports what examples/golden/ferry/connections.yaml reads"
    );
    quickstart
        .exports
        .iter()
        .map(|(name, value)| {
            let (scheme, ours) = match name.as_str() {
                "FERRY_GOLDEN_MONGO_URL" => ("mongodb://", mongo),
                _ => ("postgres://", postgres),
            };
            assert!(value.starts_with(scheme), "{name}={value}");
            (name.clone(), ours.to_owned())
        })
        .collect()
}

#[test]
fn the_quickstart_shows_every_command_and_names_files_that_exist() {
    let quickstart = quickstart();
    let commands: Vec<&str> = quickstart
        .commands
        .iter()
        .map(|args| subcommand(args))
        .collect();
    assert_eq!(
        commands,
        ["validate", "schema", "plan", "run", "resume", "retry"],
        "the quickstart walks through every command, checks first"
    );
    // Every repository path it mentions is there: the config it runs and
    // the documents it imports.
    let paths: Vec<&str> = quickstart
        .commands
        .iter()
        .flatten()
        .map(String::as_str)
        .chain(quickstart.other.iter().flat_map(|l| l.split_whitespace()))
        .filter(|word| word.starts_with("examples/") || word.starts_with("fixtures/"))
        .collect();
    assert!(paths.contains(&"fixtures/golden/source/orders.json"));
    for path in paths {
        assert!(repo(path).exists(), "the quickstart names {path}");
    }

    // Install from source: the path given is this crate, the one with the
    // `dataferry` binary.
    let text = readme();
    let install = shell_lines(section(&text, "Install from source"))
        .into_iter()
        .find(|line| line.starts_with("cargo install"))
        .expect("a cargo install line");
    let path = install
        .split_whitespace()
        .skip_while(|word| *word != "--path")
        .nth(1)
        .expect("cargo install --path <crate>");
    assert_eq!(
        repo(path).canonicalize().unwrap(),
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .canonicalize()
            .unwrap()
    );
    let manifest = std::fs::read_to_string(repo(path).join("Cargo.toml")).unwrap();
    assert!(
        manifest.contains("[[bin]]\nname = \"dataferry\""),
        "{manifest}"
    );
}

/// TEST-NET-1 (RFC 5737): a connection attempt would hang, not fail fast.
const UNROUTABLE: &str = "192.0.2.1";

#[test]
fn the_quickstart_checks_succeed_without_any_database() {
    let quickstart = quickstart();
    let env = golden_env(
        &quickstart,
        &format!("mongodb://{UNROUTABLE}:27017"),
        &format!("postgres://ferry@{UNROUTABLE}:5432/warehouse"),
    );
    let checks: Vec<&Vec<String>> = quickstart
        .commands
        .iter()
        .filter(|args| matches!(subcommand(args), "validate" | "schema"))
        .collect();
    assert_eq!(checks.len(), 2);
    for args in checks {
        let out = output(
            ferry(&repo(""))
                .timeout(Duration::from_secs(20))
                .envs(env.iter().map(|(n, v)| (n, v)))
                .args(args),
        )
        .expect(0);
        match subcommand(args) {
            "validate" => assert_eq!(
                out.stdout,
                "`golden_orders` is valid: 2 connections, 1 flow\n"
            ),
            _ => {
                let schemas: Value = serde_json::from_str(&out.stdout).unwrap();
                let files: BTreeSet<&str> = schemas
                    .as_object()
                    .unwrap()
                    .keys()
                    .map(String::as_str)
                    .collect();
                assert_eq!(
                    files,
                    BTreeSet::from([
                        "connections.schema.json",
                        "ferry.schema.json",
                        "flows.schema.json",
                        "runtime.schema.json"
                    ])
                );
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_quickstart_runs_as_written_against_real_databases() {
    let quickstart = quickstart();
    let (mongo, warehouse) = tokio::join!(Mongo::default().start(), Warehouse::start());
    let mongo = mongo.expect("start mongo");
    import(&quickstart, mongo.id());
    let mongo_url = format!(
        "mongodb://127.0.0.1:{}",
        mongo.get_host_port_ipv4(27017).await.unwrap()
    );
    let env = golden_env(&quickstart, &mongo_url, &warehouse.url);
    // The README's run writes its report next to the config; the test's
    // goes to a temporary directory, so the checkout stays clean.
    let work = tempfile::tempdir().unwrap();

    let mut run_id: Option<String> = None;
    for args in &quickstart.commands {
        let args: Vec<String> = args
            .iter()
            .map(|arg| match (arg.as_str(), &run_id) {
                ("<run-id>", Some(id)) => id.clone(),
                ("<run-id>", None) => panic!("<run-id> before `dataferry run`"),
                _ => arg.clone(),
            })
            .collect();
        let out = output(
            ferry(&repo(""))
                .envs(env.iter().map(|(n, v)| (n, v)))
                .env("FERRY_WORK_DIR", work.path())
                .args(&args),
        )
        .expect(0)
        .no_secret();
        let command = subcommand(&args);
        if command == "run" {
            let first = out.stdout.lines().next().unwrap_or_default();
            let id = first.strip_prefix("run ").unwrap_or_else(|| {
                panic!(
                    "`dataferry run` prints `run <run-id>` first: {}",
                    out.stdout
                )
            });
            run_id = Some(id.to_owned());
        }
        if matches!(command, "run" | "resume" | "retry") {
            let id = run_id.as_deref().unwrap();
            assert!(
                out.stdout.contains(&format!(
                    "run {id}: complete; 3 partitions; read 10, dead-lettered 1; \
                     wrote order_items 7, orders 8\n"
                )),
                "{command}: {}",
                out.stdout
            );
        }
    }
    let id = run_id.expect("the quickstart runs `dataferry run`");
    assert!(
        work.path()
            .join("runs")
            .join(&id)
            .join("report.md")
            .exists()
    );
    // Resuming and retrying a finished run wrote nothing twice.
    assert_eq!(warehouse.count("orders").await, 8);
    assert_eq!(warehouse.count("order_items").await, 7);
}

/// The exit codes of an `## Exit codes`-style table: the first cell of
/// every row that is a number.
fn exit_codes(text: &str, heading: &str) -> Vec<u8> {
    let start = text
        .find(heading)
        .unwrap_or_else(|| panic!("no {heading:?}"));
    text[start..]
        .lines()
        .skip(1)
        .skip_while(|line| !line.starts_with('|'))
        .take_while(|line| line.starts_with('|'))
        .filter_map(|line| line.split('|').nth(1)?.trim().parse().ok())
        .collect()
}

#[test]
fn the_readme_lists_the_same_exit_codes_as_the_cli_reference() {
    let cli = std::fs::read_to_string(repo("crates/dataferry/README.md")).unwrap();
    let ours = exit_codes(&readme(), "\n### Exit codes\n");
    assert_eq!(ours, exit_codes(&cli, "\n## Exit codes\n"));
    assert_eq!(ours, [0, 1, 2, 3, 4, 5, 6, 7, 8, 130]);
}

/// The code between `// readme:<name>:start` and `:end` in `text`.
fn marked<'a>(text: &'a str, name: &str) -> &'a str {
    let start = format!("// readme:{name}:start\n");
    let from = text.find(&start).unwrap_or_else(|| panic!("no {start}")) + start.len();
    let to = text[from..]
        .find(&format!("// readme:{name}:end\n"))
        .expect("an end marker")
        + from;
    &text[from..to]
}

/// The `rust` blocks of `text`, in order.
fn rust_blocks(text: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut block: Option<String> = None;
    for line in text.lines() {
        match (line, &mut block) {
            ("```rust", None) => block = Some(String::new()),
            ("```", Some(_)) => blocks.push(block.take().unwrap()),
            (line, Some(block)) => {
                block.push_str(line);
                block.push('\n');
            }
            _ => {}
        }
    }
    blocks
}

#[test]
fn the_readme_connector_example_is_the_code_that_compiles() {
    // tests/readme_connector.rs compiles this code, registers it and runs
    // `schema` and `validate` with it.
    let compiled =
        std::fs::read_to_string(repo("crates/dataferry/tests/readme_connector.rs")).unwrap();
    let text = readme();
    let blocks = rust_blocks(section(&text, "Adding a connector"));
    assert_eq!(
        blocks,
        [marked(&compiled, "connector"), marked(&compiled, "main")],
        "README.md's connector example differs from tests/readme_connector.rs"
    );
}
