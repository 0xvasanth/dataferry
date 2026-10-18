//! `Ferry::with_builtins()` in-process: the real `postgres` and `mongo`
//! connectors are registered, so a binary built on it that registers one
//! of them again is miswired, and one that means to swap it says so with
//! `replace`.

#![cfg(all(feature = "postgres", feature = "mongo"))]

use std::ffi::OsString;
use std::process::ExitCode;

use dataferry::{Exit, Ferry};
use dataferry_connector_mongo::MongoConnector;
use dataferry_connector_postgres::PostgresConnector;
use dataferry_state::PgStateBackend;

fn run_with(ferry: Ferry, args: &[&str]) -> (ExitCode, String) {
    let args: Vec<OsString> = std::iter::once("ferry")
        .chain(args.iter().copied())
        .map(Into::into)
        .collect();
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let code = ferry.run_with(args, &mut out, &mut err);
    (code, String::from_utf8(err).unwrap())
}

#[test]
fn registering_a_built_in_again_fails_at_startup_naming_the_type() {
    for (ferry, ty) in [
        (
            Ferry::with_builtins().register_state_capable(PostgresConnector, PgStateBackend),
            "postgres",
        ),
        (
            Ferry::with_builtins().register(PostgresConnector),
            "postgres",
        ),
        (Ferry::with_builtins().register(MongoConnector), "mongo"),
    ] {
        let (code, err) = run_with(ferry, &["schema"]);
        assert_eq!(code, ExitCode::from(Exit::Internal), "{ty}: {err}");
        assert!(
            err.starts_with(&format!(
                "error[F0002]: this binary is miswired: connector type `{ty}` is already registered\n"
            )),
            "{err}"
        );
    }
}

#[test]
fn a_built_in_can_be_replaced_on_purpose() {
    let ferry = Ferry::with_builtins()
        .replace_state_capable(PostgresConnector, PgStateBackend)
        .replace(MongoConnector);
    let (code, err) = run_with(ferry, &["schema"]);
    assert_eq!(code, ExitCode::from(Exit::Success), "{err}");
}
