# Installation

dataferry is one binary, `dataferry`, with the PostgreSQL and MongoDB
connectors built in. There are three ways to get it.

## From crates.io

You need a Rust toolchain, 1.94 or newer (install it with
[rustup](https://rustup.rs)). Then:

```sh
cargo install dataferry --version 0.1.0-beta.1 --locked
```

Because this release is a pre-release, `cargo install dataferry` without
`--version` doesn't pick it: Cargo only installs pre-releases you ask for
by version. `--locked` builds with the exact dependency versions the
release was tested with.

The build takes a few minutes: the expression language is built on Apache
DataFusion and Arrow, which are large.

## Prebuilt binaries

Each release on the
[GitHub Releases page](https://github.com/0xvasanth/dataferry/releases) has an
archive (`.tar.gz`) for each of:

| Platform | Target |
| --- | --- |
| Linux, x86-64 | `x86_64-unknown-linux-gnu` |
| Linux, ARM64 | `aarch64-unknown-linux-gnu` |
| macOS, Intel | `x86_64-apple-darwin` |
| macOS, Apple silicon | `aarch64-apple-darwin` |

next to a `.sha256` file holding its SHA-256 checksum. Download both for
your platform, check the archive against the checksum, unpack it and put
`dataferry` on your `PATH`. With the two files in the current directory
(the names below are placeholders for the ones you downloaded):

```sh
shasum -a 256 -c dataferry-<version>-<target>.tar.gz.sha256
tar -xzf dataferry-<version>-<target>.tar.gz
```

`shasum -c` prints `<archive>: OK` when the archive matches. On Linux,
`sha256sum -c` does the same. If the checksum file holds only the hash,
compare it by eye with the output of `shasum -a 256 <archive>`. Never run a
binary whose checksum doesn't match.

On macOS, a binary downloaded with a browser is quarantined by Gatekeeper.
If macOS refuses to run it, clear the flag:

```sh
xattr -d com.apple.quarantine dataferry
```

## From the Git repository

To build the latest code on `main` (or any branch or tag) instead of a
release:

```sh
cargo install --git https://github.com/0xvasanth/dataferry dataferry --locked
```

Add `--tag v0.1.0-beta.1` to build exactly that release, or `--branch
<name>` for a branch. From a clone, `cargo build --release` puts the binary
in `target/release/`.

## Leaving a connector out

The `postgres` and `mongo` connectors are default Cargo features of the
`dataferry` crate. A binary that only moves Postgres to Postgres can leave
the MongoDB driver out:

```sh
cargo install dataferry --version 0.1.0-beta.1 --locked --no-default-features --features postgres
```

`postgres` also brings the Postgres state store (`runtime.state.backend:
postgres`). Without `mongo`, a `type: mongo` connection is an unknown type.

## Check the installation

```sh
dataferry --version
dataferry --help
```

`--version` prints `dataferry 0.1.0-beta.1`. `--help` lists the commands,
which the [CLI reference](reference/cli.md) describes in full.

## Shell completion

The binary doesn't generate shell completion scripts in this release.

## Editor support

dataferry can print a JSON Schema for each of its config files, so editors
can complete and check them as you type. See
[JSON Schemas](reference/schemas.md).

## Uninstall

`cargo uninstall dataferry` removes a binary Cargo installed. A prebuilt
binary is just the file you put on your `PATH`.

dataferry doesn't install anything else. What it creates lives where you
point it: a `.ferry/` work directory next to each config you run, and
`_ferry_*` tables in the databases a run writes to or keeps its state in
([Concepts](concepts.md#where-things-are-kept)).
