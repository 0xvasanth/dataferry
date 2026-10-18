# Releasing

A release is a version tag. Pushing `v<version>` runs
[`.github/workflows/release.yml`](.github/workflows/release.yml), which
builds the binaries, creates the GitHub Release and publishes every crate to
crates.io. Nothing is published by hand.

## Once: the crates.io token

Create a crates.io API token with the `publish-new` and `publish-update`
scopes, and add it to the repository as the `CARGO_REGISTRY_TOKEN` secret
(Settings → Secrets and variables → Actions). Without it the workflow fails
in its first job, before anything is built or published.

## Cutting a release

1. **Version.** Set `version` under `[workspace.package]` in `Cargo.toml`,
   and the same version on every internal dependency in
   `[workspace.dependencies]` (they are pinned with `=`, so the crates of
   one release only ever depend on each other). Then `cargo check` to update
   `Cargo.lock`. A version with a `-` suffix (`0.2.0-beta.1`) is a
   pre-release. The `cargo install --version` lines in `README.md` and
   `crates/dataferry/README.md` name it too.
2. **CHANGELOG.md.** Move what's under `[Unreleased]` into a new
   `## [<version>] - <date>` section, and update the links at the bottom.
   The section becomes the release notes, and the workflow refuses a tag
   whose version has no section.
3. **Check.** The user guide is live at the `homepage` in `Cargo.toml`
   (published crates keep the links they were released with, and the
   workflow refuses to release while it doesn't answer). `docs.yml`
   deploys it from `main` once the `DOCS_DEPLOY` repository variable is
   `true` and GitHub Pages is set to deploy from Actions. Everything CI runs
   passes, and
   `scripts/release.py publish --dry-run` lists the crates in the order
   they'll be published. Commit, and merge to `main`.
4. **Tag** the merged commit and push the tag:

   ```sh
   git tag -s v0.1.0-beta.1 -m v0.1.0-beta.1
   git push origin v0.1.0-beta.1
   ```

## What the workflow does

1. **check:** the tag is `v` plus the workspace version, CHANGELOG.md has its
   section, the user guide answers, and `CARGO_REGISTRY_TOKEN` is set.
2. **build:** `dataferry` for `x86_64-unknown-linux-gnu`,
   `aarch64-unknown-linux-gnu` (both built on Ubuntu 22.04, so glibc 2.35 or
   newer), `x86_64-apple-darwin` and `aarch64-apple-darwin`. Each binary
   that can run on its builder must print the tagged version. Each target
   gives `dataferry-<version>-<target>.tar.gz` (the binary, LICENSE, README
   and CHANGELOG) and its `.sha256`.
3. **release:** the GitHub Release for the tag, with the four archives,
   their checksums and a combined `SHA256SUMS`. It is a pre-release when the
   version has a `-` suffix.
4. **publish:** `cargo publish` for every crate, dependencies first
   (`scripts/release.py order`). The order comes from `cargo metadata`, so a
   new crate needs no change here.

crates.io limits how fast new crates can be published (a burst of a few,
then one every few minutes). When `cargo publish` is refused for that, the
script waits and tries the same crate again, so a release that adds crates
takes longer, without anyone re-running it.

Every job can be re-run from the Actions page. A re-run of **release**
replaces the assets, and a re-run of **publish** skips the crates crates.io
already has at this version, so a publish that failed halfway (a network
error, a crates.io rate limit) continues where it stopped.

## If something goes wrong

- **A build failed before anything was published:** fix it on `main`, then
  delete the tag (locally and with `git push origin :v<version>`), and tag
  the fixed commit.
- **Some crates are published:** a version on crates.io can't be replaced,
  only yanked (`cargo yank --version <version> <crate>`). Fix forward with
  the next version.
