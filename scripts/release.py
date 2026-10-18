#!/usr/bin/env python3
"""Release helpers for .github/workflows/release.yml (RELEASING.md).

    scripts/release.py check-tag v0.1.0-beta.1   the tag matches the workspace version
    scripts/release.py notes 0.1.0-beta.1        that version's CHANGELOG.md section
    scripts/release.py order                     publishable crates, dependencies first
    scripts/release.py publish [--dry-run]       `cargo publish` each, skipping versions
                                                 crates.io already has

The publish order comes from `cargo metadata`, so a new crate is published
in the right place without editing this file. Safe to re-run after a
partial publish: a crate whose version is already in the crates.io index
is skipped, and `cargo publish` waits for each crate to reach the index
before the next one, which may depend on it, starts. A publish crates.io
refuses for its rate limit on new crates is retried after a wait.
"""

import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INDEX = "https://index.crates.io"
# crates.io lets a few new crates through at once, then one every ten
# minutes, and answers 429 Too Many Requests in between.
RATE_LIMIT_WAIT = int(os.environ.get("RELEASE_RATE_LIMIT_WAIT", "660"))
RATE_LIMIT_ATTEMPTS = 8


def fail(message):
    # `::error::` makes it an annotation on the GitHub run.
    prefix = "::error::" if os.environ.get("GITHUB_ACTIONS") == "true" else "error: "
    print(prefix + message, file=sys.stderr)
    sys.exit(1)


def metadata():
    out = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps", "--locked"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if out.returncode != 0:
        fail(f"cargo metadata failed:\n{out.stderr}")
    return json.loads(out.stdout)


def publishable(meta):
    """Workspace members crates.io gets, by name."""
    return {p["name"]: p for p in meta["packages"] if p.get("publish") != []}


def workspace_version(meta):
    versions = {p["version"] for p in publishable(meta).values()}
    if len(versions) != 1:
        fail(f"publishable crates disagree on their version: {sorted(versions)}")
    return versions.pop()


def order(meta):
    """Publishable crates, each after every workspace crate it needs.

    A crate needs its normal and build dependencies, and its dev-dependencies
    that carry a version: `cargo publish` resolves those against crates.io.
    A path-only dev-dependency is dropped from the published manifest, which
    is how a dev-dependency cycle (dataferry-state's tests use
    dataferry-dsl) is allowed.
    """
    crates = publishable(meta)
    needs = {name: set() for name in crates}
    for name, package in crates.items():
        for dep in package["dependencies"]:
            if dep["name"] not in crates or dep["name"] == name or "path" not in dep:
                continue
            if dep["kind"] == "dev" and dep["req"] == "*":
                continue
            needs[name].add(dep["name"])
    out = []
    while needs:
        ready = sorted(n for n, deps in needs.items() if not deps)
        if not ready:
            fail(f"dependency cycle among {sorted(needs)}")
        for name in ready:
            out.append(name)
            del needs[name]
        for deps in needs.values():
            deps.difference_update(ready)
    return out


def index_path(name):
    name = name.lower()
    if len(name) <= 2:
        return f"{len(name)}/{name}"
    if len(name) == 3:
        return f"3/{name[0]}/{name}"
    return f"{name[:2]}/{name[2:4]}/{name}"


def published(name, version):
    """Whether crates.io already has `name` at `version` (yanked or not)."""
    # curl rather than urllib: it falls back from IPv6 to IPv4 at once.
    out = subprocess.run(
        [
            "curl", "--silent", "--show-error", "--location", "--max-time", "60",
            "--retry", "3", "--user-agent", "dataferry-release (github.com/0xvasanth/ferry)",
            "--write-out", "\n%{http_code}", f"{INDEX}/{index_path(name)}",
        ],
        capture_output=True,
        text=True,
    )
    body, _, status = out.stdout.rpartition("\n")
    if out.returncode != 0:
        fail(f"crates.io index lookup for {name} failed: {out.stderr.strip()}")
    if status == "404":
        return False
    if status != "200":
        fail(f"crates.io index lookup for {name} failed: HTTP {status}")
    return any(json.loads(line)["vers"] == version for line in body.splitlines() if line.strip())


def publish(name, version):
    """`cargo publish` one crate, waiting out crates.io's rate limit."""
    for attempt in range(1, RATE_LIMIT_ATTEMPTS + 1):
        process = subprocess.Popen(
            ["cargo", "publish", "--locked", "-p", name],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        output = []
        for line in process.stdout:
            print(line, end="", flush=True)
            output.append(line)
        if process.wait() == 0:
            return
        text = "".join(output)
        # The upload went through, but cargo gave up waiting for the index.
        if published(name, version):
            print(f"{name} {version} is on crates.io: carrying on", flush=True)
            return
        # Cargo's words for it; a bare `429` could be a line number in a
        # build error.
        if "status 429" in text or "Too Many Requests" in text:
            if attempt == RATE_LIMIT_ATTEMPTS:
                break
            print(
                f"crates.io rate limit (attempt {attempt} of {RATE_LIMIT_ATTEMPTS}): "
                f"trying {name} again in {RATE_LIMIT_WAIT}s",
                flush=True,
            )
            time.sleep(RATE_LIMIT_WAIT)
            continue
        fail(f"cargo publish -p {name} failed (exit {process.returncode}); re-run the job to continue from here")
    fail(f"crates.io kept refusing {name} for its rate limit; re-run the job later to continue from here")


def changelog_section(version):
    text = (ROOT / "CHANGELOG.md").read_text()
    heading = re.compile(r"^## \[(?P<version>[^\]]+)\]", re.MULTILINE)
    matches = list(heading.finditer(text))
    for i, m in enumerate(matches):
        if m.group("version") == version:
            end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            # The rest of the heading line (` - 2026-10-18`) isn't notes.
            body = text[m.end():end].partition("\n")[2]
            # Nor are the link definitions Keep a Changelog puts at the end.
            body = re.sub(r"^\[[^\]]+\]: \S+\s*$", "", body, flags=re.MULTILINE)
            body = body.strip()
            if not body:
                fail(f"CHANGELOG.md's section for {version} is empty")
            return body
    fail(f"CHANGELOG.md has no `## [{version}]` section")


def main(args):
    if not args:
        fail(__doc__.strip())
    command, rest = args[0], args[1:]
    meta = metadata()
    if command == "check-tag" and len(rest) == 1:
        tag, version = rest[0], workspace_version(meta)
        if tag != f"v{version}":
            fail(f"tag {tag} doesn't match the workspace version {version} (expected v{version})")
        print(version)
    elif command == "notes" and len(rest) == 1:
        print(changelog_section(rest[0]))
    elif command == "order" and not rest:
        print("\n".join(order(meta)))
    elif command == "publish" and rest in ([], ["--dry-run"]):
        dry_run = rest == ["--dry-run"]
        if not dry_run and not os.environ.get("CARGO_REGISTRY_TOKEN"):
            fail("CARGO_REGISTRY_TOKEN is not set: add it as a repository secret (RELEASING.md)")
        version = workspace_version(meta)
        for name in order(meta):
            if published(name, version):
                print(f"skip     {name} {version}: already on crates.io", flush=True)
                continue
            if dry_run:
                print(f"publish  {name} {version} (dry run)", flush=True)
                continue
            print(f"publish  {name} {version}", flush=True)
            publish(name, version)
    else:
        fail(__doc__.strip())


if __name__ == "__main__":
    main(sys.argv[1:])
