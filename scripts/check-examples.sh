#!/usr/bin/env bash
# `dataferry validate` on every example config: each `ferry/` directory under
# examples/ must pass, and each examples/bad/<case>/ferry must be rejected
# as an invalid config (exit 3). Examples are found, not listed, so a new
# one is checked as soon as it is added. Run from anywhere; the binary is
# $FERRY, default target/debug/dataferry (build it with `cargo build -p dataferry`).
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
ferry=${FERRY:-target/debug/dataferry}
if [[ ! -x $ferry ]]; then
  echo "no dataferry binary at $ferry: run \`cargo build -p dataferry\` or set FERRY" >&2
  exit 1
fi

# Only the variables in examples/ci.env: nothing from the caller's
# environment leaks in, and bad/missing-env stays missing.
env_file=examples/ci.env
ci_env=()
while IFS= read -r line; do
  ci_env+=("$line")
done < <(grep -Ev '^[[:space:]]*(#|$)' "$env_file")
validate() {
  local status=0
  local -a cmd=(env -i "PATH=$PATH" "${ci_env[@]}" "$ferry" --color never --config "$1" validate)
  if command -v timeout >/dev/null; then
    timeout 60 "${cmd[@]}" >"$out" 2>&1 || status=$?
  else
    "${cmd[@]}" >"$out" 2>&1 || status=$?
  fi
  return "$status"
}
out=$(mktemp)
trap 'rm -f "$out"' EXIT

failed=0
valid=0
while IFS= read -r dir; do
  valid=$((valid + 1))
  # Every variable the example reads must have a CI value, or it would
  # fail for the wrong reason.
  for var in $(grep -rhoE '\$\{[A-Za-z_][A-Za-z0-9_]*\}' "$dir" | sort -u | tr -d '${}'); do
    if ! grep -q "^$var=" "$env_file"; then
      echo "FAIL $dir: reads \${$var}, which $env_file doesn't set"
      failed=1
      continue 2
    fi
  done
  status=0
  validate "$dir" || status=$?
  if [[ $status -eq 0 ]]; then
    echo "ok   $dir"
  else
    echo "FAIL $dir: dataferry validate exited $status, expected 0"
    sed 's/^/     /' "$out"
    failed=1
  fi
done < <(find examples -type d -name ferry -not -path 'examples/bad/*' | sort)

bad=0
for case in examples/bad/*/; do
  case=${case%/}
  bad=$((bad + 1))
  if [[ ! -d $case/ferry ]]; then
    echo "FAIL $case: no ferry/ directory"
    failed=1
    continue
  fi
  status=0
  validate "$case/ferry" || status=$?
  if [[ $status -eq 3 ]]; then
    echo "ok   $case (rejected)"
  else
    echo "FAIL $case: dataferry validate exited $status, expected 3 (config invalid)"
    sed 's/^/     /' "$out"
    failed=1
  fi
done

if [[ $valid -eq 0 || $bad -eq 0 ]]; then
  echo "FAIL found $valid valid examples and $bad bad cases; expected some of each"
  failed=1
fi
echo "$valid valid examples, $bad bad cases"
exit "$failed"
