#!/usr/bin/env bash
# Checks that the guide (docs/src) shows what the binary really does.
#
#   docs/scripts/check-snippets.sh            # no database needed
#   docs/scripts/check-snippets.sh --live     # also runs examples on Docker
#   docs/scripts/check-snippets.sh --update   # rewrites the generated files
#
# Without a database:
#   1. Every ```yaml block in the guide is made of {{#include}} lines (and
#      YAML comments), and every file it includes lies in a config
#      directory (`.../ferry/`) that is validated: one under docs/src/examples
#      (step 2), or one under examples/ (scripts/check-examples.sh). So no
#      YAML the guide shows goes unchecked.
#   2. Every config under docs/src/examples passes `validate`, and every
#      docs/src/examples/bad/<case>/ferry fails it with exit 3. Next to a
#      config, validate.txt (always there for a bad case) is exactly what
#      `validate` prints for it, and validate.json what `--format json`
#      prints.
#   3. docs/src/reference/cli/*.txt are the binary's `--help` texts.
#   4. The CLI reference lists the exit codes README.md lists, and the
#      diagnostic codes the code defines.
#   5. Every `dataferry ...` line in a ```sh block of the guide names a
#      subcommand and flags the binary has.
# With --live (needs Docker), also:
#   6. The quickstart's ```sh blocks run in order, as written, and what
#      each block prints is the ```text block right after it (when there is
#      one), up to run ids, times, versions and ports.
#   7. Every worked example docs/src/examples/<name>/ with a load.sh runs
#      against fresh databases and prints its expected.txt.
#
# --update rewrites what steps 2, 3 and 7 compare against from what the
# binary prints (review the diff); the quickstart's text blocks are edited
# by hand.
#
# The binary is $FERRY, default target/debug/dataferry. Needs bash, awk, sed, diff and, for --live,
# Docker: export DOCKER_HOST first if its socket isn't the default. The
# quickstart publishes Postgres on 5432 and Mongo on 27017, as it tells
# readers to; where those ports are taken, set QUICKSTART_PG_PORT and
# QUICKSTART_MONGO_PORT to free ones (CI runs it as written).
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"
src=docs/src

live=0
update=0
for arg in "$@"; do
  case $arg in
    --live) live=1 ;;
    --update) update=1 ;;
    *)
      echo "usage: $0 [--live] [--update]" >&2
      exit 2
      ;;
  esac
done

ferry=${FERRY:-target/debug/dataferry}
if [[ ! -x $ferry ]]; then
  echo "no binary: build it (cargo build) or set FERRY" >&2
  exit 1
fi
[[ $ferry == /* ]] || ferry=$root/$ferry

failed=0
failures=0
fail() {
  echo "FAIL $*"
  failed=1
  failures=$((failures + 1))
}
# A step's summary: `ok` when the step added no failure.
step_failures=0
step() {
  if [[ $failures -eq $step_failures ]]; then
    echo "ok   $*"
  else
    echo "FAIL $* ($((failures - step_failures)) problems above)"
  fi
  step_failures=$failures
}

scratch=$(mktemp -d)
cleanup_hooks=()
cleanup() {
  local hook
  for hook in "${cleanup_hooks[@]+"${cleanup_hooks[@]}"}"; do
    eval "$hook" || true
  done
  rm -rf "$scratch"
}
trap cleanup EXIT
out=$scratch/out

# The binary with nothing from the caller's environment but PATH (clap
# prints the values of the FERRY_* variables it reads in --help).
bare() {
  env -i "PATH=$PATH" "$ferry" "$@"
}

# The guide is written for the `dataferry` command. While the binary still
# has its old name, its help says `Usage: ferry`: say what the guide says.
# A no-op once the binary is `dataferry`.
as_documented() {
  sed -E 's/^Usage: [a-z-]*ferry( |$)/Usage: dataferry\1/'
}

# Splits a markdown file's fenced blocks into `dir`: for block n,
# n.lang (the fence's language), n.adjacent (1 when only blank lines
# separate it from block n-1) and n.body. Prints the number of blocks.
split_blocks() {
  local file=$1 dir=$2
  mkdir -p "$dir"
  awk -v dir="$dir" '
    BEGIN { n = 0; inside = 0; gap = 1 }
    /^```/ {
      if (!inside) {
        inside = 1; n++
        lang = substr($0, 4); sub(/[ ,].*/, "", lang)
        print lang > (dir "/" n ".lang"); close(dir "/" n ".lang")
        print ((gap == 0 && n > 1) ? 1 : 0) > (dir "/" n ".adjacent"); close(dir "/" n ".adjacent")
        printf "" > (dir "/" n ".body")
      } else {
        close(dir "/" n ".body"); inside = 0; gap = 0
      }
      next
    }
    inside { print >> (dir "/" n ".body"); next }
    /^[[:space:]]*$/ { next }
    { gap = 1 }
    END { print n }
  ' "$file"
}

# Joins `\` continuations, so each command is one line.
joined() {
  awk '/\\$/ { sub(/\\$/, ""); line = line $0; next } { print line $0; line = "" }'
}

pages=$(find "$src" -name '*.md' -not -path "$src/examples/*" | sort)

# --- 1. YAML blocks only include validated configs -----------------------

# The config directory (`.../ferry`) a repository path lies in, or nothing.
config_dir_of() {
  local path=$1
  while [[ $path == */* ]]; do
    path=${path%/*}
    if [[ ${path##*/} == ferry ]]; then
      echo "$path"
      return
    fi
  done
}

doc_configs=$(find "$src/examples" -type d -name ferry -not -path "$src/examples/bad/*" | sort)
yaml_blocks=0
page_no=0
for page in $pages; do
  page_no=$((page_no + 1))
  dir=$scratch/pages/$page_no
  count=$(split_blocks "$page" "$dir")
  for ((n = 1; n <= count; n++)); do
    lang=$(cat "$dir/$n.lang")
    [[ $lang == yaml || $lang == yml ]] || continue
    yaml_blocks=$((yaml_blocks + 1))
    while IFS= read -r text; do
      [[ $text =~ ^[[:space:]]*(#.*)?$ ]] && continue
      if [[ ! $text =~ ^\{\{#include\ ([^:}]+)(:[^}]*)?\}\}$ ]]; then
        fail "$page: a yaml block holds YAML instead of including a checked file: $text"
        continue
      fi
      file=${BASH_REMATCH[1]}
      target=$(cd "$(dirname "$page")" && realpath -q "$file" || true)
      if [[ -z $target || ! -f $target ]]; then
        fail "$page: includes $file, which doesn't exist"
        continue
      fi
      config=$(config_dir_of "${target#"$root"/}")
      if [[ -z $config ]]; then
        fail "$page: includes $file, which isn't in a config directory (ferry/)"
      elif [[ $config == examples/bad/* ]] || { [[ $config != examples/* ]] && ! grep -qxF "$config" <<<"$doc_configs"; }; then
        fail "$page: includes $file, from $config, which nothing validates"
      fi
    done <"$dir/$n.body"
  done
done
step "1. $yaml_blocks yaml blocks include only validated configs"

# --- 1b. Every {{#include}} includes something -----------------------------

# mdbook renders an include of a missing anchor, or of lines past the end
# of a file, as an empty block, without a warning.
includes=0
for page in $pages; do
  while IFS= read -r include; do
    includes=$((includes + 1))
    spec=${include#\{\{#include }
    spec=${spec%\}\}}
    file=${spec%%:*}
    target=$(cd "$(dirname "$page")" && realpath -q "$file" || true)
    if [[ -z $target || ! -f $target ]]; then
      fail "$page: {{#include $spec}}: no file $file"
      continue
    fi
    [[ $spec == *:* ]] || continue
    range=${spec#*:}
    if [[ $range =~ ^([0-9]*)(:([0-9]*))?$ ]]; then
      lines=$(wc -l <"$target" | tr -d ' ')
      from=${BASH_REMATCH[1]:-1}
      to=${BASH_REMATCH[3]:-$lines}
      [[ -z ${BASH_REMATCH[2]} && -n ${BASH_REMATCH[1]} ]] && to=$from
      if ((from < 1 || from > to || to > lines)); then
        fail "$page: {{#include $spec}}: lines $from to $to of a $lines-line file"
      elif ! sed -n "${from}p" "$target" | grep -q '[^[:space:]]'; then
        fail "$page: {{#include $spec}}: starts on a blank line (has the file moved under it?)"
      fi
    elif ! grep -qE "ANCHOR: $range\$" "$target" || ! grep -qE "ANCHOR_END: $range\$" "$target"; then
      fail "$page: {{#include $spec}}: no anchor \`$range\` in $file"
    fi
  done < <(grep -oE '\{\{#include [^}]+\}\}' "$page")
done
step "1b. $includes includes resolve to something"

# --- 2. The guide's example configs --------------------------------------

env_file=$src/examples/ci.env
ci_env=()
while IFS= read -r line; do
  ci_env+=("$line")
done < <(grep -Ev '^[[:space:]]*(#|$)' "$env_file")

# `validate` with only $env_file's variables, run next to the config so
# paths print as `ferry/...`. More arguments go before `validate`.
validate() {
  local dir=$1 to=$2 status=0
  shift 2
  (cd "$(dirname "$dir")" && env -i "PATH=$PATH" "${ci_env[@]}" "$ferry" --color never --config ferry "$@" validate) \
    >"$to" 2>&1 || status=$?
  return "$status"
}

configs=0
for config in $doc_configs; do
  configs=$((configs + 1))
  for var in $(grep -rhoE '\$\{[A-Za-z_][A-Za-z0-9_]*\}' "$config" | sort -u | tr -d '${}'); do
    grep -q "^$var=" "$env_file" || fail "$config: reads \${$var}, which $env_file doesn't set"
  done
  status=0
  validate "$config" "$out" || status=$?
  shown=$(dirname "$config")/validate.txt
  if [[ $status -ne 0 ]]; then
    fail "$config: validate exited $status, expected 0"
    sed 's/^/     /' "$out"
  elif [[ -f $shown && $update -eq 1 ]]; then
    cp "$out" "$shown"
  elif [[ -f $shown ]] && ! diff -u "$shown" "$out"; then
    fail "$config: validate printed something else than $shown (--update rewrites it)"
  fi
done
bad=0
for case in "$src"/examples/bad/*/; do
  [[ -d $case ]] || continue
  case=${case%/}
  bad=$((bad + 1))
  status=0
  validate "$case/ferry" "$out" || status=$?
  if [[ $status -ne 3 ]]; then
    fail "$case: validate exited $status, expected 3"
    sed 's/^/     /' "$out"
  elif [[ $update -eq 1 ]]; then
    cp "$out" "$case/validate.txt"
  elif ! diff -u "$case/validate.txt" "$out"; then
    fail "$case: validate printed something else than validate.txt (--update rewrites it)"
  fi
  # The same, as JSON lines, where the guide shows them.
  [[ -f $case/validate.json ]] || continue
  status=0
  validate "$case/ferry" "$out" --format json || status=$?
  if [[ $status -ne 3 ]]; then
    fail "$case: --format json validate exited $status, expected 3"
  elif [[ $update -eq 1 ]]; then
    cp "$out" "$case/validate.json"
  elif ! diff -u "$case/validate.json" "$out"; then
    fail "$case: \`--format json validate\` printed something else than validate.json (--update rewrites it)"
  fi
done
step "2. $configs example configs valid, $bad bad cases rejected as shown"

# --- 3. --help texts -------------------------------------------------------

help_dir=$src/reference/cli
commands=$(bare --help | awk '/^Commands:/ { on = 1; next } on && /^[^ ]/ { on = 0 } on && NF { print $1 }' | grep -vx help)
for command in "" $commands; do
  name=${command:-dataferry}
  file=$help_dir/$name.txt
  # shellcheck disable=SC2086 # no command: the top-level help
  bare $command --help | as_documented >"$out"
  if [[ $update -eq 1 ]]; then
    mkdir -p "$help_dir"
    cp "$out" "$file"
  elif [[ ! -f $file ]]; then
    fail "$file is missing (--update writes it)"
  elif ! diff -u "$file" "$out"; then
    fail "$file differs from \`$name --help\` (--update rewrites it)"
  fi
done
for file in "$help_dir"/*.txt; do
  name=$(basename "$file" .txt)
  [[ $name == dataferry ]] || grep -qxF "$name" <<<"$commands" ||
    fail "$file: the binary has no \`$name\` command"
done
step "3. --help of the binary and its $(wc -w <<<"$commands" | tr -d ' ') commands is $help_dir"

# --- 4. Exit and diagnostic codes ------------------------------------------

cli_page=$src/reference/cli.md
# The first cell of every row of the table under `heading` in `file`.
codes_under() {
  awk -v heading="$2" '
    $0 == heading { on = 1; next }
    on && /^#/ { exit }
    on && /^\|/ { split($0, cells, "|"); gsub(/[ `]/, "", cells[2]); print cells[2] }
  ' "$1" | grep -E '^[0-9FW]' || true
}
documented=$(codes_under "$cli_page" "## Exit codes" | tr '\n' ' ')
readme=$(codes_under README.md "### Exit codes" | tr '\n' ' ')
if [[ -z $documented || $documented != "$readme" ]]; then
  fail "$cli_page lists exit codes [$documented], README.md [$readme]"
fi
# Rows may name a range: F0100–F0109.
expand() {
  local code n
  while IFS= read -r code; do
    if [[ $code =~ ^([FW])([0-9]{4})–[FW]([0-9]{4})$ ]]; then
      for ((n = 10#${BASH_REMATCH[2]}; n <= 10#${BASH_REMATCH[3]}; n++)); do
        printf '%s%04d\n' "${BASH_REMATCH[1]}" "$n"
      done
    else
      echo "$code"
    fi
  done | sort -u
}
documented=$(codes_under "$cli_page" "## Diagnostic codes" | expand)
defined=$(grep -rhoE 'pub const [A-Z_]+: &str = "[FW][0-9]{4}"' --include diagnostic.rs crates |
  grep -oE '[FW][0-9]{4}' | sort -u)
if [[ -z $defined || $documented != "$defined" ]]; then
  fail "$cli_page's diagnostic codes differ from the code's (< guide, > code):"
  diff <(echo "$documented") <(echo "$defined") | sed 's/^/     /' || true
fi
step "4. exit codes and $(wc -l <<<"$defined" | tr -d ' ') diagnostic codes documented"

# --- 5. Command lines name real commands and flags -------------------------

lines=0
page_no=0
for page in $pages; do
  page_no=$((page_no + 1))
  dir=$scratch/pages/$page_no
  for body in "$dir"/*.body; do
    [[ -e $body ]] || continue
    [[ $(cat "${body%.body}.lang") == sh ]] || continue
    while IFS= read -r text; do
      [[ $text =~ ^[[:space:]]*dataferry([[:space:]]|$) ]] || continue
      lines=$((lines + 1))
      read -ra words <<<"${text%%#*}"
      sub=
      for word in "${words[@]:1}"; do
        if [[ $word != -* ]] && grep -qxF -- "$word" <<<"$commands"; then
          sub=$word
          break
        fi
      done
      # shellcheck disable=SC2086 # no subcommand: the top-level flags
      help=$(bare $sub --help)
      for word in "${words[@]:1}"; do
        [[ $word == -* ]] || continue
        flag=${word%%=*}
        grep -qE -- "(^|[ ,])$flag([ ,=]|$)" <<<"$help" ||
          fail "$page: \`$text\`: \`dataferry $sub --help\` has no $flag"
      done
    done < <(joined <"$body")
  done
done
step "5. $lines command lines use real commands and flags"

if [[ $live -eq 0 ]]; then
  if [[ $failed -eq 0 ]]; then
    echo "passed (without --live)"
  fi
  exit "$failed"
fi

# --- Running things --------------------------------------------------------

if ! command -v docker >/dev/null || ! docker info >/dev/null 2>&1; then
  echo "FAIL --live needs a running Docker (export DOCKER_HOST if its socket isn't the default)"
  exit 1
fi
for var in $(env | grep -oE '^FERRY_[A-Z_]+=' | tr -d = || true); do
  unset "$var"
done

# What varies from run to run, in the guide and in what the binary prints
# alike: run ids, times, durations and version numbers.
normalized() {
  sed -E \
    -e 's/[0-9a-f]{32}/<run-id>/g' \
    -e 's/[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?Z/<time>/g' \
    -e 's/\([0-9]+(\.[0-9]+)?s\)/(<seconds>)/g' \
    -e 's/^- (Engine|Verifier) version: .*/- \1 version: <version>/'
}

# Compares what ran (`actual`) with what the guide shows (`expected`).
compare() {
  local what=$1 expected=$2 actual=$3
  if ! diff -u <(normalized <"$expected") <(normalized <"$actual") >"$scratch/diff"; then
    fail "$what: the output differs from the guide (- guide, + binary):"
    sed 's/^/     /' "$scratch/diff"
  fi
}

# Runs "$@" until it succeeds, for at most two minutes.
wait_for() {
  local _
  for _ in $(seq 120); do
    "$@" >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "not ready after two minutes: $*" >&2
  return 1
}

# --- 6. The quickstart, as written ---------------------------------------

quickstart=$src/quickstart.md
pg_port=${QUICKSTART_PG_PORT:-5432}
mongo_port=${QUICKSTART_MONGO_PORT:-27017}
run_id=

# A quickstart line as it runs here: the binary for `dataferry`, the run
# id for `<run-id>`, and the local ports if they are remapped.
as_run() {
  local line=$1
  line=${line//<run-id>/$run_id}
  line=${line//5432:5432/$pg_port:5432}
  line=${line//localhost:5432/localhost:$pg_port}
  line=${line//27017:27017/$mongo_port:27017}
  line=${line//localhost:27017/localhost:$mongo_port}
  if [[ $line =~ ^dataferry([[:space:]]|$) ]]; then
    line="\"\$ferry\"${line#dataferry}"
  fi
  printf '%s' "$line"
}

# What the quickstart prints, with the ports put back as the guide has them.
as_shown() {
  sed -e "s/localhost:$pg_port/localhost:5432/g" -e "s/localhost:$mongo_port/localhost:27017/g"
}

for name in $(grep -oE -- '--name [a-z0-9-]+' "$quickstart" | awk '{ print $2 }' | sort -u); do
  docker rm -f "$name" >/dev/null 2>&1 || true
  cleanup_hooks+=("docker rm -f $name >/dev/null 2>&1")
done
rm -rf examples/golden/.ferry
cleanup_hooks+=("rm -rf '$root/examples/golden/.ferry'")

dir=$scratch/quickstart
count=$(split_blocks "$quickstart" "$dir")
ran=0
compared=0
for ((n = 1; n <= count; n++)); do
  [[ $(cat "$dir/$n.lang") == sh ]] || continue
  : >"$dir/$n.out"
  while IFS= read -r text; do
    case $text in
      '' | '#'*) continue ;;
      # The guide clones the repository and enters it; this runs in a
      # checkout already.
      'git clone '* | 'cd '*) continue ;;
    esac
    ran=$((ran + 1))
    line=$(as_run "$text")
    status=0
    eval "$line" </dev/null >"$out" 2>"$scratch/err" || status=$?
    if [[ $status -ne 0 ]]; then
      fail "$quickstart: \`$text\` exited $status"
      sed 's/^/     /' "$out" "$scratch/err"
      break 2
    fi
    if [[ -z $run_id && $text =~ ^dataferry([[:space:]].*)?[[:space:]]run([[:space:]]|$) ]]; then
      run_id=$(sed -n '1s/^run //p' "$out")
    fi
    as_shown <"$out" >>"$dir/$n.out"
  done < <(joined <"$dir/$n.body")
  next=$((n + 1))
  if ((next <= count)) && [[ $(cat "$dir/$next.lang") == text && $(cat "$dir/$next.adjacent") == 1 ]]; then
    compared=$((compared + 1))
    compare "$quickstart (block $next)" "$dir/$next.body" "$dir/$n.out"
  fi
done
[[ -n $run_id ]] || fail "$quickstart: no \`dataferry run\` printed a run id"
step "6. quickstart: $ran commands ran, $compared outputs compared"

# --- 7. Worked examples ----------------------------------------------------

docs_pg=dataferry-docs-pg
docs_mongo=dataferry-docs-mongo
password=docs-examples-pw
docker rm -f "$docs_pg" "$docs_mongo" >/dev/null 2>&1 || true
cleanup_hooks+=("docker rm -f $docs_pg $docs_mongo >/dev/null 2>&1")
docker run -d --name "$docs_pg" -p 127.0.0.1::5432 -e POSTGRES_PASSWORD="$password" postgres:16 >/dev/null
docker run -d --name "$docs_mongo" -p 127.0.0.1::27017 mongo:7 >/dev/null
port() { docker port "$1" "$2" | head -n 1 | sed 's/.*://'; }
# Over TCP: while the image initializes, its temporary server only listens
# on the socket.
wait_for docker exec "$docs_pg" pg_isready -h 127.0.0.1 -U postgres
wait_for docker exec "$docs_mongo" mongosh --quiet --eval 'db.runCommand({ ping: 1 })'
pg_port=$(port "$docs_pg" 5432)
mongo_port=$(port "$docs_mongo" 27017)

# The helpers below are for the load.sh scripts, which source them. A
# failing helper marks the example failed: load.sh runs where errexit
# doesn't apply (and some helpers inside $(...)), so its status alone
# would be lost.
load_failed=$scratch/load-failed
load_fail() {
  echo "load.sh: $*" >&2
  touch "$load_failed"
  return 1
}
# For load.sh: a new, empty Postgres database `name`; prints its URL.
# shellcheck disable=SC2329
pg_db() {
  docker exec "$docs_pg" psql -q -U postgres -c "DROP DATABASE IF EXISTS \"$1\"" -c "CREATE DATABASE \"$1\"" >/dev/null ||
    load_fail "pg_db $1"
  echo "postgres://postgres:$password@localhost:$pg_port/$1"
}
# For load.sh: runs the SQL file `file` in database `name`.
# shellcheck disable=SC2329
pg_load() {
  docker exec -i "$docs_pg" psql -q -v ON_ERROR_STOP=1 -U postgres -d "$1" <"$2" >/dev/null ||
    load_fail "pg_load $1 $2"
}
# For load.sh: replaces collection `collection` of Mongo database `db` with
# the documents of the JSON array in `file`.
# shellcheck disable=SC2329
mongo_load() {
  docker exec -i "$docs_mongo" mongoimport --quiet --drop --db "$1" --collection "$2" --jsonArray <"$3" ||
    load_fail "mongo_load $1 $2 $3"
}
# For load.sh: the Mongo the helpers load into.
# shellcheck disable=SC2034
mongo_url="mongodb://localhost:$mongo_port"

examples=0
for load in "$src"/examples/*/load.sh; do
  [[ -e $load ]] || continue
  example=$(dirname "$load")
  examples=$((examples + 1))
  actual=$scratch/example.out
  # load.sh loads the source data, exports the config's variables and sets
  # CONFIG (the config directory, from the repository root), TARGET (the
  # database check.sql queries) and, optionally, ARGS (default: run) and
  # EXIT (default: 0).
  status=0
  (
    CONFIG='' TARGET='' ARGS=run EXIT=0
    rm -f "$load_failed"
    # shellcheck source=/dev/null
    source "$load"
    if [[ -e $load_failed || -z $CONFIG || -z $TARGET ]]; then
      echo "load.sh failed, or didn't set CONFIG and TARGET"
      exit 1
    fi
    cd "$(dirname "$CONFIG")"
    rm -rf .ferry
    status=0
    # shellcheck disable=SC2086 # ARGS is a list of words
    "$ferry" --color never $ARGS --config ferry >"$actual" 2>&1 || status=$?
    rm -rf .ferry
    if [[ $status -ne $EXIT ]]; then
      echo "\`dataferry $ARGS\` exited $status, expected $EXIT"
      sed 's/^/     /' "$actual"
      exit 1
    fi
    if [[ -f $root/$example/check.sql ]]; then
      docker exec -i "$docs_pg" psql -X -q -e -v ON_ERROR_STOP=1 -P pager=off -U postgres -d "$TARGET" \
        <"$root/$example/check.sql" >>"$actual" 2>&1
    fi
  ) >"$scratch/example.log" 2>&1 || status=$?
  if [[ $status -ne 0 ]]; then
    fail "$example:"
    sed 's/^/     /' "$scratch/example.log"
    continue
  fi
  sed -i.orig -e "s/localhost:$pg_port/localhost:5432/g" -e "s/localhost:$mongo_port/localhost:27017/g" "$actual"
  # Known bug, not the guide's: the MongoDB driver (mongodb 3.x) sometimes
  # panics in a background task while the binary shuts down, after the
  # command's work and exit code are settled. Drop that one panic, and say
  # so, rather than fail the guide at random. Remove once it's fixed.
  awk '
    /^thread .* panicked at .*mongodb-[0-9.]+\/src\/cmap\/connection_requester\.rs/ { skip = 1; dropped = 1; next }
    skip && /^note: run with `RUST_BACKTRACE=1`/ { skip = 0; next }
    skip { next }
    { print }
    END { if (dropped) print "WARN " FILENAME ": dropped a mongodb driver panic at shutdown (known bug)" > "/dev/stderr" }
  ' "$actual" >"$actual.kept"
  # The panic can leave a blank line behind it. Only when one was dropped,
  # trim trailing blank lines, so a real difference still fails.
  if ! cmp -s "$actual" "$actual.kept"; then
    awk '/^$/ { blank++; next } { while (blank) { print ""; blank-- } print }' \
      "$actual.kept" >"$actual.trim"
    mv "$actual.trim" "$actual.kept"
  fi
  mv "$actual.kept" "$actual"
  if [[ $update -eq 1 ]]; then
    cp "$actual" "$example/expected.txt"
  else
    compare "$example/expected.txt" "$example/expected.txt" "$actual"
  fi
done
step "7. $examples worked examples ran"

if [[ $failed -eq 0 ]]; then
  echo "passed"
fi
exit "$failed"
