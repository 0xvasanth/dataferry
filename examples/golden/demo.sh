#!/usr/bin/env bash
# The golden demo, end to end: the ten golden documents in Mongo become
# Postgres `orders` and `order_items`, verified, with a report a person can
# read. See README.md next to this file.
#
#   examples/golden/demo.sh
#
# Brings up Mongo and Postgres in Docker (and removes them on exit), loads
# fixtures/golden/source/orders.json, then runs `dataferry validate`, `dataferry
# plan` and `dataferry run` on examples/golden/ferry, and prints report.md.
#
# Environment (all optional):
#   FERRY                   path to a dataferry binary; default: cargo run -q -p dataferry --
#   FERRY_GOLDEN_MONGO_URL  use this Mongo, already holding the golden
#   FERRY_GOLDEN_PG_URL       documents in shop.orders, and this empty
#                             Postgres, instead of starting containers
#   FERRY_WORK_DIR          where the run's state, records and report go;
#                             default: a new temporary directory
#   KEEP=1                  leave the containers running afterwards

set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
if [ -n "${FERRY:-}" ]; then
    ferry=("$FERRY")
else
    ferry=(cargo run -q --manifest-path "$repo/Cargo.toml" -p dataferry --)
fi
work=${FERRY_WORK_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/ferry-golden.XXXXXX")}
mkdir -p "$work"
export FERRY_WORK_DIR=$work

step() { printf '\n==> %s\n' "$*"; }

# Runs "$@" until it succeeds, for at most a minute, while container $1
# keeps running.
wait_for() {
    local container=$1
    shift
    for _ in $(seq 60); do
        if "$@" >/dev/null 2>&1; then
            return 0
        fi
        if [ "$(docker inspect -f '{{.State.Running}}' "$container")" != true ]; then
            echo "$container stopped:" >&2
            docker logs "$container" >&2 || true
            return 1
        fi
        sleep 1
    done
    echo "$container wasn't ready within a minute" >&2
    return 1
}

if [ -z "${FERRY_GOLDEN_MONGO_URL:-}" ] || [ -z "${FERRY_GOLDEN_PG_URL:-}" ]; then
    mongo=ferry-golden-mongo
    pg=ferry-golden-pg
    # A throwaway password for a throwaway container on localhost.
    password=golden-demo
    if [ -z "${KEEP:-}" ]; then
        trap 'docker rm -f "$mongo" "$pg" >/dev/null 2>&1 || true' EXIT
    fi
    step "Starting Mongo and Postgres in Docker"
    docker rm -f "$mongo" "$pg" >/dev/null 2>&1 || true
    docker run -d --name "$mongo" -p 127.0.0.1::27017 mongo:7 >/dev/null
    docker run -d --name "$pg" -p 127.0.0.1::5432 \
        -e POSTGRES_PASSWORD="$password" postgres:16 >/dev/null
    port() { docker port "$1" "$2" | head -n 1 | sed 's/.*://'; }
    wait_for "$mongo" docker exec "$mongo" mongosh --quiet --eval 'db.runCommand({ ping: 1 })'
    # Over TCP: the image's temporary server, while it initializes, only
    # listens on the socket.
    wait_for "$pg" docker exec "$pg" psql -h 127.0.0.1 -U postgres -c 'select 1'
    step "Loading fixtures/golden/source/orders.json into shop.orders"
    docker exec -i "$mongo" mongoimport --quiet --db shop --collection orders --jsonArray \
        < "$repo/fixtures/golden/source/orders.json"
    export FERRY_GOLDEN_MONGO_URL="mongodb://127.0.0.1:$(port "$mongo" 27017)"
    export FERRY_GOLDEN_PG_URL="postgres://postgres:$password@127.0.0.1:$(port "$pg" 5432)/postgres"
fi

config=$repo/examples/golden/ferry

step "dataferry validate: the config, without connecting"
"${ferry[@]}" validate --config "$config"

step "dataferry plan: what would run, from the live schemas"
"${ferry[@]}" plan --config "$config"

step "dataferry run: copy, verify, write the report"
# Exit 0: complete and verified. 7: verified, and an assertion failed.
# Shown as it goes, so the run id (printed first) is on screen if it stops.
status=0
"${ferry[@]}" run --config "$config" | tee "$work/run.out" || status=$?
run=$(sed -n '1s/^run //p' "$work/run.out")
report=$work/runs/$run/report.md
if [ -z "$run" ] || [ ! -f "$report" ]; then
    echo "no report was written (dataferry run exited $status)" >&2
    exit $((status == 0 ? 1 : status))
fi

step "report.md ($report)"
cat "$report"
exit "$status"
