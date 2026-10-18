# Loaded by docs/scripts/check-snippets.sh --live.
mongo_load crm signups docs/src/examples/signups.json
export SIGNUPS_MONGO_URL=$mongo_url
SIGNUPS_PG_URL=$(pg_db signups_fail_fast)
export SIGNUPS_PG_URL
CONFIG=docs/src/examples/fail-fast/ferry
TARGET=signups_fail_fast
EXIT=6
