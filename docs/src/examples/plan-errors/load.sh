# Loaded by docs/scripts/check-snippets.sh --live.
mongo_load crm signups docs/src/examples/signups.json
export SIGNUPS_MONGO_URL=$mongo_url
SIGNUPS_PG_URL=$(pg_db signups_plan_errors)
export SIGNUPS_PG_URL
CONFIG=docs/src/examples/plan-errors/ferry
TARGET=signups_plan_errors
ARGS=plan
EXIT=4
