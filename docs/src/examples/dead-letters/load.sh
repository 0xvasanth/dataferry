# Loaded by docs/scripts/check-snippets.sh --live.
mongo_load crm signups docs/src/examples/signups.json
export SIGNUPS_MONGO_URL=$mongo_url
SIGNUPS_PG_URL=$(pg_db signups_dead_letters)
export SIGNUPS_PG_URL
CONFIG=docs/src/examples/dead-letters/ferry
TARGET=signups_dead_letters
