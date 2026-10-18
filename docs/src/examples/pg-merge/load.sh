# Loaded by docs/scripts/check-snippets.sh --live: examples/pg-merge, its
# source schema and rows in `app`, and an empty `reporting`.
APP_DATABASE_URL=$(pg_db pg_merge_app)
export APP_DATABASE_URL
pg_load pg_merge_app examples/pg-merge/source.sql
REPORTING_DATABASE_URL=$(pg_db pg_merge_reporting)
export REPORTING_DATABASE_URL
CONFIG=examples/pg-merge/ferry
TARGET=pg_merge_reporting
