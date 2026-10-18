# Loaded by docs/scripts/check-snippets.sh --live: examples/golden-pg, the
# golden documents as rows in `shop`, and an empty `warehouse`.
FERRY_GOLDEN_PG_SOURCE_URL=$(pg_db golden_pg_shop)
export FERRY_GOLDEN_PG_SOURCE_URL
pg_load golden_pg_shop examples/golden-pg/source.sql
FERRY_GOLDEN_PG_URL=$(pg_db golden_pg_warehouse)
export FERRY_GOLDEN_PG_URL
CONFIG=examples/golden-pg/ferry
TARGET=golden_pg_warehouse
