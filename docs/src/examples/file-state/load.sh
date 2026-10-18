# Loaded by docs/scripts/check-snippets.sh --live: the catalog example with
# its state in the work directory instead of on the sink.
mongo_load shop products docs/src/examples/catalog/products.json
export CATALOG_MONGO_URL=$mongo_url
CATALOG_PG_URL=$(pg_db file_state)
export CATALOG_PG_URL
CONFIG=docs/src/examples/file-state/ferry
TARGET=file_state
