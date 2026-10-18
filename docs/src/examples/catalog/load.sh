# Loaded by docs/scripts/check-snippets.sh --live: the source documents in
# Mongo, and an empty Postgres for the target.
mongo_load shop products docs/src/examples/catalog/products.json
export CATALOG_MONGO_URL=$mongo_url
CATALOG_PG_URL=$(pg_db catalog)
export CATALOG_PG_URL
CONFIG=docs/src/examples/catalog/ferry
TARGET=catalog
