#!/usr/bin/env bash
# Loads products, users, transactions, and sessions into the ecommerce database.
# Safe to re-run: each collection is dropped and rebuilt fresh, so running this
# multiple times never produces duplicate documents.

set -euo pipefail

CONTAINER_NAME="mongo-ecommerce"
DB_NAME="ecommerce"
IMPORT_DIR="/data/import"   # path INSIDE the container (set by docker-compose volume mount)

echo "Target container: ${CONTAINER_NAME}, database: ${DB_NAME}"
echo ""

# Fail fast with a clear message if the container isn't up yet
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  echo "ERROR: Container '${CONTAINER_NAME}' is not running."
  echo "Start it first with: docker-compose up -d"
  exit 1
fi

import_fresh() {
  local collection="$1"
  local file="$2"
  echo "Importing ${file} -> ${collection} (--drop, fresh load)..."
  docker exec -i "$CONTAINER_NAME" mongoimport \
    --db "$DB_NAME" \
    --collection "$collection" \
    --file "${IMPORT_DIR}/${file}" \
    --jsonArray \
    --drop
}

import_append() {
  local collection="$1"
  local file="$2"
  echo "Importing ${file} -> ${collection} (append)..."
  docker exec -i "$CONTAINER_NAME" mongoimport \
    --db "$DB_NAME" \
    --collection "$collection" \
    --file "${IMPORT_DIR}/${file}" \
    --jsonArray
}

import_fresh   products      products.json
import_fresh   users         users.json
import_fresh   transactions  transactions.json
import_fresh   sessions      sessions_0.json   # drops+loads first half
import_append  sessions      sessions_1.json   # appends second half (no --drop)

echo ""
echo "Verifying document counts..."
docker exec -i "$CONTAINER_NAME" mongosh "$DB_NAME" --quiet --eval '
  print("products:     " + db.products.countDocuments());
  print("users:        " + db.users.countDocuments());
  print("transactions: " + db.transactions.countDocuments());
  print("sessions:     " + db.sessions.countDocuments());
'

echo ""
echo "Done."