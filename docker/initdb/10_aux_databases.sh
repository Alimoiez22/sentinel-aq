#!/usr/bin/env bash
# Auxiliary databases owned by the pipeline role (ADR-016):
#   warehouse_test -> integration tests (reset freely)
#   warehouse_exp  -> fault-injection experiments (reset between trials)
# Keeping them separate means a test or an experiment reset can never touch `warehouse`.
# Idempotent: runs at first start (docker-entrypoint-initdb.d) and via `make db-aux`
# on an existing volume.
set -euo pipefail

for db in warehouse_test warehouse_exp; do
  exists=$(psql -At --username "$POSTGRES_USER" --dbname postgres \
           -c "SELECT 1 FROM pg_database WHERE datname = '$db'")
  if [ "$exists" != "1" ]; then
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
      -c "CREATE DATABASE $db OWNER \"$SENTINEL_DB_USER\"" \
      -c "REVOKE CONNECT ON DATABASE $db FROM PUBLIC" \
      -c "GRANT CONNECT ON DATABASE $db TO \"$SENTINEL_DB_USER\""
  fi
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" \
    -c "CREATE EXTENSION IF NOT EXISTS postgis"
done
