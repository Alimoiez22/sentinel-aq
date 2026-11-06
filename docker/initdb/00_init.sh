#!/usr/bin/env bash
# Runs once, when the database volume is empty (standard postgres image behaviour).
# Creates two least-privilege roles and two databases on one server (ADR-002):
#   sentinel -> owns `warehouse` (pipeline, marts, ops, golden schemas)
#   airflow  -> owns `airflow_meta` (Airflow's own state)
# Neither role is a superuser. The superuser from POSTGRES_USER is used only here.
set -euo pipefail

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" --dbname postgres \
  -v sentinel_user="$SENTINEL_DB_USER" -v sentinel_pw="$SENTINEL_DB_PASSWORD" \
  -v sentinel_db="$SENTINEL_DB_NAME" \
  -v airflow_user="$AIRFLOW_DB_USER" -v airflow_pw="$AIRFLOW_DB_PASSWORD" <<'SQL'
CREATE ROLE :"sentinel_user" LOGIN PASSWORD :'sentinel_pw';
CREATE ROLE :"airflow_user"  LOGIN PASSWORD :'airflow_pw';

CREATE DATABASE :"sentinel_db"  OWNER :"sentinel_user";
CREATE DATABASE airflow_meta    OWNER :"airflow_user";

-- New databases allow CONNECT to PUBLIC by default; close that between the two.
REVOKE CONNECT ON DATABASE :"sentinel_db" FROM PUBLIC;
REVOKE CONNECT ON DATABASE airflow_meta  FROM PUBLIC;
SQL

# PostGIS must be created by a superuser; the warehouse owner can then use it.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$SENTINEL_DB_NAME" <<'SQL'
CREATE EXTENSION IF NOT EXISTS postgis;
SQL
