#!/usr/bin/env bash
# Postgres entrypoint runs every file under /docker-entrypoint-initdb.d in
# lexical order. We need two databases: `temporal` (used by Temporal Server's
# auto-setup image for its history store) and `bare_metal_tests` (our result
# store). Then we apply the bmt schema into the latter.

set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname postgres <<-EOSQL
    CREATE DATABASE temporal;
    CREATE DATABASE bare_metal_tests;
EOSQL

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname bare_metal_tests \
    -f /docker-entrypoint-initdb.d/001_init.sql
