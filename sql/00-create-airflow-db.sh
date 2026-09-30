#!/bin/bash
# Runs once when the postgres container's data volume is first created.
# The postgres image already creates $POSTGRES_DB ($POSTGRES_USER owns it);
# this just adds a second database for Airflow's metadata so we don't need
# a second Postgres container.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE ${AIRFLOW_DB:-airflow} OWNER ${POSTGRES_USER}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${AIRFLOW_DB:-airflow}')\gexec
EOSQL
