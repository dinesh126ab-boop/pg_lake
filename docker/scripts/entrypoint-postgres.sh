#!/bin/bash

set -euo pipefail

trap "echo 'Caught termination signal. Exiting...'; exit 0" SIGINT SIGTERM

# Update pg_hba.conf
echo "local all all trust" | tee $PGBASEDIR/pgsql-$PG_MAJOR/data/pg_hba.conf
echo "host all all 127.0.0.1/32 trust" | tee -a $PGBASEDIR/pgsql-$PG_MAJOR/data/pg_hba.conf
echo "host all all ::1/128 trust" | tee -a $PGBASEDIR/pgsql-$PG_MAJOR/data/pg_hba.conf

# Update postgresql.conf
echo "port = 5432" | tee -a $PGBASEDIR/pgsql-$PG_MAJOR/data/postgresql.conf
echo "shared_preload_libraries = 'pg_extension_base'" | tee -a $PGBASEDIR/pgsql-$PG_MAJOR/data/postgresql.conf
echo "pg_lake_iceberg.default_location_prefix = 's3://testbucket/pg_lake/'" | tee -a $PGBASEDIR/pgsql-$PG_MAJOR/data/postgresql.conf
echo "pg_lake_engine.host = 'host=/home/postgres/pgduck_socket_dir port=5332'" | tee -a $PGBASEDIR/pgsql-$PG_MAJOR/data/postgresql.conf

# Start PostgreSQL server
pg_ctl -D $PGBASEDIR/pgsql-$PG_MAJOR/data start -l $PGBASEDIR/pgsql-$PG_MAJOR/data/logfile

# bind volumes have root permission at start (make this readable and writable by postgres)
sudo chown -R postgres:postgres /home/postgres/pgsql-{16,17,18}/data/base/pgsql_tmp

# Run initialization script
psql -U postgres -f /init-postgres.sql

sleep infinity
