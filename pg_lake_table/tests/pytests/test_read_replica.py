import os
import pytest
from utils_pytest import *


def atest_read_replica(superuser_conn, pg_replica_conn, installcheck, s3):
    # We currently do not create a read replica of a running server,
    # since the set up steps are heavily environment dependent.
    if installcheck:
        return

    url = f"s3://{TEST_BUCKET}/test_read_replica/data.parquet"

    # Verify we can perform COPY TO from a replica
    run_command(
        f"""
        COPY (SELECT s, 'hello-'||s AS d FROM generate_series(1,100) s) TO '{url}';
    """,
        pg_replica_conn,
    )

    # Create a foreign table on the primary
    run_command(
        f"""
        SET synchronous_commit TO 'remote_apply';
        CREATE SCHEMA read_replica;
        CREATE FOREIGN TABLE read_replica.test_read_replica () SERVER pg_lake OPTIONS (path '{url}');
    """,
        superuser_conn,
    )

    superuser_conn.commit()

    # Query the foreign table from the replica
    result = run_query("SELECT * FROM read_replica.test_read_replica", pg_replica_conn)
    assert len(result) == 100

    pg_replica_conn.rollback()

    fhrgoal_key = "sample_data/fhrgoal.parquet"
    local_file_path = sampledata_filepath(f"fhrgoal.parquet")
    s3.upload_file(local_file_path, TEST_BUCKET, fhrgoal_key)

    # Create a foreign table on the primary which has few
    # composite types created underneath
    run_command(
        f"""
                CREATE FOREIGN TABLE read_replica.fhrgoal ()
                SERVER pg_lake
                OPTIONS (format 'parquet', path 's3://{TEST_BUCKET}/{fhrgoal_key}');
        """,
        superuser_conn,
    )

    superuser_conn.commit()

    # Query the foreign table from the replica
    result = run_query("SELECT * FROM read_replica.fhrgoal", pg_replica_conn)
    assert len(result) == 10
    pg_replica_conn.commit()

    run_command(
        f"""
               DROP SCHEMA read_replica cascade;
        """,
        superuser_conn,
    )
    superuser_conn.commit()
