import pytest
import psycopg2
from utils_pytest import *


def test_ddl_snapshot(s3, pg_conn, installcheck, extension, with_default_location):

    run_command(
        """
                CREATE SCHEMA test_ddl_snapshot;
                CREATE TABLE test_ddl_snapshot.ft1 (
                    id int,
                    value text NOT NULL
                ) USING iceberg WITH (autovacuum_enabled='False');
        """,
        pg_conn,
    )
    pg_conn.commit()

    # when there is no snapshot, certain DDLs should still push new snapshots
    res = run_query(
        "SELECT count(*) FROM lake_iceberg.snapshots((SELECT metadata_location FROM iceberg_tables WHERE table_name='ft1' and table_namespace='test_ddl_snapshot'))",
        pg_conn,
    )
    assert res == [[0]]

    commands = [
        "ALTER TABLE test_ddl_snapshot.ft1 ADD COLUMN col1 INT",
        "ALTER TABLE test_ddl_snapshot.ft1 DROP COLUMN col1",
        "ALTER TABLE test_ddl_snapshot.ft1 ALTER COLUMN id SET DEFAULT 15",
        "ALTER TABLE test_ddl_snapshot.ft1 ALTER COLUMN id DROP DEFAULT",
        "ALTER TABLE test_ddl_snapshot.ft1 ALTER COLUMN value DROP NOT NULL",
    ]

    expectedSnapshotCount, expectedMetadataNo = 1, 1
    for command in commands:
        run_command(command, pg_conn)
        pg_conn.commit()

        res = run_query(
            "SELECT count(*) FROM lake_iceberg.snapshots((SELECT metadata_location FROM iceberg_tables WHERE table_name='ft1' and table_namespace='test_ddl_snapshot'))",
            pg_conn,
        )
        assert res == [[expectedSnapshotCount]]
        metadata_location = run_query(
            "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'ft1' and table_namespace='test_ddl_snapshot'",
            pg_conn,
        )[0][0]
        assert str(expectedMetadataNo) + "-" in metadata_location
        expectedSnapshotCount, expectedMetadataNo = (
            expectedSnapshotCount + 1,
            expectedMetadataNo + 1,
        )

    # now, with some data
    run_command("INSERT INTO test_ddl_snapshot.ft1 VALUES (1,'1')", pg_conn)
    pg_conn.commit()

    # increment as we did an insert
    expectedSnapshotCount, expectedMetadataNo = (
        expectedSnapshotCount + 1,
        expectedMetadataNo + 1,
    )

    for command in commands:
        run_command(command, pg_conn)
        pg_conn.commit()

        res = run_query(
            "SELECT count(*) FROM lake_iceberg.snapshots((SELECT metadata_location FROM iceberg_tables WHERE table_name='ft1' and table_namespace='test_ddl_snapshot'))",
            pg_conn,
        )
        assert res == [[expectedSnapshotCount]]
        metadata_location = run_query(
            "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'ft1' and table_namespace='test_ddl_snapshot'",
            pg_conn,
        )[0][0]
        assert str(expectedMetadataNo) + "-" in metadata_location
        expectedSnapshotCount, expectedMetadataNo = (
            expectedSnapshotCount + 1,
            expectedMetadataNo + 1,
        )

    run_command("DROP SCHEMA test_ddl_snapshot CASCADE", pg_conn)
    pg_conn.commit()
