import pytest
import psycopg2
from utils_pytest import *
import json
import re


@pytest.mark.parametrize("table_type", ["heap", "iceberg"])
def test_pgaudit_integration(pg_conn, s3, extension, with_default_location, table_type):
    run_command(
        """
                CREATE SCHEMA test_pgaudit_integration;
                """,
        pg_conn,
    )
    pg_conn.commit()

    path = "s3://" + TEST_BUCKET + "/test_pgaudit_integration/data"

    # should be able to create a writable table with location
    run_command(
        f"""CREATE TABLE test_pgaudit_integration.test (
                    id int
                ) USING {table_type};
    """,
        pg_conn,
    )

    pgaudit_log_options = [
        "READ",
        "WRITE",
        "FUNCTION",
        "ROLE",
        "DDL",
        "MISC",
        "MISC_SET",
        "ALL",
    ]

    for opt in pgaudit_log_options:
        run_command(f"SET pgaudit.log TO '{opt}';", pg_conn)

        # insert
        run_command("INSERT INTO test_pgaudit_integration.test VALUES (1)", pg_conn)
        run_command(
            "INSERT INTO test_pgaudit_integration.test SELECT * FROM test_pgaudit_integration.test",
            pg_conn,
        )

        # copy
        run_command(
            f"COPY (SELECT * FROM test_pgaudit_integration.test) TO '{path}/data.csv'",
            pg_conn,
        )
        run_command(
            f"COPY test_pgaudit_integration.test FROM '{path}/data.csv'", pg_conn
        )

        # update/delete
        run_command("UPDATE test_pgaudit_integration.test SET id = id + 1", pg_conn)
        run_command(
            "DELETE FROM test_pgaudit_integration.test WHERE id > 10000", pg_conn
        )

        # select
        run_command("SELECT count(*) FROM test_pgaudit_integration.test", pg_conn)

    run_command("DROP SCHEMA test_pgaudit_integration CASCADE", pg_conn)
    pg_conn.commit()

    run_command(f"RESET pgaudit.log;", pg_conn)
