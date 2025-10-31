import pytest
from utils_pytest import *


def test_special_numeric(s3, pg_conn, extension, with_default_location):

    run_command(
        """
                CREATE SCHEMA test_special_numeric;

                CREATE TABLE test_special_numeric.unbounded(b numeric) USING iceberg;
                CREATE TABLE test_special_numeric.bounded(b numeric(20,10)) USING iceberg;

        """,
        pg_conn,
    )
    pg_conn.commit()

    values = [
        "Inf",
        "+Inf",
        "Infinity",
        "-Infinity",
        "   -inf  ",
        "nan",
        "Nan",
        " +Infinity ",
    ]
    tables = ["test_special_numeric.unbounded", "test_special_numeric.bounded"]

    for table in tables:
        for value in values:

            err = run_command(
                f"INSERT INTO {table} VALUES ('{value}')", pg_conn, raise_error=False
            )
            assert (
                "Special numeric values" in str(err)
                and "are not allowed for numeric type" in str(err)
            ) or "cannot hold an infinite value" in str(err)

            pg_conn.rollback()

    run_command(
        """
                DROP SCHEMA test_special_numeric CASCADE;
                """,
        pg_conn,
    )
    pg_conn.commit()
