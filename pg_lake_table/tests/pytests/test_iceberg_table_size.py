import pytest
import psycopg2
from utils_pytest import *


def test_iceberg_table_size(pg_conn, s3, extension, with_default_location):
    run_command(
        """
        CREATE TABLE test_iceberg_table_size USING iceberg AS
        SELECT s, md5(s::text) FROM generate_series(1,100) s
    """,
        pg_conn,
    )

    # Check the Iceberg table size function, and also the pg_table_size
    result = run_query(
        """
        SELECT lake_iceberg.table_size('test_iceberg_table_size') cts, pg_table_size('test_iceberg_table_size') pts;
    """,
        pg_conn,
    )
    assert result[0]["cts"] > 0
    assert result[0]["cts"] == result[0]["pts"]

    pg_conn.rollback()


def test_iceberg_table_size_heap(pg_conn, s3, extension, with_default_location):
    run_command(
        """
        CREATE TABLE test_iceberg_table_size_heap USING heap AS
        SELECT s, md5(s::text) FROM generate_series(1,100) s
    """,
        pg_conn,
    )

    # Check that both size function work for heap
    result = run_query(
        """
        SELECT lake_iceberg.table_size('test_iceberg_table_size_heap') cts, pg_table_size('test_iceberg_table_size_heap') pts;
    """,
        pg_conn,
    )
    assert result[0]["cts"] > 0
    assert result[0]["cts"] == result[0]["pts"]

    pg_conn.rollback()
