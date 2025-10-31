import pytest
from utils_pytest import *


def test_bytea_domain(extension, pg_conn, s3, with_default_location):
    run_command(
        f"""
        create domain bbb as bytea;
        create table test (x int, b bbb default '\\x0000') using iceberg;
        insert into test values (1);
    """,
        pg_conn,
    )

    result = run_query(
        f"""
        select b from test
    """,
        pg_conn,
    )
    assert bytes(result[0]["b"]) == b"\x00\x00"

    pg_conn.rollback()


def test_copy_from_domain(extension, pg_conn, s3, with_default_location):
    url = f"s3://{TEST_BUCKET}/test_copy_from_domain/input.parquet"

    run_command(
        f"""
        copy (select 9 as x, -1 as y) to '{url}';

        create domain positive as int check (value > 0);

        create table test_copy_from_domain (x int, y positive default 2) using iceberg;
    """,
        pg_conn,
    )

    # Domain should be checked
    error = run_command(
        f"""
        copy test_copy_from_domain from '{url}'
    """,
        pg_conn,
        raise_error=False,
    )
    assert "value for domain positive violates check constraint" in error

    pg_conn.rollback()
