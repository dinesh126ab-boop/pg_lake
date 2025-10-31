import pytest
import psycopg2
import time
import duckdb
import math
from utils_pytest import *


def test_select_with_different_timezone(
    s3, extension, superuser_conn, pg_conn, with_default_location
):
    run_command(
        "CREATE TABLE test_change_pg_timezone (a timestamptz) USING iceberg", pg_conn
    )

    # insert with Berlin timezone
    initial_timezone = change_timezone(superuser_conn, "Europe/Berlin")

    run_command(
        "INSERT INTO test_change_pg_timezone VALUES ('2025-05-05 14:00:00+04')", pg_conn
    )
    run_command(
        "INSERT INTO test_change_pg_timezone VALUES ('2022-01-01 14:00:00+04')", pg_conn
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["2022-01-01 11:00:00+01:00"],
        ["2025-05-05 12:00:00+02:00"],
    ]

    # insert with UTC timezone
    change_timezone(superuser_conn, "UTC")

    run_command(
        "INSERT INTO test_change_pg_timezone VALUES ('2024-02-29 01:00:00+04')", pg_conn
    )
    run_command(
        "INSERT INTO test_change_pg_timezone VALUES ('1999-12-12 22:15:33-03')", pg_conn
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["1999-12-13 01:15:33+00:00"],
        ["2022-01-01 10:00:00+00:00"],
        ["2024-02-28 21:00:00+00:00"],
        ["2025-05-05 10:00:00+00:00"],
    ]

    # insert with New York timezone
    change_timezone(superuser_conn, "America/New_York")

    run_command(
        "INSERT INTO test_change_pg_timezone VALUES ('2019-02-28 01:00:00-07:30')",
        pg_conn,
    )
    run_command(
        "INSERT INTO test_change_pg_timezone VALUES ('2018-01-01 01:59:59-03')", pg_conn
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["1999-12-12 20:15:33-05:00"],
        ["2017-12-31 23:59:59-05:00"],
        ["2019-02-28 03:30:00-05:00"],
        ["2022-01-01 05:00:00-05:00"],
        ["2024-02-28 16:00:00-05:00"],
        ["2025-05-05 06:00:00-04:00"],
    ]

    # insert with Bejing timezone
    change_timezone(superuser_conn, "Asia/Shanghai")

    run_command(
        "INSERT INTO test_change_pg_timezone VALUES ('1910-07-06 01:00:00+02')", pg_conn
    )
    run_command(
        "INSERT INTO test_change_pg_timezone VALUES ('1908-01-01 22:00:00-03:30')",
        pg_conn,
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["1908-01-02 09:30:00+08:00"],
        ["1910-07-06 07:00:00+08:00"],
        ["1999-12-13 09:15:33+08:00"],
        ["2018-01-01 12:59:59+08:00"],
        ["2019-02-28 16:30:00+08:00"],
        ["2022-01-01 18:00:00+08:00"],
        ["2024-02-29 05:00:00+08:00"],
        ["2025-05-05 18:00:00+08:00"],
    ]

    # reset timezone
    change_timezone(superuser_conn, initial_timezone)

    pg_conn.rollback()


def test_insert_select_with_different_timezone(
    s3, extension, superuser_conn, pg_conn, with_default_location
):
    run_command(
        """CREATE TABLE test_change_pg_timezone (a timestamptz) USING iceberg;
           CREATE TABLE test_change_pg_timezone_tmp (a timestamptz);""",
        pg_conn,
    )

    # insert with Berlin timezone
    initial_timezone = change_timezone(superuser_conn, "Europe/Berlin")

    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('2025-05-05 14:00:00+04')",
        pg_conn,
    )
    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('2022-01-01 14:00:00+04')",
        pg_conn,
    )
    run_command(
        """INSERT INTO test_change_pg_timezone SELECT * FROM test_change_pg_timezone_tmp;
            TRUNCATE test_change_pg_timezone_tmp;""",
        pg_conn,
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["2022-01-01 11:00:00+01:00"],
        ["2025-05-05 12:00:00+02:00"],
    ]

    # insert with UTC timezone
    change_timezone(superuser_conn, "UTC")

    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('2024-02-29 01:00:00+04')",
        pg_conn,
    )
    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('1999-12-12 22:15:33-03')",
        pg_conn,
    )
    run_command(
        """INSERT INTO test_change_pg_timezone SELECT * FROM test_change_pg_timezone_tmp;
            TRUNCATE test_change_pg_timezone_tmp;""",
        pg_conn,
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["1999-12-13 01:15:33+00:00"],
        ["2022-01-01 10:00:00+00:00"],
        ["2024-02-28 21:00:00+00:00"],
        ["2025-05-05 10:00:00+00:00"],
    ]

    # insert with New York timezone
    change_timezone(superuser_conn, "America/New_York")

    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('2019-02-28 01:00:00-07:30')",
        pg_conn,
    )
    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('2018-01-01 01:59:59-03')",
        pg_conn,
    )
    run_command(
        """INSERT INTO test_change_pg_timezone SELECT * FROM test_change_pg_timezone_tmp;
            TRUNCATE test_change_pg_timezone_tmp;""",
        pg_conn,
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["1999-12-12 20:15:33-05:00"],
        ["2017-12-31 23:59:59-05:00"],
        ["2019-02-28 03:30:00-05:00"],
        ["2022-01-01 05:00:00-05:00"],
        ["2024-02-28 16:00:00-05:00"],
        ["2025-05-05 06:00:00-04:00"],
    ]

    # insert with Bejing timezone
    change_timezone(superuser_conn, "Asia/Shanghai")

    run_command(
        """INSERT INTO test_change_pg_timezone SELECT '1910-07-06 01:00:00+02';
           INSERT INTO test_change_pg_timezone SELECT '1908-01-01 22:00:00-03:30';
           TRUNCATE test_change_pg_timezone_tmp;""",
        pg_conn,
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["1908-01-02 09:30:00+08:00"],
        ["1910-07-06 07:00:00+08:00"],
        ["1999-12-13 09:15:33+08:00"],
        ["2018-01-01 12:59:59+08:00"],
        ["2019-02-28 16:30:00+08:00"],
        ["2022-01-01 18:00:00+08:00"],
        ["2024-02-29 05:00:00+08:00"],
        ["2025-05-05 18:00:00+08:00"],
    ]

    # reset timezone
    change_timezone(superuser_conn, initial_timezone)

    pg_conn.rollback()


def test_copy_from_with_different_timezone(
    s3, extension, superuser_conn, pg_conn, with_default_location
):
    path = (
        "s3://" + TEST_BUCKET + "/test_copy_from_with_different_timezone/test.parquet"
    )

    run_command(
        """CREATE TABLE test_change_pg_timezone (a timestamptz) USING iceberg;
           CREATE TABLE test_change_pg_timezone_tmp (a timestamptz);""",
        pg_conn,
    )

    # insert with Berlin timezone
    initial_timezone = change_timezone(superuser_conn, "Europe/Berlin")

    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('2025-05-05 14:00:00+04')",
        pg_conn,
    )
    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('2022-01-01 14:00:00+04')",
        pg_conn,
    )
    run_command(
        f"""COPY test_change_pg_timezone_tmp TO '{path}' with (format parquet);
           COPY test_change_pg_timezone FROM '{path}' with (format parquet);
           TRUNCATE test_change_pg_timezone_tmp;""",
        pg_conn,
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["2022-01-01 11:00:00+01:00"],
        ["2025-05-05 12:00:00+02:00"],
    ]

    # insert with UTC timezone
    change_timezone(superuser_conn, "UTC")

    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('2024-02-29 01:00:00+04')",
        pg_conn,
    )
    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('1999-12-12 22:15:33-03')",
        pg_conn,
    )
    run_command(
        f"""COPY test_change_pg_timezone_tmp TO '{path}' with (format parquet);
           COPY test_change_pg_timezone FROM '{path}' with (format parquet);
           TRUNCATE test_change_pg_timezone_tmp;""",
        pg_conn,
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["1999-12-13 01:15:33+00:00"],
        ["2022-01-01 10:00:00+00:00"],
        ["2024-02-28 21:00:00+00:00"],
        ["2025-05-05 10:00:00+00:00"],
    ]

    # insert with New York timezone
    change_timezone(superuser_conn, "America/New_York")

    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('2019-02-28 01:00:00-07:30')",
        pg_conn,
    )
    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('2018-01-01 01:59:59-03')",
        pg_conn,
    )
    run_command(
        f"""COPY test_change_pg_timezone_tmp TO '{path}' with (format parquet);
           COPY test_change_pg_timezone FROM '{path}' with (format parquet);
           TRUNCATE test_change_pg_timezone_tmp;""",
        pg_conn,
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["1999-12-12 20:15:33-05:00"],
        ["2017-12-31 23:59:59-05:00"],
        ["2019-02-28 03:30:00-05:00"],
        ["2022-01-01 05:00:00-05:00"],
        ["2024-02-28 16:00:00-05:00"],
        ["2025-05-05 06:00:00-04:00"],
    ]

    # insert with Bejing timezone
    change_timezone(superuser_conn, "Asia/Shanghai")

    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('1910-07-06 01:00:00+02')",
        pg_conn,
    )
    run_command(
        "INSERT INTO test_change_pg_timezone_tmp VALUES ('1908-01-01 22:00:00-03:30')",
        pg_conn,
    )
    run_command(
        f"""COPY test_change_pg_timezone_tmp TO '{path}' with (format parquet);
           COPY test_change_pg_timezone FROM '{path}' with (format parquet);
           TRUNCATE test_change_pg_timezone_tmp;""",
        pg_conn,
    )

    result = run_query("SELECT * FROM test_change_pg_timezone order by 1", pg_conn)

    assert datetime_result_to_str(result) == [
        ["1908-01-02 09:30:00+08:00"],
        ["1910-07-06 07:00:00+08:00"],
        ["1999-12-13 09:15:33+08:00"],
        ["2018-01-01 12:59:59+08:00"],
        ["2019-02-28 16:30:00+08:00"],
        ["2022-01-01 18:00:00+08:00"],
        ["2024-02-29 05:00:00+08:00"],
        ["2025-05-05 18:00:00+08:00"],
    ]

    # reset timezone
    change_timezone(superuser_conn, initial_timezone)

    pg_conn.rollback()


def datetime_result_to_str(result):
    for row in result:
        for i, value in enumerate(row):
            row[i] = str(value)

    return result
