import pytest
import psycopg2
import time
import duckdb
import math
import datetime
import json
from decimal import *
from utils_pytest import *

test_cases = [
    (
        "int incremental",
        "select * from generate_series(1, 10) s left join gs.tbl on (s = int_val)",
        True,
    ),
    (
        "int step",
        "select * from generate_series(1, 10, 2) s left join gs.tbl on (s = int_val)",
        True,
    ),
    (
        "timestamp",
        "select * from generate_series('2024-03-01', '2024-03-31', interval '1 day') s left join gs.tbl on (s::date = date_val)",
        True,
    ),
    (
        "timestamptz",
        "select * from generate_series('2024-03-01'::timestamptz, '2024-03-31'::timestamptz, interval '1 day') s left join gs.tbl on (s::date = date_val)",
        True,
    ),
    (
        "int alias",
        "select * from generate_series(1, 10) s (a) left join gs.tbl on (a = int_val)",
        True,
    ),
    ("int target list", "select generate_series(1, int_val) from gs.tbl", False),
    (
        "int in",
        "select count(*) from gs.tbl where int_val in (select generate_series(1, 10))",
        False,
    ),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, query, pushdown",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_time_functions_pushdown(
    create_generate_series_table, pg_conn, test_id, query, pushdown
):
    if pushdown:
        assert_remote_query_contains_expression(query, "generate_series", pg_conn)
    else:
        assert_remote_query_not_contains_expression(query, "generate_series", pg_conn)

    assert_query_results_on_tables(
        query, pg_conn, ["generate_series.tbl"], ["generate_series.heap_tbl"]
    )


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_generate_series_table(pg_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/create_generate_series_table/data.parquet"

    run_command(
        f"""
        CREATE SCHEMA gs;
	    CREATE TABLE gs.heap_tbl (
            int_val int,
            date_val date
	    );
        INSERT INTO gs.heap_tbl VALUES (3, '2024-03-01'), (5, '2024-03-04');
	    COPY gs.heap_tbl TO '{url}';

	    CREATE FOREIGN TABLE gs.tbl ()
	    SERVER pg_lake OPTIONS (path '{url}');
    """,
        pg_conn,
    )

    yield

    pg_conn.rollback()
