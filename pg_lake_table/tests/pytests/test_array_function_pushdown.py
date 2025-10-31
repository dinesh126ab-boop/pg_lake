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
        "array_append",
        "WHERE array_append(col_int_array, 2) = array[13,1,2]",
        "array_append",
        True,
    ),
    (
        "array_cat",
        "WHERE array_cat(col_int_array, array[4,5]) = array[13,1,4,5]",
        "array_cat",
        True,
    ),
    (
        "array_length_1",
        "WHERE array_length(col_int_array, 1) is null",
        "array_length",
        True,
    ),
    (
        "array_length_2",
        "WHERE array_length(col_int_array, 2) is null",
        "array_length",
        False,
    ),
    (
        "array_position",
        "WHERE array_position(col_text_array, 'hello') = 1",
        "array_position",
        False,
    ),
    (
        "array_prepend",
        "WHERE array_prepend('say', col_text_array) = array['say','hello','world']",
        "array_prepend",
        True,
    ),
    (
        "array_to_string",
        "WHERE array_to_string(col_text_array, '--') = 'hello--world'",
        "array_to_string",
        False,
    ),
    ("cardinality", "WHERE cardinality(col_text_array) = 0", "array_length", True),
]


@pytest.mark.parametrize(
    "test_id, operator_expression, expected_expression, assert_pushdown",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_array_function_pushdown(
    create_test_array_function_pushdown_table,
    pg_conn,
    test_id,
    operator_expression,
    expected_expression,
    assert_pushdown,
):
    query = "SELECT * FROM array_function_pushdown.tbl " + operator_expression

    if assert_pushdown:
        assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    else:
        assert_remote_query_not_contains_expression(query, expected_expression, pg_conn)

    assert_query_results_on_tables(
        query,
        pg_conn,
        ["array_function_pushdown.tbl"],
        ["array_function_pushdown.heap_tbl"],
    )


@pytest.fixture(scope="module")
def create_test_array_function_pushdown_table(pg_conn, s3, request, extension):

    run_command(
        """
	            CREATE SCHEMA array_function_pushdown;

	            """,
        pg_conn,
    )
    pg_conn.commit()

    url = f"s3://{TEST_BUCKET}/{request.node.name}/data.parquet"
    run_command(
        f"""
			COPY (
					SELECT  NULL::int[] as int_array, NULL::text[] as text_array
						UNION ALL
					SELECT  array[3,4] as int_array, array['hello', 'world'] as text_array
						UNION ALL
					SELECT  array[13,1] as int_array, array['', 'zoo'] as text_array
						UNION ALL
					SELECT  array[]::int[] as int_array, array[]::text[] as text_array

				) TO '{url}' WITH (FORMAT 'parquet');
		""",
        pg_conn,
    )
    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
	            CREATE FOREIGN TABLE array_function_pushdown.tbl
	            (
					col_int_array int[],
					col_text_array text[]
	            ) SERVER pg_lake OPTIONS (format 'parquet', path '{}');
	            """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
	            CREATE TABLE array_function_pushdown.heap_tbl
                AS SELECT * FROM array_function_pushdown.tbl;
	            """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command("DROP SCHEMA array_function_pushdown CASCADE", pg_conn)
    pg_conn.commit()
