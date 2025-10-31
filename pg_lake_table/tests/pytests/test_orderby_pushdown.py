import pytest
from utils_pytest import *

# GROUP BY is needed for pushing down ORDER BY in fdw
queries = [
    "SELECT a FROM table_with_null GROUP BY 1 ORDER BY 1",
    "SELECT a FROM table_with_null GROUP BY 1 ORDER BY 1 DESC",
    "SELECT a FROM table_with_null GROUP BY 1 ORDER BY 1 ASC",
    "SELECT a FROM table_with_null GROUP BY 1 ORDER BY 1 DESC NULLS FIRST",
    "SELECT a FROM table_with_null GROUP BY 1 ORDER BY 1 ASC  NULLS FIRST",
    "SELECT a FROM table_with_null GROUP BY 1 ORDER BY 1 DESC NULLS LAST",
    "SELECT a FROM table_with_null GROUP BY 1 ORDER BY 1 ASC  NULLS LAST",
    "SELECT a FROM table_with_null ORDER BY 1 ASC LIMIT (SELECT max(a) FROM table_with_null)",
]


# Postgres and DuckDB has some incompatibilities in certain
# sort orders, see old repo issues/287
# for the details
# In this test, we make sure that we pushdown ORDER BYs, and return
# the exact same results as you'd expect from Postgres
def test_order_by_pushdown(pg_conn, s3, extension):
    url_nulls = f"s3://{TEST_BUCKET}/test_order_by_pushdown/nulls/"

    run_command(
        f"""
                CREATE TABLE table_with_null(a int);
                CREATE FOREIGN TABLE table_with_null_f (a int) SERVER pg_lake OPTIONS (format 'parquet', writable 'true', location '{url_nulls}');
                INSERT INTO table_with_null VALUES (-1), (0), (NULL), (1), (1), (1);
                INSERT INTO table_with_null_f SELECT * FROM table_with_null;
                """,
        pg_conn,
    )

    for query in queries:

        # here, we explicitly not use assert_query_results_on_tables
        # because that function sorts the return values, we want to
        # test ordering done by the database in this test
        heap_query_result = perform_query_on_cursor(query, pg_conn)

        fdw_query = query.replace("table_with_null", "table_with_null_f")
        fdw_query_result = perform_query_on_cursor(fdw_query, pg_conn)
        assert heap_query_result == fdw_query_result

        # make sure that we pushdown ORDER BYs
        assert_remote_query_contains_expression(fdw_query, "ORDER BY", pg_conn)

    pg_conn.rollback()
