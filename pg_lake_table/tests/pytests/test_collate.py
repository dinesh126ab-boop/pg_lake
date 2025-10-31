import pytest
from utils_pytest import *


def test_queries_with_collation(s3, pg_conn, installcheck, extension):

    url = f"s3://{TEST_BUCKET}/test_s3_collation/data.parquet"

    # Create a Parquet file in mock S3
    run_command(
        f"""
        COPY (SELECT s AS id, chr(64 + mod(s, 26)) || '-' || s AS desc FROM generate_series(1,100) s) TO '{url}';
    """,
        pg_conn,
    )

    # Create a table with 2 columns on the fdw with varying collation
    run_command(
        """
                CREATE SCHEMA test_fdw_clt;
                CREATE FOREIGN TABLE test_fdw_clt.ft1 (
                    id int,
                    value text COLLATE "POSIX"
                ) SERVER pg_lake OPTIONS (format 'parquet', path '%s');
        """
        % (url),
        pg_conn,
    )

    # Sort with collation in ORDER BY
    query = 'SELECT * FROM test_fdw_clt.ft1 ORDER BY value COLLATE "POSIX";'
    assert_remote_query_not_contains_collate(query, pg_conn)
    result = perform_query_on_cursor(query, pg_conn)
    assert len(result) == 100
    pg_conn.commit()

    # Sorting with collation in ORDER BY with concatenation
    query = "SELECT * FROM test_fdw_clt.ft1 ORDER BY value || '-' || value COLLATE \"POSIX\";"
    assert_remote_query_not_contains_collate(query, pg_conn)
    result = perform_query_on_cursor(query, pg_conn)
    assert len(result) == 100
    pg_conn.commit()

    # Direct collation comparison in WHERE clause
    query = 'SELECT * FROM test_fdw_clt.ft1 WHERE value COLLATE "C" >= \'G-20\' COLLATE "C";'
    assert_remote_query_not_contains_collate(query, pg_conn)
    result = perform_query_on_cursor(query, pg_conn)
    assert len(result) == 73
    pg_conn.commit()

    # Direct collation comparison in WHERE clause without specific collation on the constant
    query = "SELECT * FROM test_fdw_clt.ft1 WHERE value >= 'G-20';"
    assert_remote_query_not_contains_collate(query, pg_conn)
    result = perform_query_on_cursor(query, pg_conn)
    assert len(result) == 73
    pg_conn.commit()

    # Direct collation comparison in WHERE clause with exact same collation of the VAR on the constant
    query = "SELECT * FROM test_fdw_clt.ft1 WHERE value >= 'G-20' COLLATE \"POSIX\";"
    assert_remote_query_not_contains_collate(query, pg_conn)
    result = perform_query_on_cursor(query, pg_conn)
    assert len(result) == 73
    pg_conn.commit()

    # GROUP BY with collation
    query = 'SELECT SUBSTRING(value, 1, 1) COLLATE "POSIX" AS first_letter, COUNT(*) FROM test_fdw_clt.ft1 GROUP BY first_letter;'
    assert_remote_query_not_contains_collate(query, pg_conn)
    result = perform_query_on_cursor(query, pg_conn)
    assert len(result) == 26
    pg_conn.commit()

    # Aggregation with collation in ORDER BY
    query = 'SELECT array_agg(id ORDER BY value COLLATE "C") FROM test_fdw_clt.ft1;'
    assert_remote_query_not_contains_collate(query, pg_conn)
    result = perform_query_on_cursor(query, pg_conn)
    assert len(result) == 1
    pg_conn.commit()

    # Collate expression in SELECT
    query = "SELECT value COLLATE \"C\" < 'H-25' FROM test_fdw_clt.ft1;"
    assert_remote_query_not_contains_collate(query, pg_conn)
    result = perform_query_on_cursor(query, pg_conn)
    assert len(result) == 100
    pg_conn.commit()

    # Function with collation
    query = 'SELECT value, LOWER(value COLLATE "C") FROM test_fdw_clt.ft1;'
    assert_remote_query_not_contains_collate(query, pg_conn)
    result = perform_query_on_cursor(query, pg_conn)
    assert len(result) == 100
    pg_conn.commit()

    # Cleanup
    run_command("DROP SCHEMA test_fdw_clt CASCADE;", pg_conn)
    pg_conn.commit()


def assert_remote_query_not_contains_collate(query, pg_conn):
    explain = "EXPLAIN (ANALYZE, VERBOSE, format " "JSON" ") " + query
    explain_result = perform_query_on_cursor(explain, pg_conn)[0]
    remote_sql = find_remote_sql(explain_result[0][0]["Plan"])
    assert "collate".upper() not in remote_sql.upper()


def find_remote_sql(plan):
    if "Vectorized SQL" in plan:
        return plan["Vectorized SQL"]

    # If there are sub-plans, iterate through them recursively
    if "Plans" in plan:
        for subplan in plan["Plans"]:
            remote_sql = find_remote_sql(subplan)
            if remote_sql:
                return remote_sql
    assert False, "'Vectorized SQL' is found"
