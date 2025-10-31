import pytest
from utils_pytest import *


# these are queries that are NOT meant to be pushdown
# currently some of them are failing due to RECORD type being non-pushdownable
# but we explicitly wanted to add these tests such that in case say we allow RECORD
# type, we want to have explicit tests that should fail
queries = [
    """
WITH RECURSIVE search_users(user_from, user_to) AS (
            SELECT * FROM test_non_pushdown_queries.user_connections
            UNION ALL
            SELECT uc.*
            FROM test_non_pushdown_queries.user_connections uc
            JOIN search_users su ON uc.user_from = su.user_to
        ) CYCLE user_from, user_to SET is_cycle TO TRUE DEFAULT FALSE USING path
        SELECT * FROM search_users WHERE NOT is_cycle;""",
    "SELECT user_connections FROM test_non_pushdown_queries.user_connections;",
    "SELECT (user_connections)::text FROM test_non_pushdown_queries.user_connections;",
    "SELECT (user_connections.*)::text FROM test_non_pushdown_queries.user_connections;",
    "SELECT count(user_connections.*) FROM test_non_pushdown_queries.user_connections;",
]


def test_non_pushdown_queries(s3, pg_conn, extension):
    url = f"s3://{TEST_BUCKET}/test_non_pushdown_queries/data.parquet"

    run_command(
        f"""

        CREATE SCHEMA test_non_pushdown_queries;
        CREATE FOREIGN TABLE test_non_pushdown_queries.user_connections (user_from int, user_to int) SERVER pg_lake OPTIONS (writable 'true', format 'parquet', location '{url}');

        INSERT INTO test_non_pushdown_queries.user_connections VALUES (1, 2),(1, 3), (2, 4), (3, 4), (4, 5);


    """,
        pg_conn,
    )

    for query in queries:
        print(query)
        results = run_query("EXPLAIN (VERBOSE, ANALYZE) " + query, pg_conn)
        if "Custom Scan (Query Pushdown)" in str(results):
            assert False, "pushdown query that is should not be pushing down" + query
    pg_conn.rollback()
