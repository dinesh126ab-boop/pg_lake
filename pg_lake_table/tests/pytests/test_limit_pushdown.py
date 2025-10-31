import pytest
from utils_pytest import *

queries = [
    "SELECT a FROM table_with_ties ORDER BY a FETCH FIRST 2 ROWS WITH TIES",
    "SELECT a FROM table_with_ties ORDER BY a FETCH FIRST 1 ROW WITH TIES",
    "SELECT a FROM table_with_ties FETCH FIRST 2 ROWS ONLY",
    "SELECT a FROM table_with_ties FETCH FIRST 1 ROW ONLY",
]


def test_with_ties(pg_conn, s3, extension):
    url_nulls = f"s3://{TEST_BUCKET}/test_with_ties/table_with_ties/"

    run_command(
        f"""
                CREATE TABLE table_with_ties(a int);
                CREATE FOREIGN TABLE table_with_ties_f (a int) SERVER pg_lake OPTIONS (format 'parquet', writable 'true', location '{url_nulls}');
                INSERT INTO table_with_ties VALUES (-1), (-1), (-1), (NULL), (NULL), (NULL), (1), (1), (1);
                INSERT INTO table_with_ties_f SELECT * FROM table_with_ties;
                """,
        pg_conn,
    )

    for execution in ["pushdown", "fdw"]:

        if execution == "pushdown":
            run_command(
                "SET pg_lake_table.enable_full_query_pushdown TO true;",
                pg_conn,
            )
        elif execution == "fdw":
            run_command(
                "SET pg_lake_table.enable_full_query_pushdown TO false;",
                pg_conn,
            )

        for query in queries:

            # here, we explicitly not use assert_query_results_on_tables
            # because that function sorts the return values, we want to
            # test ordering done by the database in this test
            heap_query_result = perform_query_on_cursor(query, pg_conn)

            fdw_query = query.replace("table_with_ties", "table_with_ties_f")
            fdw_query_result = perform_query_on_cursor(fdw_query, pg_conn)
            assert heap_query_result == fdw_query_result

    run_command("RESET pg_lake_table.enable_full_query_pushdown;", pg_conn)
    pg_conn.rollback()
