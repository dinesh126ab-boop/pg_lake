import pytest
from utils_pytest import *

CLICKBENCH_QUERY_COUNT = 43


@pytest.mark.parametrize("table_type", ["pg_lake_iceberg", "pg_lake", "heap"])
def test_clickbench_udfs_on_sample_data(
    pg_conn, test_clickbench_uri, with_default_location, table_type
):
    # try to load data before creating the table
    error = run_command(
        f"select lake_clickbench.load('{test_clickbench_uri}')",
        pg_conn,
        raise_error=False,
    )

    assert (
        'first need to run "lake_clickbench.create()" to set up bench tables' in error
    )

    pg_conn.rollback()

    run_command(
        f"select lake_clickbench.create('{table_type}', '{test_clickbench_uri}')",
        pg_conn,
    )

    # try to create the table again
    error = run_command(
        f"select lake_clickbench.create('{table_type}', '{test_clickbench_uri}')",
        pg_conn,
        raise_error=False,
    )

    assert '"hits_auto" already exists' in error

    pg_conn.rollback()

    run_command(
        f"select lake_clickbench.create('{table_type}', '{test_clickbench_uri}')",
        pg_conn,
    )

    run_command(f"select lake_clickbench.load('{test_clickbench_uri}')", pg_conn)

    total_rows = run_query("select count(*) from hits", pg_conn)[0][0]
    assert total_rows == 100

    # show a query
    first_query = run_query("select lake_clickbench.show_query(1)", pg_conn)[0][0]
    assert first_query == "SELECT COUNT(*) FROM hits;"

    # run the first query only (not meant to run all queries, EXPLAIN ANALYZE all queries below test)
    run_command(
        f"select lake_clickbench.run_query(i) from generate_series(1, {CLICKBENCH_QUERY_COUNT}) i where i = 1",
        pg_conn,
    )

    # try to show invalid query number
    error = run_command(
        f"select lake_clickbench.show_query(0)", pg_conn, raise_error=False
    )

    assert "query id must be between 1 and 43" in error

    pg_conn.rollback()

    # try to run invalid query number
    error = run_command(
        f"select lake_clickbench.run_query(44)", pg_conn, raise_error=False
    )

    assert "query id must be between 1 and 43" in error

    pg_conn.rollback()


def test_clickbench_pushdown(pg_conn, test_clickbench_uri, with_default_location):
    run_command(
        f"select lake_clickbench.create(clickbench_uri => '{test_clickbench_uri}')",
        pg_conn,
    )

    for query_id in range(1, CLICKBENCH_QUERY_COUNT + 1):
        query = run_query(f"select lake_clickbench.show_query({query_id})", pg_conn)[0][
            0
        ]
        explain = f"EXPLAIN (ANALYZE) {query}"
        result = perform_query_on_cursor(explain, pg_conn)
        pg_conn.commit()
        first_line = result[0][0]

        if not (
            "Custom Scan (Query Pushdown)" in str(result)
            or has_foreign_scan_at_top(first_line)
        ):
            assert False, f"failed for query id: {query_id}"

    pg_conn.rollback()


def has_foreign_scan_at_top(explain_output: str) -> bool:

    if explain_output.strip().startswith("Foreign Scan"):
        return True

    return False


@pytest.fixture(scope="module")
def test_clickbench_uri(s3, pg_lake_benchmark_extension):
    hits_key = "sample_data/hits.parquet"
    local_file_path = sampledata_filepath(f"hits.parquet")
    s3.upload_file(local_file_path, TEST_BUCKET, hits_key)

    yield f"s3://{TEST_BUCKET}/{hits_key}"
