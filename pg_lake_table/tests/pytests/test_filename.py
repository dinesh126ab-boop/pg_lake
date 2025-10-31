import pytest

from utils_pytest import *


def test_create_table_with_filename(s3, pg_conn, extension, with_default_location):
    url = f"s3://{TEST_BUCKET}/test_create_table_with_filename/data.parquet"

    run_command(
        f"""
        COPY (SELECT s AS id, s::text AS desc FROM generate_series(1,5) s) TO '{url}';
    """,
        pg_conn,
    )

    run_command(
        f"""
    CREATE FOREIGN TABLE test_with_filename () SERVER pg_lake OPTIONS (path '{url}', filename 'true');
    CREATE TABLE test_with_filename_iceberg USING iceberg AS SELECT * FROM test_with_filename;
    """,
        pg_conn,
    )

    # test that star-select returns a filename
    res = run_query(f"SELECT * FROM test_with_filename LIMIT 1", pg_conn)

    assert res[0]["_filename"] == url, "select star with filename"

    # test that explicit field returns a filename
    res = run_query(f"SELECT _filename FROM test_with_filename LIMIT 1", pg_conn)

    assert res[0]["_filename"] == url, "select explicit filename field"

    # should also have been copied to iceberg
    res = run_query(
        f"SELECT _filename FROM test_with_filename_iceberg LIMIT 1", pg_conn
    )

    assert res[0]["_filename"] == url, "select explicit filename field"

    pg_conn.rollback()


def test_create_table_with_filename_multifile(s3, pg_conn, extension):
    url = f"s3://{TEST_BUCKET}/test_create_table_with_filename_multifile/"

    run_command(
        f"""
        COPY (SELECT 1 AS id, 'a'::text AS desc) TO '{url}/data1.parquet';
        COPY (SELECT 2 AS id, 'b'::text AS desc) TO '{url}/data2.parquet';
    """,
        pg_conn,
    )

    run_command(
        f"""
    CREATE FOREIGN TABLE test_with_filename () SERVER pg_lake OPTIONS (path '{url}/*.parquet', filename 'true');
    """,
        pg_conn,
    )

    res = run_query(f"SELECT id, _filename FROM test_with_filename ORDER BY 1", pg_conn)

    assert res == [
        [1, f"{url}/data1.parquet"],
        [2, f"{url}/data2.parquet"],
    ], "got correlated filenames"

    pg_conn.rollback()


def test_create_table_with_filename_hive_partitioning(s3, pg_conn, extension):
    url = f"s3://{TEST_BUCKET}/test_create_table_with_filename_hive/"

    for file_format in ["parquet", "csv", "json"]:
        copy_options = ""

        if file_format == "csv":
            copy_options = "with header"

        run_command(
            f"""
            COPY (SELECT 1 AS id, 'a'::text AS desc) TO '{url}/year=2025/month=1/data1.{file_format}' {copy_options};
            COPY (SELECT 2 AS id, 'b'::text AS desc) TO '{url}/year=2025/month=2/data2.{file_format}' {copy_options};
        """,
            pg_conn,
        )

        run_command(
            f"""
        CREATE FOREIGN TABLE test_with_filename () SERVER pg_lake OPTIONS (path '{url}/**/*.{file_format}', filename 'true');
        """,
            pg_conn,
        )

        # Check that we can query a table with filename and hive partitioning
        res = run_query(
            f"SELECT id, _filename FROM test_with_filename WHERE month = 2", pg_conn
        )

        assert res[0] == [2, f"{url}/year=2025/month=2/data2.{file_format}"]

        # Check empty result by pruning non-existent filename
        res = run_query(
            f"SELECT id, _filename FROM test_with_filename WHERE _filename = ''",
            pg_conn,
        )

        assert len(res) == 0

        pg_conn.rollback()


def test_create_table_with_explicit_fields(s3, pg_conn, extension):
    url = f"s3://{TEST_BUCKET}/test_create_table_with_explicit_fields/data.json"

    run_command(
        f"""
        COPY (SELECT s AS id, s::text AS desc FROM generate_series(1,5) s) TO '{url}';
    """,
        pg_conn,
    )

    error = run_command(
        f"""
    CREATE FOREIGN TABLE test_explicit_name_1 (id bigint, "desc" text) SERVER pg_lake OPTIONS (path '{url}', filename 'true');
    """,
        pg_conn,
        raise_error=False,
    )
    assert "no _filename column found" in error

    pg_conn.rollback()

    error = run_command(
        f"""
    CREATE FOREIGN TABLE test_explicit_name_3 (id bigint, "desc" text, _filename int) SERVER pg_lake OPTIONS (path '{url}', filename 'true');
    """,
        pg_conn,
        raise_error=False,
    )
    assert "_filename column must have type text" in error

    pg_conn.rollback()

    run_command(
        f"""
    CREATE FOREIGN TABLE test_explicit_name (id bigint, "desc" text, _filename text) SERVER pg_lake OPTIONS (path '{url}', filename 'true');
    """,
        pg_conn,
    )

    # test that star-select returns a filename
    res = run_query(f"SELECT * FROM test_explicit_name LIMIT 1", pg_conn)
    assert res[0]["_filename"] == url, "select star with filename"

    pg_conn.rollback()


def test_load_from_with_filename(s3, pg_conn, extension, with_default_location):
    url = f"s3://{TEST_BUCKET}/test_create_table_load_from_with_filename/data.csv"

    run_command(
        f"""
        COPY (SELECT s AS id, s::text AS desc FROM generate_series(1,5) s) TO '{url}';
    """,
        pg_conn,
    )

    # should be able to load_from with a filename
    run_command(
        f"""
    CREATE TABLE test_load_from_with_filename ()
    WITH (load_from = '{url}', filename = 'true');
    """,
        pg_conn,
    )

    res = run_query(f"SELECT * FROM test_load_from_with_filename LIMIT 1", pg_conn)
    assert res[0]["_filename"] == url, "select star with filename"

    # Iceberg is also allowed
    error = run_command(
        f"""
    CREATE TABLE test_load_from_with_filename_iceberg ()
    USING iceberg
    WITH (load_from = '{url}', filename = 'true');
    """,
        pg_conn,
        raise_error=False,
    )

    res = run_query(
        f"SELECT * FROM test_load_from_with_filename_iceberg LIMIT 1", pg_conn
    )
    assert res[0]["_filename"] == url, "select star with filename"

    pg_conn.rollback()


def test_copy_with_filename(s3, pg_conn, extension):
    url = f"s3://{TEST_BUCKET}/test_copy_with_filename/data.csv"

    run_command(
        f"""
        COPY (SELECT s AS id, s::text AS desc FROM generate_series(1,5) s) TO '{url}';
    """,
        pg_conn,
    )

    # mix things up a bit by reading the CSV with GDAL
    run_command(
        f"""
    CREATE TABLE test_copy_with_filename ()
    WITH (definition_from = '{url}', filename = 'true', format = 'gdal');
    COPY test_copy_with_filename FROM '{url}' WITH (format 'gdal', filename 'true');
    """,
        pg_conn,
    )

    res = run_query(f"SELECT * FROM test_copy_with_filename LIMIT 1", pg_conn)
    assert res[0]["_filename"] == url, "select star with filename"

    pg_conn.rollback()


def test_filename_pruning(s3, pg_conn, extension):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "
    url_prefix = f"s3://{TEST_BUCKET}/test_filename_pruning"

    run_command(
        f"""
        COPY (SELECT s AS id, s::text AS desc FROM generate_series(1,5) s) TO '{url_prefix}/1.parquet';
        COPY (SELECT s AS id, s::text AS desc FROM generate_series(6,10) s) TO '{url_prefix}/2.parquet';
        COPY (SELECT s AS id, s::text AS desc FROM generate_series(11,15) s) TO '{url_prefix}/3.parquet';
        COPY (SELECT s AS id, s::text AS desc FROM generate_series(101,105) s) TO '{url_prefix}/other/1.parquet';
    """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE FOREIGN TABLE test_filename_pruning ()
        SERVER pg_lake
        OPTIONS (path '{url_prefix}/*.parquet', filename 'true');
    """,
        pg_conn,
    )

    # Prune to a single file
    result = run_query(
        f"""
        {explain_prefix} SELECT * FROM test_filename_pruning WHERE _filename = '{url_prefix}/1.parquet'
    """,
        pg_conn,
    )
    assert fetch_data_files_used(result) == "1"

    result = run_query(
        f"""
        SELECT count(*), min(id), max(id) FROM test_filename_pruning WHERE _filename = '{url_prefix}/1.parquet'
    """,
        pg_conn,
    )
    assert result[0]["count"] == 5
    assert result[0]["min"] == 1
    assert result[0]["max"] == 5

    # Prune to two files
    result = run_query(
        f"""
        {explain_prefix} SELECT * FROM test_filename_pruning WHERE _filename in ('{url_prefix}/1.parquet', '{url_prefix}/3.parquet')
    """,
        pg_conn,
    )
    assert fetch_data_files_used(result) == "2"

    result = run_query(
        f"""
        SELECT count(*), min(id), max(id) FROM test_filename_pruning WHERE _filename in ('{url_prefix}/1.parquet', '{url_prefix}/3.parquet')
    """,
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert result[0]["min"] == 1
    assert result[0]["max"] == 15

    # Prune to not a file
    result = run_query(
        f"""
        {explain_prefix} SELECT * FROM test_filename_pruning WHERE _filename <> '{url_prefix}/1.parquet'
    """,
        pg_conn,
    )
    assert fetch_data_files_used(result) == "2"

    result = run_query(
        f"""
        SELECT count(*), min(id), max(id) FROM test_filename_pruning WHERE _filename <> '{url_prefix}/1.parquet'
    """,
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert result[0]["min"] == 6
    assert result[0]["max"] == 15

    # Prune greater than
    result = run_query(
        f"""
        {explain_prefix} SELECT * FROM test_filename_pruning WHERE _filename > '{url_prefix}/1.parquet'
    """,
        pg_conn,
    )
    assert fetch_data_files_used(result) == "2"

    result = run_query(
        f"""
        SELECT count(*), min(id), max(id) FROM test_filename_pruning WHERE _filename > '{url_prefix}/1.parquet'
    """,
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert result[0]["min"] == 6
    assert result[0]["max"] == 15

    # Prune to other file (no results, we don't read files that are not part of the table)
    result = run_query(
        f"""
        {explain_prefix} SELECT * FROM test_filename_pruning WHERE _filename = '{url_prefix}/other/1.parquet'
    """,
        pg_conn,
    )
    assert fetch_data_files_used(result) is "0"

    result = run_query(
        f"""
        SELECT count(*) FROM test_filename_pruning WHERE _filename = '{url_prefix}/other/1.parquet'
    """,
        pg_conn,
    )
    assert result[0]["count"] == 0

    # Prune using large batch that includes 2 of the files
    large_url_batch = ",".join(f"'{url_prefix}/{i}.parquet'" for i in range(2, 151))

    result = run_query(
        f"""
        {explain_prefix} SELECT * FROM test_filename_pruning WHERE _filename = any(array[{large_url_batch}])
    """,
        pg_conn,
    )
    assert fetch_data_files_used(result) == "2"

    # non-equality falls back to predicate_refuted_by, which does not prune
    result = run_query(
        f"""
        {explain_prefix} SELECT * FROM test_filename_pruning WHERE _filename >= any(array[{large_url_batch}])
    """,
        pg_conn,
    )
    assert fetch_data_files_used(result) == "3"

    result = run_query(
        f"""
        {explain_prefix} SELECT * FROM test_filename_pruning WHERE _filename = all(array[{large_url_batch}])
    """,
        pg_conn,
    )
    assert fetch_data_files_used(result) == "3"

    # IN is internally the same as = ANY(...), add a NULL to mix it up
    result = run_query(
        f"""
        {explain_prefix} SELECT * FROM test_filename_pruning WHERE _filename in ({large_url_batch}, NULL)
    """,
        pg_conn,
    )
    assert fetch_data_files_used(result) == "2"

    result = run_query(
        f"""
        SELECT count(*) FROM test_filename_pruning WHERE _filename in ({large_url_batch})
    """,
        pg_conn,
    )
    assert result[0]["count"] == 10

    pg_conn.rollback()
