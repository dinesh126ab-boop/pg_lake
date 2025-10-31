import pytest
import psycopg2
from utils_pytest import *


@pytest.fixture(scope="module")
def create_writable_tables(pg_conn, s3, extension):
    url_test_table = f"s3://{TEST_BUCKET}/test_writable_insert/test_table/"
    url_join_table = f"s3://{TEST_BUCKET}/test_writable_insert/join_table/"

    iceberg_url_test_table = (
        f"s3://{TEST_BUCKET}/test_writable_insert/iceberg_url_test_table/"
    )
    iceberg_url_join_table = (
        f"s3://{TEST_BUCKET}/test_writable_insert/iceberg_url_join_table/"
    )

    run_command(
        f"""
        CREATE SCHEMA test_writable_table_insert;
        SET search_path TO test_writable_table_insert;
        CREATE FOREIGN TABLE test_table (
            id SERIAL,
            name VARCHAR(50),
            age INTEGER,
            status TEXT
        )
        SERVER pg_lake
        OPTIONS (writable 'true', location '{url_test_table}', format 'parquet');

        INSERT INTO test_table (name, age) VALUES
        ('Alice', 30),
        ('Bob', 25),
        ('Charlie', 35),
        ('David', 40),
        ('Egbert', 55),
        ('Fred', NULL);

        CREATE FOREIGN TABLE join_table (
            user_id INTEGER,
            new_age INTEGER
        )
        SERVER pg_lake
        OPTIONS (writable 'true', location '{url_join_table}', format 'parquet');

        INSERT INTO join_table (user_id, new_age) VALUES
        (1, 32),
        (3, 37);

        CREATE TABLE test_table_heap
        AS SELECT * FROM test_table;

        CREATE TABLE join_table_heap
        AS SELECT * FROM join_table;

        CREATE TABLE test_table_heap_for_iceberg
        AS SELECT * FROM test_table;

        CREATE TABLE join_table_heap_for_iceberg
        AS SELECT * FROM join_table;

        CREATE TABLE test_table_iceberg USING iceberg WITH (location = '{iceberg_url_test_table}')
        AS SELECT * FROM test_table WITH NO DATA;
        INSERT INTO test_table_iceberg SELECT * FROM test_table;

        CREATE TABLE join_table_iceberg USING iceberg WITH (location = '{iceberg_url_join_table}')
        AS SELECT * FROM join_table WITH NO DATA;
        INSERT INTO join_table_iceberg SELECT * FROM join_table;

        CREATE TABLE even_ids
        AS SELECT s * 1 AS id FROM generate_series(1,10) s;

        CREATE VIEW test_view AS SELECT * FROM test_table WHERE age >= 40;
        CREATE VIEW test_view_iceberg AS SELECT * FROM test_table_iceberg WHERE age >= 40;
        CREATE VIEW test_view_heap AS SELECT * FROM test_table_heap WHERE age >= 40;
        CREATE VIEW test_view_heap_for_iceberg AS SELECT * FROM test_table_heap_for_iceberg WHERE age >= 40;
    """,
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command(
        "DROP FOREIGN TABLE test_table, join_table, test_table_iceberg, join_table_iceberg CASCADE;",
        pg_conn,
    )
    run_command(
        "DROP TABLE test_table_heap, join_table_heap, test_table_heap_for_iceberg, join_table_heap_for_iceberg CASCADE;",
        pg_conn,
    )
    pg_conn.commit()


SYSTEM_COLUMNS = ["ctid", "xmin", "cmin", "xmax", "cmax"]


@pytest.mark.parametrize("system_column", SYSTEM_COLUMNS)
def test_insert_returning_forbidden_system(
    create_writable_tables, pg_conn, system_column
):
    with pytest.raises(Exception) as exc_info:
        run_query(
            f"INSERT INTO test_table DEFAULT VALUES returning ({system_column})",
            pg_conn,
        )
    pg_conn.rollback()
    assert (
        "System column" in str(exc_info.value)
        and f"{system_column}" in str(exc_info.value)
        and "is not supported for pg_lake table" in str(exc_info.value)
    )
