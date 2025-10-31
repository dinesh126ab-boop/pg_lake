import pytest
import psycopg2
import io
from utils_pytest import *

simple_file_url = (
    f"s3://{TEST_BUCKET}/test_iceberg_copy_from_with_pushdown/data.parquet"
)
iceberg_location = f"s3://{TEST_BUCKET}/test_iceberg_copy_from_with_pushdown/iceberg/"

# Most tests load 10 rows into simple_table (an Iceberg table) via COPY and then roll back


# Test COPY happy path
def test_pushdown(
    pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location
):
    run_command(
        f"""
        COPY simple_table FROM '{simple_file_url}';
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM simple_table",
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert result[0]["pushed_down"]

    pg_conn.rollback()


# Test COPY happy path with pg_lake_table.target_file_size_mb = -1;
def test_pushdown_no_split(
    pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location
):

    run_command("SET LOCAL pg_lake_table.target_file_size_mb TO -1;", pg_conn)

    run_command(
        f"""
        COPY simple_table FROM '{simple_file_url}';
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM simple_table",
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert result[0]["pushed_down"]

    pg_conn.rollback()


# Test load_from happy path
def test_load_from(
    pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location
):
    run_command(
        f"""
        CREATE TABLE with_pushdown ()
        USING pg_lake_iceberg
        WITH (load_from = '{simple_file_url}');
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM with_pushdown",
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert result[0]["pushed_down"]

    pg_conn.rollback()


# Test column list
def test_columns(
    pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location
):
    run_command(
        f"""
        COPY simple_table (id, val, tags) FROM '{simple_file_url}';
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM simple_table",
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert not result[0]["pushed_down"]

    pg_conn.rollback()


# Test WHERE
def test_where(pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location):
    run_command(
        f"""
        COPY simple_table FROM '{simple_file_url}' WHERE id > 5;
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM simple_table",
        pg_conn,
    )
    assert result[0]["count"] == 5
    assert not result[0]["pushed_down"]

    pg_conn.rollback()


# Test CSV with an array
def test_csv_array(
    pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location
):
    csv_url = f"s3://{TEST_BUCKET}/test_iceberg_copy_from_with_pushdown/data.csv"

    run_command(
        f"""
        COPY (SELECT s id, 'hello' val, array['test',NULL] tags FROM generate_series(1,10) s) TO '{csv_url}' WITH (header);

        CREATE TABLE array_table (id int, val text, tags text[]) USING iceberg WITH (load_from = '{csv_url}');
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM array_table",
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert not result[0]["pushed_down"]

    pg_conn.rollback()


# Test CSV with a composite type
def test_csv_composite(
    pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location
):
    csv_url = f"s3://{TEST_BUCKET}/test_iceberg_copy_from_with_pushdown/data.csv"

    run_command(
        f"""
        CREATE TYPE coords AS (x int, y int);
        COPY (SELECT s id, 'hello' val, (3,4)::coords AS pos FROM generate_series(1,10) s) TO '{csv_url}' WITH (header);

        CREATE TABLE comp_type_table (id int, val text, pos coords) USING iceberg WITH (load_from = '{csv_url}');
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM comp_type_table",
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert not result[0]["pushed_down"]

    pg_conn.rollback()


# Test CSV in the happy path
def test_csv(pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location):
    csv_url = f"s3://{TEST_BUCKET}/test_iceberg_copy_from_with_pushdown/data.csv"

    run_command(
        f"""
        COPY (SELECT s id, 'hello' val FROM generate_series(1,10) s) TO '{csv_url}' WITH (header);

        CREATE TABLE csv_table (id int, val text) USING iceberg WITH (load_from = '{csv_url}');
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM csv_table",
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert result[0]["pushed_down"]

    pg_conn.rollback()


# Test with a constraint
def test_constraint(
    pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location
):
    run_command(
        f"""
        CREATE TABLE constraint_table(
            id int check(id > 0),
            val text,
            tags text[]
        )
        USING iceberg;
        COPY constraint_table FROM '{simple_file_url}';
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM constraint_table",
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert not result[0]["pushed_down"]

    pg_conn.rollback()


# Test with triggers
def test_triggers(
    pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location
):
    run_command(
        f"""
        CREATE TABLE trigger_table(
            id int,
            val text
        )
        USING iceberg;

        CREATE OR REPLACE FUNCTION worldize()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.val := 'world';
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER trigger_after_insert
        BEFORE INSERT ON trigger_table
        FOR EACH ROW EXECUTE FUNCTION worldize();

        COPY trigger_table FROM '{simple_file_url}';
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM trigger_table WHERE val = 'world'",
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert not result[0]["pushed_down"]

    pg_conn.rollback()


# Test with partitioning
def test_partitioning(
    pg_conn, extension, s3, copy_from_pushdown_setup, with_default_location
):
    run_command(
        f"""
        CREATE TABLE partitioned_table (
            id int,
            val text
        )
        PARTITION BY RANGE (id);
        CREATE TABLE child_1 PARTITION OF partitioned_table FOR VALUES FROM (0) TO (50) USING iceberg;
        CREATE TABLE child_2 PARTITION OF partitioned_table FOR VALUES  FROM (50) TO (100) USING iceberg;

        COPY partitioned_table FROM '{simple_file_url}';
    """,
        pg_conn,
    )

    # Copy into partitioned table is not pushed down
    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM partitioned_table",
        pg_conn,
    )
    assert result[0]["count"] == 10
    assert not result[0]["pushed_down"]

    # Copy into partition is also not pushed down, even if it's Iceberg
    run_command(
        f"""
        COPY child_1 FROM '{simple_file_url}';
    """,
        pg_conn,
    )
    result = run_query(
        "SELECT count(*), pg_lake_last_copy_pushed_down_test() pushed_down FROM partitioned_table",
        pg_conn,
    )
    assert result[0]["count"] == 20
    assert not result[0]["pushed_down"]

    pg_conn.rollback()


def test_copy_from_reject_limit(pg_conn, extension, s3, with_default_location):
    if get_pg_version_num(pg_conn) < 180000:
        return

    run_command(
        "create table test_copy_from_reject_limit(a int) using iceberg ;", pg_conn
    )

    copy_command_without_error_ignore = f"COPY test_copy_from_reject_limit FROM STDIN WITH (on_error ignore, reject_limit 3);"

    data = """\
        'a'
        'b'
        'c'
        'd'
        \.
        """

    try:
        with pg_conn.cursor() as cursor:
            cursor.copy_expert(copy_command_without_error_ignore, io.StringIO(data))

        assert False  # We expect an error to be raised
    except psycopg2.DatabaseError as error:
        assert "skipped more" in str(error)

    pg_conn.rollback()


@pytest.fixture(scope="module")
def copy_from_pushdown_setup(superuser_conn):
    run_command(
        f"""
        COPY (SELECT s id, 'hello' val, array['test',NULL] tags FROM generate_series(1,10) s) TO '{simple_file_url}';

        CREATE OR REPLACE FUNCTION pg_lake_last_copy_pushed_down_test()
          RETURNS bool
          LANGUAGE C
        AS 'pg_lake_copy', $function$pg_lake_last_copy_pushed_down_test$function$;

        CREATE TABLE simple_table (
           id int,
           val text,
           tags text[]
        )
        USING pg_lake_iceberg
        WITH (location = '{iceberg_location}');
        GRANT ALL ON simple_table TO public;
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    # Teardown: Drop the functions after the test(s) are done
    run_command(
        f"""
        DROP FUNCTION pg_lake_last_copy_pushed_down_test;
        DROP TABLE simple_table;
    """,
        superuser_conn,
    )
    superuser_conn.commit()
