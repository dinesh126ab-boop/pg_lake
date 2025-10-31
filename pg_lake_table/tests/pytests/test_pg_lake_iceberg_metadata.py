import pytest
import psycopg2
import time
import duckdb
import math
from utils_pytest import *


# make sure we can re-write our own empty metadata
def test_reserialize_pg_lake_iceberg_empty_metadata(
    pg_conn, extension, app_user, s3, create_helper_functions, with_default_location
):
    run_command(
        f"""
        CREATE TABLE test_pg_lake_iceberg_reserialize (a int, b int) USING pg_lake_iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_file_path = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_pg_lake_iceberg_reserialize'",
        pg_conn,
    )[0][0]
    assert_metadata_reserialize_ok(s3, pg_conn, metadata_file_path)

    run_command("DROP TABLE test_pg_lake_iceberg_reserialize", pg_conn)
    pg_conn.commit()


# make sure we can re-write our empty metadata with insert/update/delete
def test_reserialize_pg_lake_iceberg_filled_metadata(
    pg_conn,
    superuser_conn,
    extension,
    app_user,
    s3,
    create_helper_functions,
    with_default_location,
):
    # both positional delete/update
    run_command(
        f"""
        CREATE TABLE test_pg_lake_iceberg_reserialize (a int, b int) USING pg_lake_iceberg;
        INSERT INTO test_pg_lake_iceberg_reserialize SELECT i,i FROM generate_series(0,100)i;
        UPDATE test_pg_lake_iceberg_reserialize SET a = a + 1 WHERE a = 15;
        UPDATE test_pg_lake_iceberg_reserialize SET a = a + 1 WHERE a < 25;

        DELETE FROM test_pg_lake_iceberg_reserialize WHERE a = 85;
        DELETE FROM test_pg_lake_iceberg_reserialize WHERE a > 65;
    """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_file_path = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_pg_lake_iceberg_reserialize'",
        pg_conn,
    )[0][0]
    assert_metadata_reserialize_ok(s3, pg_conn, metadata_file_path)

    # make sure we can do that after VACUUM as well
    run_command_outside_tx(["VACUUM test_pg_lake_iceberg_reserialize"], superuser_conn)

    run_command("DROP TABLE test_pg_lake_iceberg_reserialize", pg_conn)
    pg_conn.commit()


def test_snapshot_operation(
    pg_conn,
    superuser_conn,
    extension,
    app_user,
    s3,
    create_helper_functions,
    with_default_location,
):
    run_command(
        f"""
        CREATE TABLE test_snapshot_operation (a int, b int) USING pg_lake_iceberg;
        INSERT INTO test_snapshot_operation SELECT i, i FROM generate_series(0,100)i;
    """,
        pg_conn,
    )
    pg_conn.commit()

    # insert creates append
    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_snapshot_operation'",
        pg_conn,
    )[0][0]
    current_snapshot = get_current_snapshot(s3, metadata_location)
    assert str(current_snapshot["summary"]) == "{'operation': 'append'}"

    # positional delete creates delete
    run_command("DELETE FROM test_snapshot_operation WHERE a = 74;", pg_conn)
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_snapshot_operation'",
        pg_conn,
    )[0][0]
    current_snapshot = get_current_snapshot(s3, metadata_location)
    assert str(current_snapshot["summary"]) == "{'operation': 'delete'}"

    # copy-on-write delete creates overwrite
    # as it re-writes some files internally
    run_command("DELETE FROM test_snapshot_operation WHERE a > 75;", pg_conn)
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_snapshot_operation'",
        pg_conn,
    )[0][0]
    current_snapshot = get_current_snapshot(s3, metadata_location)
    assert str(current_snapshot["summary"]) == "{'operation': 'overwrite'}"

    # positional update creates overwrite
    run_command("UPDATE test_snapshot_operation SET a = 500 WHERE a = 1;", pg_conn)
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_snapshot_operation'",
        pg_conn,
    )[0][0]
    current_snapshot = get_current_snapshot(s3, metadata_location)
    assert str(current_snapshot["summary"]) == "{'operation': 'overwrite'}"

    # copy on-write update creates overwrite
    run_command("UPDATE test_snapshot_operation SET a = 500 WHERE a < 30;", pg_conn)
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_snapshot_operation'",
        pg_conn,
    )[0][0]
    current_snapshot = get_current_snapshot(s3, metadata_location)
    assert str(current_snapshot["summary"]) == "{'operation': 'overwrite'}"

    # make sure we can do that after VACUUM as well
    run_command_outside_tx(["VACUUM test_snapshot_operation"], superuser_conn)
    superuser_conn.commit()

    # vacuum doesn't change the data
    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_snapshot_operation'",
        pg_conn,
    )[0][0]
    current_snapshot = get_current_snapshot(s3, metadata_location)
    assert str(current_snapshot["summary"]) == "{'operation': 'replace'}"

    # make sure we can do that after VACUUM as well
    run_command("UPDATE test_snapshot_operation SET a = 500 WHERE a < 30;", pg_conn)
    pg_conn.commit()

    run_command("UPDATE test_snapshot_operation SET a = 500 WHERE a = 30;", pg_conn)
    pg_conn.commit()
    run_command_outside_tx(["VACUUM FULL test_snapshot_operation"], superuser_conn)
    superuser_conn.commit()

    # vacuum doesn't change the data
    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_snapshot_operation'",
        pg_conn,
    )[0][0]
    current_snapshot = get_current_snapshot(s3, metadata_location)
    assert str(current_snapshot["summary"]) == "{'operation': 'replace'}"

    run_command("DROP TABLE test_snapshot_operation", pg_conn)
    pg_conn.commit()


def get_current_snapshot(s3, metadata_location):

    metadata_json = read_s3_operations(s3, metadata_location)
    metadata_json = json.loads(metadata_json)
    current_snapshot_id = metadata_json["current-snapshot-id"]
    snapshots = metadata_json["snapshots"]
    current_snapshot = list(
        filter(lambda x: x["snapshot-id"] == current_snapshot_id, snapshots)
    )[0]

    return current_snapshot


def assert_metadata_reserialize_ok(s3, pg_conn, metadata_file_path):
    json_string = read_s3_operations(s3, metadata_file_path)
    metadata_tmpfile = tempfile.NamedTemporaryFile()
    write_json_to_file(metadata_tmpfile.name, json_string)

    command = f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{metadata_file_path}'::text)::json"

    result = run_query(command, pg_conn)
    returned_json = result[0][0]
    assert_valid_json(returned_json)
    original_json = read_json(metadata_tmpfile.name)
    assert_jsons_equivalent(original_json, returned_json)


@pytest.fixture(scope="module")
def create_helper_functions(superuser_conn, app_user):

    run_command(
        f"""
        CREATE OR REPLACE FUNCTION lake_iceberg.reserialize_iceberg_table_metadata(metadataUri TEXT)
        RETURNS text
         LANGUAGE C
         IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$reserialize_iceberg_table_metadata$function$;

        GRANT USAGE ON SCHEMA lake_iceberg TO {app_user};
        GRANT SELECT ON ALL TABLES IN SCHEMA lake_iceberg TO {app_user};
        GRANT EXECUTE ON FUNCTION lake_iceberg.reserialize_iceberg_table_metadata(metadataUri TEXT) TO {app_user};

""",
        superuser_conn,
    )
    superuser_conn.commit()
    yield

    # Teardown: Drop the function after the test(s) are done
    run_command(
        f"""
        DROP FUNCTION lake_iceberg.reserialize_iceberg_table_metadata;
""",
        superuser_conn,
    )
