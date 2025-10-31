import os
import pytest
import re
from collections import namedtuple
from utils_pytest import *


def test_iceberg_not_allowed_prepared_tx(
    pg_conn, s3, app_user, extension, with_default_location
):

    error = run_command(
        f"""
        CREATE SCHEMA test_iceberg_not_allowed_prepared_tx;

        CREATE TABLE test_iceberg_not_allowed_prepared_tx.t1 (
            id int not null,
            value text
        )
        USING pg_lake_iceberg;

        PREPARE TRANSACTION 'p1';
    """,
        pg_conn,
        raise_error=False,
    )

    assert "cannot prepare a transaction that has Iceberg" in error

    pg_conn.rollback()


# show that when there is no tx, update/delete on iceberg
# works fine when we access temporary table/view/seq
def test_tmp_object_no_tx(s3, app_user, extension):
    # we use our own connection because we'd like to
    # terminate it and get rid of all temporary objects
    # created for this test
    conn = open_pg_conn(app_user)

    run_command(
        f"""SET pg_lake_iceberg.default_location_prefix = 's3://{TEST_BUCKET}/test_non_tmp_object_prepared_tx';""",
        conn,
    )

    run_command(
        f"""
        CREATE SCHEMA test_tmp_object_no_tx_nsp;
        CREATE TABLE test_tmp_object_no_tx_nsp.t1 (
            id int not null,
            value text
        )
        USING pg_lake_iceberg;

        INSERT INTO test_tmp_object_no_tx_nsp.t1 VALUES (1, 'hello');

        CREATE TEMPORARY TABLE tmp_table AS SELECT id FROM generate_series(0, 10)id;

        CREATE TABLE tmp_view_base AS SELECT id FROM generate_series(0, 10)id;
        CREATE TEMPORARY VIEW tmp_view AS SELECT * FROM tmp_view_base;

        CREATE TEMPORARY SEQUENCE tmp_seq;
    """,
        conn,
    )

    # select on tmp_table
    res = run_query(
        "SELECT count(*) FROM test_tmp_object_no_tx_nsp.t1 JOIN tmp_table USING (id)",
        conn,
    )
    assert res[0][0] == 1

    # select on tmp_view
    res = run_query(
        "SELECT count(*) FROM test_tmp_object_no_tx_nsp.t1 JOIN tmp_view USING (id)",
        conn,
    )
    assert res[0][0] == 1

    # select on tmp_seq
    res = run_query(
        "SELECT count(*) FROM test_tmp_object_no_tx_nsp.t1 JOIN (SELECT nextval('tmp_seq') as id) USING (id)",
        conn,
    )
    assert res[0][0] == 1

    # update on tmp_table
    res = run_query(
        "UPDATE test_tmp_object_no_tx_nsp.t1 t0 SET value = 'new val' FROM tmp_table t1 WHERE t0.id = t1.id RETURNING value",
        conn,
    )
    assert res[0][0] == "new val"

    # update on tmp_view
    res = run_query(
        "UPDATE test_tmp_object_no_tx_nsp.t1 t0 SET value = 'new val view', id = 2 FROM tmp_table t1 WHERE t0.id = t1.id RETURNING value",
        conn,
    )
    assert res[0][0] == "new val view"

    # update on seq
    res = run_query(
        "UPDATE test_tmp_object_no_tx_nsp.t1 t0 SET value = 'new val seq' FROM (SELECT nextval('tmp_seq') as id) t1 WHERE t0.id = t1.id RETURNING value",
        conn,
    )
    assert res[0][0] == "new val seq"

    conn.rollback()
    conn.close()


# show that when there is a tx but not a prepared tx, update/delete on iceberg
# works fine when we access temporary table/view/seq
def test_tmp_object_tx_no_prepared(s3, app_user, extension):
    # we use our own connection because we'd like to
    # terminate it and get rid of all temporary objects
    # created for this test
    conn = open_pg_conn(app_user)

    run_command(
        f"""SET pg_lake_iceberg.default_location_prefix = 's3://{TEST_BUCKET}/test_non_tmp_object_prepared_tx';""",
        conn,
    )

    run_command(
        f"""
        CREATE SCHEMA test_tmp_object_no_tx_nsp;
        CREATE TABLE test_tmp_object_no_tx_nsp.t1 (
            id int not null,
            value text
        )
        USING pg_lake_iceberg;

        INSERT INTO test_tmp_object_no_tx_nsp.t1 VALUES (1, 'hello');

        CREATE TEMPORARY TABLE tmp_table AS SELECT id FROM generate_series(0, 10)id;

        CREATE TABLE tmp_view_base AS SELECT id FROM generate_series(0, 10)id;
        CREATE TEMPORARY VIEW tmp_view AS SELECT * FROM tmp_view_base;

        CREATE TEMPORARY SEQUENCE tmp_seq;
    """,
        conn,
    )

    # select on tmp_table
    run_command("BEGIN", conn)
    res = run_query(
        "SELECT count(*) FROM test_tmp_object_no_tx_nsp.t1 JOIN tmp_table USING (id);",
        conn,
    )
    assert res[0][0] == 1
    run_command("COMMIT", conn)

    # select on tmp_view
    run_command("BEGIN", conn)
    res = run_query(
        "SELECT count(*) FROM test_tmp_object_no_tx_nsp.t1 JOIN tmp_view USING (id);",
        conn,
    )
    assert res[0][0] == 1
    run_command("COMMIT", conn)

    # select on tmp_seq
    run_command("BEGIN", conn)
    res = run_query(
        "SELECT count(*) FROM test_tmp_object_no_tx_nsp.t1 JOIN (SELECT nextval('tmp_seq') as id) USING (id);",
        conn,
    )
    assert res[0][0] == 1
    run_command("COMMIT", conn)

    # update on tmp_table
    run_command("BEGIN", conn)
    res = run_query(
        "UPDATE test_tmp_object_no_tx_nsp.t1 t0 SET value = 'new val' FROM tmp_table t1 WHERE t0.id = t1.id RETURNING value;",
        conn,
    )
    assert res[0][0] == "new val"
    run_command("COMMIT", conn)

    # update on tmp_view
    run_command("BEGIN", conn)
    res = run_query(
        "UPDATE test_tmp_object_no_tx_nsp.t1 t0 SET value = 'new val view', id = 2 FROM tmp_table t1 WHERE t0.id = t1.id RETURNING value;",
        conn,
    )
    assert res[0][0] == "new val view"
    run_command("COMMIT", conn)

    # update on seq
    run_command("BEGIN", conn)
    res = run_query(
        "UPDATE test_tmp_object_no_tx_nsp.t1 t0 SET value = 'new val seq' FROM (SELECT nextval('tmp_seq') as id) t1 WHERE t0.id = t1.id RETURNING value;",
        conn,
    )
    assert res[0][0] == "new val seq"
    run_command("COMMIT", conn)

    run_command("DROP SCHEMA test_tmp_object_no_tx_nsp cascade", conn)

    conn.close()
