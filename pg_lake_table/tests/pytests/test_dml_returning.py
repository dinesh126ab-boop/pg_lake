import pytest
from utils_pytest import *


def test_dml_returning_new_old(s3, pg_conn, extension, with_default_location):
    # insert returning new and old is introduced at PG 18
    if get_pg_version_num(pg_conn) < 180000:
        return

    run_command(
        f"CREATE TABLE test_dml_returning_new_old(a int, b text) USING iceberg",
        pg_conn,
    )

    # insert returning new
    res = run_query(
        "insert into test_dml_returning_new_old values (1, 'one'), (2, 'two'), (null, null) returning new.*;",
        pg_conn,
    )
    assert res == [
        [1, "one"],
        [2, "two"],
        [None, None],
    ]

    # insert returning old
    res = run_query(
        "insert into test_dml_returning_new_old values (1, 'one'), (2, 'two'), (null, null) returning old.*;",
        pg_conn,
    )
    assert res == [
        [None, None],
        [None, None],
        [None, None],
    ]

    # insert returning new and old
    res = run_query(
        "insert into test_dml_returning_new_old values (1, 'one'), (2, 'two'), (null, null) returning old.*, new.*, a, b;",
        pg_conn,
    )
    assert res == [
        [None, None, 1, "one", 1, "one"],
        [None, None, 2, "two", 2, "two"],
        [None, None, None, None, None, None],
    ]

    # update returning new
    res = run_query(
        "update test_dml_returning_new_old set a = 0, b = 'hey' where a = 1 returning new.*;",
        pg_conn,
    )
    assert res == [[0, "hey"], [0, "hey"], [0, "hey"]]

    # update returning new and old
    res = run_query(
        "update test_dml_returning_new_old set a = -1, b = 'heyyo' where a = 0 returning old.*, new.*, a, b;",
        pg_conn,
    )
    assert res == [
        [0, "hey", -1, "heyyo", -1, "heyyo"],
        [0, "hey", -1, "heyyo", -1, "heyyo"],
        [0, "hey", -1, "heyyo", -1, "heyyo"],
    ]

    # delete returning new
    res = run_query(
        "delete from test_dml_returning_new_old where a = -1 returning new.*;",
        pg_conn,
    )
    assert res == [[None, None], [None, None], [None, None]]

    # delete returning old
    res = run_query(
        "delete from test_dml_returning_new_old where a = 2 returning old.*;",
        pg_conn,
    )
    assert res == [[2, "two"], [2, "two"], [2, "two"]]

    run_command(
        "SET pg_lake_table.enable_full_query_pushdown TO false;",
        pg_conn,
    )

    # delete returning old and new
    res = run_query(
        "delete from test_dml_returning_new_old where a is null  returning old.*, new.*, a, b;",
        pg_conn,
    )
    assert res == [
        [None, None, None, None, None, None],
        [None, None, None, None, None, None],
        [None, None, None, None, None, None],
    ]

    pg_conn.rollback()
