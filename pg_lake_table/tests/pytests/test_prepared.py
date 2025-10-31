import pytest
import psycopg2
import duckdb
from utils_pytest import *


def test_prepared(pg_conn, pgduck_conn, extension, s3, request):
    table_name = request.node.name
    url = f"s3://{TEST_BUCKET}/{table_name}/data.parquet"

    # Use PGDuck to generate a file with an array (not yet possible using PG)
    run_command(
        f"""
        COPY (SELECT s, [1,s] a, 'hello-'||s h, [[0,s],[1,s]] md FROM (select generate_series s from generate_series(1,10))) TO '{url}';
    """,
        pgduck_conn,
    )

    # Set up the foreign table
    run_command(
        f"""
        CREATE FOREIGN TABLE {table_name} () SERVER pg_lake OPTIONS (path '{url}');
    """,
        pg_conn,
    )

    for execution_type in ["pushdown", "vectorized"]:

        if execution_type == "pushdown":
            run_command(
                "SET pg_lake_table.enable_full_query_pushdown TO true;",
                pg_conn,
            )
        else:
            run_command(
                "SET pg_lake_table.enable_full_query_pushdown TO false;",
                pg_conn,
            )

        # Int parameter
        run_command(
            f"PREPARE int_lookup(int) AS SELECT s FROM {table_name} WHERE s = $1 ORDER BY h",
            pg_conn,
        )

        # Try with NULL (not yet using generic plan)
        results = run_query(f"EXECUTE int_lookup(NULL)", pg_conn)
        assert len(results) == 0

        for s in range(1, 7):
            results = run_query(f"EXECUTE int_lookup({s})", pg_conn)
            assert results[0]["s"] == s

        # Repeat with NULL (using generic plan)
        results = run_query(f"EXECUTE int_lookup(NULL)", pg_conn)
        assert len(results) == 0

        if execution_type == "pushdown":
            results = run_query(
                "EXPLAIN (VERBOSE, format 'json') EXECUTE int_lookup(1)", pg_conn
            )
            assert "Query Pushdown" in str(results), str(results[0])
        else:
            results = run_query(
                "EXPLAIN (VERBOSE, format 'json') EXECUTE int_lookup(1)", pg_conn
            )
            assert "Vectorized" in str(results[0]), str(results[0])

        run_command("DEALLOCATE int_lookup", pg_conn)
        run_command("RESET pg_lake_table.enable_full_query_pushdown;", pg_conn)

    for execution_type in ["pushdown", "vectorized"]:

        if execution_type == "pushdown":
            run_command(
                "SET pg_lake_table.enable_full_query_pushdown TO true;",
                pg_conn,
            )
        else:
            run_command(
                "SET pg_lake_table.enable_full_query_pushdown TO false;",
                pg_conn,
            )

        # Text parameter
        run_command(
            f"PREPARE text_lookup(text) AS SELECT s FROM {table_name} WHERE h = $1 ORDER BY s",
            pg_conn,
        )

        # make sure we are in correct mode
        if execution_type == "pushdown":
            results = run_query(
                "EXPLAIN (VERBOSE, format 'json') EXECUTE text_lookup('t')", pg_conn
            )
            assert "Query Pushdown" in str(results), str(results[0])
        else:
            results = run_query(
                "EXPLAIN (VERBOSE, format 'json') EXECUTE text_lookup('t')", pg_conn
            )
            assert "Vectorized" in str(results[0]), str(results[0])

        for s in range(1, 7):
            results = run_query(f"EXECUTE text_lookup('hello-'||{s})", pg_conn)
            assert results[0]["s"] == s

        run_command("DEALLOCATE text_lookup", pg_conn)
        run_command("RESET pg_lake_table.enable_full_query_pushdown;", pg_conn)

    # Array parameter
    run_command(
        f"PREPARE arr_lookup(bigint[]) AS SELECT s FROM {table_name} WHERE a = $1",
        pg_conn,
    )

    for s in range(1, 7):
        results = run_query(f"EXECUTE arr_lookup(array[1,{s}])", pg_conn)
        assert results[0]["s"] == s

    # Array parameter with ANY
    run_command(
        f"PREPARE any_lookup(int[]) AS SELECT s FROM {table_name} WHERE s = any($1) ORDER BY s",
        pg_conn,
    )

    for s in range(1, 7):
        results = run_query(f"EXECUTE any_lookup(array[0,{s}])", pg_conn)
        assert results[0]["s"] == s

    # Int parameter with IN
    run_command(
        f"PREPARE in_lookup(int,int) AS SELECT s FROM {table_name} WHERE s IN ($1, $2) ORDER BY s",
        pg_conn,
    )

    for s in range(1, 7):
        results = run_query(f"EXECUTE in_lookup(0,{s})", pg_conn)
        assert results[0]["s"] == s

    # Text array parameter
    run_command(
        f"PREPARE text_any_lookup(text[]) AS SELECT s FROM {table_name} WHERE h = any($1)",
        pg_conn,
    )

    for s in range(1, 7):
        results = run_query(f"EXECUTE text_any_lookup('{{[,],hello-{s}}}')", pg_conn)
        assert results[0]["s"] == s

    # Multi-dimensional array parameter
    #
    # This currently errors, which is a bug that happens because postgres
    # use array types for both single- and multi-dimensional arrays and
    # casting a serialized multi-dimensional array parameter to a regular
    # array type fails on the DuckDB side.
    run_command(
        f"PREPARE md_lookup(bigint[][]) AS SELECT s FROM {table_name} WHERE md = $1 ORDER BY s",
        pg_conn,
    )

    error = run_query(
        f"EXECUTE md_lookup(array[array[0,1],array[1,1]])", pg_conn, raise_error=False
    )
    assert "Could not convert string" in error

    pg_conn.rollback()


def test_now_executed(pg_conn, pgduck_conn, extension, s3, request):

    for execution_type in ["pushdown", "vectorized"]:
        table_name = "now_prepared"
        url = f"s3://{TEST_BUCKET}/{table_name}/{execution_type}/data.parquet"

        run_command("BEGIN;", pg_conn)

        # now is a stable function, so should always give the same value
        # inside a transaction
        run_command(
            f"""
        COPY (SELECT now()::timestamp from generate_series(1,10)) TO '{url}';
        """,
            pg_conn,
        )

        # Set up the foreign table
        run_command(
            f"""
            CREATE FOREIGN TABLE {table_name} (col timestamp) SERVER pg_lake OPTIONS (path '{url}');
        """,
            pg_conn,
        )

        if execution_type == "pushdown":
            run_command(
                "SET LOCAL pg_lake_table.enable_full_query_pushdown TO true;",
                pg_conn,
            )
        else:
            run_command(
                "SET LOCAL pg_lake_table.enable_full_query_pushdown TO false;",
                pg_conn,
            )

        # run few times inside a tx, should give the same result
        for s in range(1, 5):
            results = run_query(
                f"SELECT count(*) FROM {table_name} WHERE col = now()::timestamp",
                pg_conn,
            )
            print(str(results))
            assert results[0][0] == 10

        run_command("ROLLBACK;", pg_conn)


def test_dynamic_parameters(pg_conn, with_default_location, extension, s3, request):

    for execution_type in ["pushdown", "vectorized"]:
        table_name = "test_dynamic_parameters"
        url = f"s3://{TEST_BUCKET}/{table_name}/{execution_type}/data.parquet"

        run_command("BEGIN;", pg_conn)

        # Set up the foreign table
        run_command(
            f"""
                        create table Room (
                            roomno   char(8),
                            comment  text
                        ) USING iceberg;

                        create table WSlot (
                            slotname     char(20),
                            roomno    char(8),
                            slotlink      char(20),
                            backlink        char(20)
                        ) USING iceberg;

                        create function tg_wslot_biu() returns trigger as $$
                        begin
                            if count(*) = 0 from Room where roomno = new.roomno then
                                raise exception 'Room % does not exist', new.roomno;
                            end if;
                            return new;
                        end;
                        $$ language plpgsql;

                        create trigger tg_wslot_biu before insert or update
                            on WSlot for each row execute procedure tg_wslot_biu();

        """,
            pg_conn,
        )

        if execution_type == "pushdown":
            run_command(
                "SET LOCAL pg_lake_table.enable_full_query_pushdown TO true;",
                pg_conn,
            )
        else:
            run_command(
                "SET LOCAL pg_lake_table.enable_full_query_pushdown TO false;",
                pg_conn,
            )

        # run few times inside a tx, should give the same result
        results = run_query(
            "insert into WSlot values ('WS.001.1a', '001', '', '');",
            pg_conn,
            raise_error=False,
        )

        assert "Room 001      does not exist" in str(results)

        run_command("ROLLBACK;", pg_conn)


def test_unused_parameters(pg_conn, with_default_location, extension, s3, request):

    for execution_type in ["pushdown", "vectorized"]:
        table_name = "test_dynamic_parameters"
        url = f"s3://{TEST_BUCKET}/{table_name}/{execution_type}/data.parquet"

        run_command("BEGIN;", pg_conn)

        # Set up the foreign table
        run_command(
            f"""

            CREATE TABLE prep_test (A int, b int) USING iceberg;
            INSERT INTO prep_test VALUES (1,1);

            PREPARE p1 (int, int) AS SELECT * FROM prep_test WHERE a = $1;
        """,
            pg_conn,
        )

        if execution_type == "pushdown":
            run_command(
                "SET LOCAL pg_lake_table.enable_full_query_pushdown TO true;",
                pg_conn,
            )
        else:
            run_command(
                "SET LOCAL pg_lake_table.enable_full_query_pushdown TO false;",
                pg_conn,
            )

        # run few times inside a tx, should give the same result
        results = run_query("execute p1(1,1);", pg_conn, raise_error=False)

        assert results[0] == [1, 1]

        # Also test unused parameters with explain
        if execution_type == "pushdown":
            results = run_query(
                "EXPLAIN (analyze, verbose, format 'json') EXECUTE p1(1,1)", pg_conn
            )
            assert "Query Pushdown" in str(results), str(results[0])
        else:
            results = run_query(
                "EXPLAIN (analyze, verbose, format 'json') EXECUTE p1(1,1)", pg_conn
            )
            assert "Vectorized" in str(results[0]), str(results[0])

        pg_conn.rollback()

        run_command("DEALLOCATE p1;", pg_conn)


def test_use_same_parameters_multiple_times(
    pg_conn, with_default_location, extension, s3, request
):

    for execution_type in ["pushdown", "vectorized"]:
        table_name = "test_dynamic_parameters"
        url = f"s3://{TEST_BUCKET}/{table_name}/{execution_type}/data.parquet"

        run_command("BEGIN;", pg_conn)

        # Set up the foreign table
        run_command(
            f"""

            CREATE TABLE prep_test (a int, b int, c int, d int, e int) USING iceberg;
            INSERT INTO prep_test VALUES (1,2,3,4,5);

            -- All declared parameters ($1 to $5) are used directly in the WHERE clause
            PREPARE use_all (int, int, int, int, int) AS
            SELECT * FROM prep_test WHERE a = $1 AND b = $2 AND c = $3 AND d = $4 AND e = $5;

            -- Only the first parameter ($1) is used, combined with constants in expressions
            PREPARE use_same_for_all (int, int, int, int, int) AS
            SELECT * FROM prep_test WHERE a = $1 AND b = $1 + 1 AND c = $1 + 2 AND d = $1 + 3 AND e = $1 + 4;

            -- Only the last parameter ($5) is used in all conditions
            PREPARE use_last_same_for_all (int, int, int, int, int) AS
            SELECT * FROM prep_test WHERE a = $5 AND b = $5 + 1 AND c = $5 + 2 AND d = $5 + 3 AND e = $5 + 4;

            -- Uses $2 and $3 in various expressions, testing parameter re-use and order
            PREPARE use_mid_same_for_all (int, int, int, int, int) AS
            SELECT * FROM prep_test WHERE a = $3 AND b = $3 + 1 AND c = $2 + 2 AND d = $2 + 3 AND e = $3 + 4;

            -- Only two parameters ($1 and $2) are declared and used in expressions
            PREPARE use_mixture (int, int) AS
            SELECT * FROM prep_test WHERE a = $1 AND b = $1 + 1 AND c = $2 + 2 AND d = $2 + 3 AND e = $1 + 4;

            -- Parameters $1 and $4 are used; $2 and $3 are declared but unused
            PREPARE use_mixture_skip_some (int, int, int, int) AS
            SELECT * FROM prep_test WHERE a = $1 AND b = $1 + 1 AND c = $4 + 2 AND d = $4 + 3 AND e = $1 + 4;

            -- All conditions use hardcoded values; parameters are declared but unused
            PREPARE use_none (int, int, int, int, int) AS
            SELECT * FROM prep_test WHERE a = 1 AND b = 2 AND c = 3 AND d = 4 AND e = 5;

            -- Test with unused parameters: declare 5 parameters but use only $1
            PREPARE unused_params (int, int, int, int, int) AS
            SELECT * FROM prep_test WHERE a = $1;

            -- Test with some parameters unused: use $1 and $5, skip $2, $3, $4
            PREPARE some_params_unused (int, int, int, int, int) AS
            SELECT * FROM prep_test WHERE a = $1 AND e = $5;

            -- Test with parameters used multiple times
            PREPARE repeated_params (int, int) AS
            SELECT * FROM prep_test WHERE a = $1 AND b = $2 AND c = $2 + $1 AND d = $2 + $2 AND e = $1 + $2 + $2;

            -- Test with parameters used out of order
            PREPARE out_of_order_params (int, int, int) AS
            SELECT * FROM prep_test WHERE a = $3 AND b = $2 AND c = $1;

            -- Test with parameters used in expressions
            PREPARE param_expressions (int) AS
            SELECT * FROM prep_test WHERE a = $1 AND b = $1 + 1 AND c = $1 + 2 AND d = $1 + 3 AND e = $1 + 4;

            -- Test with parameters in SELECT list
            PREPARE params_in_select_list (int, int) AS
            SELECT $1 AS x, $2 AS y FROM prep_test WHERE a = 1 AND b = 2 AND c = 3 AND d = 4 AND e = 5;

            -- Test with zero parameters
            PREPARE zero_params AS
            SELECT * FROM prep_test WHERE a = 1 AND b = 2 AND c = 3 AND d = 4 AND e = 5;

            -- Test with non-contiguous parameters used
            PREPARE non_contiguous_params (int, int, int, int, int) AS
            SELECT * FROM prep_test WHERE a = $1 AND b = $5;

            -- Test with a large number of parameters, only some used
            PREPARE large_param_list (int, int, int, int, int, int, int, int, int, int) AS
            SELECT * FROM prep_test WHERE a = $1 AND e = $10;

        """,
            pg_conn,
        )

        if execution_type == "pushdown":
            run_command(
                "SET LOCAL pg_lake_table.enable_full_query_pushdown TO true;",
                pg_conn,
            )
        else:
            run_command(
                "SET LOCAL pg_lake_table.enable_full_query_pushdown TO false;",
                pg_conn,
            )

        queries = [
            "use_all(1,2,3,4,5);",
            "use_same_for_all(1,1,1,1,1);",
            "use_last_same_for_all(1,1,1,1,1);",
            "use_mid_same_for_all(1,1,1,1,1);",
            "use_mixture(1,1);",
            "use_mixture_skip_some(1,1,1,1)",
            "use_none(1,1,1,1,1);",
            "unused_params(1, 0, 0, 0, 0);",
            "some_params_unused(1, 0, 0, 0, 5);",
            "repeated_params(1, 2);",
            "out_of_order_params(3, 2, 1);",
            "param_expressions(1);",
            "params_in_select_list(100, 200);",
            "zero_params",
            "non_contiguous_params(1, 0, 0, 0, 2)",
            "large_param_list(1, 0, 0, 0, 0, 0, 0, 0, 0, 5);",
        ]

        for query in queries:
            results = run_query(f"EXECUTE {query}", pg_conn)
            assert len(results) == 1

        pg_conn.rollback()

        run_command("DEALLOCATE all;", pg_conn)


def test_prepared_insert_select_with_array(
    pg_conn, with_default_location, extension, s3, request
):

    for execution_type in ["pushdown", "vectorized"]:

        run_command("BEGIN;", pg_conn)

        # Set up the foreign table
        run_command(
            f"""
            CREATE SCHEMA test_prepared_insert_select_with_array;
            SET search_path TO test_prepared_insert_select_with_array;

            create table test (x text) using iceberg;
            insert into test values ('hello');
            prepare foo(text[]) as insert into test select * from test where x = any($1) RETURNING x;

        """,
            pg_conn,
        )

        if execution_type == "pushdown":
            run_command(
                "SET LOCAL pg_lake_table.enable_full_query_pushdown TO true;",
                pg_conn,
            )
        else:
            run_command(
                "SET LOCAL pg_lake_table.enable_full_query_pushdown TO false;",
                pg_conn,
            )

        for i in range(0, 8):
            results = run_query(f"EXECUTE foo(array['hello']);", pg_conn)
            assert len(results) == pow(2, i)

        # run with NULL input once
        results = run_query(f"EXECUTE foo(NULL);", pg_conn)
        assert len(results) == 0

        pg_conn.rollback()

        run_command("DEALLOCATE all;", pg_conn)
