import pytest
import psycopg2
from utils_pytest import *


def test_plpgsql_function(pg_conn, extension, s3, with_default_location):

    run_command(
        """
			CREATE SCHEMA plpgsql_function;
			SET search_path TO plpgsql_function;
			CREATE TABLE employees (
			    emp_id SERIAL  ,
			    name VARCHAR(100),
			    department_id INT,
			    salary NUMERIC(10, 2)
			) USING iceberg;

			INSERT INTO employees (name, department_id, salary)
			VALUES
			    ('Alice', 1, 60000),
			    ('Bob', 2, 80000),
			    ('Charlie', 3, 70000);

			CREATE OR REPLACE FUNCTION total_salary_by_department(dept_id INT) RETURNS NUMERIC AS $$
			BEGIN
			    RETURN (SELECT SUM(salary) FROM employees WHERE department_id = dept_id);
			END;
			$$ LANGUAGE plpgsql;

			""",
        pg_conn,
    )
    pg_conn.commit()

    for execution_type in ["pushdown", "vectorized"]:
        run_command("BEGIN;", pg_conn)
        # Set up the foreign table
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

        results = run_query(f"""SELECT total_salary_by_department(1);""", pg_conn)
        assert results == [[Decimal("60000.00")]]
        run_command("ROLLBACK;", pg_conn)

    run_command("DROP SCHEMA plpgsql_function CASCADE", pg_conn)


def test_plpgsql_function_params(pg_conn, extension, s3, with_default_location):

    run_command(
        """
            CREATE SCHEMA test_plpgsql_function_params;
            SET search_path TO test_plpgsql_function_params;
            CREATE TABLE random_texts USING iceberg AS 
                SELECT gen_random_uuid()::text as text_value FROM generate_series(0,25);

            CREATE OR REPLACE FUNCTION self_check(n_times INT DEFAULT 8)
            RETURNS VOID AS $$
            DECLARE 
                selected_key TEXT;
                row_count INT;
                i INT := 0;
            BEGIN
                WHILE i < n_times LOOP
                    -- Select a random key
                    SELECT text_value INTO selected_key 
                    FROM random_texts 
                    ORDER BY random() 
                    LIMIT 1;
                    
                    -- If no key was found, raise an error
                    IF selected_key IS NULL THEN
                        RAISE EXCEPTION 'No rows found in the table!';
                    END IF;

                    -- Check if the selected key exists in the table
                    SELECT COUNT(*) INTO row_count 
                    FROM random_texts 
                    WHERE text_value = selected_key;

                    -- If the key is missing, throw an error
                    IF row_count = 0 THEN
                        RAISE EXCEPTION 'Self-check failed! Selected key % not found!', selected_key;
                    END IF;

                    -- do an INSERT .. SELECT
                    INSERT INTO random_texts
                    SELECT * FROM random_texts
                    WHERE text_value = selected_key;

                    -- Increment the loop counter
                    i := i + 1;
                END LOOP;
            END $$ LANGUAGE plpgsql;
            """,
        pg_conn,
    )
    pg_conn.commit()

    for execution_type in ["pushdown", "vectorized"]:
        run_command("BEGIN;", pg_conn)

        # Set up the foreign table
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

        run_command(f"""SELECT self_check();""", pg_conn)

        res = run_query("SHOW plan_cache_mode", pg_conn)
        assert res[0][0] == "auto"
    run_command("ROLLBACK;", pg_conn)

    run_command("DROP SCHEMA test_plpgsql_function_params CASCADE", pg_conn)


# show that we do not interfere with the plan mode for local table only transactions
def test_plpgsql_create_heap_table_plan_mode(
    pg_conn, extension, s3, with_default_location
):
    run_command(
        """
        CREATE SCHEMA test_plpgsql_create_heap_table_plan_mode;
        SET search_path TO test_plpgsql_create_heap_table_plan_mode;
        CREATE TABLE test_heap(id int, data text);

        """,
        pg_conn,
    )
    pg_conn.commit()

    for plan_mode in ["force_custom_plan", "force_generic_plan", "auto"]:

        run_command("BEGIN;", pg_conn)
        run_command(f"SET LOCAL plan_cache_mode TO {plan_mode};", pg_conn)
        run_command("SELECT count(*) FROM test_heap", pg_conn)
        res = run_query("SHOW plan_cache_mode", pg_conn)
        assert res[0][0] == f"{plan_mode}"
        run_command("ROLLBACK", pg_conn)

    res = run_query("SHOW plan_cache_mode", pg_conn)
    assert res[0][0] == "auto"

    run_command("DROP SCHEMA test_plpgsql_create_heap_table_plan_mode CASCADE", pg_conn)
    pg_conn.commit()


def test_plpgsql_create_iceberg_ddl(pg_conn, extension, s3, with_default_location):
    run_command(
        """
        CREATE SCHEMA test_plpgsql_ddl;
        SET search_path TO test_plpgsql_ddl;
        CREATE TABLE test_heap(id int, data text);
        CREATE FUNCTION test_create_ddl() RETURNS void LANGUAGE plpgsql AS $$
        BEGIN
            CREATE TABLE test_plpgsql_ddl.test_create_ddl(LIKE test_plpgsql_ddl.test_heap) USING ICEBERG;
            DROP TABLE test_plpgsql_ddl.test_create_ddl;
        END
        $$;
        """,
        pg_conn,
    )

    run_command("SELECT test_create_ddl(); SELECT test_create_ddl();", pg_conn)

    pg_conn.rollback()


# These contexts could cause memory errors if run inside a function
def test_plpgsql_pg_lake_ddl(pg_conn, extension, s3):
    url = f"s3://{TEST_BUCKET}/test_plpgsql_pg_lake_ddl/data.parquet"

    # setup some sample data
    run_command(
        f"""
        COPY (SELECT 1 as id, 'blah' as text) to '{url}';
        """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE SCHEMA test_plpgsql_ddl;
        SET search_path TO test_plpgsql_ddl;
        CREATE FUNCTION test_create_ddl() RETURNS void LANGUAGE plpgsql AS $$
        BEGIN
            CREATE TABLE test_plpgsql_ddl.test_load_from () WITH (load_from='{url}');
            CREATE TABLE test_plpgsql_ddl.test_definition_from () WITH (definition_from='{url}');
        END
        $$;
        """,
        pg_conn,
    )

    # verify no errors raised
    run_command("SELECT test_create_ddl();", pg_conn)

    pg_conn.rollback()
