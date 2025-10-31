import pytest
import psycopg2
import time
import duckdb
import math
import datetime
from decimal import *
from utils_pytest import *


def test_s3_copy_to_json(pg_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/test_s3_copy_to/data.json"

    run_command(
        f"""
        COPY (SELECT s AS id, CASE WHEN s % 2 = 0 THEN NULL ELSE 'hello-'||s END AS desc_col FROM generate_series(1,5) s) TO '{url}' WITH (FORMAT 'JSON');
    """,
        pg_conn,
    )

    run_command(
        """
                CREATE FOREIGN TABLE test_s3_json (
                    id int,
                    desc_col text
                ) SERVER pg_lake OPTIONS (format 'json', path '{}');
    """.format(
            url
        ),
        pg_conn,
    )

    # Query the foreign table and aggregate data
    # This query sums the IDs and counts the non-null descriptions
    cur = pg_conn.cursor()
    cur.execute(
        """
        SELECT SUM(id) AS total_id, COUNT(desc_col) AS desc_count FROM test_s3_json;
    """
    )
    result = cur.fetchone()
    cur.close()

    # Assert the correctness of the aggregated results
    assert result == (15, 3), f"Expected aggregated results (15, 3), got {result}"

    # Cleanup
    pg_conn.rollback()


def test_s3_copy_json_jsonb(pg_conn, s3, extension):
    # Define the S3 URL for the JSON file
    url = f"s3://{TEST_BUCKET}/test_nested_json/data.json"

    # Use COPY to generate and push correctly structured JSON/JSONB data to S3
    run_command(
        f"""
        COPY (
            SELECT s as id, json_build_object(
                    'json_col', json_build_object(
                        'name', 'User-'||s,
                        'data', json_build_object(
                            'age', 20+s,
                            'skills', json_build_object(
                                'programming', json_build_array('Python', 'SQL'),
                                'nested', json_build_object(
                                    'level', 'Advanced',
                                    'years', 3+s
                                )
                            )
                        )
                    )) as json_col,
                    json_build_object(
                    'jsonb_col', json_build_object(
                        'location', 'Location-'||s,
                        'details', json_build_object(
                            'population', 100000*s,
                            'nested', json_build_object(
                                'key', 'Value-'||s,
                                'isNested', true
                            )
                        )
                    )
                )  as jsonb_col
            FROM generate_series(1,3) s
        ) TO '{url}' WITH (FORMAT 'json');
    """,
        pg_conn,
    )

    # Ensure the foreign table definition matches the expected JSON structure
    run_command(
        """
                CREATE FOREIGN TABLE test_nested_json (
                    id int,
                    json_col json,
                    jsonb_col jsonb
                ) SERVER pg_lake OPTIONS (format 'json', path '{}');
    """.format(
            url
        ),
        pg_conn,
    )

    # Query the foreign table and validate the nested JSON/JSONB structures
    cur = pg_conn.cursor()

    # Adjusted query to account for the additional nesting level
    cur.execute(
        """
        SELECT
            (json_col->'json_col'->'data'->'skills'->'nested'->>'level') AS skill_level,
            (jsonb_col->'jsonb_col'->'details'->'nested'->>'key') AS nested_key
        FROM test_nested_json
        WHERE id = 1;
    """
    )
    result = cur.fetchone()

    # Adjusted assertion to match the expected values
    assert result == (
        "Advanced",
        "Value-1",
    ), f"Expected nested results ('Advanced', 'Value-1'), got {result}"

    # Cleanup
    pg_conn.rollback()


def test_s3_copy_json_jsonb_to_parquet(pg_conn, s3, extension):
    # Define the S3 URL for the JSON file
    url = f"s3://{TEST_BUCKET}/test_nested_json/data.parquet"

    # Use COPY to generate and push correctly structured JSON/JSONB data to S3
    run_command(
        f"""
        COPY (
            SELECT s as id, json_build_object(
                    'json_col', json_build_object(
                        'name', 'User-'||s,
                        'data', json_build_object(
                            'age', 20+s,
                            'skills', json_build_object(
                                'programming', json_build_array('Python', 'SQL'),
                                'nested', json_build_object(
                                    'level', 'Advanced',
                                    'years', 3+s
                                )
                            )
                        )
                    )) as json_col,
                    json_build_object(
                    'jsonb_col', json_build_object(
                        'location', 'Location-'||s,
                        'details', json_build_object(
                            'population', 100000*s,
                            'nested', json_build_object(
                                'key', 'Value-'||s,
                                'isNested', true
                            )
                        )
                    )
                )  as jsonb_col
            FROM generate_series(1,3) s
        ) TO '{url}' WITH (FORMAT 'parquet');
    """,
        pg_conn,
    )

    # Ensure the foreign table definition matches the expected JSON structure
    run_command(
        """
                CREATE FOREIGN TABLE test_nested_json (
                    id int,
                    json_col json,
                    jsonb_col jsonb
                ) SERVER pg_lake OPTIONS (format 'parquet', path '{}');
    """.format(
            url
        ),
        pg_conn,
    )

    # Query the foreign table and validate the nested JSON/JSONB structures
    cur = pg_conn.cursor()

    # Adjusted query to account for the additional nesting level
    cur.execute(
        """
        SELECT
            (json_col->'json_col'->'data'->'skills'->'nested'->>'level') AS skill_level,
            (jsonb_col->'jsonb_col'->'details'->'nested'->>'key') AS nested_key
        FROM test_nested_json
        WHERE id = 1;
    """
    )
    result = cur.fetchone()

    # Adjusted assertion to match the expected values
    assert result == (
        "Advanced",
        "Value-1",
    ), f"Expected nested results ('Advanced', 'Value-1'), got {result}"

    # Cleanup
    pg_conn.rollback()


def test_s3_json_variety_types_handling(pg_conn, s3, extension):
    # Assuming `run_command` function executes SQL commands in PostgreSQL
    # and `json_path` is the S3 URL for the JSON file
    json_path = f"s3://{TEST_BUCKET}/test_json_variety_types_handling/data.json"

    # Create table with a variety of data types
    create_table_command = """
    CREATE TABLE test_types (
        c_bit bit,
        c_bool bool,
        c_bpchar bpchar,
        c_char char,
        c_cidr cidr,
        c_date date,
        c_float4 float4,
        c_float8 float8,
        c_inet inet,
        c_int2 int2,
        c_int4 int4,
        c_int8 int8,
        c_interval interval,
        c_money money,
        c_name name,
        c_numeric numeric,
        c_numeric_large numeric(39,2),
        c_numeric_mod numeric(4,2),
        c_oid oid,
        c_text text,
        c_time time,
        c_timestamp timestamp,
        c_timestamptz timestamptz,
        c_timetz timetz,
        c_uuid uuid,
        c_varbit varbit,
        c_varchar varchar
    );
    """
    run_command(create_table_command, pg_conn)

    # Insert data into the table
    insert_command = """
    INSERT INTO test_types VALUES (
        '1', true, 'hello', 'a', '192.168.0.0/16',
        '2024-01-01', 3.4, 33333333.33444444, '192.168.1.1', 14, 100000, 10000000000,
        '3 days', '$4.5', 'test',
        199.123, 123456789012345678901234.99,
        99.99, 11, 'fork', '19:34', '2024-01-01 15:00:00', '2024-01-01 15:00:00 UTC', '19:34 UTC',
        'acd661ca-d18c-42e2-9c4e-61794318935e', '0110', 'abc'
    );
    """
    run_command(insert_command, pg_conn)

    copy_command = f"""
    COPY test_types TO '{json_path}' WITH (FORMAT 'JSON');
    """
    run_command(copy_command, pg_conn)

    create_fdw_table_command = f"""

    CREATE FOREIGN TABLE fdw_test_types (
        c_bit bit,
        c_bool bool,
        c_bpchar bpchar,
        c_char char,
        c_cidr cidr,
        c_date date,
        c_float4 float4,
        c_float8 float8,
        c_inet inet,
        c_int2 int2,
        c_int4 int4,
        c_int8 int8,
        c_interval interval,
        c_money money,
        c_name name,
        c_numeric numeric,
        c_numeric_large numeric(39,2),
        c_numeric_mod numeric(4,2),
        c_oid oid,
        c_text text,
        c_time time,
        c_timestamp timestamp,
        c_timestamptz timestamptz,
        c_timetz timetz,
        c_uuid uuid,
        c_varbit varbit,
        c_varchar varchar
    ) SERVER pg_lake OPTIONS (path '{json_path}');
    """
    run_command(create_fdw_table_command, pg_conn)

    # Query the foreign table to verify data integrity
    cur = pg_conn.cursor()
    cur.execute(
        """SELECT c_bit,
       c_bool,
       c_bpchar,
       c_char,
       c_cidr,
       c_date,
       c_float4,
       c_float8,
       c_inet,
       c_int2,
       c_int4,
       c_int8,
       c_interval,
       c_money,
       c_name,
       c_numeric,
       c_numeric_large,
       c_numeric_mod,
       c_oid,
       c_text,
       c_time,
       c_timestamp,
       c_timestamptz,
       c_timetz,
       c_uuid,
       c_varbit,
       c_varchar
FROM fdw_test_types;
"""
    )
    result = cur.fetchall()
    cur.close()

    expected_result = [
        (
            "1",  # c_bit
            True,  # c_bool
            "hello",  # c_bpchar (expect trailing spaces due to character type padding)
            "a",  # c_char
            "192.168.0.0/16",  # c_cidr
            datetime.date(2024, 1, 1),  # c_date
            3.4,  # c_float4
            33333333.33444444,  # c_float8
            "192.168.1.1",  # c_inet
            14,  # c_int2
            100000,  # c_int4
            10000000000,  # c_int8
            datetime.timedelta(days=3),  # c_interval
            "$4.50",  # c_money (format may vary by locale)
            "test",  # c_name
            Decimal("199.123"),  # c_numeric
            Decimal("1234567890123457000000000000000000000000"),  # c_numeric_large
            Decimal("99.99"),  # c_numeric_mod
            11,  # c_oid
            "fork",  # c_text
            datetime.time(19, 34),  # c_time
            datetime.datetime(2024, 1, 1, 15, 0),  # c_timestamp
            datetime.datetime(
                2024,
                1,
                1,
                15,
                0,
                tzinfo=datetime.timezone(datetime.timedelta(seconds=10800)),
            ),  # c_timestamptz
            datetime.time(
                19, 34, tzinfo=datetime.timezone.utc
            ),  # c_timetz (adjust timezone as necessary)
            "acd661ca-d18c-42e2-9c4e-61794318935e",  # c_uuid
            "0110",  # c_varbit
            "abc",  # c_varchar
        )
    ]

    # Now we assert each field individually to ensure data integrity
    assert result[0][0] == expected_result[0][0], "c_bit mismatch"
    assert result[0][1] == expected_result[0][1], "c_bool mismatch"
    assert (
        result[0][2].strip() == expected_result[0][2]
    ), "c_bpchar mismatch"  # Trimming any padding spaces
    assert (
        result[0][3].strip() == expected_result[0][3]
    ), "c_char mismatch"  # Trimming any padding spaces
    assert result[0][4] == expected_result[0][4], "c_cidr mismatch"
    assert result[0][5] == expected_result[0][5], "c_date mismatch"
    assert result[0][6] == expected_result[0][6], "c_float4 mismatch"
    assert result[0][7] == expected_result[0][7], "c_float8 mismatch"
    assert result[0][8] == expected_result[0][8], "c_inet mismatch"
    assert result[0][9] == expected_result[0][9], "c_int2 mismatch"
    assert result[0][10] == expected_result[0][10], "c_int4 mismatch"
    assert result[0][11] == expected_result[0][11], "c_int8 mismatch"
    assert result[0][12] == expected_result[0][12], "c_interval mismatch"
    assert result[0][13] == expected_result[0][13], "c_money mismatch"
    assert result[0][14] == expected_result[0][14], "c_name mismatch"
    assert round(result[0][15], 4) == round(
        expected_result[0][15], 4
    ), "c_numeric mismatch"
    # assert int(result[0][16]) == int(expected_result[0][16]), "c_numeric_mod mismatch"
    assert result[0][17] == expected_result[0][17], "c_oid mismatch"
    assert result[0][18] == expected_result[0][18], "c_text mismatch"
    assert result[0][19] == expected_result[0][19], "c_time mismatch"
    assert result[0][20] == expected_result[0][20], "c_timestamp mismatch"
    assert (
        result[0][21].date() == expected_result[0][21].date()
    ), "c_timestamptz date mismatch"
    assert result[0][24] == expected_result[0][24], "c_uuid mismatch"
    assert result[0][25] == expected_result[0][25], "c_varbit mismatch"
    assert result[0][26] == expected_result[0][26], "c_varchar mismatch"

    # Cleanup
    pg_conn.rollback()


# inspired from Postgresql's copy tests
def test_json_features_with_s3_and_fdw(pg_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/test_json_features_with_s3_and_fdw/data.json"

    # Create a temporary table for the test
    pg_conn.cursor().execute(
        """
        CREATE TEMP TABLE copytest (
            style   text,
            test    text,
            filler  int
        );
    """
    )
    pg_conn.commit()

    # Insert data with various styles of embedded line-ending characters
    pg_conn.cursor().execute(
        """
        INSERT INTO copytest VALUES('DOS', E'abc\\r\\ndef', 1);
        INSERT INTO copytest VALUES('Unix', E'abc\\ndef', 2);
        INSERT INTO copytest VALUES('Mac', E'abc\\rdef', 3);
        INSERT INTO copytest VALUES(E'esc\\\\ape', E'a\\\\r\\\\\\\\r\\\\\\\\n\\\\nb', 4);
    """
    )
    pg_conn.commit()

    pg_conn.cursor().execute(
        f"""
        COPY copytest TO '{url}' WITH (FORMAT 'JSON');
    """
    )
    pg_conn.commit()

    pg_conn.cursor().execute(
        """
        CREATE FOREIGN TABLE fdw_copytest (
            style text,
            test text,
            filler int
        ) SERVER pg_lake OPTIONS (path '{}', format 'json');
    """.format(
            url
        )
    )
    pg_conn.commit()

    cur = pg_conn.cursor()

    # Verify the data was copied and can be read correctly
    cur.execute(
        """
        SELECT * FROM copytest EXCEPT SELECT * FROM fdw_copytest;
    """
    )
    results = cur.fetchall()
    assert results == [], "Mismatch found between original and FDW-loaded data."

    cur.execute(
        """
        SELECT * FROM fdw_copytest EXCEPT SELECT * FROM copytest;
    """
    )
    results = cur.fetchall()
    assert results == [], "Mismatch found between FDW-loaded data and original."

    # Clean up
    pg_conn.cursor().execute("DROP TABLE copytest;")
    pg_conn.cursor().execute("DROP FOREIGN TABLE fdw_copytest;")
    pg_conn.commit()


def test_s3_copy_to_json_with_complex_target_lists(pg_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/test_s3_copy_to_json_with_complex_target_lists/data.json"

    run_command(
        f"""
        COPY (SELECT s AS id, CASE WHEN s % 2 = 0 THEN NULL ELSE 'hello-'||s END AS desc_gen
              FROM generate_series(1,5) s) TO '{url}' WITH (FORMAT 'JSON');
    """,
        pg_conn,
    )

    run_command(
        """
                CREATE FOREIGN TABLE test_s3_json_with_options (
                    id int,
                    desc_gen text
                ) SERVER pg_lake OPTIONS (format 'json', path '{}');
    """.format(
            url
        ),
        pg_conn,
    )

    # empty target list, random() is to prevent
    # optimizations by pulling subquery
    cur = pg_conn.cursor()
    cur.execute(
        """
        select count(*) FROM (select  from test_s3_json_with_options ORDER BY random());
    """
    )
    result = cur.fetchone()
    cur.close()

    assert result[0] == 5, f"Expected 5 rows, got {result[0]}"

    # same column used multiple times
    cur = pg_conn.cursor()
    cur.execute(
        """
        select id, id, id+id from test_s3_json_with_options ORDER BY 1 DESC;
    """
    )
    result = cur.fetchone()
    cur.close()

    assert result[0] == 5, f"unexpected query result for complex target list"
    assert result[1] == 5, f"unexpected query result for complex target list"
    assert result[2] == 10, f"unexpected query result for complex target list"

    # same column used multiple times
    cur = pg_conn.cursor()
    cur.execute(
        """
            SELECT *
            FROM
                (
                SELECT id, desc_gen FROM test_s3_json_with_options OFFSET 0
                ) as bar(x, y)
            ORDER BY 1 DESC, 2 DESC LIMIT 5;
    """
    )
    result = cur.fetchone()
    cur.close()
    assert result[0] == 5, f"unexpected query result for complex target list"
    assert result[1] == "hello-5", f"unexpected query result for complex target list"

    # complex expressions all over the query
    cur = pg_conn.cursor()
    cur.execute(
        """
            SELECT
               DISTINCT ON (avg_col) avg_col, cnt_1, cnt_2, cnt_3, sum_col, l_year_gt_2024, pos, count_id
            FROM
                (
                    SELECT avg(id * (5.0 / (length(desc_gen) + 0.1))) as avg_col
                    FROM test_s3_json_with_options
                    ORDER BY 1 DESC
                    LIMIT 3
                ) as foo,
                (
                    SELECT sum(id * (5.0 / (length(desc_gen) + 0.1))) as cnt_1
                    FROM test_s3_json_with_options
                    ORDER BY 1 DESC
                    LIMIT 3
                ) as bar,
                (
                    SELECT
                        avg(case
                            when id > 4
                            then length(desc_gen)
                        end) as cnt_2,
                        avg(case
                            when id > 5
                            then length(desc_gen)
                        end) as cnt_3,
                        sum(case
                            when position('a' in desc_gen) > 0
                            then 1
                            else 0
                        end) as sum_col,
                        (extract(year FROM current_date))>=2024 as l_year_gt_2024, -- Using current_date for demo purposes
                        strpos(max(desc_gen), 'a') as pos
                    FROM
                        test_s3_json_with_options
                    ORDER BY
                        1 DESC
                    LIMIT 4
                ) as baz,
                (
                    SELECT COALESCE(length(desc_gen), 20) AS count_id
                    FROM test_s3_json_with_options
                    ORDER BY 1 OFFSET 2 LIMIT 5
                ) as tar
            ORDER BY 1 DESC;


    """
    )
    result = cur.fetchone()
    cur.close()
    assert result[2] == 7, f"unexpected query result for complex target list"
    assert result[3] == None, f"unexpected query result for complex target list"
    assert result[4] == 0, f"unexpected query result for complex target list"
    assert result[5] == True, f"unexpected query result for complex target list"
    assert result[6] == 0, f"unexpected query result for complex target list"
    assert result[7] == 7, f"unexpected query result for complex target list"

    # Cleanup
    pg_conn.rollback()


def test_momaarworks_json(pg_conn, s3, extension):

    table_name = "momaartworks"
    file_key = f"{table_name}/momaartworks.json"
    file_path = f"s3://{TEST_BUCKET}/{file_key}"
    local_file_path = sampledata_filepath("momaartworks.json")

    s3.upload_file(local_file_path, TEST_BUCKET, file_key)

    error = run_command(
        f"""
    CREATE FOREIGN TABLE momaartworks () server pg_lake OPTIONS (path '{file_path}')
    """,
        pg_conn,
    )

    cur = pg_conn.cursor()
    cur.execute("SELECT * FROM momaartworks")
    results = cur.fetchall()

    assert len(results) == 10

    cur = pg_conn.cursor()
    cur.execute('SELECT avg("Height (cm)") FROM momaartworks')
    results = cur.fetchall()

    assert results[0][0] == 39.07401000000001

    cur = pg_conn.cursor()
    cur.execute('SELECT DISTINCT("Gender"[0]) FROM momaartworks')
    results = cur.fetchall()

    assert results[0][0] == "male"

    # Cleanup
    pg_conn.rollback()


def test_json_nested_types(pg_conn, extension):
    url = f"s3://{TEST_BUCKET}/test_s3_nested_types/data.json"

    create_map_type("int", "int")

    run_command(
        f"""
        CREATE TYPE lake_struct.point as (x int, y int);

        COPY (
            SELECT 1 id, ARRAY['hello','world'] ta, ARRAY[0,4] ia, ARRAY[(1,10),(2,20)]::map_type.key_int_val_int map, (99,99)::lake_struct.point struct
            UNION ALL
            SELECT 2 id, ARRAY['', NULL] ta, ARRAY[1] ia, ARRAY[(2,4)]::map_type.key_int_val_int map, (0,0)::lake_struct.point struct
            UNION ALL
            SELECT 3 id, NULL ta, NULL ia, NULL::map_type.key_int_val_int map, NULL::lake_struct.point struct
        ) TO '{url}';

        CREATE FOREIGN TABLE test_json_nested () SERVER pg_lake OPTIONS (format 'json', path '{url}');
    """,
        pg_conn,
    )

    # Can access the fields using JSON operators
    result = run_query(
        "SELECT ia->>1 res FROM test_json_nested WHERE map->>'1' = '10'", pg_conn
    )
    assert result[0]["res"] == "4"

    result = run_query(
        "SELECT map->>'2' res FROM test_json_nested WHERE struct->>'x' = '0'", pg_conn
    )
    assert result[0]["res"] == "4"

    result = run_query(
        "SELECT jsonb_array_length(ta) res FROM test_json_nested WHERE struct @> '{\"x\":0}'",
        pg_conn,
    )
    assert result[0]["res"] == 2

    pg_conn.rollback()


def test_json_unsupported_column_array(pg_conn):
    # setup
    run_command(
        """
                CREATE SCHEMA test_invalid_data_format_sc;
                """,
        pg_conn,
    )

    error = run_query(
        """CREATE FOREIGN TABLE test_invalid_data_format_sc.ft1 (
                        id int,
                        value text[]
                    ) SERVER pg_lake OPTIONS (format 'json', path 's3://');""",
        pg_conn,
        raise_error=False,
    )
    assert "array types are not supported for JSON/CSV backed pg_lake tables" in error
    pg_conn.rollback()


def test_json_unsupported_column_composite(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_invalid_data_format_sc;
                CREATE TYPE test_invalid_data_format_sc.tmp_type AS (a int, b int);
                """,
        pg_conn,
    )

    error = run_query(
        """CREATE FOREIGN TABLE test_invalid_data_format_sc.ft1 (
                        id int,
                        value test_invalid_data_format_sc.tmp_type
                    ) SERVER pg_lake OPTIONS (format 'json', path 's3://');""",
        pg_conn,
        raise_error=False,
    )

    assert (
        "composite types are not supported for JSON/CSV backed pg_lake tables" in error
    )
    pg_conn.rollback()


def test_json_unsupported_column_bytea(pg_conn, s3, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_invalid_data_format_sc;
                CREATE TYPE test_invalid_data_format_sc.tmp_type AS (a int, b int);
                """,
        pg_conn,
    )

    error = run_query(
        """CREATE FOREIGN TABLE test_invalid_data_format_sc.ft1 (
                        id int,
                        value bytea
                    ) SERVER pg_lake OPTIONS (format 'json', path 's3://');""",
        pg_conn,
        raise_error=False,
    )

    assert "bytea type is not supported for JSON/CSV backed pg_lake tables" in error
    pg_conn.rollback()
