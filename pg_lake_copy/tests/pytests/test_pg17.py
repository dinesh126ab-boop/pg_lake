import io
import pytest
from utils_pytest import *

TEST_TABLE_NAMESPACE = "test_pg17"
TEST_TABLE_NAME = "pg17"

# Define the data to be copied into the table
# only 3 lines are fine
data = """\
1,c,c,pg_lake,yes,20241010,2024-01-01 12:00:00
2,a,a,pg_lake,no,20240102,2024-01-01 12:00:00
3,a,a,pg_lake,Y,20240103,2024-01-01 12:00:00
4,a,a,pg_lake,N,20240104,2024-01-01 12:00:61
a,a,a,pg_lake,1,20240103,20241010010101
2147483648,1,1,1,0,20241201,20241010010101
5,,a,pg_lake,random,20241201,2024-01-01 12:00:51
6,,a,pg_lake,false,20243001,2024-01-01 12:00:51
"""


def test_pg17_copy(extension, s3, pg_conn, with_default_location):

    if get_pg_version_num(pg_conn) < 170000:
        return

    # Create schema and table
    run_command(f"CREATE SCHEMA {TEST_TABLE_NAMESPACE}", pg_conn)
    run_command(
        f"CREATE TABLE {TEST_TABLE_NAMESPACE}.{TEST_TABLE_NAME} (row_no integer, text_col text DEFAULT 'x'::text, text_col_2 text NOT NULL, data text, yes_or_no boolean, event_date date, event_time timestamp without time zone CONSTRAINT copy17_check CHECK (length(data) > 5)) USING pg_lake_iceberg",
        pg_conn,
    )

    # make the table visible
    pg_conn.commit()

    # Command to perform the COPY operation

    copy_command_without_error_ignore = (
        f"COPY {TEST_TABLE_NAMESPACE}.{TEST_TABLE_NAME} FROM STDIN WITH (FORMAT csv)"
    )

    # Use the run_command function to execute the COPY command
    try:
        with pg_conn.cursor() as cursor:
            data_io = io.StringIO(data)
            cursor.copy_expert(copy_command_without_error_ignore, data_io)

        # we should not get here as we already errored
        assert False
    except psycopg2.DatabaseError as error:
        assert "date/time field value out of range" in error.pgerror

    pg_conn.rollback()

    # this time we don't need try/catch as we don't expect errors
    copy_command_with_error_ignore = f"COPY {TEST_TABLE_NAMESPACE}.{TEST_TABLE_NAME} FROM STDIN WITH (FORMAT csv, ON_ERROR ignore)"
    with pg_conn.cursor() as cursor:
        data_io = io.StringIO(data)
        cursor.copy_expert(copy_command_with_error_ignore, data_io)

    pg_conn.commit()

    results = run_query(
        f"SELECT count(*) FROM {TEST_TABLE_NAMESPACE}.{TEST_TABLE_NAME}", pg_conn
    )
    print(results)
    assert results[0][0] == 3

    # clean up after test
    run_command(f"DROP SCHEMA {TEST_TABLE_NAMESPACE} CASCADE", pg_conn)
    pg_conn.commit()
