import pytest
import psycopg2
import time
import duckdb
import math
import datetime
import json
from decimal import *
from utils_pytest import *

# Asked ChatGPT for a list of formats

TO_CHAR_FORMATS = [
    # Basic Date and Time Formats
    "YYYY-MM-DD",
    "YYYY/MM/DD",
    "DD-MM-YYYY",
    "MM/DD/YYYY",
    "YYYY-MM-DD HH24:MI:SS",
    "YYYY-MM-DD HH12:MI:SS AM",
    "YYYY-MM-DDTHH24:MI:SS",
    "YYYY.MM.DD HH24:MI:SS",
    "HH24:MI:SS",
    "HH12:MI:SS AM",
    # Day and Month Names
    "Day",
    "Dy",
    "Month",
    "Mon",
    "FMDay",
    "FMDy",
    "FMMonth",
    "FMMon",
    # Week and Day of Year
    "WW",
    "IW",
    "W",
    "D",
    "ID",
    "DOY",
    # Quarter and Century
    "Q",
    "CC",
    "YYYY-Q",
    "CC-YYYY",
    # Julian Date
    "J",
    "YYYY-J",
    # Time Zone Information
    "TZ",
    "OF",
    # Roman Numerals and Ordinals
    "MMRM/DDRM/YYYYRM",
    "DDTH",
    "FMDDTH",
    "YYYYTH-MM-DDTH",
    "QTH",
    "TMMonth, FMDDth, YYYY",
    "FMDay FMDDth at HH12:MI AM",
    "iyyyth-iddth",
    # ISO Formats
    "IYYY-IW-ID",
    "IYYY-IW",
    "IYYY-MM-DD",
    # Combined and Creative Formats
    "YYYY-MM-DD HH24:MI:SS TZ",
    "yyyy-mm-dd hh12:mi:ss am tz",
    "YYYY-Q WW Day",
    "YYYY-MM-DDTHH24:MI:SSOF",
    "yyyy-mm-ddthh24:mi:ssof",
    "FMDay, FMMonth YYYY",
    "Month FMDDth, YYYY",
    "fmmonth fmddth, yyyy",
    "DDth Mon YYYY",
    "ddth mon yyyy",
    "DDth FMMonth YYYY",
    'fmday fmddth "Quarter:" q',
    "SSSSSth FF6FF5th AD",
    # Advanced Combinations
    "YYYYTH-MM-DDTH HH24:MI",
    "yyyyth-mm-ddth hh24:mi",
    'YYYY b.c. "Quarter:" QTH "Day:" DOYTH',
    'yyyy "Quarter:" qth "Day:" doyTH',
    'YYYY-MM-DD HH12:MI:SS PM "UTC Offset:" OF',
    'YYYY.MM.DD "Ordinal Day:" DDTH',
    'YYYY.MM.DD "Ordinal Day:" ddth',
    'Month DDth, YYYY "Week:" IW',
    'month ddth, yyyy "Week:" fmiw',
    "DDth FMMonth, YYYY HH12:MI AM",
    "ddth fmmonth, yyyy hh12:mi am",
    "FMMon DDth YYYYTH, HH24:MI",
    "fmmon ddth yyyyTH, hh24:mi",
    'FMDay FMDDth "Quarter:" Q "Julian:" J',
    'fmday fmddth "Quarter:" q',
    # Nonsense formats
    "YYY-MM-DD",
    "YY-MM-DD",
    "HH60:MI:SS",
    "HH24:MM:SS",
    "DD-MM-YYYY-XX",
    "YYYY-QQ",
    "YYYY-MM-DD HH24:MI:SS PM",
    "TMYEAR/TMMONTH/TMDAY",
    "123-ABC-XYZ",
    "YYYY~MM~DD",
    "FMFMMonth FMFMDay",
    "WWDOY",
    "YYYY-MM-DD HH24:MI:SEC",
    "YYYY-MM-32",
    "YYYY-MM-DD HH:MM:SS",
    "Day-Month-Year",
    "HH24:MI:SS TZAM",
    # Derived from postgres tests
    "DAY Day day DY Dy dy MONTH Month month MON Mon mon",
    "FMDAY FMDay FMday FMMONTH FMMonth FMmonth",
    "Y,YYY YYYY YYY YY Y CC Q MM WW DDD DD D",
    "FMY,YYY FMYYYY FMYYY FMYY FMY FMCC FMQ FMMM FMWW FMDDD FMDD FMD",
    "HH HH12 HH24 MI SS SSSS",
    '"HH:MI:SS is" HH:MI:SS "\\"text between quote marks\\""',
    "HH24--text--MI--text--SS",
    "YYYY A.D. YYYY a.d. YYYY bc HH:MI:SS P.M. HH:MI:SS p.m. HH:MI:SS pm",
    "IYYY IYY IY I IW ID",
    "FMIYYY FMIYY FMIY FMI FMIW FMID",
    "FF1 FF2 FF3 FF4 FF5 FF6  ff1 ff2 ff3 ff4 ff5 ff6  MS US",
]

TIMESTAMP_TYPES = ["timestamp", "timestamptz"]


@pytest.mark.parametrize("timestamp_type", TIMESTAMP_TYPES)
@pytest.mark.parametrize("format", TO_CHAR_FORMATS)
def test_time_functions_pushdown(
    create_time_function_pushdown_table, pg_conn, timestamp_type, format
):
    query = f"SELECT col_{timestamp_type}, to_char(col_{timestamp_type}, '{format}') FROM time_function_pushdown.tbl"

    assert_query_results_on_tables(
        query,
        pg_conn,
        ["time_function_pushdown.tbl"],
        ["time_function_pushdown.heap_tbl"],
    )


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_time_function_pushdown_table(pg_conn, s3, extension):

    url = f"s3://{TEST_BUCKET}/create_time_function_pushdown_table/data.parquet"
    run_command(
        f"""
			COPY (
					SELECT NULL::timestamp col_timestamp, NULL::timestamptz col_timestamptz
						UNION ALL
					SELECT '2019-12-31 08:57:55'::timestamp, '2019-12-31 23:58:59+0'::timestamptz
					 	UNION ALL
					SELECT '1900-01-01 00:07:41.33'::timestamp, '2023-01-01 00:08:24+0'::timestamptz
					 	UNION ALL
					SELECT '2023-01-01 00:07:04'::timestamp, '2023-01-01 00:08:53+0'::timestamptz
					 	UNION ALL
					SELECT '2023-02-14 14:59:59.22214'::timestamp, '1999-07-04 04:30:33.19999'::timestamptz
					 	UNION ALL
					SELECT '2023-02-28 20:59:59'::timestamp, '1970-01-01 12:34:56'::timestamptz
				) TO '{url}' WITH (FORMAT 'parquet');
		""",
        pg_conn,
    )
    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
	            CREATE SCHEMA time_function_pushdown;
	            CREATE FOREIGN TABLE time_function_pushdown.tbl
	            (
	            	col_timestamp timestamp,
	            	col_timestamptz timestamptz
	            ) SERVER pg_lake OPTIONS (format 'parquet', path '{}');
	            """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
	            CREATE TABLE time_function_pushdown.heap_tbl AS SELECT * FROM time_function_pushdown.tbl
	            """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command("DROP SCHEMA time_function_pushdown CASCADE", pg_conn)
    pg_conn.commit()
