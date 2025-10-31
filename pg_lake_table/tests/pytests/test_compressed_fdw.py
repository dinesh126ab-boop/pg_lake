import pytest
import psycopg2
import time
import duckdb
import math
import datetime
from decimal import *
from utils_pytest import *


def test_compressed_fdw_basic(pg_conn, s3, extension):

    for file_format in ["json", "csv", "parquet"]:
        for compression_format in ["none", "gzip", "zstd", "snappy"]:

            # we currently do not support these combinations
            if file_format in ["json", "csv"] and compression_format in ["snappy"]:
                continue

            url = f"s3://{TEST_BUCKET}/test_s3_copy_to/data_{compression_format}.{file_format}"

            run_command(
                f"""
                COPY (SELECT s AS id, CASE WHEN s % 2 = 0 THEN NULL ELSE 'hello-'||s END AS desc_col FROM generate_series(1,5) s) TO '{url}' WITH (FORMAT '{file_format}', compression '{compression_format}');
            """,
                pg_conn,
            )

            table_name = "test_s3_" + file_format + "_" + compression_format
            run_command(
                """
                        CREATE FOREIGN TABLE {} (
                            id int,
                            desc_col text
                        ) SERVER pg_lake OPTIONS (format '{}', path '{}', compression '{}');
            """.format(
                    table_name, file_format, url, compression_format
                ),
                pg_conn,
            )

            # execute a simple query
            cur = pg_conn.cursor()
            cur.execute(
                """
                SELECT SUM(id) AS total_id, COUNT(desc_col) AS desc_count FROM {};
            """.format(
                    table_name
                )
            )
            result = cur.fetchone()
            cur.close()

            # Assert the correctness of the aggregated results
            assert result == (
                15,
                3,
            ), f"Expected aggregated results (15, 3), got {result} for {file_format}, {compression_format}"

            # Cleanup
            pg_conn.rollback()


def test_compressed_fdw_wrong_format(pg_conn, s3, extension):

    # use proper format while creating file
    # not use non-sense format while creating fdw
    for file_format in ["json", "csv", "parquet"]:
        for compression_format in ["gzip"]:
            url = f"s3://{TEST_BUCKET}/test_s3_copy_to/data_{compression_format}.{file_format}"

            run_command(
                f"""
                COPY (SELECT s AS id, CASE WHEN s % 2 = 0 THEN NULL ELSE 'hello-'||s END AS desc_col FROM generate_series(1,5) s) TO '{url}' WITH (FORMAT '{file_format}', compression '{compression_format}');
            """,
                pg_conn,
            )

            table_name = "test_s3_" + file_format + "_" + compression_format
            try:
                run_command(
                    """
                            CREATE FOREIGN TABLE {} (
                                id int,
                                desc_col text
                            ) SERVER pg_lake OPTIONS (format '{}', path '{}', compression 'nosuchformat');
                """.format(
                        table_name, file_format, url
                    ),
                    pg_conn,
                )

                # execute a simple query
                cur = pg_conn.cursor()
                cur.execute(
                    """
                    SELECT SUM(id) AS total_id, COUNT(desc_col) AS desc_count FROM {};
                """.format(
                        table_name
                    )
                )
                result = cur.fetchone()

                # we never get here, exception is thrown
                assert False
            except psycopg2.errors.SyntaxError as e:
                assert 'compression "nosuchformat" not recognized' in str(e)

            # Cleanup
            pg_conn.rollback()


def test_compressed_fdw_format_from_url(pg_conn, s3, extension):

    # don't pass the compression option to create foreign table,
    # infer from file post-fix
    for file_format in ["json", "csv", "parquet"]:
        for compression_format in [
            ("gzip", "gz"),
            ("zstd", "zst"),
            ("none", "none"),
            ("snappy", "snappy"),
        ]:

            # we currently do not support these combinations
            if file_format in ["json", "csv"] and compression_format[0] in ["snappy"]:
                continue

            url = f"s3://{TEST_BUCKET}/test_s3_copy_to/data.{file_format}.{compression_format[1]}"

            run_command(
                f"""
                COPY (SELECT s AS id, CASE WHEN s % 2 = 0 THEN NULL ELSE 'hello-'||s END AS desc_col FROM generate_series(1,5) s) TO '{url}' WITH (FORMAT '{file_format}', compression '{compression_format[0]}');
            """,
                pg_conn,
            )

            table_name = "test_s3_" + file_format + "_" + compression_format[0]
            run_command(
                """
                        CREATE FOREIGN TABLE {} (
                            id int,
                            desc_col text
                        ) SERVER pg_lake OPTIONS (format '{}', path '{}');
            """.format(
                    table_name, file_format, url
                ),
                pg_conn,
            )

            # execute a simple query
            cur = pg_conn.cursor()
            cur.execute(
                """
                SELECT SUM(id) AS total_id, COUNT(desc_col) AS desc_count FROM {};
            """.format(
                    table_name
                )
            )
            result = cur.fetchone()

            # Assert the correctness of the aggregated results
            assert result == (
                15,
                3,
            ), f"Expected aggregated results (15, 3), got {result} for {file_format}, {compression_format[0]}"

            # Cleanup
            pg_conn.rollback()
