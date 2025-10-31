import pytest
import psycopg2
from utils_pytest import *


def test_query_s3log(pg_conn, s3, extension):
    s3log_prefix = f"s3log"
    s3log_url = f"s3://{TEST_BUCKET}/{s3log_prefix}"
    s3log_path = sampledata_filepath("s3log")

    # Upload data files
    for root, dirs, files in os.walk(s3log_path):
        for filename in files:
            s3.upload_file(
                os.path.join(root, filename), TEST_BUCKET, f"{s3log_prefix}/{filename}"
            )
            print(
                f"uploading {os.path.join(root,filename)} to {s3log_prefix}/{filename}"
            )

    run_command(
        f"""
        create foreign table s3log ()
        server pg_lake
        options (format 'log', log_format 's3', path '{s3log_url}/**');
    """,
        pg_conn,
    )

    result = run_query(
        "select http_status, count(*) from s3log where request_time < now() group by 1 order by 2 desc",
        pg_conn,
    )
    assert len(result) == 2
    assert result[0]["http_status"] == "200"
    assert result[0]["count"] == 32
    assert result[1]["http_status"] == "404"
    assert result[1]["count"] == 9

    run_command(
        f"""
        create foreign table s3log_with_filename ()
        server pg_lake
        options (format 'log', log_format 's3', path '{s3log_url}/**', filename 'true');
    """,
        pg_conn,
    )

    result = run_query(
        f"select count(*) from s3log_with_filename where _filename = '{s3log_url}/2024-09-24-00-00-00-106A30CF799F4C34'",
        pg_conn,
    )
    assert result[0]["count"] == 39

    pg_conn.rollback()


def test_invalid_options(pg_conn, s3, extension):
    s3log_url = f"s3://{TEST_BUCKET}/s3log"

    error = run_command(
        f"""
        create foreign table s3log ()
        server pg_lake
        options (format 'log', log_format 's4', path '{s3log_url}/**');
    """,
        pg_conn,
        raise_error=False,
    )
    assert "unrecognized log_format" in error

    pg_conn.rollback()

    error = run_command(
        f"""
        create foreign table s3log ()
        server pg_lake
        options (format 'log', path '{s3log_url}/**');
    """,
        pg_conn,
        raise_error=False,
    )
    assert '"log_format" option is required' in error

    pg_conn.rollback()

    error = run_command(
        f"""
        create foreign table s3log ()
        server pg_lake
        options (format 'log', log_format 's3');
    """,
        pg_conn,
        raise_error=False,
    )
    assert '"path" option is required' in error

    pg_conn.rollback()
