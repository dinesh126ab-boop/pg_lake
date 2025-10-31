import pytest
from utils_pytest import *


def test_delete_not_allowed(pg_conn, s3, extension):
    url = "s3://{TEST_BUCKET}/test_delete_not_allowed/test.parquet"

    error = run_command(
        f"""
        select lake_file.delete('{url}');
    """,
        pg_conn,
        raise_error=False,
    )
    assert "file deletion has been disabled" in error

    pg_conn.rollback()


def test_delete(superuser_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/test_delete/test.parquet"
    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/s3/{TEST_BUCKET}/test_delete/pgl-cache.test.parquet"
    )

    run_command(
        f"""
        set local pg_lake_table.enable_delete_file_function to on;
        copy (select 1) to '{url}';
    """,
        superuser_conn,
    )

    result = run_query(f"select lake_file.exists('{url}') exists", superuser_conn)
    assert result[0]["exists"]
    assert cached_path.exists()

    error = run_command(
        f"""
        select lake_file.delete('{url}')
    """,
        superuser_conn,
        raise_error=False,
    )

    result = run_query(f"select lake_file.exists('{url}') exists", superuser_conn)
    assert not result[0]["exists"]
    assert not cached_path.exists()

    superuser_conn.rollback()
