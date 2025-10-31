import os
import pytest
from utils_pytest import *


def test_hugging_face(s3, pg_conn, pgduck_conn, extension):
    # The following URL points to a real file
    # We picked this one because it has the most likes and is small
    url = f"hf://datasets/fka/awesome-chatgpt-prompts@refs%2Fconvert%2Fparquet/default/train/0000.parquet"

    run_command(
        f"""
        CREATE FOREIGN TABLE prompts () SERVER pg_lake
        OPTIONS (path '{url}');
    """,
        pg_conn,
    )

    result = run_query(
        """
        SELECT count(*) FROM prompts WHERE act ilike '%spongebob%'
    """,
        pg_conn,
    )
    assert result[0]["count"] > 0

    result = run_query(
        """
        SELECT * FROM lake_file.list('hf://datasets/fka/awesome-chatgpt-prompts/p*')
    """,
        pg_conn,
    )
    assert "prompts.csv" in result[0]["path"]

    run_command(
        f"""
        CREATE TABLE prompts_local ()
        WITH (load_from = '{url}');
    """,
        pg_conn,
    )

    result = run_query(
        """
        SELECT count(*) FROM prompts WHERE act ilike '%spongebob%'
    """,
        pg_conn,
    )
    assert result[0]["count"] > 0

    run_command(
        f"""
        TRUNCATE prompts_local;
        COPY prompts_local FROM '{url}';
    """,
        pg_conn,
    )

    result = run_query(
        """
        SELECT count(*) FROM prompts WHERE act ilike '%spongebob%'
    """,
        pg_conn,
    )
    assert result[0]["count"] > 0

    pg_conn.rollback()


def test_cache_hugging_face(s3, pg_conn, pgduck_conn, extension):
    url = f"hf://datasets/fka/awesome-chatgpt-prompts/prompts.csv"
    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/hf/datasets/fka/awesome-chatgpt-prompts/pgl-cache.prompts.csv"
    )

    run_command(
        f"""
        SELECT lake_file_cache.add('{url}');
    """,
        pg_conn,
    )

    assert cached_path.exists()

    pg_conn.rollback()
