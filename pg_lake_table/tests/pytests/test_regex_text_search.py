import pytest
from utils_pytest import *


def test_queries_with_regex_text_search(s3, pg_conn, installcheck, extension):

    url = f"s3://{TEST_BUCKET}/test_s3_regex_text_search/data.parquet"

    # Create a Parquet file in mock S3
    run_command(
        f"""
        COPY (SELECT * FROM (VALUES
                ('This is a test document with number 12345.'),
                ('Here is another example, text with special number: 98765.'),
                ('Just a simple document.'),
                ('Text data with multiple numbers: 123 and 456.'),
                ('No special numbers here, just text.')
                ) AS documents(content)
    ) TO '{url}';
    """,
        pg_conn,
    )

    # Create a table with 2 columns on the fdw with varying regex_text_search
    run_command(
        """
                CREATE SCHEMA test_regex;
                CREATE FOREIGN TABLE test_regex.documents (
                     content TEXT
                ) SERVER pg_lake OPTIONS (format 'parquet', path '%s');

                CREATE TABLE test_regex.documents_local AS SELECT * FROM test_regex.documents;
        """
        % (url),
        pg_conn,
    )

    queries = [
        "SELECT * FROM test_regex.documents WHERE content ~ '\\d+' ORDER BY 1;",
        "SELECT * FROM test_regex.documents WHERE to_tsvector('english', content) @@ plainto_tsquery('english', 'number') ORDER BY 1;"
        "SELECT content, SUBSTRING(content FROM '\\d+') AS first_number FROM test_regex.documents ORDER BY 1;"
        "SELECT content, ts_headline('english', content, plainto_tsquery('english', 'number')) AS highlighted FROM test_regex.documents ORDER BY 1;",
    ]

    for query in queries:
        result_fdw = perform_query_on_cursor(query, pg_conn)

        query_local = query.replace("documents", "documents_local")
        result_local = perform_query_on_cursor(query, pg_conn)

        assert len(result_fdw) > 0
        assert result_fdw == result_local

    # Cleanup
    run_command("DROP SCHEMA test_regex CASCADE;", pg_conn)
    pg_conn.commit()
