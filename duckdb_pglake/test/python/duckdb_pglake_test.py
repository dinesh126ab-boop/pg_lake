import duckdb
import os
import pytest

# Get a fresh connection to DuckDB with the duckdb_pglake extension binary loaded
@pytest.fixture
def duckdb_conn():
    extension_binary = os.getenv("DUCKDB_PGLAKE_EXTENSION_BINARY_PATH")
    if extension_binary == "":
        raise Exception(
            "Please make sure the `DUCKDB_PGLAKE_EXTENSION_BINARY_PATH` is set to run the python tests"
        )
    conn = duckdb.connect("", config={"allow_unsigned_extensions": "true"})
    conn.execute(f"load '{extension_binary}'")
    return conn


def test_to_date(duckdb_conn):
    duckdb_conn.execute("SELECT to_date(19804) as value;")
    res = duckdb_conn.fetchall()
    assert res[0][0] == "2024-03-22"
