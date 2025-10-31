import pytest
from utils_pytest import *


# ensure our extension can be created and pulls in expected dependency
def test_extension_deps(pg_conn, pg_lake_extension, superuser_conn):
    extensions = [
        row[0] for row in run_query("select extname from pg_extension", pg_conn)
    ]

    assert "pg_lake" in extensions
    assert "pg_lake_table" in extensions


def test_pg_lake_version(pg_conn, pg_lake_extension):
    version = run_query("select lake.version()", pg_conn)
    assert version[0][0] == pg_lake_version_from_env()
