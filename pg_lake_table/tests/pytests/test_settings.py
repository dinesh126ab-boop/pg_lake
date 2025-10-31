import pytest
from utils_pytest import *


def test_settings(superuser_conn, extension):
    # Ensure we can select from the pg_settings view; we had had some crashes reported with bad GUC
    res = run_query("select * from pg_settings", superuser_conn)

    assert len(res) > 0
