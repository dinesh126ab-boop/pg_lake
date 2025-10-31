import pytest
from utils_pytest import *


def test_recovery(superuser_conn, s3, extension):
    # Currently this only checks whether we can call the function, since it does nothing
    run_command("call lake_table.finish_postgres_recovery()", superuser_conn)
