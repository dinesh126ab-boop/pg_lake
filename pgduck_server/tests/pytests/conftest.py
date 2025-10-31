import pytest
import server_params
import psycopg2
from utils_pytest import *


@pytest.fixture(scope="module")
def pgduck_conn(pgduck_server):
    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def s3():
    client, server = create_mock_s3()
    yield client
    server.stop()


@pytest.fixture(scope="session")
def gcs():
    client, server = create_mock_gcs()
    yield client
    server.stop()


@pytest.fixture(scope="session")
def azure():
    client, process = create_mock_azure_blob_storage()
    yield client
    process.terminate()
