import os
import pytest
from utils_pytest import *

CACHE_FILE_PREFIX = "pgl-cache."


# Caching from a public web server that's not good at range requests
def test_cache_pdok(s3, pg_conn, extension):
    filename = "v1_0?request=GetFeature&service=WFS&version=2.0.0&typeName=gemeente_gegeneraliseerd&outputFormat=json"
    serverdir = f"cbs/gebiedsindelingen/2017/wfs"
    url = f"https://service.pdok.nl/{serverdir}/{filename}"

    cached_path = Path(
        f"{server_params.PGDUCK_CACHE_DIR}/https/service.pdok.nl/{serverdir}/{CACHE_FILE_PREFIX}{filename}"
    )

    run_command(
        f"""
		SELECT lake_file_cache.add('{url}');
	""",
        pg_conn,
    )

    assert cached_path.exists()
