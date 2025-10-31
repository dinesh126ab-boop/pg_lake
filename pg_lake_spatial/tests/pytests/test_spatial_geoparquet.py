import pytest
from utils_pytest import *
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile

url = f"s3://{TEST_BUCKET}/test_geoparquet/data.parquet"


def test_geoparquet_no_extension(
    user_conn, superuser_conn, postgis_extension, geoparquet_file
):
    error = run_command(
        f"""
        DROP EXTENSION IF EXISTS pg_lake_spatial CASCADE;
        CREATE FOREIGN TABLE test_geoparquet () SERVER pg_lake OPTIONS (path '{url}');
    """,
        superuser_conn,
        raise_error=False,
    )
    assert "pg_lake_spatial extension is required" in error

    superuser_conn.rollback()


def test_geoparquet_detection(
    user_conn, superuser_conn, spatial_analytics_extension, geoparquet_file
):
    run_command(
        f"""
        CREATE FOREIGN TABLE test_geoparquet () SERVER pg_lake OPTIONS (path '{url}');
    """,
        superuser_conn,
    )

    superuser_conn.commit()

    # check our type detection based on our file creation
    res = run_query(
        """
        SELECT attname, atttypid::regtype::text FROM pg_attribute
        WHERE attrelid = 'test_geoparquet'::regclass AND attnum > 0 AND NOT attisdropped
        ORDER BY 1
    """,
        user_conn,
    )

    assert res == [
        ["col_bytea", "bytea"],
        ["col_geom1", "geometry"],
        ["col_geom2", "geometry"],
        ["id", "integer"],
    ]

    user_conn.rollback()


def test_geoparquet_round_trip(user_conn, spatial_analytics_extension, azure):
    url = f"az://{TEST_BUCKET}/test_geoparquet_round_trip/geo.parquet"

    run_command(
        f"""
        COPY (SELECT 1 as id, 'POINT(3 4)'::geometry geo) TO '{url}';
        CREATE FOREIGN TABLE test_geoparquet_round_trip () SERVER pg_lake OPTIONS (path '{url}');
    """,
        user_conn,
    )

    # check our type detection based on our file creation
    res = run_query(
        """
        SELECT attname, atttypid::regtype::text FROM pg_attribute
        WHERE attrelid = 'test_geoparquet_round_trip'::regclass AND attnum > 0 AND NOT attisdropped
        ORDER BY 1
    """,
        user_conn,
    )

    assert len(res) == 2
    assert res == [["geo", "geometry"], ["id", "integer"]]

    user_conn.rollback()


def test_geoparquet_typemod(user_conn, spatial_analytics_extension, pgduck_conn, azure):
    url = f"az://{TEST_BUCKET}/test_geoparquet_round_trip/geo.parquet"

    # generate a GeoParquet with a typemod
    run_command(
        f"""
        COPY (SELECT 1 as id, 'POINT(3 4)'::geometry(point, 4326) point, 'POINT(0 0)'::geometry geo) TO '{url}';
    """,
        user_conn,
    )

    # check that geometry_types is set
    result = run_query(
        f"""
        WITH columns_lookup AS (
          SELECT decode(value)->'columns' AS coljson
          FROM parquet_kv_metadata('{url}')
          WHERE decode(key) = 'geo'
          LIMIT 1
        ),
        column_names AS (
          SELECT UNNEST(json_keys(coljson)) AS colname
          FROM columns_lookup
        )
        SELECT colname, coljson->colname->>'encoding' as encoding, coljson->colname->'geometry_types'->>0 as geometry_types
        FROM columns_lookup, column_names
        ORDER BY 1
    """,
        pgduck_conn,
    )

    assert len(result) == 2
    assert result[0]["encoding"] == "WKB"
    assert result[0]["geometry_types"] == None
    assert result[1]["encoding"] == "WKB"
    assert result[1]["geometry_types"] == "point"

    user_conn.rollback()


@pytest.fixture(scope="module")
def geoparquet_file(s3):
    """Creates a geoparquet file with appropriately tagged metadata"""

    # We want to verify good/expected behavior with a number of scenarios:
    #
    # - multiple geometry columns
    # - column case normalization
    # - binary just treated as binary
    #
    # This file serves to set up these scenarios.  Note that we are concerned
    # only with the registered /types/ of the data and the data detection
    # itself; the binary data that is provided is not valid WKB, so will not
    # parse as Geometry values.

    metadata = {
        b"geo": json.dumps(
            {
                "version": "1.0.0",
                "primary_column": "col_geom1",
                "columns": {
                    "col_geom1": {
                        "encoding": "WKB",
                        "geometry_types": [],
                    },
                    "Col_Geom2": {
                        "encoding": "WKB",
                        "geometry_types": [],
                    },
                },
            }
        ).encode("utf-8")
    }

    schema = pa.schema(
        [
            ("id", pa.int32()),
            ("col_geom1", pa.binary()),
            ("col_geom2", pa.binary()),
            ("col_bytea", pa.binary()),
        ]
    )

    schema = schema.with_metadata(metadata)

    # Create a PyArrow Table with the schema
    data = [
        pa.array([1, 2, 3], type=pa.int32()),
        pa.array(
            [b"binary_data_1", b"binary_data_2", b"binary_data_3"], type=pa.binary()
        ),
        pa.array(
            [b"binary_data_4", b"binary_data_5", b"binary_data_6"], type=pa.binary()
        ),
        pa.array(
            [b"binary_data_10", b"binary_data_11", b"binary_data_12"], type=pa.binary()
        ),
    ]
    table = pa.Table.from_arrays(data, schema=schema)

    saved_path = tempfile.NamedTemporaryFile()
    pq.write_table(table, saved_path.name)

    s3.upload_file(saved_path.name, TEST_BUCKET, "test_geoparquet/data.parquet")

    yield
