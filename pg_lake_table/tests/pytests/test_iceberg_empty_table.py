import pytest
from utils_pytest import *

from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    FloatType,
    DoubleType,
    StringType,
    BinaryType,
    FixedType,
    NestedField,
    MapType,
    StructType,
)


def test_empty_iceberg_table(pg_conn, extension, empty_bids_table):
    run_command(
        f"""
        create foreign table bids ()
        server pg_lake
        options (path '{empty_bids_table.metadata_location}', FORMAT 'ICEBERG')
    """,
        pg_conn,
    )

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'bids'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )

    assert len(result) == 8
    assert result[0] == ["datetime", "timestamp without time zone"]
    assert result[1] == ["symbol", "text"]
    assert result[2] == ["bid", "real"]
    assert result[3] == ["ask", "double precision"]
    assert result[4][0] == "details"
    assert result[4][1].startswith("lake_struct.created_by")
    assert result[5] == ["data", "bytea"]
    assert result[6] == ["fixed_data", "bytea"]
    assert result[7] == ["prx_map", "map_type.key_text_val_text"]

    pg_conn.rollback()


@pytest.fixture(scope="module")
def empty_bids_table(pg_conn, iceberg_catalog):
    create_map_type("int", "text")

    schema = Schema(
        NestedField(
            field_id=1, name="datetime", field_type=TimestampType(), required=False
        ),
        NestedField(field_id=2, name="symbol", field_type=StringType(), required=False),
        NestedField(field_id=3, name="bid", field_type=FloatType(), required=False),
        NestedField(field_id=5, name="ask", field_type=DoubleType(), required=False),
        NestedField(
            field_id=6,
            name="details",
            field_type=StructType(
                NestedField(
                    field_id=7,
                    name="created_by",
                    field_type=StringType(),
                    required=False,
                ),
            ),
            required=False,
        ),
        NestedField(field_id=8, name="data", field_type=BinaryType(), required=False),
        NestedField(
            field_id=9, name="fixed_data", field_type=FixedType(16), required=False
        ),
        NestedField(
            field_id=10,
            name="prx_map",
            field_type=MapType(
                key_id=11,
                key_type=StringType(),
                value_id=12,
                value_type=StringType(),
                value_required=False,
            ),
            required=False,
        ),
    )

    iceberg_table = iceberg_catalog.create_table(
        identifier="public.empty_bids_table",
        schema=schema,
        location=f"s3://{TEST_BUCKET}/iceberg/public/empty_bids_table",
    )

    return iceberg_table


def create_simple_external_table(pg_conn, iceberg_catalog):

    schema = Schema(
        NestedField(
            field_id=1, name="datetime", field_type=TimestampType(), required=False
        )
    )

    iceberg_table = iceberg_catalog.create_table(
        identifier="public.simple_external_table",
        schema=schema,
        location=f"s3://{TEST_BUCKET}/iceberg/public/simple_external_table",
    )

    return iceberg_table
