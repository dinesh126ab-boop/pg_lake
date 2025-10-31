import pytest
import psycopg2
import time
import duckdb
import math
from utils_pytest import *
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    FloatType,
    DoubleType,
    StringType,
    NestedField,
    StructType,
)
import pyarrow


def test_iceberg_definition_from(superuser_conn, cities_table):
    run_command(
        f"""
		create table cities () with (definition_from = '{cities_table.metadata_location}')
	""",
        superuser_conn,
    )

    result = run_query(
        """
		select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'cities'::regclass and attnum > 0 order by attnum
	""",
        superuser_conn,
    )
    assert len(result) == 3
    assert result == [
        ["city", "text"],
        ["lat", "double precision"],
        ["long", "double precision"],
    ]

    superuser_conn.rollback()


def test_iceberg_invalid_metadata(superuser_conn, cities_table):
    invalid_json_url = (
        f"s3://{TEST_BUCKET}/test_iceberg_definition_from/notreally.metadata.json"
    )

    error = run_command(
        f"""
		copy (select 'world' AS hello) to '{invalid_json_url}' with (format 'json');
		create table invalid () with (definition_from = '{invalid_json_url}')
	""",
        superuser_conn,
        raise_error=False,
    )
    assert "ERROR:  missing field in Iceberg table metadata: format-version" in error

    superuser_conn.rollback()


def test_iceberg_load_from(superuser_conn, cities_table):
    run_command(
        f"""
		create table cities () with (load_from = '{cities_table.metadata_location}', format = 'iceberg')
	""",
        superuser_conn,
    )

    result = run_query("select * from cities order by city", superuser_conn)
    assert len(result) == 4
    assert result[0]["city"] == "Amsterdam"

    superuser_conn.rollback()


def test_iceberg_copy(superuser_conn, cities_table):
    run_command(
        f"""
		create table cities (id bigserial, city text, lat double precision, long double precision);
		copy cities (city, lat, long) from '{cities_table.metadata_location}';
		copy cities (city, lat, long) from '{cities_table.metadata_location}' with (format 'iceberg');
	""",
        superuser_conn,
    )

    result = run_query("select * from cities order by city, id", superuser_conn)
    assert len(result) == 8
    assert result[0]["id"] == 1
    assert result[0]["city"] == "Amsterdam"

    error = run_command(
        f"""
		copy cities (city, lat, long) to '{cities_table.metadata_location}';
	""",
        superuser_conn,
        raise_error=False,
    )
    assert "COPY TO in Iceberg format is not supported" in error

    superuser_conn.rollback()


@pytest.fixture(scope="module")
def iceberg_catalog(superuser_conn, s3):
    run_command(
        f"""
        create extension if not exists pg_lake_iceberg cascade;
	""",
        superuser_conn,
    )

    catalog = create_iceberg_test_catalog(superuser_conn)
    yield catalog
    catalog.engine.dispose()


@pytest.fixture(scope="module")
def cities_table(iceberg_catalog):
    schema = Schema(
        NestedField(1, "city", StringType(), required=False),
        NestedField(2, "lat", DoubleType(), required=False),
        NestedField(3, "long", DoubleType(), required=False),
    )

    iceberg_table = iceberg_catalog.create_table(
        identifier="public.cities",
        schema=schema,
        location=f"s3://{TEST_BUCKET}/iceberg/public/cities",
    )

    data = pyarrow.Table.from_pylist(
        [
            {"city": "Amsterdam", "lat": 52.371807, "long": 4.896029},
            {"city": "San Francisco", "lat": 37.773972, "long": -122.431297},
            {"city": "Drachten", "lat": 53.11254, "long": 6.0989},
            {"city": "Paris", "lat": 48.864716, "long": 2.349014},
        ],
    )

    iceberg_table.append(data)

    return iceberg_table
