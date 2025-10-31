import pytest
import psycopg2
import time
import duckdb
import math
import os
import uuid
from utils_pytest import *


sanity_testdefs = [
    {
        "name": "no_exception",
        "select": "1 as a, 2 as b, 3 as c",
        "shape": ["a", "b", "c"],
        "comment": "Validate simple struct passes validate_shape",
        "exception": None,
    },
    {
        "name": "wrong_shape1",
        "select": "1 as a, 2 as b, 3 as c",
        "shape": {"a": 1, "b": 2, "c": 3},
        "comment": "complain if 'shape' is not a list",
        "exception": "shape is list",
    },
    {
        "name": "wrong_shape2",
        "select": "1 as a, 2 as b, 3 as c",
        "shape": None,
        "comment": "complain if 'shape' is not a list",
        "exception": "shape is list",
    },
    {
        "name": "wrong_shape3",
        "select": "1 as a, 2 as b, 3 as c",
        "shape": "astring",
        "comment": "complain if 'shape' is not a list",
        "exception": "shape is list",
    },
    {
        "name": "zero_column_table",
        "select": "1 as a, 2 as b, 3 as c",
        "shape": [],
        "comment": "complain if 'shape' is a zero-column table",
        "exception": "zero-column table",
    },
    {
        "name": "wrong_number_columns",
        "select": "1 as a, 2 as b, 3 as c",
        "shape": ["a", "b"],
        "comment": "complain if 'shape' has a different number of columns",
        "exception": "different number of columns",
    },
    {
        "name": "shape_column_data_type",
        "select": "1 as a, 2 as b, 3 as c",
        "shape": ["a", "b", ["column"]],
        "comment": "complain if 'shape' has a list at the column level",
        "exception": "column cannot be a list",
    },
    {
        "name": "column_different_names",
        "select": "1 as a, 2 as b, 3 as c",
        "shape": ["a", "b", "d"],
        "comment": "complain if column names do not match for simple scalar",
        "exception": "scalar column non-matching name",
    },
    {
        "name": "shape_column_implied_composite_type",
        "select": """1 as a, 2 as b, { 'd': 1 } as c""",
        "shape": ["a", "b", "c"],
        "comment": "complain if shape def implies scalar, but is composite",
        "exception": "scalar column expected non-composite type",
    },
    {
        "name": "shape_column_extended_name_matches",
        "select": "1 as a, 2 as b, 3 as c",
        "shape": [{"name": "a"}, "b", "c"],
        "comment": "verify 'name' attribute in column def",
        "exception": None,
    },
    {
        "name": "shape_column_extended_name_different",
        "select": "1 as a, 2 as b, 3 as c",
        "shape": [{"name": "q"}, "b", "c"],
        "comment": "complain if column name is different",
        "exception": "att:name",
    },
    {
        "name": "shape_column_extended_type_matches",
        "select": "1 as a, 2 as b, 3 as c",
        "shape": [{"name": "a", "type": "int4"}, "b", "c"],
        "comment": "verify column type checking",
        "exception": None,
    },
    {
        "name": "shape_column_extended_type_different",
        "select": "1 as a, 2 as b, 3 as c",
        "shape": [{"name": "a", "type": "text"}, "b", "c"],
        "comment": "complain if col type is different than expected",
        "exception": "att:type",
    },
    {
        "name": "shape_column_extended_typelike_matches",
        "select": "1 as a, 2 as b, 3 as c",
        "shape": [{"name": "a", "typelike": "int"}, "b", "c"],
        "comment": "validate typelike behavior",
        "exception": None,
    },
    {
        "name": "shape_column_extended_typelike_different",
        "select": "1 as a, 2 as b, 3 as c",
        "shape": [{"name": "a", "typelike": "text"}, "b", "c"],
        "comment": "typelike failure",
        "exception": "att:typelike",
    },
    {
        "name": "shape_column_extended_isarray_matches",
        "select": "[1,2] as a",
        "shape": [{"name": "a", "isarray": True}],
        "comment": "explicit matching isarray",
        "exception": None,
    },
    {
        "name": "shape_column_extended_isarray_different",
        "select": "1 as a",
        "shape": [{"name": "a", "isarray": True}],
        "comment": "isarray incorrect failure",
        "exception": "att:isarray",
    },
    {
        "name": "shape_cols_not_list",
        "select": """{ 'b': 1 } as a""",
        "shape": [{"name": "a", "cols": {}}],
        "comment": "pass non-cols to a cols field in shape",
        "exception": "cols is list",
    },
    {
        "name": "extra_args",
        "select": """{ 'b': 1 } as a""",
        "shape": [
            {
                "name": "a",
                "cols": [{"name": "b", "fargle-bargle": "jim-jam"}],
            }
        ],
        "comment": "fail on extra args to an extended field def",
        "exception": "no extra args",
    },
]


def test_sanity_empty_typeid(pg_conn, extension):

    # first check empty typeid disallowed
    with pytest.raises(Exception) as e:
        validate_shape(0, ["a", "b", "c"], conn=pg_conn)
    assert "empty typeid" in str(e.value)
    pg_conn.rollback()


@pytest.mark.parametrize(
    "desc", sanity_testdefs, ids=[desc["name"] for desc in sanity_testdefs]
)
def test_sanity_validate_validate_shape(pg_conn, duckdb_conn, desc, extension):
    """Sanity-check the expected handling of validate_shape()"""

    tableid = setup_testdef(desc, conn=pg_conn, duckdb_conn=duckdb_conn)
    assert tableid

    if desc["exception"]:
        with pytest.raises(Exception) as e:
            validate_shape(tableid, desc["shape"], conn=pg_conn)
        assert desc["exception"] in str(e.value)
    else:
        validate_shape(tableid, desc["shape"], conn=pg_conn)

    pg_conn.rollback()


def collect_table_data():
    folder_path = sampledata_filepath("")
    table_data = []
    for filename in os.listdir(folder_path):

        # csv is non trivial to parse due to parameters like delimiter
        if (
            os.path.isfile(os.path.join(folder_path, filename))
            and os.path.splitext(filename)[1] != ".csv"
        ):
            table_name = os.path.splitext(filename)[0]
            table_postfix = os.path.splitext(filename)[1]
            table_data.append((table_name, table_postfix, filename))
    return table_data


TABLE_DATA = collect_table_data()
ids = [table_data[0] for table_data in TABLE_DATA]


@pytest.mark.parametrize(
    "table_name,table_postfix,filename", collect_table_data(), ids=ids
)
def test_create_table_struct_from_sample_data(
    duckdb_conn, pg_conn, extension, s3, table_name, table_postfix, filename
):
    folder_path = sampledata_filepath("")
    unique_id = uuid.uuid4().hex
    local_file_path = os.path.join(folder_path, filename)
    file_key = f"{table_name}{table_postfix}"
    file_path = f"s3://{TEST_BUCKET}/{file_key}"

    s3.upload_file(local_file_path, TEST_BUCKET, file_key)

    sql_command = f"""
    CREATE FOREIGN TABLE {table_name}_{unique_id} () SERVER pg_lake OPTIONS (path '{file_path}')
    """
    run_command(sql_command, pg_conn)
    res = run_query("SELECT count(*) FROM " + table_name + "_" + unique_id, pg_conn)
    row_count = int(res[0][0])

    res = run_query("SELECT * FROM " + table_name + "_" + unique_id, pg_conn)

    assert len(res) == row_count

    pg_conn.commit()

    # We skip validating this table because duckdb detects the range of data in
    # an in field in question and uses the `usmallint` type for the generated
    # parquet, rather than the `integer` source.  While this is annoying, it's
    # not worth working around with something more complicated (like type
    # aliases), so just skip this table for now.
    #
    # covidtrials_gzip has trouble being imported as csv due to nested
    # structs/arrays and differences in how duckdb and postgres escape things.

    skip_table_names = ["hits", "covidtrials_gzip", "noaa"]

    try:
        # for parquet files, create a new parquet file based on this data and validate that the structures match
        if "parquet" in file_path and table_name not in skip_table_names:
            new_file_key = f"{table_name}_new{table_postfix}"
            run_command(
                f"COPY (SELECT * FROM {table_name}_{unique_id}) TO 's3://{TEST_BUCKET}/{new_file_key}'",
                pg_conn,
                raise_error=False,
            )

            duckdb_conn.execute(
                f"DESCRIBE SELECT * FROM read_parquet('s3://{TEST_BUCKET}/{file_key}')"
            )
            desc_old = duckdb_conn.fetchall()

            duckdb_conn.execute(
                f"DESCRIBE SELECT * FROM read_parquet('s3://{TEST_BUCKET}/{new_file_key}')"
            )
            desc_new = duckdb_conn.fetchall()

            assert [lower_list(l) for l in desc_old] == [
                lower_list(l) for l in desc_new
            ], "Saved file structure is the same as source"
    finally:
        pg_conn.rollback()


def lower_list(l):
    return list(map(lambda x: None if x is None else x.lower(), l))
