import pytest
import psycopg2
import time
import duckdb
import math
from utils_pytest import *


@pytest.fixture(scope="module")
def create_typename_funcs(superuser_conn, app_user):
    run_command(
        f"""
        CREATE OR REPLACE FUNCTION duckdb_type_by_name(name text) RETURNS int LANGUAGE C STRICT
        AS 'pg_lake_copy', $$duckdb_type_by_name$$;
        CREATE OR REPLACE FUNCTION duckdb_name_by_type(type int) RETURNS text LANGUAGE C STRICT
        AS 'pg_lake_copy', $$duckdb_name_by_type$$;

        GRANT EXECUTE ON FUNCTION duckdb_type_by_name(text) TO {app_user};
        GRANT EXECUTE ON FUNCTION duckdb_name_by_type(int) TO {app_user};
    """,
        superuser_conn,
    )

    superuser_conn.commit()

    yield

    run_command(
        """
    DROP FUNCTION duckdb_name_by_type(int);
    DROP FUNCTION duckdb_type_by_name(text);
    """,
        superuser_conn,
    )

    superuser_conn.commit()


@pytest.fixture(scope="module")
def create_parse_struct_function(superuser_conn, app_user):
    # Create test function to analyze parsing
    run_command(
        f"""
        CREATE OR REPLACE FUNCTION duckdb_parse_struct(query text, OUT name text, OUT type text, OUT typeid int, OUT isarray boolean, OUT level int)
        RETURNS setof record LANGUAGE C STRICT
        AS 'pg_lake_copy', $$duckdb_parse_struct$$;
        GRANT EXECUTE ON FUNCTION duckdb_parse_struct(text) TO {app_user};
    """,
        superuser_conn,
    )

    superuser_conn.commit()

    yield

    superuser_conn.rollback()

    run_command(
        """
    DROP FUNCTION duckdb_parse_struct(text);
    """,
        superuser_conn,
    )

    superuser_conn.commit()


@pytest.fixture(scope="module")
def cqe(superuser_conn, app_user):
    # Create test function to analyze parsing
    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_engine CASCADE;
        GRANT lake_read_write TO {app_user};
    """,
        superuser_conn,
    )

    superuser_conn.commit()

    yield

    run_command(
        """
        DROP EXTENSION pg_lake_engine CASCADE;
    """,
        superuser_conn,
    )

    superuser_conn.commit()


type_aliases = {
    "BIGINT": [
        "BIGINT",
        "INT8",
        "LONG",
        "bigint",
        "int8",
        "long",
    ],
    "BIT": [
        "BIT",
        "BITSTRING",
        "bit",
        "bitstring",
    ],
    "BLOB": [
        "BLOB",
        "BYTEA",
        "BINARY",
        "VARBINARY",
        "blob",
        "bytea",
        "binary",
        "varbinary",
    ],
    "BOOLEAN": [
        "BOOLEAN",
        "BOOL",
        "LOGICAL",
        "boolean",
        "bool",
        "logical",
    ],
    "DATE": [
        "DATE",
        "date",
    ],
    "DECIMAL": [
        "DECIMAL",
        "NUMERIC",
        "decimal",
        "numeric",
    ],
    "DOUBLE": [
        "DOUBLE",
        "FLOAT8",
        "double",
        "float8",
    ],
    "ENUM": [
        "ENUM",
        "enum",
    ],
    "REAL": [
        "FLOAT",
        "FLOAT4",
        "REAL",
        "float",
        "float4",
        "real",
    ],
    "HUGEINT": [
        "HUGEINT",
        "hugeint",
    ],
    "INTEGER": [
        "INTEGER",
        "INT4",
        "INT",
        "SIGNED",
        "integer",
        "int4",
        "int",
        "signed",
    ],
    "INTERVAL": [
        "INTERVAL",
        "interval",
    ],
    "JSON": [
        "JSON",
        "json",
    ],
    "LIST": [
        "LIST",
        "list",
    ],
    "MAP": [
        "MAP",
        "map",
        "MAP(INT, VAL)",
        "map( FLOAT4, BPCHAR )",
        "MAP(LIST(TEXT),ARRAY(BIGINT))",
    ],
    "SMALLINT": [
        "SMALLINT",
        "INT2",
        "SHORT",
        "smallint",
        "int2",
        "short",
    ],
    "STRUCT": [
        "STRUCT",
        "struct",
        "STRUCT(a STRING, b INTEGER)",
        "sTrUCt( a INT1, num ENUM )",
    ],
    # "TIMESTAMP_MS": [ "TIMESTAMP_MS", "timestamp_ms", ], # mapped to timestamp
    # "TIMESTAMP_NS": [ "TIMESTAMP_NS", "timestamp_ns", ], # mapped to timestamp
    # "TIMESTAMP_S": [ "TIMESTAMP_S", "timestamp_s", ], # mapped to timestamp
    "TIMESTAMP": [
        "TIMESTAMP",
        "DATETIME",
        "TIMESTAMP WITHOUT TIME ZONE",
        "timestamp",
        "datetime",
        "timestamp without time zone",
        "TIMESTAMP_MS",
        "timestamp_ms",
        "TIMESTAMP_MS",
        "timestamp_ms",
        "TIMESTAMP_NS",
        "timestamp_ns",
        "TIMESTAMP_S",
        "timestamp_s",
    ],
    "TIMESTAMP WITH TIME ZONE": [
        "TIMESTAMPTZ",
        "TIMESTAMP WITH TIME ZONE",
        "timestamptz",
        "timestamp with time zone",
    ],
    "TIME": ["TIME", "time", "TIME WITHOUT TIME ZONE", "time without time zone"],
    "TIMETZ": ["TIMETZ", "timetz", "TIME WITH TIME ZONE", "time with time zone"],
    "TINYINT": [
        "TINYINT",
        "INT1",
        "tinyint",
        "int1",
    ],
    "UBIGINT": [
        "UBIGINT",
        "ubigint",
    ],
    "UINTEGER": [
        "UINTEGER",
        "uinteger",
    ],
    "UNION": [
        "UNION",
        "union",
    ],
    "USMALLINT": [
        "USMALLINT",
        "usmallint",
    ],
    "UTINYINT": [
        "UTINYINT",
        "utinyint",
    ],
    "UUID": [
        "UUID",
        "uuid",
    ],
    "VARCHAR": [
        "VARCHAR",
        "CHAR",
        "BPCHAR",
        "TEXT",
        "STRING",
        "varchar",
        "char",
        "bpchar",
        "text",
        "string",
    ],
}


@pytest.mark.parametrize(
    "expected_type, variants", type_aliases.items(), ids=type_aliases.keys()
)
def test_type_mapping(pg_conn, create_typename_funcs, expected_type, variants):
    """Test the internal type mapping functionality of the duckdb type maps
    (used for STRUCT parsing, so at least nominally okay here)."""

    # Create test function to analyze parsing
    print(f"Testing variant type mapping for {expected_type}")
    for variant_type in variants:
        res = run_query(
            f"SELECT duckdb_name_by_type(duckdb_type_by_name('{variant_type}')) type",
            pg_conn,
        )
        assert (
            res[0]["type"] == expected_type
        ), f"'{variant_type}' got mapped back to '{expected_type}'"

    pg_conn.rollback()


test_struct_cases = [
    ("simple_struct", "STRUCT(x INTEGER, y INTEGER)", [["x", 0], ["y", 0]]),
    (
        "simple_nested",
        "STRUCT(x INTEGER, y INTEGER, z STRUCT(a VARCHAR, b VARCHAR ))",
        [["x", 0], ["y", 0], ["z", 0], ["a", 1], ["b", 1]],
    ),
]


@pytest.mark.parametrize(
    "test_id, struct_expression, expected_expression",
    test_struct_cases,
    ids=[ids_list[0] for ids_list in test_struct_cases],
)
def test_struct_parsing(
    pg_conn,
    create_parse_struct_function,
    test_id,
    struct_expression,
    expected_expression,
):
    print(f"testing {test_id}")
    # add more struct parsing tests
    res = run_query(
        f"""SELECT name, level from duckdb_parse_struct('{struct_expression}')""",
        pg_conn,
    )
    assert res == expected_expression

    pg_conn.rollback()


#### Verify struct parsing ####

struct_type_testdefs = [
    {
        "name": "non_struct",
        "select": "1 as a, 2 as b, 3 as c",
        "shape": ["a", "b", "c"],
        "comment": "Simple validation that non-structs work",
    },
    {
        "name": "simple_struct_1",
        "select": "{ 'x': 1, 'y': 2, 'z': 3 } as structTypes",
        "shape": [
            {"name": "structtypes", "typelike": "x_y_z_", "cols": ["x", "y", "z"]}
        ],
        "comment": "Verify we can pull columns from a top-level struct type.  Also verify the type name is combo of the attribute names",
    },
    {
        "name": "simple_struct_2",
        "select": "{ 'x': 'test', 'y': 2, 'z': 1.2 } as structTypes",
        "shape": [
            {
                "name": "structtypes",
                "cols": [
                    {"name": "x", "type": "text"},
                    {"name": "y", "type": "int4"},
                    {"name": "z", "type": "numeric"},
                ],
            }
        ],
        "comment": "Validate we can pull column type from a top-level struct type",
    },
    {
        "name": "quoted_names_1",
        "select": """{ 'Spaces And Things': 2, 'Something With τ υ φ χ ψ ω ϊ ϋ ό ύ ώ': 3 } as structTypes""",
        "shape": [
            {
                "name": "structtypes",
                "cols": ["Spaces And Things", "Something With τ υ φ χ ψ ω ϊ ϋ ό ύ ώ"],
            }
        ],
        "comment": "Test some odd quoting things and unicode characters",
    },
    {
        "name": "multibyte_1",
        "select": """{ 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaτ': 1 } as structTypes""",
        "shape": [
            {
                "name": "structtypes",
                "cols": [
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaτ"
                ],
            }
        ],
        "comment": """The next few tests determine that UTF-8 multibyte
        splitting is working correctly.  We start with a 60 chars ending in
        a final UTF-8 character and gradually increase the number of chars
        so we'd iterate through the split point.  If it does not error out
        then we successfully truncated the name without splitting the UTF-8
        char and getting an error from the database.""",
    },
    {
        "name": "multibyte_2",
        "select": """{ 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaτ': 1 } as structTypes""",
        "shape": [
            {
                "name": "structtypes",
                "cols": [
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaτ"
                ],
            }
        ],
        "comment": """See notes for multibyte_1.""",
    },
    {
        "name": "multibyte_3",
        "select": """{ 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaτ': 1 } as structTypes""",
        "shape": [
            {
                "name": "structtypes",
                "cols": [
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaτ"
                ],
            }
        ],
        "comment": """See notes for multibyte_1.""",
    },
    # {
    #     'name': 'multibyte_4',
    #     'select': """{ 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaτ': 1 } as structTypes""",
    #     'shape': [{ 'name' : 'structtypes',
    #                 'cols' : ['aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaτ'] }],
    #     'comment': """Going higher results in truncated data in
    # pg_attribute for multibyte characters, which makes psycopg2 sad.  This
    # is ostensibly not our bug, so leaving this commented out for now.  See
    # notes for multibyte_1.""", },
    {
        "name": "multi_struct_1",
        "select": "{ 'x': 1, 'y': 2, 'z': 3 } as structType1, { 'a': 4, 'b': 5 } as structType2",
        "shape": [
            {"name": "structtype1", "cols": ["x", "y", "z"]},
            {"name": "structtype2", "cols": ["a", "b"]},
        ],
        "comment": "Include multiple struct types in our select",
    },
    {
        "name": "long_names",
        "select": """
            { 'really_really_really_really': 1,'i_mean_really_really_really_really': 2, 'really_really_really_really_really_really_really': 3 } as structType1,
            { 'really_really_really_really': 1,'i_mean_really_really_really_really': 2, 'really_really_really_really_really_really_really_really': 3 } as structType2
        """,
        "shape": [
            {
                "name": "structtype1",
                "cols": [
                    "really_really_really_really",
                    "i_mean_really_really_really_really",
                    "really_really_really_really_really_really_really",
                ],
            },
            {
                "name": "structtype2",
                "cols": [
                    "really_really_really_really",
                    "i_mean_really_really_really_really",
                    "really_really_really_really_really_really_really_really",
                ],
            },
        ],
        "comment": """Test generation of type names exceeding NAMEDATALEN
        chars.  The expectation here is that we will not get a conflict
        since different structs should get different CRC hashes assigned for
        appending.""",
    },
    {
        "name": "nested_struct_1",
        "select": "{ 'x': { 'a': 1 }, 'y': 2, 'z': 3 } as structType1",
        "shape": [
            {"name": "structtype1", "cols": [{"name": "x", "cols": ["a"]}, "y", "z"]}
        ],
        "comment": "Include a nested struct types in our select",
    },
    {
        "name": "nested_struct_2",
        "select": """{
            'person': { 'first_name': 'Jimi', 'last_name': 'Hendrix' },
            'primary_instrument': 'guitar',
            'knows' : { 'first_name': 'Robert', 'last_name' : 'Plant' },
            'also_knows': { 'last_name': 'Joplin', 'first_name': 'Janis' },
            'might_know': {
                'last_name': 'Clapton',
                'first_name': 'Eric',
                'hates': { 'first_name': 'Bob', 'last_name': 'Dylan' },
                'why': 'Who knows?',
            },
        } as structType1""",
        "shape": [
            {
                "name": "structtype1",
                "cols": [
                    {"name": "person", "cols": ["first_name", "last_name"]},
                    {"name": "primary_instrument"},
                    {"name": "knows", "cols": ["first_name", "last_name"]},
                    {"name": "also_knows", "cols": ["last_name", "first_name"]},
                    {
                        "name": "might_know",
                        "cols": [
                            "last_name",
                            "first_name",
                            {"name": "hates", "cols": ["first_name", "last_name"]},
                            "why",
                        ],
                    },
                ],
            }
        ],
        "comment": """This test validates repetitive structs with the same
        name at multiple levels.  It also tests/verifies column order of
        structs is as defined in the literal SELECT.""",
    },
    ## validate_shape() needs to be taught about domains and how to resolve those into the underlying base types in order for this to work properly; for now just skip this.
    # {
    #     'name': 'map_value',
    #     'select': """{ 'mymap': MAP { 1: 'a', 2: 'b'} } mystruct""",
    #     'shape': [
    #         {'name': 'mystruct',
    #          'cols': [{'name': 'mymap',
    #                    'cols': [
    #                        {'name': 'key', 'type': 'integer' },
    #                        {'name': 'val', 'type': 'text' }]
    #                   }]
    #         },
    #     ]
    # },
    {
        "name": "nested_types_1",
        "select": """{ 'numbers': [1,2,3] } as struct_of_list,
            [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}] as list_of_struct,
            { 'points': [ {'x': 1, 'y': 2}, {'x': 3, 'y': 4}] } as struct_of_list_of_struct,
            [['a','b','c'],['d','e','f']] as list_of_lists,
            array_value({'a': 1, 'b': 2}, {'a': 3, 'b': 4}) as array_of_struct,
            array_value(NULL::text) as array_of_null,
            array_value(1,null,3,4) as array_of_int, /* note the lower-case null here */
            {'foo': array_value(NULL, {'bar': 'baz'}, {'bar': nUlL }), 'x,y,z': [1,2,3]} as mixed_struct_array_list_nulls,
        """,
        "shape": [
            {
                "name": "struct_of_list",
                "cols": [{"name": "numbers", "type": "integer", "isarray": True}],
            },
            {"name": "list_of_struct", "isarray": True, "cols": ["a", "b"]},
            {
                "name": "struct_of_list_of_struct",
                "cols": [{"name": "points", "isarray": True, "cols": ["x", "y"]}],
            },
            {"name": "list_of_lists", "isarray": True},
            {
                "name": "array_of_struct",
                "isarray": True,
                "typelike": "a_b_",
                "cols": ["a", "b"],
            },
            {"name": "array_of_null", "isarray": True, "type": "text"},
            {"name": "array_of_int", "isarray": True, "type": "integer"},
            {
                "name": "mixed_struct_array_list_nulls",
                "isarray": False,
                "typelike": "foo_x,y,z",
                "cols": [
                    {
                        "name": "foo",
                        "isarray": True,
                        "typelike": "bar_",
                        "cols": ["bar"],
                    },
                    {
                        "name": "x,y,z",
                        "isarray": True,
                        "type": "integer",
                    },
                ],
            },
        ],
        "comment": """Test a variety of nested types""",
    },
]


@pytest.mark.parametrize(
    "desc", struct_type_testdefs, ids=[desc["name"] for desc in struct_type_testdefs]
)
def test_create_table_struct(duckdb_conn, pg_conn, desc, cqe):

    tableid = setup_testdef(desc, conn=pg_conn, duckdb_conn=duckdb_conn)
    assert tableid

    validate_shape(tableid, desc["shape"], conn=pg_conn)

    pg_conn.rollback()


def test_map_array(pg_conn, duckdb_conn, cqe):
    # create a basic scalar type
    err = create_map_type("integer", "text", raise_error=False)
    assert err == None

    # verify we cannot create a map with array keys
    err = create_map_type("integer[]", "text", raise_error=False)

    assert "arrays cannot be used as the key type" in err

    # verify we can create a map with array values
    err = create_map_type("integer", "text[]", raise_error=False)

    assert err is None

    # verify we cannot create a map with array keys and array values
    err = create_map_type("integer[]", "text[]", raise_error=False)

    assert "arrays cannot be used as the key type" in err

    # Validate lack of array support for map values
    mapurl = f"s3://{TEST_BUCKET}/test_map_array/data.parquet"

    run_command(
        f"""
    COPY (SELECT MAP {{ 1: [1,2,3], 2: [3,5], 3: [9,4,2] }} mymap) to '{mapurl}';
    """,
        duckdb_conn,
    )

    err = run_command(
        f"""
    CREATE TABLE mymap () WITH (load_from='{mapurl}');
    """,
        pg_conn,
        raise_error=False,
    )

    assert err is None

    pg_conn.rollback()


def test_struct_map_child_lookup(pg_conn, duckdb_conn, cqe):
    # reproduce crash in old repo issues/385

    mapurl = f"s3://{TEST_BUCKET}/test_struct_map_child/data.parquet"

    # create out data basic scalar type
    run_command(
        f"""
    COPY (SELECT {{ 'int': 1, 'map': MAP {{ 'a': 1, 'b': 2 }} }} mystruct) to '{mapurl}'
    """,
        duckdb_conn,
    )

    err = run_command(
        f"""
    CREATE TABLE mymap () WITH (load_from='{mapurl}');
    """,
        pg_conn,
        raise_error=False,
    )

    assert err == None

    # lookup map for existing value
    res = run_query(
        f"""
    SELECT (mystruct).map->'a' from mymap
    """,
        pg_conn,
    )

    assert res == [[1]]

    # lookup map for non-existing value
    res = run_query(
        f"""
    SELECT (mystruct).map->'c' from mymap
    """,
        pg_conn,
    )

    assert res == [[None]]

    pg_conn.rollback()
