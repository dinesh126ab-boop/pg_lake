import pytest
import psycopg2
from utils_pytest import *


# get some text, url encode
# create a local file with the encoded name
# push a file to s3 with the encoded name
# such that we ensure the encoded names are
# compatible with local file system and s3
def test_url_encoded_file_names(
    create_helper_functions, superuser_conn, s3, iceberg_extension
):

    test_cases = [
        (
            "  ..!!~~**''(());;//??::@@&&==  $$,,##",
            "%20%20..%21%21%7e%7e%2a%2a%27%28%28%29%29%3b%3b%2f%2f%3f%3f%3a%3a%40%40%26%26%3d%3d%20%20%24%24%2c%2c%23%23",
        ),
        (
            "Special- Schema!_With.Multiple_Uses_Of@Chars#-Here~And*Here!",
            "Special-%20Schema%21_With.Multiple_Uses_Of%40Chars%23-Here%7eAnd%2aHere%21",
        ),
        (
            "Special-Table!_With.Multiple_Uses_Of@Chars#-Here~And*Here!",
            "Special-Table%21_With.Multiple_Uses_Of%40Chars%23-Here%7eAnd%2aHere%21",
        ),
    ]

    for text_to_encode, expected_output in test_cases:
        encoded_filename = assert_url_encoding(
            text_to_encode, expected_output, superuser_conn
        )
        encoded_local_file_path = "/tmp/" + encoded_filename

        # the content of the file is not that interesting for this
        # test, mostly for sanity checking that we can read/write
        # both local and s3 files with the encoded file names
        data_in_file = f"Test content for {text_to_encode}"

        # Write to a file with the encoded name
        with open(encoded_local_file_path, "w") as file:
            file.write(data_in_file)

        # Read from the file to ensure it was written correctly
        with open(encoded_local_file_path, "r") as file:
            content = file.read()
            assert content == f"Test content for {text_to_encode}"

        # Upload to S3 and verify the contents
        ensure_can_copy_to_file(
            encoded_local_file_path, encoded_filename, s3, data_in_file, superuser_conn
        )

        # Clean up by removing the local file after the test
        os.remove(encoded_local_file_path)


def assert_url_encoding(input_text, expected_output, superuser_conn):
    result = run_query(
        f"SELECT lake_iceberg.url_encode_path('{input_text}')", superuser_conn
    )
    assert result[0][0] == expected_output
    return result[0][0]


def ensure_can_copy_to_file(local_filepath, filename, s3, data_in_file, superuser_conn):
    location = f"s3://{TEST_BUCKET}/local_push/{filename}"
    command = f"SELECT lake_iceberg.pg_lake_copy_file('{local_filepath}', '{location}')"
    run_command(command, superuser_conn)

    returned_bytes = read_s3_operations(s3, location)
    assert returned_bytes == data_in_file


@pytest.fixture(scope="module")
def create_helper_functions(superuser_conn):

    run_command(
        f"""
        CREATE OR REPLACE FUNCTION lake_iceberg.url_encode_path(metadataUri TEXT)
        RETURNS text
         LANGUAGE C
         IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$url_encode_path$function$;
        CREATE OR REPLACE FUNCTION lake_iceberg.pg_lake_copy_file(localFile TEXT, s3Uri text)
        RETURNS void
         LANGUAGE C
         IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$pg_lake_copy_file$function$;

""",
        superuser_conn,
    )

    yield

    # Teardown: Drop the function after the test(s) are done
    run_command(
        f"""
        DROP FUNCTION lake_iceberg.url_encode_path;
         DROP FUNCTION lake_iceberg.pg_lake_copy_file;

""",
        superuser_conn,
    )
