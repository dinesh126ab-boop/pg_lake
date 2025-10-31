import pytest
import psycopg2
from utils_pytest import *


# The aim of this test is to make sure that we can read
# deletion files generated via spark, see spark_generated_iceberg_test()
def test_iceberg_modified_table(pg_conn, spark_generated_iceberg_test):
    iceberg_table_folder = (
        iceberg_sample_table_folder_path() + "/public/spark_generated_iceberg_test"
    )
    iceberg_table_metadata_location = (
        "s3://"
        + TEST_BUCKET
        + "/spark_test/public/spark_generated_iceberg_test/metadata/00009-5c29aedb-463b-4b80-b0d5-c1d7fc957770.metadata.json"
    )
    run_command(
        f"""
		create schema spark_gen_cp;
		create table spark_gen_cp.spark_generated_iceberg_test () WITH (load_from='{iceberg_table_metadata_location}')
	""",
        pg_conn,
    )
    result = run_query(
        """
	    select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'spark_gen_cp.spark_generated_iceberg_test'::regclass and attnum > 0 order by attnum
	""",
        pg_conn,
    )
    assert len(result) == 1
    assert result == [["id", "bigint"]]

    result = run_query(
        "SELECT count(*) FROM spark_gen_cp.spark_generated_iceberg_test", pg_conn
    )
    assert result[0][0] == 110

    # we deleted row 3
    result = run_query(
        "SELECT count(*) from spark_gen_cp.spark_generated_iceberg_test WHERE id = 3;",
        pg_conn,
    )
    assert result[0][0] == 0

    result = run_query(
        "SELECT id, count(*) from spark_gen_cp.spark_generated_iceberg_test GROUP BY id ORDER BY 2 DESC, 1 DESC LIMIT 4",
        pg_conn,
    )
    assert len(result) == 4
    assert result[0][0] == 5 and result[0][1] == 4
    assert result[1][0] == 4 and result[1][1] == 4
    assert result[2][0] == 2 and result[2][1] == 4
    assert result[3][0] == 1 and result[3][1] == 4

    run_command("DROP SCHEMA spark_gen_cp CASCADE", pg_conn)
    pg_conn.commit()
