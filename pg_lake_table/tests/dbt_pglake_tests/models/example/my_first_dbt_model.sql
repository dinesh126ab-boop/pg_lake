
{{ config(
    materialized='table',
    pre_hook="SET default_table_access_method TO 'iceberg'; SET pg_lake_iceberg.default_location_prefix = 's3://testbucketcdw/{{ random_string() }}';",
) }}

with source_data as (

    select 1 as id, now()::timestamp as event_time
    union all
    select 2 as id, now()::timestamp as event_time

)

select *
from source_data
