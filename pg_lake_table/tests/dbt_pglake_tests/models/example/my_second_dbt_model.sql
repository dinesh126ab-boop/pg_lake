
{{ config(
    materialized='incremental',
    unique_key='id',
    pre_hook="SET default_table_access_method TO 'iceberg'; SET pg_lake_iceberg.default_location_prefix = 's3://testbucketcdw/{{ random_string() }}';",
) }}

select id, event_time
from {{ ref('my_first_dbt_model') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
  -- (If event_time is NULL or the table is truncated, the condition will always be true and load all records)
where event_time >= (select coalesce(max(event_time),'1900-01-01') from {{ this }} )

{% endif %}
