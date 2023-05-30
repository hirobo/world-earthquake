{{ config(
  materialized='incremental',
  unique_key='id',
  on_schema_change='fail',
  merge_behavior='upsert',
  partition_by = {
     'field': 'properties_time', 
     'data_type': 'timestamp',
     'granularity': 'month'
   }) 
}}
SELECT 
  *
  EXCEPT (
    is_valid,
    valid_from,
    valid_to,
    hash_value
  )
FROM
  {{ ref('stg_usgs') }}
WHERE
  is_valid is true

{% if is_incremental() %}
  AND properties_updated > (select max(properties_updated) from {{ this }})
{% endif %}
-- dbt build --select <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}