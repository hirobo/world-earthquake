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
  id,
  type,
  properties_mag,
  properties_place,
  TIMESTAMP_MILLIS(properties_time) AS properties_time,
  TIMESTAMP_MILLIS(properties_updated) AS properties_updated,
  properties_tz,
  properties_url,
  properties_detail,
  properties_felt,
  properties_cdi,
  properties_mmi,
  properties_alert,
  properties_status,
  properties_tsunami,
  properties_sig,
  properties_net,
  properties_code,
  properties_ids,
  properties_sources,
  properties_types,
  properties_nst,
  properties_dmin,
  properties_rms,
  properties_gap,
  properties_magType AS properties_mag_type,
  properties_type,
  properties_title,
  geometry_type,
  geometry_longitude,
  geometry_latitude,
  geometry_altitude,
  is_valid,
  valid_from,
  valid_to,
  hash_value
FROM 
  {{ source('earthquake_raw','usgs_data') }} 
{% if is_incremental() %}
  WHERE TIMESTAMP_MILLIS(properties_updated) > (select max(properties_updated) from {{ this }})
{% endif %}
-- dbt build --select <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}