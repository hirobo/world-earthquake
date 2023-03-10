{{ config(materialized='table') }}

SELECT
  time,
  EXTRACT(YEAR from time) as year,
  EXTRACT(MONTH from time) as month,
  EXTRACT(DAYOFWEEK from time) as day_of_week,
  place,
  type,
  latitude,
  longitude,
  CASE
    WHEN CONTAINS_SUBSTR(place, ",") THEN REGEXP_EXTRACT(place, r',\s(.*?)$',1)
    WHEN NOT CONTAINS_SUBSTR(place, ",") THEN REGEXP_EXTRACT(place, r'\s([\w]*)$',1)
  ELSE
  NULL
END
  AS state
FROM 
{{ ref('stg_kaggle_data') }}

-- dbt build --select <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}