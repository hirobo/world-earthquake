{{ config(
  materialized='incremental',
  unique_key='id',
  on_schema_change='fail',
  merge_behavior='upsert',
  partition_by = {
     'field': 'time', 
     'data_type': 'timestamp',
     'granularity': 'month'
   }) 
}}

SELECT
  id,
  time AS time,
  EXTRACT(YEAR
  FROM
    time) AS year,
  EXTRACT(MONTH
  FROM
    time) AS month,
  EXTRACT(DAYOFWEEK
  FROM
    time) AS day_of_week,
  updated,  
  type,
  latitude,
  longitude,
  altitude,
  mag,
  mag_type,
  CASE
    WHEN REGEXP_CONTAINS(place_orig, r'of (?:the )?(\w+ ?\w+) island') THEN REGEXP_EXTRACT(place_orig, r'of (?:the )?(\w+ ?\w+) island')
    WHEN REGEXP_CONTAINS(place_orig, r'(off|near)?\s?(the)?\s?\b(bay|coast|central|south|north|east|west|southeast|southwest|northwest|northeast|southeastern)( coast)? of\s?\b(south|north|west|east|southern|northern|western|eastern|central|southeastern)?\s?') THEN REGEXP_REPLACE(place_orig, r'(off|near)?\s?(the)?\s?\b(bay|coast|central|south|north|east|west|southeast|southwest|northwest|northeast|southeastern)( coast)? of\s?\b(south|north|west|east|southern|northern|western|eastern|central|southeastern)?\s?', '')
    WHEN REGEXP_CONTAINS(place_orig, r'sea of (\w+)') THEN REGEXP_EXTRACT(place_orig, r'sea of (\w+)')
    WHEN CONTAINS_SUBSTR(place_orig, "region") THEN REGEXP_EXTRACT(place_orig, r'([\w\s-]+?)(?: border)?\sregion',1)
    WHEN place_orig = "1960 great chilean earthquake (valdivia earthquake)" THEN "chile"
    WHEN place_orig = "ca" THEN "california"
  ELSE
  place_orig
  END
  AS place,
  place_orig
FROM (
  SELECT
    id,
    properties_time AS time,
    properties_updated AS updated,
    properties_type AS type,
    geometry_longitude AS longitude,
    geometry_latitude AS latitude,
    geometry_altitude AS altitude,
    properties_mag AS mag,
    properties_mag_type AS mag_type,
    (
      CASE
        WHEN CONTAINS_SUBSTR(properties_place, ",") THEN REGEXP_EXTRACT(LOWER(properties_place), r',\s(.*?)$',1)
      ELSE
      LOWER(properties_place)
    END
      ) AS place_orig
  FROM
    {{ ref('dwh_usgs') }}
  WHERE
    properties_time >= "1950-01-01"  
)

{% if is_incremental() %}
  WHERE updated > (select max(updated) from {{ this }})
{% endif %}
-- dbt build --select <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}