{{ config(materialized='table') }}


SELECT
  id,
  time,
  EXTRACT(YEAR
  FROM
    time) AS year,
  EXTRACT(MONTH
  FROM
    time) AS month,
  EXTRACT(DAYOFWEEK
  FROM
    time) AS day_of_week,
  place,
  type,
  latitude,
  longitude,
  depth,
  mag,
  magType,
  CASE
    WHEN REGEXP_CONTAINS(region, r'of (?:the )?(\w+ ?\w+) island') THEN REGEXP_EXTRACT(region, r'of (?:the )?(\w+ ?\w+) island')
    WHEN REGEXP_CONTAINS(region, r'(off|near)?\s?(the)?\s?\b(bay|coast|central|south|north|east|west|southeast|southwest|northwest|northeast|southeastern)( coast)? of\s?\b(south|north|west|east|southern|northern|western|eastern|central|southeastern)?\s?') THEN REGEXP_REPLACE(region, r'(off|near)?\s?(the)?\s?\b(bay|coast|central|south|north|east|west|southeast|southwest|northwest|northeast|southeastern)( coast)? of\s?\b(south|north|west|east|southern|northern|western|eastern|central|southeastern)?\s?', '')
    WHEN REGEXP_CONTAINS(region, r'sea of (\w+)') THEN REGEXP_EXTRACT(region, r'sea of (\w+)')
    WHEN CONTAINS_SUBSTR(region, "region") THEN REGEXP_EXTRACT(region, r'([\w\s-]+?)(?: border)?\sregion',1)
    WHEN place = "1960 Great Chilean Earthquake (Valdivia Earthquake)" THEN "chile"
  ELSE
  region
END
  AS region
FROM (
  SELECT
    id,
    time,
    place,
    type,
    latitude,
    longitude,
    depth,
    mag,
    magType,
    (
      CASE
        WHEN CONTAINS_SUBSTR(place, ",") THEN REGEXP_EXTRACT(LOWER(place), r',\s(.*?)$',1)
      ELSE
      LOWER(place)
    END
      ) AS region
  FROM
    {{ ref('stg_kaggle_data') }} )

-- dbt build --select <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}