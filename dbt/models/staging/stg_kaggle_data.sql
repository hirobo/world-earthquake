{{ config(materialized='view') }}

select *
from {{ source('staging','kaggle_data') }}

-- dbt build --select <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}