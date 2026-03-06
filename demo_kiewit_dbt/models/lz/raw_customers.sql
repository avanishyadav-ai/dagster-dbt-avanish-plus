{{
  config(
    materialized = 'table',
    database     = 'DAGSTER_DBT_KIEWIT_DB_PLUS',
    schema       = 'lz',
    tags         = ['lz', 'bronze']
  )
}}


select
    id                  as customer_id,
    name                as full_name,
    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', current_timestamp()) as _loaded_at,
    'customer.csv' as _source_file
from {{ ref('customer') }}


