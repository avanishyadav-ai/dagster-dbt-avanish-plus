{{
  config(
    materialized = 'table',
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'lz',
    tags         = ['lz', 'bronze']
  )
}}


select
    id                  as customer_id,
    name                as full_name,
    current_timestamp() as _loaded_at,
    'customer.csv' as _source_file
from {{ source('raw_customers', 'CUSTOMER') }}
