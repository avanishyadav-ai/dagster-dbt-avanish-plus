{{
  config(
    materialized = 'table',
    database     = 'DAGSTER_DBT_KIEWIT_DB',
    schema       = 'dbo',
    tags         = ['dbo', 'gold']
  )
}}


with customers as (
    select * from {{ ref('stg_customers') }}
)


select
    customer_id,
    full_name,
    first_name,
    last_name,
    name_initials,
    current_timestamp() as _updated_at
from customers
