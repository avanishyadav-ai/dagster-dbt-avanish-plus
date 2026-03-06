{{
  config(
    materialized = 'view',
    database     = 'DAGSTER_DBT_KIEWIT_DB_PLUS',
    schema       = 'staging',
    tags         = ['staging', 'silver']
  )
}}


with source as (
    select * from {{ ref('raw_customers') }}
),


cleaned as (
    select
        customer_id,
        initcap(trim(full_name))                        as full_name,
        initcap(trim(
            split_part(trim(full_name), ' ', 1)
        ))                                              as first_name,
        nullif(
            initcap(trim(
                case
                    when position(' ' in trim(full_name)) > 0
                    then substr(
                        trim(full_name),
                        position(' ' in trim(full_name)) + 1
                    )
                    else ''
                end
            )),
            ''
        )                                               as last_name,
        upper(left(split_part(trim(full_name), ' ', 1), 1))
            || coalesce(
                upper(left(
                    nullif(
                        substr(
                            trim(full_name),
                            position(' ' in trim(full_name)) + 1
                        ), ''
                    ), 1
                )),
                ''
            )                                           as name_initials
    from source
    where customer_id is not null
      and full_name   is not null
),


deduped as (
    select *,
        row_number() over (
            partition by customer_id
            order by customer_id
        ) as _rn
    from cleaned
)

select
    customer_id, full_name, first_name,
    last_name, name_initials
from deduped
where _rn = 1
