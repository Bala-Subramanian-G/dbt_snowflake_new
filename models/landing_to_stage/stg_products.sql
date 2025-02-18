-- models/landing_to_stage/stg_products.sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    transient=true
) }}

select
    *,
    SHA2(PRODUCTID, 256) as load_hash_key,
    SHA2(
        concat(
            PRODUCTNAME,
            COALESCE(PARTNUMBER, '-1'),
            COALESCE(CATEGORY, '-1'),
            PRICE,
            COALESCE(QUANTITYINSTOCK, -1),
            COALESCE(MANUFACTURER, '-1'),
            COALESCE(WEIGHT, -1),
            COALESCE(DIMENSIONS, '-1'),
            ISACTIVE,
            DISCOUNT_PERCENTAGE,
            TAX_PERCENTAGE
        ),
        256
    ) as row_hash_key,
    current_timestamp as load_timestamp,
    current_date as load_date
from {{ source('landing_table', 'PRODUCTS') }}
