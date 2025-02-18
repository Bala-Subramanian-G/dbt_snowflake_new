-- models/landing_to_stage/stg_salesdetails.sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    transient=true

) }}

select
    *,
    SHA2(SALEDETAILID, 256) as load_hash_key,
    SHA2(
        concat(
            SALEID,
            PRODUCTID,
            QUANTITY,
            UNITPRICE,
            TOTALAMOUNT,
            DISCOUNT,
            TAX,
            COALESCE(ISACTIVE, 'UNKNOWN'),
            CREATEDAT,
            COALESCE(UPDATEDAT, '9999-01-01'),
            DISCOUNTPERCENT,
            TAXPERCENT,
            FINAL_PRODUCT_PRICE
        ),
        256
    ) as row_hash_key,
    current_timestamp as load_timestamp,
    current_date as load_date
from {{ source('landing_table', 'SALESDETAILS') }}
