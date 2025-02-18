-- models/landing_to_stage/stg_sales.sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    transient=true

) }}

select
    *,
    SHA2(SALEID, 256) as load_hash_key,
    SHA2(
        concat(
            SALEID,
            LOCATIONID,
            CUSTOMERID,
            STORE_ID,
            SALEDATE,
            TOTALAMOUNT,
            COALESCE(PAYMENTMETHOD, '-1'),
            SALESTATUS,
            COALESCE(TOTALDISCOUNTAMOUNT, -1),
            INVOICENUMBER,
            INVOICEISSUEDATE,
            COALESCE(INVOICEDUEDATE, '9999-01-01'),
            COALESCE(INVOICESTATUS, 'NO DESCRIPTION'),
            COALESCE(ISACTIVE, 'UNKNOWN'),
            CREATEDAT,
            COALESCE(UPDATEDAT, '9999-01-01'),
            FINAL_AMOUNT,
            TAXAMOUNT
        ),
        256
    ) as row_hash_key,
    current_timestamp as load_timestamp,
    current_date as load_date
from {{ source('landing_table', 'SALES') }}