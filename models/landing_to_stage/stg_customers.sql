-- models/landing_to_stage/stg_customers.sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    transient=true
)}}

select
    *,
    SHA2(CUSTOMERID, 256) as load_hash_key,
    SHA2(
        concat(
            FIRSTNAME,
            LASTNAME,
            EMAILADDRESS,
            PHONENUMBER,
            COALESCE(DATEOFBIRTH, '9999-01-01'),
            COALESCE(PREFERREDCONTACTMETHOD, 'NOTHING'),
            COALESCE(LOYALTYPROGRAMSTATUS, 'UNKNOWN'),
            COALESCE(CUSTOMERSINCE, '9999-01-01'),
            COALESCE(OPTINMARKETING, 'UNKNOWN'),
            COALESCE(LASTPURCHASEDATE, '9999-01-01'),
            COALESCE(TOTALSPENT, -1),
            COALESCE(NOTES, 'NO DESCRIPTION')
        ), 256
    ) as row_hash_key,
    current_timestamp as load_timestamp,
    current_date as load_date
from {{ source('landing_table', 'CUSTOMERS') }}
