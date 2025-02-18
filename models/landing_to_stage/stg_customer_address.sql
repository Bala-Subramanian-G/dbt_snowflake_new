-- models/landing_to_stage/stg_customer_address.sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    transient=true

)}}

select
    *,
    SHA2(ADDRESSID, 256) as load_hash_key,
    SHA2(
        concat(
            COALESCE(CUSTOMERID, -1),
            COALESCE(STREETADDRESS, 'NO DESCRIPTION'),
            COALESCE(STATEID, -1),
            COALESCE(CITYID, -1),
            COALESCE(COUNTYID, -1),
            COALESCE(COUNTRYID, -1),
            COALESCE(POSTALCODE, '00000'),
            COALESCE(ADDRESSTYPE, 'NO DESCRIPTION'),
            COALESCE(ISPRIMARY, 'UNKNOWN'),
            COALESCE(ISACTIVE, 'UNKNOWN')
        ), 256
    ) as row_hash_key,
    current_timestamp as load_timestamp,
    current_date as load_date
from {{ source('landing_table', 'CUSTOMER_ADDRESS') }}