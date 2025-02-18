-- models/landing_to_stage/stg_storedetails.sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    transient=true

) }}

select
    *,
    SHA2(STOREDETAILID, 256) as load_hash_key,
    SHA2(
        concat(
            LOCATIONID,
            STOREMANAGERID,
            STORENAME,
            COALESCE(STREETADDRESS, '-1'),
            COALESCE(STORESIZE, -1),
            STORETYPE,
            STORESTATUS,
            EMPLOYEECOUNT,
            COALESCE(OPENINGDATE, '9999-01-01'),
            COALESCE(LASTRENOVATION, '9999-01-01'),
            COALESCE(SALESPERFORMANCE, '-1'),
            COALESCE(CUSTOMERSATISFACTION, '-1'),
            STOREPHONE,
            STOREEMAIL,
            COALESCE(SALESAREANAME, '-1'),
            COALESCE(SALESAREANUMBER, '-1'),
            COALESCE(OPENINGHOURS, '-1'),
            COALESCE(ISACTIVE, 'UNKNOWN')
        ),
        256
    ) as row_hash_key,
    current_timestamp as load_timestamp,
    current_date as load_date
from {{ source('landing_table', 'STOREDETAILS') }}