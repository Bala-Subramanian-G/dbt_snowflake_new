-- models/landing_to_stage/stg_communicationchannel.sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    transient=true
) }}

select
    *,
    SHA2(CHANNELID, 256) as load_hash_key,
    SHA2(concat(
        CHANNELNAME, 
        CHANNELTYPE, 
        COALESCE(DESCRIPTION, 'NO DESCRIPTION'), 
        COALESCE(ISACTIVE, 'UNKNOWN'), 
        COALESCE(CREATEDAT, '9999-01-01'), 
        COALESCE(UPDATEDAT, '9999-01-01')
    ), 256) as row_hash_key,
    current_timestamp as load_timestamp,
    current_date as load_date
from {{ source('landing_table', 'COMMUNICATIONCHANNEL') }}