-- models/landing_to_stage/stg_preferences.sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    transient=true

) }}

select
    *,
    SHA2(PREFERENCEID, 256) as load_hash_key,
    SHA2(
        concat(
            CUSTOMERID,
            COALESCE(PARTYID, -1),
            CHANNELID,
            COALESCE(CHANNELID, -1),
            COALESCE(OPTINSTATUS, 'UNKNOWN'),
            COALESCE(FREQUENCY, '-1'),
            COALESCE(LASTUPDATED, '9999-01-01'),
            COALESCE(NOTES, '-1')
        ),
        256
    ) as row_hash_key,
    current_timestamp as load_timestamp,
    current_date as load_date
from {{ source('landing_table', 'PREFERENCES') }}