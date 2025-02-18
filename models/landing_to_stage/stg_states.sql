-- models/landing_to_stage/stg_states.sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    transient=true

) }}

select
    *,
    SHA2(STATEID, 256) as load_hash_key,
    SHA2(
        concat(
            STATENAME,
            COALESCE(COUNTRYID, -1),
            COALESCE(ISACTIVE, 'UNKNOWN')
        ),
        256
    ) as row_hash_key,
    current_timestamp as load_timestamp,
    current_date as load_date
from {{ source('landing_table', 'STATES') }}
