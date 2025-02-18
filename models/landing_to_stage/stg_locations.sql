-- models/landing_to_stage/stg_locations.sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    transient=true

)}}

select
    *,
    SHA2(LOCATIONID, 256) as load_hash_key,
    SHA2(
        concat(
            STATEID,
            CITYID,
            COUNTYID,
            COUNTRYID,
            COALESCE(POSTALCODE, '00000'),
            COALESCE(LATITUDE, -999),
            COALESCE(LONGITUDE, -999),
            COALESCE(DIVISION, '-1'),
            COALESCE(DISTRICT, '-1'),
            COALESCE(REGION, '-1')
        ), 256
    ) as row_hash_key,
    current_timestamp as load_timestamp,
    current_date as load_date
from {{ source('landing_table', 'LOCATIONS') }}
