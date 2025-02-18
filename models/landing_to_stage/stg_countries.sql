-- models/landing_to_stage/stg_countries.sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    transient=true
    ,pre_hook=[
        """
        truncate table {{ this }}
        """
    ]    
)}}

select
    *,
    SHA2(COUNTRYID, 256) as load_hash_key,
    SHA2(concat(COUNTRYNAME, COUNTRYCODE, COALESCE(ISACTIVE, 'UNKNOWN')), 256) as row_hash_key,
    current_timestamp as load_timestamp,
    current_date as load_date
from {{ source('landing_table', 'COUNTRIES') }}