-- models/landing_to_stage/stg_party.sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    transient=true

)}}

select
    *,
    SHA2(PARTYID, 256) as load_hash_key,
    SHA2(
        concat(
            PARTYNAME,
            COALESCE(PARTYTYPE, '-1'),
            CUSTOMERID,
            COALESCE(CREATIONDATE, '1-1-9999'),
            COALESCE(NUMBEROFMEMBERS, -1),
            COALESCE(ACTIVESTATUS, 'UNKNOWN'),
            PARTYCONTACTPHONE,
            PARTYCONTACTEMAIL,
            COALESCE(OPTINMARKETING, 'UNKNOWN'),
            COALESCE(PARTYPREFERENCES, '-1'),
            COALESCE(SPECIALEVENTPARTICIPATION, 'UNKNOWN'),
            COALESCE(DISCOUNTELIGIBILITY, 'UNKNOWN'),
            COALESCE(LASTEVENTDATE, '1-1-9999'),
            COALESCE(NEXTEVENTDATE, '1-1-9999')
        ), 256
    ) as row_hash_key,
    current_timestamp as load_timestamp,
    current_date as load_date
from {{ source('landing_table', 'PARTY') }}