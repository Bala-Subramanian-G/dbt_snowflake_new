-- models/landing_to_stage/stg_employee.sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    transient=true

)}}

select
    *,
    SHA2(EMPLOYEE_ID, 256) as load_hash_key,
    SHA2(
        concat(
            EMPLOYEE_NAME,
            COALESCE(GENDER, '-1'),
            COALESCE(DESIGNATION, '-1'),
            STORE_ID,
            COALESCE(JOINING_DATE, '1-1-9999'),
            COALESCE(SALARY, -1),
            COALESCE(NO_OF_LEAVES, -1),
            COALESCE(IS_ACTIVE, 'UNKNOWN')
        ), 256
    ) as row_hash_key,
    current_timestamp as load_timestamp,
    current_date as load_date
from {{ source('landing_table', 'EMPLOYEE') }}
