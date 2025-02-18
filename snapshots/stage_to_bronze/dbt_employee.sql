-- snapshots/stage_to_bronze/dbt_employee.sql
{% snapshot dbt_employee %}
    {{
        config(
            unique_key="load_hash_key",
            strategy="check",
            check_cols=["row_hash_key"]
        )
    }}

with cte as
(
    select *,row_number() over(partition by load_hash_key order by load_timestamp desc) as row_count 
    from {{ ref('stg_employee') }} 
)
select
        {{ stage_columns('STG_EMPLOYEE') }},
        to_timestamp_ntz(current_timestamp()) as dbt_process_datetime,
        load_hash_key, row_hash_key
        from cte
        where row_count = 1
        order by 1

{% endsnapshot %}
