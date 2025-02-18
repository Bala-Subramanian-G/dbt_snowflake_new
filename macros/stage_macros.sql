{% macro stage_columns(table_name) %}
    {%- set sql_query -%}
        SELECT COLUMN_NAME
        FROM DEV_DB.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'POC_STAGE'
          AND TABLE_NAME = '{{ table_name.upper() }}'
          AND COLUMN_NAME NOT IN ('LOAD_HASH_KEY','ROW_HASH_KEY','LOAD_TIMESTAMP','LOAD_DATE')
        ORDER BY ORDINAL_POSITION
    {%- endset -%}

    {# Execute the SQL query and fetch results #}
    {%- set results = run_query(sql_query) -%}
    {%- set columns = [] -%}

    {%- if results is not none -%}
        {%- for row in results -%}
            {%- do columns.append(row[0]) -%}
        {%- endfor -%}
    {%- endif -%}

    {# Join all column names with the delimiter ', ' and return the result #}
    {{ columns | join(', ') }}
{% endmacro %}