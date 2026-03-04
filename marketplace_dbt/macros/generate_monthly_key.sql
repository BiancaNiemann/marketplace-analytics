{% macro generate_monthly_key(id_column, date_column) %}
    {{ dbt_utils.generate_surrogate_key(['CAST(' ~ id_column ~ ' AS STRING)', 'FORMAT_DATE("%Y-%m", ' ~ date_column ~ ')']) }}
{% endmacro %}