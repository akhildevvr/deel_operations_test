{% macro date_range_filter(column_name, start_date, end_date) %}
    {{ column_name }} BETWEEN '{{ start_date }}' AND '{{ end_date }}'
{% endmacro %}
