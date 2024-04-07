{% test not_less_than_zero(model, column_name, ref_column) %}

{{ config(severity = 'warn') }}

with validation as (
    select
        {{ ref_column }}
        , {{ column_name }}
    from {{ model }}
),

validation_errors as (
    select *
    from validation
    where {{ column_name }} < 0
)

select *
from validation_errors

{% endtest %}