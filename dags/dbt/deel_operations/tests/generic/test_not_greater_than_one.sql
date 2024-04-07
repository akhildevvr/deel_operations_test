{% test not_greater_than_one(model, column_name) %}

    {{ config(severity = 'error') }}



    WITH validation_errors as (
        select *
        from {{ model }}
        where {{ column_name }} > 1
    )

    select *
    from validation_errors

{% endtest %}