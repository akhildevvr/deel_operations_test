{% test date_format(model, column_name) %}



SELECT COUNT(*) as cnt
FROM {{ model }}
WHERE NOT to_char({{ column_name }} , 'YYYY-MM-DD') <> '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
having cnt > 0



{% endtest %}