{% macro get_tables_in_schema(schema_name, database_name=target.database, table_pattern='%', exclude='') %}
    
    {% set tables=dbt_utils.get_relations_by_pattern(
        schema_pattern=schema_name,
        database=database_name,
        table_pattern=table_pattern,
        exclude=exclude
    ) %}


    {% set table_list= tables | map(attribute='identifier') %}

    {{ return(table_list | sort) }}

{% endmacro %}