{% macro get_column_names(location, file_format) %}
{% if execute %}
	{% set query %}
  		SELECT listagg(expression || ' AS ' || COLUMN_NAME, ',') 
  		FROM TABLE(
    		INFER_SCHEMA(
      		LOCATION=> '{{ location }}'
      		, FILE_FORMAT=> '{{ file_format }}'
      		)
    		)
	{% endset %}

	{% set result = run_query(query) %}
	{% set cols = result.rows[0][0] %}

	{{ cols }}

{% endif %}

{% endmacro %}