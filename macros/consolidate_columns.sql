{% macro consolidate_columns(schema, table) -%}

	{%- call statement('new_columns', fetch_result=True) -%}

	    SELECT distinct split_part(column_name, '__', 1)
	    FROM information_schema.columns
	    WHERE table_schema = '{{schema}}'
	    	AND table_name = '{{table}}'
	    	AND column_name LIKE '%\\_\\_%'

	{%- endcall -%}

	{%- call statement('existing_columns', fetch_result=True) -%}

	    SELECT column_name
	    FROM information_schema.columns
	    WHERE table_schema = '{{schema}}'
	    	AND table_name = '{{table}}'
	    	AND column_name NOT LIKE '%\\_\\_%'
	    	AND column_name not in (
	    		    SELECT distinct split_part(column_name, '__', 1)
				    FROM information_schema.columns
				    WHERE table_schema = '{{schema}}'
				    	AND table_name = '{{table}}'
				    	AND column_name LIKE '%\\_\\_%'
	    		)

	{%- endcall -%}

	{%- set new_columns = load_result('new_columns')['data'] | map(attribute=0)-%}
	{%- set existing_columns = load_result('existing_columns')['data'] | map(attribute=0)-%}
	
	select

	    {% for column in existing_columns | list -%}

	    "{{column}}" ,

	    {% endfor %}

		{% for column in new_columns | list -%}

			{%- call statement('new_column_parts', fetch_result=True) -%}

			    SELECT column_name
			    FROM information_schema.columns
			    WHERE table_schema = '{{schema}}'
			    	AND table_name = '{{table}}'
			    	AND split_part(column_name, '__', 1) = '{{column}}'
			
			{%- endcall -%}

			{%- set new_column_parts = load_result('new_column_parts')['data'] | map(attribute=0)-%}

				nvl(
					{% for part in new_column_parts | list -%} 
					"{{part}}"::varchar(128) 
					{% if not loop.last %} , {% endif %} 
					{% endfor %}
					) as "{{column}}"
			{% if not loop.last %} , {% endif %}
		{% endfor %}

from "{{schema}}"."{{table}}"

{%- endmacro %}

