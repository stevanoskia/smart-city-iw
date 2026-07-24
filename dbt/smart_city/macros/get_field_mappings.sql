{% macro get_field_mappings(stream_name) %}
    {#-
      Read the active field mappings for a stream from the metadata config schema
      (config.field_mappings), ordered by `ordinal`. Returns a list of agate rows with
      columns source_expr / data_type / target_column — the contract the build_staging
      macro turns into a typed SELECT.

      run_query only runs during execution (compile/run), not parsing — so guard on
      `execute` and return an empty list at parse time (dbt parses models before it has a
      warehouse connection). At run time under Airflow the config DB is always reachable.
    -#}
    {% if execute %}
        {% set query %}
            select fm.source_expr, fm.data_type, fm.target_column
            from config.field_mappings fm
            join config.streams st on st.stream_id = fm.stream_id
            where st.stream_name = '{{ stream_name }}'
              and fm.is_active
            order by fm.ordinal
        {% endset %}
        {% set results = run_query(query) %}
        {% do return(results.rows) %}
    {% else %}
        {% do return([]) %}
    {% endif %}
{% endmacro %}
