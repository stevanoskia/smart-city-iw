{% macro build_staging(stream_name) %}
    {#-
      Config-driven staging model builder — the "same engine" that generates every stg_*
      model. Emits the identical typed SELECT the hand-written stg models used to, but from
      config.field_mappings: for each active field, `source_expr [::data_type] as target_column`
      (see config/schema.sql + config/README.md).

      raw_id / extracted_at are emitted as a fixed header: they are Airbyte-managed
      (_airbyte_raw_id / _airbyte_extracted_at), always present, and not part of the API
      contract — so they live here, not in config.field_mappings. Fields follow, in `ordinal`
      order. Turning a field off (is_active=false) drops its column; adding one is an INSERT.
    -#}
    {%- set mappings = get_field_mappings(stream_name) -%}
with source as (
    select * from {{ source('staging', stream_name) }}
),

renamed as (
    select
        _airbyte_raw_id        as raw_id,
        _airbyte_extracted_at  as extracted_at
        {%- for row in mappings %}
        , {{ row['source_expr'] }}{% if row['data_type'] %}::{{ row['data_type'] }}{% endif %} as {{ row['target_column'] }}
        {%- endfor %}
    from source
)

select * from renamed
{% endmacro %}
