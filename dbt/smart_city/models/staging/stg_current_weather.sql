-- Config-driven: columns are generated from config.field_mappings (stream 'current_weather')
-- by the build_staging macro. Add/rename/disable a field with SQL against config.*, not here.
{{ build_staging('current_weather') }}
