-- Config-driven: columns are generated from config.field_mappings (stream 'traffic_flow')
-- by the build_staging macro. Add/rename/disable a field with SQL against config.*, not here.
{{ build_staging('traffic_flow') }}
