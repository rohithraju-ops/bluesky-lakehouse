-- analytics/models/staging/stg_posts.sql
-- Read from Iceberg via DuckDB iceberg_scan. Light cleanup:
--   - filter out harness test markers
--   - filter out null text
--   - add text_length derived column



SELECT
    did,
    time_us,
    event_time,
    text,
    primary_lang,
    created_at,
    LENGTH(text) AS text_length
FROM iceberg_scan(
    's3://lakehouse/warehouse/bsky/posts',
    allow_moved_paths = true
)
WHERE text IS NOT NULL
  AND did NOT LIKE 'harness:%'