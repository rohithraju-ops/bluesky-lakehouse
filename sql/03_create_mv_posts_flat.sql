-- A flattened, projected view of the raw bsky_raw stream.
-- This is the MV we'll sink to Iceberg today. Unlike the aggregating MV from
-- the previous materialized view (posts_per_minute_by_lang), this one is purely INSERT-only — each
-- incoming event produces exactly one output row, never updating existing
-- rows. That makes it trivially compatible with an append-only Iceberg sink.

CREATE MATERIALIZED VIEW IF NOT EXISTS posts_flat AS
SELECT
    did,
    time_us,
    to_timestamp(time_us / 1000000) AS event_time,
    (commit).record.text                       AS text,
    COALESCE((commit).record.langs[1], 'unknown') AS primary_lang,
    (commit).record."createdAt"                AS created_at
FROM bsky_raw
WHERE kind = 'commit'
  AND (commit).operation = 'create'
  AND (commit).collection = 'app.bsky.feed.post';