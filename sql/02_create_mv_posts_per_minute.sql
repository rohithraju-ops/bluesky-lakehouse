-- First materialized view: rolling per-minute, per-language post counts.
-- The SQL syntax is identical to what you'd write in Postgres. The semantics
-- are completely different: RisingWave doesn't run this query once and store
-- a snapshot. It builds a streaming dataflow that maintains this aggregate
-- continuously, updating row-by-row as new events arrive in bsky_raw.

CREATE MATERIALIZED VIEW IF NOT EXISTS posts_per_minute_by_lang AS
SELECT
    -- Truncate event-time to minute boundaries: grouping events into 1-minute windows
    date_trunc('minute', to_timestamp(time_us / 1000000)) AS minute_bucket,

    -- Extract first declared language from the langs array; NULL-safe
    -- Note: arrays are 1-indexed in RisingWave/Postgres. langs[0] would be NULL.
    COALESCE((commit).record.langs[1], 'unknown') AS lang,

    -- The aggregate. Engine maintains this as a delta-update counter.
    COUNT(*) AS post_count

FROM bsky_raw
WHERE kind = 'commit'                              -- skip identity/account events
  AND (commit).operation = 'create'                   -- skip updates and deletes
  AND (commit).collection = 'app.bsky.feed.post'      -- defensive: only posts

GROUP BY 1, 2;