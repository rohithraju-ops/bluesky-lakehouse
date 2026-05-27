-- sql/07_mv_trending_hashtags.sql
-- Extract hashtags via regex, count per 5-minute tumbling window.
--
-- regexp_matches(text, pattern, 'g') returns SETOF VARCHAR[] (one array per match,
-- each array holds the capture groups). unnest() turns that set into rows.
-- match[1] extracts the first (and only) capture group — the hashtag text.
--
-- The spec has a bug: it references window_start outside a TUMBLE — fixed here
-- by wrapping the extraction subquery in TUMBLE(..., event_time, INTERVAL '5 minutes').

-- RisingWave requires TUMBLE's first arg to be a named source/CTE/view, not a
-- raw subquery. Use a CTE to name the hashtag extraction step.
CREATE MATERIALIZED VIEW IF NOT EXISTS trending_hashtags_5m AS
WITH hashtag_events AS (
    SELECT
        event_time,
        unnest(regexp_matches((commit).record.text, '#(\w+)', 'g')) AS match
    FROM bsky_raw
    WHERE (commit).operation  = 'create'
      AND (commit).collection = 'app.bsky.feed.post'
      AND (commit).record.text IS NOT NULL
)
SELECT
    window_start    AS bucket_start,
    LOWER(match) AS hashtag,
    COUNT(*)        AS mentions
FROM TUMBLE(hashtag_events, event_time, INTERVAL '5 minutes')
GROUP BY window_start, LOWER(match);
