-- sql/08_mv_trending_hashtags_1h_hop.sql
-- 1-hour hopping window sliding every 5 minutes.
-- At any point 12 overlapping 1-hour windows are open simultaneously, giving
-- a "rolling popularity" view rather than the discrete 5-min buckets above.
-- Useful for smoothing out bursty spikes in a single window.

-- Same CTE pattern as sql/07: HOP also requires a named table, not a subquery.
CREATE MATERIALIZED VIEW trending_hashtags_1h_hop AS
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
    window_start,
    window_end,
    LOWER(match) AS hashtag,
    COUNT(*)        AS mentions
FROM HOP(hashtag_events, event_time, INTERVAL '5 minutes', INTERVAL '1 hour')
GROUP BY window_start, window_end, LOWER(match);
