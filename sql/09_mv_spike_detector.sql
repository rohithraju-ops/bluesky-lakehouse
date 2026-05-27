-- sql/09_mv_spike_detector.sql
-- Two-MV spike detector for per-language post rate.
--
-- Step 1: lang_post_rate_1m — 1-minute tumbling window counts per language.
-- Step 2: lang_spike_detector — self-join: compare each minute to the rolling
--   1-hour average for the same language. Flag minutes where count > 2× avg.
--
-- The self-join works because RisingWave maintains both sides as streaming state;
-- window a looks back at all b rows within the trailing 1-hour interval.

CREATE MATERIALIZED VIEW IF NOT EXISTS lang_post_rate_1m AS
SELECT
    window_start AS minute_bucket,
    COALESCE((commit).record.langs[1], 'unknown') AS lang,
    COUNT(*) AS posts_in_minute
FROM TUMBLE(bsky_raw, event_time, INTERVAL '1 minute')
WHERE (commit).operation  = 'create'
  AND (commit).collection = 'app.bsky.feed.post'
GROUP BY window_start, COALESCE((commit).record.langs[1], 'unknown');

CREATE MATERIALIZED VIEW IF NOT EXISTS lang_spike_detector AS
SELECT
    a.minute_bucket,
    a.lang,
    a.posts_in_minute,
    AVG(b.posts_in_minute) AS rolling_hr_avg,
    CASE
        WHEN a.posts_in_minute > 2 * AVG(b.posts_in_minute) THEN TRUE
        ELSE FALSE
    END AS is_spiking
FROM lang_post_rate_1m a
JOIN lang_post_rate_1m b
  ON  a.lang = b.lang
  AND b.minute_bucket BETWEEN a.minute_bucket - INTERVAL '1 hour'
                          AND a.minute_bucket - INTERVAL '1 minute'
GROUP BY a.minute_bucket, a.lang, a.posts_in_minute;
