-- sql/10_lateness_measurement.sql
-- Measures how late events typically arrive relative to now().
-- Buckets the distribution by whole-second intervals.
-- Use this to justify the 10-second watermark in sql/05:
--   if p99 lateness is <10s, the watermark is appropriately tight.
--
-- Regular VIEW (not MATERIALIZED): RisingWave forbids now() in the SELECT clause
-- of a streaming MV. A plain view evaluates now() at query time instead.

CREATE VIEW event_lateness_hist AS
SELECT
    FLOOR(EXTRACT(EPOCH FROM (now() - event_time)))::INT AS lateness_seconds,
    COUNT(*) AS event_count
FROM bsky_raw
WHERE event_time > now() - INTERVAL '10 minutes'
GROUP BY FLOOR(EXTRACT(EPOCH FROM (now() - event_time)))::INT;
