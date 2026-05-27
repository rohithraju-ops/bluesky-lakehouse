-- sql/06_mv_posts_per_second.sql
-- 1-second tumbling window: instantaneous posts/sec rate gauge.
-- Streamlit polls this MV to drive the live throughput chart.
-- Each row represents one closed 1-second bucket; older buckets never change.

CREATE MATERIALIZED VIEW IF NOT EXISTS posts_per_second AS
SELECT
    window_start AS second_bucket,
    COUNT(*)     AS post_count
FROM TUMBLE(bsky_raw, event_time, INTERVAL '1 second')
GROUP BY window_start;
