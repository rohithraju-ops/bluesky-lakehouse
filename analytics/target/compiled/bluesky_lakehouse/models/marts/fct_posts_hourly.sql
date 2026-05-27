-- analytics/models/marts/fct_posts_hourly.sql
-- Hourly post counts per language with rolling 24h totals.



WITH hourly AS (
    SELECT
        DATE_TRUNC('hour', event_time) AS hour_bucket,
        primary_lang,
        COUNT(*)                       AS post_count,
        AVG(text_length)               AS avg_text_length,
        SUM(CASE WHEN text_length > 200 THEN 1 ELSE 0 END) AS long_posts
    FROM "analytics"."main"."stg_posts"
    GROUP BY DATE_TRUNC('hour', event_time), primary_lang
)
SELECT
    hour_bucket,
    primary_lang,
    post_count,
    avg_text_length,
    long_posts,
    SUM(post_count) OVER (
        PARTITION BY primary_lang
        ORDER BY hour_bucket
        ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    ) AS rolling_24h_count
FROM hourly