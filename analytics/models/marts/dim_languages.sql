-- analytics/models/marts/dim_languages.sql
-- Language dimension: first/last seen, total posts, distinct users.

{{ config(materialized='table') }}

SELECT
    primary_lang                AS lang_code,
    MIN(event_time)             AS first_seen,
    MAX(event_time)             AS last_seen,
    COUNT(*)                    AS total_posts,
    COUNT(DISTINCT did)         AS distinct_users
FROM {{ ref('stg_posts') }}
WHERE primary_lang IS NOT NULL
GROUP BY primary_lang
