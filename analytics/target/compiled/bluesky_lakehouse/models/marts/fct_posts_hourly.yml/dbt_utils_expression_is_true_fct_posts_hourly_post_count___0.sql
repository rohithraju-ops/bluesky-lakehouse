



select
    1
from "analytics"."main"."fct_posts_hourly"

where not(post_count >= 0)

