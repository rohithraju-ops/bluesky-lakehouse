
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "analytics"."main"."fct_posts_hourly"

where not(post_count >= 0)


  
  
      
    ) dbt_internal_test