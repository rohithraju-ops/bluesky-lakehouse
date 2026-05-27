
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hour_bucket
from "analytics"."main"."fct_posts_hourly"
where hour_bucket is null



  
  
      
    ) dbt_internal_test