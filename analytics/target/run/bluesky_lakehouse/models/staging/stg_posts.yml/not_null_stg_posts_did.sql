
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select did
from "analytics"."main"."stg_posts"
where did is null



  
  
      
    ) dbt_internal_test