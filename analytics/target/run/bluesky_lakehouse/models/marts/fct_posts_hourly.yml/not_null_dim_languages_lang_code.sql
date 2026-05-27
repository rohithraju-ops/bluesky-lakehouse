
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select lang_code
from "analytics"."main"."dim_languages"
where lang_code is null



  
  
      
    ) dbt_internal_test