
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    lang_code as unique_field,
    count(*) as n_records

from "analytics"."main"."dim_languages"
where lang_code is not null
group by lang_code
having count(*) > 1



  
  
      
    ) dbt_internal_test