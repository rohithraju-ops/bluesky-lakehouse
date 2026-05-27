
    
    

select
    lang_code as unique_field,
    count(*) as n_records

from "analytics"."main"."dim_languages"
where lang_code is not null
group by lang_code
having count(*) > 1


