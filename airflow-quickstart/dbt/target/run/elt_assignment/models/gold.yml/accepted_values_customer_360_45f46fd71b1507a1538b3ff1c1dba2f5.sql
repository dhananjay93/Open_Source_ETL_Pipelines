
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        tenure_band as value_field,
        count(*) as n_records

    from "airflow"."gold"."customer_360"
    group by tenure_band

)

select *
from all_values
where value_field not in (
    'New ( <3m )','4-12m','12-24m','24-36m','36m+'
)



  
  
      
    ) dbt_internal_test