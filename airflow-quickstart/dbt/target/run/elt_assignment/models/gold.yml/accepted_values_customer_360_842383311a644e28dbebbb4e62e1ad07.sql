
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        churn_status as value_field,
        count(*) as n_records

    from "airflow"."gold"."customer_360"
    group by churn_status

)

select *
from all_values
where value_field not in (
    'Churned','Active','Unknown'
)



  
  
      
    ) dbt_internal_test