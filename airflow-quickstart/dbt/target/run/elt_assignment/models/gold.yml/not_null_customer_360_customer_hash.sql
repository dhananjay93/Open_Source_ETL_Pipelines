
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_hash
from "airflow"."gold"."customer_360"
where customer_hash is null



  
  
      
    ) dbt_internal_test