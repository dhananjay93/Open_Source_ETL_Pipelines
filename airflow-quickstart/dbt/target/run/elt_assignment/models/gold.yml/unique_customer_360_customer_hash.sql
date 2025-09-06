
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    customer_hash as unique_field,
    count(*) as n_records

from "airflow"."gold"."customer_360"
where customer_hash is not null
group by customer_hash
having count(*) > 1



  
  
      
    ) dbt_internal_test