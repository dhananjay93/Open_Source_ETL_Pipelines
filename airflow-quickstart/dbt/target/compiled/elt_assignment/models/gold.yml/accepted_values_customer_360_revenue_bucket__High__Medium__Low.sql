
    
    

with all_values as (

    select
        revenue_bucket as value_field,
        count(*) as n_records

    from "airflow"."gold"."customer_360"
    group by revenue_bucket

)

select *
from all_values
where value_field not in (
    'High','Medium','Low'
)


