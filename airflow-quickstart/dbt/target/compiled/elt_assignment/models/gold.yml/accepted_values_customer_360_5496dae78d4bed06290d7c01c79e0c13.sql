
    
    

with all_values as (

    select
        tech_support_label as value_field,
        count(*) as n_records

    from "airflow"."gold"."customer_360"
    group by tech_support_label

)

select *
from all_values
where value_field not in (
    'Yes','No','Unknown'
)


