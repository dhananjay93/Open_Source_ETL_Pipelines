
  
    

  create  table "airflow"."gold"."dim_customers__dbt_tmp"
  
  
    as
  
  (
    -- Gold dimension: anonymized customers with cleaned attributes

SELECT
  customer_hash,
  gender,
  age,
  tenure_months,
  monthly_charges,
  total_charges,
  contract_type,
  internet_service,
  has_tech_support,
  _loaded_at
FROM "airflow"."silver"."customers_clean"
  );
  