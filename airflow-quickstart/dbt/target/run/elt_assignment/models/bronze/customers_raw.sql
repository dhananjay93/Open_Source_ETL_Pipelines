
  
    

  create  table "airflow"."bronze"."customers_raw__dbt_tmp"
  
  
    as
  
  (
    -- Reference table created directly by Airflow
-- dbt will just treat this as a source
select * from bronze.customers_raw
  );
  