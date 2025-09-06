
  
    

  create  table "airflow"."silver"."customers_clean__dbt_tmp"
  
  
    as
  
  (
    WITH b AS (
  SELECT
    customerid,
    age,
    gender,
    tenure,
    monthlycharges,
    contracttype,
    internetservice,
    totalcharges,
    techsupport,
    churn
  FROM bronze.customers_raw
)
SELECT
  md5((customerid)::text) AS customer_hash,
  COALESCE(NULLIF(LOWER(gender), ''), 'unknown')   AS gender,
  COALESCE((age)::int, 0)                          AS age,
  GREATEST(COALESCE((tenure)::int, 0), 0)          AS tenure_months,
  COALESCE((monthlycharges)::numeric(10,2), 0.00)  AS monthly_charges,
  COALESCE(NULLIF(contracttype, ''), 'Unknown')    AS contract_type,
  COALESCE(NULLIF(internetservice, ''), 'Unknown') AS internet_service,
  COALESCE((totalcharges)::numeric(12,2), 0.00)    AS total_charges,
  CASE 
    WHEN LOWER(COALESCE(techsupport, '')) = 'yes' THEN TRUE
    WHEN LOWER(COALESCE(techsupport, '')) = 'no'  THEN FALSE
    ELSE NULL
  END  AS has_tech_support,
  CASE 
    WHEN LOWER(COALESCE(churn, '')) = 'yes' THEN TRUE
    WHEN LOWER(COALESCE(churn, '')) = 'no'  THEN FALSE
    ELSE NULL
  END  AS is_churned,
  CURRENT_TIMESTAMP AS _loaded_at
FROM b
  );
  