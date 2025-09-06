{{ config(materialized='table') }}

SELECT
  cc.customer_hash,
  cc.age,
  cc.gender,
  CASE
    WHEN cc.age < 25 THEN 'Under 25'
    WHEN cc.age BETWEEN 25 AND 34 THEN '25-34'
    WHEN cc.age BETWEEN 35 AND 44 THEN '35-44'
    WHEN cc.age BETWEEN 45 AND 54 THEN '45-54'
    WHEN cc.age BETWEEN 55 AND 64 THEN '55-64'
    WHEN cc.age >= 65 THEN '65+'
    ELSE 'Unknown'
  END AS age_band,
  cc.tenure_months,
  CASE
    WHEN cc.tenure_months < 3     THEN 'New ( <3m )'
    WHEN cc.tenure_months < 12    THEN '4-12m'      -- NOTE: 3-month edge is excluded by design here
    WHEN cc.tenure_months < 24    THEN '12-24m'
    WHEN cc.tenure_months < 36    THEN '24-36m'
    ELSE '36m+'
  END AS tenure_band,
  cc.monthly_charges,
  cc.total_charges,
  cc.contract_type,
  cc.internet_service,
  cc.has_tech_support,
  CASE
    WHEN cc.has_tech_support IS TRUE  THEN 'Yes'
    WHEN cc.has_tech_support IS FALSE THEN 'No'
    ELSE 'Unknown'
  END AS tech_support_label,
  cc.is_churned,
  CASE
    WHEN cc.is_churned IS TRUE  THEN 'Churned'
    WHEN cc.is_churned IS FALSE THEN 'Active'
    ELSE 'Unknown'
  END AS churn_status,
  cc.monthly_charges AS arpu,
  (cc.monthly_charges * GREATEST(cc.tenure_months, 0))::NUMERIC(12,2) AS ltv_estimate,
  CASE
    WHEN cc.monthly_charges >= 100 THEN 'High'
    WHEN cc.monthly_charges >= 60  THEN 'Medium'
    ELSE 'Low'
  END AS revenue_bucket,
  cc._loaded_at
FROM "silver"."customers_clean" AS cc