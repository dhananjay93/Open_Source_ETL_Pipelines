CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

drop table if exists bronze.customers_raw;

CREATE TABLE IF NOT EXISTS bronze.customers_raw (
    CustomerID       INT PRIMARY KEY,
    Age              INT,
    Gender           VARCHAR(10),
    Tenure           INT,
    MonthlyCharges   NUMERIC(10,2),
    ContractType     VARCHAR(20),
    InternetService  VARCHAR(20),
    TotalCharges     NUMERIC(10,2),
    TechSupport      VARCHAR(5),   -- Yes / No
    Churn            VARCHAR(5)    -- Yes / No
);

TRUNCATE TABLE bronze.customers_raw;

COPY bronze.customers_raw
FROM '/data/customer_churn_data.csv'
WITH (FORMAT csv, HEADER true);