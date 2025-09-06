from __future__ import annotations
from datetime import datetime, timedelta
import os
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor


DATA_DIR = os.getenv("CUSTOMERS_DATA_DIR", "/opt/airflow/data")
CSV_FILENAME = os.getenv("CUSTOMERS_CSV", "customer_churn_data.csv")
TARGET_TABLE = os.getenv("BRONZE_TABLE", "bronze.customers_raw")

DEFAULT_ARGS = {
    "owner": "etl",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="load_customers_to_postgres",
    start_date=datetime(2025, 9, 1),
    schedule_interval=None,  
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "ingest", "postgres"],
) as dag:

    
    @task
    def show_filepath() -> str:
        path = str(Path(DATA_DIR) / CSV_FILENAME)
        print(f"👀 Looking for file at: {path}")
        return path


    wait_for_csv = FileSensor(
        task_id="wait_for_csv",
        fs_conn_id="fs_default",
        filepath="/opt/airflow/data/customer_churn_data.csv",  # relative to /opt/airflow
        poke_interval=30,
        timeout=60 * 60,  # 1 hour
        mode="poke",
        soft_fail=False,
    )

    @task
    def create_bronze_schema_and_table():
        """Create the bronze schema and raw table if not present.
        Assumes Postgres connection id = 'postgres_default'
        """
        pg = PostgresHook(postgres_conn_id="postgres_default")
        create_schema = """
        BEGIN;
        DROP SCHEMA IF EXISTS bronze CASCADE;
        CREATE SCHEMA bronze AUTHORIZATION airflow;
        COMMIT;
        """
        # Column names match your dbt/silver model inputs
        create_table = f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            customerid TEXT,
            age INT,
            gender TEXT,
            tenure INT,
            monthlycharges NUMERIC,
            contracttype TEXT,
            internetservice TEXT,
            totalcharges NUMERIC,
            techsupport TEXT,
            churn TEXT,
            _loaded_at TIMESTAMP DEFAULT NOW()
        );
        """
        with pg.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(create_schema)
                cur.execute(create_table)
                conn.commit()

    @task
    def truncate_table():
        pg = PostgresHook(postgres_conn_id="postgres_default")
        pg.run(f"TRUNCATE TABLE {TARGET_TABLE};")

    @task
    def load_csv_to_postgres():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id="postgres_default")

        copy_sql = """
            COPY bronze.customers_raw(
                customerid, age, gender, tenure, monthlycharges,
                contracttype, internetservice, totalcharges, techsupport, churn
            )
            FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"');
        """

        csv_path = "/opt/airflow/data/customer_churn_data.csv"
        pg.copy_expert(sql=copy_sql, filename=csv_path)   # <-- pass path string here

    
    @task(trigger_rule="all_success")
    def print_success():
        print("✅ DAG run completed successfully: load_customers_to_postgres")

    chain(
        show_filepath(),
        wait_for_csv,
        create_bronze_schema_and_table(),
        truncate_table(),
        load_csv_to_postgres(),
        print_success(),
    )