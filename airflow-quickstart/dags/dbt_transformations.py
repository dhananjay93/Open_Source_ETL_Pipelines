from __future__ import annotations
from datetime import datetime
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = os.getenv("DBT_DIR", "/opt/airflow/dbt")
PROFILE_DIR = os.getenv("DBT_PROFILES_DIR", DBT_DIR)

ENV_PATH_PREFIX = 'export PATH="$PATH:/home/airflow/.local/bin"'

with DAG(
    dag_id="dbt_transformations",
    description="Run dbt deps → run → test for the mounted dbt project",
    start_date=datetime(2025, 9, 1),
    catchup=False,
    tags=["dbt", "silver", "gold"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f'{ENV_PATH_PREFIX} && '
            f'cd {DBT_DIR} && DBT_PROFILES_DIR="{PROFILE_DIR}" dbt deps'
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f'{ENV_PATH_PREFIX} && '
            f'cd {DBT_DIR} && DBT_PROFILES_DIR="{PROFILE_DIR}" '
            f'dbt run --project-dir . --profiles-dir "{PROFILE_DIR}"'
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f'{ENV_PATH_PREFIX} && '
            f'cd {DBT_DIR} && DBT_PROFILES_DIR="{PROFILE_DIR}" '
            f'dbt test --project-dir . --profiles-dir "{PROFILE_DIR}"'
        ),
    )

    dbt_deps >> dbt_run >> dbt_test
