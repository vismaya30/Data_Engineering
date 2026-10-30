import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = os.getenv("PROJECT_DIR", "/opt/airflow/project")
DBT_BIN = os.getenv("DBT_BIN", "dbt")
START_DATE = datetime.fromisoformat(os.getenv("PIPELINE_START_DATE", "2026-10-01"))

default_args = {
    "owner": "data-engineering",
    "depends_on_past": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def python_job(task_id: str, module: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd '{PROJECT_DIR}' && python -m {module} --run-date {{{{ ds }}}}",
    )


with DAG(
    dag_id="payments_daily",
    description="Generate, clean, validate, reconcile and model daily card payments",
    start_date=START_DATE,
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["payments", "reconciliation"],
) as dag:
    generate_bronze = python_job("generate_bronze", "src.generator.generate")
    build_silver = python_job("build_silver", "src.transforms.silver")
    reconcile = python_job("reconcile", "src.transforms.reconciliation")
    load_warehouse = python_job("load_warehouse", "src.transforms.load_warehouse")
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd '{PROJECT_DIR}/dbt' && '{DBT_BIN}' build --profiles-dir .",
    )

    generate_bronze >> build_silver >> reconcile >> load_warehouse >> dbt_build
