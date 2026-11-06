"""M0 smoke test: Airflow can run our CLI from its isolated venv and reach the warehouse.

Real DAGs (daily, revision watch, backfill, recovery) arrive in M8.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="smoke_check_db",
    schedule=None,  # manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["m0", "smoke"],
) as dag:
    BashOperator(
        task_id="sentinel_check_db",
        bash_command="/opt/sentinel-venv/bin/sentinel check-db",
    )
