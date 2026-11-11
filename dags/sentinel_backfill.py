"""Manual backfill (M8): process landing files for a date range, then reconcile.

Trigger with a config, e.g. {"start": "2025-11-01", "end": "2025-11-30"}. Bounded on
purpose: the range comes from the person triggering it, never from automation.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

CLI = "/opt/sentinel-venv/bin/sentinel"

with DAG(
    dag_id="sentinel_backfill",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "start": Param(type="string", format="date"),
        "end": Param(type="string", format="date"),
    },
    tags=["sentinel"],
) as dag:
    backfill = BashOperator(
        task_id="backfill",
        bash_command=(
            f"cd /opt/airflow/repo && {CLI} backfill"
            " --start {{ params.start }} --end {{ params.end }}"
        ),
    )
    golden = BashOperator(task_id="golden", bash_command=f"cd /opt/airflow/repo && {CLI} golden")
    reconcile = BashOperator(
        task_id="reconcile", bash_command=f"cd /opt/airflow/repo && {CLI} reconcile"
    )
    backfill >> golden >> reconcile
