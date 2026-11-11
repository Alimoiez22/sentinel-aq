"""Daily guarded pipeline (M8).

snapshot (Track B record + fetch new/changed files of the snapshot locations)
  -> daily (register, process, detect, classify, act/escalate, verify; last 14 days, so
     retroactive revisions inside the window are applied)
  -> reconcile (golden rebuild + comparison; fails the run if published != golden)

Every task calls the `sentinel` CLI from the isolated venv (ADR-003), so each step can
also be run by hand (RUNBOOK). Publication is gated per location-day inside `daily`.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

CLI = "/opt/sentinel-venv/bin/sentinel"

with DAG(
    dag_id="sentinel_daily",
    # 06:00 UTC: OpenAQ writes day files ~72 h after the local day ends (RESEARCH §5).
    schedule="0 6 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,  # two overlapping runs would contend for the same location-days
    default_args={"retries": 0},  # recovery is the policy engine's job, not Airflow's
    tags=["sentinel"],
) as dag:
    snapshot = BashOperator(
        task_id="snapshot", bash_command=f"cd /opt/airflow/repo && {CLI} snapshot"
    )
    daily = BashOperator(
        task_id="daily_guarded", bash_command=f"cd /opt/airflow/repo && {CLI} daily"
    )
    golden = BashOperator(task_id="golden", bash_command=f"cd /opt/airflow/repo && {CLI} golden")
    reconcile = BashOperator(
        task_id="reconcile", bash_command=f"cd /opt/airflow/repo && {CLI} reconcile"
    )
    snapshot >> daily >> golden >> reconcile
