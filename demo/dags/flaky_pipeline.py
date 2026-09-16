"""Deliberately flaky task with retries — demonstrates the (try N/M) counter
and the yellow highlighting when older attempts exist.

The task fails on tries 1..3 with a delay, so afmetric shows the run stuck
in `up_for_retry` for a while, then finally succeeds on try 4.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def flaky(**context):
    try_number = context["ti"].try_number
    print(f"flaky attempt #{try_number}")
    if try_number < 4:
        # Plain exception → Airflow schedules a retry.
        # AirflowFailException would short-circuit retries, which is the
        # opposite of what we want here.
        raise RuntimeError(f"synthetic failure on try {try_number}")
    print("finally succeeded")


with DAG(
    dag_id="flaky_pipeline",
    description="Task with retries=3, fails on 1..3, succeeds on try 4",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "execution_timeout": timedelta(minutes=5),
    },
    tags=["afmetric-demo", "retry"],
) as dag:
    PythonOperator(task_id="flaky_task", python_callable=flaky)
