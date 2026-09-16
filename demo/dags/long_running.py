"""Long-running task — stays in `running` for several minutes so it visibly
occupies Active Runs and gives the WAIT / duration columns real values to
show.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def slow_work():
    total = 5 * 60  # 5 minutes
    step = 10
    for elapsed in range(0, total, step):
        print(f"working… {elapsed}/{total}s")
        time.sleep(step)
    print("done")


with DAG(
    dag_id="long_running",
    description="Sleeps for ~5 minutes to stay visible in Active Runs",
    start_date=datetime(2024, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=10)},
    tags=["afmetric-demo", "long"],
) as dag:
    PythonOperator(task_id="slow_work", python_callable=slow_work)
