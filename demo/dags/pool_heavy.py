"""Fans out 5 parallel tasks into a 2-slot custom pool (heavy_pool, created
by airflow-init). Three of them end up queued each run, which lights up:
  * the WAIT column of Active Runs (queued > 0), and
  * the Pools panel bar / utilisation for `heavy_pool`.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def hog(idx: int):
    print(f"hog #{idx} — occupying a heavy_pool slot for 45s")
    time.sleep(45)
    print(f"hog #{idx} — done")


with DAG(
    dag_id="pool_heavy",
    description="5 parallel tasks × 2-slot heavy_pool → queue + pool bar",
    start_date=datetime(2024, 1, 1),
    schedule="*/6 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=5)},
    tags=["afmetric-demo", "pool", "wait"],
) as dag:
    for i in range(5):
        PythonOperator(
            task_id=f"hog_{i}",
            python_callable=hog,
            op_kwargs={"idx": i},
            pool="heavy_pool",
            pool_slots=1,
        )
