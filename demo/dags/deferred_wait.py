"""TimeDeltaSensorAsync — parks the task in `deferred` state, handed off to
the triggerer. Populates the WAIT column and confirms the triggerer
component is alive in the Cluster Health panel.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.time_delta import TimeDeltaSensorAsync

with DAG(
    dag_id="deferred_wait",
    description="TimeDeltaSensorAsync — deferred state, uses the triggerer",
    start_date=datetime(2024, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["afmetric-demo", "deferred", "wait"],
) as dag:
    TimeDeltaSensorAsync(
        task_id="wait_2m",
        delta=timedelta(minutes=2),
    )
